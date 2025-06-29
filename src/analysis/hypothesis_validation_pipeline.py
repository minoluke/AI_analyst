import os
import json
import pandas as pd
import sys
import logging
from datetime import datetime
from typing import Dict, List, Tuple, Optional

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from src.api import get_openai_response
from google.cloud import bigquery
from src.config import (
    PROJECT_ID,
    LLM_RESPONSES_DIR,
    SCHEMA_TXT_FILE,
    DATA_EXPLORATION_FILE,
    MAX_RETRIES,
    QUANTITATIVE_REPORT_FILE,
    HYPOTHESES_FILE,
    HYPOTHESIS_REPORTS_DIR
)

# ――― ログ設定 ―――
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('hypothesis_validation.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# ――― 設定 ―――
SQL_RETRY_LIMIT = 5  # SQL修正の上限回数
ANALYSIS_RETRY_LIMIT = 3  # 分析結果修正の上限回数
MIN_REQUIRED_ROWS = 1  # 最低限必要な結果行数

os.environ["GOOGLE_CLOUD_PROJECT"] = PROJECT_ID
client = bigquery.Client(project=PROJECT_ID)

class HypothesisValidationPipeline:
    """仮説検証パイプライン"""
    
    def __init__(self):
        self.schema_text = self.load_file(SCHEMA_TXT_FILE)
        self.data_exploration = self.load_file(DATA_EXPLORATION_FILE)
        self.hypotheses = self.load_hypotheses()
        
    def load_file(self, filepath: str) -> str:
        """ファイルを読み込む"""
        try:
            with open(filepath, "r", encoding="utf-8") as f:
                return f.read().strip()
        except FileNotFoundError:
            logger.error(f"ファイルが見つかりません: {filepath}")
            return ""
    
    def load_hypotheses(self) -> List[Dict]:
        """仮説ファイルを読み込む"""
        try:
            with open(HYPOTHESES_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except FileNotFoundError:
            logger.error(f"仮説ファイルが見つかりません: {HYPOTHESES_FILE}")
            return []
    
    def generate_sql_with_context(self, hypothesis: Dict, error_message: str = "") -> str:
        """仮説に基づいてSQLを生成（エラーメッセージ考慮）"""
        
        retry_context = ""
        if error_message:
            retry_context = f"""
            
**前回のSQL実行でエラーが発生しました:**
```
{error_message}
```

上記のエラーを解決して、正しく動作するSQLを生成してください。
"""
        
        prompt = f"""
あなたはBigQueryのエキスパートです。以下の情報を元に、仮説を検証するためのSQLクエリを生成してください。

## テーブル構造定義
```
{self.schema_text}
```

## 実際のデータ内容
```
{self.data_exploration}
```

## 検証する仮説
```json
{json.dumps(hypothesis, ensure_ascii=False, indent=2)}
```

{retry_context}

## 要件
1. 必ず実際に存在するイベント名を使用してください
2. WHERE _TABLE_SUFFIX BETWEEN '20201101' AND '20210131' を含めてください
3. 結果には以下のカラムを含めてください：
   - step3_users: ステップ3のユーザー数
   - step4_users: ステップ4のユーザー数  
   - transition_rate: 遷移率 (step4_users / step3_users)
4. デバイスカテゴリや流入元など、仮説の条件に合わせてセグメントしてください

SQLのみを出力してください（説明文は不要）：
"""
        
        return get_openai_response(prompt).strip()
    
    def execute_sql_with_retry(self, hypothesis: Dict) -> Tuple[Optional[pd.DataFrame], str]:
        """SQLを実行（再帰的リトライ付き）"""
        hyp_id = hypothesis.get("id", "UNKNOWN")
        logger.info(f"🚀 {hyp_id}: SQL生成と実行を開始")
        
        error_message = ""
        
        for attempt in range(1, SQL_RETRY_LIMIT + 1):
            logger.info(f"  試行 {attempt}/{SQL_RETRY_LIMIT}")
            
            # SQL生成
            try:
                sql = self.generate_sql_with_context(hypothesis, error_message)
                
                # SQLからマークダウンブロックを除去
                if sql.startswith("```sql"):
                    sql = sql[6:]  # ```sql を除去
                if sql.endswith("```"):
                    sql = sql[:-3]  # ``` を除去
                sql = sql.strip()
                
                # SQLの妥当性チェック
                if not sql or len(sql.strip()) < 20:
                    raise ValueError("生成されたSQLが短すぎます")
                
                # SQLをファイルに保存
                sql_file = os.path.join(LLM_RESPONSES_DIR, f"{hyp_id}.json")
                with open(sql_file, "w", encoding="utf-8") as f:
                    json.dump({"sql": sql, "attempt": attempt, "timestamp": datetime.now().isoformat()}, f, ensure_ascii=False, indent=2)
                
                logger.info(f"  SQL生成完了: {len(sql)}文字")
                
            except Exception as e:
                error_message = f"SQL生成エラー: {str(e)}"
                logger.warning(f"  {error_message}")
                continue
            
            # SQL実行
            try:
                logger.info(f"  SQL実行開始...")
                job = client.query(sql)
                result_df = job.to_dataframe()
                
                # 結果の検証
                if result_df.empty:
                    raise ValueError("クエリ結果が空です")
                
                if len(result_df) < MIN_REQUIRED_ROWS:
                    raise ValueError(f"結果行数が不足しています: {len(result_df)}行")
                
                # 必要なカラムの存在確認
                required_columns = ['step3_users', 'step4_users', 'transition_rate']
                missing_columns = [col for col in required_columns if col not in result_df.columns]
                
                if missing_columns:
                    raise ValueError(f"必要なカラムが不足: {missing_columns}")
                
                logger.info(f"  ✅ SQL実行成功: {len(result_df)}行の結果")
                return result_df, ""
                
            except Exception as e:
                error_message = f"SQL実行エラー: {str(e)}"
                logger.warning(f"  ⚠️ {error_message}")
                
                if attempt == SQL_RETRY_LIMIT:
                    logger.error(f"  ❌ {hyp_id}: {SQL_RETRY_LIMIT}回試行後も失敗")
                    return None, error_message
        
        return None, error_message
    
    def generate_analysis_report(self, hypothesis: Dict, result_df: pd.DataFrame, previous_error: str = "") -> str:
        """分析レポートを生成"""
        
        retry_context = ""
        if previous_error:
            retry_context = f"""
            
**前回の分析で問題がありました:**
```
{previous_error}
```

上記の問題を解決して、より詳細で有用な分析レポートを作成してください。
"""
        
        # 結果を文字列化
        result_summary = result_df.describe().to_string()
        result_data = result_df.to_string(index=False)
        
        prompt = f"""
以下の仮説検証結果を分析して、日本語でレポートを作成してください。

## 検証した仮説
```json
{json.dumps(hypothesis, ensure_ascii=False, indent=2)}
```

## 分析結果データ
```
{result_data}
```

## 統計サマリー
```
{result_summary}
```

{retry_context}

## レポート要件
1. 仮説の検証結果を明確に述べてください
2. 数値的な根拠を示してください
3. ビジネス上の示唆を含めてください
4. 改善提案があれば含めてください
5. 専門的すぎず、ビジネス担当者にもわかりやすく書いてください

日本語でレポートを作成してください：
"""
        
        return get_openai_response(prompt).strip()
    
    def validate_analysis_quality(self, report: str) -> Tuple[bool, str]:
        """分析レポートの品質を検証"""
        
        if len(report) < 100:
            return False, "レポートが短すぎます"
        
        if "仮説" not in report:
            return False, "仮説に関する言及がありません"
        
        if not any(char.isdigit() for char in report):
            return False, "数値的根拠が含まれていません"
        
        return True, ""
    
    def generate_analysis_with_retry(self, hypothesis: Dict, result_df: pd.DataFrame) -> str:
        """分析レポート生成（再帰的リトライ付き）"""
        hyp_id = hypothesis.get("id", "UNKNOWN")
        logger.info(f"📊 {hyp_id}: 分析レポート生成開始")
        
        previous_error = ""
        
        for attempt in range(1, ANALYSIS_RETRY_LIMIT + 1):
            logger.info(f"  分析試行 {attempt}/{ANALYSIS_RETRY_LIMIT}")
            
            try:
                # レポート生成
                report = self.generate_analysis_report(hypothesis, result_df, previous_error)
                
                # 品質検証
                is_valid, error_msg = self.validate_analysis_quality(report)
                
                if is_valid:
                    logger.info(f"  ✅ 分析レポート生成成功: {len(report)}文字")
                    return report
                else:
                    previous_error = error_msg
                    logger.warning(f"  ⚠️ 品質問題: {error_msg}")
                    
            except Exception as e:
                previous_error = f"レポート生成エラー: {str(e)}"
                logger.warning(f"  ⚠️ {previous_error}")
        
        # 最終的に失敗した場合は基本的なレポートを返す
        logger.warning(f"  🔄 基本レポートで代替")
        return f"""
# {hyp_id} 検証結果

## 仮説
{hypothesis.get('summary', '不明')}

## 結果
分析は完了しましたが、詳細レポートの生成に失敗しました。
データは正常に取得されており、結果は {len(result_df)} 行です。

手動での詳細分析をお勧めします。
"""
    
    def process_single_hypothesis(self, hypothesis: Dict) -> bool:
        """単一仮説の処理"""
        hyp_id = hypothesis.get("id", "UNKNOWN")
        logger.info(f"\n{'='*60}")
        logger.info(f"🎯 処理開始: {hyp_id}")
        logger.info(f"{'='*60}")
        
        try:
            # SQL実行（再帰的リトライ）
            result_df, sql_error = self.execute_sql_with_retry(hypothesis)
            
            if result_df is None:
                logger.error(f"❌ {hyp_id}: SQL実行に失敗しました - {sql_error}")
                return False
            
            # 分析レポート生成（再帰的リトライ）
            report = self.generate_analysis_with_retry(hypothesis, result_df)
            
            # レポートファイル保存
            report_file = os.path.join(HYPOTHESIS_REPORTS_DIR, f"{hyp_id}_report.txt")
            with open(report_file, "w", encoding="utf-8") as f:
                f.write(report)
            
            logger.info(f"✅ {hyp_id}: 処理完了 - {report_file}")
            return True
            
        except Exception as e:
            logger.error(f"❌ {hyp_id}: 予期しないエラー - {str(e)}")
            return False
    
    def generate_final_report(self, successful_hypotheses: List[str]) -> None:
        """最終統合レポート生成"""
        logger.info("\n📋 最終統合レポート生成中...")
        
        if not successful_hypotheses:
            logger.warning("成功した仮説がありません")
            return
        
        # 各仮説レポートを読み込み
        all_reports = []
        for hyp_id in successful_hypotheses:
            report_file = os.path.join(HYPOTHESIS_REPORTS_DIR, f"{hyp_id}_report.txt")
            try:
                with open(report_file, "r", encoding="utf-8") as f:
                    content = f.read().strip()
                    all_reports.append(f"## {hyp_id}\n\n{content}")
            except FileNotFoundError:
                logger.warning(f"レポートファイルが見つかりません: {report_file}")
        
        # 統合レポート作成
        final_report = f"""# GA4 仮説検証 統合レポート

**生成日時**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
**検証成功**: {len(successful_hypotheses)}件 / {len(self.hypotheses)}件

## 検証結果サマリー

{chr(10).join(all_reports)}

## 総合所見

本検証により、{len(successful_hypotheses)}件の仮説について定量的な検証を完了しました。
各仮説の詳細な分析結果は上記をご参照ください。

---
*このレポートは自動生成されました*
"""
        
        # ファイルに保存
        with open(QUANTITATIVE_REPORT_FILE, "w", encoding="utf-8") as f:
            f.write(final_report)
        
        logger.info(f"✅ 統合レポート生成完了: {QUANTITATIVE_REPORT_FILE}")
    
    def run_pipeline(self) -> None:
        """パイプライン実行"""
        start_time = datetime.now()
        logger.info(f"\n🚀 仮説検証パイプライン開始: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info(f"検証対象: {len(self.hypotheses)}件の仮説")
        
        successful_hypotheses = []
        
        for i, hypothesis in enumerate(self.hypotheses, 1):
            logger.info(f"\n進捗: {i}/{len(self.hypotheses)}")
            
            success = self.process_single_hypothesis(hypothesis)
            if success:
                successful_hypotheses.append(hypothesis.get("id", f"H{i:03d}"))
        
        # 最終レポート生成
        self.generate_final_report(successful_hypotheses)
        
        # 結果サマリー
        end_time = datetime.now()
        duration = end_time - start_time
        
        logger.info(f"\n{'='*60}")
        logger.info(f"🎉 パイプライン完了")
        logger.info(f"{'='*60}")
        logger.info(f"実行時間: {duration}")
        logger.info(f"成功: {len(successful_hypotheses)}/{len(self.hypotheses)} 件")
        logger.info(f"成功率: {len(successful_hypotheses)/len(self.hypotheses)*100:.1f}%")
        logger.info(f"統合レポート: {QUANTITATIVE_REPORT_FILE}")
        
        if successful_hypotheses:
            logger.info(f"成功した仮説: {', '.join(successful_hypotheses)}")

def main():
    """メイン実行"""
    try:
        pipeline = HypothesisValidationPipeline()
        pipeline.run_pipeline()
    except Exception as e:
        logger.error(f"パイプライン実行エラー: {str(e)}")
        raise

if __name__ == "__main__":
    main()