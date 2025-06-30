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
        logging.FileHandler('multi_step_hypothesis_validation.log', encoding='utf-8'),
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

class MultiStepHypothesisValidationPipeline:
    """複数ステップの仮説検証パイプライン"""
    
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
    
    def generate_analysis_plan(self, hypothesis: Dict) -> List[Dict]:
        """仮説に基づいて分析計画を生成"""
        
        prompt = f"""
あなたはデータ分析のエキスパートです。以下の仮説を検証するための分析計画を立ててください。

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

## 要件
この仮説を適切に検証するために、複数のSQLクエリからなる分析計画を立ててください。
各ステップでは比較対象を明確にし、仮説の妥当性を多角的に検証してください。

以下のJSON形式で分析計画を出力してください：

```json
{{
  "analysis_steps": [
    {{
      "step_id": "step1",
      "title": "ステップの説明",
      "purpose": "このステップの目的",
      "sql_requirements": [
        "SQL要件1",
        "SQL要件2"
      ]
    }},
    {{
      "step_id": "step2", 
      "title": "比較分析",
      "purpose": "対照群との比較",
      "sql_requirements": [
        "比較用SQL要件1",
        "比較用SQL要件2"
      ]
    }}
  ]
}}
```

JSONのみを出力してください（説明文は不要）：
"""
        
        response = get_openai_response(prompt).strip()
        
        # JSONの抽出
        if "```json" in response:
            response = response.split("```json")[1].split("```")[0]
        elif "```" in response:
            response = response.split("```")[1].split("```")[0]
        
        try:
            return json.loads(response.strip())["analysis_steps"]
        except (json.JSONDecodeError, KeyError) as e:
            logger.error(f"分析計画の解析に失敗: {e}")
            # フォールバック: 基本的な分析計画を返す
            return [
                {
                    "step_id": "step1",
                    "title": "基本分析",
                    "purpose": "仮説の基本検証",
                    "sql_requirements": ["基本的な遷移率分析を実行"]
                }
            ]
    
    def generate_sql_for_step(self, hypothesis: Dict, analysis_step: Dict, error_message: str = "") -> str:
        """分析ステップに基づいてSQLを生成"""
        
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
あなたはBigQueryのエキスパートです。以下の情報を元に、分析ステップに対応するSQLクエリを生成してください。

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

## 実行する分析ステップ
- **ステップID**: {analysis_step['step_id']}
- **タイトル**: {analysis_step['title']}
- **目的**: {analysis_step['purpose']}
- **要件**: {', '.join(analysis_step['sql_requirements'])}

{retry_context}

## SQL生成要件
1. 必ず実際に存在するイベント名を使用してください
2. WHERE _TABLE_SUFFIX BETWEEN '20201101' AND '20210131' を含めてください
3. 分析ステップの目的に沿った適切なカラムを含めてください
4. 必要に応じてグループ化や集計を行ってください
5. 結果が比較しやすい形式で出力してください

SQLのみを出力してください（説明文は不要）：
"""
        
        return get_openai_response(prompt).strip()
    
    def execute_sql_with_retry(self, hypothesis: Dict, analysis_step: Dict) -> Tuple[Optional[pd.DataFrame], str]:
        """SQLを実行（再帰的リトライ付き）"""
        hyp_id = hypothesis.get("id", "UNKNOWN")
        step_id = analysis_step["step_id"]
        logger.info(f"🚀 {hyp_id}-{step_id}: SQL生成と実行を開始")
        
        error_message = ""
        
        for attempt in range(1, SQL_RETRY_LIMIT + 1):
            logger.info(f"  試行 {attempt}/{SQL_RETRY_LIMIT}")
            
            # SQL生成
            try:
                sql = self.generate_sql_for_step(hypothesis, analysis_step, error_message)
                
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
                sql_file = os.path.join(LLM_RESPONSES_DIR, f"{hyp_id}_{step_id}.json")
                with open(sql_file, "w", encoding="utf-8") as f:
                    json.dump({
                        "sql": sql, 
                        "step": analysis_step,
                        "attempt": attempt, 
                        "timestamp": datetime.now().isoformat()
                    }, f, ensure_ascii=False, indent=2)
                
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
                
                logger.info(f"  ✅ SQL実行成功: {len(result_df)}行の結果")
                return result_df, ""
                
            except Exception as e:
                error_message = f"SQL実行エラー: {str(e)}"
                logger.warning(f"  ⚠️ {error_message}")
                
                if attempt == SQL_RETRY_LIMIT:
                    logger.error(f"  ❌ {hyp_id}-{step_id}: {SQL_RETRY_LIMIT}回試行後も失敗")
                    return None, error_message
        
        return None, error_message
    
    def generate_comprehensive_analysis_report(self, hypothesis: Dict, analysis_results: List[Tuple[Dict, pd.DataFrame]]) -> str:
        """複数ステップの結果を統合した包括的な分析レポートを生成"""
        
        # 全結果を文字列化
        results_summary = ""
        for step, result_df in analysis_results:
            results_summary += f"\n\n### {step['title']} ({step['step_id']})\n"
            results_summary += f"目的: {step['purpose']}\n\n"
            results_summary += "データ:\n"
            results_summary += result_df.to_string(index=False)
            results_summary += "\n\n統計サマリー:\n"
            results_summary += result_df.describe().to_string()
        
        prompt = f"""
以下の複数ステップの分析結果を統合して、包括的な仮説検証レポートを作成してください。

## 検証した仮説
```json
{json.dumps(hypothesis, ensure_ascii=False, indent=2)}
```

## 複数ステップの分析結果
{results_summary}

## レポート要件
1. 各ステップの結果を統合的に解釈してください
2. 比較分析があれば、その差異を明確に示してください
3. 仮説の検証結果を数値的根拠とともに述べてください
4. ビジネス上の示唆を含めてください
5. 改善提案があれば含めてください
6. 専門的すぎず、ビジネス担当者にもわかりやすく書いてください

## 重要な数値表現ガイドライン
- 数値を「高い」「低い」と評価する際は、必ず比較の基準を明示してください
- 例: ✅「バウンス率10.39%は業界平均15%と比較して低い」
- 例: ❌「バウンス率は高い10.39%」
- 複数のセグメント間で比較がある場合: 「Aセグメントの10.39%に対してBセグメントは15.2%で4.81ポイント高い」
- 単一のデータポイントしかない場合: 客観的に数値を報告し、絶対的な評価（高い/低い）は避けてください
- 「より高い」「より低い」など相対的な表現を優先してください
- 時系列比較や期間比較がある場合: 「前月比で2.3ポイント上昇」「前年同期と比較して15%改善」など具体的に表現してください

日本語でレポートを作成してください：
"""
        
        return get_openai_response(prompt).strip()
    
    def process_single_hypothesis(self, hypothesis: Dict) -> bool:
        """単一仮説の処理（複数ステップ）"""
        hyp_id = hypothesis.get("id", "UNKNOWN")
        logger.info(f"\n{'='*60}")
        logger.info(f"🎯 処理開始: {hyp_id}")
        logger.info(f"{'='*60}")
        
        try:
            # 分析計画の生成
            logger.info(f"📋 {hyp_id}: 分析計画を生成中...")
            analysis_plan = self.generate_analysis_plan(hypothesis)
            logger.info(f"  📋 分析計画: {len(analysis_plan)}ステップ")
            
            # 各ステップを実行
            analysis_results = []
            for step in analysis_plan:
                step_id = step["step_id"]
                logger.info(f"\n  🔄 ステップ実行: {step_id} - {step['title']}")
                
                result_df, error = self.execute_sql_with_retry(hypothesis, step)
                if result_df is None:
                    logger.error(f"    ❌ {step_id}: SQL実行に失敗 - {error}")
                    continue
                
                analysis_results.append((step, result_df))
                logger.info(f"    ✅ {step_id}: 完了")
            
            if not analysis_results:
                logger.error(f"❌ {hyp_id}: 全ステップが失敗しました")
                return False
            
            # 包括的分析レポート生成
            logger.info(f"📊 {hyp_id}: 包括的レポート生成中...")
            report = self.generate_comprehensive_analysis_report(hypothesis, analysis_results)
            
            # レポートファイル保存
            report_file = os.path.join(HYPOTHESIS_REPORTS_DIR, f"{hyp_id}_comprehensive_report.txt")
            with open(report_file, "w", encoding="utf-8") as f:
                f.write(report)
            
            logger.info(f"✅ {hyp_id}: 処理完了 - {report_file}")
            return True
            
        except Exception as e:
            logger.error(f"❌ {hyp_id}: 予期しないエラー - {str(e)}")
            return False
    
    def run_pipeline(self) -> None:
        """パイプライン実行"""
        start_time = datetime.now()
        logger.info(f"\n🚀 複数ステップ仮説検証パイプライン開始: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info(f"検証対象: {len(self.hypotheses)}件の仮説")
        
        successful_hypotheses = []
        
        for i, hypothesis in enumerate(self.hypotheses, 1):
            logger.info(f"\n進捗: {i}/{len(self.hypotheses)}")
            
            success = self.process_single_hypothesis(hypothesis)
            if success:
                successful_hypotheses.append(hypothesis.get("id", f"H{i:03d}"))
        
        # 結果サマリー
        end_time = datetime.now()
        duration = end_time - start_time
        
        logger.info(f"\n{'='*60}")
        logger.info(f"🎉 パイプライン完了")
        logger.info(f"{'='*60}")
        logger.info(f"実行時間: {duration}")
        logger.info(f"成功: {len(successful_hypotheses)}/{len(self.hypotheses)} 件")
        logger.info(f"成功率: {len(successful_hypotheses)/len(self.hypotheses)*100:.1f}%")
        
        if successful_hypotheses:
            logger.info(f"成功した仮説: {', '.join(successful_hypotheses)}")

def main():
    """メイン実行"""
    try:
        pipeline = MultiStepHypothesisValidationPipeline()
        pipeline.run_pipeline()
    except Exception as e:
        logger.error(f"パイプライン実行エラー: {str(e)}")
        raise

if __name__ == "__main__":
    main()