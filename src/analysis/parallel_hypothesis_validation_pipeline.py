import os
import json
import pandas as pd
import sys
import logging
import asyncio
from datetime import datetime
from typing import Dict, List, Tuple, Optional
import concurrent.futures

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from src.api import get_openai_response_async
from google.cloud import bigquery
from src.config import (
    PROJECT_ID,
    LLM_RESPONSES_DIR,
    SCHEMA_TXT_FILE,
    DATA_EXPLORATION_FILE,
    QUANTITATIVE_REPORT_FILE,
    HYPOTHESES_FILE,
    HYPOTHESIS_REPORTS_DIR
)
from src.api_monitor import api_monitor

# ――― ログ設定 ―――
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('parallel_hypothesis_validation.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# ――― 設定 ―――
SQL_RETRY_LIMIT = 10
ANALYSIS_RETRY_LIMIT = 5
MIN_REQUIRED_ROWS = 1
MAX_CONCURRENT_REQUESTS = 3  # 同時リクエスト数制限

os.environ["GOOGLE_CLOUD_PROJECT"] = PROJECT_ID
client = bigquery.Client(project=PROJECT_ID)

class ParallelHypothesisValidationPipeline:
    """並列処理対応の仮説検証パイプライン"""
    
    def __init__(self):
        self.schema_text = self.load_file(SCHEMA_TXT_FILE)
        self.data_exploration = self.load_file(DATA_EXPLORATION_FILE)
        self.hypotheses = self.load_hypotheses()
        self.semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)
        
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
    
    async def generate_analysis_plan_async(self, hypothesis: Dict) -> List[Dict]:
        """仮説に基づいて分析計画を非同期生成"""
        async with self.semaphore:  # 同時リクエスト数制限
            
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
この仮説を適切に検証するために、最大3つのSQLクエリからなる分析計画を立ててください。
各ステップでは比較対象を明確にし、仮説の妥当性を多角的に検証してください。

重要: 実際に存在するイベント名のみ使用してください：
- page_view, user_engagement, scroll, view_item, session_start
- add_to_cart, begin_checkout, add_shipping_info, add_payment_info, purchase

以下のJSON形式で分析計画を出力してください：

```json
{{
  "analysis_steps": [
    {{
      "step_id": "step1",
      "title": "基本分析",
      "purpose": "基本的な遷移率を測定",
      "sql_requirements": [
        "実際のイベント名を使用",
        "遷移率を計算"
      ]
    }},
    {{
      "step_id": "step2", 
      "title": "比較分析",
      "purpose": "対照群との比較",
      "sql_requirements": [
        "条件別の比較",
        "統計的な差異を確認"
      ]
    }}
  ]
}}
```

JSONのみを出力してください：
"""
            
            hyp_id = hypothesis.get("id", "UNKNOWN")
            response = await get_openai_response_async(
                prompt, 
                request_type="analysis_plan",
                hypothesis_id=hyp_id
            )
            
            # JSONの抽出
            if "```json" in response:
                response = response.split("```json")[1].split("```")[0]
            elif "```" in response:
                response = response.split("```")[1].split("```")[0]
            
            try:
                return json.loads(response.strip())["analysis_steps"]
            except (json.JSONDecodeError, KeyError) as e:
                logger.error(f"分析計画の解析に失敗: {e}")
                # フォールバック
                return [
                    {
                        "step_id": "step1",
                        "title": "基本分析", 
                        "purpose": "基本的な遷移率分析",
                        "sql_requirements": ["基本遷移率を計算"]
                    }
                ]
    
    async def generate_sql_for_step_async(self, hypothesis: Dict, analysis_step: Dict, error_message: str = "") -> str:
        """分析ステップに基づいてSQLを非同期生成"""
        async with self.semaphore:  # 同時リクエスト数制限
            
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
            
            hyp_id = hypothesis.get("id", "UNKNOWN")
            step_id = analysis_step["step_id"]
            
            return await get_openai_response_async(
                prompt,
                request_type="sql_generation",
                hypothesis_id=hyp_id,
                step_id=step_id
            )
    
    def execute_sql_sync(self, sql: str) -> Tuple[Optional[pd.DataFrame], str]:
        """SQLを同期実行"""
        try:
            job = client.query(sql)
            result_df = job.to_dataframe()
            
            if result_df.empty:
                return None, "クエリ結果が空です"
            
            if len(result_df) < MIN_REQUIRED_ROWS:
                return None, f"結果行数が不足しています: {len(result_df)}行"
            
            return result_df, ""
            
        except Exception as e:
            return None, f"SQL実行エラー: {str(e)}"
    
    async def execute_sql_with_retry_async(self, hypothesis: Dict, analysis_step: Dict) -> Tuple[Optional[pd.DataFrame], str]:
        """SQLを非同期実行（再帰的リトライ付き）"""
        hyp_id = hypothesis.get("id", "UNKNOWN")
        step_id = analysis_step["step_id"]
        logger.info(f"🚀 {hyp_id}-{step_id}: SQL生成と実行を開始")
        
        error_message = ""
        
        for attempt in range(1, SQL_RETRY_LIMIT + 1):
            logger.info(f"  試行 {attempt}/{SQL_RETRY_LIMIT}")
            
            # SQL生成（非同期）
            try:
                sql = await self.generate_sql_for_step_async(hypothesis, analysis_step, error_message)
                
                # SQLからマークダウンブロックを除去
                if sql.startswith("```sql"):
                    sql = sql[6:]
                if sql.endswith("```"):
                    sql = sql[:-3]
                sql = sql.strip()
                
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
            
            # SQL実行（非同期）
            loop = asyncio.get_event_loop()
            result_df, sql_error = await loop.run_in_executor(
                None, self.execute_sql_sync, sql
            )
            
            if result_df is not None:
                logger.info(f"  ✅ SQL実行成功: {len(result_df)}行の結果")
                return result_df, ""
            else:
                error_message = sql_error
                logger.warning(f"  ⚠️ {error_message}")
                
                if attempt == SQL_RETRY_LIMIT:
                    logger.error(f"  ❌ {hyp_id}-{step_id}: {SQL_RETRY_LIMIT}回試行後も失敗")
                    return None, error_message
        
        return None, error_message
    
    async def generate_comprehensive_analysis_report_async(self, hypothesis: Dict, analysis_results: List[Tuple[Dict, pd.DataFrame]]) -> str:
        """複数ステップの結果を統合した包括的な分析レポートを非同期生成"""
        async with self.semaphore:  # 同時リクエスト数制限
            
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
以下の複数ステップの分析結果を統合して、仮説検証レポートを作成してください。

## 検証した仮説
```json
{json.dumps(hypothesis, ensure_ascii=False, indent=2)}
```

## 複数ステップの分析結果
{results_summary}

## レポート要件
1. 仮説が検証されたかどうかを端的に述べてください
2. 分析の過程と各ステップの結果を記載してください
3. 比較分析があれば、その差異を明確に示してください
4. 仮説の検証結果を数値的根拠とともに述べてください
5. 専門的すぎず、わかりやすく書いてください

## 重要な数値表現ガイドライン
- 数値を「高い」「低い」と評価する際は、必ず比較の基準を明示してください
- 例: ✅「バウンス率10.39%は業界平均15%と比較して低い」
- 例: ❌「バウンス率は高い10.39%」
- 複数のセグメント間で比較がある場合: 「Aセグメントの10.39%に対してBセグメントは15.2%で4.81ポイント高い」
- 単一のデータポイントしかない場合: 客観的に数値を報告し、絶対的な評価（高い/低い）は避けてください
- 「より高い」「より低い」など相対的な表現を優先してください

注意: ビジネス上の示唆や改善提案は含めないでください。

日本語でレポートを作成してください：
"""
            
            hyp_id = hypothesis.get("id", "UNKNOWN")
            return await get_openai_response_async(
                prompt,
                request_type="comprehensive_report",
                hypothesis_id=hyp_id
            )
    
    async def process_single_hypothesis_async(self, hypothesis: Dict) -> bool:
        """単一仮説の非同期処理"""
        hyp_id = hypothesis.get("id", "UNKNOWN")
        logger.info(f"\n{'='*60}")
        logger.info(f"🎯 処理開始: {hyp_id}")
        logger.info(f"{'='*60}")
        
        try:
            # 分析計画の生成
            logger.info(f"📋 {hyp_id}: 分析計画を生成中...")
            analysis_plan = await self.generate_analysis_plan_async(hypothesis)
            logger.info(f"  📋 分析計画: {len(analysis_plan)}ステップ")
            
            # 各ステップを並列実行（ただし同時数制限あり）
            analysis_results = []
            
            # ステップを順次実行（SQLエラーの場合の依存関係を考慮）
            for step in analysis_plan:
                step_id = step["step_id"]
                logger.info(f"\n  🔄 ステップ実行: {step_id} - {step['title']}")
                
                result_df, error = await self.execute_sql_with_retry_async(hypothesis, step)
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
            report = await self.generate_comprehensive_analysis_report_async(hypothesis, analysis_results)
            
            # レポートファイル保存
            report_file = os.path.join(HYPOTHESIS_REPORTS_DIR, f"{hyp_id}_parallel_comprehensive_report.txt")
            with open(report_file, "w", encoding="utf-8") as f:
                f.write(report)
            
            logger.info(f"✅ {hyp_id}: 処理完了 - {report_file}")
            return True
            
        except Exception as e:
            logger.error(f"❌ {hyp_id}: 予期しないエラー - {str(e)}")
            return False
    
    async def run_pipeline_async(self) -> None:
        """パイプライン非同期実行"""
        start_time = datetime.now()
        logger.info(f"\n🚀 並列仮説検証パイプライン開始: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info(f"検証対象: {len(self.hypotheses)}件の仮説")
        logger.info(f"最大同時リクエスト数: {MAX_CONCURRENT_REQUESTS}")
        
        # 全仮説を並列処理
        tasks = [
            self.process_single_hypothesis_async(hypothesis)
            for hypothesis in self.hypotheses
        ]
        
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # 成功した仮説を集計
        successful_hypotheses = []
        for i, (hypothesis, result) in enumerate(zip(self.hypotheses, results)):
            if isinstance(result, bool) and result:
                successful_hypotheses.append(hypothesis.get("id", f"H{i+1:03d}"))
            elif isinstance(result, Exception):
                logger.error(f"仮説 {hypothesis.get('id', f'H{i+1:03d}')} でエラー: {result}")
        
        # セッションサマリー保存
        session_summary = await api_monitor.save_session_summary("parallel_session_summary.json")
        
        # 結果サマリー
        end_time = datetime.now()
        duration = end_time - start_time
        
        logger.info(f"\n{'='*60}")
        logger.info(f"🎉 並列パイプライン完了")
        logger.info(f"{'='*60}")
        logger.info(f"実行時間: {duration}")
        logger.info(f"成功: {len(successful_hypotheses)}/{len(self.hypotheses)} 件")
        logger.info(f"成功率: {len(successful_hypotheses)/len(self.hypotheses)*100:.1f}%")
        logger.info(f"総API利用料金: ${session_summary['total_cost_usd']:.4f}")
        logger.info(f"平均リクエスト単価: ${session_summary['average_cost_per_request']:.4f}")
        
        if successful_hypotheses:
            logger.info(f"成功した仮説: {', '.join(successful_hypotheses)}")

def main():
    """メイン実行"""
    try:
        pipeline = ParallelHypothesisValidationPipeline()
        asyncio.run(pipeline.run_pipeline_async())
    except Exception as e:
        logger.error(f"パイプライン実行エラー: {str(e)}")
        raise

if __name__ == "__main__":
    main()