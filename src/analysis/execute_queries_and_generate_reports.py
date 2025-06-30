import os
import json
import pandas as pd
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from src.api import get_openai_response
from google.cloud import bigquery
from src.config import (
    PROJECT_ID,
    LLM_RESPONSES_DIR,
    SCHEMA_TXT_FILE,
    MAX_RETRIES,
    QUANTITATIVE_REPORT_FILE
)

os.environ["GOOGLE_CLOUD_PROJECT"] = PROJECT_ID
client = bigquery.Client(project=PROJECT_ID)

# ――― ヘルパー：スキーマ要約読み込み ―――
def load_schema_text(path: str) -> str:
    with open(path, "r", encoding="utf-8") as f:
        return f.read().strip()

# ――― レポート用 LLM 呼び出し ―――
def call_llm_for_report(summary_df: pd.DataFrame, schema_text: str) -> str:
    # 仮説IDごとに平均をまとめたテーブルを文字列化
    table_str = summary_df.round(3).to_string(index=False)
    prompt = (
        "以下は GA4 イベントテーブルのスキーマ要約です：\n"
        "```\n" + schema_text + "\n```\n\n"
        "このスキーマを前提に、次の定量的分析結果をもとに"
        "「各仮説の検証結果」という見出しで日本語のレポートを作成してください。\n\n"
        "```csv\n" + table_str + "\n```\n\n"
        "## 重要な数値表現ガイドライン\n"
        "- 数値を「高い」「低い」と評価する際は、必ず比較の基準を明示してください\n"
        "- 例: ✅「バウンス率10.39%は業界平均15%と比較して低い」\n"
        "- 例: ❌「バウンス率は高い10.39%」\n"
        "- 複数の仮説間で比較がある場合: 「仮説Aの10.39%に対して仮説Bは15.2%で4.81ポイント高い」\n"
        "- 単一のデータポイントしかない場合: 客観的に数値を報告し、絶対的な評価（高い/低い）は避けてください\n"
        "- 「より高い」「より低い」など相対的な表現を優先してください\n"
        "- 時系列比較や期間比較がある場合: 「前月比で2.3ポイント上昇」「前年同期と比較して15%改善」など具体的に表現してください\n"
    )
    return get_openai_response(prompt).strip()

# ――― メイン処理 ―――
def main():
    # スキーマを読み込む
    schema_text = load_schema_text(SCHEMA_TXT_FILE)

    records = []
    for fname in sorted(os.listdir(LLM_RESPONSES_DIR)):
        if not fname.endswith(".json"):
            continue
        hyp_id = os.path.splitext(fname)[0]
        path   = os.path.join(LLM_RESPONSES_DIR, fname)
        with open(path, "r", encoding="utf-8") as f:
            payload = json.load(f)

        sql = payload.get("sql")
        if not sql:
            continue

        # SQL 実行を最大 MAX_RETRIES 回リトライ
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                job = client.query(sql)
                for row in job.result():
                    rec = dict(row)
                    rec["hypothesis_id"] = hyp_id
                    records.append(rec)
                break
            except Exception as e:
                err = str(e)
                print(f"⚠️ {hyp_id} attempt {attempt} failed: {err}")
                # LLM再生成を行う場合はここで error_message を ask_llm に渡すなど追加可能
                if attempt == MAX_RETRIES:
                    print(f"❌ {hyp_id} failed after {MAX_RETRIES} attempts. Skipping.")

    if not records:
        print("⚠️ 分析結果が取得できませんでした。")
        return

    # DataFrame にまとめて仮説ごとに平均値を集計
    df = pd.DataFrame(records)
    summary = (
        df.groupby("hypothesis_id", as_index=False)
          [["step3_users", "step4_users", "transition_rate"]]
          .mean()
    )

    # スキーマと集計結果を渡してレポート生成
    report = call_llm_for_report(summary, schema_text)

    # レポートをファイルに保存
    with open(QUANTITATIVE_REPORT_FILE, "w", encoding="utf-8") as f:
        f.write(report)
    print(f"✅ 定量レポートを「{QUANTITATIVE_REPORT_FILE}」に保存しました。")

if __name__ == "__main__":
    main()
