import os
import json
import pandas as pd
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from src.api import get_openai_response
from google.cloud import bigquery
from src.config import (
    PROJECT_ID, 
    LLM_RESPONSES_DIR, 
    SCHEMA_TXT_FILE, 
    MAX_RETRIES, 
    HYPOTHESIS_REPORTS_DIR
)

os.environ["GOOGLE_CLOUD_PROJECT"] = PROJECT_ID
client = bigquery.Client(project=PROJECT_ID)

# ――― ヘルパー：スキーマ要約読み込み ―――
def load_schema_text(path: str) -> str:
    with open(path, "r", encoding="utf-8") as f:
        return f.read().strip()

# ――― レポート用 LLM 呼び出し ―――
def call_llm_for_report(hyp_id: str, summary_df: pd.DataFrame, schema_text: str) -> str:
    """
    仮説IDごとの定量結果 summary_df (1行) をもとに、
    スキーマ要約 schema_text を前提にした日本語レポートを生成。
    """
    table_str = summary_df.round(3).to_string(index=False)
    prompt = (
        f"以下は GA4 イベントテーブルのスキーマ要約です：\n"
        "```\n" + schema_text + "\n```\n\n"
        f"【仮説 ID】 {hyp_id}\n"
        "このスキーマを前提に、次の定量的分析結果をもとに"
        "「各仮説の検証結果」という見出しで日本語のレポートを作成してください。\n\n"
        "```csv\n" + table_str + "\n```\n\n"
        "## 重要な数値表現ガイドライン\n"
        "- 数値を「高い」「低い」と評価する際は、必ず比較の基準を明示してください\n"
        "- 例: ✅「バウンス率10.39%は業界平均15%と比較して低い」\n"
        "- 例: ❌「バウンス率は高い10.39%」\n"
        "- 複数のセグメント間で比較がある場合: 「Aセグメントの10.39%に対してBセグメントは15.2%で4.81ポイント高い」\n"
        "- 単一のデータポイントしかない場合: 客観的に数値を報告し、絶対的な評価（高い/低い）は避けてください\n"
        "- 「より高い」「より低い」など相対的な表現を優先してください\n"
        "- 時系列比較や期間比較がある場合: 「前月比で2.3ポイント上昇」「前年同期と比較して15%改善」など具体的に表現してください\n"
    )
    return get_openai_response(prompt).strip()

def main():
    schema_text = load_schema_text(SCHEMA_TXT_FILE)

    for fname in sorted(os.listdir(LLM_RESPONSES_DIR)):
        if not fname.endswith(".json"):
            continue
        hyp_id = os.path.splitext(fname)[0]
        path   = os.path.join(LLM_RESPONSES_DIR, fname)
        with open(path, "r", encoding="utf-8") as f:
            payload = json.load(f)

        sql = payload.get("sql")
        if not sql:
            print(f"⚠️ {hyp_id}：SQL定義がありません。スキップします。")
            continue

        # 仮説ごとの実行レコードをためる
        records = []

        # 最大 MAX_RETRIES 回リトライ
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                job = client.query(sql)
                for row in job.result():
                    rec = dict(row)
                    records.append(rec)
                break
            except Exception as e:
                err = str(e)
                print(f"⚠️ {hyp_id} attempt {attempt} failed: {err}")
                if attempt == MAX_RETRIES:
                    print(f"❌ {hyp_id} は3回リトライしましたが失敗しました。レポートはスキップします。")

        # レコード取得できなければ次へ
        if not records:
            continue

        # DataFrame にまとめて平均を取りやすい形にする
        df = pd.DataFrame(records)
        # 期待指標がすべて含まれていることを確認しておく
        for col in ("step3_users", "step4_users", "transition_rate"):
            if col not in df.columns:
                df[col] = None
        # 仮説ごとに平均値を計算（通常は1行だが念のため）
        summary = df[["step3_users", "step4_users", "transition_rate"]].mean().to_frame().T

        # LLM にレポートを生成させる
        report_text = call_llm_for_report(hyp_id, summary, schema_text)

        # ファイルに書き出し
        out_path = os.path.join(HYPOTHESIS_REPORTS_DIR, f"{hyp_id}_report.txt")
        with open(out_path, "w", encoding="utf-8") as f:
            f.write(report_text)
        print(f"✅ {hyp_id} レポートを出力しました: {out_path}")

if __name__ == "__main__":
    main()
