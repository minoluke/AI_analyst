import json
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from src.api import get_openai_response
from src.config import SCHEMA_TXT_FILE, HYPOTHESES_FILE, DATA_EXPLORATION_FILE

# —————————————————————————
# ① タクティクスレベルの課題をここで指定
TACTIC_INPUT = "eコマースサイトの購買ファネル（商品閲覧→カート追加→チェックアウト→購入）における離脱要因と改善施策の仮説立案"

# ② GA4スキーマテキストの読み込み
with open(SCHEMA_TXT_FILE, "r", encoding="utf-8") as f:
    schema_text = f.read()

# データ探索結果の読み込み
with open(DATA_EXPLORATION_FILE, "r", encoding="utf-8") as f:
    data_exploration = f.read()

# ③ プロンプトの定義（JSON出力フォーマット付き）
prompt = f"""
あなたは熟練のマーケティングデータアナリストです。

以下のGoogle Analytics 4（GA4）のイベントログスキーマと実際のデータ内容に基づき、
「{TACTIC_INPUT}」という具体的な施策レベルの課題に対して
検証可能な仮説を5つ、JSON形式で提案してください。

【GA4 スキーマ概要】
{schema_text}

【実際のデータ内容（使用可能なイベント名含む）】
{data_exploration}

重要：必ず上記のデータ探索結果に記載されている実際のイベント名を使用してください。
（view_item, add_to_cart, begin_checkout, add_shipping_info, add_payment_info, purchase など）

【出力フォーマット】
[
  {{
    "id": "H001",
    "summary": "仮説の一文要約",
    "conditions": {{
      "event_name": "...",
      "device": "...",
      "その他の条件": "..."
    }},
    "expected_outcome": "期待する結果"
  }},
  ... 全5件 ...
]
"""

# ④ APIコールして結果を取得
response_text = get_openai_response(prompt)

# ⑤ JSONパース
# JSON部分のみを抽出
import re

# JSONブロックを探す（```json ... ``` または [ ... ]）
json_match = re.search(r'```json\s*(\[.*?\])\s*```', response_text, re.DOTALL)
if json_match:
    json_text = json_match.group(1)
else:
    # JSONブロックがない場合、配列を直接探す
    json_match = re.search(r'(\[.*?\])', response_text, re.DOTALL)
    if json_match:
        json_text = json_match.group(1)
    else:
        # 見つからない場合は元のテキストを使用
        json_text = response_text

try:
    hypotheses = json.loads(json_text)
except json.JSONDecodeError:
    raise ValueError(f"JSONのパースに失敗しました:\n{json_text}\n\n元の応答:\n{response_text}")

# ⑥ ファイルに保存
with open(HYPOTHESES_FILE, "w", encoding="utf-8") as f:
    json.dump(hypotheses, f, ensure_ascii=False, indent=2)

# ⑦ 結果を整形して表示
print(f"生成された仮説を以下のファイルに保存しました: {HYPOTHESES_FILE}")
print(json.dumps(hypotheses, ensure_ascii=False, indent=2))
