"""
AI-Scientist-v2とD2D_Data2Dashboardから着想を得た
改良された仮説生成システム
"""

import json
import sys
import os
from datetime import datetime
from typing import Dict, List, Optional
import asyncio

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from src.api import get_openai_response
from src.config import SCHEMA_TXT_FILE, HYPOTHESES_FILE, DATA_EXPLORATION_FILE

class EnhancedHypothesisGenerator:
    """AI-Scientist-v2スタイルの構造化された仮説生成"""
    
    def __init__(self):
        self.hypothesis_archive = []
        self.load_existing_hypotheses()
    
    def load_existing_hypotheses(self):
        """過去の仮説をアーカイブとして読み込み（新規性チェック用）"""
        try:
            with open(HYPOTHESES_FILE, 'r', encoding='utf-8') as f:
                self.hypothesis_archive = json.load(f)
        except FileNotFoundError:
            self.hypothesis_archive = []
    
    def generate_structured_hypothesis_prompt(self, schema_text: str, data_exploration: str) -> str:
        """AI-Scientist-v2スタイルの構造化プロンプト"""
        
        
        return f"""
あなたは世界最高峰のデータサイエンティスト兼研究者です。

【目標】
GA4 eコマースデータから、ビジネス改善に直結する
実用的な仮説を提案してください。

【制約条件】
1. 検証可能性: BigQueryで実証できる具体的仮説
2. ビジネス価値: 実用的な示唆を含む
3. 統計的厳密性: 比較群・対照群を明確に設定


【利用可能データ】
■ GA4スキーマ:
{schema_text}

■ 実際のデータ内容:
{data_exploration}

【出力フォーマット】
以下のJSON形式で10個の仮説を提案してください：

```json
[
  {{
    "id": "H001",
    "hypothesis": "検証可能な短い仮説文（比較対象明記）",
    "related_work": "関連する業界ベンチマーク・学術研究の想定",
    "abstract": "仮説の背景・重要性・期待される発見",
    "experiments": [
      {{
        "step": 1,
        "description": "具体的な検証ステップ",
        "sql_approach": "必要なデータ抽出方法",
        "expected_result": "予想される結果パターン"
      }}
    ],
    "risk_factors": [
      "データ制限事項",
      "統計的検出力の課題",
      "解釈上の注意点"
    ],
    "business_impact": "実用的価値の説明"
  }}
]
```

【重要】
- 必ず比較基準を明確にした仮説を作成
- 「高い・低い」ではなく「AはBより○○%高い」形式
- 統計的有意性を検証可能な設計
- 実際のGA4イベント名のみ使用
"""
    
    
    def generate_d2d_style_experiments(self, hypotheses: List[Dict]) -> List[Dict]:
        """D2D_Data2Dashboardスタイルの実験設計"""
        
        enhanced_hypotheses = []
        for hyp in hypotheses:
            # 実験バリエーション追加
            experiment_variants = [
                f"exp01_{hyp['id']}_basic_analysis",
                f"exp02_{hyp['id']}_comparative_analysis", 
                f"exp03_{hyp['id']}_advanced_segmentation"
            ]
            
            hyp['experiment_variants'] = experiment_variants
            hyp['systematic_validation'] = {
                "control_group": "対照群の定義",
                "treatment_group": "実験群の定義", 
                "success_metrics": ["主要指標", "副次指標"],
                "statistical_tests": ["使用する統計手法"]
            }
            
            enhanced_hypotheses.append(hyp)
        
        return enhanced_hypotheses
    
    def generate_enhanced_hypotheses(self) -> List[Dict]:
        """メイン生成プロセス"""
        
        # データ読み込み
        with open(SCHEMA_TXT_FILE, 'r', encoding='utf-8') as f:
            schema_text = f.read()
        
        with open(DATA_EXPLORATION_FILE, 'r', encoding='utf-8') as f:
            data_exploration = f.read()
        
        # 1. 初期仮説生成
        prompt = self.generate_structured_hypothesis_prompt(schema_text, data_exploration)
        response = get_openai_response(prompt)
        
        # JSON抽出
        import re
        json_match = re.search(r'```json\s*(\[.*?\])\s*```', response, re.DOTALL)
        if json_match:
            hypotheses = json.loads(json_match.group(1))
        else:
            json_match = re.search(r'(\[.*?\])', response, re.DOTALL)
            hypotheses = json.loads(json_match.group(1)) if json_match else []
        
        # 2. (新規性チェックは削除)
        
        # 3. D2Dスタイル実験設計強化
        hypotheses = self.generate_d2d_style_experiments(hypotheses)
        
        # 4. アーカイブ更新
        self.hypothesis_archive.extend(hypotheses)
        
        return hypotheses
    
    def save_hypotheses(self, hypotheses: List[Dict]):
        """改良された仮説をファイル保存"""
        
        # 元の形式でも保存（既存パイプライン対応）
        simple_format = []
        for hyp in hypotheses:
            simple_format.append({
                "id": hyp["id"],
                "summary": hyp["hypothesis"],
                "conditions": {
                    "event_name": "実際のGA4イベント",
                    "comparison_baseline": "比較基準",
                    "statistical_approach": "統計手法"
                },
                "expected_outcome": hyp["business_impact"]
            })
        
        # 既存形式で保存
        with open(HYPOTHESES_FILE, 'w', encoding='utf-8') as f:
            json.dump(simple_format, f, ensure_ascii=False, indent=2)
        
        # 詳細版も保存
        enhanced_file = HYPOTHESES_FILE.replace('.json', '_enhanced.json')
        with open(enhanced_file, 'w', encoding='utf-8') as f:
            json.dump(hypotheses, f, ensure_ascii=False, indent=2)
        
        print(f"Enhanced hypotheses saved: {enhanced_file}")
        print(f"Compatible format saved: {HYPOTHESES_FILE}")

if __name__ == "__main__":
    generator = EnhancedHypothesisGenerator()
    hypotheses = generator.generate_enhanced_hypotheses()
    generator.save_hypotheses(hypotheses)
    
    print(f"Generated {len(hypotheses)} enhanced hypotheses")
    for hyp in hypotheses:
        print(f"- {hyp['id']}: {hyp['hypothesis']}")