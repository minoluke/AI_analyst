"""
段階的仮説改良プロセス（AI-Scientist-v2スタイル）
"""

import json
from typing import Dict, List
from src.api import get_openai_response

class IterativeHypothesisRefiner:
    """段階的な仮説改良プロセス"""
    
    def __init__(self):
        self.refinement_rounds = 2  # 改良回数
    
    def generate_initial_hypotheses(self, schema_text: str, data_exploration: str) -> List[Dict]:
        """ステップ1: 初期仮説生成"""
        prompt = f"""
GA4データから3つの初期仮説を生成してください。

【データ】
{schema_text}
{data_exploration}

【出力形式】
```json
[
  {{
    "id": "H001",
    "hypothesis": "検証可能な仮説",
    "rationale": "根拠",
    "potential_issues": ["想定される問題点"]
  }}
]
```
"""
        
        response = get_openai_response(prompt)
        return self._extract_json(response)
    
    def critical_evaluation(self, hypotheses: List[Dict]) -> List[Dict]:
        """ステップ2: 批判的評価"""
        evaluation_prompt = f"""
以下の仮説を批判的に評価し、改善点を指摘してください。

【仮説】
{json.dumps(hypotheses, ensure_ascii=False, indent=2)}

【評価観点】
1. 検証可能性（BigQueryで実際に測定できるか）
2. 比較基準の明確性（何と比較するか）
3. ビジネス価値（結果が意思決定に役立つか）
4. 統計的妥当性（サンプル数・バイアス等）
5. 実行可能性（リソース・時間制約）

【出力形式】
```json
[
  {{
    "hypothesis_id": "H001",
    "strengths": ["強み"],
    "weaknesses": ["弱点"],
    "improvement_suggestions": ["具体的改善案"],
    "feasibility_score": 1-10
  }}
]
```
"""
        
        response = get_openai_response(evaluation_prompt)
        return self._extract_json(response)
    
    def refine_hypotheses(self, original_hypotheses: List[Dict], evaluations: List[Dict]) -> List[Dict]:
        """ステップ3: 改良版生成"""
        refinement_prompt = f"""
評価結果を基に仮説を改良してください。

【元の仮説】
{json.dumps(original_hypotheses, ensure_ascii=False, indent=2)}

【評価結果】
{json.dumps(evaluations, ensure_ascii=False, indent=2)}

【改良要求】
- 弱点を修正
- 改善提案を反映
- より具体的で検証可能に
- 比較基準を明確化

【出力形式】
```json
[
  {{
    "id": "H001_refined",
    "original_hypothesis": "元の仮説",
    "refined_hypothesis": "改良された仮説",
    "improvements_made": ["行った改良"],
    "comparison_baseline": "明確な比較基準",
    "verification_method": "検証方法"
  }}
]
```
"""
        
        response = get_openai_response(refinement_prompt)
        return self._extract_json(response)
    
    def final_validation(self, refined_hypotheses: List[Dict]) -> List[Dict]:
        """ステップ4: 最終検証"""
        validation_prompt = f"""
改良された仮説の最終検証を行ってください。

【改良仮説】
{json.dumps(refined_hypotheses, ensure_ascii=False, indent=2)}

【検証項目】
1. 検証可能性: BigQueryで実行可能か
2. 明確性: 曖昧さがないか
3. 価値: ビジネス改善に繋がるか
4. 実現性: 現実的に実行できるか

【出力】
各仮説について合格/不合格を判定し、最終版を出力。

```json
[
  {{
    "id": "H001_final",
    "hypothesis": "最終仮説",
    "validation_status": "pass/fail",
    "confidence_score": 1-10,
    "business_impact": "期待される価値"
  }}
]
```
"""
        
        response = get_openai_response(validation_prompt)
        return self._extract_json(response)
    
    def iterative_refinement_process(self, schema_text: str, data_exploration: str) -> List[Dict]:
        """完全な段階的改良プロセス"""
        
        print("🎯 ステップ1: 初期仮説生成")
        hypotheses = self.generate_initial_hypotheses(schema_text, data_exploration)
        print(f"生成された初期仮説: {len(hypotheses)}件")
        
        # 複数回の改良ラウンド
        for round_num in range(self.refinement_rounds):
            print(f"🔄 ラウンド{round_num + 1}: 批判的評価と改良")
            
            # 批判的評価
            evaluations = self.critical_evaluation(hypotheses)
            print(f"評価完了: {len(evaluations)}件")
            
            # 改良
            refined = self.refine_hypotheses(hypotheses, evaluations)
            print(f"改良完了: {len(refined)}件")
            
            # 次ラウンドのために更新
            hypotheses = refined
        
        print("✅ ステップ4: 最終検証")
        final_hypotheses = self.final_validation(hypotheses)
        print(f"最終仮説: {len(final_hypotheses)}件")
        
        return final_hypotheses
    
    def _extract_json(self, response: str) -> List[Dict]:
        """JSONレスポンス抽出"""
        import re
        
        json_match = re.search(r'```json\s*(\[.*?\])\s*```', response, re.DOTALL)
        if json_match:
            return json.loads(json_match.group(1))
        
        json_match = re.search(r'(\[.*?\])', response, re.DOTALL)
        if json_match:
            return json.loads(json_match.group(1))
        
        return []

# 使用例
if __name__ == "__main__":
    refiner = IterativeHypothesisRefiner()
    
    # データ読み込み（既存ファイルから）
    from src.config import SCHEMA_TXT_FILE, DATA_EXPLORATION_FILE
    
    with open(SCHEMA_TXT_FILE, 'r', encoding='utf-8') as f:
        schema_text = f.read()
    
    with open(DATA_EXPLORATION_FILE, 'r', encoding='utf-8') as f:
        data_exploration = f.read()
    
    # 段階的改良プロセス実行
    final_hypotheses = refiner.iterative_refinement_process(schema_text, data_exploration)
    
    # 結果保存
    with open('/Users/idenominoru/Desktop/AI_analyst/data/hypotheses/hypotheses_iterative.json', 'w', encoding='utf-8') as f:
        json.dump(final_hypotheses, f, ensure_ascii=False, indent=2)
    
    print("段階的改良プロセス完了！")