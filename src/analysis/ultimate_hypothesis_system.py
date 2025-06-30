"""
究極の仮説生成・改良・検証システム
- 段階的改良プロセス（AI-Scientist-v2スタイル）
- 系統的実験実行（D2D_Data2Dashboardスタイル）
"""

import json
import asyncio
from typing import Dict, List, Optional
from dataclasses import dataclass, asdict
import sys
import os
from datetime import datetime

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from src.api import get_openai_response
from src.config import PROJECT_ID, DATASET_ID, SCHEMA_TXT_FILE, DATA_EXPLORATION_FILE
from src.config_analysis import get_analysis_config
from google.cloud import bigquery

@dataclass
class ExperimentResult:
    """実験結果の構造"""
    experiment_id: str
    hypothesis_id: str
    control_group_size: int
    treatment_group_size: int
    control_metric: float
    treatment_metric: float
    effect_size: float
    statistical_significance: bool
    confidence_interval: tuple

@dataclass
class HypothesisEvolution:
    """仮説の進化プロセス"""
    original: Dict
    evaluations: List[Dict]
    refinements: List[Dict]
    final_version: Dict
    validation_score: float

class UltimateHypothesisSystem:
    """統合された仮説生成・改良・検証システム"""
    
    def __init__(self):
        os.environ["GOOGLE_CLOUD_PROJECT"] = PROJECT_ID
        self.client = bigquery.Client(project=PROJECT_ID)
        self.config = get_analysis_config()
        self.refinement_rounds = self.config.processing.refinement_rounds
        self.session_start = datetime.now()
        
    # ==========================================
    # Phase 1: 段階的改良プロセス
    # ==========================================
    
    def generate_initial_hypotheses(self, schema_text: str, data_exploration: str) -> List[Dict]:
        """🎯 Phase 1.1: 初期仮説生成"""
        print("🎯 Phase 1.1: 初期仮説生成中...")
        
        prompt = f"""
GA4 eコマースデータから、ビジネス改善に直結する3つの初期仮説を生成してください。

【データ】
■ GA4スキーマ:
{schema_text[:2000]}...

■ 実際のデータ内容:
{data_exploration[:2000]}...

【要件】
1. 検証可能性: BigQueryで測定できる
2. 比較基準の明確性: 何と何を比較するか明記
3. ビジネス価値: 意思決定に直結する洞察

【出力形式】
```json
[
  {{
    "id": "H001",
    "hypothesis": "具体的で検証可能な仮説（比較対象明記）",
    "rationale": "仮説を立てた根拠・背景",
    "business_question": "解決したいビジネス課題",
    "potential_issues": ["想定される分析上の問題点"],
    "success_criteria": "仮説が成立する条件"
  }}
]
```
"""
        
        response = get_openai_response(prompt)
        hypotheses = self._extract_json_list(response)
        print(f"✅ 初期仮説 {len(hypotheses)}件 生成完了")
        return hypotheses
    
    def critical_evaluation(self, hypotheses: List[Dict]) -> List[Dict]:
        """🔍 Phase 1.2: 批判的評価"""
        print("🔍 Phase 1.2: 批判的評価中...")
        
        evaluation_prompt = f"""
以下の仮説を厳格に評価し、改善点を指摘してください。

【仮説】
{json.dumps(hypotheses, ensure_ascii=False, indent=2)}

【評価基準】
1. 検証可能性（BigQueryで実際に測定できるか）
2. 比較基準の明確性（AとBの比較が明確か）
3. ビジネス価値（結果が具体的なアクションに繋がるか）
4. 統計的妥当性（サンプル数・バイアス・検出力）
5. 実行可能性（現実的な時間・リソース内で実行可能か）
6. データ制約（利用可能データでの制限）

【出力形式】
```json
[
  {{
    "hypothesis_id": "H001",
    "overall_score": 7.5,
    "strengths": ["具体的な強み"],
    "critical_weaknesses": ["重大な弱点"],
    "improvement_suggestions": [
      {{
        "category": "検証可能性",
        "suggestion": "具体的改善案",
        "priority": "high/medium/low"
      }}
    ],
    "feasibility_concerns": ["実行上の懸念点"],
    "recommendation": "accept/revise/reject"
  }}
]
```
"""
        
        response = get_openai_response(evaluation_prompt)
        evaluations = self._extract_json_list(response)
        print(f"✅ 評価完了: {len(evaluations)}件")
        return evaluations
    
    def refine_hypotheses(self, original_hypotheses: List[Dict], evaluations: List[Dict]) -> List[Dict]:
        """⚡ Phase 1.3: 改良版生成"""
        print("⚡ Phase 1.3: 仮説改良中...")
        
        refinement_prompt = f"""
評価結果を基に仮説を根本的に改良してください。

【元の仮説】
{json.dumps(original_hypotheses, ensure_ascii=False, indent=2)}

【評価結果】
{json.dumps(evaluations, ensure_ascii=False, indent=2)}

【改良要求】
- 重大な弱点を根本的に修正
- 改善提案を全て反映
- より具体的で測定可能に
- 比較基準を数値で明確化
- ビジネス価値を具体化

【出力形式】
```json
[
  {{
    "id": "H001_refined",
    "original_hypothesis": "元の仮説",
    "refined_hypothesis": "根本的に改良された仮説",
    "improvements_made": [
      {{
        "area": "改良領域",
        "before": "改良前の状態",
        "after": "改良後の状態"
      }}
    ],
    "comparison_baseline": "明確な比較基準（数値含む）",
    "verification_method": "具体的検証手法",
    "expected_business_impact": "期待されるビジネス効果"
  }}
]
```
"""
        
        response = get_openai_response(refinement_prompt)
        refined = self._extract_json_list(response)
        print(f"✅ 改良完了: {len(refined)}件")
        return refined
    
    def final_validation(self, refined_hypotheses: List[Dict]) -> List[Dict]:
        """✅ Phase 1.4: 最終検証"""
        print("✅ Phase 1.4: 最終検証中...")
        
        validation_prompt = f"""
改良された仮説の最終品質検証を行ってください。

【改良仮説】
{json.dumps(refined_hypotheses, ensure_ascii=False, indent=2)}

【検証項目】
1. 完全性: 全ての要素が適切に定義されているか
2. 明確性: 曖昧さや解釈の余地がないか
3. 実行可能性: 現実的に検証可能か
4. ビジネス価値: 意思決定に明確に貢献するか
5. 統計的厳密性: 適切な検証方法が定義されているか

【出力形式】
```json
[
  {{
    "id": "H001_final",
    "hypothesis": "最終確定仮説",
    "validation_status": "pass",
    "confidence_score": 8.5,
    "business_impact": "具体的なビジネス価値",
    "verification_plan": {{
      "primary_metric": "主要測定指標",
      "comparison_groups": "比較対象",
      "success_threshold": "成功基準",
      "statistical_method": "統計手法"
    }},
    "quality_assurance": {{
      "completeness": 9.0,
      "clarity": 8.5,
      "feasibility": 8.0,
      "business_value": 9.0
    }}
  }}
]
```
"""
        
        response = get_openai_response(validation_prompt)
        final_hypotheses = self._extract_json_list(response)
        print(f"✅ 最終検証完了: {len(final_hypotheses)}件")
        return final_hypotheses
    
    # ==========================================
    # Phase 2: 系統的実験実行
    # ==========================================
    
    def design_experimental_groups(self, hypothesis: Dict) -> Dict:
        """🧪 Phase 2.1: 実験群・対照群設計"""
        print(f"🧪 Phase 2.1: {hypothesis['id']} 実験設計中...")
        
        design_prompt = f"""
以下の仮説に対して、厳密なA/Bテスト風の実験設計を行ってください。

【仮説】
{hypothesis['hypothesis']}

【検証計画】
{json.dumps(hypothesis.get('verification_plan', {}), ensure_ascii=False, indent=2)}

【設計要求】
1. 対照群・実験群の具体的定義（SQL WHERE条件含む）
2. 主要指標と副次指標の計算式
3. サンプルサイズの推定
4. 統計的検出力の設定
5. 潜在的交絡因子の特定

【出力形式】
```json
{{
  "control_group": {{
    "definition": "対照群の明確な定義",
    "sql_filter": "WHERE device.category = 'desktop'",
    "expected_size": 50000,
    "characteristics": "グループの特徴"
  }},
  "treatment_group": {{
    "definition": "実験群の明確な定義",
    "sql_filter": "WHERE device.category = 'mobile'", 
    "expected_size": 45000,
    "characteristics": "グループの特徴"
  }},
  "primary_metric": {{
    "name": "購入転換率",
    "calculation": "購入イベント数 / ユニークユーザー数",
    "sql_expression": "COUNTIF(event_name = 'purchase') / COUNT(DISTINCT user_pseudo_id)"
  }},
  "secondary_metrics": [
    {{
      "name": "平均購入額",
      "sql_expression": "AVG(purchase_revenue_in_usd)"
    }}
  ],
  "statistical_approach": {{
    "test_type": "two-sample t-test",
    "alpha": 0.05,
    "power": 0.8,
    "minimum_detectable_effect": 0.1
  }},
  "confounding_factors": ["時間帯", "地域", "プロモーション"],
  "data_quality_checks": ["欠損値の処理", "外れ値の検出"]
}}
```
"""
        
        response = get_openai_response(design_prompt)
        design = self._extract_json_object(response)
        print(f"✅ 実験設計完了: {design.get('primary_metric', {}).get('name', 'Unknown')}")
        return design
    
    def execute_exp01_basic_analysis(self, hypothesis: Dict, design: Dict) -> ExperimentResult:
        """🔬 exp01: 基本記述統計分析"""
        print(f"🔬 exp01_{hypothesis['id']}_basic_analysis 実行中...")
        
        basic_sql = f"""
        WITH control_group AS (
          SELECT 
            user_pseudo_id,
            event_name,
            CASE WHEN event_name = 'purchase' THEN 1 ELSE 0 END as purchase_flag
          FROM `{PROJECT_ID}.{DATASET_ID}.events_*`
          WHERE {design['control_group']['sql_filter']}
            AND {self.config.get_sql_date_filter()}
        ),
        treatment_group AS (
          SELECT 
            user_pseudo_id,
            event_name,
            CASE WHEN event_name = 'purchase' THEN 1 ELSE 0 END as purchase_flag
          FROM `{PROJECT_ID}.{DATASET_ID}.events_*`
          WHERE {design['treatment_group']['sql_filter']}
            AND {self.config.get_sql_date_filter()}
        ),
        control_stats AS (
          SELECT 
            'control' as group_type,
            COUNT(DISTINCT user_pseudo_id) as total_users,
            COUNT(DISTINCT CASE WHEN purchase_flag = 1 THEN user_pseudo_id END) as purchasers,
            COUNT(DISTINCT CASE WHEN purchase_flag = 1 THEN user_pseudo_id END) / COUNT(DISTINCT user_pseudo_id) as conversion_rate
          FROM control_group
        ),
        treatment_stats AS (
          SELECT 
            'treatment' as group_type,
            COUNT(DISTINCT user_pseudo_id) as total_users,
            COUNT(DISTINCT CASE WHEN purchase_flag = 1 THEN user_pseudo_id END) as purchasers,
            COUNT(DISTINCT CASE WHEN purchase_flag = 1 THEN user_pseudo_id END) / COUNT(DISTINCT user_pseudo_id) as conversion_rate
          FROM treatment_group
        )
        SELECT * FROM control_stats
        UNION ALL
        SELECT * FROM treatment_stats
        """
        
        try:
            results = self.client.query(basic_sql).to_dataframe()
            
            if len(results) >= 2:
                control_data = results[results['group_type'] == 'control'].iloc[0]
                treatment_data = results[results['group_type'] == 'treatment'].iloc[0]
                
                control_rate = float(control_data['conversion_rate'])
                treatment_rate = float(treatment_data['conversion_rate'])
                effect_size = (treatment_rate - control_rate) / control_rate if control_rate > 0 else 0
                
                result = ExperimentResult(
                    experiment_id="exp01_basic",
                    hypothesis_id=hypothesis['id'],
                    control_group_size=int(control_data['total_users']),
                    treatment_group_size=int(treatment_data['total_users']),
                    control_metric=control_rate,
                    treatment_metric=treatment_rate,
                    effect_size=effect_size,
                    statistical_significance=False,
                    confidence_interval=(0, 0)
                )
                
                print(f"✅ exp01 完了: エフェクトサイズ {effect_size:.4f}")
                return result
            else:
                print("❌ exp01: データ不足")
                return None
                
        except Exception as e:
            print(f"❌ exp01 実行エラー: {e}")
            return None
    
    def execute_exp02_comparative_analysis(self, hypothesis: Dict, design: Dict) -> ExperimentResult:
        """📊 exp02: 統計的比較分析"""
        print(f"📊 exp02_{hypothesis['id']}_comparative_analysis 実行中...")
        
        comparative_sql = f"""
        WITH control_group AS (
          SELECT 
            user_pseudo_id,
            CASE WHEN event_name = 'purchase' THEN 1 ELSE 0 END as purchase_flag
          FROM `{PROJECT_ID}.{DATASET_ID}.events_*`
          WHERE {design['control_group']['sql_filter']}
            AND {self.config.get_sql_date_filter()}
        ),
        treatment_group AS (
          SELECT 
            user_pseudo_id,
            CASE WHEN event_name = 'purchase' THEN 1 ELSE 0 END as purchase_flag
          FROM `{PROJECT_ID}.{DATASET_ID}.events_*`
          WHERE {design['treatment_group']['sql_filter']}
            AND {self.config.get_sql_date_filter()}
        ),
        control_agg AS (
          SELECT 
            'control' as group_type,
            COUNT(DISTINCT user_pseudo_id) as sample_size,
            AVG(purchase_flag) as conversion_rate,
            STDDEV(purchase_flag) as std_dev
          FROM control_group
        ),
        treatment_agg AS (
          SELECT 
            'treatment' as group_type,
            COUNT(DISTINCT user_pseudo_id) as sample_size,
            AVG(purchase_flag) as conversion_rate,
            STDDEV(purchase_flag) as std_dev
          FROM treatment_group
        )
        SELECT * FROM control_agg
        UNION ALL  
        SELECT * FROM treatment_agg
        """
        
        try:
            results = self.client.query(comparative_sql).to_dataframe()
            
            if len(results) >= 2:
                control_data = results[results['group_type'] == 'control'].iloc[0]
                treatment_data = results[results['group_type'] == 'treatment'].iloc[0]
                
                control_rate = float(control_data['conversion_rate'])
                treatment_rate = float(treatment_data['conversion_rate'])
                
                # エフェクトサイズ計算
                effect_size = (treatment_rate - control_rate) / control_rate if control_rate > 0 else 0
                
                # 統計的有意性判定（設定値を使用）
                significance = self.config.is_significant(effect_size)
                
                # 信頼区間（簡易計算）
                n_treatment = float(treatment_data['sample_size'])
                std_treatment = float(treatment_data['std_dev']) if treatment_data['std_dev'] else 0
                
                margin_of_error = 1.96 * (std_treatment / (n_treatment ** 0.5)) if n_treatment > 0 else 0
                ci_lower = treatment_rate - margin_of_error
                ci_upper = treatment_rate + margin_of_error
                
                result = ExperimentResult(
                    experiment_id="exp02_comparative",
                    hypothesis_id=hypothesis['id'],
                    control_group_size=int(control_data['sample_size']),
                    treatment_group_size=int(treatment_data['sample_size']),
                    control_metric=control_rate,
                    treatment_metric=treatment_rate,
                    effect_size=effect_size,
                    statistical_significance=significance,
                    confidence_interval=(ci_lower, ci_upper)
                )
                
                print(f"✅ exp02 完了: 有意性 {'○' if significance else '×'}")
                return result
            else:
                print("❌ exp02: データ不足")
                return None
                
        except Exception as e:
            print(f"❌ exp02 実行エラー: {e}")
            return None
    
    def execute_exp03_advanced_segmentation(self, hypothesis: Dict, design: Dict) -> ExperimentResult:
        """🎯 exp03: 高度セグメント分析"""
        print(f"🎯 exp03_{hypothesis['id']}_advanced_segmentation 実行中...")
        
        segmentation_sql = f"""
        WITH all_data AS (
          SELECT 
            {self.config.schema.user_id_field},
            {self.config.schema.device_category_field} as device_type,
            {self.config.schema.geo_country_field} as country,
            CASE WHEN {design['control_group']['sql_filter'].replace('WHERE ', '')} THEN 'control'
                 WHEN {design['treatment_group']['sql_filter'].replace('WHERE ', '')} THEN 'treatment'
                 ELSE 'other' END as group_type,
            CASE WHEN {self.config.schema.purchase_event_condition} THEN 1 ELSE 0 END as purchase_flag
          FROM {self.config.get_full_table_reference()}
          WHERE {self.config.get_sql_date_filter()}
            AND ({design['control_group']['sql_filter'].replace('WHERE ', '')} 
                OR {design['treatment_group']['sql_filter'].replace('WHERE ', '')})
        ),
        segmented_results AS (
          SELECT 
            group_type,
            device_type,
            COUNT(DISTINCT {self.config.schema.user_id_field}) as sample_size,
            AVG(purchase_flag) as conversion_rate
          FROM all_data
          WHERE group_type IN ('control', 'treatment')
            AND device_type IS NOT NULL
          GROUP BY group_type, device_type
          HAVING COUNT(DISTINCT {self.config.schema.user_id_field}) > {self.config.statistical.min_sample_size}
        )
        SELECT * FROM segmented_results
        ORDER BY device_type, group_type
        """
        
        try:
            results = self.client.query(segmentation_sql).to_dataframe()
            
            if len(results) > 0:
                # デバイス別効果サイズ計算
                device_effects = []
                total_control = 0
                total_treatment = 0
                
                for device in results['device_type'].unique():
                    device_data = results[results['device_type'] == device]
                    
                    control_subset = device_data[device_data['group_type'] == 'control']
                    treatment_subset = device_data[device_data['group_type'] == 'treatment']
                    
                    if len(control_subset) > 0 and len(treatment_subset) > 0:
                        control_rate = float(control_subset.iloc[0]['conversion_rate'])
                        treatment_rate = float(treatment_subset.iloc[0]['conversion_rate'])
                        
                        if control_rate > 0:
                            device_effect = (treatment_rate - control_rate) / control_rate
                            device_effects.append(device_effect)
                
                # 全体集計
                control_total = results[results['group_type'] == 'control']['sample_size'].sum()
                treatment_total = results[results['group_type'] == 'treatment']['sample_size'].sum()
                
                overall_effect = sum(device_effects) / len(device_effects) if device_effects else 0
                significance = self.config.is_significant(overall_effect)
                
                result = ExperimentResult(
                    experiment_id="exp03_segmentation",
                    hypothesis_id=hypothesis['id'],
                    control_group_size=int(control_total),
                    treatment_group_size=int(treatment_total),
                    control_metric=0.0,  # セグメント分析では個別値は意味なし
                    treatment_metric=0.0,
                    effect_size=overall_effect,
                    statistical_significance=significance,
                    confidence_interval=(0, 0)
                )
                
                print(f"✅ exp03 完了: セグメント効果 {overall_effect:.4f}")
                return result
            else:
                print("❌ exp03: データ不足")
                return None
                
        except Exception as e:
            print(f"❌ exp03 実行エラー: {e}")
            return None
    
    # ==========================================
    # Phase 3: 統合実行
    # ==========================================
    
    def run_complete_pipeline(self) -> Dict:
        """🚀 完全パイプライン実行"""
        print("🚀 究極の仮説生成・改良・検証システム開始")
        print(f"開始時刻: {self.session_start}")
        print("=" * 60)
        
        # データ読み込み
        with open(SCHEMA_TXT_FILE, 'r', encoding='utf-8') as f:
            schema_text = f.read()
        with open(DATA_EXPLORATION_FILE, 'r', encoding='utf-8') as f:
            data_exploration = f.read()
        
        pipeline_results = {
            'session_info': {
                'start_time': self.session_start.isoformat(),
                'system_version': 'v1.0_ultimate'
            },
            'phase1_iterative_refinement': {},
            'phase2_systematic_experiments': {},
            'overall_summary': {}
        }
        
        # Phase 1: 段階的改良プロセス
        print("\n🎯 PHASE 1: 段階的仮説改良プロセス")
        print("=" * 60)
        
        # 1.1 初期生成
        initial_hypotheses = self.generate_initial_hypotheses(schema_text, data_exploration)
        
        # 1.2-1.4 反復改良
        current_hypotheses = initial_hypotheses
        evolution_history = []
        
        for round_num in range(self.refinement_rounds):
            print(f"\n🔄 改良ラウンド {round_num + 1}/{self.refinement_rounds}")
            
            # 批判的評価
            evaluations = self.critical_evaluation(current_hypotheses)
            
            # 改良
            refined = self.refine_hypotheses(current_hypotheses, evaluations)
            
            evolution_history.append({
                'round': round_num + 1,
                'evaluations': evaluations,
                'refinements': refined
            })
            
            current_hypotheses = refined
        
        # 最終検証
        final_hypotheses = self.final_validation(current_hypotheses)
        
        pipeline_results['phase1_iterative_refinement'] = {
            'initial_hypotheses': initial_hypotheses,
            'evolution_history': evolution_history,
            'final_hypotheses': final_hypotheses
        }
        
        # Phase 2: 系統的実験実行
        print("\n🧪 PHASE 2: 系統的実験実行")
        print("=" * 60)
        
        experiment_results = {}
        
        for hypothesis in final_hypotheses:
            if hypothesis.get('validation_status') == 'pass':
                print(f"\n🎯 {hypothesis['id']} の系統的実験開始")
                
                # 実験設計
                design = self.design_experimental_groups(hypothesis)
                
                if design:
                    # 3段階実験実行
                    exp_results = {}
                    
                    # exp01
                    exp01 = self.execute_exp01_basic_analysis(hypothesis, design)
                    if exp01:
                        exp_results['exp01'] = asdict(exp01)
                    
                    # exp02  
                    exp02 = self.execute_exp02_comparative_analysis(hypothesis, design)
                    if exp02:
                        exp_results['exp02'] = asdict(exp02)
                    
                    # exp03
                    exp03 = self.execute_exp03_advanced_segmentation(hypothesis, design)
                    if exp03:
                        exp_results['exp03'] = asdict(exp03)
                    
                    experiment_results[hypothesis['id']] = {
                        'hypothesis': hypothesis,
                        'experimental_design': design,
                        'experiment_results': exp_results,
                        'summary': self._generate_experiment_summary(exp_results)
                    }
                else:
                    print(f"❌ {hypothesis['id']}: 実験設計失敗")
            else:
                print(f"⏭️ {hypothesis['id']}: 検証パス不合格のためスキップ")
        
        pipeline_results['phase2_systematic_experiments'] = experiment_results
        
        # 全体サマリー生成
        pipeline_results['overall_summary'] = self._generate_overall_summary(
            pipeline_results['phase1_iterative_refinement'],
            pipeline_results['phase2_systematic_experiments']
        )
        
        # 結果保存
        output_file = self.config.output.get_output_path('ultimate_hypothesis_results.json')
        
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(pipeline_results, f, ensure_ascii=False, indent=2, default=str)
        
        print("\n🎉 究極システム完了!")
        print("=" * 60)
        print(f"📊 総実行時間: {datetime.now() - self.session_start}")
        print(f"📁 結果保存: {output_file}")
        print(f"📈 最終仮説数: {len(final_hypotheses)}")
        print(f"🧪 実験実行数: {len(experiment_results)}")
        
        return pipeline_results
    
    def _generate_experiment_summary(self, exp_results: Dict) -> str:
        """実験結果サマリー"""
        if not exp_results:
            return "実験実行失敗"
        
        summaries = []
        for exp_name, result in exp_results.items():
            effect = result.get('effect_size', 0)
            significance = result.get('statistical_significance', False)
            summaries.append(f"{exp_name}: 効果{effect:.3f} {'(有意)' if significance else '(非有意)'}")
        
        return " | ".join(summaries)
    
    def _generate_overall_summary(self, phase1_results: Dict, phase2_results: Dict) -> Dict:
        """全体サマリー生成"""
        return {
            'initial_hypotheses_count': len(phase1_results.get('initial_hypotheses', [])),
            'final_hypotheses_count': len(phase1_results.get('final_hypotheses', [])),
            'experiments_executed': len(phase2_results),
            'significant_findings': sum(
                1 for exp_data in phase2_results.values()
                for exp_result in exp_data.get('experiment_results', {}).values()
                if exp_result.get('statistical_significance', False)
            ),
            'avg_effect_sizes': [
                exp_result.get('effect_size', 0)
                for exp_data in phase2_results.values()
                for exp_result in exp_data.get('experiment_results', {}).values()
                if 'effect_size' in exp_result
            ]
        }
    
    def _extract_json_list(self, response: str) -> List[Dict]:
        """JSONリスト抽出"""
        import re
        json_match = re.search(r'```json\s*(\[.*?\])\s*```', response, re.DOTALL)
        if json_match:
            return json.loads(json_match.group(1))
        json_match = re.search(r'(\[.*?\])', response, re.DOTALL)
        return json.loads(json_match.group(1)) if json_match else []
    
    def _extract_json_object(self, response: str) -> Dict:
        """JSONオブジェクト抽出"""
        import re
        json_match = re.search(r'```json\s*(\{.*?\})\s*```', response, re.DOTALL)
        if json_match:
            return json.loads(json_match.group(1))
        json_match = re.search(r'(\{.*?\})', response, re.DOTALL)
        return json.loads(json_match.group(1)) if json_match else {}

# 実行
if __name__ == "__main__":
    system = UltimateHypothesisSystem()
    results = system.run_complete_pipeline()