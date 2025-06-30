"""
D2D_Data2Dashboard スタイルの系統的実験実行
exp01, exp02, exp03... の実際の実装
"""

import json
import asyncio
from typing import Dict, List, Optional
from dataclasses import dataclass
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from src.api import get_openai_response
from src.config import PROJECT_ID, DATASET_ID
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

class SystematicExperimentRunner:
    """D2Dスタイルの系統的実験実行器"""
    
    def __init__(self):
        os.environ["GOOGLE_CLOUD_PROJECT"] = PROJECT_ID
        self.client = bigquery.Client(project=PROJECT_ID)
        self.config = get_analysis_config()
    
    def design_experimental_groups(self, hypothesis: Dict) -> Dict:
        """Step 1: 実験群・対照群の具体的設計"""
        
        design_prompt = f"""
以下の仮説に対して、A/Bテスト風の実験設計を行ってください。

【仮説】
{hypothesis['hypothesis']}

【要求】
1. 対照群（Control Group）の具体的定義
2. 実験群（Treatment Group）の具体的定義  
3. 主要成功指標（Primary Metric）
4. 副次指標（Secondary Metrics）
5. 最小検出効果サイズ
6. 必要サンプルサイズの推定

【出力形式】
```json
{{
  "control_group": {{
    "definition": "対照群の具体的条件",
    "sql_filter": "WHERE条件",
    "expected_size": "推定サンプル数"
  }},
  "treatment_group": {{
    "definition": "実験群の具体的条件", 
    "sql_filter": "WHERE条件",
    "expected_size": "推定サンプル数"
  }},
  "primary_metric": {{
    "name": "主要指標名",
    "calculation": "指標の計算方法",
    "sql_expression": "SQL式"
  }},
  "secondary_metrics": [
    {{
      "name": "副次指標名",
      "calculation": "計算方法"
    }}
  ],
  "statistical_approach": {{
    "test_type": "使用する統計検定",
    "alpha": 0.05,
    "power": 0.8,
    "minimum_effect_size": "最小検出効果"
  }}
}}
```
"""
        
        response = get_openai_response(design_prompt)
        return self._extract_json_object(response)
    
    def execute_exp01_basic_analysis(self, hypothesis: Dict, design: Dict) -> ExperimentResult:
        """exp01: 基本的な記述統計分析"""
        
        print(f"🔬 exp01_{hypothesis['id']}_basic_analysis 実行中...")
        
        # 基本統計SQL生成
        basic_sql = f"""
        WITH control_group AS (
          SELECT 
            {self.config.schema.user_id_field},
            {design['primary_metric']['sql_expression']} as metric_value
          FROM {self.config.get_full_table_reference()}
          WHERE {design['control_group']['sql_filter']}
            AND {self.config.get_sql_date_filter()}
        ),
        treatment_group AS (
          SELECT 
            {self.config.schema.user_id_field},
            {design['primary_metric']['sql_expression']} as metric_value  
          FROM {self.config.get_full_table_reference()}
          WHERE {design['treatment_group']['sql_filter']}
            AND {self.config.get_sql_date_filter()}
        )
        SELECT 
          'control' as group_type,
          COUNT(*) as sample_size,
          AVG(metric_value) as mean_metric,
          STDDEV(metric_value) as std_metric
        FROM control_group
        UNION ALL
        SELECT 
          'treatment' as group_type,
          COUNT(*) as sample_size, 
          AVG(metric_value) as mean_metric,
          STDDEV(metric_value) as std_metric
        FROM treatment_group
        """
        
        try:
            results = self.client.query(basic_sql).to_dataframe()
            
            control_data = results[results['group_type'] == 'control'].iloc[0]
            treatment_data = results[results['group_type'] == 'treatment'].iloc[0]
            
            effect_size = (treatment_data['mean_metric'] - control_data['mean_metric']) / control_data['mean_metric']
            
            return ExperimentResult(
                experiment_id="exp01_basic",
                hypothesis_id=hypothesis['id'],
                control_group_size=int(control_data['sample_size']),
                treatment_group_size=int(treatment_data['sample_size']),
                control_metric=float(control_data['mean_metric']),
                treatment_metric=float(treatment_data['mean_metric']),
                effect_size=float(effect_size),
                statistical_significance=False,  # exp01では統計検定なし
                confidence_interval=(0, 0)
            )
            
        except Exception as e:
            print(f"❌ exp01 実行エラー: {e}")
            return None
    
    def execute_exp02_comparative_analysis(self, hypothesis: Dict, design: Dict) -> ExperimentResult:
        """exp02: 統計的比較分析（t検定等）"""
        
        print(f"🔬 exp02_{hypothesis['id']}_comparative_analysis 実行中...")
        
        # 統計検定用のSQL（より詳細な分析）
        comparative_sql = f"""
        WITH control_group AS (
          SELECT 
            {self.config.schema.user_id_field},
            {design['primary_metric']['sql_expression']} as metric_value,
            'control' as group_type
          FROM {self.config.get_full_table_reference()}
          WHERE {design['control_group']['sql_filter']}
            AND {self.config.get_sql_date_filter()}
        ),
        treatment_group AS (
          SELECT 
            {self.config.schema.user_id_field},
            {design['primary_metric']['sql_expression']} as metric_value,
            'treatment' as group_type
          FROM {self.config.get_full_table_reference()}
          WHERE {design['treatment_group']['sql_filter']}
            AND {self.config.get_sql_date_filter()}
        ),
        combined_data AS (
          SELECT * FROM control_group
          UNION ALL  
          SELECT * FROM treatment_group
        )
        SELECT 
          group_type,
          COUNT(*) as sample_size,
          AVG(metric_value) as mean_metric,
          STDDEV(metric_value) as std_metric,
          -- 信頼区間計算用
          AVG(metric_value) - 1.96 * STDDEV(metric_value)/SQRT(COUNT(*)) as ci_lower,
          AVG(metric_value) + 1.96 * STDDEV(metric_value)/SQRT(COUNT(*)) as ci_upper
        FROM combined_data
        GROUP BY group_type
        """
        
        try:
            results = self.client.query(comparative_sql).to_dataframe()
            
            control_data = results[results['group_type'] == 'control'].iloc[0]
            treatment_data = results[results['group_type'] == 'treatment'].iloc[0]
            
            # エフェクトサイズ計算
            effect_size = (treatment_data['mean_metric'] - control_data['mean_metric']) / control_data['mean_metric']
            
            # 統計的有意性判定（設定値を使用）
            significance = self.config.is_significant(effect_size)
            
            return ExperimentResult(
                experiment_id="exp02_comparative",
                hypothesis_id=hypothesis['id'],
                control_group_size=int(control_data['sample_size']),
                treatment_group_size=int(treatment_data['sample_size']),
                control_metric=float(control_data['mean_metric']),
                treatment_metric=float(treatment_data['mean_metric']),
                effect_size=float(effect_size),
                statistical_significance=significance,
                confidence_interval=(float(treatment_data['ci_lower']), float(treatment_data['ci_upper']))
            )
            
        except Exception as e:
            print(f"❌ exp02 実行エラー: {e}")
            return None
    
    def execute_exp03_advanced_segmentation(self, hypothesis: Dict, design: Dict) -> ExperimentResult:
        """exp03: 高度なセグメント分析"""
        
        print(f"🔬 exp03_{hypothesis['id']}_advanced_segmentation 実行中...")
        
        # セグメント別分析SQL
        segmentation_sql = f"""
        WITH control_group AS (
          SELECT 
            {self.config.schema.user_id_field},
            {self.config.schema.device_category_field} as device_type,
            {self.config.schema.geo_country_field} as country,
            {design['primary_metric']['sql_expression']} as metric_value,
            'control' as group_type
          FROM {self.config.get_full_table_reference()}
          WHERE {design['control_group']['sql_filter']}
            AND {self.config.get_sql_date_filter()}
        ),
        treatment_group AS (
          SELECT 
            {self.config.schema.user_id_field},
            {self.config.schema.device_category_field} as device_type,
            {self.config.schema.geo_country_field} as country,
            {design['primary_metric']['sql_expression']} as metric_value,
            'treatment' as group_type
          FROM {self.config.get_full_table_reference()}
          WHERE {design['treatment_group']['sql_filter']}
            AND {self.config.get_sql_date_filter()}
        ),
        combined_data AS (
          SELECT * FROM control_group
          UNION ALL
          SELECT * FROM treatment_group  
        )
        SELECT 
          group_type,
          device_type,
          COUNT(*) as sample_size,
          AVG(metric_value) as mean_metric,
          STDDEV(metric_value) as std_metric
        FROM combined_data
        WHERE device_type IS NOT NULL
        GROUP BY group_type, device_type
        ORDER BY device_type, group_type
        """
        
        try:
            results = self.client.query(segmentation_sql).to_dataframe()
            
            # デバイス別の効果を集計
            device_effects = []
            for device in results['device_type'].unique():
                device_data = results[results['device_type'] == device]
                if len(device_data) == 2:  # control & treatment 両方存在
                    control = device_data[device_data['group_type'] == 'control'].iloc[0]
                    treatment = device_data[device_data['group_type'] == 'treatment'].iloc[0]
                    
                    device_effect = (treatment['mean_metric'] - control['mean_metric']) / control['mean_metric']
                    device_effects.append(device_effect)
            
            # 全体の平均エフェクト
            overall_effect = sum(device_effects) / len(device_effects) if device_effects else 0
            
            # 全体サンプルサイズ
            total_control = results[results['group_type'] == 'control']['sample_size'].sum()
            total_treatment = results[results['group_type'] == 'treatment']['sample_size'].sum()
            
            return ExperimentResult(
                experiment_id="exp03_segmentation",
                hypothesis_id=hypothesis['id'],
                control_group_size=int(total_control),
                treatment_group_size=int(total_treatment),
                control_metric=0.0,  # セグメント分析なので個別値は無意味
                treatment_metric=0.0,
                effect_size=float(overall_effect),
                statistical_significance=self.config.is_significant(overall_effect)
                confidence_interval=(0, 0)
            )
            
        except Exception as e:
            print(f"❌ exp03 実行エラー: {e}")
            return None
    
    def run_systematic_experiments(self, hypothesis: Dict) -> Dict:
        """仮説に対する系統的実験の完全実行"""
        
        print(f"\n🚀 {hypothesis['id']} の系統的実験開始")
        
        # Step 1: 実験設計
        print("📋 Step 1: 実験群・対照群設計")
        design = self.design_experimental_groups(hypothesis)
        
        if not design:
            print("❌ 実験設計に失敗")
            return None
        
        print(f"✅ 設計完了: {design.get('primary_metric', {}).get('name', 'Unknown')}")
        
        # Step 2: 3段階の実験実行
        results = {}
        
        # exp01: 基本分析
        exp01_result = self.execute_exp01_basic_analysis(hypothesis, design)
        if exp01_result:
            results['exp01'] = exp01_result
            print(f"✅ exp01 完了: エフェクトサイズ {exp01_result.effect_size:.3f}")
        
        # exp02: 比較分析  
        exp02_result = self.execute_exp02_comparative_analysis(hypothesis, design)
        if exp02_result:
            results['exp02'] = exp02_result
            print(f"✅ exp02 完了: 有意性 {exp02_result.statistical_significance}")
        
        # exp03: セグメント分析
        exp03_result = self.execute_exp03_advanced_segmentation(hypothesis, design)
        if exp03_result:
            results['exp03'] = exp03_result  
            print(f"✅ exp03 完了: セグメント効果 {exp03_result.effect_size:.3f}")
        
        return {
            'hypothesis_id': hypothesis['id'],
            'experimental_design': design,
            'experiment_results': results,
            'summary': self._generate_experiment_summary(results)
        }
    
    def _generate_experiment_summary(self, results: Dict) -> str:
        """実験結果サマリー生成"""
        
        if not results:
            return "実験実行に失敗しました。"
        
        summary_parts = []
        
        for exp_name, result in results.items():
            summary_parts.append(
                f"{exp_name}: エフェクトサイズ {result.effect_size:.3f}, "
                f"有意性 {'○' if result.statistical_significance else '×'}"
            )
        
        return " | ".join(summary_parts)
    
    def _extract_json_object(self, response: str) -> Dict:
        """JSON オブジェクト抽出"""
        import re
        
        json_match = re.search(r'```json\s*(\{.*?\})\s*```', response, re.DOTALL)
        if json_match:
            return json.loads(json_match.group(1))
        
        json_match = re.search(r'(\{.*?\})', response, re.DOTALL)
        if json_match:
            return json.loads(json_match.group(1))
        
        return {}

# 使用例
if __name__ == "__main__":
    runner = SystematicExperimentRunner()
    
    # 既存の仮説を読み込み
    with open('/Users/idenominoru/Desktop/AI_analyst/data/hypotheses/hypotheses_enhanced.json', 'r', encoding='utf-8') as f:
        hypotheses = json.load(f)
    
    # 最初の仮説で実験実行
    if hypotheses:
        result = runner.run_systematic_experiments(hypotheses[0])
        
        # 結果保存
        with open('/Users/idenominoru/Desktop/AI_analyst/results/systematic_experiment_results.json', 'w', encoding='utf-8') as f:
            json.dump(result, f, ensure_ascii=False, indent=2, default=str)
        
        print(f"\n🎉 系統的実験完了: {result['summary']}")