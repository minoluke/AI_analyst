"""
分析パイプライン用の設定ファイル
ハードコードされた値を全て設定可能にする
"""

import os
from datetime import datetime, timedelta
from dataclasses import dataclass
from typing import List, Dict, Optional

@dataclass
class DateRangeConfig:
    """日付範囲設定"""
    start_date: str
    end_date: str
    short_range_days: int = 7
    
    @property
    def short_end_date(self) -> str:
        """短期分析用の終了日"""
        start = datetime.strptime(self.start_date, '%Y%m%d')
        short_end = start + timedelta(days=self.short_range_days)
        return short_end.strftime('%Y%m%d')

@dataclass
class TableConfig:
    """テーブル設定"""
    project_id: str
    dataset_id: str
    table_pattern: str = "events_*"
    public_dataset_project: Optional[str] = None
    
    @property
    def full_table_reference(self) -> str:
        """完全なテーブル参照を生成"""
        if self.public_dataset_project:
            return f"`{self.public_dataset_project}.{self.dataset_id}.{self.table_pattern}`"
        return f"`{self.project_id}.{self.dataset_id}.{self.table_pattern}`"
    
    @property
    def is_public_dataset(self) -> bool:
        return self.public_dataset_project is not None

@dataclass
class SchemaConfig:
    """スキーマ設定"""
    user_id_field: str = "user_pseudo_id"
    event_name_field: str = "event_name"
    device_category_field: str = "device.category"
    geo_country_field: str = "geo.country"
    traffic_source_field: str = "traffic_source.source"
    traffic_medium_field: str = "traffic_source.medium"
    event_timestamp_field: str = "event_timestamp"
    table_suffix_field: str = "_TABLE_SUFFIX"
    
    @property
    def purchase_event_condition(self) -> str:
        return f"{self.event_name_field} = 'purchase'"
    
    def get_event_filter(self, event_names: List[str]) -> str:
        events = "', '".join(event_names)
        return f"{self.event_name_field} IN ('{events}')"

@dataclass
class DeviceConfig:
    """デバイス設定"""
    categories: List[str]
    default_control: str
    default_treatment: str
    category_field: str = "device.category"
    
    @property
    def control_filter(self) -> str:
        return f"{self.category_field} = '{self.default_control}'"
    
    @property
    def treatment_filter(self) -> str:
        return f"{self.category_field} = '{self.default_treatment}'"

@dataclass
class EventConfig:
    """イベント設定"""
    primary_events: List[str]
    funnel_events: List[str]
    engagement_events: List[str]
    
    @property
    def all_events(self) -> List[str]:
        return list(set(self.primary_events + self.funnel_events + self.engagement_events))

@dataclass
class StatisticalConfig:
    """統計設定"""
    alpha: float = 0.05
    power: float = 0.8
    confidence_level: float = 0.95
    min_effect_size: float = 0.05
    min_sample_size: int = 100
    z_score_95: float = 1.96
    
    @property
    def significance_threshold(self) -> float:
        return self.min_effect_size

@dataclass
class ProcessingConfig:
    """処理設定"""
    sql_retry_limit: int = 5
    analysis_retry_limit: int = 3
    min_required_rows: int = 1
    refinement_rounds: int = 2
    max_concurrent_requests: int = 3
    request_timeout: int = 60

@dataclass
class OutputConfig:
    """出力設定"""
    base_dir: str
    reports_dir: str
    filename_prefix: str
    timestamp_format: str = '%Y%m%d_%H%M%S'
    
    def get_output_path(self, filename: str) -> str:
        """出力パスを動的生成"""
        timestamp = datetime.now().strftime(self.timestamp_format)
        return os.path.join(self.base_dir, f"{self.filename_prefix}_{timestamp}_{filename}")
    
    def get_report_path(self, report_type: str) -> str:
        """レポートパスを動的生成"""
        timestamp = datetime.now().strftime(self.timestamp_format)
        return os.path.join(self.reports_dir, f"{report_type}_{timestamp}.md")

class AnalysisConfig:
    """分析設定の統合クラス"""
    
    def __init__(self, 
                 environment: str = "development",
                 custom_config: Optional[Dict] = None):
        
        self.environment = environment
        
        # デフォルト設定
        self._init_default_configs()
        
        # 環境別設定の適用
        self._apply_environment_configs()
        
        # カスタム設定の適用
        if custom_config:
            self._apply_custom_config(custom_config)
    
    def _init_default_configs(self):
        """デフォルト設定の初期化"""
        
        # 日付設定
        self.date_range = DateRangeConfig(
            start_date="20201101",
            end_date="20210131",
            short_range_days=7
        )
        
        # テーブル設定
        self.table = TableConfig(
            project_id="ai-analyst-test-1",
            dataset_id="ga4_obfuscated_sample_ecommerce",
            table_pattern="events_*",
            public_dataset_project="bigquery-public-data"
        )
        
        # スキーマ設定
        self.schema = SchemaConfig()
        
        # デバイス設定
        self.device = DeviceConfig(
            categories=["mobile", "desktop", "tablet"],
            default_control="desktop",
            default_treatment="mobile"
        )
        
        # イベント設定
        self.events = EventConfig(
            primary_events=["purchase", "add_to_cart", "begin_checkout"],
            funnel_events=["view_item", "add_to_cart", "begin_checkout", "purchase"],
            engagement_events=["page_view", "scroll", "user_engagement", "session_start"]
        )
        
        # 統計設定
        self.statistical = StatisticalConfig()
        
        # 処理設定
        self.processing = ProcessingConfig()
        
        # 出力設定
        project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        self.output = OutputConfig(
            base_dir=os.path.join(project_root, "results"),
            reports_dir=os.path.join(project_root, "results", "reports"),
            filename_prefix="analysis_results"
        )
    
    def _apply_environment_configs(self):
        """環境別設定の適用"""
        
        if self.environment == "production":
            # 本番環境: より厳密な統計設定
            self.statistical.alpha = 0.01
            self.statistical.power = 0.9
            self.statistical.min_sample_size = 1000
            self.processing.sql_retry_limit = 10
            
        elif self.environment == "testing":
            # テスト環境: 高速実行用の設定
            self.date_range.short_range_days = 3
            self.processing.refinement_rounds = 1
            self.statistical.min_sample_size = 10
            
        elif self.environment == "demo":
            # デモ環境: 軽量設定
            self.date_range = DateRangeConfig(
                start_date="20201101",
                end_date="20201107",
                short_range_days=3
            )
            self.processing.refinement_rounds = 1
    
    def _apply_custom_config(self, custom_config: Dict):
        """カスタム設定の適用"""
        
        for section, values in custom_config.items():
            if hasattr(self, section):
                config_obj = getattr(self, section)
                for key, value in values.items():
                    if hasattr(config_obj, key):
                        setattr(config_obj, key, value)
    
    def get_sql_date_filter(self, use_short_range: bool = False) -> str:
        """SQL用の日付フィルタを生成"""
        end_date = self.date_range.short_end_date if use_short_range else self.date_range.end_date
        return f"{self.schema.table_suffix_field} BETWEEN '{self.date_range.start_date}' AND '{end_date}'"
    
    def get_full_table_reference(self) -> str:
        """完全なテーブル参照を取得"""
        return self.table.full_table_reference
    
    def get_base_sql_query(self, select_fields: List[str], where_conditions: Optional[List[str]] = None) -> str:
        """基本的なSQLクエリテンプレートを生成"""
        fields = ", ".join(select_fields)
        base_query = f"SELECT {fields} FROM {self.get_full_table_reference()}"
        
        conditions = [self.get_sql_date_filter()]
        if where_conditions:
            conditions.extend(where_conditions)
        
        if conditions:
            base_query += f" WHERE {' AND '.join(conditions)}"
        
        return base_query
    
    def get_experiment_design(self, 
                            control_device: Optional[str] = None,
                            treatment_device: Optional[str] = None) -> Dict:
        """実験設計を動的生成"""
        
        control = control_device or self.device.default_control
        treatment = treatment_device or self.device.default_treatment
        
        return {
            'control_group': {
                'sql_filter': f"{self.schema.device_category_field} = '{control}'"
            },
            'treatment_group': {
                'sql_filter': f"{self.schema.device_category_field} = '{treatment}'"
            }
        }
    
    def get_event_filter(self, event_types: List[str]) -> str:
        """イベントフィルタを生成"""
        return self.schema.get_event_filter(event_types)
    
    def is_significant(self, effect_size: float) -> bool:
        """効果サイズの有意性判定"""
        return abs(effect_size) > self.statistical.significance_threshold
    
    def to_dict(self) -> Dict:
        """設定を辞書形式で出力"""
        return {
            'environment': self.environment,
            'date_range': {
                'start_date': self.date_range.start_date,
                'end_date': self.date_range.end_date,
                'short_range_days': self.date_range.short_range_days
            },
            'table': {
                'project_id': self.table.project_id,
                'dataset_id': self.table.dataset_id,
                'table_pattern': self.table.table_pattern,
                'public_dataset_project': self.table.public_dataset_project,
                'full_reference': self.table.full_table_reference
            },
            'schema': {
                'user_id_field': self.schema.user_id_field,
                'event_name_field': self.schema.event_name_field,
                'device_category_field': self.schema.device_category_field,
                'geo_country_field': self.schema.geo_country_field
            },
            'device': {
                'categories': self.device.categories,
                'default_control': self.device.default_control,
                'default_treatment': self.device.default_treatment
            },
            'events': {
                'primary_events': self.events.primary_events,
                'funnel_events': self.events.funnel_events,
                'engagement_events': self.events.engagement_events
            },
            'statistical': {
                'alpha': self.statistical.alpha,
                'power': self.statistical.power,
                'min_effect_size': self.statistical.min_effect_size,
                'min_sample_size': self.statistical.min_sample_size
            },
            'processing': {
                'sql_retry_limit': self.processing.sql_retry_limit,
                'refinement_rounds': self.processing.refinement_rounds
            }
        }

# 使用例
def get_analysis_config(environment: str = "development", 
                       custom_overrides: Optional[Dict] = None) -> AnalysisConfig:
    """分析設定を取得"""
    return AnalysisConfig(environment=environment, custom_config=custom_overrides)

# グローバルインスタンス（デフォルト）
default_config = get_analysis_config()

if __name__ == "__main__":
    # 設定のテスト出力
    import json
    
    print("=== デフォルト設定 ===")
    config = get_analysis_config()
    print(json.dumps(config.to_dict(), indent=2, ensure_ascii=False))
    
    print("\n=== 本番環境設定 ===")
    prod_config = get_analysis_config("production")
    print(json.dumps(prod_config.to_dict(), indent=2, ensure_ascii=False))
    
    print("\n=== カスタム設定例 ===")
    custom = {
        'date_range': {'start_date': '20220101', 'end_date': '20221231'},
        'device': {'default_control': 'tablet', 'default_treatment': 'mobile'},
        'statistical': {'alpha': 0.01, 'min_effect_size': 0.1}
    }
    custom_config = get_analysis_config(custom_overrides=custom)
    print(json.dumps(custom_config.to_dict(), indent=2, ensure_ascii=False))