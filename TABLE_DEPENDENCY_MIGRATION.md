# テーブル依存ハードコード値の完全設定化

## 🎯 概要
AI Analystパイプラインの全てのテーブル依存ハードコード値を特定し、包括的な設定システムに移行しました。これにより、異なるGA4データセット、カスタムスキーマ、マルチテナント環境での柔軟な実行が可能になりました。

## 📊 移行されたハードコード値

### 1. **テーブル参照関連**
#### Before (ハードコード)
```sql
FROM `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`
FROM `{PROJECT_ID}.{DATASET_ID}.events_*`
```

#### After (設定化)
```python
# デフォルト: BigQuery Public Dataset
config.get_full_table_reference()
# → `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`

# プライベートプロジェクト
config = get_analysis_config(custom_overrides={
    'table': {'public_dataset_project': None}
})
# → `my-project.my_dataset.events_*`
```

### 2. **日付フィルタ関連**
#### Before (ハードコード)
```sql
WHERE _TABLE_SUFFIX BETWEEN '20201101' AND '20210131'
```

#### After (設定化)
```python
config.get_sql_date_filter()
# → _TABLE_SUFFIX BETWEEN '20201101' AND '20210131'

config.get_sql_date_filter(use_short_range=True)
# → _TABLE_SUFFIX BETWEEN '20201101' AND '20201108'
```

### 3. **スキーマフィールド関連**
#### Before (ハードコード)
```sql
user_pseudo_id
event_name = 'purchase'
device.category
geo.country
```

#### After (設定化)
```python
config.schema.user_id_field          # → user_pseudo_id
config.schema.purchase_event_condition # → event_name = 'purchase'
config.schema.device_category_field   # → device.category
config.schema.geo_country_field       # → geo.country
```

### 4. **イベント名関連**
#### Before (ハードコード)
```python
primary_events = ["purchase", "add_to_cart", "begin_checkout"]
funnel_events = ["view_item", "add_to_cart", "begin_checkout", "purchase"]
```

#### After (設定化)
```python
config.events.primary_events
config.events.funnel_events
config.events.engagement_events
config.events.all_events  # 重複除去された全イベント
```

## 🏗️ 新設定クラス

### 1. `TableConfig`
```python
@dataclass
class TableConfig:
    project_id: str
    dataset_id: str
    table_pattern: str = "events_*"
    public_dataset_project: Optional[str] = None
    
    @property
    def full_table_reference(self) -> str
    @property  
    def is_public_dataset(self) -> bool
```

### 2. `SchemaConfig`
```python
@dataclass
class SchemaConfig:
    user_id_field: str = "user_pseudo_id"
    event_name_field: str = "event_name"
    device_category_field: str = "device.category"
    geo_country_field: str = "geo.country"
    # ... その他のフィールド
    
    def get_event_filter(self, event_names: List[str]) -> str
```

## 🚀 新機能

### 1. **動的SQL生成**
```python
# 基本SQLテンプレート
config.get_base_sql_query(
    select_fields=["user_pseudo_id", "event_name"],
    where_conditions=["event_name = 'purchase'"]
)
```

### 2. **マルチデータソース対応**
```python
# GA4 Public Sample
config_ga4 = get_analysis_config()

# Firebase Analytics  
config_firebase = get_analysis_config(custom_overrides={
    'table': {
        'project_id': 'my-firebase-project',
        'dataset_id': 'analytics_123456789'
    }
})

# カスタムスキーマ
config_custom = get_analysis_config(custom_overrides={
    'schema': {
        'user_id_field': 'customer_id',
        'event_name_field': 'action_type'
    }
})
```

### 3. **マルチテナント対応**
```python
# テナント別設定
for tenant in ['tenant_a', 'tenant_b', 'tenant_c']:
    config = get_analysis_config(custom_overrides={
        'table': {
            'project_id': f'{tenant}-analytics',
            'dataset_id': f'{tenant}_ga4_data'
        }
    })
```

## 📝 更新されたファイル

### 1. **src/config_analysis.py** (新機能追加)
- `TableConfig`: テーブル参照設定
- `SchemaConfig`: スキーマフィールド設定
- `get_full_table_reference()`: 完全テーブル参照生成
- `get_base_sql_query()`: SQLテンプレート生成

### 2. **src/analysis/ultimate_hypothesis_system.py**
- 全SQL内の`user_pseudo_id` → `config.schema.user_id_field`
- 全SQL内の`event_name = 'purchase'` → `config.schema.purchase_event_condition`
- 全SQL内の`device.category` → `config.schema.device_category_field`
- 全SQL内のテーブル参照 → `config.get_full_table_reference()`

### 3. **src/analysis/systematic_experiment_runner.py**
- 同様のスキーマフィールド設定化
- テーブル参照の設定化

### 4. **src/analysis/hypothesis_validation_pipeline.py**
- 日付フィルタの設定化
- デバイスカテゴリの設定化

## 🎛️ 設定例

### 1. **BigQuery Public Dataset (デフォルト)**
```python
config = get_analysis_config()
# テーブル: bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*
# 期間: 20201101-20210131
```

### 2. **プライベートプロジェクト**
```python
config = get_analysis_config(custom_overrides={
    'table': {
        'project_id': 'my-private-project',
        'dataset_id': 'my_ga4_dataset',
        'public_dataset_project': None
    }
})
```

### 3. **カスタムスキーマ**
```python
config = get_analysis_config(custom_overrides={
    'schema': {
        'user_id_field': 'customer_id',
        'event_name_field': 'action_name',
        'device_category_field': 'device_info.type'
    },
    'table': {
        'table_pattern': 'user_actions_*'
    }
})
```

### 4. **異なる分析期間**
```python
config = get_analysis_config(custom_overrides={
    'date_range': {
        'start_date': '20230101',
        'end_date': '20231231'
    }
})
```

### 5. **カスタムイベント**
```python
config = get_analysis_config(custom_overrides={
    'events': {
        'primary_events': ['order_complete', 'payment_success'],
        'funnel_events': ['product_view', 'add_cart', 'checkout_start', 'order_complete']
    }
})
```

## ✅ 利点

### 1. **柔軟性**
- 異なるGA4プロパティで同じ分析を実行
- カスタムイベント名・スキーマに対応
- 複数の分析期間での並行実行

### 2. **再利用性**
- 設定を変更するだけで異なるデータソースに対応
- コード変更なしでの環境切り替え

### 3. **保守性**
- テーブル構造変更時は設定のみ更新
- 一箇所での設定管理

### 4. **テスト性**
- 異なる設定での単体テスト
- モックデータでのテスト実行

### 5. **スケーラビリティ**
- マルチテナント環境での展開
- 大規模な分析パイプラインでの活用

## 🧪 テスト結果

`test_table_configuration.py`で以下を検証済み:
- ✅ デフォルト設定でのテーブル参照
- ✅ プライベートプロジェクト設定
- ✅ カスタムスキーマ設定
- ✅ カスタムイベント設定
- ✅ マルチテナント設定
- ✅ SQLテンプレート生成
- ✅ Before/After比較

## 🎉 結論

全てのテーブル依存ハードコード値が設定システムに移行され、AI Analystパイプラインが完全に設定可能になりました。これにより、様々なGA4環境、カスタムスキーマ、異なるビジネス要件に対応できる柔軟で再利用可能なシステムが実現されています。