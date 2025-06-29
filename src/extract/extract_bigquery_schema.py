from google.cloud import bigquery
import pandas as pd
import sys
import os
from datetime import datetime
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from src.config import PROJECT_ID, DATASET_ID, SCHEMA_CSV_FILE, SCHEMA_TXT_FILE, DATA_EXPLORATION_FILE

# データセットを分解
dataset_parts = DATASET_ID.split(".")
source_project_id = dataset_parts[0] if len(dataset_parts) > 1 else PROJECT_ID
dataset_name = dataset_parts[1] if len(dataset_parts) > 1 else dataset_parts[0]

# 自分のプロジェクトでクライアントを初期化
client = bigquery.Client(project=PROJECT_ID)

# データセット参照
dataset_ref = bigquery.DatasetReference(source_project_id, dataset_name)

def explore_data_content():
    """実際のデータ内容を探索してLLM用の情報を収集"""
    
    # 基本的なデータ探索クエリ
    base_table = f"`{source_project_id}.{dataset_name}.events_*`"
    
    explorations = {}
    
    print("🔍 GA4データ探索を開始...")
    
    # 1. 利用可能なイベント名とその頻度
    try:
        query = f"""
        SELECT 
            event_name,
            COUNT(*) as event_count,
            COUNT(DISTINCT user_pseudo_id) as unique_users
        FROM {base_table}
        WHERE _TABLE_SUFFIX BETWEEN '20201101' AND '20210131'
        GROUP BY event_name
        ORDER BY event_count DESC
        LIMIT 20
        """
        result = client.query(query).to_dataframe()
        explorations['available_events'] = result.to_dict('records')
        print(f"✅ イベント種類: {len(result)}個発見")
    except Exception as e:
        print(f"⚠️ イベント探索エラー: {e}")
        explorations['available_events'] = []
    
    # 2. デバイスカテゴリ
    try:
        query = f"""
        SELECT 
            device.category as device_category,
            COUNT(*) as event_count,
            COUNT(DISTINCT user_pseudo_id) as unique_users
        FROM {base_table}
        WHERE _TABLE_SUFFIX BETWEEN '20201101' AND '20210131'
        AND device.category IS NOT NULL
        GROUP BY device.category
        ORDER BY event_count DESC
        """
        result = client.query(query).to_dataframe()
        explorations['device_categories'] = result.to_dict('records')
        print(f"✅ デバイスカテゴリ: {len(result)}個発見")
    except Exception as e:
        print(f"⚠️ デバイス探索エラー: {e}")
        explorations['device_categories'] = []
    
    # 3. 流入元情報
    try:
        query = f"""
        SELECT 
            traffic_source.source,
            traffic_source.medium,
            COUNT(*) as event_count
        FROM {base_table}
        WHERE _TABLE_SUFFIX BETWEEN '20201101' AND '20210131'
        AND traffic_source.source IS NOT NULL
        GROUP BY traffic_source.source, traffic_source.medium
        ORDER BY event_count DESC
        LIMIT 15
        """
        result = client.query(query).to_dataframe()
        explorations['traffic_sources'] = result.to_dict('records')
        print(f"✅ 流入元: {len(result)}個発見")
    except Exception as e:
        print(f"⚠️ 流入元探索エラー: {e}")
        explorations['traffic_sources'] = []
    
    # 4. よく使われるevent_params
    try:
        query = f"""
        SELECT 
            param.key as param_key,
            COUNT(*) as usage_count
        FROM {base_table},
        UNNEST(event_params) as param
        WHERE _TABLE_SUFFIX BETWEEN '20201101' AND '20210131'
        GROUP BY param.key
        ORDER BY usage_count DESC
        LIMIT 20
        """
        result = client.query(query).to_dataframe()
        explorations['common_event_params'] = result.to_dict('records')
        print(f"✅ イベントパラメータ: {len(result)}個発見")
    except Exception as e:
        print(f"⚠️ パラメータ探索エラー: {e}")
        explorations['common_event_params'] = []
    
    # 5. データ期間と規模
    try:
        query = f"""
        SELECT 
            MIN(event_date) as min_date,
            MAX(event_date) as max_date,
            COUNT(*) as total_events,
            COUNT(DISTINCT user_pseudo_id) as total_users
        FROM {base_table}
        WHERE _TABLE_SUFFIX BETWEEN '20201101' AND '20210131'
        """
        result = client.query(query).to_dataframe()
        explorations['data_summary'] = result.to_dict('records')[0]
        print(f"✅ データ規模情報を取得")
    except Exception as e:
        print(f"⚠️ データ規模探索エラー: {e}")
        explorations['data_summary'] = {}
    
    # 6. 地理情報
    try:
        query = f"""
        SELECT 
            geo.country,
            COUNT(*) as event_count
        FROM {base_table}
        WHERE _TABLE_SUFFIX BETWEEN '20201101' AND '20210131'
        AND geo.country IS NOT NULL
        GROUP BY geo.country
        ORDER BY event_count DESC
        LIMIT 10
        """
        result = client.query(query).to_dataframe()
        explorations['top_countries'] = result.to_dict('records')
        print(f"✅ 地理情報: {len(result)}個発見")
    except Exception as e:
        print(f"⚠️ 地理情報探索エラー: {e}")
        explorations['top_countries'] = []
    
    return explorations

def generate_schema_file(schema_df):
    """純粋なテーブル定義書としてのスキーマファイルを生成"""
    
    # 日次分割テーブルを統合（すべて同じ構造）
    # events_YYYYMMDD パターンのテーブルを1つにまとめる
    events_tables = [t for t in schema_df['Table'].unique() if t.startswith('events_')]
    other_tables = [t for t in schema_df['Table'].unique() if not t.startswith('events_')]
    
    schema_content = f"""# GA4 Table Schema Definition
Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
Dataset: {DATASET_ID}

## テーブル構造定義

このファイルは GA4 events テーブルの構造定義（スキーマ）のみを記載しています。
実際のデータ内容については data_exploration.txt を参照してください。

"""
    
    # eventsテーブル（日次分割）を統合して表示
    if events_tables:
        # 最初のeventsテーブルの構造を代表として使用
        first_events_table = events_tables[0]
        table_fields = schema_df[schema_df['Table'] == first_events_table]
        
        schema_content += f"""
## events_* (日次分割テーブル)

**注意**: このテーブルは日付ごとに分割されています（events_20201101, events_20201102, ... events_20210131）
すべて同じ構造を持つため、以下に代表的な構造を示します。

**テーブル数**: {len(events_tables)}個のテーブル
**期間**: {min(events_tables).replace('events_', '')} ～ {max(events_tables).replace('events_', '')}

| フィールド名 | データ型 | モード | 説明 |
|-------------|---------|-------|------|"""
        
        for _, field in table_fields.iterrows():
            description = get_field_description(field['Field'])
            schema_content += f"""
| {field['Field']} | {field['Type']} | {field['Mode']} | {description} |"""
        
        schema_content += f"""

### ネストされたフィールドの詳細構造

以下のRECORDタイプフィールドは複雑な構造を持ちます：

"""
        
        # RECORDタイプのフィールドの詳細を別途記載
        record_fields = table_fields[table_fields['Type'] == 'RECORD']
        for _, record_field in record_fields.iterrows():
            nested_fields = get_nested_fields(schema_df, first_events_table, record_field['Field'])
            if nested_fields:
                schema_content += f"""
#### {record_field['Field']}
| サブフィールド | データ型 | モード |
|--------------|---------|-------|"""
                for nested in nested_fields:
                    schema_content += f"""
| {nested['Field']} | {nested['Type']} | {nested['Mode']} |"""
                schema_content += f"""

"""
    
    # その他のテーブル（もしあれば）
    for table_name in other_tables:
        table_fields = schema_df[schema_df['Table'] == table_name]
        schema_content += f"""
## {table_name}

| フィールド名 | データ型 | モード | 説明 |
|-------------|---------|-------|------|"""
        
        for _, field in table_fields.iterrows():
            description = get_field_description(field['Field'])
            schema_content += f"""
| {field['Field']} | {field['Type']} | {field['Mode']} | {description} |"""
        
        schema_content += f"""

"""
    
    schema_content += f"""
## クエリ時の重要な注意事項

1. **日次分割テーブルの参照方法**:
   - 単一日: `{DATASET_ID}.events_20201101`
   - 複数日（推奨）: `{DATASET_ID}.events_*`
   - 期間指定: `WHERE _TABLE_SUFFIX BETWEEN '20201101' AND '20210131'`

2. **データ型について**:
   - RECORDタイプ: ネストされた構造（上記詳細参照）
   - REPEATEDモード: 配列として格納
   - NULLABLEモード: NULL値を許可

3. **パフォーマンス最適化**:
   - 必ず日付範囲を指定してください（_TABLE_SUFFIX条件）
   - 必要なフィールドのみを SELECT してください

## 参考リンク

- 実際のデータ内容とSQL例: data_exploration.txt
- BigQuery公式ドキュメント: https://cloud.google.com/bigquery/docs/
- GA4 BigQuery Export スキーマ: https://support.google.com/analytics/answer/7029846
"""
    
    return schema_content

def get_nested_fields(schema_df, table_name, parent_field):
    """ネストされたフィールド構造を取得"""
    # BigQueryのスキーマでは、ネストされたフィールドは parent.child の形式で表現される
    nested_pattern = f"{parent_field}."
    nested_fields = schema_df[
        (schema_df['Table'] == table_name) & 
        (schema_df['Field'].str.startswith(nested_pattern))
    ]
    
    # 直接の子フィールドのみを取得（孫フィールドは除外）
    direct_children = []
    for _, field in nested_fields.iterrows():
        field_name = field['Field']
        # parent_fieldを除去した部分
        child_part = field_name[len(nested_pattern):]
        # さらにネストしていない（.が含まれていない）ものだけ
        if '.' not in child_part:
            direct_children.append({
                'Field': child_part,
                'Type': field['Type'],
                'Mode': field['Mode']
            })
    
    return direct_children

def generate_data_exploration_file(explorations):
    """データ探索結果を別ファイルに生成"""
    
    exploration_content = f"""# GA4 Data Exploration Report
Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
Dataset: {DATASET_ID}

このファイルには実際のデータ内容とSQL作成に必要な情報が含まれています。

## 📊 データ概要
"""
    
    # データサマリー
    if explorations.get('data_summary'):
        summary = explorations['data_summary']
        exploration_content += f"""
- **期間**: {summary.get('min_date', 'N/A')} ～ {summary.get('max_date', 'N/A')}
- **総イベント数**: {summary.get('total_events', 'N/A'):,}
- **総ユーザー数**: {summary.get('total_users', 'N/A'):,}
"""
    
    # 利用可能なイベント
    exploration_content += f"""
## 🎯 利用可能なイベント名 (上位20個)

実際にデータに存在するイベント名とその頻度:
"""
    for event in explorations.get('available_events', []):
        exploration_content += f"""
- **{event['event_name']}**: {event['event_count']:,}回 ({event['unique_users']:,}ユーザー)"""
    
    # デバイスカテゴリ
    exploration_content += f"""

## 📱 利用可能なデバイスカテゴリ

device.category の実際の値:
"""
    for device in explorations.get('device_categories', []):
        exploration_content += f"""
- **{device['device_category']}**: {device['event_count']:,}回 ({device['unique_users']:,}ユーザー)"""
    
    # 流入元
    exploration_content += f"""

## 🌐 主要な流入元

traffic_source の実際の値:
"""
    for source in explorations.get('traffic_sources', []):
        exploration_content += f"""
- source: **{source['source']}**, medium: **{source['medium']}** ({source['event_count']:,}回)"""
    
    # イベントパラメータ
    exploration_content += f"""

## 🔧 よく使用されるevent_params

実際のパラメータキー:
"""
    for param in explorations.get('common_event_params', []):
        exploration_content += f"""
- **{param['param_key']}**: {param['usage_count']:,}回使用"""
    
    # 地理情報
    exploration_content += f"""

## 🌍 主要な国・地域

geo.country の実際の値:
"""
    for country in explorations.get('top_countries', []):
        exploration_content += f"""
- **{country['country']}**: {country['event_count']:,}回"""
    
    exploration_content += f"""

## 💡 SQL作成時の重要な指針

1. **イベント名**: 上記の「利用可能なイベント名」から選択してください
2. **デバイスカテゴリ**: {', '.join([d['device_category'] for d in explorations.get('device_categories', [])])}
3. **日付範囲**: {explorations.get('data_summary', {}).get('min_date', '20201101')} ～ {explorations.get('data_summary', {}).get('max_date', '20210131')}
4. **テーブル名**: `{source_project_id}.{dataset_name}.events_*`
5. **必須WHERE句**: `WHERE _TABLE_SUFFIX BETWEEN '20201101' AND '20210131'`

## SQL例

```sql
-- 基本的なクエリ例
SELECT 
    event_name,
    COUNT(*) as event_count,
    COUNT(DISTINCT user_pseudo_id) as unique_users
FROM `{source_project_id}.{dataset_name}.events_*`
WHERE _TABLE_SUFFIX BETWEEN '20201101' AND '20210131'
GROUP BY event_name
ORDER BY event_count DESC
```
"""
    
    return exploration_content

def get_field_description(field_name):
    """フィールド名に基づく説明を返す"""
    descriptions = {
        'event_date': 'イベント発生日（YYYYMMDD形式）',
        'event_timestamp': 'イベント発生タイムスタンプ（マイクロ秒）',
        'event_name': 'イベント名（page_view, purchase等）',
        'event_value_in_usd': 'イベント価値（米ドル）',
        'user_id': 'ログイン済みユーザーID',
        'user_pseudo_id': '匿名ユーザー識別子',
        'user_first_touch_timestamp': '初回接触タイムスタンプ',
        'event_params': 'イベントパラメータ（key-value構造）',
        'user_properties': 'ユーザー属性情報',
        'device': 'デバイス情報（カテゴリ、OS等）',
        'geo': '地理情報（国、地域等）',
        'traffic_source': '流入元情報（source, medium等）',
        'app_info': 'アプリ情報',
        'user_ltv': 'ライフタイムバリュー',
        'privacy_info': 'プライバシー同意情報'
    }
    return descriptions.get(field_name, 'GA4標準フィールド')

# メイン処理
def main():
    # テーブル一覧の取得
    tables = list(client.list_tables(dataset_ref))
    
    # 各テーブルのスキーマ情報を取得
    rows = []
    for table_item in tables:
        table_id = table_item.table_id
        table_ref = dataset_ref.table(table_id)
        table = client.get_table(table_ref)
        for field in table.schema:
            rows.append({
                "Table": table_id,
                "Field": field.name,
                "Type": field.field_type,
                "Mode": field.mode
            })
    
    # DataFrame に整形
    schema_df = pd.DataFrame(rows)
    print(f"📋 基本スキーマ情報: {len(schema_df)}フィールド")
    
    # CSVに保存
    schema_df.to_csv(SCHEMA_CSV_FILE, index=False)
    
    # データ内容を探索
    explorations = explore_data_content()
    
    # 純粋なスキーマファイルを生成
    schema_content = generate_schema_file(schema_df)
    with open(SCHEMA_TXT_FILE, 'w', encoding='utf-8') as f:
        f.write(schema_content)
    
    # データ探索結果を別ファイルに生成
    exploration_content = generate_data_exploration_file(explorations)
    with open(DATA_EXPLORATION_FILE, 'w', encoding='utf-8') as f:
        f.write(exploration_content)
    
    print(f"\n✅ テーブル定義ファイルを生成: {SCHEMA_TXT_FILE}")
    print(f"✅ データ探索ファイルを生成: {DATA_EXPLORATION_FILE}")
    print(f"✅ CSVスキーマファイルを生成: {SCHEMA_CSV_FILE}")

if __name__ == "__main__":
    main()
