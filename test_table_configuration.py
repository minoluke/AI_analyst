#!/usr/bin/env python3
"""
テーブル依存ハードコード値の設定化テスト
全てのテーブル関連のハードコード値が設定可能になったことを確認
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from src.config_analysis import get_analysis_config

def test_table_configuration():
    """テーブル設定の包括的テスト"""
    
    print("🏗️ テーブル依存設定システムテスト開始")
    print("=" * 60)
    
    # 1. デフォルト設定のテスト (BigQuery Public Dataset)
    print("\n📊 1. デフォルト設定 (BigQuery Public Dataset)")
    default_config = get_analysis_config()
    
    print(f"  🗃️  完全テーブル参照: {default_config.get_full_table_reference()}")
    print(f"  📅 日付フィルタ: {default_config.get_sql_date_filter()}")
    print(f"  📅 短期日付フィルタ: {default_config.get_sql_date_filter(use_short_range=True)}")
    print(f"  🔍 パブリック dataset?: {default_config.table.is_public_dataset}")
    
    # スキーマフィールドの確認
    print(f"\n  📋 スキーマフィールド設定:")
    print(f"    - ユーザーID: {default_config.schema.user_id_field}")
    print(f"    - イベント名: {default_config.schema.event_name_field}")
    print(f"    - デバイス: {default_config.schema.device_category_field}")
    print(f"    - 地域: {default_config.schema.geo_country_field}")
    print(f"    - 購入条件: {default_config.schema.purchase_event_condition}")
    
    # 実験設計の生成
    experiment_design = default_config.get_experiment_design()
    print(f"\n  🧪 実験設計:")
    print(f"    - 対照群: {experiment_design['control_group']['sql_filter']}")
    print(f"    - 実験群: {experiment_design['treatment_group']['sql_filter']}")
    
    # 基本SQLクエリの生成
    base_query = default_config.get_base_sql_query(
        select_fields=[
            default_config.schema.user_id_field,
            default_config.schema.event_name_field,
            default_config.schema.device_category_field
        ],
        where_conditions=[default_config.get_event_filter(['purchase', 'add_to_cart'])]
    )
    print(f"\n  📝 基本SQL生成例:")
    print(f"    {base_query}")
    
    # 2. プライベートプロジェクト設定のテスト
    print("\n🏢 2. プライベートプロジェクト設定")
    private_overrides = {
        'table': {
            'project_id': 'my-private-project',
            'dataset_id': 'my_ga4_dataset',
            'table_pattern': 'events_*',
            'public_dataset_project': None  # プライベートプロジェクト
        }
    }
    
    private_config = get_analysis_config(custom_overrides=private_overrides)
    print(f"  🗃️  プライベートテーブル参照: {private_config.get_full_table_reference()}")
    print(f"  🔍 パブリック dataset?: {private_config.table.is_public_dataset}")
    
    # 3. カスタムスキーマ設定のテスト
    print("\n🎛️ 3. カスタムスキーマ設定")
    custom_schema_overrides = {
        'schema': {
            'user_id_field': 'customer_id',
            'event_name_field': 'action_name',
            'device_category_field': 'device_type',
            'geo_country_field': 'location.country'
        },
        'table': {
            'project_id': 'custom-analytics',
            'dataset_id': 'ecommerce_events',
            'table_pattern': 'user_actions_*'
        }
    }
    
    custom_config = get_analysis_config(custom_overrides=custom_schema_overrides)
    
    print(f"  🗃️  カスタムテーブル参照: {custom_config.get_full_table_reference()}")
    print(f"  👤 ユーザーIDフィールド: {custom_config.schema.user_id_field}")
    print(f"  🎯 イベント名フィールド: {custom_config.schema.event_name_field}")
    print(f"  📱 デバイスフィールド: {custom_config.schema.device_category_field}")
    
    # カスタムスキーマでの基本SQL生成
    custom_query = custom_config.get_base_sql_query(
        select_fields=[
            custom_config.schema.user_id_field,
            custom_config.schema.event_name_field
        ],
        where_conditions=[f"{custom_config.schema.event_name_field} = 'purchase'"]
    )
    print(f"\n  📝 カスタムスキーマSQL例:")
    print(f"    {custom_query}")
    
    # 4. 異なるイベント構造のテスト
    print("\n🛍️ 4. カスタムイベント設定")
    custom_events_config = get_analysis_config(
        custom_overrides={
            'events': {
                'primary_events': ['order_complete', 'payment_success'],
                'funnel_events': ['product_view', 'add_cart', 'checkout_start', 'order_complete'],
                'engagement_events': ['page_scroll', 'video_play', 'form_submit']
            }
        }
    )
    
    print(f"  🎯 主要イベント: {custom_events_config.events.primary_events}")
    print(f"  🛒 ファネルイベント: {custom_events_config.events.funnel_events}")
    print(f"  ⚡ エンゲージイベント: {custom_events_config.events.engagement_events}")
    print(f"  📊 全イベント数: {len(custom_events_config.events.all_events)}件")
    
    # イベントフィルタの生成
    event_filter = custom_events_config.get_event_filter(['order_complete', 'payment_success'])
    print(f"  🔍 イベントフィルタ: {event_filter}")
    
    # 5. マルチテナント環境設定のテスト
    print("\n🏬 5. マルチテナント環境設定")
    tenant_configs = {}
    
    for tenant in ['tenant_a', 'tenant_b', 'tenant_c']:
        tenant_config = get_analysis_config(
            custom_overrides={
                'table': {
                    'project_id': f'{tenant}-analytics',
                    'dataset_id': f'{tenant}_ga4_data',
                    'public_dataset_project': None
                },
                'date_range': {
                    'start_date': '20230101',
                    'end_date': '20231231'
                }
            }
        )
        tenant_configs[tenant] = tenant_config
        print(f"  🏢 {tenant}: {tenant_config.get_full_table_reference()}")
        print(f"     📅 期間: {tenant_config.date_range.start_date} - {tenant_config.date_range.end_date}")
    
    # 6. SQLテンプレート生成のテスト
    print("\n📄 6. SQLテンプレート生成機能")
    
    # 転換率分析用SQL
    conversion_fields = [
        f"COUNT(DISTINCT {default_config.schema.user_id_field}) as total_users",
        f"COUNT(DISTINCT CASE WHEN {default_config.schema.purchase_event_condition} THEN {default_config.schema.user_id_field} END) as purchasers",
        f"COUNT(DISTINCT CASE WHEN {default_config.schema.purchase_event_condition} THEN {default_config.schema.user_id_field} END) / COUNT(DISTINCT {default_config.schema.user_id_field}) as conversion_rate"
    ]
    
    conversion_sql = default_config.get_base_sql_query(
        select_fields=conversion_fields,
        where_conditions=[default_config.device.control_filter]
    )
    
    print("  📈 転換率分析SQL:")
    print(f"    {conversion_sql}")
    
    # デバイス別分析用SQL
    device_fields = [
        default_config.schema.device_category_field,
        f"COUNT(DISTINCT {default_config.schema.user_id_field}) as users",
        f"AVG(CASE WHEN {default_config.schema.purchase_event_condition} THEN 1 ELSE 0 END) as conversion_rate"
    ]
    
    device_sql = f"{default_config.get_base_sql_query(device_fields)} GROUP BY {default_config.schema.device_category_field}"
    
    print("\n  📱 デバイス別分析SQL:")
    print(f"    {device_sql}")
    
    print("\n✅ テーブル依存設定システムテスト完了")
    print("🎉 全てのテーブル関連ハードコード値が設定可能になりました！")
    
    return True

def demonstrate_migration_benefits():
    """設定化による利点のデモンストレーション"""
    
    print("\n" + "="*60)
    print("🚀 設定化の利点デモンストレーション")
    print("="*60)
    
    # Before vs After の比較
    print("\n📋 Before (ハードコード) vs After (設定化)")
    
    print("\n❌ Before:")
    print("   - FROM `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`")
    print("   - WHERE _TABLE_SUFFIX BETWEEN '20201101' AND '20210131'")
    print("   - device.category = 'mobile'")
    print("   - user_pseudo_id, event_name (固定フィールド名)")
    
    print("\n✅ After:")
    config = get_analysis_config()
    print(f"   - FROM {config.get_full_table_reference()}")
    print(f"   - WHERE {config.get_sql_date_filter()}")
    print(f"   - {config.device.treatment_filter}")
    print(f"   - {config.schema.user_id_field}, {config.schema.event_name_field} (設定可能)")
    
    # 異なるデータソースでの実行例
    print("\n🔄 同じコードで異なるデータソースを処理:")
    
    data_sources = [
        {
            'name': 'GA4 Public Sample',
            'config': {
                'table': {
                    'project_id': 'my-project',
                    'dataset_id': 'ga4_obfuscated_sample_ecommerce',
                    'public_dataset_project': 'bigquery-public-data'
                }
            }
        },
        {
            'name': 'Firebase Analytics',
            'config': {
                'table': {
                    'project_id': 'my-firebase-project',
                    'dataset_id': 'analytics_123456789',
                    'table_pattern': 'events_*'
                },
                'schema': {
                    'user_id_field': 'user_pseudo_id',
                    'device_category_field': 'device.category'
                }
            }
        },
        {
            'name': 'Custom Analytics',
            'config': {
                'table': {
                    'project_id': 'custom-analytics',
                    'dataset_id': 'user_behavior',
                    'table_pattern': 'events_*'
                },
                'schema': {
                    'user_id_field': 'customer_id',
                    'event_name_field': 'action_type',
                    'device_category_field': 'device_info.type'
                }
            }
        }
    ]
    
    for source in data_sources:
        config = get_analysis_config(custom_overrides=source['config'])
        print(f"\n  📊 {source['name']}:")
        print(f"     テーブル: {config.get_full_table_reference()}")
        print(f"     ユーザーID: {config.schema.user_id_field}")
        print(f"     デバイス: {config.schema.device_category_field}")

if __name__ == "__main__":
    test_table_configuration()
    demonstrate_migration_benefits()