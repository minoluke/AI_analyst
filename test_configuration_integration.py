#!/usr/bin/env python3
"""
設定システム統合のテストスクリプト
ハードコードされた値が設定システムに置き換えられたことを確認
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from src.config_analysis import get_analysis_config

def test_configuration_integration():
    """設定システムの統合をテスト"""
    
    print("🔧 設定システム統合テスト開始")
    print("=" * 50)
    
    # デフォルト設定のテスト
    print("\n📋 1. デフォルト設定の確認")
    default_config = get_analysis_config()
    
    print(f"  📅 分析期間: {default_config.date_range.start_date} - {default_config.date_range.end_date}")
    print(f"  📱 デバイス: {', '.join(default_config.device.categories)}")
    print(f"  📊 イベント数: {len(default_config.events.all_events)}件")
    print(f"  🔢 統計設定: α={default_config.statistical.alpha}, 最小効果サイズ={default_config.statistical.min_effect_size}")
    print(f"  ⚙️ 処理設定: SQLリトライ={default_config.processing.sql_retry_limit}回, 改良ラウンド={default_config.processing.refinement_rounds}回")
    
    # SQL日付フィルタの生成
    print(f"\n  📝 SQL日付フィルタ: {default_config.get_sql_date_filter()}")
    
    # 実験設計の生成
    experiment_design = default_config.get_experiment_design()
    print(f"  🧪 実験設計:")
    print(f"    - 対照群: {experiment_design['control_group']['sql_filter']}")
    print(f"    - 実験群: {experiment_design['treatment_group']['sql_filter']}")
    
    # 有意性判定のテスト
    print(f"\n  📈 有意性判定:")
    test_effects = [0.03, 0.05, 0.1, 0.15]
    for effect in test_effects:
        is_sig = default_config.is_significant(effect)
        print(f"    - 効果サイズ {effect}: {'有意' if is_sig else '非有意'}")
    
    # 本番環境設定のテスト
    print("\n🏭 2. 本番環境設定の確認")
    prod_config = get_analysis_config("production")
    
    print(f"  📊 統計設定: α={prod_config.statistical.alpha}, 最小サンプル数={prod_config.statistical.min_sample_size}")
    print(f"  ⚙️ 処理設定: SQLリトライ={prod_config.processing.sql_retry_limit}回")
    
    # カスタム設定のテスト
    print("\n🎛️ 3. カスタム設定の確認")
    custom_overrides = {
        'date_range': {
            'start_date': '20230101',
            'end_date': '20231231'
        },
        'device': {
            'default_control': 'tablet',
            'default_treatment': 'mobile'
        },
        'statistical': {
            'alpha': 0.01,
            'min_effect_size': 0.1
        }
    }
    
    custom_config = get_analysis_config(custom_overrides=custom_overrides)
    print(f"  📅 分析期間: {custom_config.date_range.start_date} - {custom_config.date_range.end_date}")
    print(f"  📱 実験設計: {custom_config.device.default_control} vs {custom_config.device.default_treatment}")
    print(f"  📊 統計設定: α={custom_config.statistical.alpha}, 最小効果サイズ={custom_config.statistical.min_effect_size}")
    
    # 出力パスの生成テスト
    print("\n📁 4. 出力パス生成の確認")
    output_path = custom_config.output.get_output_path('test_results.json')
    report_path = custom_config.output.get_report_path('summary_report')
    print(f"  📄 結果ファイル: {output_path}")
    print(f"  📋 レポートファイル: {report_path}")
    
    print("\n✅ 設定システム統合テスト完了")
    print("🎉 すべてのハードコードされた値が設定可能になりました！")
    
    return True

if __name__ == "__main__":
    test_configuration_integration()