"""
究極システムの結果から日本語レポートを生成
"""

import json
from datetime import datetime
from typing import Dict

def generate_japanese_report(results_file: str) -> str:
    """結果JSONから日本語レポートを生成"""
    
    with open(results_file, 'r', encoding='utf-8') as f:
        results = json.load(f)
    
    report = []
    report.append("=" * 60)
    report.append("📊 仮説検証システム 実行結果レポート")
    report.append("=" * 60)
    report.append("")
    
    # セッション情報
    report.append("【実行情報】")
    report.append(f"実行開始時刻: {results['session_info']['start_time']}")
    report.append(f"実行時間: {results['session_info']['duration']}")
    report.append(f"システムバージョン: {results['session_info']['version']}")
    report.append("")
    
    # 仮説サマリー
    report.append("【生成された仮説】")
    report.append(f"総仮説数: {results['summary']['hypotheses_count']}件")
    report.append(f"実験実行数: {results['summary']['experiments_count']}件")
    report.append(f"統計的有意な結果: {results['summary']['significant_results']}件")
    report.append("")
    
    # 各仮説の詳細
    report.append("=" * 60)
    report.append("📋 仮説詳細")
    report.append("=" * 60)
    
    for i, hypothesis in enumerate(results['hypotheses'], 1):
        report.append(f"\n### 仮説 {i}: {hypothesis['id']}")
        report.append(f"【内容】")
        report.append(f"{hypothesis['hypothesis']}")
        report.append(f"\n【信頼度スコア】{hypothesis['confidence_score']}/10")
        report.append(f"【期待されるビジネス価値】")
        report.append(f"{hypothesis['business_impact']}")
        report.append(f"\n【検証計画】")
        report.append(f"- 主要指標: {hypothesis['verification_plan']['primary_metric']}")
        report.append(f"- 比較対象: {hypothesis['verification_plan']['comparison_groups']}")
        report.append("")
    
    # 実験結果
    report.append("=" * 60)
    report.append("🧪 実験結果")
    report.append("=" * 60)
    
    for hyp_id, exp_data in results['experiments'].items():
        report.append(f"\n### {hyp_id} の実験結果")
        
        if exp_data['experiment_results']:
            for exp_name, exp_result in exp_data['experiment_results'].items():
                report.append(f"\n【{exp_name}】")
                report.append(f"対照群（デスクトップ）:")
                report.append(f"  - ユーザー数: {exp_result['control_users']:,}人")
                report.append(f"  - 転換率: {exp_result['control_rate']:.2%}")
                report.append(f"実験群（モバイル）:")
                report.append(f"  - ユーザー数: {exp_result['treatment_users']:,}人")
                report.append(f"  - 転換率: {exp_result['treatment_rate']:.2%}")
                report.append(f"\n📊 効果サイズ: {exp_result['effect_size']:.1%}")
                
                if exp_result['significant']:
                    report.append("✅ 統計的に有意な差が検出されました")
                else:
                    report.append("❌ 統計的に有意な差は検出されませんでした")
        else:
            report.append("実験実行に失敗しました")
    
    # 総合評価
    report.append("\n" + "=" * 60)
    report.append("💡 総合評価と洞察")
    report.append("=" * 60)
    
    # エフェクトサイズに基づく洞察
    effect_sizes = []
    for exp_data in results['experiments'].values():
        for exp_result in exp_data['experiment_results'].values():
            effect_sizes.append(exp_result['effect_size'])
    
    if effect_sizes:
        avg_effect = sum(effect_sizes) / len(effect_sizes)
        report.append(f"\n平均効果サイズ: {avg_effect:.1%}")
        
        if avg_effect < 0:
            report.append("\n【主要な発見】")
            report.append(f"モバイルデバイスの転換率がデスクトップより{abs(avg_effect):.1%}低いことが判明しました。")
            report.append("これは、モバイルユーザー体験の改善が急務であることを示しています。")
            report.append("\n【推奨アクション】")
            report.append("1. モバイルサイトのUI/UX改善")
            report.append("2. モバイル向けチェックアウトプロセスの簡素化")
            report.append("3. モバイル専用プロモーションの検討")
        else:
            report.append("\n【主要な発見】")
            report.append(f"モバイルデバイスの転換率がデスクトップより{avg_effect:.1%}高いことが判明しました。")
            report.append("モバイルファーストの戦略が功を奏しています。")
    
    report.append("\n" + "=" * 60)
    report.append(f"レポート生成日時: {datetime.now().strftime('%Y年%m月%d日 %H:%M:%S')}")
    report.append("=" * 60)
    
    return "\n".join(report)

# 実行
if __name__ == "__main__":
    import sys
    import os
    
    # 最新の結果ファイルを探す
    results_dir = "/Users/idenominoru/Desktop/AI_analyst/results"
    result_files = [f for f in os.listdir(results_dir) if f.startswith("ultimate_lite_results_")]
    
    if result_files:
        # 最新のファイルを選択
        latest_file = sorted(result_files)[-1]
        results_path = os.path.join(results_dir, latest_file)
        
        # レポート生成
        report = generate_japanese_report(results_path)
        
        # ファイル保存
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        report_file = f"/Users/idenominoru/Desktop/AI_analyst/results/reports/japanese_report_{timestamp}.txt"
        
        with open(report_file, 'w', encoding='utf-8') as f:
            f.write(report)
        
        # 画面表示
        print(report)
        print(f"\n📁 レポート保存先: {report_file}")
    else:
        print("結果ファイルが見つかりません")