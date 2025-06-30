"""
LLMベースの柔軟なレポート生成システム
"""

import json
import os
from datetime import datetime
from typing import Optional
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from src.api import get_openai_response

class FlexibleReportGenerator:
    """LLMを活用した拡張性の高いレポート生成"""
    
    def __init__(self):
        self.timestamp = datetime.now()
    
    def generate_report(self, 
                       results_file: str, 
                       report_type: str = "executive_summary",
                       language: str = "japanese",
                       custom_requirements: Optional[str] = None) -> str:
        """
        柔軟なレポート生成
        
        Args:
            results_file: 結果JSONファイルパス
            report_type: レポートタイプ（executive_summary, technical_detail, action_items等）
            language: 言語（japanese, english等）
            custom_requirements: カスタム要求事項
        """
        
        # 結果データ読み込み
        with open(results_file, 'r', encoding='utf-8') as f:
            results = json.load(f)
        
        # レポートタイプ別のプロンプト設定
        report_prompts = {
            "executive_summary": "経営層向けのエグゼクティブサマリー（ビジネス価値重視）",
            "technical_detail": "技術者向けの詳細分析レポート（統計手法・実装詳細含む）",
            "action_items": "実行可能なアクションアイテムリスト（優先順位付き）",
            "visual_insights": "視覚的に理解しやすいインサイトレポート（グラフ説明付き）",
            "stakeholder_update": "ステークホルダー向け進捗報告書",
            "custom": custom_requirements or "標準的なレポート"
        }
        
        # 言語設定
        language_instructions = {
            "japanese": "日本語で、敬語を使用し、ビジネス文書として適切な形式で",
            "english": "In professional English with clear structure",
            "chinese": "用专业的中文撰写",
            "mixed": "日本語メインで重要な専門用語は英語併記"
        }
        
        # LLMプロンプト構築
        prompt = f"""
以下の分析結果から、{report_prompts.get(report_type, report_prompts['custom'])}を生成してください。

【言語要件】
{language_instructions.get(language, language_instructions['japanese'])}作成してください。

【分析結果データ】
{json.dumps(results, ensure_ascii=False, indent=2)}

【レポート要件】
1. 読み手に合わせた適切なトーンと詳細度
2. 重要な発見を強調
3. 実用的な洞察を含める
4. 必要に応じて図表の説明を含める（実際の図表は生成不要）
5. 明確な構造（セクション分け）

{f"【追加要件】{custom_requirements}" if custom_requirements else ""}

【出力形式】
マークダウン形式で、以下の構造を含めてください：
- タイトル
- エグゼクティブサマリー（該当する場合）
- 主要な発見
- 詳細分析（必要に応じて）
- 推奨事項
- 次のステップ
"""
        
        # LLM呼び出し
        report_content = get_openai_response(prompt)
        
        # メタデータ追加
        metadata = f"""
---
生成日時: {self.timestamp.strftime('%Y年%m月%d日 %H:%M:%S')}
レポートタイプ: {report_type}
言語: {language}
データソース: {os.path.basename(results_file)}
---

"""
        
        return metadata + report_content
    
    def generate_comparison_report(self, 
                                 results_files: list,
                                 comparison_focus: str = "performance") -> str:
        """複数の結果を比較するレポート生成"""
        
        all_results = {}
        for file_path in results_files:
            with open(file_path, 'r', encoding='utf-8') as f:
                all_results[os.path.basename(file_path)] = json.load(f)
        
        comparison_prompts = {
            "performance": "各実行の性能比較（効果サイズ、有意性等）",
            "hypothesis_quality": "仮説の品質と改良度合いの比較",
            "time_efficiency": "実行時間とリソース効率の比較",
            "business_impact": "ビジネスインパクトの比較"
        }
        
        prompt = f"""
複数の分析結果を比較して、{comparison_prompts.get(comparison_focus, '総合的な比較')}レポートを作成してください。

【比較データ】
{json.dumps(all_results, ensure_ascii=False, indent=2)}

【レポート要件】
1. 各結果の主要指標を表形式で比較
2. 改善点と劣化点を明確に指摘
3. トレンドや傾向の分析
4. 最も効果的だった手法の特定
5. 今後の改善提案

日本語でマークダウン形式で出力してください。
"""
        
        return get_openai_response(prompt)
    
    def generate_custom_analysis(self, 
                               results_file: str,
                               analysis_questions: list) -> str:
        """カスタム質問に基づく分析レポート"""
        
        with open(results_file, 'r', encoding='utf-8') as f:
            results = json.load(f)
        
        questions_text = "\n".join([f"{i+1}. {q}" for i, q in enumerate(analysis_questions)])
        
        prompt = f"""
以下の分析結果について、指定された質問に答える形式でレポートを作成してください。

【分析結果】
{json.dumps(results, ensure_ascii=False, indent=2)}

【回答すべき質問】
{questions_text}

【回答形式】
各質問に対して：
- 明確な回答
- データに基づく根拠
- 実用的な示唆
を含めて日本語で回答してください。
"""
        
        return get_openai_response(prompt)

# 使用例
def main():
    generator = FlexibleReportGenerator()
    
    # 最新の結果ファイルを探す
    results_dir = "/Users/idenominoru/Desktop/AI_analyst/results"
    result_files = sorted([f for f in os.listdir(results_dir) if f.startswith("ultimate_lite_results_")])
    
    if not result_files:
        print("結果ファイルが見つかりません")
        return
    
    latest_file = os.path.join(results_dir, result_files[-1])
    
    # 1. エグゼクティブサマリー生成
    print("📊 エグゼクティブサマリー生成中...")
    executive_report = generator.generate_report(
        latest_file, 
        report_type="executive_summary",
        language="japanese"
    )
    
    # 2. 技術詳細レポート生成
    print("🔧 技術詳細レポート生成中...")
    technical_report = generator.generate_report(
        latest_file,
        report_type="technical_detail",
        language="mixed"
    )
    
    # 3. アクションアイテム生成
    print("✅ アクションアイテム生成中...")
    action_report = generator.generate_report(
        latest_file,
        report_type="action_items",
        language="japanese",
        custom_requirements="優先度と期限の目安も含めてください"
    )
    
    # 4. カスタム分析
    print("🎯 カスタム分析実行中...")
    custom_questions = [
        "モバイルとデスクトップの差が生じる根本原因は何か？",
        "投資対効果（ROI）の観点から最優先で取り組むべき施策は？",
        "この結果から予測される3ヶ月後の状況は？"
    ]
    custom_report = generator.generate_custom_analysis(latest_file, custom_questions)
    
    # レポート保存
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    reports = {
        "executive_summary": executive_report,
        "technical_detail": technical_report,
        "action_items": action_report,
        "custom_analysis": custom_report
    }
    
    for report_type, content in reports.items():
        filename = f"/Users/idenominoru/Desktop/AI_analyst/results/reports/{report_type}_{timestamp}.md"
        with open(filename, 'w', encoding='utf-8') as f:
            f.write(content)
        print(f"📁 {report_type} 保存完了: {filename}")
    
    # 最初のレポートを表示
    print("\n" + "="*60)
    print("📊 エグゼクティブサマリー")
    print("="*60)
    print(executive_report)

if __name__ == "__main__":
    main()