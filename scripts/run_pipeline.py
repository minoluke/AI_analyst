#!/usr/bin/env python3
"""
AI Analyst パイプライン実行スクリプト
GA4データから仮説を生成し、SQLで検証して、レポートを作成します。
"""
import sys
import os
import subprocess

# プロジェクトルートをパスに追加
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

def run_step(name, script_path):
    """各ステップを実行する"""
    print(f"\n{'='*60}")
    print(f"🚀 実行中: {name}")
    print(f"{'='*60}")
    
    result = subprocess.run([sys.executable, script_path], capture_output=True, text=True)
    
    if result.returncode == 0:
        print(result.stdout)
        print(f"✅ {name} が正常に完了しました")
    else:
        print(f"❌ {name} でエラーが発生しました:")
        print(result.stderr)
        return False
    
    return True

def main():
    """メインパイプライン"""
    print("=== AI Analyst パイプライン ===")
    
    # スクリプトのパスを定義
    scripts = [
        ("BigQueryスキーマ抽出", "src/extract/extract_bigquery_schema.py"),
        ("スキーマから仮説を生成", "src/analysis/generate_hypotheses_from_schema.py"),
        ("仮説検証パイプライン（統合版）", "src/analysis/hypothesis_validation_pipeline.py")
    ]
    
    # 各ステップを実行
    for name, script in scripts:
        if not run_step(name, script):
            print("\n⚠️  パイプラインが中断されました")
            sys.exit(1)
    
    print("\n✨ すべてのステップが完了しました！")
    print("📊 結果は results/ ディレクトリに保存されています")

if __name__ == "__main__":
    main()