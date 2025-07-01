# CLAUDE.md

このファイルは、Claude Code (claude.ai/code) がこのリポジトリのコードを扱う際のガイダンスを提供します。

## 概要

これは、データスキーマからビジネス仮説を自動生成し、BigQuery SQL クエリを使用してテストする GA4（Google Analytics 4）データ分析パイプラインです。パイプラインは、LLM（OpenAI GPT-4o mini）を使用して仮説を生成し、分析レポートを作成します。

## プロジェクト構造

```
AI_analyst/
├── src/                         # ソースコード
│   ├── api.py                  # OpenAI API インターフェース
│   ├── config.py               # 設定管理
│   ├── extract/                           # データ抽出
│   │   └── extract_bigquery_schema.py    # BigQueryスキーマ抽出
│   ├── analysis/                          # 分析処理（詳細は後述）
│   │   ├── generate_hypotheses_from_schema.py      # スキーマから仮説生成
│   │   ├── create_sql_queries_from_hypotheses.py   # 仮説からSQL生成・実行
│   │   ├── execute_queries_and_generate_reports.py # クエリ実行・統計検証
│   │   ├── hypothesis_validation_pipeline.py       # 統合検証パイプライン
│   │   ├── ultimate_hypothesis_system.py           # 究極の仮説システム
│   │   ├── generate_flexible_report.py             # 柔軟なレポート生成
│   │   └── systematic_experiment_runner.py         # 系統的実験実行
│   └── utils/                             # ユーティリティ
│       └── convert_csv_to_schema_text.py  # CSV→スキーマテキスト変換（未実装）
├── data/                       # データファイル
│   ├── schemas/               # スキーマ関連
│   │   ├── ga4_schema.txt
│   │   └── ga4_table_schema.csv
│   └── hypotheses/            # 仮説関連
│       └── hypotheses.json
├── results/                    # 実行結果
│   ├── llm_responses/         # LLM応答
│   ├── reports/               # レポート
│   │   ├── hypothesis_reports/
│   │   └── quantitative_report.txt
│   └── hypothesis_results.csv
├── scripts/                    # 実行スクリプト
│   └── run_pipeline.py        # パイプライン一括実行
├── .env.example               # 環境変数テンプレート
├── .gitignore                 # Git除外設定
├── requirements.txt           # Python依存関係
└── CLAUDE.md                  # このファイル
```

## アーキテクチャ

データ処理パイプラインは以下のフローに従います：

```
1. src/extract/extract_bigquery_schema.py → BigQueryテーブルスキーマを抽出 → data/schemas/ga4_schema.txt, data/schemas/data_exploration.txt
2. src/analysis/generate_hypotheses_from_schema.py → LLMを使用して仮説を生成 → data/hypotheses/hypotheses.json
3. src/analysis/hypothesis_validation_pipeline.py → 仮説から検証まで統合実行（再帰ループ付き） → results/reports/quantitative_report.txt
```

### 統合パイプラインの特徴

- **SQL 修正再帰ループ**: SQL 実行エラー時に自動修正（最大 5 回）
- **分析品質検証**: レポート品質チェックと自動改善（最大 3 回）
- **詳細ログ出力**: 全プロセスの詳細ログを記録
- **マークダウン除去**: OpenAI 生成 SQL の`sql`ブロック自動除去
- **エラーハンドリング**: 各ステップでの適切なエラー処理
- **実績**: 5 件中 2 件の仮説検証成功（成功率 40%）

## よく使うコマンド

### 初期セットアップ

```bash
# ステップ1: 環境ファイルをコピーして設定
cp .env.example .env
# .envを編集してOpenAI APIキーを追加

# ステップ2: 依存関係をインストール
pip install google-cloud-bigquery openai pandas python-dotenv
```

### パイプラインの実行

#### オプション 1: すべてのステップを自動実行

```bash
python scripts/run_pipeline.py
```

#### オプション 2: 個別ステップの実行

```bash
# ステップ1: BigQueryスキーマを抽出（必要な場合）
python src/extract/extract_bigquery_schema.py

# ステップ2: スキーマから仮説を生成
python src/analysis/generate_hypotheses_from_schema.py

# ステップ3: 仮説検証パイプライン（統合版）
python src/analysis/hypothesis_validation_pipeline.py

# 注意: 実行には約4-6分かかります（BigQuery + OpenAI API呼び出し）
```

### 依存関係

このプロジェクトには以下の Python パッケージが必要です：

- `google-cloud-bigquery`
- `openai`
- `pandas`
- `python-dotenv`
- `db-dtypes` (BigQuery 結果の DataFrame 変換用)

インストール方法：

```bash
pip install google-cloud-bigquery openai pandas python-dotenv db-dtypes
```

注意: conda ユーザーは新しい環境での実行を推奨:

```bash
conda create -n ai-analyst python=3.12
conda activate ai-analyst
pip install -r requirements.txt
```

## 設定

すべての設定は `config.py` に一元化されており、環境変数または `.env` ファイルから読み込まれます。

### 環境変数

テンプレートから `.env` ファイルを作成：

```bash
cp .env.example .env
```

主要な変数：

- **OPENAI_API_KEY**: OpenAI API キー（必須）
- **GCP_PROJECT_ID**: Google Cloud プロジェクト ID（デフォルト: "ai-analyst-test-1"）
- **GCP_DATASET_ID**: BigQuery データセット ID（デフォルト: "bigquery-public-data.ga4_obfuscated_sample_ecommerce"）

利用可能なすべての設定オプションについては `.env.example` を参照してください。

### Google Cloud 認証

デフォルト認証情報またはサービスアカウントを使用します。セットアップ方法：

```bash
gcloud auth application-default login
```

### 主要なファイルとディレクトリ

- `data/schemas/ga4_schema.txt`: GA4 テーブルスキーマ要約（仮説生成に必須）
- `data/hypotheses/hypotheses.json`: 生成された仮説
- `results/llm_responses/`: 各仮説の SQL クエリを含むディレクトリ
- `results/reports/`: 分析レポートを含むディレクトリ
- `src/`: 機能別に整理されたすべてのソースコード

## 重要な注意事項

1. **セキュリティ**: API キーは`.env`ファイルの環境変数で管理されます。`.env`ファイルをバージョン管理にコミットしないでください。

2. **手動ステップ**: `src/utils/convert_csv_to_schema_text.py`は空です。スキーマテキストファイル（`data/schemas/ga4_schema.txt`）は CSV 出力から手動で作成する必要があります。

3. **BigQuery データセット**: 公開 GA4 サンプルデータセットを使用: `bigquery-public-data.ga4_obfuscated_sample_ecommerce`

4. **言語**: コードには日本語のコメントが含まれ、日本語のレポートを生成します。

5. **エラーハンドリング**: SQL クエリには堅牢性のためのリトライ機構（3 回試行）があります。

6. **ディレクトリ作成**: 必要なディレクトリは`config.py`によって自動的に作成されます。

7. **プロジェクト構成**:

   - ソースコードは`src/`ディレクトリに配置
   - 入力データファイルは`data/`ディレクトリに配置
   - 出力結果は`results/`ディレクトリに配置
   - ユーティリティスクリプトは`scripts/`ディレクトリに配置

8. **ログファイル**:
   - `hypothesis_validation.log` に詳細な実行ログが記録されます
   - 各仮説の SQL 生成・実行・分析過程を追跡できます

## 一般的なワークフロー

1. 環境をセットアップ: `cp .env.example .env` を実行し、OpenAI API キーを追加
2. 依存関係をインストール: `pip install -r requirements.txt`
3. `data/schemas/ga4_schema.txt`にスキーマ要約が存在することを確認
4. 完全なパイプラインを実行: `python scripts/run_pipeline.py`
5. 結果を確認:
   - `results/reports/hypothesis_reports/` - 個別の仮説レポート
   - `results/reports/quantitative_report.txt` - 要約レポート
   - `results/llm_responses/` - 生成された SQL クエリ
   - `hypothesis_validation.log` - 実行ログ

## トラブルシューティング

### よくある問題と解決策

1. **BigQuery 認証エラー**:

   ```bash
   gcloud auth application-default login
   ```

2. **NumPy/pandas インポートエラー（conda 環境）**:

   ```bash
   conda create -n ai-analyst python=3.12
   conda activate ai-analyst
   pip install -r requirements.txt
   ```

3. **SQL 構文エラー**:

   - 統合パイプラインが自動修正（最大 5 回）
   - ログファイルで詳細なエラー情報を確認

4. **API 制限エラー**:
   - OpenAI API の利用制限を確認
   - BigQuery 無料枠の制限に注意

## analysis ディレクトリの詳細解説

`src/analysis/`には複数の分析コンポーネントが含まれており、それぞれ異なる機能を提供します：

### コアコンポーネント

#### `generate_hypotheses_from_schema.py`

- **目的**: GA4 スキーマからビジネス仮説を生成
- **機能**:
  - OpenAI GPT を使用して e コマース購買ファネルの仮説を 5 つ生成
  - JSON 形式で仮説 ID、条件、期待結果を構造化
  - 実際の GA4 イベント名（view_item, add_to_cart, purchase 等）を使用
- **入力**: `data/schemas/ga4_schema.txt`, `data/schemas/data_exploration.txt`
- **出力**: `data/hypotheses/hypotheses.json`

#### `create_sql_queries_from_hypotheses.py`

- **目的**: 仮説から BigQuery SQL クエリを生成・実行
- **機能**:
  - 各仮説に対応する SQL 分析クエリを生成
  - マークダウン除去（`sql`ブロック自動削除）
  - リトライ機構（最大 3 回）で SQL 実行エラーを処理
  - 日本語レポート生成
- **入力**: `data/hypotheses/hypotheses.json`
- **出力**: `results/llm_responses/`, `results/reports/hypothesis_reports/`

#### `execute_queries_and_generate_reports.py`

- **目的**: SQL クエリ実行結果から統計レポート生成
- **機能**:
  - 仮説別の定量分析結果を集計
  - 統計的比較（相対値、ポイント差等）
  - 客観的数値表現（業界平均との比較等）
- **入力**: `results/llm_responses/`配下の SQL 実行結果
- **出力**: `results/reports/quantitative_report.txt`

### 統合システム

#### `hypothesis_validation_pipeline.py`

- **目的**: 仮説生成から検証まで統合実行
- **機能**:
  - SQL 修正再帰ループ（最大 5 回）
  - 分析品質検証と自動改善（最大 3 回）
  - 詳細ログ出力（`hypothesis_validation.log`）
  - エラーハンドリングと例外処理
- **特徴**: 成功率 40%（5 件中 2 件の仮説検証成功）の実績
- **実行時間**: 約 4-6 分（BigQuery + OpenAI API 呼び出し）

### 高度なシステム

#### `ultimate_hypothesis_system.py`

- **目的**: AI-Scientist-v2 スタイルの段階的改良プロセス
- **機能**:
  - 仮説の進化プロセス管理（`HypothesisEvolution`クラス）
  - 実験結果の構造化（`ExperimentResult`クラス）
  - 非同期処理による並列実行
- **特徴**: 系統的実験実行と D2D_Data2Dashboard スタイル

#### `generate_flexible_report.py`

- **目的**: LLM ベースの柔軟なレポート生成
- **機能**:
  - 複数のレポートタイプ対応
    - `executive_summary`: 経営層向けエグゼクティブサマリー
    - `technical_detail`: 技術者向け詳細分析
    - `action_items`: 実行可能アクションアイテムリスト
    - `visual_insights`: 視覚的インサイトレポート
  - 多言語対応（日本語・英語）
  - カスタム要求事項への対応
- **マークダウン出力**: タイトル、エグゼクティブサマリー、主要発見、詳細分析

#### `systematic_experiment_runner.py`

- **目的**: D2D スタイルの系統的実験実行
- **機能**:
  - A/B テスト風の実験設計
  - 実験群・対照群の具体的設計
  - 統計的有意性検定
  - 信頼区間計算
- **構造**: `ExperimentResult`データクラスによる結果管理

### アーキテクチャの特徴

1. **モジュラー設計**: 各コンポーネントが独立して実行可能
2. **エラー耐性**: 複数レベルでのリトライ機構
3. **ログ追跡**: 全プロセスの詳細ログ出力
4. **品質保証**: 自動品質チェックと改善ループ
5. **スケーラビリティ**: 並列処理と非同期実行対応

### 推奨実行パターン

- **基本**: `hypothesis_validation_pipeline.py` で統合実行
- **高度**: `ultimate_hypothesis_system.py` で段階的改良
- **カスタム**: 個別コンポーネントを組み合わせて使用
