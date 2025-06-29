# CLAUDE.md

このファイルは、Claude Code (claude.ai/code) がこのリポジトリのコードを扱う際のガイダンスを提供します。

## 概要

これは、データスキーマからビジネス仮説を自動生成し、BigQuery SQLクエリを使用してテストするGA4（Google Analytics 4）データ分析パイプラインです。パイプラインは、LLM（OpenAI GPT-4）を使用して仮説を生成し、分析レポートを作成します。

## プロジェクト構造

```
AI_analyst/
├── src/                         # ソースコード
│   ├── api.py                  # OpenAI API インターフェース
│   ├── config.py               # 設定管理
│   ├── extract/                           # データ抽出
│   │   └── extract_bigquery_schema.py    # BigQueryスキーマ抽出
│   ├── analysis/                          # 分析処理
│   │   ├── generate_hypotheses_from_schema.py      # スキーマから仮説生成
│   │   ├── create_sql_queries_from_hypotheses.py   # 仮説からSQL生成・実行
│   │   └── execute_queries_and_generate_reports.py # クエリ実行・統計検証
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

- **SQL修正再帰ループ**: SQL実行エラー時に自動修正（最大5回）
- **分析品質検証**: レポート品質チェックと自動改善（最大3回）
- **詳細ログ出力**: 全プロセスの詳細ログを記録
- **エラーハンドリング**: 各ステップでの適切なエラー処理

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

#### オプション1: すべてのステップを自動実行
```bash
python scripts/run_pipeline.py
```

#### オプション2: 個別ステップの実行
```bash
# ステップ1: BigQueryスキーマを抽出（必要な場合）
python src/extract/extract_bigquery_schema.py

# ステップ2: スキーマから仮説を生成
python src/analysis/generate_hypotheses_from_schema.py

# ステップ3: 仮説検証パイプライン（統合版）
python src/analysis/hypothesis_validation_pipeline.py
```

### 依存関係

このプロジェクトには以下のPythonパッケージが必要です：
- `google-cloud-bigquery`
- `openai`
- `pandas`
- `python-dotenv`

インストール方法：
```bash
pip install google-cloud-bigquery openai pandas python-dotenv
```

## 設定

すべての設定は `config.py` に一元化されており、環境変数または `.env` ファイルから読み込まれます。

### 環境変数

テンプレートから `.env` ファイルを作成：
```bash
cp .env.example .env
```

主要な変数：
- **OPENAI_API_KEY**: OpenAI APIキー（必須）
- **GCP_PROJECT_ID**: Google CloudプロジェクトID（デフォルト: "ai-analyst-test-1"）
- **GCP_DATASET_ID**: BigQueryデータセットID（デフォルト: "bigquery-public-data.ga4_obfuscated_sample_ecommerce"）

利用可能なすべての設定オプションについては `.env.example` を参照してください。

### Google Cloud認証
デフォルト認証情報またはサービスアカウントを使用します。セットアップ方法：
```bash
gcloud auth application-default login
```

### 主要なファイルとディレクトリ
- `data/schemas/ga4_schema.txt`: GA4テーブルスキーマ要約（仮説生成に必須）
- `data/hypotheses/hypotheses.json`: 生成された仮説
- `results/llm_responses/`: 各仮説のSQLクエリを含むディレクトリ
- `results/reports/`: 分析レポートを含むディレクトリ
- `src/`: 機能別に整理されたすべてのソースコード

## 重要な注意事項

1. **セキュリティ**: APIキーは`.env`ファイルの環境変数で管理されます。`.env`ファイルをバージョン管理にコミットしないでください。

2. **手動ステップ**: `src/utils/convert_csv_to_schema_text.py`は空です。スキーマテキストファイル（`data/schemas/ga4_schema.txt`）はCSV出力から手動で作成する必要があります。

3. **BigQueryデータセット**: 公開GA4サンプルデータセットを使用: `bigquery-public-data.ga4_obfuscated_sample_ecommerce`

4. **言語**: コードには日本語のコメントが含まれ、日本語のレポートを生成します。

5. **エラーハンドリング**: SQLクエリには堅牢性のためのリトライ機構（3回試行）があります。

6. **ディレクトリ作成**: 必要なディレクトリは`config.py`によって自動的に作成されます。

7. **プロジェクト構成**: 
   - ソースコードは`src/`ディレクトリに配置
   - 入力データファイルは`data/`ディレクトリに配置
   - 出力結果は`results/`ディレクトリに配置
   - ユーティリティスクリプトは`scripts/`ディレクトリに配置

## 一般的なワークフロー

1. 環境をセットアップ: `cp .env.example .env` を実行し、OpenAI APIキーを追加
2. 依存関係をインストール: `pip install -r requirements.txt`
3. `data/schemas/ga4_schema.txt`にスキーマ要約が存在することを確認
4. 完全なパイプラインを実行: `python scripts/run_pipeline.py`
5. 結果を確認:
   - `results/reports/hypothesis_reports/` - 個別の仮説レポート
   - `results/reports/quantitative_report.txt` - 要約レポート
   - `results/llm_responses/` - 生成されたSQLクエリ