import os
from dotenv import load_dotenv

# プロジェクトルートディレクトリを取得
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# .envファイルから環境変数を読み込む
load_dotenv(os.path.join(PROJECT_ROOT, '.env'))

# ――― Google Cloud設定 ―――
PROJECT_ID = os.getenv("GCP_PROJECT_ID", "ai-analyst-test-1")
DATASET_ID = os.getenv("GCP_DATASET_ID", "ga4_obfuscated_sample_ecommerce")

# ――― OpenAI API設定 ―――
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4o-mini")

# ――― ファイルパス設定 ―――
SCHEMA_CSV_FILE = os.path.join(PROJECT_ROOT, "data", "schemas", "ga4_table_schema.csv")
SCHEMA_TXT_FILE = os.path.join(PROJECT_ROOT, "data", "schemas", "ga4_schema.txt")
DATA_EXPLORATION_FILE = os.path.join(PROJECT_ROOT, "data", "schemas", "data_exploration.txt")
HYPOTHESES_FILE = os.path.join(PROJECT_ROOT, "data", "hypotheses", "hypotheses.json")
LLM_RESPONSES_DIR = os.path.join(PROJECT_ROOT, "results", "llm_responses")
HYPOTHESIS_REPORTS_DIR = os.path.join(PROJECT_ROOT, "results", "reports", "hypothesis_reports")
QUANTITATIVE_REPORT_FILE = os.path.join(PROJECT_ROOT, "results", "reports", "quantitative_report.txt")
HYPOTHESIS_RESULTS_FILE = os.path.join(PROJECT_ROOT, "results", "hypothesis_results.csv")

# ――― 処理設定 ―――
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "3"))
REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", "60"))

# ――― 検証 ―――
def validate_config():
    """必須の設定値が存在するか検証"""
    errors = []
    
    if not OPENAI_API_KEY:
        errors.append("OPENAI_API_KEY is not set. Please set it in .env file or environment variables.")
    
    if not PROJECT_ID:
        errors.append("GCP_PROJECT_ID is not set.")
    
    if errors:
        raise ValueError("\n".join(errors))

# ディレクトリが存在しない場合は作成
os.makedirs(LLM_RESPONSES_DIR, exist_ok=True)
os.makedirs(HYPOTHESIS_REPORTS_DIR, exist_ok=True)
os.makedirs(os.path.dirname(SCHEMA_CSV_FILE), exist_ok=True)
os.makedirs(os.path.dirname(HYPOTHESES_FILE), exist_ok=True)