from openai import OpenAI
from .config import OPENAI_API_KEY, OPENAI_MODEL, validate_config

# 設定の検証
validate_config()

def get_openai_response(prompt: str) -> str:
    client = OpenAI(api_key=OPENAI_API_KEY)
    response = client.chat.completions.create(
        model=OPENAI_MODEL,
        messages=[{"role": "user", "content": prompt}]
    )
    return response.choices[0].message.content