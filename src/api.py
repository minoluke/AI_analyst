import asyncio
from openai import OpenAI
from .config import OPENAI_API_KEY, OPENAI_MODEL, validate_config
from .api_monitor import api_monitor

# 設定の検証
validate_config()

def get_openai_response(prompt: str, request_type: str = "general", 
                       hypothesis_id: str = None, step_id: str = None) -> str:
    """OpenAI APIからレスポンスを取得（料金監視付き）"""
    client = OpenAI(api_key=OPENAI_API_KEY)
    response = client.chat.completions.create(
        model=OPENAI_MODEL,
        messages=[{"role": "user", "content": prompt}]
    )
    
    # 利用料金をログ
    usage = response.usage
    api_monitor.log_api_usage(
        model=OPENAI_MODEL,
        prompt_tokens=usage.prompt_tokens,
        completion_tokens=usage.completion_tokens,
        request_type=request_type,
        hypothesis_id=hypothesis_id,
        step_id=step_id
    )
    
    return response.choices[0].message.content

async def get_openai_response_async(prompt: str, request_type: str = "general",
                                  hypothesis_id: str = None, step_id: str = None) -> str:
    """OpenAI APIからレスポンスを非同期取得（レート制限対応）"""
    # レート制限を考慮した待機
    await api_monitor.wait_for_rate_limit()
    
    # 同期APIを非同期で実行
    loop = asyncio.get_event_loop()
    client = OpenAI(api_key=OPENAI_API_KEY)
    
    response = await loop.run_in_executor(
        None, 
        lambda: client.chat.completions.create(
            model=OPENAI_MODEL,
            messages=[{"role": "user", "content": prompt}]
        )
    )
    
    # 利用料金をログ
    usage = response.usage
    api_monitor.log_api_usage(
        model=OPENAI_MODEL,
        prompt_tokens=usage.prompt_tokens,
        completion_tokens=usage.completion_tokens,
        request_type=request_type,
        hypothesis_id=hypothesis_id,
        step_id=step_id
    )
    
    return response.choices[0].message.content