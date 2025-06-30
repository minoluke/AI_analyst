import os
import json
import time
import logging
from datetime import datetime
from typing import Dict, Optional
from dataclasses import dataclass, asdict
import asyncio
import aiofiles

@dataclass
class APIUsageLog:
    """API利用ログの構造"""
    timestamp: str
    model: str
    prompt_tokens: int
    completion_tokens: int
    total_tokens: int
    estimated_cost_usd: float
    request_type: str
    hypothesis_id: Optional[str] = None
    step_id: Optional[str] = None

class OpenAIAPIMonitor:
    """OpenAI API利用料金とリクエスト監視クラス"""
    
    # 料金表 (2024年12月時点の料金)
    PRICING = {
        "gpt-4o-mini": {
            "input": 0.00015 / 1000,  # $0.00015 per 1K input tokens
            "output": 0.0006 / 1000   # $0.0006 per 1K output tokens
        },
        "gpt-4o": {
            "input": 0.005 / 1000,    # $0.005 per 1K input tokens
            "output": 0.015 / 1000    # $0.015 per 1K output tokens
        },
        "gpt-4-turbo": {
            "input": 0.01 / 1000,     # $0.01 per 1K input tokens
            "output": 0.03 / 1000     # $0.03 per 1K output tokens
        },
        "gpt-4": {
            "input": 0.03 / 1000,     # $0.03 per 1K input tokens  
            "output": 0.06 / 1000     # $0.06 per 1K output tokens
        }
    }
    
    def __init__(self, log_file: str = "openai_usage.log"):
        self.log_file = log_file
        self.logger = logging.getLogger("OpenAIMonitor")
        self.session_start = datetime.now()
        self.session_usage = {
            "total_requests": 0,
            "total_tokens": 0,
            "total_cost_usd": 0.0
        }
        
        # ログファイル初期化
        self._setup_logging()
        
        # レート制限用のタイミング管理
        self.last_request_time = 0
        self.min_request_interval = 0.1  # 100ms間隔（10 req/sec）
    
    def _setup_logging(self):
        """ログ設定の初期化"""
        handler = logging.FileHandler(self.log_file, encoding='utf-8')
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        self.logger.addHandler(handler)
        self.logger.setLevel(logging.INFO)
    
    def calculate_cost(self, model: str, prompt_tokens: int, completion_tokens: int) -> float:
        """利用料金を計算"""
        model_key = model.lower()
        
        # モデル名の正規化
        if "gpt-4o-mini" in model_key:
            pricing = self.PRICING["gpt-4o-mini"]
        elif "gpt-4o" in model_key:
            pricing = self.PRICING["gpt-4o"]
        elif "gpt-4-turbo" in model_key or "gpt-4.1" in model_key:
            pricing = self.PRICING["gpt-4-turbo"]
        elif "gpt-4" in model_key:
            pricing = self.PRICING["gpt-4"]
        else:
            # デフォルトはGPT-4o Mini料金
            pricing = self.PRICING["gpt-4o-mini"]
            self.logger.warning(f"Unknown model {model}, using gpt-4o-mini pricing")
        
        input_cost = prompt_tokens * pricing["input"]
        output_cost = completion_tokens * pricing["output"]
        total_cost = input_cost + output_cost
        
        return total_cost
    
    async def wait_for_rate_limit(self):
        """レート制限を考慮した待機"""
        current_time = time.time()
        time_since_last = current_time - self.last_request_time
        
        if time_since_last < self.min_request_interval:
            wait_time = self.min_request_interval - time_since_last
            await asyncio.sleep(wait_time)
        
        self.last_request_time = time.time()
    
    def log_api_usage(self, 
                     model: str,
                     prompt_tokens: int, 
                     completion_tokens: int,
                     request_type: str,
                     hypothesis_id: Optional[str] = None,
                     step_id: Optional[str] = None) -> Dict:
        """API利用を記録"""
        
        total_tokens = prompt_tokens + completion_tokens
        estimated_cost = self.calculate_cost(model, prompt_tokens, completion_tokens)
        
        # セッション累計更新
        self.session_usage["total_requests"] += 1
        self.session_usage["total_tokens"] += total_tokens
        self.session_usage["total_cost_usd"] += estimated_cost
        
        # ログエントリ作成
        log_entry = APIUsageLog(
            timestamp=datetime.now().isoformat(),
            model=model,
            prompt_tokens=prompt_tokens,
            completion_tokens=completion_tokens,
            total_tokens=total_tokens,
            estimated_cost_usd=estimated_cost,
            request_type=request_type,
            hypothesis_id=hypothesis_id,
            step_id=step_id
        )
        
        # ログ出力
        self.logger.info(f"API_USAGE: {json.dumps(asdict(log_entry), ensure_ascii=False)}")
        
        # コンソール出力
        print(f"💰 API利用: {model} | {total_tokens}トークン | ${estimated_cost:.4f} | 累計: ${self.session_usage['total_cost_usd']:.4f}")
        
        return {
            "tokens": total_tokens,
            "cost": estimated_cost,
            "session_total_cost": self.session_usage["total_cost_usd"]
        }
    
    def get_session_summary(self) -> Dict:
        """セッション利用サマリーを取得"""
        duration = datetime.now() - self.session_start
        
        return {
            "session_start": self.session_start.isoformat(),
            "duration_minutes": duration.total_seconds() / 60,
            "total_requests": self.session_usage["total_requests"],
            "total_tokens": self.session_usage["total_tokens"],
            "total_cost_usd": self.session_usage["total_cost_usd"],
            "average_cost_per_request": self.session_usage["total_cost_usd"] / max(1, self.session_usage["total_requests"]),
            "tokens_per_minute": self.session_usage["total_tokens"] / max(1, duration.total_seconds() / 60)
        }
    
    async def save_session_summary(self, output_file: str = "session_summary.json"):
        """セッションサマリーをファイルに保存"""
        summary = self.get_session_summary()
        
        async with aiofiles.open(output_file, 'w', encoding='utf-8') as f:
            await f.write(json.dumps(summary, ensure_ascii=False, indent=2))
        
        self.logger.info(f"Session summary saved to {output_file}")
        return summary

# グローバルインスタンス
api_monitor = OpenAIAPIMonitor()