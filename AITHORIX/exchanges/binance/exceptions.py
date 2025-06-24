"""Binance specific exceptions"""

from ...core.exceptions import ExchangeConnectionError


class BinanceAPIError(ExchangeConnectionError):
    """Binance API error"""
    
    def __init__(self, code: int, message: str):
        super().__init__("binance", f"API Error {code}: {message}")
        self.code = code
        

class BinanceOrderError(BinanceAPIError):
    """Binance order error"""
    pass
    

class BinanceRateLimitError(BinanceAPIError):
    """Binance rate limit error"""
    
    def __init__(self, retry_after: int = 60):
        super().__init__(429, "Rate limit exceeded")
        self.retry_after = retry_after
