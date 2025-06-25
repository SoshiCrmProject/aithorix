"""
AITHORIX Hyperliquid Exceptions
Custom exceptions for Hyperliquid exchange operations
"""

from typing import Optional, Dict, Any


class HyperliquidException(Exception):
    """Base exception for Hyperliquid operations"""
    
    def __init__(self, message: str, code: Optional[str] = None, 
                 details: Optional[Dict[str, Any]] = None):
        super().__init__(message)
        self.message = message
        self.code = code
        self.details = details or {}
    
    def __str__(self):
        if self.code:
            return f"[{self.code}] {self.message}"
        return self.message


class HyperliquidAPIException(HyperliquidException):
    """Exception for API-related errors"""
    
    def __init__(self, message: str, status_code: Optional[int] = None,
                 response_data: Optional[Dict[str, Any]] = None):
        super().__init__(message)
        self.status_code = status_code
        self.response_data = response_data or {}


class HyperliquidAuthenticationException(HyperliquidException):
    """Exception for authentication failures"""
    pass


class HyperliquidSignatureException(HyperliquidException):
    """Exception for signature-related errors"""
    pass


class HyperliquidRateLimitException(HyperliquidException):
    """Exception for rate limit violations"""
    
    def __init__(self, message: str, retry_after: Optional[int] = None):
        super().__init__(message, code="RATE_LIMIT_EXCEEDED")
        self.retry_after = retry_after


class HyperliquidOrderException(HyperliquidException):
    """Exception for order-related errors"""
    
    def __init__(self, message: str, order_id: Optional[str] = None,
                 reason: Optional[str] = None):
        super().__init__(message)
        self.order_id = order_id
        self.reason = reason
        self.details = {
            "order_id": order_id,
            "reason": reason
        }


class HyperliquidInsufficientBalanceException(HyperliquidOrderException):
    """Exception for insufficient balance"""
    
    def __init__(self, required: float, available: float, asset: str = "USDC"):
        message = f"Insufficient {asset} balance. Required: {required}, Available: {available}"
        super().__init__(message, reason="INSUFFICIENT_BALANCE")
        self.required = required
        self.available = available
        self.asset = asset


class HyperliquidPositionException(HyperliquidException):
    """Exception for position-related errors"""
    
    def __init__(self, message: str, symbol: Optional[str] = None,
                 position_size: Optional[float] = None):
        super().__init__(message)
        self.symbol = symbol
        self.position_size = position_size
        self.details = {
            "symbol": symbol,
            "position_size": position_size
        }


class HyperliquidLiquidationException(HyperliquidPositionException):
    """Exception for liquidation-related errors"""
    
    def __init__(self, symbol: str, liquidation_price: float, 
                 current_price: float, margin_ratio: float):
        message = (f"Position {symbol} near liquidation. "
                  f"Liquidation price: {liquidation_price}, "
                  f"Current price: {current_price}, "
                  f"Margin ratio: {margin_ratio}%")
        super().__init__(message, symbol=symbol)
        self.liquidation_price = liquidation_price
        self.current_price = current_price
        self.margin_ratio = margin_ratio


class HyperliquidWebSocketException(HyperliquidException):
    """Exception for WebSocket-related errors"""
    pass


class HyperliquidConnectionException(HyperliquidException):
    """Exception for connection-related errors"""
    
    def __init__(self, message: str, retry_count: int = 0):
        super().__init__(message)
        self.retry_count = retry_count


class HyperliquidTimeoutException(HyperliquidException):
    """Exception for timeout errors"""
    
    def __init__(self, message: str, timeout_seconds: float):
        super().__init__(message)
        self.timeout_seconds = timeout_seconds


class HyperliquidDataException(HyperliquidException):
    """Exception for data parsing/validation errors"""
    pass


class HyperliquidConfigurationException(HyperliquidException):
    """Exception for configuration errors"""
    pass


# Exception Factory
class HyperliquidExceptionFactory:
    """Factory for creating appropriate exceptions from API responses"""
    
    @staticmethod
    def from_response(response_data: Dict[str, Any]) -> HyperliquidException:
        """Create exception from API response"""
        error_msg = response_data.get('error', 'Unknown error')
        error_code = response_data.get('code')
        
        # Map specific error messages to exception types
        if 'insufficient' in error_msg.lower() and 'balance' in error_msg.lower():
            # Parse balance info if available
            return HyperliquidInsufficientBalanceException(
                required=0, available=0  # Would parse from response
            )
        
        elif 'rate limit' in error_msg.lower():
            return HyperliquidRateLimitException(
                message=error_msg,
                retry_after=response_data.get('retry_after')
            )
        
        elif 'authentication' in error_msg.lower() or 'unauthorized' in error_msg.lower():
            return HyperliquidAuthenticationException(error_msg)
        
        elif 'signature' in error_msg.lower():
            return HyperliquidSignatureException(error_msg)
        
        elif 'order' in error_msg.lower():
            return HyperliquidOrderException(
                message=error_msg,
                order_id=response_data.get('order_id'),
                reason=error_code
            )
        
        elif 'position' in error_msg.lower():
            return HyperliquidPositionException(
                message=error_msg,
                symbol=response_data.get('symbol')
            )
        
        elif 'timeout' in error_msg.lower():
            return HyperliquidTimeoutException(
                message=error_msg,
                timeout_seconds=response_data.get('timeout', 30)
            )
        
        else:
            # Default to base exception
            return HyperliquidException(
                message=error_msg,
                code=error_code,
                details=response_data
            )


# Utility functions for exception handling
def handle_hyperliquid_error(func):
    """Decorator for handling Hyperliquid exceptions"""
    import functools
    import logging
    
    logger = logging.getLogger(__name__)
    
    @functools.wraps(func)
    async def wrapper(*args, **kwargs):
        try:
            return await func(*args, **kwargs)
        except HyperliquidRateLimitException as e:
            logger.warning(f"Rate limit hit: {e}. Retry after {e.retry_after}s")
            raise
        except HyperliquidAuthenticationException as e:
            logger.error(f"Authentication failed: {e}")
            raise
        except HyperliquidOrderException as e:
            logger.error(f"Order error: {e}")
            raise
        except HyperliquidException as e:
            logger.error(f"Hyperliquid error: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error in {func.__name__}: {e}")
            raise HyperliquidException(f"Unexpected error: {str(e)}")
    
    return wrapper