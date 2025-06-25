"""
AITHORIX Exchange Exceptions
Common exceptions for all exchanges
"""

from typing import Optional, Dict, Any


class ExchangeException(Exception):
    """Base exception for all exchange operations"""
    
    def __init__(self, message: str, exchange: str = "Unknown", 
                 code: Optional[str] = None, response: Optional[Dict[str, Any]] = None):
        super().__init__(message)
        self.exchange = exchange
        self.code = code
        self.response = response


class AuthenticationException(ExchangeException):
    """Authentication or permission error"""
    pass


class InvalidAPIKeyException(AuthenticationException):
    """Invalid API key"""
    pass


class InvalidSignatureException(AuthenticationException):
    """Invalid request signature"""
    pass


class PermissionDeniedException(AuthenticationException):
    """Insufficient permissions for operation"""
    pass


class RateLimitException(ExchangeException):
    """Rate limit exceeded"""
    
    def __init__(self, message: str, exchange: str = "Unknown", 
                 retry_after: Optional[int] = None, **kwargs):
        super().__init__(message, exchange, **kwargs)
        self.retry_after = retry_after


class OrderException(ExchangeException):
    """Order-related error"""
    pass


class InsufficientBalanceException(OrderException):
    """Insufficient balance for order"""
    pass


class InvalidOrderException(OrderException):
    """Invalid order parameters"""
    pass


class OrderNotFilledException(OrderException):
    """Order not filled"""
    pass


class OrderAlreadyCancelledException(OrderException):
    """Order already cancelled"""
    pass


class OrderNotFoundException(OrderException):
    """Order not found"""
    pass


class MinOrderSizeException(OrderException):
    """Order size below minimum"""
    pass


class MaxOrderSizeException(OrderException):
    """Order size above maximum"""
    pass


class MarketException(ExchangeException):
    """Market-related error"""
    pass


class SymbolNotFoundException(MarketException):
    """Symbol not found or not tradeable"""
    pass


class MarketClosedException(MarketException):
    """Market is closed"""
    pass


class InvalidTimeframeException(MarketException):
    """Invalid timeframe for candles"""
    pass


class NetworkException(ExchangeException):
    """Network-related error"""
    pass


class TimeoutException(NetworkException):
    """Request timeout"""
    pass


class ConnectionException(NetworkException):
    """Connection error"""
    pass


class MaintenanceException(ExchangeException):
    """Exchange under maintenance"""
    pass


class ResponseException(ExchangeException):
    """Invalid response from exchange"""
    pass


class WebSocketException(ExchangeException):
    """WebSocket-related error"""
    pass


class SubscriptionException(WebSocketException):
    """Subscription error"""
    pass


# Exchange-specific exceptions
class BinanceException(ExchangeException):
    """Binance-specific exception"""
    pass


class HyperliquidException(ExchangeException):
    """Hyperliquid-specific exception"""
    pass


class MEXCException(ExchangeException):
    """MEXC-specific exception"""
    pass


class BybitException(ExchangeException):
    """Bybit-specific exception"""
    pass


class OKXException(ExchangeException):
    """OKX-specific exception"""
    pass


# Helper function to create appropriate exception from exchange response
def create_exchange_exception(
    exchange: str,
    message: str,
    code: Optional[str] = None,
    response: Optional[Dict[str, Any]] = None
) -> ExchangeException:
    """
    Create appropriate exception based on error code and message
    """
    # Common error patterns
    message_lower = message.lower()
    
    # Authentication errors
    if any(word in message_lower for word in ['api key', 'apikey', 'invalid key', 'unauthorized']):
        return InvalidAPIKeyException(message, exchange, code, response)
    
    if any(word in message_lower for word in ['signature', 'sign', 'hmac']):
        return InvalidSignatureException(message, exchange, code, response)
    
    if any(word in message_lower for word in ['permission', 'forbidden', 'not allowed']):
        return PermissionDeniedException(message, exchange, code, response)
    
    # Rate limiting
    if any(word in message_lower for word in ['rate limit', 'too many requests', 'exceeded']):
        return RateLimitException(message, exchange, code=code, response=response)
    
    # Order errors
    if any(word in message_lower for word in ['insufficient balance', 'not enough']):
        return InsufficientBalanceException(message, exchange, code, response)
    
    if 'order not found' in message_lower:
        return OrderNotFoundException(message, exchange, code, response)
    
    if any(word in message_lower for word in ['already cancelled', 'already canceled']):
        return OrderAlreadyCancelledException(message, exchange, code, response)
    
    if any(word in message_lower for word in ['min size', 'minimum size', 'too small']):
        return MinOrderSizeException(message, exchange, code, response)
    
    if any(word in message_lower for word in ['max size', 'maximum size', 'too large']):
        return MaxOrderSizeException(message, exchange, code, response)
    
    # Market errors
    if any(word in message_lower for word in ['symbol not found', 'invalid symbol']):
        return SymbolNotFoundException(message, exchange, code, response)
    
    if any(word in message_lower for word in ['market closed', 'trading halted']):
        return MarketClosedException(message, exchange, code, response)
    
    # Network errors
    if any(word in message_lower for word in ['timeout', 'timed out']):
        return TimeoutException(message, exchange, code, response)
    
    if any(word in message_lower for word in ['connection', 'network']):
        return ConnectionException(message, exchange, code, response)
    
    # Maintenance
    if any(word in message_lower for word in ['maintenance', 'upgrading', 'unavailable']):
        return MaintenanceException(message, exchange, code, response)
    
    # Exchange-specific
    exchange_exceptions = {
        'binance': BinanceException,
        'hyperliquid': HyperliquidException,
        'mexc': MEXCException,
        'bybit': BybitException,
        'okx': OKXException
    }
    
    exception_class = exchange_exceptions.get(exchange.lower(), ExchangeException)
    return exception_class(message, exchange, code, response)