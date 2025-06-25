"""
AITHORIX MEXC Exceptions
Custom exceptions for MEXC exchange operations
"""

from typing import Optional, Dict, Any


class MEXCException(Exception):
    """Base exception for MEXC operations"""
    
    def __init__(self, message: str, code: Optional[int] = None, 
                 response: Optional[Dict[str, Any]] = None):
        super().__init__(message)
        self.code = code
        self.response = response


class MEXCAPIException(MEXCException):
    """MEXC API error"""
    pass


class MEXCOrderException(MEXCException):
    """MEXC order-related error"""
    pass


class MEXCInsufficientBalance(MEXCOrderException):
    """Insufficient balance for operation"""
    pass


class MEXCOrderNotFound(MEXCOrderException):
    """Order not found"""
    pass


class MEXCSymbolNotFound(MEXCException):
    """Symbol not found or not tradeable"""
    pass


class MEXCRateLimitException(MEXCException):
    """Rate limit exceeded"""
    
    def __init__(self, message: str, retry_after: Optional[int] = None):
        super().__init__(message)
        self.retry_after = retry_after


class MEXCMaintenanceException(MEXCException):
    """Exchange under maintenance"""
    pass


class MEXCAuthenticationException(MEXCException):
    """Authentication failed"""
    pass


class MEXCPermissionException(MEXCException):
    """Insufficient permissions"""
    pass


class MEXCValidationException(MEXCException):
    """Request validation failed"""
    pass


class MEXCWebSocketException(MEXCException):
    """WebSocket-related error"""
    pass


class MEXCConnectionException(MEXCException):
    """Connection error"""
    pass


# Error code mapping
ERROR_CODES = {
    # General errors
    10000: "Unknown error",
    10001: "Internal server error",
    10002: "Service unavailable",
    10003: "Service timeout",
    10004: "Request rate limit exceeded",
    10005: "IP rate limit exceeded",
    
    # Authentication errors
    10006: "Invalid API key",
    10007: "Invalid signature",
    10008: "Invalid timestamp",
    10009: "API key not activated",
    10010: "Insufficient permissions",
    10011: "IP not whitelisted",
    
    # Trading errors
    20001: "Insufficient balance",
    20002: "Order not found",
    20003: "Order already filled",
    20004: "Order already cancelled",
    20005: "Invalid order type",
    20006: "Invalid order side",
    20007: "Invalid order quantity",
    20008: "Invalid order price",
    20009: "Invalid symbol",
    20010: "Symbol not tradeable",
    20011: "Market order not supported",
    20012: "Order quantity too small",
    20013: "Order quantity too large",
    20014: "Order price out of range",
    20015: "Position not found",
    20016: "Position already closed",
    20017: "Invalid leverage",
    20018: "Risk limit exceeded",
    
    # Account errors
    30001: "Account suspended",
    30002: "Account not activated",
    30003: "KYC required",
    30004: "Withdrawal suspended",
    30005: "Trading suspended",
    
    # Contract errors
    40001: "Contract not found",
    40002: "Contract expired",
    40003: "Contract not tradeable",
    40004: "Invalid contract size",
    40005: "Funding rate not available",
    40006: "Mark price not available"
}


def get_exception_from_error_code(code: int, message: Optional[str] = None,
                                  response: Optional[Dict[str, Any]] = None) -> MEXCException:
    """Get appropriate exception based on error code"""
    
    error_message = message or ERROR_CODES.get(code, f"Unknown error code: {code}")
    
    # Map error codes to exception types
    if code in [10006, 10007, 10008, 10009, 10010, 10011]:
        return MEXCAuthenticationException(error_message, code, response)
    elif code in [10004, 10005]:
        return MEXCRateLimitException(error_message)
    elif code in [20001]:
        return MEXCInsufficientBalance(error_message, code, response)
    elif code in [20002, 20015]:
        return MEXCOrderNotFound(error_message, code, response)
    elif code in [20009, 20010]:
        return MEXCSymbolNotFound(error_message, code, response)
    elif code in range(20001, 20019):
        return MEXCOrderException(error_message, code, response)
    elif code in [10002, 10003]:
        return MEXCMaintenanceException(error_message, code, response)
    elif code in range(30001, 30006):
        return MEXCPermissionException(error_message, code, response)
    else:
        return MEXCAPIException(error_message, code, response)