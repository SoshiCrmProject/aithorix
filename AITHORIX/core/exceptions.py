"""
AITHORIX Exception Classes
Custom exceptions for the trading system
"""

from typing import Optional, Dict, Any


class AithorixException(Exception):
    """Base exception for all AITHORIX errors"""
    
    def __init__(self, message: str, error_code: Optional[str] = None, details: Optional[Dict[str, Any]] = None):
        super().__init__(message)
        self.error_code = error_code
        self.details = details or {}
        

class InsufficientBalanceError(AithorixException):
    """Raised when account balance is insufficient for trade"""
    
    def __init__(self, required: float, available: float, asset: str):
        message = f"Insufficient {asset} balance. Required: {required}, Available: {available}"
        super().__init__(message, "INSUFFICIENT_BALANCE", {
            "required": required,
            "available": available,
            "asset": asset
        })
        

class OrderExecutionError(AithorixException):
    """Raised when order execution fails"""
    
    def __init__(self, message: str, order_id: str, exchange: str):
        super().__init__(message, "ORDER_EXECUTION_FAILED", {
            "order_id": order_id,
            "exchange": exchange
        })
        

class RiskLimitExceededError(AithorixException):
    """Raised when risk limits are exceeded"""
    
    def __init__(self, limit_type: str, current_value: float, limit_value: float):
        message = f"Risk limit exceeded: {limit_type}. Current: {current_value}, Limit: {limit_value}"
        super().__init__(message, "RISK_LIMIT_EXCEEDED", {
            "limit_type": limit_type,
            "current_value": current_value,
            "limit_value": limit_value
        })
        

class ModelInferenceError(AithorixException):
    """Raised when ML model inference fails"""
    
    def __init__(self, model_id: str, reason: str):
        message = f"Model inference failed for {model_id}: {reason}"
        super().__init__(message, "MODEL_INFERENCE_FAILED", {
            "model_id": model_id,
            "reason": reason
        })
        

class ExchangeConnectionError(AithorixException):
    """Raised when exchange connection fails"""
    
    def __init__(self, exchange: str, reason: str):
        message = f"Failed to connect to {exchange}: {reason}"
        super().__init__(message, "EXCHANGE_CONNECTION_FAILED", {
            "exchange": exchange,
            "reason": reason
        })
        

class DataValidationError(AithorixException):
    """Raised when data validation fails"""
    
    def __init__(self, field: str, value: Any, expected_type: str):
        message = f"Data validation failed for {field}. Expected {expected_type}, got {type(value).__name__}"
        super().__init__(message, "DATA_VALIDATION_FAILED", {
            "field": field,
            "value": value,
            "expected_type": expected_type
        })
        

class AuthenticationError(AithorixException):
    """Raised when authentication fails"""
    
    def __init__(self, reason: str):
        super().__init__(f"Authentication failed: {reason}", "AUTHENTICATION_FAILED")
        

class AuthorizationError(AithorixException):
    """Raised when authorization fails"""
    
    def __init__(self, resource: str, action: str):
        message = f"Not authorized to {action} on {resource}"
        super().__init__(message, "AUTHORIZATION_FAILED", {
            "resource": resource,
            "action": action
        })
        

class RateLimitError(AithorixException):
    """Raised when rate limit is exceeded"""
    
    def __init__(self, limit: int, window: str, retry_after: Optional[int] = None):
        message = f"Rate limit exceeded: {limit} requests per {window}"
        super().__init__(message, "RATE_LIMIT_EXCEEDED", {
            "limit": limit,
            "window": window,
            "retry_after": retry_after
        })
        

class ConfigurationError(AithorixException):
    """Raised when configuration is invalid"""
    
    def __init__(self, config_key: str, reason: str):
        message = f"Invalid configuration for {config_key}: {reason}"
        super().__init__(message, "CONFIGURATION_ERROR", {
            "config_key": config_key,
            "reason": reason
        })
