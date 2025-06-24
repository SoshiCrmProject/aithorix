"""
AITHORIX Core System
Production-ready AI trading system
"""

__version__ = "1.0.0"
__author__ = "AITHORIX Team"

from .engine.trading_engine import TradingEngine
from .exceptions import (
    AithorixException,
    InsufficientBalanceError,
    OrderExecutionError,
    RiskLimitExceededError,
    ModelInferenceError
)

__all__ = [
    "TradingEngine",
    "AithorixException",
    "InsufficientBalanceError",
    "OrderExecutionError",
    "RiskLimitExceededError",
    "ModelInferenceError"
]
