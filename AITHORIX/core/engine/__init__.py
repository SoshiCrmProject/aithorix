"""
AITHORIX Engine Module
Core trading engines for the AITHORIX system

This module contains the fundamental engines that power the trading system:
- TradingEngine: Central orchestrator for all trading operations
- OrderManager: Manages order lifecycle and execution
- PositionManager: Tracks and manages trading positions
- MarketDataEngine: Ingests and processes market data
- RiskEngine: Real-time risk assessment and management
- ExecutionEngine: High-performance order execution
"""

from .trading_engine import TradingEngine
from .order_manager import OrderManager
from .position_manager import PositionManager
from .market_data import MarketDataEngine
from .risk_engine import RiskEngine
from .execution_engine import ExecutionEngine

__all__ = [
    "TradingEngine",
    "OrderManager",
    "PositionManager",
    "MarketDataEngine",
    "RiskEngine",
    "ExecutionEngine",
]