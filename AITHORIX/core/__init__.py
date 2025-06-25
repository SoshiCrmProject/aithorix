"""
AITHORIX Core Module
Advanced Trading Intelligence System - Core Components

This module provides the foundational trading infrastructure including:
- Trading Engine: Central orchestration of all trading operations
- Order Management: Sophisticated order execution and management
- Position Management: Real-time position tracking and risk control
- Market Data: High-performance market data ingestion and processing
- Risk Engine: Real-time risk assessment and protection
- Execution Engine: Ultra-low latency order execution

Copyright (c) 2024 AITHORIX. All rights reserved.
"""

__version__ = "1.0.0"
__author__ = "AITHORIX Development Team"

# Core component imports
from .engine.trading_engine import TradingEngine
from .engine.order_manager import OrderManager
from .engine.position_manager import PositionManager
from .engine.market_data import MarketDataEngine
from .engine.risk_engine import RiskEngine
from .engine.execution_engine import ExecutionEngine

# Coordinator imports
from .coordinator.strategy_coordinator import StrategyCoordinator
from .coordinator.model_coordinator import ModelCoordinator
from .coordinator.exchange_coordinator import ExchangeCoordinator

# API imports
from .api.rest_api import create_app
from .api.endpoints import router
from .api.middleware import SecurityMiddleware, RateLimitMiddleware

# WebSocket imports
from .websocket.ws_server import WebSocketServer
from .websocket.ws_client import WebSocketClient
from .websocket.ws_handlers import WebSocketHandlers

# Authentication imports
from .auth.authentication import AuthenticationManager
from .auth.authorization import AuthorizationManager
from .auth.jwt_handler import JWTHandler

# Executor imports
from .executor.order_executor import OrderExecutor
from .executor.position_executor import PositionExecutor
from .executor.trade_executor import TradeExecutor

__all__ = [
    # Version info
    "__version__",
    "__author__",
    
    # Core engines
    "TradingEngine",
    "OrderManager",
    "PositionManager",
    "MarketDataEngine",
    "RiskEngine",
    "ExecutionEngine",
    
    # Coordinators
    "StrategyCoordinator",
    "ModelCoordinator",
    "ExchangeCoordinator",
    
    # API components
    "create_app",
    "router",
    "SecurityMiddleware",
    "RateLimitMiddleware",
    
    # WebSocket components
    "WebSocketServer",
    "WebSocketClient",
    "WebSocketHandlers",
    
    # Authentication
    "AuthenticationManager",
    "AuthorizationManager",
    "JWTHandler",
    
    # Executors
    "OrderExecutor",
    "PositionExecutor",
    "TradeExecutor",
]