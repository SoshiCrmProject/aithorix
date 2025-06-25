"""
AITHORIX Exchange Module
Unified interface for all cryptocurrency exchanges
"""

from .base_exchange import (
    BaseExchange,
    ExchangeConfig,
    OrderType,
    OrderSide,
    OrderStatus,
    TimeInForce,
    Order,
    Trade,
    Position,
    Balance,
    Ticker,
    OrderBook,
    Candle
)

from .manager import ExchangeManager, ExchangeType, ExchangeStats, ArbitrageOpportunity

from .binance.client import BinanceClient
from .hyperliquid.client import HyperliquidClient
from .mexc.client import MEXCClient
from .bybit.client import BybitClient
from .okx.client import OKXClient

__all__ = [
    # Base classes
    'BaseExchange',
    'ExchangeConfig',
    'OrderType',
    'OrderSide',
    'OrderStatus',
    'TimeInForce',
    'Order',
    'Trade',
    'Position',
    'Balance',
    'Ticker',
    'OrderBook',
    'Candle',
    
    # Manager
    'ExchangeManager',
    'ExchangeType',
    'ExchangeStats',
    'ArbitrageOpportunity',
    
    # Exchange clients
    'BinanceClient',
    'HyperliquidClient',
    'MEXCClient',
    'BybitClient',
    'OKXClient'
]

__version__ = '1.0.0'