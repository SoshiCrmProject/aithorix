"""
Order type mappings for different exchanges
"""

from typing import Dict
from ...core.engine.trading_engine import OrderType


BINANCE_ORDER_TYPES: Dict[OrderType, str] = {
    OrderType.MARKET: "MARKET",
    OrderType.LIMIT: "LIMIT",
    OrderType.STOP_LOSS: "STOP_LOSS_LIMIT",
    OrderType.TAKE_PROFIT: "TAKE_PROFIT_LIMIT",
    OrderType.TRAILING_STOP: "TRAILING_STOP_MARKET"
}

HYPERLIQUID_ORDER_TYPES: Dict[OrderType, str] = {
    OrderType.MARKET: "MARKET",
    OrderType.LIMIT: "LIMIT",
    OrderType.STOP_LOSS: "STOP_MARKET",
    OrderType.TAKE_PROFIT: "TAKE_PROFIT_MARKET",
    OrderType.TRAILING_STOP: "TRAILING_STOP"
}

MEXC_ORDER_TYPES: Dict[OrderType, str] = {
    OrderType.MARKET: "MARKET",
    OrderType.LIMIT: "LIMIT",
    OrderType.STOP_LOSS: "STOP_LIMIT",
    OrderType.TAKE_PROFIT: "TAKE_PROFIT_LIMIT",
    OrderType.TRAILING_STOP: "TRAILING_STOP"
}

BYBIT_ORDER_TYPES: Dict[OrderType, str] = {
    OrderType.MARKET: "Market",
    OrderType.LIMIT: "Limit",
    OrderType.STOP_LOSS: "StopLoss",
    OrderType.TAKE_PROFIT: "TakeProfit",
    OrderType.TRAILING_STOP: "TrailingStop"
}

OKX_ORDER_TYPES: Dict[OrderType, str] = {
    OrderType.MARKET: "market",
    OrderType.LIMIT: "limit",
    OrderType.STOP_LOSS: "stop",
    OrderType.TAKE_PROFIT: "tp",
    OrderType.TRAILING_STOP: "trailing"
}
