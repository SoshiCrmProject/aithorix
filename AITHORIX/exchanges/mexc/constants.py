"""
AITHORIX MEXC Constants
Constants and configurations for MEXC exchange
"""

from enum import Enum
from typing import Dict, List


# API URLs
class MEXCUrls:
    """MEXC API URLs"""
    
    # Production URLs
    REST_URL = "https://api.mexc.com"
    WS_URL = "wss://wbs.mexc.com/ws"
    CONTRACT_WS_URL = "wss://contract.mexc.com/ws"
    
    # Testnet URLs
    TESTNET_REST_URL = "https://sandbox-api.mexc.com"
    TESTNET_WS_URL = "wss://sandbox-ws.mexc.com/ws"
    TESTNET_CONTRACT_WS_URL = "wss://sandbox-contract.mexc.com/ws"
    
    # API endpoints
    SPOT_API_V3 = "/api/v3"
    CONTRACT_API_V1 = "/contract/v1"
    ETFSWAP_API_V1 = "/api/v1/etfswap"


# Trading constants
class MEXCOrderType(Enum):
    """MEXC order types"""
    LIMIT = 1
    MARKET = 5
    STOP = 3
    STOP_LIMIT = 4
    POST_ONLY = 1  # Limit order with post-only flag
    IOC = 2  # Immediate or Cancel
    FOK = 6  # Fill or Kill


class MEXCOrderSide(Enum):
    """MEXC order sides for futures"""
    OPEN_LONG = 1
    CLOSE_SHORT = 2
    OPEN_SHORT = 3
    CLOSE_LONG = 4


class MEXCOrderStatus(Enum):
    """MEXC order status"""
    NOT_TRIGGERED = 1
    NEW = 2
    PARTIALLY_FILLED = 3
    FILLED = 4
    CANCELED = 5
    PARTIALLY_CANCELED = 6
    CANCELED_BY_SYSTEM = 7


class MEXCPositionType(Enum):
    """MEXC position types"""
    LONG = 1
    SHORT = 2


class MEXCMarginMode(Enum):
    """MEXC margin modes"""
    ISOLATED = 1
    CROSS = 2


class MEXCTriggerType(Enum):
    """MEXC trigger price types"""
    LAST_PRICE = 1
    INDEX_PRICE = 2
    MARK_PRICE = 3


# Symbol constants
MEXC_QUOTE_ASSETS = ['USDT', 'USDC', 'BTC', 'ETH', 'MX']
MEXC_STABLE_COINS = ['USDT', 'USDC', 'BUSD', 'DAI', 'TUSD']

# Minimum order sizes (approximate, check symbol info for exact values)
MEXC_MIN_ORDER_VALUES = {
    'USDT': 5.0,
    'BTC': 0.0001,
    'ETH': 0.001,
    'MX': 10.0
}

# Rate limits
MEXC_RATE_LIMITS = {
    'spot': {
        'public': 20,  # requests per second
        'private': 10,  # requests per second
        'order': 10    # orders per second
    },
    'futures': {
        'public': 100,  # requests per second
        'private': 50,  # requests per second
        'order': 50     # orders per second
    }
}

# WebSocket limits
MEXC_WS_LIMITS = {
    'max_subscriptions_per_connection': 50,
    'max_connections': 5,
    'ping_interval': 20,  # seconds
    'reconnect_delay': 5,  # seconds
    'max_reconnect_attempts': 5
}

# Time constants
MEXC_TIME_IN_FORCE = {
    'GTC': 'GTC',  # Good Till Cancel
    'IOC': 'IOC',  # Immediate or Cancel
    'FOK': 'FOK',  # Fill or Kill
    'GTX': 'GTX'   # Good Till Crossing (Post Only)
}

# Kline intervals
MEXC_KLINE_INTERVALS = {
    '1m': '1m',
    '5m': '5m',
    '15m': '15m',
    '30m': '30m',
    '1h': '60m',
    '4h': '4h',
    '1d': '1d',
    '1w': '1W',
    '1M': '1M'
}

# Contract kline intervals
MEXC_CONTRACT_KLINE_INTERVALS = {
    '1m': 'Min1',
    '5m': 'Min5',
    '15m': 'Min15',
    '30m': 'Min30',
    '1h': 'Min60',
    '4h': 'Hour4',
    '8h': 'Hour8',
    '1d': 'Day1',
    '1w': 'Week1',
    '1M': 'Month1'
}

# Fee rates (default, check VIP level for actual rates)
MEXC_FEE_RATES = {
    'spot': {
        'maker': 0.002,  # 0.2%
        'taker': 0.002,  # 0.2%
        'mx_discount': 0.8  # 20% discount with MX
    },
    'futures': {
        'maker': 0.0002,  # 0.02%
        'taker': 0.0006   # 0.06%
    }
}

# Contract specifications
MEXC_CONTRACT_SPECS = {
    'BTC_USDT': {
        'contract_size': 0.0001,
        'tick_size': 0.1,
        'min_volume': 1,
        'max_leverage': 125
    },
    'ETH_USDT': {
        'contract_size': 0.001,
        'tick_size': 0.01,
        'min_volume': 1,
        'max_leverage': 100
    },
    'DEFAULT': {
        'contract_size': 1,
        'tick_size': 0.0001,
        'min_volume': 1,
        'max_leverage': 50
    }
}

# WebSocket channels
MEXC_WS_CHANNELS = {
    'spot': {
        'ticker': 'spot@public.bookTicker.v3.api',
        'depth': 'spot@public.limit.depth.v3.api',
        'trades': 'spot@public.deals.v3.api',
        'kline': 'spot@public.kline.v3.api',
        'account': 'spot@private.account.v3.api',
        'orders': 'spot@private.orders.v3.api',
        'deals': 'spot@private.deals.v3.api'
    },
    'futures': {
        'ticker': 'ticker',
        'depth': 'depth',
        'deals': 'deal',
        'kline': 'kline',
        'index': 'index_price',
        'funding': 'funding_rate',
        'account': 'personal.assets',
        'orders': 'personal.order',
        'positions': 'personal.position'
    }
}

# Error message patterns
MEXC_ERROR_PATTERNS = {
    'insufficient_balance': ['insufficient', 'not enough', 'balance'],
    'order_not_found': ['order not found', 'order does not exist'],
    'symbol_not_found': ['symbol not found', 'invalid symbol'],
    'rate_limit': ['rate limit', 'too many requests'],
    'maintenance': ['maintenance', 'system upgrade']
}

# Special symbols requiring different handling
MEXC_SPECIAL_SYMBOLS = {
    'leveraged_tokens': ['BULL', 'BEAR', '3L', '3S', '5L', '5S'],
    'etf': ['ETF'],
    'test_symbols': ['TEST', 'DEMO']
}