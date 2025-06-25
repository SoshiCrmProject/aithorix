"""
AITHORIX OKX Constants
Constants and configurations for OKX exchange
"""

from enum import Enum
from typing import Dict, List


# API URLs
class OKXUrls:
    """OKX API URLs"""
    
    # Production URLs
    REST_URL = "https://www.okx.com"
    WS_PUBLIC_URL = "wss://ws.okx.com:8443/ws/v5/public"
    WS_PRIVATE_URL = "wss://ws.okx.com:8443/ws/v5/private"
    WS_BUSINESS_URL = "wss://ws.okx.com:8443/ws/v5/business"
    
    # AWS URLs (lower latency)
    AWS_REST_URL = "https://aws.okx.com"
    AWS_WS_PUBLIC_URL = "wss://wsaws.okx.com:8443/ws/v5/public"
    AWS_WS_PRIVATE_URL = "wss://wsaws.okx.com:8443/ws/v5/private"
    
    # Demo trading URLs
    DEMO_REST_URL = "https://www.okx.com"
    DEMO_WS_PUBLIC_URL = "wss://wspap.okx.com:8443/ws/v5/public?brokerId=9999"
    DEMO_WS_PRIVATE_URL = "wss://wspap.okx.com:8443/ws/v5/private?brokerId=9999"


# Instrument types
class OKXInstType(Enum):
    """OKX instrument types"""
    SPOT = "SPOT"
    MARGIN = "MARGIN"
    SWAP = "SWAP"
    FUTURES = "FUTURES"
    OPTION = "OPTION"
    ANY = "ANY"  # For filtering


# Order types
class OKXOrderType(Enum):
    """OKX order types"""
    MARKET = "market"
    LIMIT = "limit"
    POST_ONLY = "post_only"
    FOK = "fok"
    IOC = "ioc"
    OPTIMAL_LIMIT_IOC = "optimal_limit_ioc"


# Trade modes
class OKXTradeMode(Enum):
    """OKX trade modes"""
    CASH = "cash"           # Spot
    ISOLATED = "isolated"   # Isolated margin
    CROSS = "cross"         # Cross margin
    FUTURES = "futures"     # Futures (deprecated, use cross)


# Position modes
class OKXPositionMode(Enum):
    """OKX position modes"""
    NET_MODE = "net_mode"           # Net position mode
    LONG_SHORT_MODE = "long_short_mode"  # Long/short position mode


# Position sides
class OKXPositionSide(Enum):
    """OKX position sides"""
    LONG = "long"
    SHORT = "short"
    NET = "net"  # For net mode


# Margin modes
class OKXMarginMode(Enum):
    """OKX margin modes"""
    CROSS = "cross"
    ISOLATED = "isolated"


# Account levels
class OKXAccountLevel(Enum):
    """OKX account levels"""
    SIMPLE = "1"                # Simple
    SINGLE_CURRENCY = "2"       # Single-currency margin
    MULTI_CURRENCY = "3"        # Multi-currency margin
    PORTFOLIO_MARGIN = "4"      # Portfolio margin


# Order states
class OKXOrderState(Enum):
    """OKX order states"""
    LIVE = "live"
    PARTIALLY_FILLED = "partially_filled"
    FILLED = "filled"
    CANCELLED = "canceled"
    MMP_CANCELED = "mmp_canceled"  # Market maker protection


# Algo order types
class OKXAlgoOrderType(Enum):
    """OKX algo order types"""
    CONDITIONAL = "conditional"         # Stop order
    OCO = "oco"                        # One-cancels-the-other
    TRIGGER = "trigger"                # Trigger order
    MOVE_ORDER_STOP = "move_order_stop"  # Trailing order
    ICEBERG = "iceberg"                # Iceberg order
    TWAP = "twap"                      # Time-weighted average price


# Greeks types
class OKXGreeksType(Enum):
    """OKX greeks types"""
    PA = "PA"   # Greeks in coins
    BS = "BS"   # Black-Scholes greeks in dollars


# Channels
OKX_PUBLIC_CHANNELS = [
    "instruments",      # Instrument updates
    "tickers",         # Ticker updates
    "open-interest",   # Open interest
    "candle1m",        # 1-minute candles
    "candle3m",        # 3-minute candles
    "candle5m",        # 5-minute candles
    "candle15m",       # 15-minute candles
    "candle30m",       # 30-minute candles
    "candle1H",        # 1-hour candles
    "candle2H",        # 2-hour candles
    "candle4H",        # 4-hour candles
    "candle6H",        # 6-hour candles
    "candle12H",       # 12-hour candles
    "candle1D",        # Daily candles
    "candle2D",        # 2-day candles
    "candle3D",        # 3-day candles
    "candle1W",        # Weekly candles
    "candle1M",        # Monthly candles
    "candle3M",        # 3-month candles
    "trades",          # Trade updates
    "books",           # Order book (400 depth, snapshot)
    "books5",          # Order book (5 depth)
    "books-l2-tbt",    # Order book (tick-by-tick, 400 depth)
    "books50-l2-tbt",  # Order book (tick-by-tick, 50 depth)
    "opt-summary",     # Option summary
    "funding-rate",    # Funding rate
    "index-tickers",   # Index tickers
    "status",          # System status
    "liquidation-orders"  # Liquidation orders
]

OKX_PRIVATE_CHANNELS = [
    "account",             # Account updates
    "positions",           # Position updates
    "balance_and_position", # Balance and position updates
    "orders",              # Order updates
    "orders-algo",         # Algo order updates
    "algo-advance",        # Advanced algo updates
    "liquidation-warning", # Liquidation warning
    "account-greeks",      # Account greeks
    "rfqs",               # RFQ updates
    "quotes",             # Quote updates
    "struc-block-trades", # Structured block trades
    "spot-grid-orders",   # Spot grid orders
    "grid-orders-contract", # Contract grid orders
    "grid-positions",     # Grid positions
    "grid-sub-orders"     # Grid sub-orders
]

# Rate limits
OKX_RATE_LIMITS = {
    "REST": {
        "GET": 20,      # requests per 2 seconds
        "POST": 20,     # requests per 2 seconds  
        "orders": 60,   # orders per 2 seconds
        "algo_orders": 20  # algo orders per 2 seconds
    },
    "WebSocket": {
        "connections": 20,      # max connections per IP
        "subscriptions": 240,   # max subscriptions per connection
        "messages": 100         # messages per second
    }
}

# Trading rules
OKX_TRADING_RULES = {
    "min_order_size": {
        "BTC": 0.00001,
        "ETH": 0.001,
        "default": 1
    },
    "max_order_size": {
        "default": 10000000
    },
    "max_orders": {
        "spot": 200,
        "futures": 200,
        "total": 500
    },
    "max_algo_orders": {
        "conditional": 200,
        "total": 200
    }
}

# Fee rates (default, actual rates from API)
OKX_FEE_RATES = {
    "spot": {
        "maker": -0.0001,  # -0.01% (rebate)
        "taker": 0.0015    # 0.15%
    },
    "futures": {
        "maker": 0.0002,   # 0.02%
        "taker": 0.0005    # 0.05%
    },
    "option": {
        "maker": 0.0002,   # 0.02%
        "taker": 0.0003    # 0.03%
    }
}

# Leverage tiers
OKX_LEVERAGE_TIERS = {
    "BTC-USDT": [
        {"min": 0, "max": 50000, "max_leverage": 125},
        {"min": 50000, "max": 100000, "max_leverage": 100},
        {"min": 100000, "max": 200000, "max_leverage": 50},
        {"min": 200000, "max": 500000, "max_leverage": 20},
        {"min": 500000, "max": float('inf'), "max_leverage": 10}
    ],
    "ETH-USDT": [
        {"min": 0, "max": 100000, "max_leverage": 100},
        {"min": 100000, "max": 200000, "max_leverage": 50},
        {"min": 200000, "max": 500000, "max_leverage": 20},
        {"min": 500000, "max": float('inf'), "max_leverage": 10}
    ],
    "default": [
        {"min": 0, "max": 100000, "max_leverage": 75},
        {"min": 100000, "max": 500000, "max_leverage": 50},
        {"min": 500000, "max": float('inf'), "max_leverage": 20}
    ]
}

# Error codes
OKX_ERROR_CODES = {
    # General errors (0-999)
    "0": "Success",
    "1": "Operation failed",
    "2": "Bulk operation partially succeeded",
    
    # API errors (50000-53999)
    "50000": "Body cannot be empty",
    "50001": "Service temporarily unavailable",
    "50002": "JSON data format error",
    "50004": "Endpoint request timeout",
    "50005": "API is offline or unavailable",
    "50006": "Invalid Content-Type",
    "50007": "Account blocked",
    "50008": "User does not exist",
    "50009": "Account is suspended",
    "50010": "User ID cannot be empty",
    "50011": "Rate limit exceeded",
    "50013": "System busy",
    "50014": "Invalid parameter",
    "50015": "Invalid parameter type",
    "50016": "Required parameter missing",
    "50024": "Parameter value out of range",
    "50025": "Parameter length exceeds limit",
    "50026": "System error",
    "50027": "Account restricted",
    "50028": "Unable to bind multiple IPs",
    
    # Trading errors (51000-51999)
    "51000": "Parameter verification failed",
    "51001": "Instrument ID does not exist",
    "51002": "Instrument ID not tradeable",
    "51003": "Invalid operation for position side",
    "51004": "Order failed, insufficient balance",
    "51005": "Order amount is less than minimum",
    "51006": "Order amount exceeds maximum",
    "51007": "Account status invalid",
    "51008": "Order failed, insufficient position",
    "51009": "Order frozen due to close position",
    "51010": "Operation not supported under current account mode",
    "51011": "Order price is not within limit",
    "51012": "Invalid order price step",
    "51013": "Price exceeds limit",
    "51014": "Order type not supported",
    "51015": "Leverage exceeds maximum",
    "51016": "Order failed, margin required exceeds account balance",
    "51017": "Pending orders exceed limit",
    "51018": "Reduce-only order failed",
    "51019": "Order failed, available margin is 0",
    "51020": "Order amount should be multiple of contract value",
    
    # Other error ranges
    # 52000-52999: Data errors
    # 54000-54999: WebSocket errors
    # 58000-58999: Account errors
    # 59000-59999: Position errors
}

# Currency pairs commonly traded
OKX_MAJOR_PAIRS = [
    # Spot pairs
    "BTC-USDT", "ETH-USDT", "SOL-USDT", "XRP-USDT", "ADA-USDT",
    "MATIC-USDT", "DOGE-USDT", "DOT-USDT", "AVAX-USDT", "LINK-USDT",
    "UNI-USDT", "ATOM-USDT", "LTC-USDT", "ETC-USDT", "FIL-USDT",
    
    # Perpetual swaps
    "BTC-USDT-SWAP", "ETH-USDT-SWAP", "SOL-USDT-SWAP", "XRP-USDT-SWAP",
    "ADA-USDT-SWAP", "MATIC-USDT-SWAP", "DOGE-USDT-SWAP", "DOT-USDT-SWAP",
    
    # Inverse perpetuals
    "BTC-USD-SWAP", "ETH-USD-SWAP", "LTC-USD-SWAP", "XRP-USD-SWAP",
    
    # Futures (quarterly)
    "BTC-USD-230331", "ETH-USD-230331",  # Example dates
]

# Option types
OKX_OPTION_TYPES = {
    "C": "Call",
    "P": "Put"
}

# Contract types
OKX_CONTRACT_TYPES = {
    "linear": "Linear (USDT margined)",
    "inverse": "Inverse (Coin margined)"
}