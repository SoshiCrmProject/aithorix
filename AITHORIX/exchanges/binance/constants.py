"""Binance specific constants"""

# API endpoints
SPOT_BASE_URL = "https://api.binance.com"
FUTURES_BASE_URL = "https://fapi.binance.com"
TESTNET_SPOT_URL = "https://testnet.binance.vision"
TESTNET_FUTURES_URL = "https://testnet.binancefuture.com"

# WebSocket endpoints
SPOT_WS_URL = "wss://stream.binance.com:9443/ws"
FUTURES_WS_URL = "wss://fstream.binance.com/ws"
TESTNET_SPOT_WS = "wss://testnet.binance.vision/ws"
TESTNET_FUTURES_WS = "wss://testnet.binancefuture.com/ws"

# Rate limits
SPOT_REQUEST_LIMIT = 1200  # per minute
SPOT_ORDER_LIMIT = 50     # per 10 seconds
FUTURES_REQUEST_LIMIT = 2400  # per minute
WEIGHT_LIMIT = 6000       # per minute

# Order types
ORDER_TYPES = [
    "LIMIT",
    "MARKET",
    "STOP_LOSS",
    "STOP_LOSS_LIMIT",
    "TAKE_PROFIT",
    "TAKE_PROFIT_LIMIT",
    "LIMIT_MAKER"
]

# Time in force
TIME_IN_FORCE = ["GTC", "IOC", "FOK", "GTX"]

# Kline intervals
KLINE_INTERVALS = [
    "1m", "3m", "5m", "15m", "30m",
    "1h", "2h", "4h", "6h", "8h", "12h",
    "1d", "3d", "1w", "1M"
]
