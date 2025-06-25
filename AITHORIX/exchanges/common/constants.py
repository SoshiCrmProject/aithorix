"""
AITHORIX Common Exchange Constants
Shared constants for all exchanges
"""

# Common quote currencies
COMMON_QUOTE_CURRENCIES = [
    'USDT', 'USDC', 'BUSD', 'USD', 'EUR', 'GBP',
    'BTC', 'ETH', 'BNB', 'TUSD', 'DAI'
]

# Stable coins
STABLE_COINS = [
    'USDT', 'USDC', 'BUSD', 'TUSD', 'DAI', 'USDP',
    'USDD', 'USDN', 'USDX', 'SUSD', 'HUSD', 'GUSD',
    'PAX', 'USDK', 'USDS', 'MUSD', 'DUSD'
]

# Futures contract suffixes
FUTURES_SUFFIXES = [
    'PERP', 'SWAP', 'PERPETUAL', 'FUTURES',
    '_PERP', '-PERP', '-SWAP', '_SWAP',
    'USD', 'USDT', 'INVERSE'
]

# Common timeframes
COMMON_TIMEFRAMES = [
    '1m', '3m', '5m', '15m', '30m',
    '1h', '2h', '4h', '6h', '8h', '12h',
    '1d', '3d', '1w', '1M'
]

# Timeframe mappings for different exchanges
TIMEFRAME_MAPPINGS = {
    'binance': {
        '1m': '1m', '3m': '3m', '5m': '5m', '15m': '15m', '30m': '30m',
        '1h': '1h', '2h': '2h', '4h': '4h', '6h': '6h', '8h': '8h', '12h': '12h',
        '1d': '1d', '3d': '3d', '1w': '1w', '1M': '1M'
    },
    'hyperliquid': {
        '1m': '1m', '5m': '5m', '15m': '15m', '30m': '30m',
        '1h': '1h', '4h': '4h', '1d': '1d'
    },
    'mexc': {
        '1m': '1m', '5m': '5m', '15m': '15m', '30m': '30m',
        '1h': '60m', '4h': '4h', '1d': '1d', '1w': '1W', '1M': '1M'
    },
    'bybit': {
        '1m': '1', '3m': '3', '5m': '5', '15m': '15', '30m': '30',
        '1h': '60', '2h': '120', '4h': '240', '6h': '360', '12h': '720',
        '1d': 'D', '1w': 'W', '1M': 'M'
    },
    'okx': {
        '1m': '1m', '3m': '3m', '5m': '5m', '15m': '15m', '30m': '30m',
        '1h': '1H', '2h': '2H', '4h': '4H', '6h': '6H', '12h': '12H',
        '1d': '1D', '2d': '2D', '3d': '3D', '1w': '1W', '1M': '1M', '3M': '3M'
    }
}

# Default API limits
DEFAULT_LIMITS = {
    'ticker': 24,           # 24 hour ticker
    'orderbook': 100,       # Order book depth
    'trades': 500,          # Recent trades
    'candles': 500,         # Historical candles
    'orders': 500,          # Open orders
    'order_history': 500,   # Order history
    'positions': 100,       # Open positions
    'balances': 100         # Account balances
}

# Order types mapping
ORDER_TYPE_MAPPING = {
    'market': ['MARKET', 'market', 'Market'],
    'limit': ['LIMIT', 'limit', 'Limit'],
    'stop': ['STOP', 'stop', 'Stop', 'STOP_LOSS'],
    'stop_limit': ['STOP_LIMIT', 'stop_limit', 'StopLimit', 'STOP_LOSS_LIMIT'],
    'trailing_stop': ['TRAILING_STOP', 'trailing_stop', 'TrailingStop'],
    'post_only': ['POST_ONLY', 'post_only', 'PostOnly', 'LIMIT_MAKER'],
    'fok': ['FOK', 'fok', 'FillOrKill', 'FILL_OR_KILL'],
    'ioc': ['IOC', 'ioc', 'ImmediateOrCancel', 'IMMEDIATE_OR_CANCEL']
}

# Position sides
POSITION_SIDES = {
    'both': ['BOTH', 'both', 'Both', 'NET'],
    'long': ['LONG', 'long', 'Long', 'BUY'],
    'short': ['SHORT', 'short', 'Short', 'SELL']
}

# Time in force options
TIME_IN_FORCE = {
    'gtc': ['GTC', 'gtc', 'GoodTillCancel', 'GOOD_TILL_CANCEL'],
    'ioc': ['IOC', 'ioc', 'ImmediateOrCancel', 'IMMEDIATE_OR_CANCEL'],
    'fok': ['FOK', 'fok', 'FillOrKill', 'FILL_OR_KILL'],
    'post_only': ['POST_ONLY', 'post_only', 'PostOnly', 'MAKER_ONLY']
}

# Margin modes
MARGIN_MODES = {
    'cross': ['CROSS', 'cross', 'Cross', 'CROSSED'],
    'isolated': ['ISOLATED', 'isolated', 'Isolated', 'FIXED']
}

# Fee rates (default)
DEFAULT_FEE_RATES = {
    'spot': {
        'maker': 0.001,  # 0.1%
        'taker': 0.001   # 0.1%
    },
    'futures': {
        'maker': 0.0002, # 0.02%
        'taker': 0.0004  # 0.04%
    }
}

# Risk parameters
RISK_PARAMETERS = {
    'max_leverage': {
        'spot': 1,
        'futures': 125,
        'default': 20
    },
    'min_order_size': {
        'BTC': 0.00001,
        'ETH': 0.0001,
        'default': 1
    },
    'max_position_size': {
        'BTC': 1000,
        'ETH': 10000,
        'default': 100000
    }
}