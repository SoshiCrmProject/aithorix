"""
AITHORIX Hyperliquid Constants
Constants and configuration values for Hyperliquid exchange
"""

from enum import Enum
from typing import Dict, List


class HyperliquidConstants:
    """Hyperliquid exchange constants"""
    
    # API Endpoints
    MAINNET_API_URL = "https://api.hyperliquid.xyz"
    TESTNET_API_URL = "https://api.hyperliquid-testnet.xyz"
    MAINNET_WS_URL = "wss://api.hyperliquid.xyz/ws"
    TESTNET_WS_URL = "wss://api.hyperliquid-testnet.xyz/ws"
    
    # Rate Limits
    MAX_REQUESTS_PER_SECOND = 100
    MAX_ORDERS_PER_BATCH = 100
    MAX_CANCELS_PER_BATCH = 100
    
    # Order Constraints
    MIN_ORDER_SIZE = 0.001  # Minimum order size for most assets
    MAX_LEVERAGE = 50  # Maximum leverage allowed
    DEFAULT_LEVERAGE = 10
    
    # Time Constants
    FUNDING_INTERVAL_HOURS = 8
    NONCE_WINDOW_MS = 60000  # 60 seconds
    
    # Fee Structure
    MAKER_FEE_RATE = 0.00025  # 0.025%
    TAKER_FEE_RATE = 0.0005   # 0.05%
    
    # Gas Costs (in USDC)
    BASE_GAS_COST = 0.10
    URGENT_GAS_MULTIPLIER = 2.0
    
    # WebSocket Constants
    WS_HEARTBEAT_INTERVAL = 30  # seconds
    WS_RECONNECT_DELAY = 5      # seconds
    WS_MAX_RECONNECT_ATTEMPTS = 10
    
    # Precision Mapping
    ASSET_PRECISION = {
        'BTC': 3,
        'ETH': 3,
        'SOL': 2,
        'ARB': 1,
        'MATIC': 1,
        'AVAX': 1,
        'BNB': 2,
        'DOGE': 0,
        'ATOM': 1,
        'APT': 1,
        'SUI': 1,
        'TIA': 1,
        'SEI': 1,
        'INJ': 1,
        'WLD': 1,
        'BLUR': 1,
        'JUP': 1,
        'STRK': 1,
        'PYTH': 0,
        'ORDI': 2,
        'TRB': 2,
        'LINK': 1,
        'XRP': 0,
        'NEAR': 1,
        'GALA': 0,
        'RDNT': 0,
        'STX': 1,
        'MANTA': 1,
        'ALT': 0,
        'ZETA': 1,
        'DYM': 1,
        'MEME': 0,
        'BONK': 0,
        'PEPE': 0,
        'SHIB': 0,
        'FLOKI': 0
    }
    
    # Default precision for unknown assets
    DEFAULT_PRECISION = 2


class OrderType(Enum):
    """Hyperliquid order types"""
    LIMIT = "limit"
    MARKET = "market"
    STOP_LIMIT = "stop_limit"
    STOP_MARKET = "stop_market"
    TAKE_PROFIT = "take_profit"
    TAKE_PROFIT_MARKET = "take_profit_market"


class TimeInForce(Enum):
    """Hyperliquid time in force options"""
    GTC = "Gtc"  # Good Till Cancelled
    IOC = "Ioc"  # Immediate or Cancel
    ALO = "Alo"  # Add Liquidity Only (Post-Only)


class PositionMode(Enum):
    """Position modes"""
    ONE_WAY = "one_way"
    HEDGE = "hedge"


class MarginType(Enum):
    """Margin types"""
    CROSSED = "crossed"
    ISOLATED = "isolated"


class OrderStatus(Enum):
    """Order status"""
    OPEN = "open"
    FILLED = "filled"
    CANCELED = "canceled"
    REJECTED = "rejected"
    PARTIALLY_FILLED = "partially_filled"


class WebSocketChannel(Enum):
    """WebSocket subscription channels"""
    L2_BOOK = "l2Book"
    TRADES = "trades"
    CANDLE = "candle"
    USER_EVENTS = "userEvents"
    USER_FILLS = "userFills"
    USER_FUNDING = "userFunding"
    ACTIVE_ASSET_CTX = "activeAssetCtx"
    NOTIFICATION = "notification"


class ActionType(Enum):
    """Hyperliquid action types"""
    ORDER = "order"
    CANCEL = "cancel"
    CANCEL_BY_CLOID = "cancelByCloid"
    MODIFY = "modify"
    BATCH_MODIFY = "batchModify"
    UPDATE_LEVERAGE = "updateLeverage"
    UPDATE_ISOLATED_MARGIN = "updateIsolatedMargin"
    VAULT_TRANSFER = "vaultTransfer"
    USD_SEND = "usdSend"
    WITHDRAW = "withdraw"


class TpslType(Enum):
    """Take profit / Stop loss type"""
    TP = "tp"  # Take profit
    SL = "sl"  # Stop loss


# Error Messages
ERROR_MESSAGES = {
    "INSUFFICIENT_BALANCE": "Insufficient balance for order",
    "INVALID_LEVERAGE": "Invalid leverage value",
    "POSITION_NOT_FOUND": "Position not found",
    "ORDER_NOT_FOUND": "Order not found",
    "RATE_LIMIT_EXCEEDED": "Rate limit exceeded",
    "INVALID_SIGNATURE": "Invalid signature",
    "NONCE_TOO_OLD": "Nonce too old",
    "INVALID_ORDER_SIZE": "Order size below minimum",
    "MAX_POSITION_EXCEEDED": "Maximum position size exceeded",
    "MARKET_CLOSED": "Market is closed",
    "INVALID_PRICE": "Invalid order price",
    "INSUFFICIENT_MARGIN": "Insufficient margin",
    "LIQUIDATION_IMMINENT": "Position close to liquidation",
    "REDUCE_ONLY_ORDER": "Reduce-only order would increase position",
    "POST_ONLY_FAILED": "Post-only order would cross the book"
}


# Response Status Codes
class ResponseStatus:
    """API response status codes"""
    SUCCESS = "ok"
    ERROR = "error"
    PARTIAL_SUCCESS = "partial"


# Utility Functions
def get_precision(asset: str) -> int:
    """Get decimal precision for an asset"""
    return HyperliquidConstants.ASSET_PRECISION.get(
        asset, 
        HyperliquidConstants.DEFAULT_PRECISION
    )


def format_size(size: float, asset: str) -> str:
    """Format size according to asset precision"""
    precision = get_precision(asset)
    return f"{size:.{precision}f}".rstrip('0').rstrip('.')


def is_valid_leverage(leverage: int) -> bool:
    """Check if leverage value is valid"""
    return 1 <= leverage <= HyperliquidConstants.MAX_LEVERAGE


def calculate_gas_cost(base_cost: float, urgency: str = "normal") -> float:
    """Calculate gas cost based on urgency"""
    multipliers = {
        "low": 0.8,
        "normal": 1.0,
        "high": 1.5,
        "urgent": HyperliquidConstants.URGENT_GAS_MULTIPLIER
    }
    multiplier = multipliers.get(urgency, 1.0)
    return base_cost * multiplier