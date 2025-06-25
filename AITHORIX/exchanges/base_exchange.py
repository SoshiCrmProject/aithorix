"""
AITHORIX Base Exchange Class
Abstract base class for all exchange implementations
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Dict, List, Optional, Any, Callable, Tuple
import asyncio
import logging
import time
import hmac
import hashlib
import json
import aiohttp
from decimal import Decimal

logger = logging.getLogger(__name__)


class OrderType(Enum):
    """Order types"""
    MARKET = "market"
    LIMIT = "limit"
    STOP = "stop"
    STOP_LIMIT = "stop_limit"
    TRAILING_STOP = "trailing_stop"
    POST_ONLY = "post_only"
    FOK = "fok"  # Fill or Kill
    IOC = "ioc"  # Immediate or Cancel


class OrderSide(Enum):
    """Order side"""
    BUY = "buy"
    SELL = "sell"


class OrderStatus(Enum):
    """Order status"""
    PENDING = "pending"
    OPEN = "open"
    PARTIALLY_FILLED = "partially_filled"
    FILLED = "filled"
    CANCELLED = "cancelled"
    REJECTED = "rejected"
    EXPIRED = "expired"


class TimeInForce(Enum):
    """Time in force"""
    GTC = "gtc"  # Good Till Cancelled
    IOC = "ioc"  # Immediate or Cancel
    FOK = "fok"  # Fill or Kill
    GTX = "gtx"  # Good Till Crossing
    DAY = "day"  # Day order


class PositionSide(Enum):
    """Position side for futures"""
    LONG = "long"
    SHORT = "short"
    BOTH = "both"  # For one-way mode


@dataclass
class ExchangeConfig:
    """Exchange configuration"""
    api_key: str
    api_secret: str
    passphrase: Optional[str] = None  # For exchanges that require it
    testnet: bool = False
    
    # API endpoints
    rest_url: str = ""
    ws_url: str = ""
    
    # Rate limits
    rate_limit_per_second: int = 10
    rate_limit_per_minute: int = 1200
    
    # Trading fees
    maker_fee: float = 0.001
    taker_fee: float = 0.001
    
    # Connection settings
    timeout: int = 30
    max_retries: int = 3
    retry_delay: float = 1.0
    
    # Features
    supports_futures: bool = True
    supports_options: bool = False
    supports_margin: bool = True
    supports_websocket: bool = True
    
    # Risk limits
    max_position_size: float = 100000
    max_leverage: int = 20
    
    # Custom settings per exchange
    custom_settings: Dict[str, Any] = field(default_factory=dict)


@dataclass
class Order:
    """Order data structure"""
    order_id: str
    client_order_id: Optional[str]
    symbol: str
    side: OrderSide
    order_type: OrderType
    status: OrderStatus
    price: Optional[float]
    size: float
    filled_size: float
    average_price: Optional[float]
    fee: float
    fee_currency: str
    time_in_force: TimeInForce
    created_at: datetime
    updated_at: datetime
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class Trade:
    """Trade data structure"""
    trade_id: str
    order_id: str
    symbol: str
    side: OrderSide
    price: float
    size: float
    fee: float
    fee_currency: str
    timestamp: datetime
    is_maker: bool
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class Position:
    """Position data structure"""
    symbol: str
    side: PositionSide
    size: float
    entry_price: float
    mark_price: float
    liquidation_price: Optional[float]
    unrealized_pnl: float
    realized_pnl: float
    margin: float
    leverage: int
    created_at: datetime
    updated_at: datetime
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class Balance:
    """Balance data structure"""
    currency: str
    free: float
    used: float
    total: float
    usd_value: Optional[float] = None
    updated_at: datetime = field(default_factory=datetime.now)


@dataclass
class Ticker:
    """Ticker data structure"""
    symbol: str
    bid: float
    ask: float
    last: float
    volume_24h: float
    quote_volume_24h: float
    high_24h: float
    low_24h: float
    change_24h: float
    change_percent_24h: float
    timestamp: datetime


@dataclass
class OrderBook:
    """Order book data structure"""
    symbol: str
    bids: List[Tuple[float, float]]  # [(price, size), ...]
    asks: List[Tuple[float, float]]  # [(price, size), ...]
    timestamp: datetime
    sequence: Optional[int] = None


@dataclass
class Candle:
    """Candlestick data structure"""
    symbol: str
    timeframe: str
    open_time: datetime
    close_time: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float
    quote_volume: float
    trades: int


class BaseExchange(ABC):
    """
    Abstract base class for all exchange implementations
    """
    
    def __init__(self, config: ExchangeConfig):
        self.config = config
        self.session: Optional[aiohttp.ClientSession] = None
        self.ws_session: Optional[aiohttp.ClientSession] = None
        self.ws_connection = None
        
        # Rate limiting
        self.last_request_time = 0
        self.request_count = 0
        self.minute_request_count = 0
        self.minute_start_time = time.time()
        
        # Callbacks
        self.error_callback: Optional[Callable] = None
        self.order_callback: Optional[Callable] = None
        
        # Cache
        self.symbol_info_cache: Dict[str, Any] = {}
        self.balance_cache: Dict[str, Balance] = {}
        self.position_cache: Dict[str, Position] = {}
        
        # State
        self.is_initialized = False
        self.is_connected = False
        
        logger.info(f"Initialized {self.__class__.__name__} base exchange")
    
    async def initialize(self):
        """Initialize exchange connection"""
        if self.is_initialized:
            return
        
        # Create HTTP session
        self.session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=self.config.timeout),
            connector=aiohttp.TCPConnector(limit=100)
        )
        
        # Create WebSocket session if supported
        if self.config.supports_websocket:
            self.ws_session = aiohttp.ClientSession()
        
        # Exchange-specific initialization
        await self._initialize_exchange()
        
        # Load market info
        await self._load_markets()
        
        self.is_initialized = True
        logger.info(f"{self.__class__.__name__} initialized successfully")
    
    async def shutdown(self):
        """Shutdown exchange connection"""
        self.is_initialized = False
        self.is_connected = False
        
        # Close WebSocket
        if self.ws_connection:
            await self.ws_connection.close()
        
        # Close sessions
        if self.session:
            await self.session.close()
        if self.ws_session:
            await self.ws_session.close()
        
        logger.info(f"{self.__class__.__name__} shut down")
    
    async def check_connection(self) -> bool:
        """Check if exchange is reachable"""
        try:
            # Simple ping or time endpoint
            await self._check_connection()
            self.is_connected = True
            return True
        except Exception as e:
            logger.error(f"Connection check failed: {str(e)}")
            self.is_connected = False
            return False
    
    # Rate limiting
    async def _rate_limit(self):
        """Enforce rate limits"""
        current_time = time.time()
        
        # Per-second rate limit
        time_since_last = current_time - self.last_request_time
        if time_since_last < 1.0 / self.config.rate_limit_per_second:
            await asyncio.sleep(1.0 / self.config.rate_limit_per_second - time_since_last)
        
        # Per-minute rate limit
        if current_time - self.minute_start_time >= 60:
            self.minute_request_count = 0
            self.minute_start_time = current_time
        
        if self.minute_request_count >= self.config.rate_limit_per_minute:
            sleep_time = 60 - (current_time - self.minute_start_time)
            if sleep_time > 0:
                logger.warning(f"Rate limit reached, sleeping for {sleep_time:.1f}s")
                await asyncio.sleep(sleep_time)
                self.minute_request_count = 0
                self.minute_start_time = time.time()
        
        self.last_request_time = time.time()
        self.request_count += 1
        self.minute_request_count += 1
    
    # HTTP methods
    async def _request(
        self,
        method: str,
        endpoint: str,
        params: Optional[Dict] = None,
        data: Optional[Dict] = None,
        headers: Optional[Dict] = None,
        signed: bool = False
    ) -> Dict[str, Any]:
        """Make HTTP request with retries"""
        await self._rate_limit()
        
        url = f"{self.config.rest_url}{endpoint}"
        
        # Add authentication if needed
        if signed:
            headers = headers or {}
            auth_headers = await self._get_auth_headers(method, endpoint, params, data)
            headers.update(auth_headers)
        
        # Retry logic
        for attempt in range(self.config.max_retries):
            try:
                async with self.session.request(
                    method=method,
                    url=url,
                    params=params,
                    json=data,
                    headers=headers
                ) as response:
                    response_data = await response.json()
                    
                    if response.status >= 400:
                        error_msg = response_data.get('msg', 'Unknown error')
                        raise Exception(f"API error {response.status}: {error_msg}")
                    
                    return response_data
                    
            except Exception as e:
                if attempt == self.config.max_retries - 1:
                    logger.error(f"Request failed after {self.config.max_retries} attempts: {str(e)}")
                    raise
                
                await asyncio.sleep(self.config.retry_delay * (attempt + 1))
    
    async def _get(self, endpoint: str, params: Optional[Dict] = None, signed: bool = False) -> Dict:
        """GET request"""
        return await self._request("GET", endpoint, params=params, signed=signed)
    
    async def _post(self, endpoint: str, data: Optional[Dict] = None, signed: bool = False) -> Dict:
        """POST request"""
        return await self._request("POST", endpoint, data=data, signed=signed)
    
    async def _delete(self, endpoint: str, params: Optional[Dict] = None, signed: bool = False) -> Dict:
        """DELETE request"""
        return await self._request("DELETE", endpoint, params=params, signed=signed)
    
    # Abstract methods that must be implemented by each exchange
    @abstractmethod
    async def _initialize_exchange(self):
        """Exchange-specific initialization"""
        pass
    
    @abstractmethod
    async def _load_markets(self):
        """Load market information"""
        pass
    
    @abstractmethod
    async def _check_connection(self):
        """Exchange-specific connection check"""
        pass
    
    @abstractmethod
    async def _get_auth_headers(
        self,
        method: str,
        endpoint: str,
        params: Optional[Dict],
        data: Optional[Dict]
    ) -> Dict[str, str]:
        """Generate authentication headers"""
        pass
    
    # Trading methods
    @abstractmethod
    async def place_order(
        self,
        symbol: str,
        side: str,
        order_type: OrderType,
        size: float,
        price: Optional[float] = None,
        params: Optional[Dict] = None
    ) -> Order:
        """Place an order"""
        pass
    
    @abstractmethod
    async def cancel_order(self, order_id: str, symbol: Optional[str] = None) -> bool:
        """Cancel an order"""
        pass
    
    @abstractmethod
    async def get_order(self, order_id: str, symbol: Optional[str] = None) -> Order:
        """Get order details"""
        pass
    
    @abstractmethod
    async def get_open_orders(self, symbol: Optional[str] = None) -> List[Order]:
        """Get all open orders"""
        pass
    
    @abstractmethod
    async def get_order_history(
        self,
        symbol: Optional[str] = None,
        limit: int = 100,
        start_time: Optional[datetime] = None
    ) -> List[Order]:
        """Get order history"""
        pass
    
    # Market data methods
    @abstractmethod
    async def get_ticker(self, symbol: str) -> Ticker:
        """Get ticker for a symbol"""
        pass
    
    @abstractmethod
    async def get_order_book(self, symbol: str, limit: int = 20) -> OrderBook:
        """Get order book"""
        pass
    
    @abstractmethod
    async def get_candles(
        self,
        symbol: str,
        timeframe: str,
        limit: int = 100,
        start_time: Optional[datetime] = None
    ) -> List[Candle]:
        """Get candlestick data"""
        pass
    
    @abstractmethod
    async def get_trades(self, symbol: str, limit: int = 100) -> List[Trade]:
        """Get recent trades"""
        pass
    
    # Account methods
    @abstractmethod
    async def get_balance(self) -> Dict[str, Balance]:
        """Get account balance"""
        pass
    
    @abstractmethod
    async def get_open_positions(self) -> List[Position]:
        """Get open positions (for futures)"""
        pass
    
    @abstractmethod
    async def get_position(self, symbol: str) -> Optional[Position]:
        """Get specific position"""
        pass
    
    # WebSocket methods
    async def start_websocket(
        self,
        on_message: Callable,
        on_error: Optional[Callable] = None,
        on_close: Optional[Callable] = None
    ):
        """Start WebSocket connection"""
        if not self.config.supports_websocket:
            raise NotImplementedError(f"{self.__class__.__name__} does not support WebSocket")
        
        self.ws_on_message = on_message
        self.ws_on_error = on_error or self._default_error_handler
        self.ws_on_close = on_close or self._default_close_handler
        
        await self._connect_websocket()
        return self.ws_connection
    
    @abstractmethod
    async def _connect_websocket(self):
        """Connect to WebSocket"""
        pass
    
    @abstractmethod
    async def subscribe_ticker(self, symbol: str):
        """Subscribe to ticker updates"""
        pass
    
    @abstractmethod
    async def subscribe_orderbook(self, symbol: str, depth: int = 20):
        """Subscribe to order book updates"""
        pass
    
    @abstractmethod
    async def subscribe_trades(self, symbol: str):
        """Subscribe to trade updates"""
        pass
    
    @abstractmethod
    async def subscribe_user_orders(self):
        """Subscribe to user order updates"""
        pass
    
    @abstractmethod
    async def subscribe_user_positions(self):
        """Subscribe to user position updates"""
        pass
    
    # Helper methods
    def _normalize_symbol(self, symbol: str) -> str:
        """Normalize symbol format for the exchange"""
        # Default implementation, override if needed
        return symbol.replace('/', '').upper()
    
    def _denormalize_symbol(self, exchange_symbol: str) -> str:
        """Convert exchange symbol to standard format"""
        # Default implementation, override if needed
        # Assumes format like BTCUSDT -> BTC/USDT
        if len(exchange_symbol) >= 6:
            # Try common patterns
            for i in [3, 4]:  # BTC/USDT or DOGE/USDT
                base = exchange_symbol[:i]
                quote = exchange_symbol[i:]
                if quote in ['USDT', 'USDC', 'BUSD', 'USD', 'BTC', 'ETH']:
                    return f"{base}/{quote}"
        return exchange_symbol
    
    def _parse_order_status(self, status: str) -> OrderStatus:
        """Parse exchange-specific status to standard OrderStatus"""
        # Default mapping, override for specific exchanges
        status_map = {
            'new': OrderStatus.OPEN,
            'open': OrderStatus.OPEN,
            'partially_filled': OrderStatus.PARTIALLY_FILLED,
            'filled': OrderStatus.FILLED,
            'canceled': OrderStatus.CANCELLED,
            'cancelled': OrderStatus.CANCELLED,
            'rejected': OrderStatus.REJECTED,
            'expired': OrderStatus.EXPIRED
        }
        return status_map.get(status.lower(), OrderStatus.OPEN)
    
    def _parse_order_type(self, order_type: str) -> OrderType:
        """Parse exchange-specific order type to standard OrderType"""
        type_map = {
            'market': OrderType.MARKET,
            'limit': OrderType.LIMIT,
            'stop': OrderType.STOP,
            'stop_limit': OrderType.STOP_LIMIT,
            'limit_maker': OrderType.POST_ONLY
        }
        return type_map.get(order_type.lower(), OrderType.LIMIT)
    
    def _parse_order_side(self, side: str) -> OrderSide:
        """Parse order side"""
        return OrderSide.BUY if side.lower() == 'buy' else OrderSide.SELL
    
    async def _default_error_handler(self, error: Exception):
        """Default WebSocket error handler"""
        logger.error(f"WebSocket error: {str(error)}")
        if self.error_callback:
            await self.error_callback(error)
    
    async def _default_close_handler(self):
        """Default WebSocket close handler"""
        logger.warning("WebSocket connection closed")
        self.is_connected = False
    
    async def get_top_symbols(self, limit: int = 50) -> List[str]:
        """Get top trading symbols by volume"""
        # Default implementation - override for specific exchanges
        tickers = await self.get_all_tickers()
        
        # Sort by 24h volume
        sorted_tickers = sorted(
            tickers.items(),
            key=lambda x: x[1].quote_volume_24h,
            reverse=True
        )
        
        return [symbol for symbol, _ in sorted_tickers[:limit]]
    
    @abstractmethod
    async def get_all_tickers(self) -> Dict[str, Ticker]:
        """Get all tickers"""
        pass
    
    def calculate_order_value(self, order: Order) -> float:
        """Calculate total order value"""
        if order.average_price:
            return order.filled_size * order.average_price
        elif order.price:
            return order.size * order.price
        return 0.0
    
    def calculate_pnl(self, position: Position) -> float:
        """Calculate position PnL"""
        return position.unrealized_pnl + position.realized_pnl
    
    def calculate_position_value(self, position: Position) -> float:
        """Calculate position value"""
        return position.size * position.mark_price