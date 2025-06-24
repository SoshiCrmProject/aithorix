"""
AITHORIX Base Exchange Classes
Abstract base classes for all exchange implementations
"""

import asyncio
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal
from typing import Dict, List, Optional, Any, Tuple, AsyncGenerator
from enum import Enum
import hashlib
import hmac
import json

import aiohttp
import websockets
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
import structlog

from ..core.exceptions import (
    ExchangeConnectionError,
    OrderExecutionError,
    RateLimitError,
    AuthenticationError
)
from ..core.engine.trading_engine import Order, OrderType, OrderSide, OrderStatus
from ..stealth.antidetection.api_normalizer import APIRateLimiter
from ..stealth.antidetection.timing_jitter import TimingJitter


logger = structlog.get_logger()


@dataclass
class ExchangeCredentials:
    """Exchange API credentials"""
    api_key: str
    api_secret: str
    passphrase: Optional[str] = None  # For OKX
    testnet: bool = False
    

@dataclass
class MarketInfo:
    """Market trading rules and information"""
    symbol: str
    base_asset: str
    quote_asset: str
    min_quantity: Decimal
    max_quantity: Decimal
    quantity_precision: int
    min_price: Decimal
    max_price: Decimal
    price_precision: int
    min_notional: Decimal
    is_trading: bool
    maker_fee: Decimal
    taker_fee: Decimal
    

@dataclass
class OrderBook:
    """Order book snapshot"""
    timestamp: datetime
    symbol: str
    bids: List[Tuple[Decimal, Decimal]]  # (price, quantity)
    asks: List[Tuple[Decimal, Decimal]]  # (price, quantity)
    

@dataclass
class Ticker:
    """Market ticker data"""
    timestamp: datetime
    symbol: str
    bid: Decimal
    ask: Decimal
    last: Decimal
    volume_24h: Decimal
    high_24h: Decimal
    low_24h: Decimal
    change_24h: Decimal
    

@dataclass
class Balance:
    """Account balance"""
    asset: str
    free: Decimal
    locked: Decimal
    total: Decimal
    

class BaseExchange(ABC):
    """
    Abstract base class for all exchange implementations
    Provides common functionality and enforces interface
    """
    
    def __init__(
        self, 
        credentials: ExchangeCredentials,
        rate_limiter: Optional[APIRateLimiter] = None,
        timing_jitter: Optional[TimingJitter] = None
    ):
        self.credentials = credentials
        self.rate_limiter = rate_limiter or APIRateLimiter()
        self.timing_jitter = timing_jitter or TimingJitter()
        
        # Exchange specific attributes (override in subclasses)
        self.name: str = "base"
        self.base_url: str = ""
        self.ws_url: str = ""
        self.rate_limits: Dict[str, int] = {}
        
        # Connection management
        self.session: Optional[aiohttp.ClientSession] = None
        self.ws_connections: Dict[str, websockets.WebSocketClientProtocol] = {}
        
        # Market information cache
        self.markets: Dict[str, MarketInfo] = {}
        self.last_market_update: float = 0
        self.market_update_interval: int = 3600  # 1 hour
        
        # Performance tracking
        self.request_count: int = 0
        self.error_count: int = 0
        self.last_request_time: float = 0
        
    async def __aenter__(self):
        """Async context manager entry"""
        await self.connect()
        return self
        
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit"""
        await self.disconnect()
        
    async def connect(self) -> None:
        """Initialize exchange connection"""
        if not self.session:
            timeout = aiohttp.ClientTimeout(total=30)
            connector = aiohttp.TCPConnector(limit=100, limit_per_host=30)
            
            self.session = aiohttp.ClientSession(
                timeout=timeout,
                connector=connector,
                headers=self._get_default_headers()
            )
            
        # Load market information
        await self._update_markets()
        
        logger.info(f"Connected to {self.name} exchange")
        
    async def disconnect(self) -> None:
        """Close exchange connections"""
        # Close WebSocket connections
        for ws in self.ws_connections.values():
            await ws.close()
        self.ws_connections.clear()
        
        # Close HTTP session
        if self.session:
            await self.session.close()
            self.session = None
            
        logger.info(f"Disconnected from {self.name} exchange")
        
    def _get_default_headers(self) -> Dict[str, str]:
        """Get default HTTP headers with stealth modifications"""
        return {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "application/json",
            "Accept-Language": "en-US,en;q=0.9",
            "Accept-Encoding": "gzip, deflate, br",
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
        }
        
    @abstractmethod
    def _sign_request(self, method: str, path: str, params: Dict[str, Any]) -> Dict[str, str]:
        """Sign request for authentication - implement in subclasses"""
        pass
        
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        retry=retry_if_exception_type((aiohttp.ClientError, asyncio.TimeoutError))
    )
    async def _make_request(
        self, 
        method: str, 
        endpoint: str, 
        params: Optional[Dict[str, Any]] = None,
        signed: bool = False
    ) -> Dict[str, Any]:
        """Make HTTP request with retry logic and rate limiting"""
        
        # Apply rate limiting
        await self.rate_limiter.acquire(self.name, endpoint)
        
        # Apply timing jitter for stealth
        await self.timing_jitter.add_delay()
        
        url = f"{self.base_url}{endpoint}"
        
        # Sign request if needed
        headers = self._get_default_headers()
        if signed:
            headers.update(self._sign_request(method, endpoint, params or {}))
            
        # Track request timing
        start_time = time.time()
        self.request_count += 1
        
        try:
            if method == "GET":
                async with self.session.get(url, params=params, headers=headers) as response:
                    return await self._handle_response(response)
            elif method == "POST":
                async with self.session.post(url, json=params, headers=headers) as response:
                    return await self._handle_response(response)
            elif method == "PUT":
                async with self.session.put(url, json=params, headers=headers) as response:
                    return await self._handle_response(response)
            elif method == "DELETE":
                async with self.session.delete(url, params=params, headers=headers) as response:
                    return await self._handle_response(response)
            else:
                raise ValueError(f"Unsupported HTTP method: {method}")
                
        except Exception as e:
            self.error_count += 1
            logger.error(f"Request failed to {self.name}: {e}")
            raise ExchangeConnectionError(self.name, str(e))
        finally:
            self.last_request_time = time.time() - start_time
            
    async def _handle_response(self, response: aiohttp.ClientResponse) -> Dict[str, Any]:
        """Handle API response with error checking"""
        text = await response.text()
        
        # Check for rate limit
        if response.status == 429:
            retry_after = int(response.headers.get("Retry-After", 60))
            raise RateLimitError(
                limit=self.rate_limits.get("default", 1000),
                window="minute",
                retry_after=retry_after
            )
            
        # Check for auth errors
        if response.status in [401, 403]:
            raise AuthenticationError(f"Authentication failed on {self.name}")
            
        # Check for other errors
        if response.status >= 400:
            try:
                error_data = json.loads(text)
                error_msg = error_data.get("msg", error_data.get("message", text))
            except:
                error_msg = text
            raise ExchangeConnectionError(self.name, f"HTTP {response.status}: {error_msg}")
            
        # Parse JSON response
        try:
            return json.loads(text)
        except json.JSONDecodeError:
            raise ExchangeConnectionError(self.name, f"Invalid JSON response: {text}")
            
    async def _update_markets(self) -> None:
        """Update market information cache"""
        current_time = time.time()
        
        # Check if update needed
        if current_time - self.last_market_update < self.market_update_interval:
            return
            
        try:
            markets_data = await self.get_markets()
            
            self.markets.clear()
            for market in markets_data:
                self.markets[market.symbol] = market
                
            self.last_market_update = current_time
            logger.info(f"Updated {len(self.markets)} markets for {self.name}")
            
        except Exception as e:
            logger.error(f"Failed to update markets for {self.name}: {e}")
            
    def get_market_info(self, symbol: str) -> Optional[MarketInfo]:
        """Get market information for symbol"""
        return self.markets.get(symbol)
        
    def round_quantity(self, symbol: str, quantity: Decimal) -> Decimal:
        """Round quantity to exchange precision"""
        market = self.get_market_info(symbol)
        if not market:
            return quantity
            
        precision = market.quantity_precision
        return Decimal(str(round(float(quantity), precision)))
        
    def round_price(self, symbol: str, price: Decimal) -> Decimal:
        """Round price to exchange precision"""
        market = self.get_market_info(symbol)
        if not market:
            return price
            
        precision = market.price_precision
        return Decimal(str(round(float(price), precision)))
        
    def validate_order(self, order: Order) -> Tuple[bool, Optional[str]]:
        """Validate order against exchange rules"""
        market = self.get_market_info(order.symbol)
        if not market:
            return False, f"Unknown market: {order.symbol}"
            
        # Check if market is trading
        if not market.is_trading:
            return False, f"Market {order.symbol} is not trading"
            
        # Check quantity limits
        if order.quantity < market.min_quantity:
            return False, f"Quantity {order.quantity} below minimum {market.min_quantity}"
            
        if order.quantity > market.max_quantity:
            return False, f"Quantity {order.quantity} above maximum {market.max_quantity}"
            
        # Check price limits for limit orders
        if order.order_type == OrderType.LIMIT and order.price:
            if order.price < market.min_price:
                return False, f"Price {order.price} below minimum {market.min_price}"
                
            if order.price > market.max_price:
                return False, f"Price {order.price} above maximum {market.max_price}"
                
        # Check minimum notional
        notional = order.quantity * (order.price or market.last)
        if notional < market.min_notional:
            return False, f"Notional {notional} below minimum {market.min_notional}"
            
        return True, None
        
    # Abstract methods to be implemented by subclasses
    @abstractmethod
    async def get_markets(self) -> List[MarketInfo]:
        """Get all available markets"""
        pass
        
    @abstractmethod
    async def get_ticker(self, symbol: str) -> Ticker:
        """Get current ticker for symbol"""
        pass
        
    @abstractmethod
    async def get_order_book(self, symbol: str, limit: int = 100) -> OrderBook:
        """Get order book for symbol"""
        pass
        
    @abstractmethod
    async def get_balance(self) -> List[Balance]:
        """Get account balances"""
        pass
        
    @abstractmethod
    async def place_order(self, order: Order) -> Dict[str, Any]:
        """Place new order"""
        pass
        
    @abstractmethod
    async def cancel_order(self, order_id: str, symbol: str) -> Dict[str, Any]:
        """Cancel existing order"""
        pass
        
    @abstractmethod
    async def get_order_status(self, order_id: str, symbol: str) -> Dict[str, Any]:
        """Get order status"""
        pass
        
    @abstractmethod
    async def get_open_orders(self, symbol: Optional[str] = None) -> List[Dict[str, Any]]:
        """Get all open orders"""
        pass
        
    @abstractmethod
    async def subscribe_ticker(self, symbol: str) -> AsyncGenerator[Ticker, None]:
        """Subscribe to ticker updates via WebSocket"""
        pass
        
    @abstractmethod
    async def subscribe_order_book(self, symbol: str) -> AsyncGenerator[OrderBook, None]:
        """Subscribe to order book updates via WebSocket"""
        pass
        
    @abstractmethod
    async def subscribe_trades(self, symbol: str) -> AsyncGenerator[Dict[str, Any], None]:
        """Subscribe to trade updates via WebSocket"""
        pass


class WebSocketManager:
    """
    WebSocket connection manager with automatic reconnection
    """
    
    def __init__(self, url: str, headers: Optional[Dict[str, str]] = None):
        self.url = url
        self.headers = headers or {}
        self.ws: Optional[websockets.WebSocketClientProtocol] = None
        self.is_connected = False
        self.reconnect_delay = 5
        self.max_reconnect_delay = 60
        self._stop = False
        
    async def connect(self) -> None:
        """Establish WebSocket connection"""
        try:
            self.ws = await websockets.connect(
                self.url,
                extra_headers=self.headers,
                ping_interval=20,
                ping_timeout=10
            )
            self.is_connected = True
            self.reconnect_delay = 5  # Reset delay on successful connection
            logger.info(f"WebSocket connected to {self.url}")
            
        except Exception as e:
            logger.error(f"WebSocket connection failed: {e}")
            raise
            
    async def disconnect(self) -> None:
        """Close WebSocket connection"""
        self._stop = True
        if self.ws:
            await self.ws.close()
            self.ws = None
        self.is_connected = False
        logger.info(f"WebSocket disconnected from {self.url}")
        
    async def send(self, message: Dict[str, Any]) -> None:
        """Send message to WebSocket"""
        if not self.is_connected or not self.ws:
            raise ExchangeConnectionError("websocket", "WebSocket not connected")
            
        await self.ws.send(json.dumps(message))
        
    async def receive(self) -> AsyncGenerator[Dict[str, Any], None]:
        """Receive messages from WebSocket with auto-reconnection"""
        while not self._stop:
            try:
                if not self.is_connected:
                    await self.connect()
                    
                async for message in self.ws:
                    try:
                        data = json.loads(message)
                        yield data
                    except json.JSONDecodeError:
                        logger.error(f"Invalid JSON from WebSocket: {message}")
                        continue
                        
            except websockets.exceptions.ConnectionClosed:
                logger.warning("WebSocket connection closed, reconnecting...")
                self.is_connected = False
                await self._handle_reconnect()
                
            except Exception as e:
                logger.error(f"WebSocket error: {e}")
                self.is_connected = False
                await self._handle_reconnect()
                
    async def _handle_reconnect(self) -> None:
        """Handle reconnection with exponential backoff"""
        if self._stop:
            return
            
        await asyncio.sleep(self.reconnect_delay)
        self.reconnect_delay = min(self.reconnect_delay * 2, self.max_reconnect_delay)
        
        try:
            await self.connect()
        except Exception as e:
            logger.error(f"Reconnection failed: {e}")
            

class OrderBookManager:
    """
    Manages order book updates and maintains current state
    """
    
    def __init__(self, symbol: str):
        self.symbol = symbol
        self.bids: Dict[Decimal, Decimal] = {}
        self.asks: Dict[Decimal, Decimal] = {}
        self.last_update_id: Optional[int] = None
        self.snapshot_received = False
        
    def update_snapshot(self, bids: List[List], asks: List[List], update_id: Optional[int] = None) -> None:
        """Update order book with snapshot data"""
        self.bids.clear()
        self.asks.clear()
        
        for bid in bids:
            price = Decimal(str(bid[0]))
            quantity = Decimal(str(bid[1]))
            if quantity > 0:
                self.bids[price] = quantity
                
        for ask in asks:
            price = Decimal(str(ask[0]))
            quantity = Decimal(str(ask[1]))
            if quantity > 0:
                self.asks[price] = quantity
                
        self.last_update_id = update_id
        self.snapshot_received = True
        
    def update_delta(self, bids: List[List], asks: List[List], update_id: Optional[int] = None) -> None:
        """Update order book with delta updates"""
        if not self.snapshot_received:
            return
            
        # Skip old updates
        if update_id and self.last_update_id and update_id <= self.last_update_id:
            return
            
        # Update bids
        for bid in bids:
            price = Decimal(str(bid[0]))
            quantity = Decimal(str(bid[1]))
            
            if quantity == 0:
                self.bids.pop(price, None)
            else:
                self.bids[price] = quantity
                
        # Update asks
        for ask in asks:
            price = Decimal(str(ask[0]))
            quantity = Decimal(str(ask[1]))
            
            if quantity == 0:
                self.asks.pop(price, None)
            else:
                self.asks[price] = quantity
                
        self.last_update_id = update_id
        
    def get_order_book(self, limit: int = 100) -> OrderBook:
        """Get current order book state"""
        # Sort and limit order book
        sorted_bids = sorted(self.bids.items(), key=lambda x: x[0], reverse=True)[:limit]
        sorted_asks = sorted(self.asks.items(), key=lambda x: x[0])[:limit]
        
        return OrderBook(
            timestamp=datetime.now(timezone.utc),
            symbol=self.symbol,
            bids=sorted_bids,
            asks=sorted_asks
        )
        
    def get_best_bid(self) -> Optional[Tuple[Decimal, Decimal]]:
        """Get best bid price and quantity"""
        if not self.bids:
            return None
        price = max(self.bids.keys())
        return price, self.bids[price]
        
    def get_best_ask(self) -> Optional[Tuple[Decimal, Decimal]]:
        """Get best ask price and quantity"""
        if not self.asks:
            return None
        price = min(self.asks.keys())
        return price, self.asks[price]
        
    def get_mid_price(self) -> Optional[Decimal]:
        """Get mid price between best bid and ask"""
        best_bid = self.get_best_bid()
        best_ask = self.get_best_ask()
        
        if not best_bid or not best_ask:
            return None
            
        return (best_bid[0] + best_ask[0]) / Decimal("2")