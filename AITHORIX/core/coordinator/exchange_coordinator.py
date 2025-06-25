"""
AITHORIX Exchange Coordinator
Manages connections and operations across multiple exchanges

This module coordinates:
- Exchange connections and health monitoring
- Balance management across exchanges
- Order routing and execution
- Market data aggregation
"""

import asyncio
import logging
from typing import Dict, List, Optional, Any, Set, Tuple
from datetime import datetime, timedelta
from decimal import Decimal
from dataclasses import dataclass, field
from enum import Enum
from collections import defaultdict, deque
import aiohttp
import ccxt.async_support as ccxt
import json

from core.engine.order_manager import Order, OrderStatus, OrderType
from utils.helpers import get_timestamp, synchronized
from monitoring.metrics import MetricsCollector


class ExchangeStatus(Enum):
    """Exchange connection status"""
    CONNECTED = "connected"
    DISCONNECTED = "disconnected"
    CONNECTING = "connecting"
    ERROR = "error"
    MAINTENANCE = "maintenance"


class ExchangeType(Enum):
    """Exchange type categories"""
    SPOT = "spot"
    FUTURES = "futures"
    DEX = "dex"
    HYBRID = "hybrid"


@dataclass
class ExchangeInfo:
    """Exchange configuration and metadata"""
    exchange_id: str
    name: str
    exchange_type: ExchangeType
    
    # API configuration
    api_key: str = ""
    api_secret: str = ""
    api_password: Optional[str] = None  # For exchanges that require it
    
    # Features
    supports_futures: bool = False
    supports_options: bool = False
    supports_margin: bool = False
    supports_staking: bool = False
    
    # Rate limits
    rate_limit: int = 1200  # requests per minute
    rate_limit_remaining: int = 1200
    rate_limit_reset: Optional[datetime] = None
    
    # Connection info
    status: ExchangeStatus = ExchangeStatus.DISCONNECTED
    last_ping: Optional[datetime] = None
    latency_ms: float = 0.0
    
    # Trading info
    maker_fee: Decimal = Decimal("0.001")
    taker_fee: Decimal = Decimal("0.001")
    
    # Metadata
    connected_at: Optional[datetime] = None
    last_error: Optional[str] = None
    error_count: int = 0


@dataclass
class ExchangeBalance:
    """Balance information for an exchange"""
    exchange_id: str
    timestamp: datetime
    
    # Spot balances
    spot_balances: Dict[str, Decimal] = field(default_factory=dict)
    
    # Futures balances
    futures_balance: Decimal = Decimal("0")
    futures_pnl: Decimal = Decimal("0")
    
    # Margin info
    margin_balance: Decimal = Decimal("0")
    margin_level: Decimal = Decimal("0")
    
    # Totals
    total_btc_value: Decimal = Decimal("0")
    total_usd_value: Decimal = Decimal("0")
    
    # Account info
    can_trade: bool = True
    can_withdraw: bool = True
    can_deposit: bool = True


@dataclass
class MarketInfo:
    """Market trading information"""
    symbol: str
    exchange_id: str
    
    # Trading rules
    min_quantity: Decimal
    max_quantity: Decimal
    quantity_step: Decimal
    
    min_price: Decimal
    max_price: Decimal
    price_step: Decimal
    
    min_notional: Decimal  # Min order value
    
    # Market status
    is_active: bool = True
    is_spot_trading_allowed: bool = True
    is_margin_trading_allowed: bool = False
    
    # Fees
    maker_fee: Optional[Decimal] = None
    taker_fee: Optional[Decimal] = None


class ExchangeCoordinator:
    """
    Coordinates operations across multiple cryptocurrency exchanges
    
    Manages connections, balances, and operations for all configured exchanges
    in the AITHORIX system.
    """
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.logger = logging.getLogger("AITHORIX.ExchangeCoordinator")
        
        # Exchange instances
        self.exchanges: Dict[str, Any] = {}  # exchange_id -> ccxt instance
        self.exchange_info: Dict[str, ExchangeInfo] = {}
        
        # Market information cache
        self.market_info: Dict[Tuple[str, str], MarketInfo] = {}  # (exchange, symbol) -> info
        self.markets_last_updated: Dict[str, datetime] = {}
        
        # Balance tracking
        self.balances: Dict[str, ExchangeBalance] = {}
        self.balance_history: Dict[str, deque] = defaultdict(lambda: deque(maxlen=100))
        
        # Connection management
        self.reconnect_delays: Dict[str, int] = {}  # Exponential backoff
        self.max_reconnect_delay = 300  # 5 minutes
        
        # Performance tracking
        self.metrics_collector = MetricsCollector()
        self.api_call_times: Dict[str, deque] = defaultdict(lambda: deque(maxlen=100))
        
        # Configuration
        self.update_markets_interval = config.get("update_markets_interval", 3600)  # 1 hour
        self.update_balance_interval = config.get("update_balance_interval", 60)  # 1 minute
        self.health_check_interval = config.get("health_check_interval", 30)  # 30 seconds
        
        # State
        self._lock = asyncio.Lock()
        self.is_initialized = False
        
    async def initialize(self) -> None:
        """Initialize the exchange coordinator"""
        self.logger.info("Initializing Exchange Coordinator...")
        
        # Load exchange configurations
        await self._load_exchange_configs()
        
        # Start monitoring tasks
        asyncio.create_task(self._connection_monitor_loop())
        asyncio.create_task(self._balance_update_loop())
        asyncio.create_task(self._health_check_loop())
        asyncio.create_task(self._rate_limit_reset_loop())
        
        self.is_initialized = True
        self.logger.info(f"Exchange Coordinator initialized with {len(self.exchange_info)} exchanges")
    
    async def connect_all(self) -> None:
        """Connect to all configured exchanges"""
        self.logger.info("Connecting to all exchanges...")
        
        connect_tasks = []
        for exchange_id in self.exchange_info:
            task = asyncio.create_task(self.connect_exchange(exchange_id))
            connect_tasks.append(task)
        
        # Wait for all connections
        results = await asyncio.gather(*connect_tasks, return_exceptions=True)
        
        # Log results
        connected = sum(1 for r in results if r is True)
        self.logger.info(f"Connected to {connected}/{len(self.exchange_info)} exchanges")
    
    async def connect_exchange(self, exchange_id: str) -> bool:
        """Connect to a specific exchange"""
        info = self.exchange_info.get(exchange_id)
        if not info:
            self.logger.error(f"Exchange {exchange_id} not configured")
            return False
        
        try:
            info.status = ExchangeStatus.CONNECTING
            self.logger.info(f"Connecting to {info.name}...")
            
            # Create exchange instance
            exchange_class = getattr(ccxt, exchange_id)
            exchange = exchange_class({
                'apiKey': info.api_key,
                'secret': info.api_secret,
                'password': info.api_password,
                'enableRateLimit': True,
                'rateLimit': 60000 / info.rate_limit,  # ms between requests
                'options': {
                    'defaultType': 'spot',  # or 'future'
                    'adjustForTimeDifference': True,
                }
            })
            
            # Test connection
            await exchange.load_markets()
            
            # Store instance
            self.exchanges[exchange_id] = exchange
            
            # Update status
            info.status = ExchangeStatus.CONNECTED
            info.connected_at = datetime.utcnow()
            info.error_count = 0
            
            # Load market info
            await self._update_market_info(exchange_id)
            
            # Get initial balance
            await self.update_balance(exchange_id)
            
            self.logger.info(f"Successfully connected to {info.name}")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to connect to {exchange_id}: {e}")
            info.status = ExchangeStatus.ERROR
            info.last_error = str(e)
            info.error_count += 1
            
            # Update reconnect delay
            self.reconnect_delays[exchange_id] = min(
                self.reconnect_delays.get(exchange_id, 1) * 2,
                self.max_reconnect_delay
            )
            
            return False
    
    async def disconnect_all(self) -> None:
        """Disconnect from all exchanges"""
        self.logger.info("Disconnecting from all exchanges...")
        
        for exchange_id in list(self.exchanges.keys()):
            await self.disconnect_exchange(exchange_id)
    
    async def disconnect_exchange(self, exchange_id: str) -> None:
        """Disconnect from a specific exchange"""
        exchange = self.exchanges.get(exchange_id)
        if exchange:
            try:
                await exchange.close()
            except Exception as e:
                self.logger.error(f"Error closing {exchange_id}: {e}")
            
            del self.exchanges[exchange_id]
        
        info = self.exchange_info.get(exchange_id)
        if info:
            info.status = ExchangeStatus.DISCONNECTED
            info.connected_at = None
    
    @synchronized
    async def place_order(self, order: Order) -> Dict[str, Any]:
        """Place an order on the specified exchange"""
        exchange = self.exchanges.get(order.exchange)
        if not exchange:
            raise ValueError(f"Exchange {order.exchange} not connected")
        
        try:
            # Prepare order parameters
            symbol = order.symbol
            order_type = order.order_type.value.lower()
            side = order.side.lower()
            amount = float(order.quantity)
            
            params = {}
            
            # Add order-specific parameters
            if order.reduce_only:
                params['reduceOnly'] = True
            if order.post_only:
                params['postOnly'] = True
            
            # Place order based on type
            if order.order_type == OrderType.MARKET:
                result = await exchange.create_order(
                    symbol, order_type, side, amount, None, params
                )
            elif order.order_type == OrderType.LIMIT:
                price = float(order.price)
                result = await exchange.create_order(
                    symbol, order_type, side, amount, price, params
                )
            else:
                raise ValueError(f"Unsupported order type: {order.order_type}")
            
            # Update rate limit tracking
            await self._track_api_call(order.exchange)
            
            # Record metrics
            self.metrics_collector.record_exchange_order(order.exchange, order)
            
            return result
            
        except Exception as e:
            self.logger.error(f"Failed to place order on {order.exchange}: {e}")
            raise
    
    @synchronized
    async def cancel_order(self, order_id: str, symbol: str, exchange_id: str) -> bool:
        """Cancel an order on the exchange"""
        exchange = self.exchanges.get(exchange_id)
        if not exchange:
            raise ValueError(f"Exchange {exchange_id} not connected")
        
        try:
            await exchange.cancel_order(order_id, symbol)
            await self._track_api_call(exchange_id)
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to cancel order {order_id} on {exchange_id}: {e}")
            return False
    
    @synchronized
    async def get_order_status(self, order_id: str, symbol: str, exchange_id: str) -> Dict[str, Any]:
        """Get order status from exchange"""
        exchange = self.exchanges.get(exchange_id)
        if not exchange:
            raise ValueError(f"Exchange {exchange_id} not connected")
        
        try:
            order_info = await exchange.fetch_order(order_id, symbol)
            await self._track_api_call(exchange_id)
            return order_info
            
        except Exception as e:
            self.logger.error(f"Failed to get order status for {order_id}: {e}")
            raise
    
    @synchronized
    async def update_balance(self, exchange_id: str) -> ExchangeBalance:
        """Update balance for an exchange"""
        exchange = self.exchanges.get(exchange_id)
        if not exchange:
            raise ValueError(f"Exchange {exchange_id} not connected")
        
        try:
            # Fetch balance
            balance_data = await exchange.fetch_balance()
            await self._track_api_call(exchange_id)
            
            # Create balance object
            balance = ExchangeBalance(
                exchange_id=exchange_id,
                timestamp=datetime.utcnow()
            )
            
            # Parse spot balances
            if 'free' in balance_data and 'used' in balance_data:
                for currency in balance_data['total']:
                    total = balance_data['total'][currency]
                    if total > 0:
                        balance.spot_balances[currency] = Decimal(str(total))
            
            # Calculate total values (simplified)
            # In production, would fetch current prices
            btc_price = Decimal("50000")  # Placeholder
            
            total_btc = Decimal("0")
            for currency, amount in balance.spot_balances.items():
                if currency == "BTC":
                    total_btc += amount
                elif currency == "USDT":
                    total_btc += amount / btc_price
                # Add more conversions as needed
            
            balance.total_btc_value = total_btc
            balance.total_usd_value = total_btc * btc_price
            
            # Store balance
            self.balances[exchange_id] = balance
            self.balance_history[exchange_id].append(balance)
            
            # Record metrics
            self.metrics_collector.record_exchange_balance(exchange_id, balance)
            
            return balance
            
        except Exception as e:
            self.logger.error(f"Failed to update balance for {exchange_id}: {e}")
            raise
    
    async def get_ticker(self, symbol: str, exchange_id: str) -> Dict[str, Any]:
        """Get ticker data from exchange"""
        exchange = self.exchanges.get(exchange_id)
        if not exchange:
            raise ValueError(f"Exchange {exchange_id} not connected")
        
        try:
            ticker = await exchange.fetch_ticker(symbol)
            await self._track_api_call(exchange_id)
            return ticker
            
        except Exception as e:
            self.logger.error(f"Failed to get ticker for {symbol} on {exchange_id}: {e}")
            raise
    
    async def get_orderbook(self, symbol: str, exchange_id: str, limit: int = 20) -> Dict[str, Any]:
        """Get orderbook from exchange"""
        exchange = self.exchanges.get(exchange_id)
        if not exchange:
            raise ValueError(f"Exchange {exchange_id} not connected")
        
        try:
            orderbook = await exchange.fetch_order_book(symbol, limit)
            await self._track_api_call(exchange_id)
            return orderbook
            
        except Exception as e:
            self.logger.error(f"Failed to get orderbook for {symbol} on {exchange_id}: {e}")
            raise
    
    async def get_trades(self, symbol: str, exchange_id: str, limit: int = 100) -> List[Dict[str, Any]]:
        """Get recent trades from exchange"""
        exchange = self.exchanges.get(exchange_id)
        if not exchange:
            raise ValueError(f"Exchange {exchange_id} not connected")
        
        try:
            trades = await exchange.fetch_trades(symbol, limit=limit)
            await self._track_api_call(exchange_id)
            return trades
            
        except Exception as e:
            self.logger.error(f"Failed to get trades for {symbol} on {exchange_id}: {e}")
            raise
    
    def get_connected_exchanges(self) -> List[str]:
        """Get list of connected exchanges"""
        return [
            exchange_id
            for exchange_id, info in self.exchange_info.items()
            if info.status == ExchangeStatus.CONNECTED
        ]
    
    def get_exchange_info(self, exchange_id: str) -> Optional[ExchangeInfo]:
        """Get information about an exchange"""
        return self.exchange_info.get(exchange_id)
    
    def get_market_info(self, symbol: str, exchange_id: str) -> Optional[MarketInfo]:
        """Get market trading rules"""
        return self.market_info.get((exchange_id, symbol))
    
    async def check_connectivity(self) -> bool:
        """Check if at least one exchange is connected"""
        return any(
            info.status == ExchangeStatus.CONNECTED
            for info in self.exchange_info.values()
        )
    
    def get_total_balance(self) -> Dict[str, Decimal]:
        """Get total balance across all exchanges"""
        total_btc = Decimal("0")
        total_usd = Decimal("0")
        
        for balance in self.balances.values():
            total_btc += balance.total_btc_value
            total_usd += balance.total_usd_value
        
        return {
            "BTC": total_btc,
            "USD": total_usd
        }
    
    def get_best_exchange_for_order(self, symbol: str, side: str, quantity: Decimal) -> Optional[str]:
        """Find best exchange for an order based on fees and liquidity"""
        best_exchange = None
        best_score = float('inf')
        
        for exchange_id in self.get_connected_exchanges():
            # Check if market exists
            market_info = self.get_market_info(symbol, exchange_id)
            if not market_info or not market_info.is_active:
                continue
            
            # Check quantity constraints
            if quantity < market_info.min_quantity or quantity > market_info.max_quantity:
                continue
            
            # Calculate score based on fees
            info = self.exchange_info[exchange_id]
            if side == "BUY":
                fee = info.taker_fee  # Assume taker for now
            else:
                fee = info.maker_fee  # Assume maker for sells
            
            score = float(fee)
            
            # Could add liquidity scoring here
            
            if score < best_score:
                best_score = score
                best_exchange = exchange_id
        
        return best_exchange
    
    async def _load_exchange_configs(self) -> None:
        """Load exchange configurations"""
        exchanges_config = self.config.get("exchanges", {})
        
        # Default exchange configurations
        default_exchanges = {
            "binance": {
                "name": "Binance",
                "type": "hybrid",
                "supports_futures": True,
                "supports_margin": True,
                "maker_fee": "0.001",
                "taker_fee": "0.001",
                "rate_limit": 1200
            },
            "hyperliquid": {
                "name": "Hyperliquid",
                "type": "dex",
                "supports_futures": True,
                "maker_fee": "0.00025",
                "taker_fee": "0.0005",
                "rate_limit": 100
            },
            "mexc": {
                "name": "MEXC",
                "type": "spot",
                "supports_futures": True,
                "maker_fee": "0.002",
                "taker_fee": "0.002",
                "rate_limit": 600
            },
            "bybit": {
                "name": "Bybit",
                "type": "futures",
                "supports_futures": True,
                "maker_fee": "0.0001",
                "taker_fee": "0.0006",
                "rate_limit": 600
            },
            "okx": {
                "name": "OKX",
                "type": "hybrid",
                "supports_futures": True,
                "supports_options": True,
                "maker_fee": "0.0008",
                "taker_fee": "0.001",
                "rate_limit": 300
            }
        }
        
        # Create exchange info objects
        for exchange_id, default_config in default_exchanges.items():
            if exchange_id in exchanges_config:
                # Merge with user config
                config = {**default_config, **exchanges_config[exchange_id]}
                
                info = ExchangeInfo(
                    exchange_id=exchange_id,
                    name=config["name"],
                    exchange_type=ExchangeType(config["type"]),
                    api_key=config.get("api_key", ""),
                    api_secret=config.get("api_secret", ""),
                    api_password=config.get("api_password"),
                    supports_futures=config.get("supports_futures", False),
                    supports_options=config.get("supports_options", False),
                    supports_margin=config.get("supports_margin", False),
                    maker_fee=Decimal(config.get("maker_fee", "0.001")),
                    taker_fee=Decimal(config.get("taker_fee", "0.001")),
                    rate_limit=config.get("rate_limit", 1200)
                )
                
                self.exchange_info[exchange_id] = info
    
    async def _update_market_info(self, exchange_id: str) -> None:
        """Update market information for an exchange"""
        exchange = self.exchanges.get(exchange_id)
        if not exchange:
            return
        
        try:
            markets = exchange.markets
            
            for symbol, market in markets.items():
                if market['active']:
                    market_info = MarketInfo(
                        symbol=symbol,
                        exchange_id=exchange_id,
                        min_quantity=Decimal(str(market['limits']['amount']['min'] or 0)),
                        max_quantity=Decimal(str(market['limits']['amount']['max'] or 999999999)),
                        quantity_step=Decimal(str(market['precision']['amount'] or 0.00000001)),
                        min_price=Decimal(str(market['limits']['price']['min'] or 0)),
                        max_price=Decimal(str(market['limits']['price']['max'] or 999999999)),
                        price_step=Decimal(str(market['precision']['price'] or 0.00000001)),
                        min_notional=Decimal(str(market['limits']['cost']['min'] or 0)),
                        is_active=market['active'],
                        is_spot_trading_allowed=market['spot'],
                        is_margin_trading_allowed=market.get('margin', False)
                    )
                    
                    self.market_info[(exchange_id, symbol)] = market_info
            
            self.markets_last_updated[exchange_id] = datetime.utcnow()
            self.logger.info(f"Updated {len(markets)} markets for {exchange_id}")
            
        except Exception as e:
            self.logger.error(f"Failed to update markets for {exchange_id}: {e}")
    
    async def _track_api_call(self, exchange_id: str) -> None:
        """Track API call for rate limiting"""
        info = self.exchange_info.get(exchange_id)
        if info:
            info.rate_limit_remaining -= 1
            self.api_call_times[exchange_id].append(datetime.utcnow())
            
            # Update latency
            exchange = self.exchanges.get(exchange_id)
            if exchange and hasattr(exchange, 'last_response_headers'):
                # Some exchanges provide latency info in headers
                pass
    
    async def _connection_monitor_loop(self) -> None:
        """Monitor and reconnect disconnected exchanges"""
        while True:
            try:
                for exchange_id, info in self.exchange_info.items():
                    if info.status == ExchangeStatus.DISCONNECTED:
                        # Check if we should reconnect
                        delay = self.reconnect_delays.get(exchange_id, 0)
                        if delay == 0 or (info.connected_at and 
                            (datetime.utcnow() - info.connected_at).total_seconds() > delay):
                            asyncio.create_task(self.connect_exchange(exchange_id))
                    
                    elif info.status == ExchangeStatus.ERROR:
                        # Retry after backoff delay
                        delay = self.reconnect_delays.get(exchange_id, 60)
                        await asyncio.sleep(delay)
                        asyncio.create_task(self.connect_exchange(exchange_id))
                
                await asyncio.sleep(30)  # Check every 30 seconds
                
            except Exception as e:
                self.logger.error(f"Error in connection monitor: {e}")
                await asyncio.sleep(30)
    
    async def _balance_update_loop(self) -> None:
        """Periodically update balances"""
        while True:
            try:
                for exchange_id in self.get_connected_exchanges():
                    try:
                        await self.update_balance(exchange_id)
                    except Exception as e:
                        self.logger.error(f"Failed to update balance for {exchange_id}: {e}")
                
                await asyncio.sleep(self.update_balance_interval)
                
            except Exception as e:
                self.logger.error(f"Error in balance update loop: {e}")
                await asyncio.sleep(self.update_balance_interval)
    
    async def _health_check_loop(self) -> None:
        """Perform health checks on connected exchanges"""
        while True:
            try:
                for exchange_id in self.get_connected_exchanges():
                    exchange = self.exchanges.get(exchange_id)
                    if exchange:
                        try:
                            # Simple ping test
                            start_time = datetime.utcnow()
                            await exchange.fetch_time()
                            latency = (datetime.utcnow() - start_time).total_seconds() * 1000
                            
                            info = self.exchange_info[exchange_id]
                            info.last_ping = datetime.utcnow()
                            info.latency_ms = latency
                            
                        except Exception as e:
                            self.logger.warning(f"Health check failed for {exchange_id}: {e}")
                            info = self.exchange_info[exchange_id]
                            info.error_count += 1
                            
                            if info.error_count > 5:
                                info.status = ExchangeStatus.ERROR
                                await self.disconnect_exchange(exchange_id)
                
                await asyncio.sleep(self.health_check_interval)
                
            except Exception as e:
                self.logger.error(f"Error in health check loop: {e}")
                await asyncio.sleep(self.health_check_interval)
    
    async def _rate_limit_reset_loop(self) -> None:
        """Reset rate limits periodically"""
        while True:
            try:
                current_time = datetime.utcnow()
                
                for exchange_id, info in self.exchange_info.items():
                    # Reset rate limit every minute
                    if not info.rate_limit_reset or current_time > info.rate_limit_reset:
                        info.rate_limit_remaining = info.rate_limit
                        info.rate_limit_reset = current_time + timedelta(minutes=1)
                        
                        # Clear old API call times
                        cutoff_time = current_time - timedelta(minutes=1)
                        self.api_call_times[exchange_id] = deque(
                            [t for t in self.api_call_times[exchange_id] if t > cutoff_time],
                            maxlen=100
                        )
                
                await asyncio.sleep(10)  # Check every 10 seconds
                
            except Exception as e:
                self.logger.error(f"Error in rate limit reset loop: {e}")
                await asyncio.sleep(10)
    
    def get_status(self) -> Dict[str, Any]:
        """Get exchange coordinator status"""
        connected_exchanges = self.get_connected_exchanges()
        total_balance = self.get_total_balance()
        
        exchange_statuses = {}
        for exchange_id, info in self.exchange_info.items():
            exchange_statuses[exchange_id] = {
                "status": info.status.value,
                "latency_ms": info.latency_ms,
                "error_count": info.error_count,
                "rate_limit_remaining": info.rate_limit_remaining,
                "last_ping": info.last_ping.isoformat() if info.last_ping else None
            }
        
        return {
            "total_exchanges": len(self.exchange_info),
            "connected_exchanges": len(connected_exchanges),
            "exchanges": exchange_statuses,
            "total_balance": {
                "BTC": float(total_balance["BTC"]),
                "USD": float(total_balance["USD"])
            }
        }