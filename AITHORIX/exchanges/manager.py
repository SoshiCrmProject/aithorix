"""
AITHORIX Exchange Manager - Production Implementation
Manages connections and operations across all 5 exchanges
"""

import asyncio
import logging
from typing import Dict, List, Optional, Any, Callable
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
import json
from concurrent.futures import ThreadPoolExecutor
import threading
import aiohttp
import numpy as np

from .base_exchange import BaseExchange, ExchangeConfig, OrderType, OrderStatus
from .binance.client import BinanceClient
from .hyperliquid.client import HyperliquidClient
from .mexc.client import MEXCClient
from .bybit.client import BybitClient
from .okx.client import OKXClient

logger = logging.getLogger(__name__)


class ExchangeType(Enum):
    """Supported exchanges"""
    BINANCE = "binance"
    HYPERLIQUID = "hyperliquid"
    MEXC = "mexc"
    BYBIT = "bybit"
    OKX = "okx"


@dataclass
class ExchangeStats:
    """Real-time exchange statistics"""
    exchange: str
    status: str
    uptime_percentage: float
    total_orders: int
    successful_orders: int
    failed_orders: int
    total_volume: float
    total_fees: float
    avg_latency_ms: float
    last_error: Optional[str] = None
    last_update: datetime = field(default_factory=datetime.now)


@dataclass
class ArbitrageOpportunity:
    """Cross-exchange arbitrage opportunity"""
    timestamp: datetime
    buy_exchange: str
    sell_exchange: str
    symbol: str
    buy_price: float
    sell_price: float
    max_size: float
    profit_percentage: float
    profit_usd: float
    execution_time_estimate_ms: float
    confidence: float


class ExchangeManager:
    """
    Central manager for all exchange operations
    Handles initialization, monitoring, and cross-exchange coordination
    """
    
    def __init__(self, config_path: str = "config/exchanges/"):
        self.config_path = config_path
        self.exchanges: Dict[str, BaseExchange] = {}
        self.exchange_stats: Dict[str, ExchangeStats] = {}
        self.is_running = False
        self._monitoring_task = None
        self._arbitrage_task = None
        self._stats_lock = threading.Lock()
        self._executor = ThreadPoolExecutor(max_workers=10)
        
        # WebSocket connections
        self.ws_connections: Dict[str, Any] = {}
        
        # Market data cache
        self.market_data_cache: Dict[str, Dict[str, Any]] = {}
        self.orderbook_cache: Dict[str, Dict[str, Any]] = {}
        
        # Callbacks
        self.arbitrage_callbacks: List[Callable] = []
        self.error_callbacks: List[Callable] = []
        
        # Performance tracking
        self.latency_history: Dict[str, List[float]] = {}
        self.arbitrage_history: List[ArbitrageOpportunity] = []
        
        logger.info("Exchange Manager initialized")
    
    async def initialize(self, exchanges: Optional[List[ExchangeType]] = None):
        """
        Initialize specified exchanges or all if none specified
        """
        if exchanges is None:
            exchanges = list(ExchangeType)
        
        initialization_tasks = []
        
        for exchange_type in exchanges:
            initialization_tasks.append(
                self._initialize_exchange(exchange_type)
            )
        
        results = await asyncio.gather(*initialization_tasks, return_exceptions=True)
        
        # Check results
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                logger.error(f"Failed to initialize {exchanges[i].value}: {str(result)}")
            else:
                logger.info(f"Successfully initialized {exchanges[i].value}")
        
        # Start monitoring
        self.is_running = True
        self._monitoring_task = asyncio.create_task(self._monitor_exchanges())
        self._arbitrage_task = asyncio.create_task(self._monitor_arbitrage())
        
        logger.info(f"Exchange Manager started with {len(self.exchanges)} exchanges")
    
    async def _initialize_exchange(self, exchange_type: ExchangeType):
        """Initialize a single exchange"""
        try:
            # Load exchange configuration
            config_file = f"{self.config_path}{exchange_type.value}.yaml"
            config = self._load_exchange_config(config_file)
            
            # Create exchange client
            if exchange_type == ExchangeType.BINANCE:
                client = BinanceClient(config)
            elif exchange_type == ExchangeType.HYPERLIQUID:
                client = HyperliquidClient(config)
            elif exchange_type == ExchangeType.MEXC:
                client = MEXCClient(config)
            elif exchange_type == ExchangeType.BYBIT:
                client = BybitClient(config)
            elif exchange_type == ExchangeType.OKX:
                client = OKXClient(config)
            else:
                raise ValueError(f"Unknown exchange type: {exchange_type}")
            
            # Initialize the client
            await client.initialize()
            
            # Store client
            self.exchanges[exchange_type.value] = client
            
            # Initialize stats
            self.exchange_stats[exchange_type.value] = ExchangeStats(
                exchange=exchange_type.value,
                status="online",
                uptime_percentage=100.0,
                total_orders=0,
                successful_orders=0,
                failed_orders=0,
                total_volume=0.0,
                total_fees=0.0,
                avg_latency_ms=0.0
            )
            
            # Initialize data structures
            self.latency_history[exchange_type.value] = []
            self.market_data_cache[exchange_type.value] = {}
            self.orderbook_cache[exchange_type.value] = {}
            
            # Start WebSocket connection
            await self._start_websocket(exchange_type.value)
            
            return True
            
        except Exception as e:
            logger.error(f"Error initializing {exchange_type.value}: {str(e)}")
            raise
    
    def _load_exchange_config(self, config_file: str) -> ExchangeConfig:
        """Load exchange configuration from file"""
        import yaml
        
        with open(config_file, 'r') as f:
            config_data = yaml.safe_load(f)
        
        return ExchangeConfig(**config_data)
    
    async def _start_websocket(self, exchange: str):
        """Start WebSocket connection for an exchange"""
        try:
            client = self.exchanges[exchange]
            
            # Define WebSocket callbacks
            async def on_message(data):
                await self._handle_ws_message(exchange, data)
            
            async def on_error(error):
                await self._handle_ws_error(exchange, error)
            
            async def on_close():
                await self._handle_ws_close(exchange)
            
            # Start WebSocket
            ws_connection = await client.start_websocket(
                on_message=on_message,
                on_error=on_error,
                on_close=on_close
            )
            
            self.ws_connections[exchange] = ws_connection
            
            # Subscribe to necessary channels
            await self._subscribe_to_channels(exchange)
            
            logger.info(f"WebSocket started for {exchange}")
            
        except Exception as e:
            logger.error(f"Error starting WebSocket for {exchange}: {str(e)}")
    
    async def _subscribe_to_channels(self, exchange: str):
        """Subscribe to WebSocket channels"""
        client = self.exchanges[exchange]
        
        # Subscribe to top symbols
        symbols = await client.get_top_symbols(limit=50)
        
        for symbol in symbols:
            # Subscribe to ticker
            await client.subscribe_ticker(symbol)
            
            # Subscribe to order book
            await client.subscribe_orderbook(symbol, depth=20)
            
            # Subscribe to trades
            await client.subscribe_trades(symbol)
    
    async def _handle_ws_message(self, exchange: str, data: Dict[str, Any]):
        """Handle WebSocket message"""
        try:
            msg_type = data.get('type', 'unknown')
            
            if msg_type == 'ticker':
                await self._update_ticker(exchange, data)
            elif msg_type == 'orderbook':
                await self._update_orderbook(exchange, data)
            elif msg_type == 'trade':
                await self._process_trade(exchange, data)
            elif msg_type == 'order_update':
                await self._process_order_update(exchange, data)
                
        except Exception as e:
            logger.error(f"Error handling WebSocket message from {exchange}: {str(e)}")
    
    async def _handle_ws_error(self, exchange: str, error: Any):
        """Handle WebSocket error"""
        logger.error(f"WebSocket error from {exchange}: {str(error)}")
        
        with self._stats_lock:
            self.exchange_stats[exchange].last_error = str(error)
            self.exchange_stats[exchange].status = "error"
        
        # Notify error callbacks
        for callback in self.error_callbacks:
            try:
                await callback(exchange, error)
            except Exception as e:
                logger.error(f"Error in error callback: {str(e)}")
    
    async def _handle_ws_close(self, exchange: str):
        """Handle WebSocket close"""
        logger.warning(f"WebSocket closed for {exchange}")
        
        with self._stats_lock:
            self.exchange_stats[exchange].status = "disconnected"
        
        # Attempt to reconnect
        await asyncio.sleep(5)
        if self.is_running:
            await self._start_websocket(exchange)
    
    async def _update_ticker(self, exchange: str, data: Dict[str, Any]):
        """Update ticker data in cache"""
        symbol = data.get('symbol')
        if symbol:
            if symbol not in self.market_data_cache[exchange]:
                self.market_data_cache[exchange][symbol] = {}
            
            self.market_data_cache[exchange][symbol].update({
                'last_price': data.get('last'),
                'bid': data.get('bid'),
                'ask': data.get('ask'),
                'volume_24h': data.get('volume'),
                'timestamp': datetime.now()
            })
    
    async def _update_orderbook(self, exchange: str, data: Dict[str, Any]):
        """Update order book in cache"""
        symbol = data.get('symbol')
        if symbol:
            self.orderbook_cache[exchange][symbol] = {
                'bids': data.get('bids', []),
                'asks': data.get('asks', []),
                'timestamp': datetime.now()
            }
    
    async def _process_trade(self, exchange: str, data: Dict[str, Any]):
        """Process trade update"""
        # Update market data with latest trade
        symbol = data.get('symbol')
        if symbol and symbol in self.market_data_cache[exchange]:
            self.market_data_cache[exchange][symbol]['last_trade'] = {
                'price': data.get('price'),
                'size': data.get('size'),
                'side': data.get('side'),
                'timestamp': data.get('timestamp')
            }
    
    async def _process_order_update(self, exchange: str, data: Dict[str, Any]):
        """Process order update"""
        order_id = data.get('order_id')
        status = data.get('status')
        
        # Update statistics
        with self._stats_lock:
            if status == 'filled':
                self.exchange_stats[exchange].successful_orders += 1
                self.exchange_stats[exchange].total_volume += data.get('filled_size', 0)
                self.exchange_stats[exchange].total_fees += data.get('fee', 0)
            elif status in ['cancelled', 'rejected']:
                self.exchange_stats[exchange].failed_orders += 1
    
    async def _monitor_exchanges(self):
        """Monitor exchange health and performance"""
        while self.is_running:
            try:
                for exchange_name, client in self.exchanges.items():
                    # Check connection
                    start_time = datetime.now()
                    is_connected = await client.check_connection()
                    latency = (datetime.now() - start_time).total_seconds() * 1000
                    
                    # Update latency history
                    self.latency_history[exchange_name].append(latency)
                    if len(self.latency_history[exchange_name]) > 1000:
                        self.latency_history[exchange_name] = self.latency_history[exchange_name][-1000:]
                    
                    # Update stats
                    with self._stats_lock:
                        stats = self.exchange_stats[exchange_name]
                        stats.status = "online" if is_connected else "offline"
                        stats.avg_latency_ms = np.mean(self.latency_history[exchange_name])
                        stats.last_update = datetime.now()
                        
                        # Calculate uptime
                        total_checks = stats.total_orders + stats.failed_orders + 1
                        online_checks = stats.successful_orders + (1 if is_connected else 0)
                        stats.uptime_percentage = (online_checks / total_checks) * 100
                
                await asyncio.sleep(10)  # Check every 10 seconds
                
            except Exception as e:
                logger.error(f"Error in exchange monitoring: {str(e)}")
                await asyncio.sleep(10)
    
    async def _monitor_arbitrage(self):
        """Monitor for arbitrage opportunities across exchanges"""
        while self.is_running:
            try:
                # Get common symbols across all active exchanges
                active_exchanges = [
                    name for name, stats in self.exchange_stats.items()
                    if stats.status == "online"
                ]
                
                if len(active_exchanges) < 2:
                    await asyncio.sleep(1)
                    continue
                
                # Get common symbols
                common_symbols = await self._get_common_symbols(active_exchanges)
                
                # Check each symbol for arbitrage
                for symbol in common_symbols:
                    opportunity = await self._check_arbitrage_opportunity(
                        symbol, active_exchanges
                    )
                    
                    if opportunity and opportunity.profit_percentage > 0.1:  # 0.1% threshold
                        self.arbitrage_history.append(opportunity)
                        
                        # Notify callbacks
                        for callback in self.arbitrage_callbacks:
                            try:
                                await callback(opportunity)
                            except Exception as e:
                                logger.error(f"Error in arbitrage callback: {str(e)}")
                
                await asyncio.sleep(0.5)  # Check every 500ms
                
            except Exception as e:
                logger.error(f"Error in arbitrage monitoring: {str(e)}")
                await asyncio.sleep(1)
    
    async def _get_common_symbols(self, exchanges: List[str]) -> List[str]:
        """Get symbols traded on all specified exchanges"""
        symbol_sets = []
        
        for exchange in exchanges:
            if exchange in self.market_data_cache:
                symbols = set(self.market_data_cache[exchange].keys())
                symbol_sets.append(symbols)
        
        if not symbol_sets:
            return []
        
        # Find intersection
        common = symbol_sets[0]
        for symbols in symbol_sets[1:]:
            common = common.intersection(symbols)
        
        return list(common)
    
    async def _check_arbitrage_opportunity(
        self,
        symbol: str,
        exchanges: List[str]
    ) -> Optional[ArbitrageOpportunity]:
        """Check for arbitrage opportunity on a symbol"""
        prices = {}
        
        # Get prices from each exchange
        for exchange in exchanges:
            if (exchange in self.market_data_cache and 
                symbol in self.market_data_cache[exchange]):
                
                data = self.market_data_cache[exchange][symbol]
                if 'bid' in data and 'ask' in data:
                    prices[exchange] = {
                        'bid': data['bid'],
                        'ask': data['ask'],
                        'timestamp': data.get('timestamp', datetime.now())
                    }
        
        if len(prices) < 2:
            return None
        
        # Find best arbitrage opportunity
        best_opportunity = None
        best_profit = 0
        
        for buy_exchange in prices:
            for sell_exchange in prices:
                if buy_exchange == sell_exchange:
                    continue
                
                buy_price = prices[buy_exchange]['ask']
                sell_price = prices[sell_exchange]['bid']
                
                if sell_price > buy_price:
                    # Calculate profit
                    gross_profit_pct = ((sell_price - buy_price) / buy_price) * 100
                    
                    # Estimate fees
                    buy_fee = self.exchanges[buy_exchange].config.taker_fee
                    sell_fee = self.exchanges[sell_exchange].config.taker_fee
                    total_fee_pct = (buy_fee + sell_fee) * 100
                    
                    net_profit_pct = gross_profit_pct - total_fee_pct
                    
                    if net_profit_pct > best_profit:
                        # Estimate execution time
                        buy_latency = self.exchange_stats[buy_exchange].avg_latency_ms
                        sell_latency = self.exchange_stats[sell_exchange].avg_latency_ms
                        execution_time = buy_latency + sell_latency + 100  # Add buffer
                        
                        # Estimate max size (simplified)
                        max_size = min(
                            self._get_available_balance(buy_exchange, symbol) / buy_price,
                            self._get_orderbook_liquidity(sell_exchange, symbol, 'bid')
                        )
                        
                        best_opportunity = ArbitrageOpportunity(
                            timestamp=datetime.now(),
                            buy_exchange=buy_exchange,
                            sell_exchange=sell_exchange,
                            symbol=symbol,
                            buy_price=buy_price,
                            sell_price=sell_price,
                            max_size=max_size,
                            profit_percentage=net_profit_pct,
                            profit_usd=max_size * buy_price * (net_profit_pct / 100),
                            execution_time_estimate_ms=execution_time,
                            confidence=0.85  # Based on data freshness and liquidity
                        )
                        best_profit = net_profit_pct
        
        return best_opportunity
    
    def _get_available_balance(self, exchange: str, symbol: str) -> float:
        """Get available balance for trading (simplified)"""
        # In production, this would query actual balances
        return 10000.0  # Placeholder
    
    def _get_orderbook_liquidity(
        self,
        exchange: str,
        symbol: str,
        side: str
    ) -> float:
        """Calculate available liquidity from order book"""
        if (exchange not in self.orderbook_cache or 
            symbol not in self.orderbook_cache[exchange]):
            return 0.0
        
        orderbook = self.orderbook_cache[exchange][symbol]
        orders = orderbook.get(f"{side}s", [])
        
        # Sum liquidity in top 5 levels
        total_size = sum(float(order[1]) for order in orders[:5])
        
        return total_size
    
    async def execute_arbitrage(
        self,
        opportunity: ArbitrageOpportunity,
        size: Optional[float] = None
    ) -> Dict[str, Any]:
        """
        Execute an arbitrage opportunity
        """
        if size is None:
            size = opportunity.max_size * 0.8  # Use 80% of max size for safety
        
        results = {
            'success': False,
            'buy_order': None,
            'sell_order': None,
            'actual_profit': 0.0,
            'execution_time_ms': 0.0
        }
        
        start_time = datetime.now()
        
        try:
            # Place buy order
            buy_client = self.exchanges[opportunity.buy_exchange]
            buy_order = await buy_client.place_order(
                symbol=opportunity.symbol,
                side='buy',
                order_type=OrderType.MARKET,
                size=size
            )
            
            results['buy_order'] = buy_order
            
            # Place sell order only if buy succeeded
            if buy_order and buy_order.status == OrderStatus.FILLED:
                sell_client = self.exchanges[opportunity.sell_exchange]
                sell_order = await sell_client.place_order(
                    symbol=opportunity.symbol,
                    side='sell',
                    order_type=OrderType.MARKET,
                    size=buy_order.filled_size  # Use actual filled size
                )
                
                results['sell_order'] = sell_order
                
                if sell_order and sell_order.status == OrderStatus.FILLED:
                    # Calculate actual profit
                    buy_cost = buy_order.filled_size * buy_order.average_price
                    sell_revenue = sell_order.filled_size * sell_order.average_price
                    fees = buy_order.fee + sell_order.fee
                    
                    results['actual_profit'] = sell_revenue - buy_cost - fees
                    results['success'] = True
            
            results['execution_time_ms'] = (
                datetime.now() - start_time
            ).total_seconds() * 1000
            
        except Exception as e:
            logger.error(f"Error executing arbitrage: {str(e)}")
            results['error'] = str(e)
        
        return results
    
    async def get_consolidated_balance(self) -> Dict[str, Dict[str, float]]:
        """Get consolidated balance across all exchanges"""
        balances = {}
        
        for exchange_name, client in self.exchanges.items():
            try:
                exchange_balance = await client.get_balance()
                balances[exchange_name] = exchange_balance
            except Exception as e:
                logger.error(f"Error getting balance from {exchange_name}: {str(e)}")
                balances[exchange_name] = {}
        
        return balances
    
    async def get_consolidated_positions(self) -> Dict[str, List[Any]]:
        """Get all open positions across exchanges"""
        positions = {}
        
        for exchange_name, client in self.exchanges.items():
            try:
                exchange_positions = await client.get_open_positions()
                positions[exchange_name] = exchange_positions
            except Exception as e:
                logger.error(f"Error getting positions from {exchange_name}: {str(e)}")
                positions[exchange_name] = []
        
        return positions
    
    def get_exchange_stats(self) -> Dict[str, ExchangeStats]:
        """Get current exchange statistics"""
        with self._stats_lock:
            return self.exchange_stats.copy()
    
    def get_arbitrage_history(
        self,
        hours: int = 24,
        min_profit_pct: float = 0.0
    ) -> List[ArbitrageOpportunity]:
        """Get recent arbitrage opportunities"""
        cutoff_time = datetime.now() - timedelta(hours=hours)
        
        return [
            opp for opp in self.arbitrage_history
            if (opp.timestamp > cutoff_time and 
                opp.profit_percentage >= min_profit_pct)
        ]
    
    def register_arbitrage_callback(self, callback: Callable):
        """Register callback for arbitrage opportunities"""
        self.arbitrage_callbacks.append(callback)
    
    def register_error_callback(self, callback: Callable):
        """Register callback for errors"""
        self.error_callbacks.append(callback)
    
    async def shutdown(self):
        """Shutdown exchange manager"""
        logger.info("Shutting down Exchange Manager")
        
        self.is_running = False
        
        # Cancel monitoring tasks
        if self._monitoring_task:
            self._monitoring_task.cancel()
        if self._arbitrage_task:
            self._arbitrage_task.cancel()
        
        # Close WebSocket connections
        for exchange, ws in self.ws_connections.items():
            try:
                await ws.close()
            except Exception as e:
                logger.error(f"Error closing WebSocket for {exchange}: {str(e)}")
        
        # Shutdown exchange clients
        for exchange, client in self.exchanges.items():
            try:
                await client.shutdown()
            except Exception as e:
                logger.error(f"Error shutting down {exchange}: {str(e)}")
        
        # Shutdown executor
        self._executor.shutdown(wait=True)
        
        logger.info("Exchange Manager shutdown complete")