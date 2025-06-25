"""
AITHORIX Market Data Engine
High-performance market data ingestion and processing

This module handles:
- Real-time market data ingestion from multiple exchanges
- Order book management and aggregation
- Trade flow analysis
- Market data normalization and distribution
"""

import asyncio
import logging
from typing import Dict, List, Optional, Any, Set, Tuple, Callable
from datetime import datetime, timedelta
from decimal import Decimal
from dataclasses import dataclass, field
from collections import defaultdict, deque
import numpy as np
import json
from enum import Enum

from utils.helpers import get_timestamp, synchronized
from monitoring.metrics import MetricsCollector


class MarketDataType(Enum):
    """Market data type enumeration"""
    TICKER = "ticker"
    ORDERBOOK = "orderbook"
    TRADES = "trades"
    OHLCV = "ohlcv"
    FUNDING = "funding"
    LIQUIDATIONS = "liquidations"


@dataclass
class Ticker:
    """Market ticker data"""
    symbol: str
    exchange: str
    timestamp: datetime
    bid: Decimal
    ask: Decimal
    last: Decimal
    volume_24h: Decimal
    quote_volume_24h: Decimal
    high_24h: Decimal
    low_24h: Decimal
    open_24h: Decimal
    change_24h: Decimal
    change_percent_24h: Decimal
    vwap_24h: Optional[Decimal] = None
    bid_size: Optional[Decimal] = None
    ask_size: Optional[Decimal] = None
    
    @property
    def mid_price(self) -> Decimal:
        """Calculate mid price"""
        return (self.bid + self.ask) / Decimal("2")
    
    @property
    def spread(self) -> Decimal:
        """Calculate bid-ask spread"""
        return self.ask - self.bid
    
    @property
    def spread_percentage(self) -> Decimal:
        """Calculate spread as percentage of mid price"""
        if self.mid_price == 0:
            return Decimal("0")
        return (self.spread / self.mid_price) * Decimal("100")


@dataclass
class OrderBookLevel:
    """Single level in order book"""
    price: Decimal
    quantity: Decimal
    order_count: Optional[int] = None
    
    @property
    def total_value(self) -> Decimal:
        """Calculate total value at this level"""
        return self.price * self.quantity


@dataclass
class OrderBook:
    """Order book snapshot"""
    symbol: str
    exchange: str
    timestamp: datetime
    bids: List[OrderBookLevel] = field(default_factory=list)
    asks: List[OrderBookLevel] = field(default_factory=list)
    sequence: Optional[int] = None
    
    def get_best_bid(self) -> Optional[OrderBookLevel]:
        """Get best bid level"""
        return self.bids[0] if self.bids else None
    
    def get_best_ask(self) -> Optional[OrderBookLevel]:
        """Get best ask level"""
        return self.asks[0] if self.asks else None
    
    @property
    def mid_price(self) -> Optional[Decimal]:
        """Calculate mid price from best bid/ask"""
        best_bid = self.get_best_bid()
        best_ask = self.get_best_ask()
        
        if best_bid and best_ask:
            return (best_bid.price + best_ask.price) / Decimal("2")
        return None
    
    @property
    def spread(self) -> Optional[Decimal]:
        """Calculate spread"""
        best_bid = self.get_best_bid()
        best_ask = self.get_best_ask()
        
        if best_bid and best_ask:
            return best_ask.price - best_bid.price
        return None
    
    def get_depth(self, side: str, levels: int = 10) -> Decimal:
        """Calculate total depth for given side and levels"""
        book_side = self.bids if side == "bid" else self.asks
        total = Decimal("0")
        
        for i, level in enumerate(book_side):
            if i >= levels:
                break
            total += level.quantity
        
        return total
    
    def get_weighted_price(self, side: str, quantity: Decimal) -> Optional[Decimal]:
        """Calculate weighted average price for given quantity"""
        book_side = self.bids if side == "bid" else self.asks
        remaining = quantity
        total_value = Decimal("0")
        total_quantity = Decimal("0")
        
        for level in book_side:
            if remaining <= 0:
                break
            
            level_quantity = min(remaining, level.quantity)
            total_value += level.price * level_quantity
            total_quantity += level_quantity
            remaining -= level_quantity
        
        if total_quantity > 0:
            return total_value / total_quantity
        return None
    
    def get_market_impact(self, side: str, quantity: Decimal) -> Optional[Decimal]:
        """Calculate market impact for given order size"""
        weighted_price = self.get_weighted_price(side, quantity)
        if not weighted_price or not self.mid_price:
            return None
        
        return abs(weighted_price - self.mid_price) / self.mid_price * Decimal("100")


@dataclass
class Trade:
    """Individual trade data"""
    symbol: str
    exchange: str
    timestamp: datetime
    trade_id: str
    price: Decimal
    quantity: Decimal
    side: str  # "buy" or "sell"
    is_buyer_maker: bool = False
    
    @property
    def value(self) -> Decimal:
        """Calculate trade value"""
        return self.price * self.quantity


@dataclass
class OHLCV:
    """OHLCV candle data"""
    symbol: str
    exchange: str
    timestamp: datetime
    interval: str  # "1m", "5m", "1h", etc.
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: Decimal
    quote_volume: Decimal
    trade_count: Optional[int] = None
    
    @property
    def is_bullish(self) -> bool:
        """Check if candle is bullish"""
        return self.close > self.open
    
    @property
    def body_size(self) -> Decimal:
        """Calculate candle body size"""
        return abs(self.close - self.open)
    
    @property
    def upper_shadow(self) -> Decimal:
        """Calculate upper shadow size"""
        return self.high - max(self.open, self.close)
    
    @property
    def lower_shadow(self) -> Decimal:
        """Calculate lower shadow size"""
        return min(self.open, self.close) - self.low


@dataclass
class MarketData:
    """Aggregated market data container"""
    symbol: str
    timestamp: datetime
    ticker: Optional[Ticker] = None
    orderbook: Optional[OrderBook] = None
    recent_trades: List[Trade] = field(default_factory=list)
    ohlcv: Dict[str, OHLCV] = field(default_factory=dict)  # interval -> OHLCV
    metadata: Dict[str, Any] = field(default_factory=dict)


class MarketDataEngine:
    """
    High-performance market data engine
    
    Manages real-time market data ingestion, processing, and distribution
    across multiple exchanges with minimal latency.
    """
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.logger = logging.getLogger("AITHORIX.MarketDataEngine")
        
        # Data storage
        self.tickers: Dict[str, Dict[str, Ticker]] = defaultdict(dict)  # symbol -> exchange -> ticker
        self.orderbooks: Dict[str, Dict[str, OrderBook]] = defaultdict(dict)  # symbol -> exchange -> orderbook
        self.trades: Dict[str, deque] = defaultdict(lambda: deque(maxlen=1000))  # symbol -> recent trades
        self.ohlcv_data: Dict[str, Dict[str, deque]] = defaultdict(lambda: defaultdict(lambda: deque(maxlen=1000)))
        
        # Aggregated data
        self.market_data: Dict[str, MarketData] = {}
        
        # Subscriptions
        self.subscriptions: Dict[str, Set[MarketDataType]] = defaultdict(set)
        self.callbacks: Dict[str, List[Callable]] = defaultdict(list)
        
        # Exchange handlers
        self.exchange_handlers: Dict[str, Any] = {}
        
        # Performance tracking
        self.metrics_collector = MetricsCollector()
        self.data_latencies: deque = deque(maxlen=1000)
        self.update_counts: Dict[str, int] = defaultdict(int)
        
        # Configuration
        self.orderbook_depth = config.get("orderbook_depth", 20)
        self.trade_history_size = config.get("trade_history_size", 1000)
        self.aggregation_interval = config.get("aggregation_interval", 0.1)  # 100ms
        self.enable_data_validation = config.get("enable_data_validation", True)
        
        # State
        self._lock = asyncio.Lock()
        self.is_initialized = False
        self.is_running = False
    
    async def initialize(self) -> None:
        """Initialize the market data engine"""
        self.logger.info("Initializing Market Data Engine...")
        
        # Initialize exchange handlers
        await self._initialize_exchange_handlers()
        
        # Start background tasks
        asyncio.create_task(self._aggregation_task())
        asyncio.create_task(self._cleanup_task())
        asyncio.create_task(self._monitoring_task())
        
        self.is_initialized = True
        self.logger.info("Market Data Engine initialized")
    
    async def start(self) -> None:
        """Start market data feeds"""
        self.logger.info("Starting market data feeds...")
        self.is_running = True
        
        # Start exchange connections
        for exchange_name, handler in self.exchange_handlers.items():
            try:
                await handler.connect()
                self.logger.info(f"Connected to {exchange_name} market data")
            except Exception as e:
                self.logger.error(f"Failed to connect to {exchange_name}: {e}")
    
    async def stop(self) -> None:
        """Stop market data feeds"""
        self.logger.info("Stopping market data feeds...")
        self.is_running = False
        
        # Disconnect from exchanges
        for exchange_name, handler in self.exchange_handlers.items():
            try:
                await handler.disconnect()
                self.logger.info(f"Disconnected from {exchange_name}")
            except Exception as e:
                self.logger.error(f"Error disconnecting from {exchange_name}: {e}")
    
    async def subscribe(
        self, 
        symbol: str, 
        data_types: Optional[List[MarketDataType]] = None,
        callback: Optional[Callable] = None
    ) -> None:
        """Subscribe to market data for a symbol"""
        if data_types is None:
            data_types = [MarketDataType.TICKER, MarketDataType.ORDERBOOK, MarketDataType.TRADES]
        
        # Add to subscriptions
        for data_type in data_types:
            self.subscriptions[symbol].add(data_type)
        
        # Add callback if provided
        if callback:
            self.callbacks[symbol].append(callback)
        
        # Subscribe on all exchanges
        for exchange_name, handler in self.exchange_handlers.items():
            try:
                await handler.subscribe(symbol, data_types)
                self.logger.info(f"Subscribed to {symbol} on {exchange_name}")
            except Exception as e:
                self.logger.error(f"Failed to subscribe to {symbol} on {exchange_name}: {e}")
    
    async def unsubscribe(self, symbol: str) -> None:
        """Unsubscribe from market data for a symbol"""
        # Remove subscriptions
        self.subscriptions.pop(symbol, None)
        self.callbacks.pop(symbol, None)
        
        # Unsubscribe on all exchanges
        for exchange_name, handler in self.exchange_handlers.items():
            try:
                await handler.unsubscribe(symbol)
                self.logger.info(f"Unsubscribed from {symbol} on {exchange_name}")
            except Exception as e:
                self.logger.error(f"Failed to unsubscribe from {symbol} on {exchange_name}: {e}")
    
    @synchronized
    async def update_ticker(self, ticker: Ticker) -> None:
        """Update ticker data"""
        # Validate data if enabled
        if self.enable_data_validation and not self._validate_ticker(ticker):
            self.logger.warning(f"Invalid ticker data: {ticker}")
            return
        
        # Calculate latency
        latency = (datetime.utcnow() - ticker.timestamp).total_seconds()
        self.data_latencies.append(latency)
        
        # Store ticker
        self.tickers[ticker.symbol][ticker.exchange] = ticker
        self.update_counts["ticker"] += 1
        
        # Update aggregated data
        await self._update_market_data(ticker.symbol)
        
        # Notify callbacks
        await self._notify_callbacks(ticker.symbol, MarketDataType.TICKER, ticker)
        
        # Record metrics
        self.metrics_collector.record_market_data_update("ticker", ticker.symbol, latency)
    
    @synchronized
    async def update_orderbook(self, orderbook: OrderBook) -> None:
        """Update order book data"""
        # Validate data if enabled
        if self.enable_data_validation and not self._validate_orderbook(orderbook):
            self.logger.warning(f"Invalid orderbook data: {orderbook.symbol}")
            return
        
        # Calculate latency
        latency = (datetime.utcnow() - orderbook.timestamp).total_seconds()
        self.data_latencies.append(latency)
        
        # Trim orderbook to configured depth
        if len(orderbook.bids) > self.orderbook_depth:
            orderbook.bids = orderbook.bids[:self.orderbook_depth]
        if len(orderbook.asks) > self.orderbook_depth:
            orderbook.asks = orderbook.asks[:self.orderbook_depth]
        
        # Store orderbook
        self.orderbooks[orderbook.symbol][orderbook.exchange] = orderbook
        self.update_counts["orderbook"] += 1
        
        # Update aggregated data
        await self._update_market_data(orderbook.symbol)
        
        # Notify callbacks
        await self._notify_callbacks(orderbook.symbol, MarketDataType.ORDERBOOK, orderbook)
        
        # Record metrics
        self.metrics_collector.record_market_data_update("orderbook", orderbook.symbol, latency)
    
    @synchronized
    async def update_trade(self, trade: Trade) -> None:
        """Update trade data"""
        # Validate data if enabled
        if self.enable_data_validation and not self._validate_trade(trade):
            self.logger.warning(f"Invalid trade data: {trade}")
            return
        
        # Store trade
        self.trades[trade.symbol].append(trade)
        self.update_counts["trade"] += 1
        
        # Update aggregated data
        await self._update_market_data(trade.symbol)
        
        # Notify callbacks
        await self._notify_callbacks(trade.symbol, MarketDataType.TRADES, trade)
        
        # Record metrics
        self.metrics_collector.record_market_data_update("trade", trade.symbol, 0)
    
    @synchronized
    async def update_ohlcv(self, ohlcv: OHLCV) -> None:
        """Update OHLCV data"""
        # Store OHLCV
        self.ohlcv_data[ohlcv.symbol][ohlcv.interval].append(ohlcv)
        self.update_counts["ohlcv"] += 1
        
        # Update aggregated data
        await self._update_market_data(ohlcv.symbol)
        
        # Notify callbacks
        await self._notify_callbacks(ohlcv.symbol, MarketDataType.OHLCV, ohlcv)
    
    def get_ticker(self, symbol: str, exchange: Optional[str] = None) -> Optional[Ticker]:
        """Get latest ticker for symbol"""
        if exchange:
            return self.tickers.get(symbol, {}).get(exchange)
        
        # Return best ticker across exchanges
        tickers = self.tickers.get(symbol, {})
        if tickers:
            # Return ticker with best price
            return min(tickers.values(), key=lambda t: t.spread_percentage)
        return None
    
    def get_orderbook(self, symbol: str, exchange: Optional[str] = None) -> Optional[OrderBook]:
        """Get latest orderbook for symbol"""
        if exchange:
            return self.orderbooks.get(symbol, {}).get(exchange)
        
        # Return aggregated orderbook across exchanges
        orderbooks = self.orderbooks.get(symbol, {})
        if orderbooks:
            return self._aggregate_orderbooks(list(orderbooks.values()))
        return None
    
    def get_recent_trades(self, symbol: str, limit: int = 100) -> List[Trade]:
        """Get recent trades for symbol"""
        trades = list(self.trades.get(symbol, []))
        return trades[-limit:]
    
    def get_ohlcv(self, symbol: str, interval: str, limit: int = 100) -> List[OHLCV]:
        """Get OHLCV data for symbol"""
        ohlcv_deque = self.ohlcv_data.get(symbol, {}).get(interval, deque())
        return list(ohlcv_deque)[-limit:]
    
    async def get_latest_data(self) -> Dict[str, Dict[str, Any]]:
        """Get latest market data for all subscribed symbols"""
        result = {}
        
        for symbol in self.subscriptions:
            market_data = self.market_data.get(symbol)
            if market_data:
                result[symbol] = {
                    "price": float(market_data.ticker.last) if market_data.ticker else None,
                    "bid": float(market_data.ticker.bid) if market_data.ticker else None,
                    "ask": float(market_data.ticker.ask) if market_data.ticker else None,
                    "volume": float(market_data.ticker.volume_24h) if market_data.ticker else None,
                    "timestamp": market_data.timestamp.isoformat()
                }
        
        return result
    
    def get_aggregated_orderbook(self, symbol: str, levels: int = 10) -> Optional[Dict[str, Any]]:
        """Get aggregated orderbook across all exchanges"""
        orderbooks = self.orderbooks.get(symbol, {})
        if not orderbooks:
            return None
        
        # Aggregate all orderbooks
        aggregated = self._aggregate_orderbooks(list(orderbooks.values()))
        
        return {
            "symbol": symbol,
            "timestamp": aggregated.timestamp.isoformat(),
            "bids": [
                {"price": float(level.price), "quantity": float(level.quantity)}
                for level in aggregated.bids[:levels]
            ],
            "asks": [
                {"price": float(level.price), "quantity": float(level.quantity)}
                for level in aggregated.asks[:levels]
            ],
            "spread": float(aggregated.spread) if aggregated.spread else None,
            "mid_price": float(aggregated.mid_price) if aggregated.mid_price else None
        }
    
    def _aggregate_orderbooks(self, orderbooks: List[OrderBook]) -> OrderBook:
        """Aggregate multiple orderbooks into one"""
        if not orderbooks:
            return None
        
        # Combine all bids and asks
        all_bids = defaultdict(Decimal)
        all_asks = defaultdict(Decimal)
        
        for ob in orderbooks:
            for bid in ob.bids:
                all_bids[bid.price] += bid.quantity
            for ask in ob.asks:
                all_asks[ask.price] += ask.quantity
        
        # Sort and create levels
        sorted_bids = sorted(all_bids.items(), key=lambda x: x[0], reverse=True)
        sorted_asks = sorted(all_asks.items(), key=lambda x: x[0])
        
        aggregated = OrderBook(
            symbol=orderbooks[0].symbol,
            exchange="AGGREGATE",
            timestamp=max(ob.timestamp for ob in orderbooks),
            bids=[OrderBookLevel(price=p, quantity=q) for p, q in sorted_bids],
            asks=[OrderBookLevel(price=p, quantity=q) for p, q in sorted_asks]
        )
        
        return aggregated
    
    async def _update_market_data(self, symbol: str) -> None:
        """Update aggregated market data for symbol"""
        # Get latest data from all sources
        tickers = self.tickers.get(symbol, {})
        orderbooks = self.orderbooks.get(symbol, {})
        
        if not tickers and not orderbooks:
            return
        
        # Create or update market data
        market_data = self.market_data.get(symbol)
        if not market_data:
            market_data = MarketData(symbol=symbol, timestamp=datetime.utcnow())
            self.market_data[symbol] = market_data
        
        # Update with best ticker
        if tickers:
            best_ticker = min(tickers.values(), key=lambda t: t.spread_percentage)
            market_data.ticker = best_ticker
        
        # Update with aggregated orderbook
        if orderbooks:
            market_data.orderbook = self._aggregate_orderbooks(list(orderbooks.values()))
        
        # Update recent trades
        market_data.recent_trades = self.get_recent_trades(symbol, limit=100)
        
        # Update timestamp
        market_data.timestamp = datetime.utcnow()
    
    async def _notify_callbacks(self, symbol: str, data_type: MarketDataType, data: Any) -> None:
        """Notify registered callbacks for symbol"""
        callbacks = self.callbacks.get(symbol, [])
        
        for callback in callbacks:
            try:
                await callback(symbol, data_type, data)
            except Exception as e:
                self.logger.error(f"Error in market data callback: {e}")
    
    def _validate_ticker(self, ticker: Ticker) -> bool:
        """Validate ticker data"""
        if ticker.bid <= 0 or ticker.ask <= 0:
            return False
        if ticker.bid >= ticker.ask:
            return False
        if ticker.volume_24h < 0:
            return False
        return True
    
    def _validate_orderbook(self, orderbook: OrderBook) -> bool:
        """Validate orderbook data"""
        if not orderbook.bids or not orderbook.asks:
            return False
        
        # Check bid prices are descending
        for i in range(1, len(orderbook.bids)):
            if orderbook.bids[i].price >= orderbook.bids[i-1].price:
                return False
        
        # Check ask prices are ascending
        for i in range(1, len(orderbook.asks)):
            if orderbook.asks[i].price <= orderbook.asks[i-1].price:
                return False
        
        # Check best bid < best ask
        if orderbook.bids[0].price >= orderbook.asks[0].price:
            return False
        
        return True
    
    def _validate_trade(self, trade: Trade) -> bool:
        """Validate trade data"""
        if trade.price <= 0 or trade.quantity <= 0:
            return False
        if trade.side not in ["buy", "sell"]:
            return False
        return True
    
    async def _initialize_exchange_handlers(self) -> None:
        """Initialize exchange-specific market data handlers"""
        # This would initialize actual exchange connections
        # For now, just log
        self.logger.info("Initializing exchange handlers...")
    
    async def _aggregation_task(self) -> None:
        """Periodically aggregate market data"""
        while True:
            try:
                # Aggregate data for all symbols
                for symbol in self.subscriptions:
                    await self._update_market_data(symbol)
                
                await asyncio.sleep(self.aggregation_interval)
                
            except Exception as e:
                self.logger.error(f"Error in aggregation task: {e}")
                await asyncio.sleep(1)
    
    async def _cleanup_task(self) -> None:
        """Clean up old market data"""
        while True:
            try:
                cutoff_time = datetime.utcnow() - timedelta(hours=1)
                
                # Clean up old trades
                for symbol, trades in self.trades.items():
                    while trades and trades[0].timestamp < cutoff_time:
                        trades.popleft()
                
                await asyncio.sleep(300)  # Every 5 minutes
                
            except Exception as e:
                self.logger.error(f"Error in cleanup task: {e}")
                await asyncio.sleep(300)
    
    async def _monitoring_task(self) -> None:
        """Monitor market data health"""
        while True:
            try:
                # Check data freshness
                stale_symbols = []
                for symbol, market_data in self.market_data.items():
                    age = (datetime.utcnow() - market_data.timestamp).total_seconds()
                    if age > 30:  # Data older than 30 seconds
                        stale_symbols.append(symbol)
                
                if stale_symbols:
                    self.logger.warning(f"Stale market data for symbols: {stale_symbols}")
                
                # Log statistics
                avg_latency = sum(self.data_latencies) / len(self.data_latencies) if self.data_latencies else 0
                self.logger.info(f"Market data stats - Updates: {self.update_counts}, "
                               f"Avg latency: {avg_latency:.3f}s")
                
                # Reset counters
                self.update_counts.clear()
                
                await asyncio.sleep(60)  # Every minute
                
            except Exception as e:
                self.logger.error(f"Error in monitoring task: {e}")
                await asyncio.sleep(60)
    
    def get_status(self) -> Dict[str, Any]:
        """Get market data engine status"""
        return {
            "running": self.is_running,
            "subscriptions": len(self.subscriptions),
            "symbols": list(self.subscriptions.keys()),
            "exchanges": list(self.exchange_handlers.keys()),
            "data_points": {
                "tickers": sum(len(exchanges) for exchanges in self.tickers.values()),
                "orderbooks": sum(len(exchanges) for exchanges in self.orderbooks.values()),
                "trades": sum(len(trades) for trades in self.trades.values())
            },
            "average_latency": sum(self.data_latencies) / len(self.data_latencies) if self.data_latencies else 0,
            "update_rate": sum(self.update_counts.values())
        }