"""
AITHORIX Data Coordinator
Manages all data flow and coordination across the system

This module handles:
- Market data aggregation from multiple exchanges
- Data normalization and validation
- Real-time data distribution
- Historical data management
- Feature engineering coordination
"""

import asyncio
import logging
from typing import Dict, List, Optional, Any, Callable, Set
from datetime import datetime, timedelta
from decimal import Decimal
from dataclasses import dataclass, field
from enum import Enum
from collections import defaultdict, deque
import numpy as np
import pandas as pd

from core.engine.market_data import MarketData, OrderBook, Trade, Ticker
from exchanges.manager import ExchangeManager
from models.base_model import BaseModel
from utils.helpers import get_timestamp, synchronized
from monitoring.metrics import MetricsCollector


class DataType(Enum):
    """Types of market data"""
    ORDERBOOK = "orderbook"
    TRADES = "trades"
    TICKER = "ticker"
    OHLCV = "ohlcv"
    FUNDING = "funding"
    LIQUIDATIONS = "liquidations"
    OPEN_INTEREST = "open_interest"
    INDEX_PRICE = "index_price"


class DataQuality(Enum):
    """Data quality levels"""
    EXCELLENT = "excellent"
    GOOD = "good"
    FAIR = "fair"
    POOR = "poor"
    INVALID = "invalid"


@dataclass
class DataSubscription:
    """Data subscription details"""
    subscription_id: str
    data_type: DataType
    symbol: str
    exchange: str
    callback: Callable
    filters: Dict[str, Any] = field(default_factory=dict)
    created_at: datetime = field(default_factory=datetime.utcnow)
    last_update: Optional[datetime] = None
    update_count: int = 0


@dataclass
class DataSnapshot:
    """Point-in-time data snapshot"""
    timestamp: datetime
    symbol: str
    exchange: str
    
    # Market data
    bid: Decimal
    ask: Decimal
    last: Decimal
    volume_24h: Decimal
    
    # Order book metrics
    bid_liquidity_10bps: Decimal  # Liquidity within 10 basis points
    ask_liquidity_10bps: Decimal
    book_imbalance: float  # -1 to 1, negative = more sells
    
    # Trade flow
    buy_volume: Decimal
    sell_volume: Decimal
    trade_count: int
    vwap: Decimal
    
    # Additional metrics
    funding_rate: Optional[Decimal] = None
    open_interest: Optional[Decimal] = None
    liquidations_24h: Optional[Decimal] = None
    
    # Quality metrics
    data_quality: DataQuality = DataQuality.GOOD
    latency_ms: Optional[int] = None


@dataclass
class AggregatedData:
    """Aggregated data across exchanges"""
    timestamp: datetime
    symbol: str
    
    # Best prices across all exchanges
    best_bid: Decimal
    best_bid_exchange: str
    best_ask: Decimal
    best_ask_exchange: str
    
    # Aggregated metrics
    total_volume_24h: Decimal
    average_price: Decimal
    price_variance: float
    
    # Arbitrage opportunities
    arbitrage_spread: Decimal
    arbitrage_exchanges: List[str] = field(default_factory=list)
    
    # Exchange-specific data
    exchange_data: Dict[str, DataSnapshot] = field(default_factory=dict)


class DataCoordinator:
    """
    Master data coordination system
    
    Manages all market data flow, aggregation, and distribution
    across the trading system.
    """
    
    def __init__(
        self,
        exchange_manager: ExchangeManager,
        config: Dict[str, Any]
    ):
        self.exchange_manager = exchange_manager
        self.config = config
        self.logger = logging.getLogger("AITHORIX.DataCoordinator")
        
        # Data storage
        self.order_books: Dict[str, Dict[str, OrderBook]] = defaultdict(dict)  # symbol -> exchange -> orderbook
        self.trades: Dict[str, Dict[str, deque]] = defaultdict(lambda: defaultdict(lambda: deque(maxlen=1000)))
        self.tickers: Dict[str, Dict[str, Ticker]] = defaultdict(dict)
        self.ohlcv_data: Dict[str, Dict[str, pd.DataFrame]] = defaultdict(dict)
        
        # Aggregated data
        self.aggregated_data: Dict[str, AggregatedData] = {}
        self.data_snapshots: Dict[str, deque] = defaultdict(lambda: deque(maxlen=1000))
        
        # Subscriptions
        self.subscriptions: Dict[str, DataSubscription] = {}
        self.symbol_subscriptions: Dict[str, Set[str]] = defaultdict(set)  # symbol -> subscription_ids
        
        # Feature stores for models
        self.feature_stores: Dict[str, pd.DataFrame] = {}
        self.feature_update_callbacks: Dict[str, List[Callable]] = defaultdict(list)
        
        # Configuration
        self.snapshot_interval = config.get("snapshot_interval", 1)  # seconds
        self.aggregation_interval = config.get("aggregation_interval", 0.1)  # seconds
        self.max_data_age = config.get("max_data_age", 300)  # seconds
        self.quality_threshold = config.get("quality_threshold", 0.8)
        
        # WebSocket connections
        self.ws_connections: Dict[str, Any] = {}
        
        # Metrics
        self.metrics_collector = MetricsCollector()
        self.data_stats: Dict[str, Dict[str, int]] = defaultdict(lambda: defaultdict(int))
        
        # Control
        self._lock = asyncio.Lock()
        self.is_running = False
    
    async def start(self) -> None:
        """Start the data coordinator"""
        self.logger.info("Starting Data Coordinator...")
        self.is_running = True
        
        # Connect to all exchanges
        await self._connect_exchanges()
        
        # Start data processing loops
        asyncio.create_task(self._snapshot_loop())
        asyncio.create_task(self._aggregation_loop())
        asyncio.create_task(self._cleanup_loop())
        asyncio.create_task(self._quality_monitoring_loop())
        asyncio.create_task(self._feature_update_loop())
    
    async def stop(self) -> None:
        """Stop the data coordinator"""
        self.logger.info("Stopping Data Coordinator...")
        self.is_running = False
        
        # Disconnect from exchanges
        await self._disconnect_exchanges()
    
    async def subscribe(
        self,
        data_type: DataType,
        symbol: str,
        exchange: str,
        callback: Callable,
        filters: Optional[Dict[str, Any]] = None
    ) -> str:
        """Subscribe to market data"""
        subscription_id = f"{data_type.value}_{symbol}_{exchange}_{get_timestamp()}"
        
        subscription = DataSubscription(
            subscription_id=subscription_id,
            data_type=data_type,
            symbol=symbol,
            exchange=exchange,
            callback=callback,
            filters=filters or {}
        )
        
        async with self._lock:
            self.subscriptions[subscription_id] = subscription
            self.symbol_subscriptions[symbol].add(subscription_id)
        
        # Start data feed if not already active
        await self._ensure_data_feed(data_type, symbol, exchange)
        
        self.logger.info(f"Created subscription {subscription_id}")
        return subscription_id
    
    async def unsubscribe(self, subscription_id: str) -> bool:
        """Unsubscribe from market data"""
        async with self._lock:
            subscription = self.subscriptions.pop(subscription_id, None)
            if subscription:
                self.symbol_subscriptions[subscription.symbol].discard(subscription_id)
                self.logger.info(f"Removed subscription {subscription_id}")
                return True
        return False
    
    @synchronized
    async def get_orderbook(self, symbol: str, exchange: str) -> Optional[OrderBook]:
        """Get current order book"""
        return self.order_books.get(symbol, {}).get(exchange)
    
    @synchronized
    async def get_aggregated_orderbook(self, symbol: str) -> Dict[str, OrderBook]:
        """Get order books from all exchanges"""
        return self.order_books.get(symbol, {})
    
    @synchronized
    async def get_ticker(self, symbol: str, exchange: str) -> Optional[Ticker]:
        """Get current ticker data"""
        return self.tickers.get(symbol, {}).get(exchange)
    
    @synchronized
    async def get_recent_trades(self, symbol: str, exchange: str, limit: int = 100) -> List[Trade]:
        """Get recent trades"""
        trades = self.trades.get(symbol, {}).get(exchange, deque())
        return list(trades)[-limit:]
    
    @synchronized
    async def get_ohlcv(
        self,
        symbol: str,
        exchange: str,
        timeframe: str = "1m",
        limit: int = 100
    ) -> Optional[pd.DataFrame]:
        """Get OHLCV data"""
        key = f"{exchange}_{timeframe}"
        df = self.ohlcv_data.get(symbol, {}).get(key)
        if df is not None and not df.empty:
            return df.tail(limit)
        return None
    
    async def get_aggregated_data(self, symbol: str) -> Optional[AggregatedData]:
        """Get aggregated data across all exchanges"""
        return self.aggregated_data.get(symbol)
    
    async def get_data_snapshot(self, symbol: str, exchange: str) -> Optional[DataSnapshot]:
        """Get latest data snapshot"""
        snapshots = self.data_snapshots.get(f"{symbol}_{exchange}")
        if snapshots:
            return snapshots[-1]
        return None
    
    async def get_historical_snapshots(
        self,
        symbol: str,
        exchange: str,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None
    ) -> List[DataSnapshot]:
        """Get historical data snapshots"""
        snapshots = self.data_snapshots.get(f"{symbol}_{exchange}", deque())
        
        result = []
        for snapshot in snapshots:
            if start_time and snapshot.timestamp < start_time:
                continue
            if end_time and snapshot.timestamp > end_time:
                continue
            result.append(snapshot)
        
        return result
    
    async def register_feature_callback(self, feature_name: str, callback: Callable) -> None:
        """Register callback for feature updates"""
        self.feature_update_callbacks[feature_name].append(callback)
    
    async def get_features(self, symbol: str, features: List[str]) -> Optional[pd.DataFrame]:
        """Get feature data for models"""
        if symbol not in self.feature_stores:
            return None
        
        df = self.feature_stores[symbol]
        available_features = [f for f in features if f in df.columns]
        
        if available_features:
            return df[available_features].copy()
        return None
    
    async def _connect_exchanges(self) -> None:
        """Connect to all configured exchanges"""
        exchanges = self.config.get("exchanges", ["binance", "hyperliquid", "mexc", "bybit", "okx"])
        
        for exchange in exchanges:
            try:
                # Connect to exchange WebSocket
                ws_client = await self.exchange_manager.connect_websocket(exchange)
                self.ws_connections[exchange] = ws_client
                self.logger.info(f"Connected to {exchange} WebSocket")
            except Exception as e:
                self.logger.error(f"Failed to connect to {exchange}: {e}")
    
    async def _disconnect_exchanges(self) -> None:
        """Disconnect from all exchanges"""
        for exchange, ws_client in self.ws_connections.items():
            try:
                await ws_client.close()
                self.logger.info(f"Disconnected from {exchange}")
            except Exception as e:
                self.logger.error(f"Error disconnecting from {exchange}: {e}")
    
    async def _ensure_data_feed(self, data_type: DataType, symbol: str, exchange: str) -> None:
        """Ensure data feed is active for subscription"""
        ws_client = self.ws_connections.get(exchange)
        if not ws_client:
            return
        
        # Subscribe to appropriate data stream
        if data_type == DataType.ORDERBOOK:
            await ws_client.subscribe_orderbook(symbol, self._handle_orderbook_update)
        elif data_type == DataType.TRADES:
            await ws_client.subscribe_trades(symbol, self._handle_trade_update)
        elif data_type == DataType.TICKER:
            await ws_client.subscribe_ticker(symbol, self._handle_ticker_update)
    
    async def _handle_orderbook_update(self, exchange: str, symbol: str, orderbook: OrderBook) -> None:
        """Handle order book update"""
        # Store order book
        self.order_books[symbol][exchange] = orderbook
        
        # Update statistics
        self.data_stats[symbol][f"{exchange}_orderbook_updates"] += 1
        
        # Notify subscribers
        await self._notify_subscribers(DataType.ORDERBOOK, symbol, exchange, orderbook)
    
    async def _handle_trade_update(self, exchange: str, symbol: str, trade: Trade) -> None:
        """Handle trade update"""
        # Store trade
        self.trades[symbol][exchange].append(trade)
        
        # Update statistics
        self.data_stats[symbol][f"{exchange}_trade_updates"] += 1
        
        # Notify subscribers
        await self._notify_subscribers(DataType.TRADES, symbol, exchange, trade)
    
    async def _handle_ticker_update(self, exchange: str, symbol: str, ticker: Ticker) -> None:
        """Handle ticker update"""
        # Store ticker
        self.tickers[symbol][exchange] = ticker
        
        # Update statistics
        self.data_stats[symbol][f"{exchange}_ticker_updates"] += 1
        
        # Notify subscribers
        await self._notify_subscribers(DataType.TICKER, symbol, exchange, ticker)
    
    async def _notify_subscribers(self, data_type: DataType, symbol: str, exchange: str, data: Any) -> None:
        """Notify subscribers of data update"""
        subscription_ids = self.symbol_subscriptions.get(symbol, set())
        
        for sub_id in subscription_ids:
            subscription = self.subscriptions.get(sub_id)
            if not subscription:
                continue
            
            # Check if subscription matches
            if (subscription.data_type == data_type and
                subscription.symbol == symbol and
                subscription.exchange == exchange):
                
                try:
                    # Apply filters if any
                    if self._apply_filters(data, subscription.filters):
                        await subscription.callback(data)
                        subscription.last_update = datetime.utcnow()
                        subscription.update_count += 1
                except Exception as e:
                    self.logger.error(f"Error in subscription callback {sub_id}: {e}")
    
    def _apply_filters(self, data: Any, filters: Dict[str, Any]) -> bool:
        """Apply subscription filters to data"""
        if not filters:
            return True
        
        # Implement filter logic based on data type
        # For now, return True
        return True
    
    async def _snapshot_loop(self) -> None:
        """Create periodic data snapshots"""
        while self.is_running:
            try:
                # Create snapshots for all active symbols
                for symbol in self.order_books.keys():
                    for exchange, orderbook in self.order_books[symbol].items():
                        snapshot = await self._create_snapshot(symbol, exchange)
                        if snapshot:
                            key = f"{symbol}_{exchange}"
                            self.data_snapshots[key].append(snapshot)
                
                await asyncio.sleep(self.snapshot_interval)
                
            except Exception as e:
                self.logger.error(f"Error in snapshot loop: {e}")
                await asyncio.sleep(self.snapshot_interval)
    
    async def _create_snapshot(self, symbol: str, exchange: str) -> Optional[DataSnapshot]:
        """Create a data snapshot"""
        try:
            orderbook = self.order_books.get(symbol, {}).get(exchange)
            ticker = self.tickers.get(symbol, {}).get(exchange)
            recent_trades = list(self.trades.get(symbol, {}).get(exchange, deque()))
            
            if not orderbook or not ticker:
                return None
            
            # Calculate metrics
            bid = Decimal(str(orderbook.bids[0][0])) if orderbook.bids else Decimal("0")
            ask = Decimal(str(orderbook.asks[0][0])) if orderbook.asks else Decimal("0")
            
            # Calculate liquidity within 10 basis points
            bid_liquidity = self._calculate_liquidity(orderbook.bids, bid, Decimal("0.001"))
            ask_liquidity = self._calculate_liquidity(orderbook.asks, ask, Decimal("0.001"))
            
            # Calculate book imbalance
            book_imbalance = self._calculate_book_imbalance(orderbook)
            
            # Calculate trade flow
            buy_volume, sell_volume, vwap = self._calculate_trade_flow(recent_trades)
            
            snapshot = DataSnapshot(
                timestamp=datetime.utcnow(),
                symbol=symbol,
                exchange=exchange,
                bid=bid,
                ask=ask,
                last=Decimal(str(ticker.last)),
                volume_24h=Decimal(str(ticker.volume)),
                bid_liquidity_10bps=bid_liquidity,
                ask_liquidity_10bps=ask_liquidity,
                book_imbalance=book_imbalance,
                buy_volume=buy_volume,
                sell_volume=sell_volume,
                trade_count=len(recent_trades),
                vwap=vwap,
                data_quality=self._assess_data_quality(orderbook, ticker, recent_trades)
            )
            
            return snapshot
            
        except Exception as e:
            self.logger.error(f"Error creating snapshot for {symbol}@{exchange}: {e}")
            return None
    
    def _calculate_liquidity(self, orders: List[List[float]], reference_price: Decimal, threshold: Decimal) -> Decimal:
        """Calculate liquidity within threshold of reference price"""
        liquidity = Decimal("0")
        
        for price, quantity in orders:
            price_decimal = Decimal(str(price))
            if abs(price_decimal - reference_price) / reference_price <= threshold:
                liquidity += Decimal(str(quantity))
            else:
                break  # Orders are sorted by price
        
        return liquidity
    
    def _calculate_book_imbalance(self, orderbook: OrderBook) -> float:
        """Calculate order book imbalance (-1 to 1)"""
        if not orderbook.bids or not orderbook.asks:
            return 0.0
        
        # Sum top 10 levels
        bid_volume = sum(qty for _, qty in orderbook.bids[:10])
        ask_volume = sum(qty for _, qty in orderbook.asks[:10])
        
        total_volume = bid_volume + ask_volume
        if total_volume == 0:
            return 0.0
        
        return (bid_volume - ask_volume) / total_volume
    
    def _calculate_trade_flow(self, trades: List[Trade]) -> Tuple[Decimal, Decimal, Decimal]:
        """Calculate buy/sell volume and VWAP"""
        buy_volume = Decimal("0")
        sell_volume = Decimal("0")
        total_value = Decimal("0")
        total_volume = Decimal("0")
        
        for trade in trades:
            volume = Decimal(str(trade.quantity))
            price = Decimal(str(trade.price))
            value = volume * price
            
            if trade.side == "buy":
                buy_volume += volume
            else:
                sell_volume += volume
            
            total_volume += volume
            total_value += value
        
        vwap = total_value / total_volume if total_volume > 0 else Decimal("0")
        
        return buy_volume, sell_volume, vwap
    
    def _assess_data_quality(self, orderbook: OrderBook, ticker: Ticker, trades: List[Trade]) -> DataQuality:
        """Assess data quality"""
        quality_score = 1.0
        
        # Check data freshness
        now = datetime.utcnow()
        
        if orderbook.timestamp:
            age = (now - orderbook.timestamp).total_seconds()
            if age > 5:
                quality_score *= 0.8
            if age > 10:
                quality_score *= 0.5
        
        # Check data completeness
        if not orderbook.bids or not orderbook.asks:
            quality_score *= 0.7
        
        if len(orderbook.bids) < 10 or len(orderbook.asks) < 10:
            quality_score *= 0.9
        
        # Check spread sanity
        if orderbook.bids and orderbook.asks:
            spread = (orderbook.asks[0][0] - orderbook.bids[0][0]) / orderbook.bids[0][0]
            if spread > 0.01:  # More than 1% spread
                quality_score *= 0.9
            if spread > 0.05:  # More than 5% spread
                quality_score *= 0.5
        
        # Map score to quality level
        if quality_score >= 0.9:
            return DataQuality.EXCELLENT
        elif quality_score >= 0.7:
            return DataQuality.GOOD
        elif quality_score >= 0.5:
            return DataQuality.FAIR
        elif quality_score >= 0.3:
            return DataQuality.POOR
        else:
            return DataQuality.INVALID
    
    async def _aggregation_loop(self) -> None:
        """Aggregate data across exchanges"""
        while self.is_running:
            try:
                # Aggregate data for all active symbols
                for symbol in self.order_books.keys():
                    aggregated = await self._aggregate_symbol_data(symbol)
                    if aggregated:
                        self.aggregated_data[symbol] = aggregated
                
                await asyncio.sleep(self.aggregation_interval)
                
            except Exception as e:
                self.logger.error(f"Error in aggregation loop: {e}")
                await asyncio.sleep(self.aggregation_interval)
    
    async def _aggregate_symbol_data(self, symbol: str) -> Optional[AggregatedData]:
        """Aggregate data for a symbol across all exchanges"""
        exchange_data = {}
        prices = []
        volumes = []
        
        # Collect data from all exchanges
        for exchange in self.order_books.get(symbol, {}).keys():
            snapshot = await self.get_data_snapshot(symbol, exchange)
            if snapshot and snapshot.data_quality != DataQuality.INVALID:
                exchange_data[exchange] = snapshot
                prices.append(float(snapshot.last))
                volumes.append(float(snapshot.volume_24h))
        
        if not exchange_data:
            return None
        
        # Find best bid/ask
        best_bid = Decimal("0")
        best_bid_exchange = ""
        best_ask = Decimal("999999999")
        best_ask_exchange = ""
        
        for exchange, snapshot in exchange_data.items():
            if snapshot.bid > best_bid:
                best_bid = snapshot.bid
                best_bid_exchange = exchange
            if snapshot.ask < best_ask:
                best_ask = snapshot.ask
                best_ask_exchange = exchange
        
        # Calculate aggregated metrics
        total_volume = sum(volumes)
        average_price = sum(p * v for p, v in zip(prices, volumes)) / total_volume if total_volume > 0 else 0
        price_variance = np.var(prices) if len(prices) > 1 else 0
        
        # Check for arbitrage
        arbitrage_spread = best_bid - best_ask if best_bid > best_ask else Decimal("0")
        arbitrage_exchanges = [best_bid_exchange, best_ask_exchange] if arbitrage_spread > 0 else []
        
        return AggregatedData(
            timestamp=datetime.utcnow(),
            symbol=symbol,
            best_bid=best_bid,
            best_bid_exchange=best_bid_exchange,
            best_ask=best_ask,
            best_ask_exchange=best_ask_exchange,
            total_volume_24h=Decimal(str(total_volume)),
            average_price=Decimal(str(average_price)),
            price_variance=price_variance,
            arbitrage_spread=arbitrage_spread,
            arbitrage_exchanges=arbitrage_exchanges,
            exchange_data=exchange_data
        )
    
    async def _cleanup_loop(self) -> None:
        """Clean up old data"""
        while self.is_running:
            try:
                current_time = datetime.utcnow()
                
                # Clean old trades
                for symbol_trades in self.trades.values():
                    for exchange_trades in symbol_trades.values():
                        # Deque automatically maintains size limit
                        pass
                
                # Clean old snapshots based on age
                for key, snapshots in self.data_snapshots.items():
                    while snapshots and (current_time - snapshots[0].timestamp).total_seconds() > self.max_data_age:
                        snapshots.popleft()
                
                # Clean inactive subscriptions
                inactive_subs = []
                for sub_id, sub in self.subscriptions.items():
                    if sub.last_update and (current_time - sub.last_update).total_seconds() > 3600:  # 1 hour
                        inactive_subs.append(sub_id)
                
                for sub_id in inactive_subs:
                    await self.unsubscribe(sub_id)
                
                await asyncio.sleep(60)  # Run every minute
                
            except Exception as e:
                self.logger.error(f"Error in cleanup loop: {e}")
                await asyncio.sleep(60)
    
    async def _quality_monitoring_loop(self) -> None:
        """Monitor data quality"""
        while self.is_running:
            try:
                quality_stats = defaultdict(lambda: {"total": 0, "good": 0})
                
                # Check quality of recent snapshots
                for key, snapshots in self.data_snapshots.items():
                    if snapshots:
                        recent_snapshots = list(snapshots)[-10:]  # Last 10 snapshots
                        for snapshot in recent_snapshots:
                            quality_stats[key]["total"] += 1
                            if snapshot.data_quality in [DataQuality.EXCELLENT, DataQuality.GOOD]:
                                quality_stats[key]["good"] += 1
                
                # Alert on poor quality
                for key, stats in quality_stats.items():
                    if stats["total"] > 0:
                        quality_ratio = stats["good"] / stats["total"]
                        if quality_ratio < self.quality_threshold:
                            self.logger.warning(f"Poor data quality for {key}: {quality_ratio:.2%}")
                
                # Record metrics
                self._record_data_metrics()
                
                await asyncio.sleep(30)  # Check every 30 seconds
                
            except Exception as e:
                self.logger.error(f"Error in quality monitoring: {e}")
                await asyncio.sleep(30)
    
    async def _feature_update_loop(self) -> None:
        """Update features for models"""
        while self.is_running:
            try:
                # Update features for each symbol
                for symbol in self.aggregated_data.keys():
                    features = await self._calculate_features(symbol)
                    if features is not None:
                        self.feature_stores[symbol] = features
                        
                        # Notify callbacks
                        for callback in self.feature_update_callbacks.get(symbol, []):
                            try:
                                await callback(features)
                            except Exception as e:
                                self.logger.error(f"Error in feature callback: {e}")
                
                await asyncio.sleep(1)  # Update every second
                
            except Exception as e:
                self.logger.error(f"Error in feature update loop: {e}")
                await asyncio.sleep(1)
    
    async def _calculate_features(self, symbol: str) -> Optional[pd.DataFrame]:
        """Calculate features for ML models"""
        try:
            # Get recent snapshots
            snapshots = []
            for exchange in self.order_books.get(symbol, {}).keys():
                recent = await self.get_historical_snapshots(
                    symbol, exchange,
                    start_time=datetime.utcnow() - timedelta(minutes=5)
                )
                snapshots.extend(recent)
            
            if len(snapshots) < 10:
                return None
            
            # Convert to DataFrame
            data = []
            for snapshot in snapshots:
                data.append({
                    "timestamp": snapshot.timestamp,
                    "exchange": snapshot.exchange,
                    "bid": float(snapshot.bid),
                    "ask": float(snapshot.ask),
                    "last": float(snapshot.last),
                    "volume": float(snapshot.volume_24h),
                    "spread": float(snapshot.ask - snapshot.bid),
                    "mid": float((snapshot.bid + snapshot.ask) / 2),
                    "book_imbalance": snapshot.book_imbalance,
                    "bid_liquidity": float(snapshot.bid_liquidity_10bps),
                    "ask_liquidity": float(snapshot.ask_liquidity_10bps),
                    "vwap": float(snapshot.vwap)
                })
            
            df = pd.DataFrame(data)
            df.set_index("timestamp", inplace=True)
            
            # Calculate additional features
            for exchange in df["exchange"].unique():
                mask = df["exchange"] == exchange
                
                # Price features
                df.loc[mask, "return_1m"] = df.loc[mask, "last"].pct_change()
                df.loc[mask, "return_5m"] = df.loc[mask, "last"].pct_change(5)
                
                # Rolling statistics
                df.loc[mask, "volatility_1m"] = df.loc[mask, "return_1m"].rolling(10).std()
                df.loc[mask, "volume_ma"] = df.loc[mask, "volume"].rolling(10).mean()
                
                # Microstructure features
                df.loc[mask, "spread_ma"] = df.loc[mask, "spread"].rolling(10).mean()
                df.loc[mask, "imbalance_ma"] = df.loc[mask, "book_imbalance"].rolling(10).mean()
            
            return df
            
        except Exception as e:
            self.logger.error(f"Error calculating features for {symbol}: {e}")
            return None
    
    def _record_data_metrics(self) -> None:
        """Record data metrics"""
        metrics = {
            "active_symbols": len(self.order_books),
            "active_subscriptions": len(self.subscriptions),
            "total_orderbook_updates": sum(stats.get(f"{ex}_orderbook_updates", 0) 
                                          for symbol_stats in self.data_stats.values() 
                                          for ex in ["binance", "hyperliquid", "mexc", "bybit", "okx"] 
                                          for stats in [symbol_stats]),
            "total_trade_updates": sum(stats.get(f"{ex}_trade_updates", 0) 
                                     for symbol_stats in self.data_stats.values() 
                                     for ex in ["binance", "hyperliquid", "mexc", "bybit", "okx"] 
                                     for stats in [symbol_stats]),
            "aggregated_symbols": len(self.aggregated_data),
            "feature_stores": len(self.feature_stores)
        }
        
        self.metrics_collector.record_data_coordinator_metrics(metrics)
    
    def get_data_statistics(self) -> Dict[str, Any]:
        """Get data coordinator statistics"""
        total_updates = 0
        for symbol_stats in self.data_stats.values():
            total_updates += sum(symbol_stats.values())
        
        quality_stats = {}
        for key, snapshots in self.data_snapshots.items():
            if snapshots:
                recent = list(snapshots)[-100:]
                good_quality = sum(1 for s in recent if s.data_quality in [DataQuality.EXCELLENT, DataQuality.GOOD])
                quality_stats[key] = good_quality / len(recent) if recent else 0
        
        return {
            "active_symbols": len(self.order_books),
            "active_exchanges": len(self.ws_connections),
            "active_subscriptions": len(self.subscriptions),
            "total_updates": total_updates,
            "update_rate": total_updates / max((datetime.utcnow() - self.metrics_collector.start_time).total_seconds(), 1),
            "aggregated_symbols": len(self.aggregated_data),
            "feature_stores": len(self.feature_stores),
            "average_quality": sum(quality_stats.values()) / len(quality_stats) if quality_stats else 0,
            "snapshot_count": sum(len(snapshots) for snapshots in self.data_snapshots.values())
        }