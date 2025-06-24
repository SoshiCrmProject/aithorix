"""
AITHORIX Exchange Manager
Manages connections to all 5 exchanges with intelligent routing
"""

import asyncio
from typing import Dict, List, Optional, Any, Tuple
from decimal import Decimal
from dataclasses import dataclass
import structlog

from .base_exchange import BaseExchange, ExchangeCredentials, OrderBook, Ticker, Balance
from .binance.client import BinanceExchange
from .hyperliquid.client import HyperliquidExchange
from .mexc.client import MEXCExchange
from .bybit.client import BybitExchange
from .okx.client import OKXExchange

from ..core.engine.trading_engine import Order, OrderType, OrderSide
from ..core.exceptions import ExchangeConnectionError, ConfigurationError
from ..stealth.antidetection.api_normalizer import APIRateLimiter
from ..stealth.antidetection.timing_jitter import TimingJitter


logger = structlog.get_logger()


@dataclass
class ExchangeStatus:
    """Exchange connection status"""
    name: str
    connected: bool
    last_error: Optional[str] = None
    latency_ms: Optional[float] = None
    available_balance: Dict[str, Decimal] = None
    

@dataclass
class RouteDecision:
    """Smart order routing decision"""
    exchange: str
    reasons: List[str]
    expected_fees: Decimal
    expected_slippage: Decimal
    confidence: float
    

class ExchangeManager:
    """
    Manages all exchange connections and provides intelligent routing
    """
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.exchanges: Dict[str, BaseExchange] = {}
        self.status: Dict[str, ExchangeStatus] = {}
        
        # Shared stealth components
        self.rate_limiter = APIRateLimiter()
        self.timing_jitter = TimingJitter()
        
        # Performance tracking
        self.order_routing_stats: Dict[str, Dict[str, int]] = {
            exchange: {"success": 0, "failed": 0}
            for exchange in ["binance", "hyperliquid", "mexc", "bybit", "okx"]
        }
        
    async def initialize(self) -> None:
        """Initialize all exchange connections"""
        logger.info("Initializing exchange connections")
        
        # Create exchange instances
        exchanges_config = [
            ("binance", BinanceExchange, ["api_key", "api_secret"]),
            ("hyperliquid", HyperliquidExchange, ["api_key", "api_secret", "wallet_address"]),
            ("mexc", MEXCExchange, ["api_key", "api_secret"]),
            ("bybit", BybitExchange, ["api_key", "api_secret"]),
            ("okx", OKXExchange, ["api_key", "api_secret", "passphrase"])
        ]
        
        # Initialize each exchange
        for exchange_name, exchange_class, required_fields in exchanges_config:
            try:
                # Get credentials
                creds_dict = {}
                for field in required_fields:
                    key = f"{exchange_name.upper()}_{field.upper()}"
                    value = self.config.get(key)
                    if not value:
                        logger.warning(f"Missing {key} for {exchange_name}")
                        continue
                    creds_dict[field] = value
                    
                if len(creds_dict) != len(required_fields):
                    logger.warning(f"Skipping {exchange_name} due to missing credentials")
                    continue
                    
                # Create credentials object
                credentials = ExchangeCredentials(**creds_dict)
                
                # Create exchange instance
                exchange = exchange_class(
                    credentials=credentials,
                    rate_limiter=self.rate_limiter,
                    timing_jitter=self.timing_jitter
                )
                
                # Connect to exchange
                await exchange.connect()
                
                # Store exchange
                self.exchanges[exchange_name] = exchange
                
                # Update status
                self.status[exchange_name] = ExchangeStatus(
                    name=exchange_name,
                    connected=True,
                    latency_ms=None,
                    available_balance={}
                )
                
                logger.info(f"Connected to {exchange_name}")
                
            except Exception as e:
                logger.error(f"Failed to connect to {exchange_name}: {e}")
                self.status[exchange_name] = ExchangeStatus(
                    name=exchange_name,
                    connected=False,
                    last_error=str(e)
                )
                
        # Start monitoring tasks
        asyncio.create_task(self._monitor_connections())
        asyncio.create_task(self._update_balances())
        
    async def shutdown(self) -> None:
        """Shutdown all exchange connections"""
        logger.info("Shutting down exchange connections")
        
        for exchange in self.exchanges.values():
            try:
                await exchange.disconnect()
            except Exception as e:
                logger.error(f"Error disconnecting from {exchange.name}: {e}")
                
        self.exchanges.clear()
        
    async def _monitor_connections(self) -> None:
        """Monitor exchange connections and latency"""
        while True:
            try:
                for name, exchange in self.exchanges.items():
                    try:
                        # Measure latency with simple request
                        import time
                        start = time.time()
                        await exchange.get_ticker("BTC/USDT")
                        latency = (time.time() - start) * 1000
                        
                        self.status[name].latency_ms = latency
                        self.status[name].connected = True
                        self.status[name].last_error = None
                        
                    except Exception as e:
                        self.status[name].connected = False
                        self.status[name].last_error = str(e)
                        logger.error(f"Connection check failed for {name}: {e}")
                        
                await asyncio.sleep(30)  # Check every 30 seconds
                
            except Exception as e:
                logger.error(f"Connection monitor error: {e}")
                await asyncio.sleep(60)
                
    async def _update_balances(self) -> None:
        """Update account balances periodically"""
        while True:
            try:
                for name, exchange in self.exchanges.items():
                    if not self.status[name].connected:
                        continue
                        
                    try:
                        balances = await exchange.get_balance()
                        balance_dict = {
                            b.asset: b.free for b in balances
                            if b.free > 0
                        }
                        self.status[name].available_balance = balance_dict
                        
                    except Exception as e:
                        logger.error(f"Failed to update balance for {name}: {e}")
                        
                await asyncio.sleep(60)  # Update every minute
                
            except Exception as e:
                logger.error(f"Balance update error: {e}")
                await asyncio.sleep(60)
                
    def get_connected_exchanges(self) -> List[str]:
        """Get list of connected exchanges"""
        return [
            name for name, status in self.status.items()
            if status.connected
        ]
        
    async def get_best_price(
        self, 
        symbol: str, 
        side: OrderSide
    ) -> Tuple[str, Decimal]:
        """Get best price across all exchanges"""
        best_exchange = None
        best_price = None
        
        for name, exchange in self.exchanges.items():
            if not self.status[name].connected:
                continue
                
            try:
                ticker = await exchange.get_ticker(symbol)
                
                if side == OrderSide.BUY:
                    price = ticker.ask
                    if best_price is None or price < best_price:
                        best_price = price
                        best_exchange = name
                else:
                    price = ticker.bid
                    if best_price is None or price > best_price:
                        best_price = price
                        best_exchange = name
                        
            except Exception as e:
                logger.error(f"Failed to get price from {name}: {e}")
                continue
                
        if best_exchange is None:
            raise ExchangeConnectionError("all", "No exchanges available")
            
        return best_exchange, best_price
        
    async def route_order(self, order: Order) -> RouteDecision:
        """
        Intelligent order routing based on multiple factors
        """
        candidates = []
        
        for name, exchange in self.exchanges.items():
            if not self.status[name].connected:
                continue
                
            try:
                # Check if exchange supports the symbol
                market_info = exchange.get_market_info(order.symbol)
                if not market_info:
                    continue
                    
                # Validate order
                valid, reason = exchange.validate_order(order)
                if not valid:
                    logger.debug(f"{name} rejected order: {reason}")
                    continue
                    
                # Calculate expected costs
                expected_fees = self._calculate_expected_fees(
                    exchange, order, market_info
                )
                
                # Estimate slippage
                expected_slippage = await self._estimate_slippage(
                    exchange, order
                )
                
                # Check balance
                required_balance = self._calculate_required_balance(order)
                asset = order.symbol.split("/")[1]  # Quote asset
                
                available = self.status[name].available_balance.get(asset, Decimal("0"))
                if available < required_balance:
                    logger.debug(f"{name} insufficient balance: {available} < {required_balance}")
                    continue
                    
                # Calculate routing score
                score = self._calculate_routing_score(
                    name, expected_fees, expected_slippage
                )
                
                candidates.append({
                    "exchange": name,
                    "fees": expected_fees,
                    "slippage": expected_slippage,
                    "score": score,
                    "reasons": []
                })
                
            except Exception as e:
                logger.error(f"Error evaluating {name} for routing: {e}")
                continue
                
        if not candidates:
            raise ExchangeConnectionError("all", "No suitable exchange for order")
            
        # Sort by score (higher is better)
        candidates.sort(key=lambda x: x["score"], reverse=True)
        best = candidates[0]
        
        # Build routing decision
        reasons = []
        
        # Add reasons for selection
        if best["fees"] < Decimal("0.001"):
            reasons.append("Lowest fees")
        if best["slippage"] < Decimal("0.0005"):
            reasons.append("Minimal slippage")
        if self.status[best["exchange"]].latency_ms < 50:
            reasons.append("Low latency")
            
        # Special exchange advantages
        exchange_advantages = {
            "binance": "Highest liquidity",
            "hyperliquid": "Best for perpetuals",
            "mexc": "New token availability",
            "bybit": "Derivatives specialist",
            "okx": "Advanced features"
        }
        
        if best["exchange"] in exchange_advantages:
            reasons.append(exchange_advantages[best["exchange"]])
            
        return RouteDecision(
            exchange=best["exchange"],
            reasons=reasons,
            expected_fees=best["fees"],
            expected_slippage=best["slippage"],
            confidence=min(0.95, best["score"])
        )
        
    def _calculate_expected_fees(
        self, 
        exchange: BaseExchange, 
        order: Order,
        market_info: Any
    ) -> Decimal:
        """Calculate expected trading fees"""
        # Base fee rate
        if order.post_only:
            fee_rate = market_info.maker_fee
        else:
            fee_rate = market_info.taker_fee
            
        # Calculate notional value
        notional = order.quantity * (order.price or market_info.last)
        
        # Calculate fee
        fee = notional * fee_rate
        
        return fee
        
    async def _estimate_slippage(
        self, 
        exchange: BaseExchange, 
        order: Order
    ) -> Decimal:
        """Estimate potential slippage"""
        try:
            # Get order book
            order_book = await exchange.get_order_book(order.symbol, limit=50)
            
            # Calculate market impact
            remaining_quantity = order.quantity
            total_cost = Decimal("0")
            
            if order.side == OrderSide.BUY:
                # Walk through asks
                for price, quantity in order_book.asks:
                    if remaining_quantity <= 0:
                        break
                        
                    fill_quantity = min(remaining_quantity, quantity)
                    total_cost += fill_quantity * price
                    remaining_quantity -= fill_quantity
                    
                if remaining_quantity > 0:
                    # Not enough liquidity
                    return Decimal("0.01")  # 1% slippage estimate
                    
                avg_price = total_cost / order.quantity
                best_ask = order_book.asks[0][0] if order_book.asks else order.price
                slippage = (avg_price - best_ask) / best_ask
                
            else:
                # Walk through bids
                for price, quantity in order_book.bids:
                    if remaining_quantity <= 0:
                        break
                        
                    fill_quantity = min(remaining_quantity, quantity)
                    total_cost += fill_quantity * price
                    remaining_quantity -= fill_quantity
                    
                if remaining_quantity > 0:
                    return Decimal("0.01")
                    
                avg_price = total_cost / order.quantity
                best_bid = order_book.bids[0][0] if order_book.bids else order.price
                slippage = (best_bid - avg_price) / best_bid
                
            return abs(slippage)
            
        except Exception as e:
            logger.error(f"Error estimating slippage: {e}")
            return Decimal("0.005")  # Default 0.5% estimate
            
    def _calculate_required_balance(self, order: Order) -> Decimal:
        """Calculate required balance for order"""
        notional = order.quantity * (order.price or Decimal("50000"))  # Use 50k as default
        
        # Add margin for fees and slippage
        margin = Decimal("1.01")  # 1% margin
        
        # Adjust for leverage
        if order.leverage > 1:
            required = notional / Decimal(str(order.leverage))
        else:
            required = notional
            
        return required * margin
        
    def _calculate_routing_score(
        self, 
        exchange: str, 
        fees: Decimal, 
        slippage: Decimal
    ) -> float:
        """Calculate routing score for exchange selection"""
        # Base score
        score = 1.0
        
        # Fee component (lower is better)
        fee_score = float(1 - min(fees / Decimal("0.01"), 1))  # Normalize to 0.01
        score *= (0.4 + 0.6 * fee_score)  # 40-100% based on fees
        
        # Slippage component (lower is better)
        slippage_score = float(1 - min(slippage / Decimal("0.01"), 1))
        score *= (0.4 + 0.6 * slippage_score)
        
        # Latency component
        latency = self.status[exchange].latency_ms or 100
        latency_score = max(0, 1 - (latency - 10) / 100)  # Best at 10ms, worst at 110ms
        score *= (0.8 + 0.2 * latency_score)
        
        # Success rate component
        stats = self.order_routing_stats[exchange]
        total = stats["success"] + stats["failed"]
        if total > 10:
            success_rate = stats["success"] / total
            score *= (0.7 + 0.3 * success_rate)
            
        # Exchange-specific bonuses
        exchange_bonuses = {
            "binance": 1.1,      # Highest liquidity
            "hyperliquid": 1.05, # Low fees
            "mexc": 1.0,         # Standard
            "bybit": 1.02,       # Good derivatives
            "okx": 1.03          # Good features
        }
        
        score *= exchange_bonuses.get(exchange, 1.0)
        
        return score
        
    async def execute_order(self, order: Order) -> Dict[str, Any]:
        """Execute order on selected exchange"""
        # Route order
        routing = await self.route_order(order)
        logger.info(f"Routing order to {routing.exchange}: {routing.reasons}")
        
        # Get exchange
        exchange = self.exchanges.get(routing.exchange)
        if not exchange:
            raise ExchangeConnectionError(routing.exchange, "Exchange not available")
            
        try:
            # Execute order
            result = await exchange.place_order(order)
            
            # Update stats
            self.order_routing_stats[routing.exchange]["success"] += 1
            
            return {
                "exchange": routing.exchange,
                "order_result": result,
                "routing": routing
            }
            
        except Exception as e:
            # Update stats
            self.order_routing_stats[routing.exchange]["failed"] += 1
            
            logger.error(f"Order execution failed on {routing.exchange}: {e}")
            
            # Try fallback exchange
            if len(self.get_connected_exchanges()) > 1:
                # Remove failed exchange temporarily
                original_status = self.status[routing.exchange].connected
                self.status[routing.exchange].connected = False
                
                try:
                    # Re-route to different exchange
                    fallback_routing = await self.route_order(order)
                    fallback_exchange = self.exchanges[fallback_routing.exchange]
                    
                    logger.info(f"Fallback routing to {fallback_routing.exchange}")
                    result = await fallback_exchange.place_order(order)
                    
                    self.order_routing_stats[fallback_routing.exchange]["success"] += 1
                    
                    return {
                        "exchange": fallback_routing.exchange,
                        "order_result": result,
                        "routing": fallback_routing,
                        "fallback": True
                    }
                    
                finally:
                    # Restore original status
                    self.status[routing.exchange].connected = original_status
                    
            raise
            
    async def get_aggregated_order_book(
        self, 
        symbol: str, 
        limit: int = 20
    ) -> OrderBook:
        """Get aggregated order book from all exchanges"""
        all_bids = []
        all_asks = []
        
        for name, exchange in self.exchanges.items():
            if not self.status[name].connected:
                continue
                
            try:
                order_book = await exchange.get_order_book(symbol, limit)
                
                # Add exchange tag to each level
                for price, quantity in order_book.bids:
                    all_bids.append((price, quantity, name))
                    
                for price, quantity in order_book.asks:
                    all_asks.append((price, quantity, name))
                    
            except Exception as e:
                logger.error(f"Failed to get order book from {name}: {e}")
                continue
                
        # Sort and aggregate
        all_bids.sort(key=lambda x: x[0], reverse=True)  # Highest first
        all_asks.sort(key=lambda x: x[0])  # Lowest first
        
        # Aggregate by price level
        aggregated_bids = []
        aggregated_asks = []
        
        current_price = None
        current_quantity = Decimal("0")
        
        for price, quantity, exchange in all_bids[:limit]:
            if current_price != price:
                if current_price is not None:
                    aggregated_bids.append((current_price, current_quantity))
                current_price = price
                current_quantity = quantity
            else:
                current_quantity += quantity
                
        if current_price is not None:
            aggregated_bids.append((current_price, current_quantity))
            
        current_price = None
        current_quantity = Decimal("0")
        
        for price, quantity, exchange in all_asks[:limit]:
            if current_price != price:
                if current_price is not None:
                    aggregated_asks.append((current_price, current_quantity))
                current_price = price
                current_quantity = quantity
            else:
                current_quantity += quantity
                
        if current_price is not None:
            aggregated_asks.append((current_price, current_quantity))
            
        return OrderBook(
            timestamp=datetime.now(timezone.utc),
            symbol=symbol,
            bids=aggregated_bids[:limit],
            asks=aggregated_asks[:limit]
        )
