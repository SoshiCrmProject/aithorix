"""
AITHORIX Execution Engine
High-performance order execution with smart routing

This module handles:
- Smart order routing across exchanges
- Execution optimization
- Slippage minimization
- Order splitting and aggregation
"""

import asyncio
import logging
from typing import Dict, List, Optional, Any, Tuple, Set
from datetime import datetime, timedelta
from decimal import Decimal
from dataclasses import dataclass, field
from enum import Enum
import numpy as np
from collections import defaultdict, deque
import uuid

from core.engine.order_manager import Order, OrderStatus, OrderType, TimeInForce
from utils.helpers import get_timestamp, synchronized
from monitoring.metrics import MetricsCollector


class ExecutionStrategy(Enum):
    """Execution strategy types"""
    AGGRESSIVE = "aggressive"  # Market orders, immediate execution
    PASSIVE = "passive"  # Limit orders, wait for fills
    ADAPTIVE = "adaptive"  # Mix based on market conditions
    ICEBERG = "iceberg"  # Hidden quantity orders
    TWAP = "twap"  # Time-weighted average price
    VWAP = "vwap"  # Volume-weighted average price
    SMART = "smart"  # ML-based optimal execution


class ExecutionVenue(Enum):
    """Execution venue types"""
    EXCHANGE = "exchange"  # Direct exchange execution
    DARK_POOL = "dark_pool"  # Dark pool execution
    AGGREGATOR = "aggregator"  # Aggregated liquidity


@dataclass
class ExecutionReport:
    """Execution report for filled orders"""
    order_id: str
    execution_id: str
    exchange_order_id: str
    timestamp: datetime
    status: OrderStatus
    executed_quantity: Decimal
    remaining_quantity: Decimal
    average_price: Decimal
    last_price: Decimal
    last_quantity: Decimal
    commission: Decimal
    commission_asset: str
    is_maker: bool = False
    venue: str = ExecutionVenue.EXCHANGE.value
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    # Additional execution details
    slippage: Optional[Decimal] = None
    market_impact: Optional[Decimal] = None
    execution_time_ms: Optional[int] = None


@dataclass
class ExecutionPlan:
    """Execution plan for complex orders"""
    order_id: str
    strategy: ExecutionStrategy
    venues: List[str]  # Exchanges to use
    
    # Order splitting
    split_count: int = 1
    split_sizes: List[Decimal] = field(default_factory=list)
    split_timings: List[datetime] = field(default_factory=list)
    
    # Execution parameters
    urgency: float = 0.5  # 0 = passive, 1 = aggressive
    max_spread_cross: Decimal = Decimal("0.001")  # Max spread to cross
    min_fill_size: Optional[Decimal] = None
    max_participation_rate: Decimal = Decimal("0.1")  # Max % of volume
    
    # Smart routing
    use_dark_pools: bool = False
    allow_partial_fills: bool = True
    
    # Price limits
    price_limit: Optional[Decimal] = None
    slippage_limit: Optional[Decimal] = None


@dataclass
class VenueMetrics:
    """Metrics for execution venue performance"""
    venue: str
    symbol: str
    
    # Fill metrics
    fill_rate: float = 0.0
    average_fill_time: float = 0.0
    average_slippage: float = 0.0
    
    # Liquidity metrics
    average_spread: Decimal = Decimal("0")
    average_depth: Decimal = Decimal("0")
    
    # Cost metrics
    average_fee: Decimal = Decimal("0")
    rebate_rate: Decimal = Decimal("0")
    
    # Reliability
    success_rate: float = 1.0
    latency_ms: float = 0.0
    
    # Historical data
    executions: int = 0
    total_volume: Decimal = Decimal("0")
    last_updated: datetime = field(default_factory=datetime.utcnow)


class ExecutionEngine:
    """
    High-performance execution engine with smart routing
    
    Optimizes order execution across multiple venues to achieve:
    - Best execution price
    - Minimal market impact
    - Lowest transaction costs
    - Fastest execution times
    """
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.logger = logging.getLogger("AITHORIX.ExecutionEngine")
        
        # Exchange connections (injected)
        self.exchange_clients: Dict[str, Any] = {}
        
        # Execution tracking
        self.active_orders: Dict[str, Order] = {}
        self.execution_plans: Dict[str, ExecutionPlan] = {}
        self.execution_history: deque = deque(maxlen=10000)
        
        # Venue metrics
        self.venue_metrics: Dict[Tuple[str, str], VenueMetrics] = {}
        
        # Performance tracking
        self.metrics_collector = MetricsCollector()
        self.execution_latencies: deque = deque(maxlen=1000)
        self.slippage_history: deque = deque(maxlen=1000)
        
        # Configuration
        self.default_strategy = ExecutionStrategy(config.get("default_strategy", "smart"))
        self.enable_smart_routing = config.get("enable_smart_routing", True)
        self.enable_order_splitting = config.get("enable_order_splitting", True)
        self.max_order_splits = config.get("max_order_splits", 10)
        self.min_order_size = Decimal(str(config.get("min_order_size", "10")))
        
        # Behavioral simulation
        self.enable_behavioral_delays = config.get("enable_behavioral_delays", True)
        self.human_reaction_time = config.get("human_reaction_time", (0.2, 0.8))  # seconds
        
        # State
        self._lock = asyncio.Lock()
        self.is_initialized = False
        
        # Recent executions for pattern analysis
        self.recent_executions: Dict[str, deque] = defaultdict(lambda: deque(maxlen=100))
    
    async def initialize(self) -> None:
        """Initialize the execution engine"""
        self.logger.info("Initializing Execution Engine...")
        
        # Initialize venue metrics
        await self._initialize_venue_metrics()
        
        # Start monitoring tasks
        asyncio.create_task(self._execution_monitoring_loop())
        asyncio.create_task(self._venue_metrics_update_loop())
        
        self.is_initialized = True
        self.logger.info("Execution Engine initialized")
    
    @synchronized
    async def execute_order(self, order: Order) -> ExecutionReport:
        """Execute an order with smart routing"""
        self.logger.info(f"Executing order {order.order_id}: {order.symbol} "
                        f"{order.side} {order.quantity} @ {order.price or 'MARKET'}")
        
        start_time = datetime.utcnow()
        
        # Add behavioral delay if enabled
        if self.enable_behavioral_delays:
            await self._add_behavioral_delay()
        
        # Create execution plan
        plan = await self._create_execution_plan(order)
        self.execution_plans[order.order_id] = plan
        
        # Store active order
        self.active_orders[order.order_id] = order
        
        # Execute based on strategy
        if plan.strategy == ExecutionStrategy.SMART:
            report = await self._execute_smart_order(order, plan)
        elif plan.strategy == ExecutionStrategy.ICEBERG:
            report = await self._execute_iceberg_order(order, plan)
        elif plan.strategy == ExecutionStrategy.TWAP:
            report = await self._execute_twap_order(order, plan)
        elif plan.strategy == ExecutionStrategy.VWAP:
            report = await self._execute_vwap_order(order, plan)
        else:
            report = await self._execute_simple_order(order, plan)
        
        # Calculate execution metrics
        execution_time = (datetime.utcnow() - start_time).total_seconds() * 1000
        report.execution_time_ms = int(execution_time)
        self.execution_latencies.append(execution_time)
        
        # Calculate slippage if applicable
        if order.price and report.average_price:
            if order.side == "BUY":
                report.slippage = (report.average_price - order.price) / order.price
            else:
                report.slippage = (order.price - report.average_price) / order.price
            self.slippage_history.append(float(report.slippage))
        
        # Store execution
        self.execution_history.append(report)
        self.recent_executions[order.symbol].append(report)
        
        # Update venue metrics
        await self._update_venue_metrics_from_execution(report)
        
        # Record metrics
        self.metrics_collector.record_order_execution(order, report)
        
        # Clean up
        self.active_orders.pop(order.order_id, None)
        
        return report
    
    async def _create_execution_plan(self, order: Order) -> ExecutionPlan:
        """Create optimal execution plan for order"""
        # Analyze market conditions
        market_conditions = await self._analyze_market_conditions(order.symbol)
        
        # Determine execution strategy
        strategy = self._determine_execution_strategy(order, market_conditions)
        
        # Select venues
        venues = await self._select_execution_venues(order, market_conditions)
        
        # Determine order splitting
        split_plan = self._calculate_order_splits(order, market_conditions)
        
        # Create plan
        plan = ExecutionPlan(
            order_id=order.order_id,
            strategy=strategy,
            venues=venues,
            split_count=split_plan["count"],
            split_sizes=split_plan["sizes"],
            split_timings=split_plan["timings"],
            urgency=self._calculate_urgency(order),
            max_spread_cross=self._calculate_max_spread_cross(order),
            max_participation_rate=Decimal("0.1"),
            use_dark_pools=len(venues) > 1 and order.quantity > self.min_order_size * 10
        )
        
        return plan
    
    async def _execute_smart_order(self, order: Order, plan: ExecutionPlan) -> ExecutionReport:
        """Execute order using smart routing logic"""
        total_executed = Decimal("0")
        total_value = Decimal("0")
        executions = []
        
        # Try each venue in order of preference
        for venue in plan.venues:
            if total_executed >= order.quantity:
                break
            
            remaining = order.quantity - total_executed
            
            # Get market data for venue
            market_data = await self._get_venue_market_data(venue, order.symbol)
            
            # Calculate optimal execution size for this venue
            venue_size = self._calculate_venue_execution_size(
                remaining, market_data, plan
            )
            
            if venue_size <= 0:
                continue
            
            # Execute on venue
            try:
                venue_execution = await self._execute_on_venue(
                    venue, order, venue_size, market_data
                )
                
                if venue_execution:
                    executions.append(venue_execution)
                    total_executed += venue_execution["executed_quantity"]
                    total_value += venue_execution["executed_quantity"] * venue_execution["price"]
                    
            except Exception as e:
                self.logger.error(f"Execution failed on {venue}: {e}")
                continue
        
        # Create aggregate execution report
        if total_executed > 0:
            average_price = total_value / total_executed
            status = OrderStatus.FILLED if total_executed >= order.quantity else OrderStatus.PARTIAL
        else:
            average_price = Decimal("0")
            status = OrderStatus.FAILED
        
        report = ExecutionReport(
            order_id=order.order_id,
            execution_id=str(uuid.uuid4()),
            exchange_order_id=executions[0]["exchange_order_id"] if executions else "",
            timestamp=datetime.utcnow(),
            status=status,
            executed_quantity=total_executed,
            remaining_quantity=order.quantity - total_executed,
            average_price=average_price,
            last_price=executions[-1]["price"] if executions else Decimal("0"),
            last_quantity=executions[-1]["executed_quantity"] if executions else Decimal("0"),
            commission=sum(e["commission"] for e in executions),
            commission_asset=executions[0]["commission_asset"] if executions else "USDT",
            venue="SMART",
            metadata={"executions": executions}
        )
        
        return report
    
    async def _execute_iceberg_order(self, order: Order, plan: ExecutionPlan) -> ExecutionReport:
        """Execute iceberg order with hidden quantity"""
        # Calculate visible and hidden portions
        total_quantity = order.quantity
        visible_size = min(
            total_quantity * Decimal("0.1"),  # 10% visible
            Decimal("1000")  # Or max 1000 units
        )
        
        total_executed = Decimal("0")
        executions = []
        
        while total_executed < total_quantity:
            # Create visible order
            current_size = min(visible_size, total_quantity - total_executed)
            
            # Execute visible portion
            sub_order = Order(
                order_id=f"{order.order_id}_iceberg_{len(executions)}",
                symbol=order.symbol,
                exchange=order.exchange,
                side=order.side,
                order_type=order.order_type,
                quantity=current_size,
                price=order.price,
                time_in_force=order.time_in_force
            )
            
            execution = await self._execute_simple_order(sub_order, plan)
            
            if execution.status == OrderStatus.FILLED:
                executions.append(execution)
                total_executed += execution.executed_quantity
            else:
                break  # Stop if we can't fill
            
            # Add delay between iceberg slices
            await asyncio.sleep(np.random.uniform(1, 3))
        
        # Aggregate results
        if executions:
            total_value = sum(e.executed_quantity * e.average_price for e in executions)
            average_price = total_value / total_executed if total_executed > 0 else Decimal("0")
            
            report = ExecutionReport(
                order_id=order.order_id,
                execution_id=str(uuid.uuid4()),
                exchange_order_id=executions[0].exchange_order_id,
                timestamp=datetime.utcnow(),
                status=OrderStatus.FILLED if total_executed >= order.quantity else OrderStatus.PARTIAL,
                executed_quantity=total_executed,
                remaining_quantity=order.quantity - total_executed,
                average_price=average_price,
                last_price=executions[-1].average_price,
                last_quantity=executions[-1].executed_quantity,
                commission=sum(e.commission for e in executions),
                commission_asset=executions[0].commission_asset,
                venue="ICEBERG",
                metadata={"slices": len(executions)}
            )
        else:
            report = self._create_failed_execution_report(order)
        
        return report
    
    async def _execute_twap_order(self, order: Order, plan: ExecutionPlan) -> ExecutionReport:
        """Execute TWAP (Time-Weighted Average Price) order"""
        # Calculate time slices
        duration_minutes = 30  # Execute over 30 minutes
        slice_count = min(plan.max_order_splits, duration_minutes // 2)
        slice_size = order.quantity / Decimal(str(slice_count))
        slice_interval = duration_minutes * 60 / slice_count  # seconds
        
        total_executed = Decimal("0")
        executions = []
        start_time = datetime.utcnow()
        
        for i in range(slice_count):
            # Check if we should stop
            if total_executed >= order.quantity:
                break
            
            current_size = min(slice_size, order.quantity - total_executed)
            
            # Execute slice
            sub_order = Order(
                order_id=f"{order.order_id}_twap_{i}",
                symbol=order.symbol,
                exchange=order.exchange,
                side=order.side,
                order_type=OrderType.LIMIT if order.price else OrderType.MARKET,
                quantity=current_size,
                price=order.price,
                time_in_force=TimeInForce.IOC  # Immediate or cancel
            )
            
            execution = await self._execute_simple_order(sub_order, plan)
            
            if execution.executed_quantity > 0:
                executions.append(execution)
                total_executed += execution.executed_quantity
            
            # Wait for next slice (with some randomness)
            if i < slice_count - 1:
                wait_time = slice_interval + np.random.uniform(-5, 5)
                await asyncio.sleep(wait_time)
        
        # Create aggregate report
        if executions:
            total_value = sum(e.executed_quantity * e.average_price for e in executions)
            average_price = total_value / total_executed if total_executed > 0 else Decimal("0")
            
            report = ExecutionReport(
                order_id=order.order_id,
                execution_id=str(uuid.uuid4()),
                exchange_order_id=executions[0].exchange_order_id,
                timestamp=datetime.utcnow(),
                status=OrderStatus.FILLED if total_executed >= order.quantity else OrderStatus.PARTIAL,
                executed_quantity=total_executed,
                remaining_quantity=order.quantity - total_executed,
                average_price=average_price,
                last_price=executions[-1].average_price,
                last_quantity=executions[-1].executed_quantity,
                commission=sum(e.commission for e in executions),
                commission_asset=executions[0].commission_asset,
                venue="TWAP",
                metadata={
                    "slices": len(executions),
                    "duration_seconds": (datetime.utcnow() - start_time).total_seconds()
                }
            )
        else:
            report = self._create_failed_execution_report(order)
        
        return report
    
    async def _execute_vwap_order(self, order: Order, plan: ExecutionPlan) -> ExecutionReport:
        """Execute VWAP (Volume-Weighted Average Price) order"""
        # Get historical volume profile
        volume_profile = await self._get_volume_profile(order.symbol)
        
        # Calculate execution schedule based on volume
        schedule = self._calculate_vwap_schedule(order, volume_profile)
        
        total_executed = Decimal("0")
        executions = []
        
        for scheduled_time, scheduled_size in schedule:
            # Wait until scheduled time
            wait_time = (scheduled_time - datetime.utcnow()).total_seconds()
            if wait_time > 0:
                await asyncio.sleep(wait_time)
            
            # Execute scheduled portion
            sub_order = Order(
                order_id=f"{order.order_id}_vwap_{len(executions)}",
                symbol=order.symbol,
                exchange=order.exchange,
                side=order.side,
                order_type=OrderType.LIMIT if order.price else OrderType.MARKET,
                quantity=scheduled_size,
                price=order.price,
                time_in_force=TimeInForce.IOC
            )
            
            execution = await self._execute_simple_order(sub_order, plan)
            
            if execution.executed_quantity > 0:
                executions.append(execution)
                total_executed += execution.executed_quantity
        
        # Create aggregate report
        if executions:
            total_value = sum(e.executed_quantity * e.average_price for e in executions)
            average_price = total_value / total_executed if total_executed > 0 else Decimal("0")
            
            report = ExecutionReport(
                order_id=order.order_id,
                execution_id=str(uuid.uuid4()),
                exchange_order_id=executions[0].exchange_order_id,
                timestamp=datetime.utcnow(),
                status=OrderStatus.FILLED if total_executed >= order.quantity else OrderStatus.PARTIAL,
                executed_quantity=total_executed,
                remaining_quantity=order.quantity - total_executed,
                average_price=average_price,
                last_price=executions[-1].average_price,
                last_quantity=executions[-1].executed_quantity,
                commission=sum(e.commission for e in executions),
                commission_asset=executions[0].commission_asset,
                venue="VWAP",
                metadata={"executions": len(executions)}
            )
        else:
            report = self._create_failed_execution_report(order)
        
        return report
    
    async def _execute_simple_order(self, order: Order, plan: ExecutionPlan) -> ExecutionReport:
        """Execute a simple order on best venue"""
        # Select best venue for simple execution
        venue = plan.venues[0] if plan.venues else order.exchange
        
        # Get market data
        market_data = await self._get_venue_market_data(venue, order.symbol)
        
        # Execute on venue
        try:
            result = await self._execute_on_venue(venue, order, order.quantity, market_data)
            
            if result:
                report = ExecutionReport(
                    order_id=order.order_id,
                    execution_id=str(uuid.uuid4()),
                    exchange_order_id=result["exchange_order_id"],
                    timestamp=datetime.utcnow(),
                    status=OrderStatus.FILLED if result["executed_quantity"] >= order.quantity else OrderStatus.PARTIAL,
                    executed_quantity=result["executed_quantity"],
                    remaining_quantity=order.quantity - result["executed_quantity"],
                    average_price=result["price"],
                    last_price=result["price"],
                    last_quantity=result["executed_quantity"],
                    commission=result["commission"],
                    commission_asset=result["commission_asset"],
                    is_maker=result.get("is_maker", False),
                    venue=venue
                )
                return report
                
        except Exception as e:
            self.logger.error(f"Simple order execution failed: {e}")
        
        return self._create_failed_execution_report(order)
    
    async def _execute_on_venue(
        self, 
        venue: str, 
        order: Order, 
        quantity: Decimal,
        market_data: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Execute order on specific venue"""
        # This would interface with actual exchange APIs
        # For now, simulate execution
        
        # Simulate execution delay
        await asyncio.sleep(np.random.uniform(0.05, 0.2))
        
        # Simulate execution price based on order type
        if order.order_type == OrderType.MARKET:
            if order.side == "BUY":
                execution_price = market_data.get("ask", order.price or Decimal("50000"))
            else:
                execution_price = market_data.get("bid", order.price or Decimal("50000"))
        else:
            execution_price = order.price
        
        # Simulate partial fills
        fill_rate = np.random.uniform(0.95, 1.0) if order.order_type == OrderType.MARKET else np.random.uniform(0.7, 1.0)
        executed_quantity = quantity * Decimal(str(fill_rate))
        
        # Calculate commission
        fee_rate = Decimal("0.0005")  # 0.05%
        commission = executed_quantity * execution_price * fee_rate
        
        return {
            "exchange_order_id": f"{venue}_{uuid.uuid4().hex[:8]}",
            "executed_quantity": executed_quantity,
            "price": execution_price,
            "commission": commission,
            "commission_asset": "USDT",
            "is_maker": order.order_type == OrderType.LIMIT
        }
    
    def _determine_execution_strategy(
        self, 
        order: Order, 
        market_conditions: Dict[str, Any]
    ) -> ExecutionStrategy:
        """Determine optimal execution strategy"""
        # Use configured strategy if set
        if order.metadata.get("execution_strategy"):
            return ExecutionStrategy(order.metadata["execution_strategy"])
        
        # Large orders use specialized strategies
        if order.quantity > self.min_order_size * 50:
            if market_conditions.get("volatility", 0) > 0.02:
                return ExecutionStrategy.TWAP  # Time slice in volatile markets
            else:
                return ExecutionStrategy.ICEBERG  # Hide size in calm markets
        
        # Use VWAP for medium orders during regular hours
        if order.quantity > self.min_order_size * 10:
            current_hour = datetime.utcnow().hour
            if 8 <= current_hour <= 20:  # Regular trading hours
                return ExecutionStrategy.VWAP
        
        # Default to smart routing
        return ExecutionStrategy.SMART
    
    async def _select_execution_venues(
        self, 
        order: Order, 
        market_conditions: Dict[str, Any]
    ) -> List[str]:
        """Select best venues for execution"""
        available_venues = list(self.exchange_clients.keys())
        
        if not self.enable_smart_routing:
            return [order.exchange]
        
        # Score venues based on metrics
        venue_scores = []
        
        for venue in available_venues:
            metrics_key = (venue, order.symbol)
            metrics = self.venue_metrics.get(metrics_key)
            
            if not metrics:
                continue
            
            # Calculate venue score
            score = self._calculate_venue_score(metrics, order, market_conditions)
            venue_scores.append((venue, score))
        
        # Sort by score and return top venues
        venue_scores.sort(key=lambda x: x[1], reverse=True)
        selected_venues = [v[0] for v in venue_scores[:3]]  # Top 3 venues
        
        # Always include primary exchange
        if order.exchange not in selected_venues:
            selected_venues.insert(0, order.exchange)
        
        return selected_venues
    
    def _calculate_venue_score(
        self, 
        metrics: VenueMetrics, 
        order: Order,
        market_conditions: Dict[str, Any]
    ) -> float:
        """Calculate venue suitability score"""
        score = 0.0
        
        # Fill rate is most important
        score += metrics.fill_rate * 30
        
        # Low slippage is critical
        score += (1 - metrics.average_slippage) * 25
        
        # Fast execution for market orders
        if order.order_type == OrderType.MARKET:
            score += (1 / (metrics.average_fill_time + 1)) * 20
        
        # Low fees
        score += (1 - float(metrics.average_fee)) * 15
        
        # Good liquidity
        if metrics.average_depth > 0:
            score += min(float(metrics.average_depth / order.quantity), 1) * 10
        
        return score
    
    def _calculate_order_splits(
        self, 
        order: Order, 
        market_conditions: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Calculate how to split large orders"""
        if not self.enable_order_splitting or order.quantity <= self.min_order_size * 5:
            return {
                "count": 1,
                "sizes": [order.quantity],
                "timings": [datetime.utcnow()]
            }
        
        # Calculate optimal split count
        market_impact = market_conditions.get("expected_impact", 0.001)
        optimal_splits = min(
            int(order.quantity / (self.min_order_size * 10)),
            self.max_order_splits
        )
        
        # Randomize sizes slightly
        base_size = order.quantity / Decimal(str(optimal_splits))
        sizes = []
        remaining = order.quantity
        
        for i in range(optimal_splits - 1):
            variation = Decimal(str(np.random.uniform(0.8, 1.2)))
            size = min(base_size * variation, remaining)
            sizes.append(size)
            remaining -= size
        
        sizes.append(remaining)  # Last split gets remainder
        
        # Calculate timings
        total_duration = min(300, optimal_splits * 30)  # Max 5 minutes
        interval = total_duration / optimal_splits
        timings = [
            datetime.utcnow() + timedelta(seconds=i * interval)
            for i in range(optimal_splits)
        ]
        
        return {
            "count": optimal_splits,
            "sizes": sizes,
            "timings": timings
        }
    
    def _calculate_urgency(self, order: Order) -> float:
        """Calculate order urgency (0=passive, 1=aggressive)"""
        # Market orders are urgent
        if order.order_type == OrderType.MARKET:
            return 1.0
        
        # IOC/FOK orders are urgent
        if order.time_in_force in [TimeInForce.IOC, TimeInForce.FOK]:
            return 0.9
        
        # Check if price is favorable
        if order.metadata.get("urgency"):
            return float(order.metadata["urgency"])
        
        # Default based on order type
        return 0.3 if order.order_type == OrderType.LIMIT else 0.7
    
    def _calculate_max_spread_cross(self, order: Order) -> Decimal:
        """Calculate maximum spread to cross"""
        # Market orders cross full spread
        if order.order_type == OrderType.MARKET:
            return Decimal("1.0")
        
        # Limit orders don't cross spread
        if order.order_type == OrderType.LIMIT:
            return Decimal("0")
        
        # Adaptive based on urgency
        if hasattr(order, "metadata") and "max_spread_cross" in order.metadata:
            return Decimal(str(order.metadata["max_spread_cross"]))
        
        return Decimal("0.001")  # 0.1% default
    
    def _calculate_venue_execution_size(
        self,
        remaining_quantity: Decimal,
        market_data: Dict[str, Any],
        plan: ExecutionPlan
    ) -> Decimal:
        """Calculate optimal size to execute on venue"""
        # Check available liquidity
        if "depth" in market_data:
            available_liquidity = market_data["depth"]
            max_participation = available_liquidity * plan.max_participation_rate
            
            # Don't exceed participation rate
            size = min(remaining_quantity, max_participation)
        else:
            size = remaining_quantity
        
        # Apply minimum size
        if plan.min_fill_size and size < plan.min_fill_size:
            return Decimal("0")
        
        return size
    
    async def _analyze_market_conditions(self, symbol: str) -> Dict[str, Any]:
        """Analyze current market conditions for symbol"""
        # This would analyze real market data
        # For now, return simulated conditions
        
        recent_trades = self.recent_executions.get(symbol, [])
        
        # Calculate recent volatility
        if len(recent_trades) >= 10:
            prices = [float(t.average_price) for t in recent_trades]
            volatility = np.std(prices) / np.mean(prices) if prices else 0.01
        else:
            volatility = 0.01
        
        return {
            "volatility": volatility,
            "expected_impact": volatility * 0.1,
            "liquidity": "normal",
            "trend": "neutral"
        }
    
    async def _get_venue_market_data(self, venue: str, symbol: str) -> Dict[str, Any]:
        """Get current market data from venue"""
        # This would fetch real market data
        # For now, return simulated data
        
        base_price = Decimal("50000")  # Placeholder
        spread = base_price * Decimal("0.0001")  # 0.01% spread
        
        return {
            "bid": base_price - spread / 2,
            "ask": base_price + spread / 2,
            "last": base_price,
            "volume": Decimal("1000000"),
            "depth": Decimal("50000")
        }
    
    async def _get_volume_profile(self, symbol: str) -> List[Tuple[int, float]]:
        """Get historical volume profile for VWAP"""
        # This would analyze historical data
        # For now, return typical intraday profile
        
        # Typical U-shaped volume profile
        profile = []
        for hour in range(24):
            if 9 <= hour <= 16:  # Market hours
                if hour in [9, 16]:  # Open/close
                    volume_pct = 0.15
                else:
                    volume_pct = 0.07
            else:
                volume_pct = 0.02
            
            profile.append((hour, volume_pct))
        
        return profile
    
    def _calculate_vwap_schedule(
        self, 
        order: Order, 
        volume_profile: List[Tuple[int, float]]
    ) -> List[Tuple[datetime, Decimal]]:
        """Calculate VWAP execution schedule"""
        schedule = []
        current_time = datetime.utcnow()
        
        # Distribute order according to volume profile
        for hour, volume_pct in volume_profile:
            # Skip past hours
            target_time = current_time.replace(hour=hour, minute=0, second=0)
            if target_time < current_time:
                continue
            
            # Calculate size for this hour
            size = order.quantity * Decimal(str(volume_pct))
            
            if size > self.min_order_size:
                # Add some randomness to timing
                minutes_offset = np.random.randint(0, 59)
                execution_time = target_time + timedelta(minutes=minutes_offset)
                schedule.append((execution_time, size))
        
        return schedule[:self.max_order_splits]
    
    def _create_failed_execution_report(self, order: Order) -> ExecutionReport:
        """Create execution report for failed order"""
        return ExecutionReport(
            order_id=order.order_id,
            execution_id=str(uuid.uuid4()),
            exchange_order_id="",
            timestamp=datetime.utcnow(),
            status=OrderStatus.FAILED,
            executed_quantity=Decimal("0"),
            remaining_quantity=order.quantity,
            average_price=Decimal("0"),
            last_price=Decimal("0"),
            last_quantity=Decimal("0"),
            commission=Decimal("0"),
            commission_asset="",
            venue=order.exchange
        )
    
    async def _add_behavioral_delay(self) -> None:
        """Add human-like reaction delay"""
        min_delay, max_delay = self.human_reaction_time
        delay = np.random.uniform(min_delay, max_delay)
        await asyncio.sleep(delay)
    
    async def _update_venue_metrics_from_execution(self, report: ExecutionReport) -> None:
        """Update venue metrics based on execution"""
        if report.venue == "SMART":
            return  # Skip aggregate executions
        
        venue = report.venue
        symbol = self.active_orders.get(report.order_id, Order).symbol if report.order_id in self.active_orders else ""
        
        if not symbol:
            return
        
        metrics_key = (venue, symbol)
        
        if metrics_key not in self.venue_metrics:
            self.venue_metrics[metrics_key] = VenueMetrics(venue=venue, symbol=symbol)
        
        metrics = self.venue_metrics[metrics_key]
        
        # Update metrics
        metrics.executions += 1
        metrics.total_volume += report.executed_quantity
        
        # Update fill rate
        if report.status == OrderStatus.FILLED:
            metrics.fill_rate = (metrics.fill_rate * (metrics.executions - 1) + 1.0) / metrics.executions
        else:
            metrics.fill_rate = (metrics.fill_rate * (metrics.executions - 1)) / metrics.executions
        
        # Update average fill time
        if report.execution_time_ms:
            metrics.average_fill_time = (
                (metrics.average_fill_time * (metrics.executions - 1) + report.execution_time_ms) / 
                metrics.executions
            )
        
        # Update slippage
        if report.slippage is not None:
            metrics.average_slippage = (
                (metrics.average_slippage * (metrics.executions - 1) + float(report.slippage)) / 
                metrics.executions
            )
        
        metrics.last_updated = datetime.utcnow()
    
    async def _initialize_venue_metrics(self) -> None:
        """Initialize venue metrics with defaults"""
        # This would load historical metrics
        self.logger.info("Initializing venue metrics...")
    
    async def _execution_monitoring_loop(self) -> None:
        """Monitor execution performance"""
        while True:
            try:
                # Log execution statistics
                if self.execution_latencies:
                    avg_latency = sum(self.execution_latencies) / len(self.execution_latencies)
                    self.logger.info(f"Average execution latency: {avg_latency:.1f}ms")
                
                if self.slippage_history:
                    avg_slippage = sum(self.slippage_history) / len(self.slippage_history)
                    self.logger.info(f"Average slippage: {avg_slippage:.4%}")
                
                await asyncio.sleep(60)  # Every minute
                
            except Exception as e:
                self.logger.error(f"Error in execution monitoring: {e}")
                await asyncio.sleep(60)
    
    async def _venue_metrics_update_loop(self) -> None:
        """Update venue metrics periodically"""
        while True:
            try:
                # This would fetch fresh venue statistics
                await asyncio.sleep(300)  # Every 5 minutes
                
            except Exception as e:
                self.logger.error(f"Error updating venue metrics: {e}")
                await asyncio.sleep(300)
    
    def get_recent_executions(self, limit: int = 100) -> List[ExecutionReport]:
        """Get recent executions"""
        return list(self.execution_history)[-limit:]
    
    def get_execution_stats(self) -> Dict[str, Any]:
        """Get execution statistics"""
        total_executions = len(self.execution_history)
        
        if total_executions == 0:
            return {
                "total_executions": 0,
                "fill_rate": 0,
                "average_latency": 0,
                "average_slippage": 0
            }
        
        filled = sum(1 for e in self.execution_history if e.status == OrderStatus.FILLED)
        
        return {
            "total_executions": total_executions,
            "fill_rate": filled / total_executions,
            "average_latency": sum(self.execution_latencies) / len(self.execution_latencies) if self.execution_latencies else 0,
            "average_slippage": sum(self.slippage_history) / len(self.slippage_history) if self.slippage_history else 0,
            "venue_metrics": {
                f"{v.venue}_{v.symbol}": {
                    "fill_rate": v.fill_rate,
                    "avg_slippage": v.average_slippage,
                    "executions": v.executions
                }
                for v in self.venue_metrics.values()
            }
        }