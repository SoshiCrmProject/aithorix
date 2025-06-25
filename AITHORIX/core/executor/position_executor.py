"""
AITHORIX Position Executor
Manages position lifecycle including opening, adjusting, and closing positions

This module handles:
- Position lifecycle management
- Risk-based position sizing
- Position adjustment and rebalancing
- Emergency position management
- Cross-exchange position coordination
"""

import asyncio
import logging
from typing import Dict, List, Optional, Any, Tuple, Set
from datetime import datetime, timedelta
from decimal import Decimal
from dataclasses import dataclass, field
from enum import Enum
import uuid
from collections import defaultdict

from core.engine.position_manager import Position, PositionStatus, PositionType
from core.engine.order_manager import Order, OrderType, OrderStatus, TimeInForce
from core.engine.risk_engine import RiskEngine, RiskLevel
from core.executor.order_executor import OrderExecutor, OrderExecutionPlan
from core.coordinator.exchange_coordinator import ExchangeCoordinator
from utils.helpers import get_timestamp, synchronized
from monitoring.metrics import MetricsCollector


class PositionAction(Enum):
    """Position management actions"""
    OPEN = "open"
    INCREASE = "increase"
    REDUCE = "reduce"
    CLOSE = "close"
    HEDGE = "hedge"
    REBALANCE = "rebalance"
    EMERGENCY_CLOSE = "emergency_close"
    STOP_LOSS = "stop_loss"
    TAKE_PROFIT = "take_profit"


@dataclass
class PositionAdjustment:
    """Position adjustment details"""
    adjustment_id: str
    position_id: str
    action: PositionAction
    
    # Adjustment parameters
    target_quantity: Optional[Decimal] = None
    delta_quantity: Optional[Decimal] = None
    target_leverage: Optional[Decimal] = None
    
    # Price levels
    entry_price: Optional[Decimal] = None
    stop_loss: Optional[Decimal] = None
    take_profit: Optional[Decimal] = None
    
    # Risk parameters
    max_slippage: Decimal = Decimal("0.005")  # 0.5%
    urgency: str = "normal"  # normal, high, critical
    
    # Execution tracking
    orders_created: List[str] = field(default_factory=list)
    executed_quantity: Decimal = Decimal("0")
    average_price: Decimal = Decimal("0")
    
    # Status
    status: str = "pending"
    created_at: datetime = field(default_factory=datetime.utcnow)
    completed_at: Optional[datetime] = None
    error_message: Optional[str] = None


@dataclass
class PositionExecutionPlan:
    """Comprehensive position execution plan"""
    plan_id: str
    position_id: str
    action: PositionAction
    
    # Risk validation
    risk_approved: bool = False
    risk_score: float = 0.0
    risk_warnings: List[str] = field(default_factory=list)
    
    # Market analysis
    market_conditions: Dict[str, Any] = field(default_factory=dict)
    optimal_timing: Optional[datetime] = None
    
    # Execution strategy
    execution_strategy: str = "standard"
    split_orders: bool = False
    order_count: int = 1
    
    # Adjustments
    adjustments: List[PositionAdjustment] = field(default_factory=list)
    
    # Results
    total_executed: Decimal = Decimal("0")
    average_price: Decimal = Decimal("0")
    total_fees: Decimal = Decimal("0")
    slippage: Decimal = Decimal("0")
    
    # Timing
    created_at: datetime = field(default_factory=datetime.utcnow)
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    
    # Status
    status: str = "created"
    success: bool = False
    failure_reason: Optional[str] = None


class PositionExecutor:
    """
    Advanced position execution management
    
    Handles all aspects of position lifecycle management including
    opening, sizing, adjusting, and closing positions across multiple exchanges.
    """
    
    def __init__(
        self,
        order_executor: OrderExecutor,
        risk_engine: RiskEngine,
        exchange_coordinator: ExchangeCoordinator,
        config: Dict[str, Any]
    ):
        self.order_executor = order_executor
        self.risk_engine = risk_engine
        self.exchange_coordinator = exchange_coordinator
        self.config = config
        self.logger = logging.getLogger("AITHORIX.PositionExecutor")
        
        # Position tracking
        self.active_positions: Dict[str, Position] = {}
        self.position_history: List[Position] = []
        self.execution_plans: Dict[str, PositionExecutionPlan] = {}
        
        # Performance tracking
        self.metrics_collector = MetricsCollector()
        self.position_stats: Dict[str, Dict[str, Any]] = defaultdict(dict)
        
        # Configuration
        self.max_position_size = Decimal(str(config.get("max_position_size", "1000000")))
        self.max_leverage = Decimal(str(config.get("max_leverage", "10")))
        self.default_stop_loss = Decimal(str(config.get("default_stop_loss", "0.02")))  # 2%
        self.default_take_profit = Decimal(str(config.get("default_take_profit", "0.05")))  # 5%
        self.enable_auto_hedging = config.get("enable_auto_hedging", True)
        self.enable_pyramiding = config.get("enable_pyramiding", True)
        self.max_pyramiding_levels = config.get("max_pyramiding_levels", 5)
        
        # Position queues
        self.position_queue: asyncio.Queue = asyncio.Queue()
        self.emergency_queue: asyncio.Queue = asyncio.Queue()
        
        # State
        self._lock = asyncio.Lock()
        self.is_running = False
        self.emergency_mode = False
        
    async def start(self) -> None:
        """Start the position executor"""
        self.logger.info("Starting Position Executor...")
        self.is_running = True
        
        # Start worker tasks
        asyncio.create_task(self._position_worker())
        asyncio.create_task(self._emergency_worker())
        asyncio.create_task(self._position_monitoring_loop())
        asyncio.create_task(self._stop_loss_monitoring_loop())
        
    async def stop(self) -> None:
        """Stop the position executor"""
        self.logger.info("Stopping Position Executor...")
        self.is_running = False
        
        # Close all positions if configured
        if self.config.get("close_on_shutdown", False):
            await self.close_all_positions("system_shutdown")
    
    @synchronized
    async def open_position(
        self,
        symbol: str,
        exchange: str,
        side: str,
        quantity: Decimal,
        position_type: PositionType = PositionType.SPOT,
        leverage: Optional[Decimal] = None,
        stop_loss: Optional[Decimal] = None,
        take_profit: Optional[Decimal] = None,
        metadata: Optional[Dict[str, Any]] = None
    ) -> PositionExecutionPlan:
        """Open a new position"""
        position_id = f"POS_{get_timestamp()}_{uuid.uuid4().hex[:8]}"
        
        # Create position object
        position = Position(
            position_id=position_id,
            symbol=symbol,
            exchange=exchange,
            side=side,
            quantity=quantity,
            position_type=position_type,
            leverage=leverage or Decimal("1"),
            status=PositionStatus.PENDING,
            metadata=metadata or {}
        )
        
        # Create execution plan
        plan = PositionExecutionPlan(
            plan_id=f"PLAN_{get_timestamp()}",
            position_id=position_id,
            action=PositionAction.OPEN
        )
        
        # Store position and plan
        self.active_positions[position_id] = position
        self.execution_plans[plan.plan_id] = plan
        
        # Queue for execution
        await self.position_queue.put((position, plan, {
            "stop_loss": stop_loss,
            "take_profit": take_profit
        }))
        
        self.logger.info(f"Queued position {position_id} for opening")
        return plan
    
    @synchronized
    async def adjust_position(
        self,
        position_id: str,
        action: PositionAction,
        quantity: Optional[Decimal] = None,
        stop_loss: Optional[Decimal] = None,
        take_profit: Optional[Decimal] = None,
        urgency: str = "normal"
    ) -> PositionAdjustment:
        """Adjust an existing position"""
        position = self.active_positions.get(position_id)
        if not position:
            raise ValueError(f"Position {position_id} not found")
        
        adjustment = PositionAdjustment(
            adjustment_id=f"ADJ_{get_timestamp()}",
            position_id=position_id,
            action=action,
            target_quantity=quantity,
            stop_loss=stop_loss,
            take_profit=take_profit,
            urgency=urgency
        )
        
        # Queue based on urgency
        if urgency == "critical" or action == PositionAction.EMERGENCY_CLOSE:
            await self.emergency_queue.put((position, adjustment))
        else:
            await self.position_queue.put((position, None, adjustment))
        
        self.logger.info(f"Queued adjustment {adjustment.adjustment_id} for position {position_id}")
        return adjustment
    
    async def close_position(
        self,
        position_id: str,
        reason: str = "manual",
        urgency: str = "normal"
    ) -> PositionAdjustment:
        """Close a position"""
        return await self.adjust_position(
            position_id=position_id,
            action=PositionAction.CLOSE,
            urgency=urgency
        )
    
    async def close_all_positions(self, reason: str = "manual") -> List[PositionAdjustment]:
        """Close all active positions"""
        adjustments = []
        
        for position_id in list(self.active_positions.keys()):
            try:
                adj = await self.close_position(position_id, reason, urgency="high")
                adjustments.append(adj)
            except Exception as e:
                self.logger.error(f"Error closing position {position_id}: {e}")
        
        return adjustments
    
    async def _position_worker(self) -> None:
        """Main position execution worker"""
        while self.is_running:
            try:
                # Get item from queue
                item = await asyncio.wait_for(self.position_queue.get(), timeout=1.0)
                
                position, plan, params = item
                
                if plan:
                    # New position
                    await self._execute_position_open(position, plan, params)
                else:
                    # Position adjustment
                    adjustment = params
                    await self._execute_position_adjustment(position, adjustment)
                
            except asyncio.TimeoutError:
                continue
            except Exception as e:
                self.logger.error(f"Error in position worker: {e}")
                await asyncio.sleep(1)
    
    async def _emergency_worker(self) -> None:
        """Emergency position handler"""
        while self.is_running:
            try:
                position, adjustment = await asyncio.wait_for(
                    self.emergency_queue.get(),
                    timeout=0.1
                )
                
                self.logger.warning(f"Emergency adjustment for position {position.position_id}")
                await self._execute_emergency_adjustment(position, adjustment)
                
            except asyncio.TimeoutError:
                continue
            except Exception as e:
                self.logger.error(f"Error in emergency worker: {e}")
                await asyncio.sleep(0.1)
    
    async def _execute_position_open(
        self,
        position: Position,
        plan: PositionExecutionPlan,
        params: Dict[str, Any]
    ) -> None:
        """Execute position opening"""
        plan.started_at = datetime.utcnow()
        plan.status = "executing"
        
        try:
            # Step 1: Risk validation
            plan.risk_approved, risk_message = await self._validate_position_risk(position)
            if not plan.risk_approved:
                plan.status = "rejected"
                plan.failure_reason = risk_message
                position.status = PositionStatus.REJECTED
                return
            
            # Step 2: Market analysis
            plan.market_conditions = await self._analyze_market_conditions(
                position.symbol,
                position.exchange
            )
            
            # Step 3: Determine execution strategy
            plan.execution_strategy = self._determine_execution_strategy(
                position,
                plan.market_conditions
            )
            
            # Step 4: Create orders
            orders = await self._create_position_orders(position, plan, params)
            
            # Step 5: Execute orders
            execution_results = []
            for order in orders:
                result = await self.order_executor.execute_order(order)
                execution_results.append(result)
            
            # Step 6: Process results
            await self._process_execution_results(position, plan, execution_results)
            
            # Step 7: Set up monitoring
            if position.status == PositionStatus.OPEN:
                await self._setup_position_monitoring(position, params)
            
            plan.completed_at = datetime.utcnow()
            plan.status = "completed"
            plan.success = True
            
        except Exception as e:
            self.logger.error(f"Error executing position open: {e}")
            plan.status = "failed"
            plan.failure_reason = str(e)
            plan.completed_at = datetime.utcnow()
            position.status = PositionStatus.FAILED
    
    async def _execute_position_adjustment(
        self,
        position: Position,
        adjustment: PositionAdjustment
    ) -> None:
        """Execute position adjustment"""
        try:
            adjustment.status = "executing"
            
            # Validate adjustment
            valid, message = await self._validate_adjustment(position, adjustment)
            if not valid:
                adjustment.status = "rejected"
                adjustment.error_message = message
                return
            
            # Execute based on action
            if adjustment.action == PositionAction.INCREASE:
                await self._execute_position_increase(position, adjustment)
            elif adjustment.action == PositionAction.REDUCE:
                await self._execute_position_reduce(position, adjustment)
            elif adjustment.action == PositionAction.CLOSE:
                await self._execute_position_close(position, adjustment)
            elif adjustment.action == PositionAction.HEDGE:
                await self._execute_position_hedge(position, adjustment)
            elif adjustment.action == PositionAction.REBALANCE:
                await self._execute_position_rebalance(position, adjustment)
            elif adjustment.action in [PositionAction.STOP_LOSS, PositionAction.TAKE_PROFIT]:
                await self._execute_position_exit(position, adjustment)
            
            adjustment.status = "completed"
            adjustment.completed_at = datetime.utcnow()
            
        except Exception as e:
            self.logger.error(f"Error executing adjustment: {e}")
            adjustment.status = "failed"
            adjustment.error_message = str(e)
            adjustment.completed_at = datetime.utcnow()
    
    async def _execute_emergency_adjustment(
        self,
        position: Position,
        adjustment: PositionAdjustment
    ) -> None:
        """Execute emergency position adjustment"""
        self.logger.critical(f"Emergency adjustment for position {position.position_id}")
        
        try:
            # Skip most validations for emergency
            adjustment.status = "emergency_executing"
            
            # Create market order to close
            order = Order(
                order_id=f"EMRG_{get_timestamp()}",
                symbol=position.symbol,
                exchange=position.exchange,
                order_type=OrderType.MARKET,
                side="SELL" if position.side == "BUY" else "BUY",
                quantity=position.quantity,
                time_in_force=TimeInForce.IOC,
                metadata={
                    "position_id": position.position_id,
                    "adjustment_id": adjustment.adjustment_id,
                    "emergency": True
                }
            )
            
            # Execute immediately with high priority
            order.metadata["execution_priority"] = "critical"
            result = await self.order_executor.execute_order(order)
            
            # Update position status
            if result.final_status == OrderExecutionStatus.COMPLETED:
                position.status = PositionStatus.CLOSED
                position.closed_at = datetime.utcnow()
                adjustment.status = "completed"
                self.logger.info(f"Emergency close completed for position {position.position_id}")
            else:
                adjustment.status = "failed"
                adjustment.error_message = "Emergency close failed"
                self.logger.error(f"Emergency close failed for position {position.position_id}")
            
        except Exception as e:
            self.logger.error(f"Error in emergency adjustment: {e}")
            adjustment.status = "failed"
            adjustment.error_message = str(e)
    
    async def _validate_position_risk(self, position: Position) -> Tuple[bool, str]:
        """Validate position risk"""
        # Check position size
        if position.quantity * position.entry_price > self.max_position_size:
            return False, "Position size exceeds maximum allowed"
        
        # Check leverage
        if position.leverage > self.max_leverage:
            return False, f"Leverage {position.leverage} exceeds maximum {self.max_leverage}"
        
        # Check with risk engine
        risk_check = await self.risk_engine.evaluate_position_risk(position)
        if not risk_check["approved"]:
            return False, risk_check.get("reason", "Risk check failed")
        
        # Check account balance
        balance_check = await self._check_account_balance(position)
        if not balance_check["sufficient"]:
            return False, "Insufficient account balance"
        
        return True, "Risk validation passed"
    
    async def _analyze_market_conditions(self, symbol: str, exchange: str) -> Dict[str, Any]:
        """Analyze current market conditions"""
        conditions = {
            "timestamp": datetime.utcnow(),
            "volatility": "normal",
            "liquidity": "good",
            "trend": "neutral",
            "spread": Decimal("0"),
            "suitable_for_entry": True
        }
        
        try:
            # Get market data
            ticker = await self.exchange_coordinator.get_ticker(symbol, exchange)
            orderbook = await self.exchange_coordinator.get_orderbook(symbol, exchange)
            
            if ticker and orderbook:
                # Calculate spread
                if orderbook["bids"] and orderbook["asks"]:
                    best_bid = Decimal(str(orderbook["bids"][0][0]))
                    best_ask = Decimal(str(orderbook["asks"][0][0]))
                    conditions["spread"] = (best_ask - best_bid) / best_bid
                
                # Assess volatility (simplified)
                if ticker.get("percentage") and abs(ticker["percentage"]) > 5:
                    conditions["volatility"] = "high"
                elif ticker.get("percentage") and abs(ticker["percentage"]) > 2:
                    conditions["volatility"] = "medium"
                
                # Check liquidity
                if orderbook["bids"] and orderbook["asks"]:
                    bid_depth = sum(float(b[1]) for b in orderbook["bids"][:10])
                    ask_depth = sum(float(a[1]) for a in orderbook["asks"][:10])
                    total_depth = bid_depth + ask_depth
                    
                    if total_depth < 10000:  # Adjust threshold based on asset
                        conditions["liquidity"] = "poor"
                    elif total_depth < 50000:
                        conditions["liquidity"] = "fair"
                
                # Simple trend detection
                if ticker.get("percentage", 0) > 1:
                    conditions["trend"] = "bullish"
                elif ticker.get("percentage", 0) < -1:
                    conditions["trend"] = "bearish"
                
                # Entry suitability
                if conditions["volatility"] == "high" or conditions["liquidity"] == "poor":
                    conditions["suitable_for_entry"] = False
                    
        except Exception as e:
            self.logger.error(f"Error analyzing market conditions: {e}")
        
        return conditions
    
    def _determine_execution_strategy(
        self,
        position: Position,
        market_conditions: Dict[str, Any]
    ) -> str:
        """Determine optimal execution strategy"""
        # Large positions use special strategies
        if position.quantity * position.entry_price > Decimal("100000"):
            if market_conditions["liquidity"] == "poor":
                return "patient_iceberg"
            else:
                return "twap"
        
        # High volatility requires careful execution
        if market_conditions["volatility"] == "high":
            return "defensive"
        
        # Poor liquidity requires patience
        if market_conditions["liquidity"] == "poor":
            return "passive"
        
        # Default strategy
        return "standard"
    
    async def _create_position_orders(
        self,
        position: Position,
        plan: PositionExecutionPlan,
        params: Dict[str, Any]
    ) -> List[Order]:
        """Create orders for position entry"""
        orders = []
        
        # Determine order type based on strategy
        order_type = OrderType.LIMIT
        if plan.execution_strategy in ["aggressive", "emergency"]:
            order_type = OrderType.MARKET
        
        # Split large orders if needed
        if plan.execution_strategy in ["twap", "patient_iceberg"] and position.quantity > Decimal("10000"):
            # Split into smaller chunks
            chunk_count = min(10, int(position.quantity / Decimal("1000")))
            chunk_size = position.quantity / chunk_count
            
            for i in range(chunk_count):
                order = Order(
                    order_id=f"ORD_{position.position_id}_{i}",
                    symbol=position.symbol,
                    exchange=position.exchange,
                    order_type=order_type,
                    side=position.side,
                    quantity=chunk_size,
                    metadata={
                        "position_id": position.position_id,
                        "plan_id": plan.plan_id,
                        "chunk": i + 1,
                        "total_chunks": chunk_count,
                        "execution_strategy": plan.execution_strategy
                    }
                )
                
                # Set price for limit orders
                if order_type == OrderType.LIMIT:
                    # Get current market price
                    ticker = await self.exchange_coordinator.get_ticker(position.symbol, position.exchange)
                    if ticker:
                        if position.side == "BUY":
                            order.price = Decimal(str(ticker["bid"])) * Decimal("0.999")  # Slightly below bid
                        else:
                            order.price = Decimal(str(ticker["ask"])) * Decimal("1.001")  # Slightly above ask
                
                orders.append(order)
                
        else:
            # Single order
            order = Order(
                order_id=f"ORD_{position.position_id}",
                symbol=position.symbol,
                exchange=position.exchange,
                order_type=order_type,
                side=position.side,
                quantity=position.quantity,
                metadata={
                    "position_id": position.position_id,
                    "plan_id": plan.plan_id,
                    "execution_strategy": plan.execution_strategy
                }
            )
            
            if order_type == OrderType.LIMIT:
                ticker = await self.exchange_coordinator.get_ticker(position.symbol, position.exchange)
                if ticker:
                    if position.side == "BUY":
                        order.price = Decimal(str(ticker["bid"]))
                    else:
                        order.price = Decimal(str(ticker["ask"]))
            
            orders.append(order)
        
        return orders
    
    async def _process_execution_results(
        self,
        position: Position,
        plan: PositionExecutionPlan,
        execution_results: List[OrderExecutionPlan]
    ) -> None:
        """Process order execution results"""
        total_executed = Decimal("0")
        total_value = Decimal("0")
        total_fees = Decimal("0")
        
        for result in execution_results:
            if result.final_status == OrderExecutionStatus.COMPLETED:
                for report in result.execution_reports:
                    total_executed += report.executed_quantity
                    total_value += report.executed_quantity * report.average_price
                    total_fees += report.fees
        
        # Update position
        if total_executed > 0:
            position.quantity = total_executed
            position.entry_price = total_value / total_executed
            position.status = PositionStatus.OPEN
            position.opened_at = datetime.utcnow()
            
            # Update plan
            plan.total_executed = total_executed
            plan.average_price = position.entry_price
            plan.total_fees = total_fees
            
            # Calculate slippage
            if position.entry_price and plan.market_conditions.get("last_price"):
                expected_price = Decimal(str(plan.market_conditions["last_price"]))
                plan.slippage = abs(position.entry_price - expected_price) / expected_price
            
            self.logger.info(f"Position {position.position_id} opened: {total_executed} @ {position.entry_price}")
        else:
            position.status = PositionStatus.FAILED
            self.logger.error(f"Failed to open position {position.position_id}")
    
    async def _setup_position_monitoring(self, position: Position, params: Dict[str, Any]) -> None:
        """Set up position monitoring and stops"""
        # Set stop loss
        if params.get("stop_loss"):
            position.stop_loss = Decimal(str(params["stop_loss"]))
        elif self.default_stop_loss:
            if position.side == "BUY":
                position.stop_loss = position.entry_price * (Decimal("1") - self.default_stop_loss)
            else:
                position.stop_loss = position.entry_price * (Decimal("1") + self.default_stop_loss)
        
        # Set take profit
        if params.get("take_profit"):
            position.take_profit = Decimal(str(params["take_profit"]))
        elif self.default_take_profit:
            if position.side == "BUY":
                position.take_profit = position.entry_price * (Decimal("1") + self.default_take_profit)
            else:
                position.take_profit = position.entry_price * (Decimal("1") - self.default_take_profit)
        
        # Update position stats
        self.position_stats[position.position_id] = {
            "opened_at": position.opened_at,
            "entry_price": float(position.entry_price),
            "stop_loss": float(position.stop_loss) if position.stop_loss else None,
            "take_profit": float(position.take_profit) if position.take_profit else None,
            "max_profit": 0.0,
            "max_loss": 0.0,
            "current_pnl": 0.0
        }
    
    async def _validate_adjustment(
        self,
        position: Position,
        adjustment: PositionAdjustment
    ) -> Tuple[bool, str]:
        """Validate position adjustment"""
        # Check position status
        if position.status != PositionStatus.OPEN:
            return False, f"Position is not open (status: {position.status})"
        
        # Validate action-specific requirements
        if adjustment.action == PositionAction.INCREASE:
            if not adjustment.target_quantity and not adjustment.delta_quantity:
                return False, "Increase action requires quantity"
            
            # Check pyramiding settings
            if self.enable_pyramiding:
                pyramid_count = position.metadata.get("pyramid_level", 1)
                if pyramid_count >= self.max_pyramiding_levels:
                    return False, f"Maximum pyramiding level {self.max_pyramiding_levels} reached"
        
        elif adjustment.action == PositionAction.REDUCE:
            if not adjustment.target_quantity and not adjustment.delta_quantity:
                return False, "Reduce action requires quantity"
            
            # Check if reduction is valid
            reduce_qty = adjustment.delta_quantity or (position.quantity - adjustment.target_quantity)
            if reduce_qty >= position.quantity:
                return False, "Reduction quantity exceeds position size"
        
        return True, "Validation passed"
    
    async def _execute_position_increase(
        self,
        position: Position,
        adjustment: PositionAdjustment
    ) -> None:
        """Execute position increase (pyramiding)"""
        # Calculate increase quantity
        if adjustment.delta_quantity:
            increase_qty = adjustment.delta_quantity
        else:
            increase_qty = adjustment.target_quantity - position.quantity
        
        # Create order for increase
        order = Order(
            order_id=f"INC_{adjustment.adjustment_id}",
            symbol=position.symbol,
            exchange=position.exchange,
            order_type=OrderType.MARKET if adjustment.urgency == "high" else OrderType.LIMIT,
            side=position.side,
            quantity=increase_qty,
            metadata={
                "position_id": position.position_id,
                "adjustment_id": adjustment.adjustment_id,
                "action": "increase"
            }
        )
        
        # Execute order
        result = await self.order_executor.execute_order(order)
        
        # Update position if successful
        if result.final_status == OrderExecutionStatus.COMPLETED:
            old_cost = position.quantity * position.entry_price
            new_cost = result.total_executed * result.average_price
            position.quantity += result.total_executed
            position.entry_price = (old_cost + new_cost) / position.quantity
            
            # Update pyramid level
            position.metadata["pyramid_level"] = position.metadata.get("pyramid_level", 1) + 1
            
            adjustment.executed_quantity = result.total_executed
            adjustment.average_price = result.average_price
    
    async def _execute_position_reduce(
        self,
        position: Position,
        adjustment: PositionAdjustment
    ) -> None:
        """Execute position reduction"""
        # Calculate reduction quantity
        if adjustment.delta_quantity:
            reduce_qty = adjustment.delta_quantity
        else:
            reduce_qty = position.quantity - adjustment.target_quantity
        
        # Create order for reduction
        order = Order(
            order_id=f"RED_{adjustment.adjustment_id}",
            symbol=position.symbol,
            exchange=position.exchange,
            order_type=OrderType.MARKET if adjustment.urgency == "high" else OrderType.LIMIT,
            side="SELL" if position.side == "BUY" else "BUY",
            quantity=reduce_qty,
            metadata={
                "position_id": position.position_id,
                "adjustment_id": adjustment.adjustment_id,
                "action": "reduce"
            }
        )
        
        # Execute order
        result = await self.order_executor.execute_order(order)
        
        # Update position if successful
        if result.final_status == OrderExecutionStatus.COMPLETED:
            position.quantity -= result.total_executed
            
            # Calculate realized PnL
            if position.side == "BUY":
                pnl = (result.average_price - position.entry_price) * result.total_executed
            else:
                pnl = (position.entry_price - result.average_price) * result.total_executed
            
            position.realized_pnl += pnl
            
            adjustment.executed_quantity = result.total_executed
            adjustment.average_price = result.average_price
            
            # Close position if fully reduced
            if position.quantity == 0:
                position.status = PositionStatus.CLOSED
                position.closed_at = datetime.utcnow()
    
    async def _execute_position_close(
        self,
        position: Position,
        adjustment: PositionAdjustment
    ) -> None:
        """Execute position close"""
        # Set adjustment to close full position
        adjustment.delta_quantity = position.quantity
        await self._execute_position_reduce(position, adjustment)
    
    async def _execute_position_hedge(
        self,
        position: Position,
        adjustment: PositionAdjustment
    ) -> None:
        """Execute position hedge"""
        # Create opposite position as hedge
        hedge_side = "SELL" if position.side == "BUY" else "BUY"
        
        # Determine hedge instrument (could be different from position)
        hedge_symbol = position.symbol  # Could use futures/options
        hedge_exchange = position.exchange
        
        # Create hedge position
        hedge_plan = await self.open_position(
            symbol=hedge_symbol,
            exchange=hedge_exchange,
            side=hedge_side,
            quantity=position.quantity,  # Full hedge
            position_type=PositionType.HEDGE,
            metadata={
                "hedge_for": position.position_id,
                "original_position": position.position_id
            }
        )
        
        # Link positions
        position.metadata["hedge_position"] = hedge_plan.position_id
        adjustment.orders_created.append(hedge_plan.position_id)
    
    async def _execute_position_rebalance(
        self,
        position: Position,
        adjustment: PositionAdjustment
    ) -> None:
        """Execute position rebalance"""
        # Get target allocation
        target_value = adjustment.metadata.get("target_value")
        if not target_value:
            return
        
        # Calculate current value
        current_price = await self._get_current_price(position.symbol, position.exchange)
        current_value = position.quantity * current_price
        
        # Determine adjustment needed
        value_diff = Decimal(str(target_value)) - current_value
        
        if abs(value_diff) / current_value > Decimal("0.02"):  # 2% threshold
            # Calculate quantity adjustment
            qty_adjustment = value_diff / current_price
            
            if qty_adjustment > 0:
                adjustment.delta_quantity = qty_adjustment
                await self._execute_position_increase(position, adjustment)
            else:
                adjustment.delta_quantity = abs(qty_adjustment)
                await self._execute_position_reduce(position, adjustment)
    
    async def _execute_position_exit(
        self,
        position: Position,
        adjustment: PositionAdjustment
    ) -> None:
        """Execute stop loss or take profit"""
        # Close position with appropriate urgency
        adjustment.urgency = "high"  # Stops should execute quickly
        await self._execute_position_close(position, adjustment)
        
        # Record exit reason
        position.metadata["exit_reason"] = adjustment.action.value
        position.metadata["exit_price"] = float(adjustment.average_price) if adjustment.average_price else None
    
    async def _check_account_balance(self, position: Position) -> Dict[str, Any]:
        """Check if account has sufficient balance"""
        try:
            balance = await self.exchange_coordinator.get_balance(position.exchange)
            
            # Calculate required balance
            if position.position_type == PositionType.FUTURES:
                # Margin requirement
                required = (position.quantity * position.entry_price) / position.leverage
            else:
                # Full position value
                required = position.quantity * position.entry_price
            
            # Add buffer
            required *= Decimal("1.05")  # 5% buffer
            
            # Check appropriate currency
            if position.side == "BUY":
                # Need quote currency (e.g., USDT)
                quote_currency = position.symbol.split("/")[1] if "/" in position.symbol else "USDT"
                available = balance.get(quote_currency, Decimal("0"))
            else:
                # Need base currency
                base_currency = position.symbol.split("/")[0] if "/" in position.symbol else position.symbol
                available = balance.get(base_currency, Decimal("0"))
            
            return {
                "sufficient": available >= required,
                "available": available,
                "required": required
            }
            
        except Exception as e:
            self.logger.error(f"Error checking account balance: {e}")
            return {"sufficient": False, "error": str(e)}
    
    async def _get_current_price(self, symbol: str, exchange: str) -> Decimal:
        """Get current market price"""
        ticker = await self.exchange_coordinator.get_ticker(symbol, exchange)
        if ticker and "last" in ticker:
            return Decimal(str(ticker["last"]))
        return Decimal("0")
    
    async def _position_monitoring_loop(self) -> None:
        """Monitor all positions"""
        while self.is_running:
            try:
                for position_id, position in list(self.active_positions.items()):
                    if position.status == PositionStatus.OPEN:
                        await self._monitor_position(position)
                
                await asyncio.sleep(1)  # Check every second
                
            except Exception as e:
                self.logger.error(f"Error in position monitoring: {e}")
                await asyncio.sleep(1)
    
    async def _monitor_position(self, position: Position) -> None:
        """Monitor individual position"""
        try:
            # Get current price
            current_price = await self._get_current_price(position.symbol, position.exchange)
            if not current_price:
                return
            
            # Calculate PnL
            if position.side == "BUY":
                pnl_pct = (current_price - position.entry_price) / position.entry_price
            else:
                pnl_pct = (position.entry_price - current_price) / position.entry_price
            
            # Update position stats
            stats = self.position_stats.get(position.position_id, {})
            stats["current_pnl"] = float(pnl_pct)
            stats["max_profit"] = max(stats.get("max_profit", 0), float(pnl_pct))
            stats["max_loss"] = min(stats.get("max_loss", 0), float(pnl_pct))
            
            # Record metrics
            self._record_position_metrics(position, current_price, pnl_pct)
            
        except Exception as e:
            self.logger.error(f"Error monitoring position {position.position_id}: {e}")
    
    async def _stop_loss_monitoring_loop(self) -> None:
        """Monitor stop losses and take profits"""
        while self.is_running:
            try:
                for position_id, position in list(self.active_positions.items()):
                    if position.status == PositionStatus.OPEN:
                        await self._check_exit_conditions(position)
                
                await asyncio.sleep(0.5)  # Check every 500ms for stops
                
            except Exception as e:
                self.logger.error(f"Error in stop monitoring: {e}")
                await asyncio.sleep(0.5)
    
    async def _check_exit_conditions(self, position: Position) -> None:
        """Check if position should be exited"""
        try:
            current_price = await self._get_current_price(position.symbol, position.exchange)
            if not current_price:
                return
            
            should_exit = False
            exit_reason = None
            
            # Check stop loss
            if position.stop_loss:
                if position.side == "BUY" and current_price <= position.stop_loss:
                    should_exit = True
                    exit_reason = PositionAction.STOP_LOSS
                elif position.side == "SELL" and current_price >= position.stop_loss:
                    should_exit = True
                    exit_reason = PositionAction.STOP_LOSS
            
            # Check take profit
            if position.take_profit and not should_exit:
                if position.side == "BUY" and current_price >= position.take_profit:
                    should_exit = True
                    exit_reason = PositionAction.TAKE_PROFIT
                elif position.side == "SELL" and current_price <= position.take_profit:
                    should_exit = True
                    exit_reason = PositionAction.TAKE_PROFIT
            
            # Execute exit if needed
            if should_exit and exit_reason:
                self.logger.info(f"Triggering {exit_reason.value} for position {position.position_id}")
                await self.adjust_position(
                    position_id=position.position_id,
                    action=exit_reason,
                    urgency="high"
                )
                
        except Exception as e:
            self.logger.error(f"Error checking exit conditions: {e}")
    
    def _record_position_metrics(
        self,
        position: Position,
        current_price: Decimal,
        pnl_pct: Decimal
    ) -> None:
        """Record position metrics"""
        metrics = {
            "position_id": position.position_id,
            "symbol": position.symbol,
            "exchange": position.exchange,
            "side": position.side,
            "quantity": float(position.quantity),
            "entry_price": float(position.entry_price),
            "current_price": float(current_price),
            "pnl_percent": float(pnl_pct),
            "leverage": float(position.leverage),
            "status": position.status.value
        }
        
        self.metrics_collector.record_position_metrics(metrics)
    
    def get_active_positions(self) -> Dict[str, Position]:
        """Get all active positions"""
        return {
            pid: pos for pid, pos in self.active_positions.items()
            if pos.status == PositionStatus.OPEN
        }
    
    def get_position_statistics(self) -> Dict[str, Any]:
        """Get position statistics"""
        active_positions = self.get_active_positions()
        
        total_value = Decimal("0")
        total_pnl = Decimal("0")
        
        for position in active_positions.values():
            stats = self.position_stats.get(position.position_id, {})
            position_value = position.quantity * position.entry_price
            total_value += position_value
            total_pnl += position_value * Decimal(str(stats.get("current_pnl", 0)))
        
        return {
            "active_positions": len(active_positions),
            "total_positions": len(self.position_history),
            "total_value": float(total_value),
            "total_pnl": float(total_pnl),
            "average_pnl": float(total_pnl / total_value) if total_value > 0 else 0,
            "positions_by_exchange": self._count_by_exchange(active_positions),
            "positions_by_side": self._count_by_side(active_positions)
        }
    
    def _count_by_exchange(self, positions: Dict[str, Position]) -> Dict[str, int]:
        """Count positions by exchange"""
        counts = defaultdict(int)
        for position in positions.values():
            counts[position.exchange] += 1
        return dict(counts)
    
    def _count_by_side(self, positions: Dict[str, Position]) -> Dict[str, int]:
        """Count positions by side"""
        counts = defaultdict(int)
        for position in positions.values():
            counts[position.side] += 1
        return dict(counts)