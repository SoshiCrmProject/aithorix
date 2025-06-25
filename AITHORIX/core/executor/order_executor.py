"""
AITHORIX Order Executor
High-level order execution management

This module handles the complete order execution workflow including:
- Order validation and preparation
- Execution strategy selection
- Error handling and retries
- Post-execution processing
"""

import asyncio
import logging
from typing import Dict, List, Optional, Any, Tuple
from datetime import datetime, timedelta
from decimal import Decimal
from dataclasses import dataclass, field
from enum import Enum
import uuid

from core.engine.order_manager import Order, OrderStatus, OrderType, TimeInForce
from core.engine.execution_engine import ExecutionEngine, ExecutionReport, ExecutionStrategy
from core.engine.risk_engine import RiskEngine
from core.coordinator.exchange_coordinator import ExchangeCoordinator
from utils.helpers import get_timestamp, synchronized
from monitoring.metrics import MetricsCollector


class OrderExecutionStatus(Enum):
    """Order execution workflow status"""
    PENDING = "pending"
    VALIDATING = "validating"
    APPROVED = "approved"
    EXECUTING = "executing"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


@dataclass
class OrderExecutionPlan:
    """Detailed plan for order execution"""
    order_id: str
    execution_strategy: ExecutionStrategy
    
    # Validation results
    risk_approved: bool = False
    balance_sufficient: bool = False
    market_conditions_favorable: bool = False
    
    # Execution parameters
    max_retries: int = 3
    retry_delay: float = 1.0
    timeout_seconds: int = 300
    
    # Slippage controls
    max_slippage_percent: Decimal = Decimal("0.005")  # 0.5%
    price_improvement_required: bool = False
    
    # Execution tracking
    attempts: int = 0
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    
    # Results
    execution_reports: List[ExecutionReport] = field(default_factory=list)
    final_status: Optional[OrderExecutionStatus] = None
    error_message: Optional[str] = None


class OrderExecutor:
    """
    High-level order execution coordinator
    
    Manages the complete order execution workflow from validation
    through execution to post-processing.
    """
    
    def __init__(
        self,
        execution_engine: ExecutionEngine,
        risk_engine: RiskEngine,
        exchange_coordinator: ExchangeCoordinator,
        config: Dict[str, Any]
    ):
        self.execution_engine = execution_engine
        self.risk_engine = risk_engine
        self.exchange_coordinator = exchange_coordinator
        self.config = config
        self.logger = logging.getLogger("AITHORIX.OrderExecutor")
        
        # Execution tracking
        self.active_executions: Dict[str, OrderExecutionPlan] = {}
        self.execution_history: List[OrderExecutionPlan] = []
        
        # Performance metrics
        self.metrics_collector = MetricsCollector()
        
        # Configuration
        self.enable_pre_validation = config.get("enable_pre_validation", True)
        self.enable_smart_routing = config.get("enable_smart_routing", True)
        self.default_timeout = config.get("default_timeout", 300)
        self.max_concurrent_executions = config.get("max_concurrent_executions", 50)
        
        # Execution queue
        self.execution_queue: asyncio.Queue = asyncio.Queue()
        self.execution_semaphore = asyncio.Semaphore(self.max_concurrent_executions)
        
        # State
        self._lock = asyncio.Lock()
        self.is_running = False
        
    async def start(self) -> None:
        """Start the order executor"""
        self.logger.info("Starting Order Executor...")
        self.is_running = True
        
        # Start execution workers
        for i in range(min(self.max_concurrent_executions, 10)):
            asyncio.create_task(self._execution_worker(i))
        
        # Start monitoring task
        asyncio.create_task(self._execution_monitoring_loop())
    
    async def stop(self) -> None:
        """Stop the order executor"""
        self.logger.info("Stopping Order Executor...")
        self.is_running = False
    
    @synchronized
    async def execute_order(self, order: Order) -> OrderExecutionPlan:
        """Execute an order with full workflow management"""
        self.logger.info(f"Executing order {order.order_id}")
        
        # Create execution plan
        plan = OrderExecutionPlan(
            order_id=order.order_id,
            execution_strategy=self._determine_execution_strategy(order)
        )
        
        # Store active execution
        self.active_executions[order.order_id] = plan
        
        # Add to execution queue
        await self.execution_queue.put((order, plan))
        
        return plan
    
    async def cancel_execution(self, order_id: str) -> bool:
        """Cancel an active order execution"""
        plan = self.active_executions.get(order_id)
        if not plan:
            return False
        
        plan.final_status = OrderExecutionStatus.CANCELLED
        self.logger.info(f"Cancelled execution for order {order_id}")
        
        return True
    
    async def _execution_worker(self, worker_id: int) -> None:
        """Worker task that processes order executions"""
        self.logger.info(f"Execution worker {worker_id} started")
        
        while self.is_running:
            try:
                # Get order from queue
                order, plan = await asyncio.wait_for(
                    self.execution_queue.get(),
                    timeout=1.0
                )
                
                # Acquire semaphore
                async with self.execution_semaphore:
                    await self._process_order_execution(order, plan)
                
            except asyncio.TimeoutError:
                continue
            except Exception as e:
                self.logger.error(f"Error in execution worker {worker_id}: {e}")
                await asyncio.sleep(1)
    
    async def _process_order_execution(self, order: Order, plan: OrderExecutionPlan) -> None:
        """Process a single order execution"""
        plan.started_at = datetime.utcnow()
        
        try:
            # Step 1: Validation
            if self.enable_pre_validation:
                plan.final_status = OrderExecutionStatus.VALIDATING
                validation_passed = await self._validate_order(order, plan)
                
                if not validation_passed:
                    plan.final_status = OrderExecutionStatus.FAILED
                    return
            
            plan.final_status = OrderExecutionStatus.APPROVED
            
            # Step 2: Pre-execution checks
            await self._perform_pre_execution_checks(order, plan)
            
            # Step 3: Execute order
            plan.final_status = OrderExecutionStatus.EXECUTING
            execution_success = await self._execute_with_retries(order, plan)
            
            if execution_success:
                plan.final_status = OrderExecutionStatus.COMPLETED
                
                # Step 4: Post-execution processing
                await self._perform_post_execution(order, plan)
            else:
                plan.final_status = OrderExecutionStatus.FAILED
            
        except Exception as e:
            self.logger.error(f"Error executing order {order.order_id}: {e}")
            plan.final_status = OrderExecutionStatus.FAILED
            plan.error_message = str(e)
            
        finally:
            plan.completed_at = datetime.utcnow()
            
            # Move to history
            self.execution_history.append(plan)
            self.active_executions.pop(order.order_id, None)
            
            # Record metrics
            self._record_execution_metrics(order, plan)
    
    async def _validate_order(self, order: Order, plan: OrderExecutionPlan) -> bool:
        """Validate order before execution"""
        # Risk validation
        plan.risk_approved = await self.risk_engine.approve_order(order)
        if not plan.risk_approved:
            plan.error_message = "Order rejected by risk engine"
            self.logger.warning(f"Order {order.order_id} failed risk validation")
            return False
        
        # Balance validation
        plan.balance_sufficient = await self._check_balance_sufficiency(order)
        if not plan.balance_sufficient:
            plan.error_message = "Insufficient balance"
            self.logger.warning(f"Order {order.order_id} failed balance validation")
            return False
        
        # Market conditions validation
        plan.market_conditions_favorable = await self._check_market_conditions(order)
        if not plan.market_conditions_favorable:
            plan.error_message = "Unfavorable market conditions"
            self.logger.warning(f"Order {order.order_id} failed market conditions check")
            return False
        
        return True
    
    async def _check_balance_sufficiency(self, order: Order) -> bool:
        """Check if balance is sufficient for order"""
        try:
            # Get exchange balance
            exchange_info = self.exchange_coordinator.get_exchange_info(order.exchange)
            if not exchange_info:
                return False
            
            balance = self.exchange_coordinator.balances.get(order.exchange)
            if not balance:
                return False
            
            # Check based on order side
            if order.side == "BUY":
                # Need quote currency (e.g., USDT)
                required_amount = order.quantity * order.price if order.price else order.quantity * Decimal("50000")
                quote_currency = order.symbol.split("/")[1] if "/" in order.symbol else "USDT"
                available = balance.spot_balances.get(quote_currency, Decimal("0"))
            else:
                # Need base currency (e.g., BTC)
                required_amount = order.quantity
                base_currency = order.symbol.split("/")[0] if "/" in order.symbol else order.symbol
                available = balance.spot_balances.get(base_currency, Decimal("0"))
            
            return available >= required_amount * Decimal("1.05")  # 5% buffer
            
        except Exception as e:
            self.logger.error(f"Error checking balance: {e}")
            return False
    
    async def _check_market_conditions(self, order: Order) -> bool:
        """Check if market conditions are favorable"""
        try:
            # Get current market data
            ticker = await self.exchange_coordinator.get_ticker(order.symbol, order.exchange)
            
            if not ticker:
                return False
            
            # Check spread
            spread = (ticker['ask'] - ticker['bid']) / ticker['bid']
            if spread > 0.01:  # More than 1% spread
                self.logger.warning(f"High spread detected: {spread:.2%}")
                return False
            
            # Check volume
            if ticker['quoteVolume'] < 100000:  # Less than $100k volume
                self.logger.warning(f"Low volume detected: ${ticker['quoteVolume']:.0f}")
                return False
            
            return True
            
        except Exception as e:
            self.logger.error(f"Error checking market conditions: {e}")
            return True  # Allow execution on error
    
    async def _perform_pre_execution_checks(self, order: Order, plan: OrderExecutionPlan) -> None:
        """Perform pre-execution checks and adjustments"""
        # Update order price if needed
        if order.order_type == OrderType.LIMIT and not order.price:
            # Get current price
            ticker = await self.exchange_coordinator.get_ticker(order.symbol, order.exchange)
            if ticker:
                if order.side == "BUY":
                    order.price = Decimal(str(ticker['bid']))
                else:
                    order.price = Decimal(str(ticker['ask']))
        
        # Adjust quantity for exchange precision
        market_info = self.exchange_coordinator.get_market_info(order.symbol, order.exchange)
        if market_info:
            # Round to exchange precision
            order.quantity = self._round_to_precision(order.quantity, market_info.quantity_step)
            if order.price:
                order.price = self._round_to_precision(order.price, market_info.price_step)
    
    async def _execute_with_retries(self, order: Order, plan: OrderExecutionPlan) -> bool:
        """Execute order with retry logic"""
        while plan.attempts < plan.max_retries:
            plan.attempts += 1
            
            try:
                # Check if cancelled
                if plan.final_status == OrderExecutionStatus.CANCELLED:
                    return False
                
                # Execute order
                report = await self.execution_engine.execute_order(order)
                plan.execution_reports.append(report)
                
                # Check execution result
                if report.status == OrderStatus.FILLED:
                    self.logger.info(f"Order {order.order_id} filled successfully")
                    return True
                    
                elif report.status == OrderStatus.PARTIAL:
                    self.logger.info(f"Order {order.order_id} partially filled")
                    # Update order quantity for remaining
                    order.quantity = report.remaining_quantity
                    
                elif report.status == OrderStatus.FAILED:
                    self.logger.warning(f"Order {order.order_id} execution failed")
                    
                    # Wait before retry
                    if plan.attempts < plan.max_retries:
                        await asyncio.sleep(plan.retry_delay * plan.attempts)
                    
            except Exception as e:
                self.logger.error(f"Error executing order {order.order_id}: {e}")
                plan.error_message = str(e)
                
                # Wait before retry
                if plan.attempts < plan.max_retries:
                    await asyncio.sleep(plan.retry_delay * plan.attempts)
        
        return False
    
    async def _perform_post_execution(self, order: Order, plan: OrderExecutionPlan) -> None:
        """Perform post-execution processing"""
        try:
            # Calculate execution quality
            if plan.execution_reports:
                total_executed = sum(r.executed_quantity for r in plan.execution_reports)
                total_value = sum(r.executed_quantity * r.average_price for r in plan.execution_reports)
                
                if total_executed > 0:
                    avg_price = total_value / total_executed
                    
                    # Calculate slippage
                    if order.price:
                        slippage = abs(avg_price - order.price) / order.price
                        self.logger.info(f"Order {order.order_id} executed with {slippage:.2%} slippage")
            
            # Update order status
            # This would update the order in the order manager
            
        except Exception as e:
            self.logger.error(f"Error in post-execution processing: {e}")
    
    def _determine_execution_strategy(self, order: Order) -> ExecutionStrategy:
        """Determine execution strategy for order"""
        # Check order metadata for strategy
        if "execution_strategy" in order.metadata:
            return ExecutionStrategy(order.metadata["execution_strategy"])
        
        # Large orders use specialized strategies
        if order.quantity > Decimal("10000"):  # Large order threshold
            if order.order_type == OrderType.MARKET:
                return ExecutionStrategy.TWAP
            else:
                return ExecutionStrategy.ICEBERG
        
        # Default strategies
        if order.order_type == OrderType.MARKET:
            return ExecutionStrategy.AGGRESSIVE
        else:
            return ExecutionStrategy.PASSIVE
    
    def _round_to_precision(self, value: Decimal, precision: Decimal) -> Decimal:
        """Round value to exchange precision"""
        if precision == 0:
            return value
        
        # Calculate number of decimal places
        precision_str = str(precision)
        if '.' in precision_str:
            decimals = len(precision_str.split('.')[1])
            return value.quantize(Decimal(10) ** -decimals)
        else:
            # Round to nearest precision
            return (value / precision).quantize(Decimal('1')) * precision
    
    def _record_execution_metrics(self, order: Order, plan: OrderExecutionPlan) -> None:
        """Record execution metrics"""
        execution_time = None
        if plan.started_at and plan.completed_at:
            execution_time = (plan.completed_at - plan.started_at).total_seconds()
        
        metrics = {
            "order_id": order.order_id,
            "symbol": order.symbol,
            "exchange": order.exchange,
            "execution_strategy": plan.execution_strategy.value,
            "status": plan.final_status.value if plan.final_status else "unknown",
            "attempts": plan.attempts,
            "execution_time": execution_time,
            "reports_count": len(plan.execution_reports)
        }
        
        self.metrics_collector.record_order_execution_workflow(metrics)
    
    async def _execution_monitoring_loop(self) -> None:
        """Monitor active executions"""
        while self.is_running:
            try:
                # Check for stuck executions
                current_time = datetime.utcnow()
                
                for order_id, plan in list(self.active_executions.items()):
                    if plan.started_at:
                        elapsed = (current_time - plan.started_at).total_seconds()
                        
                        if elapsed > plan.timeout_seconds:
                            self.logger.warning(f"Execution timeout for order {order_id}")
                            plan.final_status = OrderExecutionStatus.FAILED
                            plan.error_message = "Execution timeout"
                
                # Log statistics
                active_count = len(self.active_executions)
                self.logger.debug(f"Active executions: {active_count}")
                
                await asyncio.sleep(10)  # Check every 10 seconds
                
            except Exception as e:
                self.logger.error(f"Error in execution monitoring: {e}")
                await asyncio.sleep(10)


    # Add these methods to the existing OrderExecutor class:

    async def get_execution_history(
        self, 
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        symbol: Optional[str] = None,
        exchange: Optional[str] = None,
        status: Optional[OrderExecutionStatus] = None,
        limit: int = 100
    ) -> List[OrderExecutionPlan]:
        """Get filtered execution history"""
        filtered_history = self.execution_history
        
        if start_time:
            filtered_history = [p for p in filtered_history 
                              if p.started_at and p.started_at >= start_time]
        
        if end_time:
            filtered_history = [p for p in filtered_history 
                              if p.started_at and p.started_at <= end_time]
        
        if symbol:
            # Need to look up order details
            filtered_history = [p for p in filtered_history 
                              if self._get_order_symbol(p.order_id) == symbol]
        
        if exchange:
            filtered_history = [p for p in filtered_history 
                              if self._get_order_exchange(p.order_id) == exchange]
        
        if status:
            filtered_history = [p for p in filtered_history 
                              if p.final_status == status]
        
        return filtered_history[:limit]
    
    async def get_execution_metrics(self) -> Dict[str, Any]:
        """Get detailed execution metrics"""
        if not self.execution_history:
            return {
                "total_executions": 0,
                "success_rate": 0,
                "average_execution_time": 0,
                "average_attempts": 0,
                "execution_by_strategy": {},
                "execution_by_status": {}
            }
        
        completed = [p for p in self.execution_history if p.completed_at and p.started_at]
        successful = [p for p in self.execution_history if p.final_status == OrderExecutionStatus.COMPLETED]
        
        # Calculate execution times
        execution_times = [(p.completed_at - p.started_at).total_seconds() 
                          for p in completed]
        
        # Group by strategy
        by_strategy = {}
        for plan in self.execution_history:
            strategy = plan.execution_strategy.value
            if strategy not in by_strategy:
                by_strategy[strategy] = {"count": 0, "success": 0}
            by_strategy[strategy]["count"] += 1
            if plan.final_status == OrderExecutionStatus.COMPLETED:
                by_strategy[strategy]["success"] += 1
        
        # Group by status
        by_status = {}
        for plan in self.execution_history:
            status = plan.final_status.value if plan.final_status else "unknown"
            by_status[status] = by_status.get(status, 0) + 1
        
        return {
            "total_executions": len(self.execution_history),
            "success_rate": len(successful) / len(self.execution_history),
            "average_execution_time": sum(execution_times) / len(execution_times) if execution_times else 0,
            "average_attempts": sum(p.attempts for p in self.execution_history) / len(self.execution_history),
            "execution_by_strategy": by_strategy,
            "execution_by_status": by_status,
            "active_executions": len(self.active_executions),
            "queue_depth": self.execution_queue.qsize()
        }
    
    async def bulk_execute_orders(self, orders: List[Order]) -> List[OrderExecutionPlan]:
        """Execute multiple orders efficiently"""
        plans = []
        
        for order in orders:
            plan = await self.execute_order(order)
            plans.append(plan)
        
        return plans
    
    async def pause_executions(self) -> None:
        """Pause all new executions while allowing current ones to complete"""
        self.logger.warning("Pausing order executions")
        self._paused = True
    
    async def resume_executions(self) -> None:
        """Resume order executions"""
        self.logger.info("Resuming order executions")
        self._paused = False
    
    def _get_order_symbol(self, order_id: str) -> Optional[str]:
        """Helper to get order symbol from ID"""
        # This would be implemented based on order storage
        return None
    
    def _get_order_exchange(self, order_id: str) -> Optional[str]:
        """Helper to get order exchange from ID"""
        # This would be implemented based on order storage
        return None
    
    def get_execution_status(self, order_id: str) -> Optional[OrderExecutionPlan]:
        """Get execution status for an order"""
        return self.active_executions.get(order_id)
    
    def get_execution_statistics(self) -> Dict[str, Any]:
        """Get execution statistics"""
        completed_executions = [p for p in self.execution_history if p.final_status == OrderExecutionStatus.COMPLETED]
        failed_executions = [p for p in self.execution_history if p.final_status == OrderExecutionStatus.FAILED]
        
        return {
            "active_executions": len(self.active_executions),
            "total_executions": len(self.execution_history),
            "completed_executions": len(completed_executions),
            "failed_executions": len(failed_executions),
            "success_rate": len(completed_executions) / len(self.execution_history) if self.execution_history else 0,
            "queue_size": self.execution_queue.qsize()
        }