"""
AITHORIX Trade Executor
Low-level trade execution and reconciliation

This module handles:
- Direct exchange trade execution
- Trade confirmation and reconciliation
- Execution quality analysis
- Trade reporting and analytics
- Error recovery and retry logic
"""

import asyncio
import logging
from typing import Dict, List, Optional, Any, Tuple
from datetime import datetime, timedelta
from decimal import Decimal
from dataclasses import dataclass, field
from enum import Enum
import uuid
from collections import deque

from core.engine.order_manager import Order, OrderType, OrderStatus
from exchanges.manager import ExchangeManager
from utils.helpers import get_timestamp, synchronized
from monitoring.metrics import MetricsCollector


class ExecutionQuality(Enum):
    """Trade execution quality ratings"""
    EXCELLENT = "excellent"  # < 0.1% slippage
    GOOD = "good"           # 0.1-0.3% slippage
    FAIR = "fair"           # 0.3-0.5% slippage
    POOR = "poor"           # 0.5-1% slippage
    TERRIBLE = "terrible"   # > 1% slippage


@dataclass
class TradeExecution:
    """Individual trade execution details"""
    execution_id: str
    order_id: str
    exchange_order_id: str
    
    # Trade details
    symbol: str
    exchange: str
    side: str
    order_type: OrderType
    
    # Quantities and prices
    requested_quantity: Decimal
    executed_quantity: Decimal
    remaining_quantity: Decimal
    requested_price: Optional[Decimal]
    executed_price: Decimal
    average_price: Decimal
    
    # Execution details
    execution_time: datetime
    latency_ms: int
    fees: Decimal
    fee_currency: str
    
    # Quality metrics
    slippage: Decimal
    execution_quality: ExecutionQuality
    
    # Status
    status: OrderStatus
    is_partial: bool
    error_message: Optional[str] = None
    
    # Metadata
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class TradeReport:
    """Comprehensive trade execution report"""
    report_id: str
    order_id: str
    created_at: datetime
    
    # Execution summary
    total_requested: Decimal
    total_executed: Decimal
    fill_rate: Decimal
    
    # Price analysis
    average_price: Decimal
    best_price: Decimal
    worst_price: Decimal
    price_improvement: Decimal
    
    # Cost analysis
    total_fees: Decimal
    effective_price: Decimal  # Including fees
    total_slippage: Decimal
    
    # Execution details
    execution_count: int
    executions: List[TradeExecution] = field(default_factory=list)
    
    # Quality assessment
    overall_quality: ExecutionQuality
    execution_time_ms: int
    
    # Status
    is_complete: bool
    success_rate: float


class TradeExecutor:
    """
    Low-level trade execution engine
    
    Handles direct interaction with exchange APIs for trade execution,
    confirmation, and reconciliation.
    """
    
    def __init__(
        self,
        exchange_manager: ExchangeManager,
        config: Dict[str, Any]
    ):
        self.exchange_manager = exchange_manager
        self.config = config
        self.logger = logging.getLogger("AITHORIX.TradeExecutor")
        
        # Execution tracking
        self.active_orders: Dict[str, Order] = {}
        self.execution_history: deque = deque(maxlen=10000)
        self.trade_reports: Dict[str, TradeReport] = {}
        
        # Performance tracking
        self.execution_stats: Dict[str, Dict[str, Any]] = {
            "binance": self._init_exchange_stats(),
            "hyperliquid": self._init_exchange_stats(),
            "mexc": self._init_exchange_stats(),
            "bybit": self._init_exchange_stats(),
            "okx": self._init_exchange_stats()
        }
        
        # Configuration
        self.max_retries = config.get("max_retries", 3)
        self.retry_delay = config.get("retry_delay", 1.0)
        self.slippage_threshold = Decimal(str(config.get("slippage_threshold", "0.005")))  # 0.5%
        self.enable_price_improvement = config.get("enable_price_improvement", True)
        self.execution_timeout = config.get("execution_timeout", 30)  # seconds
        
        # Rate limiting
        self.rate_limiters: Dict[str, asyncio.Semaphore] = {
            "binance": asyncio.Semaphore(20),      # 20 orders/second
            "hyperliquid": asyncio.Semaphore(10),  # 10 orders/second
            "mexc": asyncio.Semaphore(5),          # 5 orders/second
            "bybit": asyncio.Semaphore(10),        # 10 orders/second
            "okx": asyncio.Semaphore(5)            # 5 orders/second
        }
        
        # Metrics
        self.metrics_collector = MetricsCollector()
        
        # State
        self._lock = asyncio.Lock()
        self.is_running = False
    
    async def start(self) -> None:
        """Start the trade executor"""
        self.logger.info("Starting Trade Executor...")
        self.is_running = True
        
        # Start monitoring tasks
        asyncio.create_task(self._order_monitoring_loop())
        asyncio.create_task(self._reconciliation_loop())
        asyncio.create_task(self._metrics_reporting_loop())
    
    async def stop(self) -> None:
        """Stop the trade executor"""
        self.logger.info("Stopping Trade Executor...")
        self.is_running = False
        
        # Cancel any pending orders
        await self._cancel_all_pending_orders()
    
    @synchronized
    async def execute_order(self, order: Order) -> TradeReport:
        """Execute an order on the exchange"""
        start_time = datetime.utcnow()
        
        # Create trade report
        report = TradeReport(
            report_id=f"RPT_{get_timestamp()}",
            order_id=order.order_id,
            created_at=start_time,
            total_requested=order.quantity,
            total_executed=Decimal("0"),
            fill_rate=Decimal("0"),
            average_price=Decimal("0"),
            best_price=Decimal("999999999"),
            worst_price=Decimal("0"),
            price_improvement=Decimal("0"),
            total_fees=Decimal("0"),
            effective_price=Decimal("0"),
            total_slippage=Decimal("0"),
            execution_count=0,
            overall_quality=ExecutionQuality.POOR,
            execution_time_ms=0,
            is_complete=False,
            success_rate=0.0
        )
        
        # Store order and report
        self.active_orders[order.order_id] = order
        self.trade_reports[order.order_id] = report
        
        try:
            # Execute based on order type
            if order.order_type == OrderType.MARKET:
                execution = await self._execute_market_order(order)
            elif order.order_type == OrderType.LIMIT:
                execution = await self._execute_limit_order(order)
            elif order.order_type == OrderType.STOP:
                execution = await self._execute_stop_order(order)
            elif order.order_type == OrderType.STOP_LIMIT:
                execution = await self._execute_stop_limit_order(order)
            else:
                raise ValueError(f"Unsupported order type: {order.order_type}")
            
            # Process execution
            if execution:
                report.executions.append(execution)
                await self._update_report(report, execution)
            
            # Finalize report
            end_time = datetime.utcnow()
            report.execution_time_ms = int((end_time - start_time).total_seconds() * 1000)
            report.is_complete = report.total_executed >= order.quantity * Decimal("0.99")  # 99% filled
            report.success_rate = float(report.total_executed / order.quantity) if order.quantity > 0 else 0
            
            # Assess overall quality
            report.overall_quality = self._assess_execution_quality(report)
            
            # Record metrics
            self._record_execution_metrics(order, report)
            
        except Exception as e:
            self.logger.error(f"Error executing order {order.order_id}: {e}")
            report.is_complete = True
            report.success_rate = 0.0
            
        finally:
            # Clean up
            self.active_orders.pop(order.order_id, None)
            
        return report
    
    async def _execute_market_order(self, order: Order) -> Optional[TradeExecution]:
        """Execute market order"""
        exchange_client = self.exchange_manager.get_client(order.exchange)
        if not exchange_client:
            raise ValueError(f"Exchange {order.exchange} not available")
        
        # Rate limiting
        async with self.rate_limiters[order.exchange]:
            start_time = datetime.utcnow()
            
            try:
                # Place market order
                response = await exchange_client.create_order(
                    symbol=order.symbol,
                    order_type="market",
                    side=order.side.lower(),
                    amount=float(order.quantity),
                    params=order.metadata
                )
                
                # Process response
                execution = await self._process_order_response(order, response, start_time)
                return execution
                
            except Exception as e:
                self.logger.error(f"Market order execution failed: {e}")
                
                # Retry logic
                if order.metadata.get("retry_count", 0) < self.max_retries:
                    order.metadata["retry_count"] = order.metadata.get("retry_count", 0) + 1
                    await asyncio.sleep(self.retry_delay)
                    return await self._execute_market_order(order)
                
                raise
    
    async def _execute_limit_order(self, order: Order) -> Optional[TradeExecution]:
        """Execute limit order"""
        exchange_client = self.exchange_manager.get_client(order.exchange)
        if not exchange_client:
            raise ValueError(f"Exchange {order.exchange} not available")
        
        # Validate price
        if not order.price:
            raise ValueError("Limit order requires price")
        
        # Rate limiting
        async with self.rate_limiters[order.exchange]:
            start_time = datetime.utcnow()
            
            try:
                # Place limit order
                response = await exchange_client.create_order(
                    symbol=order.symbol,
                    order_type="limit",
                    side=order.side.lower(),
                    amount=float(order.quantity),
                    price=float(order.price),
                    params=order.metadata
                )
                
                # For limit orders, we need to monitor fills
                exchange_order_id = response.get("id")
                if exchange_order_id:
                    # Store for monitoring
                    order.metadata["exchange_order_id"] = exchange_order_id
                    
                    # Wait for initial fill or timeout
                    execution = await self._wait_for_fill(
                        order,
                        exchange_order_id,
                        start_time,
                        timeout=self.execution_timeout
                    )
                    
                    return execution
                
            except Exception as e:
                self.logger.error(f"Limit order execution failed: {e}")
                raise
    
    async def _execute_stop_order(self, order: Order) -> Optional[TradeExecution]:
        """Execute stop order"""
        exchange_client = self.exchange_manager.get_client(order.exchange)
        if not exchange_client:
            raise ValueError(f"Exchange {order.exchange} not available")
        
        # Check if exchange supports stop orders
        if not exchange_client.has.get("createStopOrder"):
            # Fallback to manual stop monitoring
            return await self._execute_manual_stop(order)
        
        # Rate limiting
        async with self.rate_limiters[order.exchange]:
            start_time = datetime.utcnow()
            
            try:
                # Place stop order
                response = await exchange_client.create_order(
                    symbol=order.symbol,
                    order_type="stop",
                    side=order.side.lower(),
                    amount=float(order.quantity),
                    stopPrice=float(order.stop_price),
                    params=order.metadata
                )
                
                # Process response
                execution = await self._process_order_response(order, response, start_time)
                return execution
                
            except Exception as e:
                self.logger.error(f"Stop order execution failed: {e}")
                raise
    
    async def _execute_stop_limit_order(self, order: Order) -> Optional[TradeExecution]:
        """Execute stop-limit order"""
        exchange_client = self.exchange_manager.get_client(order.exchange)
        if not exchange_client:
            raise ValueError(f"Exchange {order.exchange} not available")
        
        # Validate prices
        if not order.stop_price or not order.price:
            raise ValueError("Stop-limit order requires both stop price and limit price")
        
        # Check if exchange supports stop-limit orders
        if not exchange_client.has.get("createStopLimitOrder"):
            # Fallback to manual stop-limit monitoring
            return await self._execute_manual_stop_limit(order)
        
        # Rate limiting
        async with self.rate_limiters[order.exchange]:
            start_time = datetime.utcnow()
            
            try:
                # Place stop-limit order
                response = await exchange_client.create_order(
                    symbol=order.symbol,
                    order_type="stop_limit",
                    side=order.side.lower(),
                    amount=float(order.quantity),
                    price=float(order.price),
                    stopPrice=float(order.stop_price),
                    params=order.metadata
                )
                
                # Process response
                execution = await self._process_order_response(order, response, start_time)
                return execution
                
            except Exception as e:
                self.logger.error(f"Stop-limit order execution failed: {e}")
                raise
    
    async def _execute_manual_stop(self, order: Order) -> Optional[TradeExecution]:
        """Execute stop order manually by monitoring price"""
        # Monitor price until stop is triggered
        while True:
            ticker = await self.exchange_manager.get_ticker(order.symbol, order.exchange)
            if not ticker:
                await asyncio.sleep(0.5)
                continue
            
            current_price = Decimal(str(ticker["last"]))
            
            # Check if stop is triggered
            triggered = False
            if order.side == "SELL" and current_price <= order.stop_price:
                triggered = True
            elif order.side == "BUY" and current_price >= order.stop_price:
                triggered = True
            
            if triggered:
                # Convert to market order and execute
                order.order_type = OrderType.MARKET
                return await self._execute_market_order(order)
            
            await asyncio.sleep(0.5)
    
    async def _execute_manual_stop_limit(self, order: Order) -> Optional[TradeExecution]:
        """Execute stop-limit order manually"""
        # Monitor price until stop is triggered
        while True:
            ticker = await self.exchange_manager.get_ticker(order.symbol, order.exchange)
            if not ticker:
                await asyncio.sleep(0.5)
                continue
            
            current_price = Decimal(str(ticker["last"]))
            
            # Check if stop is triggered
            triggered = False
            if order.side == "SELL" and current_price <= order.stop_price:
                triggered = True
            elif order.side == "BUY" and current_price >= order.stop_price:
                triggered = True
            
            if triggered:
                # Convert to limit order and execute
                order.order_type = OrderType.LIMIT
                return await self._execute_limit_order(order)
            
            await asyncio.sleep(0.5)
    
    async def _process_order_response(
        self,
        order: Order,
        response: Dict[str, Any],
        start_time: datetime
    ) -> TradeExecution:
        """Process exchange order response"""
        end_time = datetime.utcnow()
        latency_ms = int((end_time - start_time).total_seconds() * 1000)
        
        # Extract execution details
        exchange_order_id = response.get("id", "")
        status = self._map_order_status(response.get("status", ""))
        executed_quantity = Decimal(str(response.get("filled", 0)))
        average_price = Decimal(str(response.get("average", 0)))
        fees = Decimal(str(response.get("fee", {}).get("cost", 0)))
        fee_currency = response.get("fee", {}).get("currency", "")
        
        # Calculate remaining
        remaining_quantity = order.quantity - executed_quantity
        
        # Calculate slippage
        slippage = Decimal("0")
        if order.price and average_price > 0:
            if order.side == "BUY":
                slippage = (average_price - order.price) / order.price
            else:
                slippage = (order.price - average_price) / order.price
        
        # Assess execution quality
        execution_quality = self._assess_slippage(slippage)
        
        # Create execution record
        execution = TradeExecution(
            execution_id=f"EXEC_{get_timestamp()}",
            order_id=order.order_id,
            exchange_order_id=exchange_order_id,
            symbol=order.symbol,
            exchange=order.exchange,
            side=order.side,
            order_type=order.order_type,
            requested_quantity=order.quantity,
            executed_quantity=executed_quantity,
            remaining_quantity=remaining_quantity,
            requested_price=order.price,
            executed_price=average_price,
            average_price=average_price,
            execution_time=end_time,
            latency_ms=latency_ms,
            fees=fees,
            fee_currency=fee_currency,
            slippage=slippage,
            execution_quality=execution_quality,
            status=status,
            is_partial=executed_quantity < order.quantity,
            metadata=response
        )
        
        # Store execution
        self.execution_history.append(execution)
        
        return execution
    
    async def _wait_for_fill(
        self,
        order: Order,
        exchange_order_id: str,
        start_time: datetime,
        timeout: int
    ) -> Optional[TradeExecution]:
        """Wait for limit order to fill"""
        exchange_client = self.exchange_manager.get_client(order.exchange)
        
        while (datetime.utcnow() - start_time).total_seconds() < timeout:
            try:
                # Check order status
                order_info = await exchange_client.fetch_order(
                    exchange_order_id,
                    order.symbol
                )
                
                # Check if filled
                if order_info.get("status") in ["closed", "filled"]:
                    return await self._process_order_response(order, order_info, start_time)
                
                # Check if cancelled
                elif order_info.get("status") == "canceled":
                    return await self._process_order_response(order, order_info, start_time)
                
                # Still open, wait
                await asyncio.sleep(0.5)
                
            except Exception as e:
                self.logger.error(f"Error checking order status: {e}")
                await asyncio.sleep(1)
        
        # Timeout - cancel order
        try:
            await exchange_client.cancel_order(exchange_order_id, order.symbol)
        except:
            pass
        
        return None
    
    async def _update_report(self, report: TradeReport, execution: TradeExecution) -> None:
        """Update trade report with execution"""
        report.execution_count += 1
        report.total_executed += execution.executed_quantity
        report.total_fees += execution.fees
        
        # Update prices
        if execution.average_price > 0:
            if report.average_price == 0:
                report.average_price = execution.average_price
            else:
                # Weighted average
                total_value = (report.average_price * (report.total_executed - execution.executed_quantity) +
                             execution.average_price * execution.executed_quantity)
                report.average_price = total_value / report.total_executed
            
            report.best_price = min(report.best_price, execution.average_price)
            report.worst_price = max(report.worst_price, execution.average_price)
        
        # Update fill rate
        report.fill_rate = report.total_executed / report.total_requested
        
        # Calculate effective price (including fees)
        if report.total_executed > 0:
            report.effective_price = (report.average_price * report.total_executed + report.total_fees) / report.total_executed
        
        # Update slippage
        report.total_slippage += execution.slippage * execution.executed_quantity
    
    def _map_order_status(self, exchange_status: str) -> OrderStatus:
        """Map exchange order status to internal status"""
        status_map = {
            "open": OrderStatus.OPEN,
            "closed": OrderStatus.FILLED,
            "filled": OrderStatus.FILLED,
            "canceled": OrderStatus.CANCELLED,
            "cancelled": OrderStatus.CANCELLED,
            "expired": OrderStatus.EXPIRED,
            "rejected": OrderStatus.REJECTED,
            "partial": OrderStatus.PARTIAL,
            "partially_filled": OrderStatus.PARTIAL
        }
        
        return status_map.get(exchange_status.lower(), OrderStatus.UNKNOWN)
    
    def _assess_slippage(self, slippage: Decimal) -> ExecutionQuality:
        """Assess execution quality based on slippage"""
        slippage_abs = abs(slippage)
        
        if slippage_abs < Decimal("0.001"):  # < 0.1%
            return ExecutionQuality.EXCELLENT
        elif slippage_abs < Decimal("0.003"):  # < 0.3%
            return ExecutionQuality.GOOD
        elif slippage_abs < Decimal("0.005"):  # < 0.5%
            return ExecutionQuality.FAIR
        elif slippage_abs < Decimal("0.01"):   # < 1%
            return ExecutionQuality.POOR
        else:
            return ExecutionQuality.TERRIBLE
    
    def _assess_execution_quality(self, report: TradeReport) -> ExecutionQuality:
        """Assess overall execution quality"""
        if report.total_executed == 0:
            return ExecutionQuality.POOR
        
        # Calculate average slippage
        avg_slippage = report.total_slippage / report.total_executed if report.total_executed > 0 else Decimal("0")
        
        # Factor in fill rate
        quality_score = 1.0
        
        # Slippage impact (60% weight)
        slippage_score = 1.0 - min(float(abs(avg_slippage)) * 100, 1.0)
        quality_score *= (slippage_score * 0.6 + 0.4)
        
        # Fill rate impact (30% weight)
        fill_score = float(report.fill_rate)
        quality_score *= (fill_score * 0.3 + 0.7)
        
        # Speed impact (10% weight)
        speed_score = max(0, 1.0 - (report.execution_time_ms / 1000.0))  # Penalize if > 1 second
        quality_score *= (speed_score * 0.1 + 0.9)
        
        # Map to quality rating
        if quality_score >= 0.95:
            return ExecutionQuality.EXCELLENT
        elif quality_score >= 0.85:
            return ExecutionQuality.GOOD
        elif quality_score >= 0.70:
            return ExecutionQuality.FAIR
        elif quality_score >= 0.50:
            return ExecutionQuality.POOR
        else:
            return ExecutionQuality.TERRIBLE
    
    def _init_exchange_stats(self) -> Dict[str, Any]:
        """Initialize exchange statistics"""
        return {
            "total_orders": 0,
            "successful_orders": 0,
            "failed_orders": 0,
            "total_volume": Decimal("0"),
            "total_fees": Decimal("0"),
            "average_slippage": Decimal("0"),
            "average_latency": 0,
            "quality_distribution": {
                "excellent": 0,
                "good": 0,
                "fair": 0,
                "poor": 0,
                "terrible": 0
            }
        }
    
    def _record_execution_metrics(self, order: Order, report: TradeReport) -> None:
        """Record execution metrics"""
        # Update exchange stats
        stats = self.execution_stats[order.exchange]
        stats["total_orders"] += 1
        
        if report.success_rate > 0.95:
            stats["successful_orders"] += 1
        else:
            stats["failed_orders"] += 1
        
        stats["total_volume"] += report.total_executed
        stats["total_fees"] += report.total_fees
        
        # Update averages
        if stats["total_orders"] > 1:
            stats["average_slippage"] = (
                (stats["average_slippage"] * (stats["total_orders"] - 1) + 
                 abs(report.total_slippage / report.total_executed if report.total_executed > 0 else 0)) /
                stats["total_orders"]
            )
            stats["average_latency"] = (
                (stats["average_latency"] * (stats["total_orders"] - 1) + report.execution_time_ms) /
                stats["total_orders"]
            )
        else:
            stats["average_slippage"] = abs(report.total_slippage / report.total_executed) if report.total_executed > 0 else Decimal("0")
            stats["average_latency"] = report.execution_time_ms
        
        # Update quality distribution
        stats["quality_distribution"][report.overall_quality.value] += 1
        
        # Record to metrics collector
        self.metrics_collector.record_trade_execution({
            "order_id": order.order_id,
            "symbol": order.symbol,
            "exchange": order.exchange,
            "order_type": order.order_type.value,
            "side": order.side,
            "requested_quantity": float(order.quantity),
            "executed_quantity": float(report.total_executed),
            "fill_rate": float(report.fill_rate),
            "average_price": float(report.average_price),
            "total_fees": float(report.total_fees),
            "slippage": float(report.total_slippage / report.total_executed) if report.total_executed > 0 else 0,
            "execution_time_ms": report.execution_time_ms,
            "quality": report.overall_quality.value
        })
    
    async def _order_monitoring_loop(self) -> None:
        """Monitor active orders"""
        while self.is_running:
            try:
                # Check active orders
                for order_id, order in list(self.active_orders.items()):
                    exchange_order_id = order.metadata.get("exchange_order_id")
                    if exchange_order_id:
                        # Check order status
                        await self._check_order_status(order, exchange_order_id)
                
                await asyncio.sleep(1)
                
            except Exception as e:
                self.logger.error(f"Error in order monitoring: {e}")
                await asyncio.sleep(1)
    
    async def _check_order_status(self, order: Order, exchange_order_id: str) -> None:
        """Check status of an order"""
        try:
            exchange_client = self.exchange_manager.get_client(order.exchange)
            if not exchange_client:
                return
            
            # Fetch order info
            order_info = await exchange_client.fetch_order(exchange_order_id, order.symbol)
            
            # Update order status
            status = self._map_order_status(order_info.get("status", ""))
            order.status = status
            
            # Log if status changed
            if status in [OrderStatus.FILLED, OrderStatus.CANCELLED, OrderStatus.REJECTED]:
                self.logger.info(f"Order {order.order_id} status: {status.value}")
                
        except Exception as e:
            self.logger.error(f"Error checking order status: {e}")
    
    async def _reconciliation_loop(self) -> None:
        """Reconcile trades with exchange"""
        while self.is_running:
            try:
                # Reconcile recent trades
                await self._reconcile_recent_trades()
                
                await asyncio.sleep(60)  # Run every minute
                
            except Exception as e:
                self.logger.error(f"Error in reconciliation: {e}")
                await asyncio.sleep(60)
    
    async def _reconcile_recent_trades(self) -> None:
        """Reconcile recent trades with exchange records"""
        # This would fetch trade history from exchanges and compare
        # with internal records to ensure consistency
        pass
    
    async def _metrics_reporting_loop(self) -> None:
        """Report execution metrics"""
        while self.is_running:
            try:
                # Log execution statistics
                for exchange, stats in self.execution_stats.items():
                    if stats["total_orders"] > 0:
                        self.logger.info(
                            f"{exchange} stats - Orders: {stats['total_orders']}, "
                            f"Success: {stats['successful_orders']}, "
                            f"Avg Slippage: {stats['average_slippage']:.3%}, "
                            f"Avg Latency: {stats['average_latency']}ms"
                        )
                
                await asyncio.sleep(300)  # Report every 5 minutes
                
            except Exception as e:
                self.logger.error(f"Error in metrics reporting: {e}")
                await asyncio.sleep(300)
    
    async def _cancel_all_pending_orders(self) -> None:
        """Cancel all pending orders on shutdown"""
        for order_id, order in list(self.active_orders.items()):
            try:
                exchange_order_id = order.metadata.get("exchange_order_id")
                if exchange_order_id:
                    exchange_client = self.exchange_manager.get_client(order.exchange)
                    if exchange_client:
                        await exchange_client.cancel_order(exchange_order_id, order.symbol)
                        self.logger.info(f"Cancelled order {order_id} on shutdown")
            except Exception as e:
                self.logger.error(f"Error cancelling order {order_id}: {e}")
    
    def get_execution_statistics(self) -> Dict[str, Any]:
        """Get execution statistics"""
        total_stats = {
            "total_orders": 0,
            "successful_orders": 0,
            "failed_orders": 0,
            "total_volume": Decimal("0"),
            "total_fees": Decimal("0"),
            "by_exchange": self.execution_stats
        }
        
        # Aggregate stats
        for stats in self.execution_stats.values():
            total_stats["total_orders"] += stats["total_orders"]
            total_stats["successful_orders"] += stats["successful_orders"]
            total_stats["failed_orders"] += stats["failed_orders"]
            total_stats["total_volume"] += stats["total_volume"]
            total_stats["total_fees"] += stats["total_fees"]
        
        return total_stats
    
    def get_recent_executions(self, limit: int = 100) -> List[TradeExecution]:
        """Get recent trade executions"""
        return list(self.execution_history)[-limit:]