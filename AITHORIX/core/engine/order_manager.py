"""
AITHORIX Order Manager
Manages order lifecycle, tracking, and execution

This module handles all order-related operations including:
- Order creation and validation
- Order state management
- Order execution tracking
- Order modification and cancellation
"""

import asyncio
import logging
from typing import Dict, List, Optional, Any, Set, Tuple
from datetime import datetime, timedelta
from decimal import Decimal
from enum import Enum
from dataclasses import dataclass, field
import uuid
from collections import defaultdict
import json

from utils.helpers import get_timestamp, synchronized
from monitoring.metrics import MetricsCollector


class OrderStatus(Enum):
    """Order status enumeration"""
    PENDING = "pending"
    SUBMITTED = "submitted"
    PARTIAL = "partial"
    FILLED = "filled"
    CANCELLED = "cancelled"
    REJECTED = "rejected"
    EXPIRED = "expired"
    FAILED = "failed"


class OrderType(Enum):
    """Order type enumeration"""
    MARKET = "market"
    LIMIT = "limit"
    STOP = "stop"
    STOP_LIMIT = "stop_limit"
    TRAILING_STOP = "trailing_stop"
    ICEBERG = "iceberg"


class OrderSide(Enum):
    """Order side enumeration"""
    BUY = "buy"
    SELL = "sell"


class TimeInForce(Enum):
    """Time in force enumeration"""
    GTC = "gtc"  # Good Till Cancelled
    IOC = "ioc"  # Immediate or Cancel
    FOK = "fok"  # Fill or Kill
    GTD = "gtd"  # Good Till Date
    DAY = "day"  # Day order


@dataclass
class OrderExecution:
    """Represents a single execution/fill of an order"""
    execution_id: str
    order_id: str
    timestamp: datetime
    quantity: Decimal
    price: Decimal
    commission: Decimal = Decimal("0")
    commission_asset: str = ""
    is_maker: bool = False


@dataclass
class Order:
    """
    Represents a trading order with all necessary information
    
    This class tracks the complete lifecycle of an order from creation
    to final settlement, including all executions and state changes.
    """
    order_id: str
    symbol: str
    exchange: str
    side: str  # Using string for flexibility with OrderSide enum
    order_type: OrderType
    quantity: Decimal
    price: Optional[Decimal] = None
    stop_price: Optional[Decimal] = None
    
    # Status tracking
    status: OrderStatus = OrderStatus.PENDING
    created_at: datetime = field(default_factory=datetime.utcnow)
    submitted_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    
    # Execution details
    filled_quantity: Decimal = Decimal("0")
    average_price: Decimal = Decimal("0")
    executions: List[OrderExecution] = field(default_factory=list)
    
    # Order parameters
    time_in_force: TimeInForce = TimeInForce.GTC
    expire_time: Optional[datetime] = None
    reduce_only: bool = False
    post_only: bool = False
    
    # Iceberg order parameters
    iceberg_quantity: Optional[Decimal] = None
    visible_quantity: Optional[Decimal] = None
    
    # Tracking
    client_order_id: Optional[str] = None
    exchange_order_id: Optional[str] = None
    strategy_id: Optional[str] = None
    signal_id: Optional[str] = None
    parent_order_id: Optional[str] = None
    
    # Risk parameters
    max_slippage: Optional[Decimal] = None
    timeout_seconds: Optional[int] = None
    
    # Metadata
    metadata: Dict[str, Any] = field(default_factory=dict)
    tags: Set[str] = field(default_factory=set)
    
    # Behavioral simulation
    creation_delay: Optional[float] = None  # Simulated human delay
    
    def __post_init__(self):
        """Validate order after initialization"""
        self._validate()
    
    def _validate(self) -> None:
        """Validate order parameters"""
        if self.quantity <= 0:
            raise ValueError("Order quantity must be positive")
        
        if self.order_type in [OrderType.LIMIT, OrderType.STOP_LIMIT] and self.price is None:
            raise ValueError(f"{self.order_type.value} order requires price")
        
        if self.order_type in [OrderType.STOP, OrderType.STOP_LIMIT] and self.stop_price is None:
            raise ValueError(f"{self.order_type.value} order requires stop price")
        
        if self.iceberg_quantity and self.visible_quantity:
            if self.visible_quantity > self.iceberg_quantity:
                raise ValueError("Visible quantity cannot exceed iceberg quantity")
    
    @property
    def remaining_quantity(self) -> Decimal:
        """Calculate remaining quantity to be filled"""
        return self.quantity - self.filled_quantity
    
    @property
    def is_active(self) -> bool:
        """Check if order is still active"""
        return self.status in [OrderStatus.PENDING, OrderStatus.SUBMITTED, OrderStatus.PARTIAL]
    
    @property
    def is_complete(self) -> bool:
        """Check if order is complete"""
        return self.status in [OrderStatus.FILLED, OrderStatus.CANCELLED, 
                               OrderStatus.REJECTED, OrderStatus.EXPIRED, OrderStatus.FAILED]
    
    @property
    def fill_rate(self) -> float:
        """Calculate fill rate as percentage"""
        if self.quantity == 0:
            return 0.0
        return float(self.filled_quantity / self.quantity)
    
    @property
    def total_commission(self) -> Decimal:
        """Calculate total commission paid"""
        return sum(exec.commission for exec in self.executions)
    
    def add_execution(self, execution: OrderExecution) -> None:
        """Add an execution to this order"""
        self.executions.append(execution)
        self.filled_quantity += execution.quantity
        
        # Update average price
        if self.filled_quantity > 0:
            total_value = sum(exec.quantity * exec.price for exec in self.executions)
            self.average_price = total_value / self.filled_quantity
        
        # Update status
        if self.filled_quantity >= self.quantity:
            self.status = OrderStatus.FILLED
            self.completed_at = datetime.utcnow()
        elif self.filled_quantity > 0:
            self.status = OrderStatus.PARTIAL
        
        self.updated_at = datetime.utcnow()
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert order to dictionary representation"""
        return {
            "order_id": self.order_id,
            "symbol": self.symbol,
            "exchange": self.exchange,
            "side": self.side,
            "order_type": self.order_type.value,
            "quantity": str(self.quantity),
            "price": str(self.price) if self.price else None,
            "stop_price": str(self.stop_price) if self.stop_price else None,
            "status": self.status.value,
            "created_at": self.created_at.isoformat(),
            "filled_quantity": str(self.filled_quantity),
            "average_price": str(self.average_price),
            "remaining_quantity": str(self.remaining_quantity),
            "fill_rate": self.fill_rate,
            "time_in_force": self.time_in_force.value,
            "metadata": self.metadata
        }


class OrderManager:
    """
    Manages all orders in the trading system
    
    This class provides centralized order management including:
    - Order lifecycle management
    - Order state tracking
    - Order execution handling
    - Order analytics and reporting
    """
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.logger = logging.getLogger("AITHORIX.OrderManager")
        
        # Order storage
        self.orders: Dict[str, Order] = {}
        self.orders_by_status: Dict[OrderStatus, Set[str]] = defaultdict(set)
        self.orders_by_symbol: Dict[str, Set[str]] = defaultdict(set)
        self.orders_by_exchange: Dict[str, Set[str]] = defaultdict(set)
        
        # Order tracking
        self.client_to_exchange_map: Dict[str, str] = {}
        self.exchange_to_client_map: Dict[str, str] = {}
        
        # Performance tracking
        self.metrics_collector = MetricsCollector()
        self.order_latencies: List[float] = []
        
        # Configuration
        self.max_open_orders = config.get("max_open_orders", 100)
        self.order_timeout = config.get("order_timeout", 300)  # 5 minutes
        self.enable_iceberg = config.get("enable_iceberg", True)
        self.max_retry_attempts = config.get("max_retry_attempts", 3)
        
        # State
        self._lock = asyncio.Lock()
        self.is_initialized = False
    
    async def initialize(self) -> None:
        """Initialize the order manager"""
        self.logger.info("Initializing Order Manager...")
        
        # Load any persisted orders
        await self._load_persisted_orders()
        
        # Start background tasks
        asyncio.create_task(self._order_timeout_monitor())
        asyncio.create_task(self._order_cleanup_task())
        
        self.is_initialized = True
        self.logger.info("Order Manager initialized")
    
    @synchronized
    async def add_order(self, order: Order) -> None:
        """Add a new order to the manager"""
        if order.order_id in self.orders:
            raise ValueError(f"Order {order.order_id} already exists")
        
        # Check order limits
        active_orders = self.get_active_order_count()
        if active_orders >= self.max_open_orders:
            raise RuntimeError(f"Maximum open orders ({self.max_open_orders}) reached")
        
        # Store order
        self.orders[order.order_id] = order
        self.orders_by_status[order.status].add(order.order_id)
        self.orders_by_symbol[order.symbol].add(order.order_id)
        self.orders_by_exchange[order.exchange].add(order.order_id)
        
        # Generate client order ID if not provided
        if not order.client_order_id:
            order.client_order_id = self._generate_client_order_id(order)
        
        # Log order creation
        self.logger.info(f"Added order: {order.order_id} - {order.symbol} "
                        f"{order.side} {order.quantity} @ {order.price or 'MARKET'}")
        
        # Record metrics
        self.metrics_collector.record_order_created(order)
    
    @synchronized
    async def update_order_status(
        self, 
        order_id: str, 
        new_status: OrderStatus,
        exchange_order_id: Optional[str] = None
    ) -> None:
        """Update order status"""
        order = self.orders.get(order_id)
        if not order:
            raise ValueError(f"Order {order_id} not found")
        
        old_status = order.status
        
        # Remove from old status set
        self.orders_by_status[old_status].discard(order_id)
        
        # Update order
        order.status = new_status
        order.updated_at = datetime.utcnow()
        
        if new_status == OrderStatus.SUBMITTED:
            order.submitted_at = datetime.utcnow()
            if exchange_order_id:
                order.exchange_order_id = exchange_order_id
                self._update_order_mappings(order)
        
        if order.is_complete:
            order.completed_at = datetime.utcnow()
        
        # Add to new status set
        self.orders_by_status[new_status].add(order_id)
        
        # Log status change
        self.logger.info(f"Order {order_id} status changed: {old_status.value} -> {new_status.value}")
        
        # Record metrics
        self.metrics_collector.record_order_status_change(order, old_status, new_status)
    
    @synchronized
    async def update_order_from_execution(self, execution_report: Any) -> None:
        """Update order from execution report"""
        order_id = execution_report.order_id
        order = self.orders.get(order_id)
        
        if not order:
            # Try to find by exchange order ID
            client_order_id = self.exchange_to_client_map.get(execution_report.exchange_order_id)
            if client_order_id:
                order = self.orders.get(client_order_id)
        
        if not order:
            self.logger.error(f"Order not found for execution: {execution_report}")
            return
        
        # Create execution record
        execution = OrderExecution(
            execution_id=execution_report.execution_id,
            order_id=order.order_id,
            timestamp=execution_report.timestamp,
            quantity=execution_report.quantity,
            price=execution_report.price,
            commission=execution_report.commission,
            commission_asset=execution_report.commission_asset,
            is_maker=execution_report.is_maker
        )
        
        # Add execution to order
        order.add_execution(execution)
        
        # Calculate latency
        if order.submitted_at:
            latency = (execution.timestamp - order.submitted_at).total_seconds()
            self.order_latencies.append(latency)
            if len(self.order_latencies) > 1000:
                self.order_latencies = self.order_latencies[-500:]
        
        # Log execution
        self.logger.info(f"Order {order.order_id} execution: {execution.quantity} @ {execution.price}")
        
        # Record metrics
        self.metrics_collector.record_order_execution(order, execution)
    
    @synchronized
    async def cancel_order(self, order_id: str, reason: str = "") -> None:
        """Cancel an order"""
        order = self.orders.get(order_id)
        if not order:
            raise ValueError(f"Order {order_id} not found")
        
        if not order.is_active:
            raise ValueError(f"Order {order_id} is not active (status: {order.status.value})")
        
        # Update status
        await self.update_order_status(order_id, OrderStatus.CANCELLED)
        
        # Add cancellation reason to metadata
        order.metadata["cancellation_reason"] = reason
        order.metadata["cancelled_at"] = datetime.utcnow().isoformat()
        
        self.logger.info(f"Order {order_id} cancelled: {reason}")
    
    @synchronized
    async def modify_order(
        self,
        order_id: str,
        new_quantity: Optional[Decimal] = None,
        new_price: Optional[Decimal] = None
    ) -> None:
        """Modify an existing order"""
        order = self.orders.get(order_id)
        if not order:
            raise ValueError(f"Order {order_id} not found")
        
        if not order.is_active:
            raise ValueError(f"Order {order_id} is not active")
        
        # Store original values
        original_quantity = order.quantity
        original_price = order.price
        
        # Update order
        if new_quantity is not None:
            if new_quantity <= order.filled_quantity:
                raise ValueError("New quantity must be greater than filled quantity")
            order.quantity = new_quantity
        
        if new_price is not None:
            if order.order_type not in [OrderType.LIMIT, OrderType.STOP_LIMIT]:
                raise ValueError("Cannot modify price for non-limit orders")
            order.price = new_price
        
        order.updated_at = datetime.utcnow()
        
        # Log modification
        self.logger.info(f"Order {order_id} modified: "
                        f"quantity {original_quantity} -> {order.quantity}, "
                        f"price {original_price} -> {order.price}")
        
        # Record metrics
        self.metrics_collector.record_order_modification(order)
    
    def get_order(self, order_id: str) -> Optional[Order]:
        """Get order by ID"""
        return self.orders.get(order_id)
    
    def get_order_by_exchange_id(self, exchange_order_id: str) -> Optional[Order]:
        """Get order by exchange order ID"""
        client_order_id = self.exchange_to_client_map.get(exchange_order_id)
        if client_order_id:
            return self.orders.get(client_order_id)
        return None
    
    def get_orders_by_status(self, status: OrderStatus) -> List[Order]:
        """Get all orders with specific status"""
        order_ids = self.orders_by_status.get(status, set())
        return [self.orders[oid] for oid in order_ids if oid in self.orders]
    
    def get_orders_by_symbol(self, symbol: str) -> List[Order]:
        """Get all orders for a symbol"""
        order_ids = self.orders_by_symbol.get(symbol, set())
        return [self.orders[oid] for oid in order_ids if oid in self.orders]
    
    def get_orders_by_exchange(self, exchange: str) -> List[Order]:
        """Get all orders for an exchange"""
        order_ids = self.orders_by_exchange.get(exchange, set())
        return [self.orders[oid] for oid in order_ids if oid in self.orders]
    
    def get_active_orders(self) -> List[Order]:
        """Get all active orders"""
        active_orders = []
        for status in [OrderStatus.PENDING, OrderStatus.SUBMITTED, OrderStatus.PARTIAL]:
            active_orders.extend(self.get_orders_by_status(status))
        return active_orders
    
    def get_open_orders(self) -> List[Order]:
        """Get open orders (submitted and partial)"""
        open_orders = []
        for status in [OrderStatus.SUBMITTED, OrderStatus.PARTIAL]:
            open_orders.extend(self.get_orders_by_status(status))
        return open_orders
    
    def get_pending_orders(self) -> List[Order]:
        """Get pending orders"""
        return self.get_orders_by_status(OrderStatus.PENDING)
    
    def get_filled_orders(self) -> List[Order]:
        """Get filled orders"""
        return self.get_orders_by_status(OrderStatus.FILLED)
    
    def get_expired_orders(self) -> List[Order]:
        """Get expired orders"""
        return self.get_orders_by_status(OrderStatus.EXPIRED)
    
    def get_active_order_count(self) -> int:
        """Get count of active orders"""
        return len(self.get_active_orders())
    
    def get_order_fill_stats(self) -> Dict[str, Any]:
        """Get order fill statistics"""
        filled_orders = self.get_filled_orders()
        
        if not filled_orders:
            return {
                "total_filled": 0,
                "average_fill_time": 0,
                "average_slippage": 0,
                "fill_rate": 0
            }
        
        fill_times = []
        slippages = []
        
        for order in filled_orders:
            if order.submitted_at and order.completed_at:
                fill_time = (order.completed_at - order.submitted_at).total_seconds()
                fill_times.append(fill_time)
            
            if order.price and order.average_price:
                slippage = abs(float(order.average_price - order.price) / order.price)
                slippages.append(slippage)
        
        return {
            "total_filled": len(filled_orders),
            "average_fill_time": sum(fill_times) / len(fill_times) if fill_times else 0,
            "average_slippage": sum(slippages) / len(slippages) if slippages else 0,
            "fill_rate": len(filled_orders) / len(self.orders) if self.orders else 0
        }
    
    async def update_order_statuses(self) -> None:
        """Update order statuses from exchanges"""
        # This would be called periodically to sync with exchange
        active_orders = self.get_active_orders()
        
        for order in active_orders:
            try:
                # Would query exchange for order status
                # For now, just check for timeouts
                if order.timeout_seconds and order.submitted_at:
                    elapsed = (datetime.utcnow() - order.submitted_at).total_seconds()
                    if elapsed > order.timeout_seconds:
                        await self.update_order_status(order.order_id, OrderStatus.EXPIRED)
                
            except Exception as e:
                self.logger.error(f"Error updating order {order.order_id}: {e}")
    
    def _generate_client_order_id(self, order: Order) -> str:
        """Generate unique client order ID"""
        # Include strategy and timestamp for tracking
        timestamp = datetime.utcnow().strftime("%Y%m%d%H%M%S")
        strategy_prefix = order.strategy_id[:8] if order.strategy_id else "MANUAL"
        return f"AITH-{strategy_prefix}-{timestamp}-{str(uuid.uuid4())[:8]}"
    
    def _update_order_mappings(self, order: Order) -> None:
        """Update order ID mappings"""
        if order.client_order_id and order.exchange_order_id:
            self.client_to_exchange_map[order.client_order_id] = order.exchange_order_id
            self.exchange_to_client_map[order.exchange_order_id] = order.client_order_id
    
    async def _order_timeout_monitor(self) -> None:
        """Monitor orders for timeout"""
        while True:
            try:
                current_time = datetime.utcnow()
                
                for order in self.get_active_orders():
                    # Check explicit timeout
                    if order.timeout_seconds and order.created_at:
                        elapsed = (current_time - order.created_at).total_seconds()
                        if elapsed > order.timeout_seconds:
                            await self.update_order_status(order.order_id, OrderStatus.EXPIRED)
                    
                    # Check time in force
                    if order.time_in_force == TimeInForce.DAY:
                        if order.created_at.date() < current_time.date():
                            await self.update_order_status(order.order_id, OrderStatus.EXPIRED)
                    
                    elif order.time_in_force == TimeInForce.GTD and order.expire_time:
                        if current_time > order.expire_time:
                            await self.update_order_status(order.order_id, OrderStatus.EXPIRED)
                
                await asyncio.sleep(10)  # Check every 10 seconds
                
            except Exception as e:
                self.logger.error(f"Error in timeout monitor: {e}")
                await asyncio.sleep(60)
    
    async def _order_cleanup_task(self) -> None:
        """Clean up old completed orders"""
        while True:
            try:
                # Keep completed orders for 24 hours
                cutoff_time = datetime.utcnow() - timedelta(hours=24)
                
                orders_to_remove = []
                for order_id, order in self.orders.items():
                    if order.is_complete and order.completed_at:
                        if order.completed_at < cutoff_time:
                            orders_to_remove.append(order_id)
                
                # Remove old orders
                async with self._lock:
                    for order_id in orders_to_remove:
                        order = self.orders[order_id]
                        
                        # Remove from indices
                        self.orders_by_status[order.status].discard(order_id)
                        self.orders_by_symbol[order.symbol].discard(order_id)
                        self.orders_by_exchange[order.exchange].discard(order_id)
                        
                        # Remove from main storage
                        del self.orders[order_id]
                
                if orders_to_remove:
                    self.logger.info(f"Cleaned up {len(orders_to_remove)} old orders")
                
                await asyncio.sleep(3600)  # Run hourly
                
            except Exception as e:
                self.logger.error(f"Error in cleanup task: {e}")
                await asyncio.sleep(3600)
    
    async def _load_persisted_orders(self) -> None:
        """Load persisted orders from storage"""
        # Implementation would load from database
        # For now, just log
        self.logger.info("Loading persisted orders...")
    
    def get_performance_metrics(self) -> Dict[str, Any]:
        """Get order manager performance metrics"""
        return {
            "total_orders": len(self.orders),
            "active_orders": self.get_active_order_count(),
            "fill_stats": self.get_order_fill_stats(),
            "average_latency": sum(self.order_latencies) / len(self.order_latencies) if self.order_latencies else 0,
            "orders_by_status": {
                status.value: len(self.orders_by_status[status])
                for status in OrderStatus
            }
        }