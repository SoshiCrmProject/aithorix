"""
AITHORIX Position Manager
Manages trading positions and P&L calculation

This module handles:
- Position tracking across all exchanges
- Real-time P&L calculation
- Position risk metrics
- Position lifecycle management
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
import numpy as np

from core.engine.order_manager import Order, OrderExecution, OrderStatus
from utils.helpers import get_timestamp, synchronized
from monitoring.metrics import MetricsCollector


class PositionSide(Enum):
    """Position side enumeration"""
    LONG = "long"
    SHORT = "short"


class PositionStatus(Enum):
    """Position status enumeration"""
    OPEN = "open"
    CLOSING = "closing"
    CLOSED = "closed"


@dataclass
class Trade:
    """Represents a single trade that affects a position"""
    trade_id: str
    position_id: str
    order_id: str
    execution_id: str
    timestamp: datetime
    side: str  # BUY or SELL
    quantity: Decimal
    price: Decimal
    commission: Decimal = Decimal("0")
    commission_asset: str = ""
    realized_pnl: Decimal = Decimal("0")


@dataclass
class Position:
    """
    Represents a trading position
    
    Tracks the complete lifecycle of a position including:
    - Entry and exit trades
    - Realized and unrealized P&L
    - Risk metrics
    - Performance analytics
    """
    position_id: str
    symbol: str
    exchange: str
    side: PositionSide
    
    # Position sizing
    quantity: Decimal
    entry_price: Decimal
    current_price: Decimal = Decimal("0")
    
    # Status tracking
    status: PositionStatus = PositionStatus.OPEN
    opened_at: datetime = field(default_factory=datetime.utcnow)
    closed_at: Optional[datetime] = None
    last_updated: datetime = field(default_factory=datetime.utcnow)
    
    # P&L tracking
    realized_pnl: Decimal = Decimal("0")
    unrealized_pnl: Decimal = Decimal("0")
    total_commission: Decimal = Decimal("0")
    
    # Trade tracking
    trades: List[Trade] = field(default_factory=list)
    entry_trades: List[Trade] = field(default_factory=list)
    exit_trades: List[Trade] = field(default_factory=list)
    
    # Risk parameters
    stop_loss: Optional[Decimal] = None
    take_profit: Optional[Decimal] = None
    trailing_stop_distance: Optional[Decimal] = None
    max_position_value: Optional[Decimal] = None
    
    # Leverage
    leverage: Decimal = Decimal("1")
    margin_used: Decimal = Decimal("0")
    liquidation_price: Optional[Decimal] = None
    
    # Strategy tracking
    strategy_id: Optional[str] = None
    signal_id: Optional[str] = None
    
    # Metadata
    metadata: Dict[str, Any] = field(default_factory=dict)
    tags: Set[str] = field(default_factory=set)
    
    # Performance metrics
    max_unrealized_profit: Decimal = Decimal("0")
    max_unrealized_loss: Decimal = Decimal("0")
    time_in_profit: timedelta = timedelta()
    time_in_loss: timedelta = timedelta()
    
    @property
    def is_open(self) -> bool:
        """Check if position is open"""
        return self.status == PositionStatus.OPEN
    
    @property
    def is_closed(self) -> bool:
        """Check if position is closed"""
        return self.status == PositionStatus.CLOSED
    
    @property
    def position_value(self) -> Decimal:
        """Calculate current position value"""
        return self.quantity * self.current_price
    
    @property
    def entry_value(self) -> Decimal:
        """Calculate entry value"""
        return self.quantity * self.entry_price
    
    @property
    def pnl_percentage(self) -> Decimal:
        """Calculate P&L percentage"""
        if self.entry_value == 0:
            return Decimal("0")
        return ((self.position_value - self.entry_value) / self.entry_value) * Decimal("100")
    
    @property
    def net_pnl(self) -> Decimal:
        """Calculate net P&L after commissions"""
        return self.realized_pnl + self.unrealized_pnl - self.total_commission
    
    @property
    def risk_reward_ratio(self) -> Optional[Decimal]:
        """Calculate risk/reward ratio if stops are set"""
        if not self.stop_loss or not self.take_profit:
            return None
        
        risk = abs(self.entry_price - self.stop_loss)
        reward = abs(self.take_profit - self.entry_price)
        
        if risk == 0:
            return None
        
        return reward / risk
    
    @property
    def time_held(self) -> timedelta:
        """Calculate time position has been held"""
        end_time = self.closed_at if self.closed_at else datetime.utcnow()
        return end_time - self.opened_at
    
    def update_price(self, new_price: Decimal) -> None:
        """Update current price and unrealized P&L"""
        self.current_price = new_price
        self.last_updated = datetime.utcnow()
        
        # Calculate unrealized P&L
        if self.side == PositionSide.LONG:
            self.unrealized_pnl = (new_price - self.entry_price) * self.quantity
        else:  # SHORT
            self.unrealized_pnl = (self.entry_price - new_price) * self.quantity
        
        # Update max profit/loss
        if self.unrealized_pnl > self.max_unrealized_profit:
            self.max_unrealized_profit = self.unrealized_pnl
        if self.unrealized_pnl < self.max_unrealized_loss:
            self.max_unrealized_loss = self.unrealized_pnl
        
        # Update trailing stop if applicable
        if self.trailing_stop_distance:
            self._update_trailing_stop(new_price)
    
    def _update_trailing_stop(self, current_price: Decimal) -> None:
        """Update trailing stop based on current price"""
        if self.side == PositionSide.LONG:
            new_stop = current_price - self.trailing_stop_distance
            if not self.stop_loss or new_stop > self.stop_loss:
                self.stop_loss = new_stop
        else:  # SHORT
            new_stop = current_price + self.trailing_stop_distance
            if not self.stop_loss or new_stop < self.stop_loss:
                self.stop_loss = new_stop
    
    def add_trade(self, trade: Trade) -> None:
        """Add a trade to this position"""
        self.trades.append(trade)
        self.total_commission += trade.commission
        
        if trade.side == "BUY" and self.side == PositionSide.LONG:
            self.entry_trades.append(trade)
        elif trade.side == "SELL" and self.side == PositionSide.SHORT:
            self.entry_trades.append(trade)
        else:
            self.exit_trades.append(trade)
            self.realized_pnl += trade.realized_pnl
        
        self.last_updated = datetime.utcnow()
    
    def calculate_liquidation_price(self, maintenance_margin_rate: Decimal) -> Optional[Decimal]:
        """Calculate liquidation price for leveraged positions"""
        if self.leverage <= 1:
            return None
        
        if self.side == PositionSide.LONG:
            # Long liquidation: price falls below this level
            self.liquidation_price = self.entry_price * (
                Decimal("1") - (Decimal("1") / self.leverage) + maintenance_margin_rate
            )
        else:  # SHORT
            # Short liquidation: price rises above this level
            self.liquidation_price = self.entry_price * (
                Decimal("1") + (Decimal("1") / self.leverage) - maintenance_margin_rate
            )
        
        return self.liquidation_price
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert position to dictionary representation"""
        return {
            "position_id": self.position_id,
            "symbol": self.symbol,
            "exchange": self.exchange,
            "side": self.side.value,
            "quantity": str(self.quantity),
            "entry_price": str(self.entry_price),
            "current_price": str(self.current_price),
            "status": self.status.value,
            "opened_at": self.opened_at.isoformat(),
            "closed_at": self.closed_at.isoformat() if self.closed_at else None,
            "realized_pnl": str(self.realized_pnl),
            "unrealized_pnl": str(self.unrealized_pnl),
            "net_pnl": str(self.net_pnl),
            "pnl_percentage": str(self.pnl_percentage),
            "leverage": str(self.leverage),
            "liquidation_price": str(self.liquidation_price) if self.liquidation_price else None,
            "time_held": str(self.time_held),
            "metadata": self.metadata
        }


class PositionManager:
    """
    Manages all trading positions across exchanges
    
    Provides centralized position management including:
    - Position lifecycle management
    - P&L calculation and tracking
    - Risk metrics calculation
    - Position analytics and reporting
    """
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.logger = logging.getLogger("AITHORIX.PositionManager")
        
        # Position storage
        self.positions: Dict[str, Position] = {}
        self.positions_by_status: Dict[PositionStatus, Set[str]] = defaultdict(set)
        self.positions_by_symbol: Dict[str, Set[str]] = defaultdict(set)
        self.positions_by_exchange: Dict[str, Set[str]] = defaultdict(set)
        
        # Performance tracking
        self.metrics_collector = MetricsCollector()
        self.total_realized_pnl: Decimal = Decimal("0")
        self.total_commission_paid: Decimal = Decimal("0")
        
        # Risk parameters
        self.max_position_size = Decimal(str(config.get("max_position_size", "10000")))
        self.max_total_exposure = Decimal(str(config.get("max_total_exposure", "100000")))
        self.max_positions_per_symbol = config.get("max_positions_per_symbol", 3)
        self.default_leverage = Decimal(str(config.get("default_leverage", "1")))
        
        # Maintenance margin rates by exchange
        self.maintenance_margin_rates = {
            "binance": Decimal("0.004"),  # 0.4%
            "hyperliquid": Decimal("0.003"),  # 0.3%
            "mexc": Decimal("0.005"),  # 0.5%
            "bybit": Decimal("0.005"),  # 0.5%
            "okx": Decimal("0.004"),  # 0.4%
        }
        
        # State
        self._lock = asyncio.Lock()
        self.is_initialized = False
        
        # Price update tracking
        self.last_price_updates: Dict[str, datetime] = {}
    
    async def initialize(self) -> None:
        """Initialize the position manager"""
        self.logger.info("Initializing Position Manager...")
        
        # Load any persisted positions
        await self._load_persisted_positions()
        
        # Start background tasks
        asyncio.create_task(self._position_monitoring_task())
        asyncio.create_task(self._pnl_calculation_task())
        
        self.is_initialized = True
        self.logger.info("Position Manager initialized")
    
    @synchronized
    async def create_or_update_position(self, execution_report: Any) -> Position:
        """Create new position or update existing from execution"""
        # Find existing position for this symbol/exchange/side
        existing_position = await self._find_matching_position(
            execution_report.symbol,
            execution_report.exchange,
            execution_report.side
        )
        
        if existing_position and existing_position.is_open:
            # Update existing position
            position = existing_position
            await self._update_position_from_execution(position, execution_report)
        else:
            # Create new position
            position = await self._create_position_from_execution(execution_report)
        
        return position
    
    async def _create_position_from_execution(self, execution_report: Any) -> Position:
        """Create a new position from execution report"""
        # Determine position side
        position_side = PositionSide.LONG if execution_report.side == "BUY" else PositionSide.SHORT
        
        # Create position
        position = Position(
            position_id=str(uuid.uuid4()),
            symbol=execution_report.symbol,
            exchange=execution_report.exchange,
            side=position_side,
            quantity=execution_report.quantity,
            entry_price=execution_report.price,
            current_price=execution_report.price,
            leverage=self.default_leverage,
            strategy_id=execution_report.strategy_id,
            signal_id=execution_report.signal_id
        )
        
        # Calculate margin and liquidation price
        position.margin_used = position.entry_value / position.leverage
        margin_rate = self.maintenance_margin_rates.get(position.exchange, Decimal("0.005"))
        position.calculate_liquidation_price(margin_rate)
        
        # Create initial trade
        trade = Trade(
            trade_id=str(uuid.uuid4()),
            position_id=position.position_id,
            order_id=execution_report.order_id,
            execution_id=execution_report.execution_id,
            timestamp=execution_report.timestamp,
            side=execution_report.side,
            quantity=execution_report.quantity,
            price=execution_report.price,
            commission=execution_report.commission,
            commission_asset=execution_report.commission_asset
        )
        
        position.add_trade(trade)
        
        # Store position
        await self._store_position(position)
        
        # Log position creation
        self.logger.info(f"Created position {position.position_id}: "
                        f"{position.symbol} {position.side.value} "
                        f"{position.quantity} @ {position.entry_price}")
        
        # Record metrics
        self.metrics_collector.record_position_opened(position)
        
        return position
    
    async def _update_position_from_execution(self, position: Position, execution_report: Any) -> None:
        """Update existing position from execution"""
        # Create trade record
        trade = Trade(
            trade_id=str(uuid.uuid4()),
            position_id=position.position_id,
            order_id=execution_report.order_id,
            execution_id=execution_report.execution_id,
            timestamp=execution_report.timestamp,
            side=execution_report.side,
            quantity=execution_report.quantity,
            price=execution_report.price,
            commission=execution_report.commission,
            commission_asset=execution_report.commission_asset
        )
        
        # Check if this is a closing trade
        is_closing = (
            (position.side == PositionSide.LONG and execution_report.side == "SELL") or
            (position.side == PositionSide.SHORT and execution_report.side == "BUY")
        )
        
        if is_closing:
            # Calculate realized P&L for this trade
            if position.side == PositionSide.LONG:
                trade.realized_pnl = (execution_report.price - position.entry_price) * execution_report.quantity
            else:  # SHORT
                trade.realized_pnl = (position.entry_price - execution_report.price) * execution_report.quantity
            
            # Update position quantity
            position.quantity -= execution_report.quantity
            
            # Check if position is fully closed
            if position.quantity <= 0:
                position.status = PositionStatus.CLOSED
                position.closed_at = datetime.utcnow()
                position.unrealized_pnl = Decimal("0")
                
                # Move position to closed status
                self.positions_by_status[PositionStatus.OPEN].discard(position.position_id)
                self.positions_by_status[PositionStatus.CLOSED].add(position.position_id)
                
                self.logger.info(f"Closed position {position.position_id}: "
                               f"Realized P&L: {position.realized_pnl}")
                
                # Record metrics
                self.metrics_collector.record_position_closed(position)
        else:
            # Adding to position
            # Recalculate average entry price
            total_value = position.entry_value + (execution_report.quantity * execution_report.price)
            total_quantity = position.quantity + execution_report.quantity
            position.entry_price = total_value / total_quantity
            position.quantity = total_quantity
            
            # Update margin and liquidation price
            position.margin_used = position.entry_value / position.leverage
            margin_rate = self.maintenance_margin_rates.get(position.exchange, Decimal("0.005"))
            position.calculate_liquidation_price(margin_rate)
        
        # Add trade to position
        position.add_trade(trade)
        
        # Update total realized P&L
        self.total_realized_pnl += trade.realized_pnl
        self.total_commission_paid += trade.commission
    
    @synchronized
    async def update_position_prices(self, price_updates: Dict[str, Decimal]) -> None:
        """Update position prices and calculate unrealized P&L"""
        for symbol, price in price_updates.items():
            position_ids = self.positions_by_symbol.get(symbol, set())
            
            for position_id in position_ids:
                position = self.positions.get(position_id)
                if position and position.is_open:
                    position.update_price(price)
                    
                    # Check stop loss and take profit
                    await self._check_position_stops(position)
            
            self.last_price_updates[symbol] = datetime.utcnow()
    
    async def _check_position_stops(self, position: Position) -> None:
        """Check if position stop loss or take profit is triggered"""
        if position.stop_loss:
            if (position.side == PositionSide.LONG and position.current_price <= position.stop_loss) or \
               (position.side == PositionSide.SHORT and position.current_price >= position.stop_loss):
                self.logger.warning(f"Stop loss triggered for position {position.position_id}")
                position.metadata["stop_triggered"] = "stop_loss"
                position.metadata["stop_trigger_time"] = datetime.utcnow().isoformat()
        
        if position.take_profit:
            if (position.side == PositionSide.LONG and position.current_price >= position.take_profit) or \
               (position.side == PositionSide.SHORT and position.current_price <= position.take_profit):
                self.logger.info(f"Take profit triggered for position {position.position_id}")
                position.metadata["stop_triggered"] = "take_profit"
                position.metadata["stop_trigger_time"] = datetime.utcnow().isoformat()
    
    @synchronized
    async def _store_position(self, position: Position) -> None:
        """Store position in manager"""
        # Check position limits
        symbol_positions = self.positions_by_symbol.get(position.symbol, set())
        open_symbol_positions = [
            p for p_id in symbol_positions 
            if p_id in self.positions and self.positions[p_id].is_open
        ]
        
        if len(open_symbol_positions) >= self.max_positions_per_symbol:
            raise RuntimeError(f"Maximum positions per symbol ({self.max_positions_per_symbol}) reached for {position.symbol}")
        
        # Check total exposure
        total_exposure = self.get_total_exposure()
        if total_exposure + position.position_value > self.max_total_exposure:
            raise RuntimeError(f"Maximum total exposure ({self.max_total_exposure}) would be exceeded")
        
        # Store position
        self.positions[position.position_id] = position
        self.positions_by_status[position.status].add(position.position_id)
        self.positions_by_symbol[position.symbol].add(position.position_id)
        self.positions_by_exchange[position.exchange].add(position.position_id)
    
    async def _find_matching_position(
        self, 
        symbol: str, 
        exchange: str, 
        side: str
    ) -> Optional[Position]:
        """Find matching open position"""
        position_side = PositionSide.LONG if side == "BUY" else PositionSide.SHORT
        
        for position_id in self.positions_by_symbol.get(symbol, set()):
            position = self.positions.get(position_id)
            if position and position.is_open and \
               position.exchange == exchange and \
               position.side == position_side:
                return position
        
        return None
    
    def get_position(self, position_id: str) -> Optional[Position]:
        """Get position by ID"""
        return self.positions.get(position_id)
    
    def get_positions_by_status(self, status: PositionStatus) -> List[Position]:
        """Get all positions with specific status"""
        position_ids = self.positions_by_status.get(status, set())
        return [self.positions[pid] for pid in position_ids if pid in self.positions]
    
    def get_positions_by_symbol(self, symbol: str) -> List[Position]:
        """Get all positions for a symbol"""
        position_ids = self.positions_by_symbol.get(symbol, set())
        return [self.positions[pid] for pid in position_ids if pid in self.positions]
    
    def get_positions_by_exchange(self, exchange: str) -> List[Position]:
        """Get all positions for an exchange"""
        position_ids = self.positions_by_exchange.get(exchange, set())
        return [self.positions[pid] for pid in position_ids if pid in self.positions]
    
    def get_open_positions(self) -> List[Position]:
        """Get all open positions"""
        return self.get_positions_by_status(PositionStatus.OPEN)
    
    def get_closed_positions(self) -> List[Position]:
        """Get all closed positions"""
        return self.get_positions_by_status(PositionStatus.CLOSED)
    
    def get_all_positions(self) -> List[Position]:
        """Get all positions"""
        return list(self.positions.values())
    
    def get_total_exposure(self) -> Decimal:
        """Calculate total exposure across all open positions"""
        total = Decimal("0")
        for position in self.get_open_positions():
            total += position.position_value
        return total
    
    def get_total_position_value(self) -> Decimal:
        """Get total value of all open positions"""
        return self.get_total_exposure()
    
    def get_unrealized_pnl(self) -> Decimal:
        """Get total unrealized P&L"""
        total = Decimal("0")
        for position in self.get_open_positions():
            total += position.unrealized_pnl
        return total
    
    def get_realized_pnl(self) -> Decimal:
        """Get total realized P&L"""
        return self.total_realized_pnl
    
    def get_portfolio_metrics(self) -> Dict[str, Any]:
        """Calculate portfolio-wide metrics"""
        open_positions = self.get_open_positions()
        closed_positions = self.get_closed_positions()
        
        if not open_positions and not closed_positions:
            return {
                "total_positions": 0,
                "open_positions": 0,
                "closed_positions": 0,
                "total_exposure": 0,
                "unrealized_pnl": 0,
                "realized_pnl": 0,
                "total_pnl": 0,
                "win_rate": 0,
                "average_win": 0,
                "average_loss": 0,
                "profit_factor": 0,
                "sharpe_ratio": 0
            }
        
        # Calculate win/loss statistics
        winning_positions = [p for p in closed_positions if p.realized_pnl > 0]
        losing_positions = [p for p in closed_positions if p.realized_pnl < 0]
        
        win_rate = len(winning_positions) / len(closed_positions) if closed_positions else 0
        
        average_win = (
            sum(p.realized_pnl for p in winning_positions) / len(winning_positions)
            if winning_positions else Decimal("0")
        )
        
        average_loss = (
            sum(p.realized_pnl for p in losing_positions) / len(losing_positions)
            if losing_positions else Decimal("0")
        )
        
        # Calculate profit factor
        gross_profit = sum(p.realized_pnl for p in winning_positions)
        gross_loss = abs(sum(p.realized_pnl for p in losing_positions))
        profit_factor = float(gross_profit / gross_loss) if gross_loss > 0 else 0
        
        # Calculate returns for Sharpe ratio
        if closed_positions:
            returns = [float(p.realized_pnl / p.entry_value) for p in closed_positions]
            if len(returns) > 1:
                sharpe_ratio = np.mean(returns) / np.std(returns) * np.sqrt(252) if np.std(returns) > 0 else 0
            else:
                sharpe_ratio = 0
        else:
            sharpe_ratio = 0
        
        return {
            "total_positions": len(self.positions),
            "open_positions": len(open_positions),
            "closed_positions": len(closed_positions),
            "total_exposure": float(self.get_total_exposure()),
            "unrealized_pnl": float(self.get_unrealized_pnl()),
            "realized_pnl": float(self.get_realized_pnl()),
            "total_pnl": float(self.get_realized_pnl() + self.get_unrealized_pnl()),
            "total_commission": float(self.total_commission_paid),
            "win_rate": win_rate,
            "average_win": float(average_win),
            "average_loss": float(average_loss),
            "profit_factor": profit_factor,
            "sharpe_ratio": sharpe_ratio,
            "positions_by_symbol": {
                symbol: len(positions)
                for symbol, positions in self.positions_by_symbol.items()
            }
        }
    
    async def update_from_executions(self, executions: List[Any]) -> None:
        """Update positions from a list of executions"""
        for execution in executions:
            try:
                await self.create_or_update_position(execution)
            except Exception as e:
                self.logger.error(f"Error updating position from execution: {e}")
    
    async def _position_monitoring_task(self) -> None:
        """Monitor positions for risk and performance"""
        while True:
            try:
                open_positions = self.get_open_positions()
                
                for position in open_positions:
                    # Check liquidation risk
                    if position.liquidation_price:
                        if (position.side == PositionSide.LONG and 
                            position.current_price <= position.liquidation_price * Decimal("1.05")) or \
                           (position.side == PositionSide.SHORT and 
                            position.current_price >= position.liquidation_price * Decimal("0.95")):
                            self.logger.warning(f"Position {position.position_id} near liquidation!")
                            position.metadata["liquidation_warning"] = datetime.utcnow().isoformat()
                    
                    # Update time in profit/loss
                    if position.unrealized_pnl > 0:
                        position.time_in_profit += timedelta(seconds=60)
                    else:
                        position.time_in_loss += timedelta(seconds=60)
                
                await asyncio.sleep(60)  # Check every minute
                
            except Exception as e:
                self.logger.error(f"Error in position monitoring: {e}")
                await asyncio.sleep(60)
    
    async def _pnl_calculation_task(self) -> None:
        """Periodically recalculate P&L metrics"""
        while True:
            try:
                # Log portfolio metrics
                metrics = self.get_portfolio_metrics()
                self.logger.info(f"Portfolio metrics: {metrics}")
                
                # Record metrics
                self.metrics_collector.record_portfolio_metrics(metrics)
                
                await asyncio.sleep(300)  # Every 5 minutes
                
            except Exception as e:
                self.logger.error(f"Error in P&L calculation: {e}")
                await asyncio.sleep(300)
    
    async def _load_persisted_positions(self) -> None:
        """Load persisted positions from storage"""
        # Implementation would load from database
        self.logger.info("Loading persisted positions...")
    
    def get_position_by_order(self, order_id: str) -> Optional[Position]:
        """Find position associated with an order"""
        for position in self.positions.values():
            for trade in position.trades:
                if trade.order_id == order_id:
                    return position
        return None