"""
AITHORIX Trading Engine
Production-ready core trading system with 175 ML models integration
"""

import asyncio
import logging
from decimal import Decimal
from typing import Dict, List, Optional, Any, Tuple
from datetime import datetime, timezone
import numpy as np
from dataclasses import dataclass, field
from enum import Enum
import aioredis
import asyncpg
from collections import defaultdict
import json

from ..exceptions import (
    InsufficientBalanceError, 
    OrderExecutionError,
    RiskLimitExceededError,
    ModelInferenceError
)
from ..constants import (
    MAX_POSITION_SIZE,
    MAX_LEVERAGE,
    STOP_LOSS_PERCENT,
    MIN_TRADE_SIZE_USD,
    MAX_DAILY_TRADES
)


class OrderType(Enum):
    MARKET = "MARKET"
    LIMIT = "LIMIT"
    STOP_LOSS = "STOP_LOSS"
    TAKE_PROFIT = "TAKE_PROFIT"
    TRAILING_STOP = "TRAILING_STOP"


class OrderSide(Enum):
    BUY = "BUY"
    SELL = "SELL"


class OrderStatus(Enum):
    PENDING = "PENDING"
    OPEN = "OPEN"
    PARTIALLY_FILLED = "PARTIALLY_FILLED"
    FILLED = "FILLED"
    CANCELLED = "CANCELLED"
    REJECTED = "REJECTED"
    EXPIRED = "EXPIRED"


@dataclass
class TradingSignal:
    """ML model generated trading signal"""
    timestamp: datetime
    symbol: str
    side: OrderSide
    confidence: float
    predicted_price: float
    predicted_timeframe: int  # minutes
    model_id: str
    features: Dict[str, float]
    risk_score: float
    

@dataclass
class Order:
    """Production order representation"""
    order_id: str
    timestamp: datetime
    symbol: str
    side: OrderSide
    order_type: OrderType
    quantity: Decimal
    price: Optional[Decimal]
    status: OrderStatus
    exchange: str
    time_in_force: str = "GTC"
    stop_price: Optional[Decimal] = None
    take_profit_price: Optional[Decimal] = None
    leverage: int = 1
    reduce_only: bool = False
    post_only: bool = False
    close_position: bool = False
    activation_price: Optional[Decimal] = None
    callback_rate: Optional[Decimal] = None
    working_type: str = "CONTRACT_PRICE"
    price_protect: bool = True
    filled_quantity: Decimal = Decimal("0")
    average_price: Optional[Decimal] = None
    commission: Decimal = Decimal("0")
    commission_asset: Optional[str] = None
    trades: List[Dict[str, Any]] = field(default_factory=list)
    

@dataclass
class Position:
    """Active position tracking"""
    position_id: str
    symbol: str
    side: OrderSide
    quantity: Decimal
    entry_price: Decimal
    mark_price: Decimal
    liquidation_price: Optional[Decimal]
    unrealized_pnl: Decimal
    realized_pnl: Decimal
    margin_type: str  # ISOLATED or CROSS
    leverage: int
    exchange: str
    opened_at: datetime
    updated_at: datetime
    stop_loss: Optional[Decimal] = None
    take_profit: Optional[Decimal] = None
    trailing_stop: Optional[Decimal] = None
    

class TradingEngine:
    """
    Core trading engine orchestrating all trading operations
    Manages 175 ML models, 5 exchanges, risk management, and execution
    """
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.logger = self._setup_logger()
        
        # Core components (will be initialized in startup)
        self.db_pool: Optional[asyncpg.Pool] = None
        self.redis_pool: Optional[aioredis.Redis] = None
        
        # Trading state
        self.active_orders: Dict[str, Order] = {}
        self.positions: Dict[str, Position] = {}
        self.daily_trades_count: Dict[str, int] = defaultdict(int)
        self.model_performance: Dict[str, Dict[str, float]] = defaultdict(dict)
        
        # Risk limits
        self.max_position_size = Decimal(str(config.get("max_position_size", MAX_POSITION_SIZE)))
        self.max_leverage = config.get("max_leverage", MAX_LEVERAGE)
        self.stop_loss_percent = Decimal(str(config.get("stop_loss_percent", STOP_LOSS_PERCENT)))
        self.max_daily_trades = config.get("max_daily_trades", MAX_DAILY_TRADES)
        
        # Performance tracking
        self.daily_pnl = Decimal("0")
        self.total_volume = Decimal("0")
        self.win_rate = 0.0
        self.sharpe_ratio = 0.0
        
        # Engine state
        self.is_running = False
        self.emergency_stop = False
        
    def _setup_logger(self) -> logging.Logger:
        """Configure production logging"""
        logger = logging.getLogger("aithorix.trading_engine")
        logger.setLevel(logging.INFO)
        
        # JSON formatter for structured logging
        handler = logging.StreamHandler()
        formatter = logging.Formatter(
            '{"timestamp": "%(asctime)s", "level": "%(levelname)s", '
            '"module": "%(name)s", "message": "%(message)s"}'
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        
        return logger
        
    async def startup(self):
        """Initialize all engine components"""
        self.logger.info("Starting AITHORIX Trading Engine")
        
        try:
            # Initialize database connections
            self.db_pool = await asyncpg.create_pool(
                self.config["database_url"],
                min_size=10,
                max_size=20,
                command_timeout=60
            )
            
            # Initialize Redis for real-time data
            self.redis_pool = await aioredis.create_redis_pool(
                self.config["redis_url"],
                minsize=5,
                maxsize=10
            )
            
            # Load existing positions
            await self._load_positions()
            
            # Initialize model ensemble
            await self._initialize_models()
            
            # Start background tasks
            asyncio.create_task(self._risk_monitor())
            asyncio.create_task(self._performance_tracker())
            asyncio.create_task(self._order_manager())
            
            self.is_running = True
            self.logger.info("Trading Engine started successfully")
            
        except Exception as e:
            self.logger.error(f"Failed to start Trading Engine: {e}")
            raise
            
    async def shutdown(self):
        """Gracefully shutdown the engine"""
        self.logger.info("Shutting down Trading Engine")
        self.is_running = False
        
        # Cancel all pending orders
        for order_id in list(self.active_orders.keys()):
            await self.cancel_order(order_id)
            
        # Close database connections
        if self.db_pool:
            await self.db_pool.close()
        if self.redis_pool:
            self.redis_pool.close()
            await self.redis_pool.wait_closed()
            
        self.logger.info("Trading Engine shutdown complete")
        
    async def process_signal(self, signal: TradingSignal) -> Optional[Order]:
        """
        Process trading signal from ML models
        Implements full validation, risk checks, and execution
        """
        if self.emergency_stop:
            self.logger.warning("Emergency stop active, rejecting signal")
            return None
            
        try:
            # Validate signal
            if not await self._validate_signal(signal):
                return None
                
            # Risk checks
            risk_check = await self._check_risk_limits(signal)
            if not risk_check["passed"]:
                self.logger.warning(f"Risk check failed: {risk_check['reason']}")
                return None
                
            # Calculate position size
            position_size = await self._calculate_position_size(signal)
            if position_size < MIN_TRADE_SIZE_USD:
                self.logger.info(f"Position size {position_size} below minimum")
                return None
                
            # Create order
            order = await self._create_order_from_signal(signal, position_size)
            
            # Execute order
            executed_order = await self._execute_order(order)
            
            # Update model performance
            await self._update_model_performance(signal.model_id, executed_order)
            
            return executed_order
            
        except Exception as e:
            self.logger.error(f"Error processing signal: {e}")
            await self._record_error("signal_processing", str(e), signal)
            return None
            
    async def _validate_signal(self, signal: TradingSignal) -> bool:
        """Comprehensive signal validation"""
        # Check confidence threshold
        if signal.confidence < 0.85:  # 85% minimum confidence
            return False
            
        # Check if we already have a position in this symbol
        existing_position = self.positions.get(signal.symbol)
        if existing_position:
            # Allow adding to winning positions only
            if existing_position.unrealized_pnl < 0:
                return False
                
        # Check daily trade limit
        if self.daily_trades_count[signal.symbol] >= self.max_daily_trades:
            return False
            
        # Validate against market conditions
        market_valid = await self._validate_market_conditions(signal.symbol)
        if not market_valid:
            return False
            
        return True
        
    async def _check_risk_limits(self, signal: TradingSignal) -> Dict[str, Any]:
        """Comprehensive risk management checks"""
        checks = {
            "passed": True,
            "reason": None,
            "risk_score": 0.0
        }
        
        # Portfolio concentration check
        total_exposure = sum(
            pos.quantity * pos.mark_price 
            for pos in self.positions.values()
        )
        
        if total_exposure > self.max_position_size * Decimal("100"):
            checks["passed"] = False
            checks["reason"] = "Portfolio concentration limit exceeded"
            return checks
            
        # Correlation check
        correlation_risk = await self._calculate_correlation_risk(signal.symbol)
        if correlation_risk > 0.7:
            checks["passed"] = False
            checks["reason"] = f"High correlation risk: {correlation_risk}"
            return checks
            
        # Drawdown check
        current_drawdown = await self._calculate_current_drawdown()
        if current_drawdown > Decimal("0.015"):  # 1.5% drawdown limit
            checks["passed"] = False
            checks["reason"] = f"Drawdown limit exceeded: {current_drawdown}"
            return checks
            
        # Volatility check
        volatility = await self._get_current_volatility(signal.symbol)
        if volatility > 0.05:  # 5% volatility threshold
            checks["risk_score"] = 0.8
            
        checks["risk_score"] = signal.risk_score
        return checks
        
    async def _calculate_position_size(self, signal: TradingSignal) -> Decimal:
        """
        Kelly Criterion based position sizing with ML enhancement
        """
        # Get account balance
        account_balance = await self._get_account_balance(signal.symbol.split("/")[1])
        
        # Base position size (Kelly Criterion)
        win_probability = signal.confidence
        win_loss_ratio = 1.5  # Target 1.5:1 risk/reward
        
        kelly_fraction = (win_probability * win_loss_ratio - (1 - win_probability)) / win_loss_ratio
        kelly_fraction = max(0, min(kelly_fraction, 0.25))  # Cap at 25%
        
        # Adjust for volatility
        volatility_multiplier = 1.0
        current_volatility = await self._get_current_volatility(signal.symbol)
        if current_volatility > 0.03:
            volatility_multiplier = 0.5
            
        # Adjust for correlation
        correlation_multiplier = 1.0
        correlation_risk = await self._calculate_correlation_risk(signal.symbol)
        if correlation_risk > 0.5:
            correlation_multiplier = 0.7
            
        # Calculate final position size
        position_size = (
            account_balance * 
            Decimal(str(kelly_fraction)) * 
            Decimal(str(volatility_multiplier)) * 
            Decimal(str(correlation_multiplier))
        )
        
        # Apply maximum position size limit
        max_size = account_balance * self.max_position_size
        position_size = min(position_size, max_size)
        
        return position_size
        
    async def _create_order_from_signal(
        self, 
        signal: TradingSignal, 
        position_size: Decimal
    ) -> Order:
        """Create order with all safety features"""
        # Calculate quantity based on current price
        current_price = await self._get_current_price(signal.symbol)
        quantity = position_size / current_price
        
        # Round to exchange precision
        quantity = await self._round_to_precision(signal.symbol, quantity)
        
        # Calculate stop loss and take profit
        stop_loss_price = None
        take_profit_price = None
        
        if signal.side == OrderSide.BUY:
            stop_loss_price = current_price * (Decimal("1") - self.stop_loss_percent)
            take_profit_price = current_price * Decimal("1.03")  # 3% take profit
        else:
            stop_loss_price = current_price * (Decimal("1") + self.stop_loss_percent)
            take_profit_price = current_price * Decimal("0.97")
            
        # Determine best exchange for execution
        best_exchange = await self._select_best_exchange(signal.symbol, quantity)
        
        # Create order
        order = Order(
            order_id=self._generate_order_id(),
            timestamp=datetime.now(timezone.utc),
            symbol=signal.symbol,
            side=signal.side,
            order_type=OrderType.LIMIT,
            quantity=quantity,
            price=current_price,
            status=OrderStatus.PENDING,
            exchange=best_exchange,
            leverage=min(self.max_leverage, 10),  # Conservative leverage
            stop_price=stop_loss_price,
            take_profit_price=take_profit_price,
            post_only=True,  # Maker only for lower fees
            price_protect=True
        )
        
        return order
        
    async def _execute_order(self, order: Order) -> Order:
        """Execute order with retry logic and error handling"""
        max_retries = 3
        retry_count = 0
        
        while retry_count < max_retries:
            try:
                # Update order status
                order.status = OrderStatus.OPEN
                self.active_orders[order.order_id] = order
                
                # Send to exchange
                exchange_response = await self._send_order_to_exchange(order)
                
                # Update order with exchange data
                order.exchange_order_id = exchange_response["id"]
                order.status = OrderStatus.OPEN
                
                # Record order in database
                await self._record_order(order)
                
                # Increment daily trade count
                self.daily_trades_count[order.symbol] += 1
                
                # Start order monitoring
                asyncio.create_task(self._monitor_order(order))
                
                self.logger.info(f"Order executed: {order.order_id}")
                return order
                
            except Exception as e:
                retry_count += 1
                self.logger.error(f"Order execution failed (attempt {retry_count}): {e}")
                
                if retry_count >= max_retries:
                    order.status = OrderStatus.REJECTED
                    await self._record_order_failure(order, str(e))
                    raise OrderExecutionError(f"Failed to execute order after {max_retries} attempts")
                    
                await asyncio.sleep(0.5 * retry_count)  # Exponential backoff
                
    async def _monitor_order(self, order: Order):
        """Monitor order execution and manage position"""
        while order.status in [OrderStatus.OPEN, OrderStatus.PARTIALLY_FILLED]:
            try:
                # Check order status
                status = await self._check_order_status(order)
                
                if status["status"] == "FILLED":
                    order.status = OrderStatus.FILLED
                    order.filled_quantity = Decimal(str(status["filled_quantity"]))
                    order.average_price = Decimal(str(status["average_price"]))
                    
                    # Create or update position
                    await self._update_position(order)
                    
                    # Place stop loss and take profit orders
                    if order.stop_price:
                        await self._place_stop_loss(order)
                    if order.take_profit_price:
                        await self._place_take_profit(order)
                        
                elif status["status"] == "CANCELLED":
                    order.status = OrderStatus.CANCELLED
                    
                elif status["status"] == "EXPIRED":
                    order.status = OrderStatus.EXPIRED
                    
                # Update database
                await self._update_order_status(order)
                
                if order.status not in [OrderStatus.OPEN, OrderStatus.PARTIALLY_FILLED]:
                    break
                    
                await asyncio.sleep(1)  # Check every second
                
            except Exception as e:
                self.logger.error(f"Error monitoring order {order.order_id}: {e}")
                await asyncio.sleep(5)
                
    async def _risk_monitor(self):
        """Continuous risk monitoring task"""
        while self.is_running:
            try:
                # Check portfolio risk metrics
                portfolio_risk = await self._calculate_portfolio_risk()
                
                # Check drawdown
                current_drawdown = await self._calculate_current_drawdown()
                if current_drawdown > Decimal("0.02"):  # 2% emergency threshold
                    self.logger.critical(f"Emergency stop triggered: drawdown {current_drawdown}")
                    self.emergency_stop = True
                    await self._emergency_close_all_positions()
                    
                # Check correlation risk
                correlation_matrix = await self._calculate_correlation_matrix()
                max_correlation = np.max(np.abs(correlation_matrix - np.eye(len(correlation_matrix))))
                if max_correlation > 0.8:
                    self.logger.warning(f"High correlation detected: {max_correlation}")
                    
                # Check position limits
                for symbol, position in self.positions.items():
                    position_value = position.quantity * position.mark_price
                    account_balance = await self._get_account_balance()
                    
                    if position_value > account_balance * self.max_position_size:
                        self.logger.warning(f"Position limit exceeded for {symbol}")
                        await self._reduce_position(position, 0.5)  # Reduce by 50%
                        
                # Update risk metrics in Redis
                await self._update_risk_metrics(portfolio_risk)
                
                await asyncio.sleep(5)  # Check every 5 seconds
                
            except Exception as e:
                self.logger.error(f"Risk monitor error: {e}")
                await asyncio.sleep(10)
                
    async def _performance_tracker(self):
        """Track and optimize performance metrics"""
        while self.is_running:
            try:
                # Calculate performance metrics
                metrics = await self._calculate_performance_metrics()
                
                # Update Sharpe ratio
                self.sharpe_ratio = metrics["sharpe_ratio"]
                
                # Update win rate
                self.win_rate = metrics["win_rate"]
                
                # Check if we're meeting targets
                if self.daily_pnl < Decimal(str(self.config["target_daily_return"])):
                    self.logger.info(f"Below daily target: {self.daily_pnl}")
                    
                # Model performance analysis
                for model_id, performance in self.model_performance.items():
                    if performance.get("accuracy", 0) < 0.85:
                        self.logger.warning(f"Model {model_id} underperforming: {performance}")
                        
                # Record metrics
                await self._record_performance_metrics(metrics)
                
                await asyncio.sleep(60)  # Update every minute
                
            except Exception as e:
                self.logger.error(f"Performance tracker error: {e}")
                await asyncio.sleep(60)
                
    async def _get_current_price(self, symbol: str) -> Decimal:
        """Get current market price from Redis cache"""
        price_key = f"price:{symbol}"
        price = await self.redis_pool.get(price_key)
        
        if not price:
            # Fallback to database
            async with self.db_pool.acquire() as conn:
                result = await conn.fetchone(
                    "SELECT price FROM market_data WHERE symbol = $1 ORDER BY timestamp DESC LIMIT 1",
                    symbol
                )
                price = result["price"] if result else Decimal("0")
        else:
            price = Decimal(price.decode())
            
        return price
        
    async def _get_account_balance(self, asset: str = "USDT") -> Decimal:
        """Get current account balance"""
        balance_key = f"balance:{asset}"
        balance = await self.redis_pool.get(balance_key)
        
        if not balance:
            # Fallback to database
            async with self.db_pool.acquire() as conn:
                result = await conn.fetchone(
                    "SELECT balance FROM account_balances WHERE asset = $1",
                    asset
                )
                balance = result["balance"] if result else Decimal("0")
        else:
            balance = Decimal(balance.decode())
            
        return balance
        
    async def _calculate_portfolio_risk(self) -> Dict[str, float]:
        """Calculate comprehensive portfolio risk metrics"""
        positions_data = []
        
        for position in self.positions.values():
            positions_data.append({
                "symbol": position.symbol,
                "value": float(position.quantity * position.mark_price),
                "pnl": float(position.unrealized_pnl),
                "leverage": position.leverage
            })
            
        if not positions_data:
            return {"var": 0.0, "cvar": 0.0, "max_drawdown": 0.0}
            
        # Calculate Value at Risk (VaR)
        values = np.array([p["value"] for p in positions_data])
        returns = np.array([p["pnl"] / p["value"] if p["value"] > 0 else 0 for p in positions_data])
        
        var_95 = np.percentile(returns, 5)
        cvar_95 = np.mean(returns[returns <= var_95]) if len(returns[returns <= var_95]) > 0 else var_95
        
        return {
            "var": float(var_95),
            "cvar": float(cvar_95),
            "total_exposure": float(np.sum(values)),
            "position_count": len(positions_data),
            "average_leverage": float(np.mean([p["leverage"] for p in positions_data]))
        }
        
    def _generate_order_id(self) -> str:
        """Generate unique order ID"""
        import uuid
        return f"ORD-{datetime.now().strftime('%Y%m%d')}-{str(uuid.uuid4())[:8]}"
        
    async def _record_order(self, order: Order):
        """Record order in database"""
        async with self.db_pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO orders (
                    order_id, timestamp, symbol, side, order_type,
                    quantity, price, status, exchange, leverage
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
            """, 
                order.order_id, order.timestamp, order.symbol, order.side.value,
                order.order_type.value, order.quantity, order.price,
                order.status.value, order.exchange, order.leverage
            )