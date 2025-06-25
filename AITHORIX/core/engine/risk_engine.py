"""
AITHORIX Risk Engine
Real-time risk management and monitoring

This module provides comprehensive risk management including:
- Real-time risk calculation and monitoring
- Position and portfolio risk assessment
- Risk limit enforcement
- Emergency risk controls
"""

import asyncio
import logging
from typing import Dict, List, Optional, Any, Set, Tuple
from datetime import datetime, timedelta
from decimal import Decimal
from dataclasses import dataclass, field
from enum import Enum
import numpy as np
from collections import defaultdict
import json

from core.engine.position_manager import Position, PositionStatus
from core.engine.order_manager import Order, OrderStatus
from core.coordinator.strategy_coordinator import Signal
from utils.helpers import get_timestamp, synchronized
from monitoring.metrics import MetricsCollector


class RiskLevel(Enum):
    """Risk level enumeration"""
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


class RiskAlertSeverity(Enum):
    """Risk alert severity levels"""
    INFO = "info"
    WARNING = "warning"
    HIGH = "high"
    CRITICAL = "critical"


@dataclass
class RiskMetrics:
    """Container for risk metrics"""
    timestamp: datetime
    
    # Portfolio metrics
    total_exposure: Decimal
    net_exposure: Decimal
    gross_exposure: Decimal
    
    # P&L metrics
    unrealized_pnl: Decimal
    realized_pnl: Decimal
    total_pnl: Decimal
    
    # Risk metrics
    portfolio_var: Decimal  # Value at Risk
    portfolio_cvar: Decimal  # Conditional VaR
    sharpe_ratio: float
    sortino_ratio: float
    max_drawdown: Decimal
    current_drawdown: Decimal
    
    # Concentration metrics
    largest_position_pct: Decimal
    top_5_concentration: Decimal
    herfindahl_index: Decimal
    
    # Greeks (if applicable)
    portfolio_delta: Decimal = Decimal("0")
    portfolio_gamma: Decimal = Decimal("0")
    portfolio_vega: Decimal = Decimal("0")
    portfolio_theta: Decimal = Decimal("0")
    
    # Leverage metrics
    gross_leverage: Decimal = Decimal("1")
    net_leverage: Decimal = Decimal("1")
    
    # Correlation metrics
    average_correlation: float = 0.0
    max_correlation: float = 0.0
    
    # Additional metrics
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class RiskAlert:
    """Risk alert notification"""
    alert_id: str
    timestamp: datetime
    severity: str  # Using string for RiskAlertSeverity
    alert_type: str
    message: str
    metrics: Dict[str, Any]
    recommended_action: Optional[str] = None
    auto_action_taken: bool = False
    resolved: bool = False
    resolved_at: Optional[datetime] = None


@dataclass
class RiskLimits:
    """Risk limit configuration"""
    # Position limits
    max_position_size: Decimal
    max_position_value: Decimal
    max_positions_per_symbol: int
    max_total_positions: int
    
    # Exposure limits
    max_gross_exposure: Decimal
    max_net_exposure: Decimal
    max_sector_exposure: Decimal
    max_exchange_exposure: Decimal
    
    # Loss limits
    max_daily_loss: Decimal
    max_drawdown: Decimal
    max_consecutive_losses: int
    
    # VaR limits
    var_limit_95: Decimal
    var_limit_99: Decimal
    cvar_limit: Decimal
    
    # Leverage limits
    max_gross_leverage: Decimal
    max_net_leverage: Decimal
    
    # Concentration limits
    max_concentration_single: Decimal  # Max % in single position
    max_concentration_top5: Decimal   # Max % in top 5 positions
    
    # Greeks limits (for options)
    max_portfolio_delta: Optional[Decimal] = None
    max_portfolio_gamma: Optional[Decimal] = None
    max_portfolio_vega: Optional[Decimal] = None


class RiskEngine:
    """
    Comprehensive risk management engine
    
    Monitors and manages all aspects of trading risk including:
    - Position and portfolio risk
    - Market risk metrics
    - Operational risk
    - Compliance and limits
    """
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.logger = logging.getLogger("AITHORIX.RiskEngine")
        
        # Risk limits
        self.risk_limits = self._create_risk_limits(config.get("limits", {}))
        
        # Risk state
        self.current_metrics: Optional[RiskMetrics] = None
        self.risk_alerts: List[RiskAlert] = []
        self.active_alerts: Dict[str, RiskAlert] = {}
        
        # Historical data for calculations
        self.returns_history: deque = deque(maxlen=252)  # 1 year of daily returns
        self.metrics_history: deque = deque(maxlen=1440)  # 24 hours of minute data
        self.high_water_mark: Decimal = Decimal("0")
        
        # Risk calculation parameters
        self.var_confidence = Decimal(str(config.get("var_confidence", "0.95")))
        self.var_horizon = config.get("var_horizon", 1)  # days
        self.correlation_window = config.get("correlation_window", 20)  # days
        
        # Monitoring configuration
        self.monitoring_interval = config.get("monitoring_interval", 1)  # seconds
        self.alert_cooldown = config.get("alert_cooldown", 300)  # 5 minutes
        
        # Performance tracking
        self.metrics_collector = MetricsCollector()
        
        # State
        self._lock = asyncio.Lock()
        self.is_initialized = False
        self.is_monitoring = False
        
        # Alert history
        self.alert_history: Dict[str, datetime] = {}  # alert_type -> last_alert_time
    
    async def initialize(self) -> None:
        """Initialize the risk engine"""
        self.logger.info("Initializing Risk Engine...")
        
        # Load historical data if available
        await self._load_historical_data()
        
        # Start monitoring tasks
        asyncio.create_task(self._risk_monitoring_loop())
        asyncio.create_task(self._alert_management_loop())
        
        self.is_initialized = True
        self.logger.info("Risk Engine initialized")
    
    async def start(self) -> None:
        """Start risk monitoring"""
        self.logger.info("Starting risk monitoring...")
        self.is_monitoring = True
    
    async def stop(self) -> None:
        """Stop risk monitoring"""
        self.logger.info("Stopping risk monitoring...")
        self.is_monitoring = False
    
    async def configure(self, params: Dict[str, Any]) -> None:
        """Update risk configuration"""
        if "limits" in params:
            self.risk_limits = self._create_risk_limits(params["limits"])
        
        if "var_confidence" in params:
            self.var_confidence = Decimal(str(params["var_confidence"]))
        
        if "monitoring_interval" in params:
            self.monitoring_interval = params["monitoring_interval"]
        
        self.logger.info("Risk configuration updated")
    
    @synchronized
    async def calculate_risk_metrics(
        self, 
        positions: List[Position], 
        open_orders: List[Order]
    ) -> RiskMetrics:
        """Calculate comprehensive risk metrics"""
        timestamp = datetime.utcnow()
        
        # Calculate exposure metrics
        exposure_metrics = self._calculate_exposure_metrics(positions)
        
        # Calculate P&L metrics
        pnl_metrics = self._calculate_pnl_metrics(positions)
        
        # Calculate VaR and CVaR
        var_metrics = await self._calculate_var_metrics(positions)
        
        # Calculate performance ratios
        performance_metrics = self._calculate_performance_metrics()
        
        # Calculate concentration metrics
        concentration_metrics = self._calculate_concentration_metrics(positions)
        
        # Calculate drawdown
        drawdown_metrics = self._calculate_drawdown_metrics(pnl_metrics["total_pnl"])
        
        # Create risk metrics
        metrics = RiskMetrics(
            timestamp=timestamp,
            total_exposure=exposure_metrics["total_exposure"],
            net_exposure=exposure_metrics["net_exposure"],
            gross_exposure=exposure_metrics["gross_exposure"],
            unrealized_pnl=pnl_metrics["unrealized_pnl"],
            realized_pnl=pnl_metrics["realized_pnl"],
            total_pnl=pnl_metrics["total_pnl"],
            portfolio_var=var_metrics["var_95"],
            portfolio_cvar=var_metrics["cvar_95"],
            sharpe_ratio=performance_metrics["sharpe_ratio"],
            sortino_ratio=performance_metrics["sortino_ratio"],
            max_drawdown=drawdown_metrics["max_drawdown"],
            current_drawdown=drawdown_metrics["current_drawdown"],
            largest_position_pct=concentration_metrics["largest_position_pct"],
            top_5_concentration=concentration_metrics["top_5_concentration"],
            herfindahl_index=concentration_metrics["herfindahl_index"],
            gross_leverage=exposure_metrics["gross_leverage"],
            net_leverage=exposure_metrics["net_leverage"]
        )
        
        # Store current metrics
        self.current_metrics = metrics
        self.metrics_history.append(metrics)
        
        # Record metrics
        self.metrics_collector.update_risk_metrics(metrics)
        
        return metrics
    
    @synchronized
    async def check_risk_limits(self, metrics: RiskMetrics) -> List[RiskAlert]:
        """Check if any risk limits are breached"""
        alerts = []
        
        # Check exposure limits
        if metrics.gross_exposure > self.risk_limits.max_gross_exposure:
            alerts.append(self._create_alert(
                severity=RiskAlertSeverity.HIGH,
                alert_type="gross_exposure_breach",
                message=f"Gross exposure {metrics.gross_exposure} exceeds limit {self.risk_limits.max_gross_exposure}",
                metrics={"gross_exposure": float(metrics.gross_exposure)}
            ))
        
        # Check drawdown limits
        if metrics.current_drawdown > self.risk_limits.max_drawdown:
            alerts.append(self._create_alert(
                severity=RiskAlertSeverity.CRITICAL,
                alert_type="max_drawdown_breach",
                message=f"Drawdown {metrics.current_drawdown} exceeds limit {self.risk_limits.max_drawdown}",
                metrics={"current_drawdown": float(metrics.current_drawdown)},
                recommended_action="Close all positions"
            ))
        
        # Check VaR limits
        if metrics.portfolio_var > self.risk_limits.var_limit_95:
            alerts.append(self._create_alert(
                severity=RiskAlertSeverity.WARNING,
                alert_type="var_limit_breach",
                message=f"95% VaR {metrics.portfolio_var} exceeds limit {self.risk_limits.var_limit_95}",
                metrics={"var_95": float(metrics.portfolio_var)}
            ))
        
        # Check concentration limits
        if metrics.largest_position_pct > self.risk_limits.max_concentration_single:
            alerts.append(self._create_alert(
                severity=RiskAlertSeverity.WARNING,
                alert_type="concentration_breach",
                message=f"Largest position {metrics.largest_position_pct}% exceeds limit {self.risk_limits.max_concentration_single}%",
                metrics={"largest_position_pct": float(metrics.largest_position_pct)}
            ))
        
        # Check leverage limits
        if metrics.gross_leverage > self.risk_limits.max_gross_leverage:
            alerts.append(self._create_alert(
                severity=RiskAlertSeverity.HIGH,
                alert_type="leverage_breach",
                message=f"Gross leverage {metrics.gross_leverage}x exceeds limit {self.risk_limits.max_gross_leverage}x",
                metrics={"gross_leverage": float(metrics.gross_leverage)}
            ))
        
        # Store alerts
        for alert in alerts:
            self.risk_alerts.append(alert)
            self.active_alerts[alert.alert_type] = alert
        
        return alerts
    
    async def approve_signal(self, signal: Signal) -> bool:
        """Check if a trading signal passes risk checks"""
        if not self.current_metrics:
            self.logger.warning("No current risk metrics available")
            return True  # Allow if no metrics yet
        
        # Check if we're in a critical risk state
        critical_alerts = [
            alert for alert in self.active_alerts.values()
            if alert.severity == RiskAlertSeverity.CRITICAL.value and not alert.resolved
        ]
        
        if critical_alerts:
            self.logger.warning(f"Signal rejected due to critical risk alerts: {critical_alerts}")
            return False
        
        # Check position limits
        # This would be more sophisticated in production
        estimated_position_value = signal.quantity * signal.entry_price
        
        if estimated_position_value > self.risk_limits.max_position_value:
            self.logger.warning(f"Signal rejected: Position value {estimated_position_value} exceeds limit")
            return False
        
        # Check if adding this position would breach exposure limits
        new_exposure = self.current_metrics.gross_exposure + estimated_position_value
        if new_exposure > self.risk_limits.max_gross_exposure:
            self.logger.warning(f"Signal rejected: Would breach gross exposure limit")
            return False
        
        return True
    
    async def approve_order(self, order: Order) -> bool:
        """Check if an order passes risk checks"""
        # Similar to approve_signal but for orders
        return True  # Simplified for now
    
    def get_risk_level(self) -> RiskLevel:
        """Get current overall risk level"""
        if not self.current_metrics:
            return RiskLevel.LOW
        
        # Count active alerts by severity
        critical_count = sum(
            1 for alert in self.active_alerts.values()
            if alert.severity == RiskAlertSeverity.CRITICAL.value and not alert.resolved
        )
        high_count = sum(
            1 for alert in self.active_alerts.values()
            if alert.severity == RiskAlertSeverity.HIGH.value and not alert.resolved
        )
        
        if critical_count > 0:
            return RiskLevel.CRITICAL
        elif high_count > 0:
            return RiskLevel.HIGH
        elif self.current_metrics.current_drawdown > self.risk_limits.max_drawdown * Decimal("0.5"):
            return RiskLevel.MEDIUM
        else:
            return RiskLevel.LOW
    
    def _calculate_exposure_metrics(self, positions: List[Position]) -> Dict[str, Decimal]:
        """Calculate exposure metrics"""
        long_exposure = Decimal("0")
        short_exposure = Decimal("0")
        
        for position in positions:
            if not position.is_open:
                continue
            
            position_value = position.position_value
            if position.side.value == "long":
                long_exposure += position_value
            else:
                short_exposure += position_value
        
        gross_exposure = long_exposure + short_exposure
        net_exposure = long_exposure - short_exposure
        total_exposure = gross_exposure
        
        # Calculate leverage (simplified - would need account equity)
        account_equity = Decimal("100000")  # Placeholder
        gross_leverage = gross_exposure / account_equity if account_equity > 0 else Decimal("0")
        net_leverage = abs(net_exposure) / account_equity if account_equity > 0 else Decimal("0")
        
        return {
            "total_exposure": total_exposure,
            "gross_exposure": gross_exposure,
            "net_exposure": net_exposure,
            "long_exposure": long_exposure,
            "short_exposure": short_exposure,
            "gross_leverage": gross_leverage,
            "net_leverage": net_leverage
        }
    
    def _calculate_pnl_metrics(self, positions: List[Position]) -> Dict[str, Decimal]:
        """Calculate P&L metrics"""
        unrealized_pnl = Decimal("0")
        realized_pnl = Decimal("0")
        
        for position in positions:
            if position.is_open:
                unrealized_pnl += position.unrealized_pnl
            realized_pnl += position.realized_pnl
        
        total_pnl = unrealized_pnl + realized_pnl
        
        return {
            "unrealized_pnl": unrealized_pnl,
            "realized_pnl": realized_pnl,
            "total_pnl": total_pnl
        }
    
    async def _calculate_var_metrics(self, positions: List[Position]) -> Dict[str, Decimal]:
        """Calculate Value at Risk metrics"""
        if len(self.returns_history) < 20:  # Need minimum history
            return {
                "var_95": Decimal("0"),
                "var_99": Decimal("0"),
                "cvar_95": Decimal("0"),
                "cvar_99": Decimal("0")
            }
        
        # Convert returns to numpy array
        returns = np.array([float(r) for r in self.returns_history])
        
        # Calculate VaR at different confidence levels
        var_95 = np.percentile(returns, 5)  # 95% confidence
        var_99 = np.percentile(returns, 1)  # 99% confidence
        
        # Calculate CVaR (expected shortfall)
        cvar_95 = np.mean(returns[returns <= var_95])
        cvar_99 = np.mean(returns[returns <= var_99])
        
        # Scale by current portfolio value
        portfolio_value = sum(p.position_value for p in positions if p.is_open)
        
        return {
            "var_95": Decimal(str(abs(var_95))) * portfolio_value,
            "var_99": Decimal(str(abs(var_99))) * portfolio_value,
            "cvar_95": Decimal(str(abs(cvar_95))) * portfolio_value,
            "cvar_99": Decimal(str(abs(cvar_99))) * portfolio_value
        }
    
    def _calculate_performance_metrics(self) -> Dict[str, float]:
        """Calculate performance metrics"""
        if len(self.returns_history) < 20:
            return {
                "sharpe_ratio": 0.0,
                "sortino_ratio": 0.0,
                "calmar_ratio": 0.0
            }
        
        returns = np.array([float(r) for r in self.returns_history])
        
        # Sharpe ratio (assuming risk-free rate = 0)
        if np.std(returns) > 0:
            sharpe_ratio = np.mean(returns) / np.std(returns) * np.sqrt(252)
        else:
            sharpe_ratio = 0.0
        
        # Sortino ratio (downside deviation)
        downside_returns = returns[returns < 0]
        if len(downside_returns) > 0 and np.std(downside_returns) > 0:
            sortino_ratio = np.mean(returns) / np.std(downside_returns) * np.sqrt(252)
        else:
            sortino_ratio = 0.0
        
        # Calmar ratio
        max_dd = float(self.current_metrics.max_drawdown) if self.current_metrics else 0.0
        if max_dd > 0:
            annual_return = np.mean(returns) * 252
            calmar_ratio = annual_return / max_dd
        else:
            calmar_ratio = 0.0
        
        return {
            "sharpe_ratio": sharpe_ratio,
            "sortino_ratio": sortino_ratio,
            "calmar_ratio": calmar_ratio
        }
    
    def _calculate_concentration_metrics(self, positions: List[Position]) -> Dict[str, Decimal]:
        """Calculate concentration metrics"""
        open_positions = [p for p in positions if p.is_open]
        
        if not open_positions:
            return {
                "largest_position_pct": Decimal("0"),
                "top_5_concentration": Decimal("0"),
                "herfindahl_index": Decimal("0")
            }
        
        # Sort by position value
        sorted_positions = sorted(open_positions, key=lambda p: p.position_value, reverse=True)
        total_value = sum(p.position_value for p in open_positions)
        
        if total_value == 0:
            return {
                "largest_position_pct": Decimal("0"),
                "top_5_concentration": Decimal("0"),
                "herfindahl_index": Decimal("0")
            }
        
        # Largest position percentage
        largest_position_pct = (sorted_positions[0].position_value / total_value) * Decimal("100")
        
        # Top 5 concentration
        top_5_value = sum(p.position_value for p in sorted_positions[:5])
        top_5_concentration = (top_5_value / total_value) * Decimal("100")
        
        # Herfindahl index (sum of squared market shares)
        herfindahl_index = sum(
            (p.position_value / total_value) ** 2 for p in open_positions
        ) * Decimal("10000")  # Scale to 0-10000
        
        return {
            "largest_position_pct": largest_position_pct,
            "top_5_concentration": top_5_concentration,
            "herfindahl_index": herfindahl_index
        }
    
    def _calculate_drawdown_metrics(self, current_value: Decimal) -> Dict[str, Decimal]:
        """Calculate drawdown metrics"""
        # Update high water mark
        if current_value > self.high_water_mark:
            self.high_water_mark = current_value
        
        # Calculate current drawdown
        if self.high_water_mark > 0:
            current_drawdown = (self.high_water_mark - current_value) / self.high_water_mark
        else:
            current_drawdown = Decimal("0")
        
        # Update max drawdown
        if not hasattr(self, 'max_drawdown'):
            self.max_drawdown = Decimal("0")
        
        if current_drawdown > self.max_drawdown:
            self.max_drawdown = current_drawdown
        
        return {
            "current_drawdown": current_drawdown,
            "max_drawdown": self.max_drawdown,
            "high_water_mark": self.high_water_mark
        }
    
    def _create_risk_limits(self, config: Dict[str, Any]) -> RiskLimits:
        """Create risk limits from configuration"""
        return RiskLimits(
            # Position limits
            max_position_size=Decimal(str(config.get("max_position_size", "10000"))),
            max_position_value=Decimal(str(config.get("max_position_value", "50000"))),
            max_positions_per_symbol=config.get("max_positions_per_symbol", 3),
            max_total_positions=config.get("max_total_positions", 50),
            
            # Exposure limits
            max_gross_exposure=Decimal(str(config.get("max_gross_exposure", "1000000"))),
            max_net_exposure=Decimal(str(config.get("max_net_exposure", "500000"))),
            max_sector_exposure=Decimal(str(config.get("max_sector_exposure", "300000"))),
            max_exchange_exposure=Decimal(str(config.get("max_exchange_exposure", "400000"))),
            
            # Loss limits
            max_daily_loss=Decimal(str(config.get("max_daily_loss", "0.02"))),
            max_drawdown=Decimal(str(config.get("max_drawdown", "0.05"))),
            max_consecutive_losses=config.get("max_consecutive_losses", 5),
            
            # VaR limits
            var_limit_95=Decimal(str(config.get("var_limit_95", "50000"))),
            var_limit_99=Decimal(str(config.get("var_limit_99", "100000"))),
            cvar_limit=Decimal(str(config.get("cvar_limit", "150000"))),
            
            # Leverage limits
            max_gross_leverage=Decimal(str(config.get("max_gross_leverage", "10"))),
            max_net_leverage=Decimal(str(config.get("max_net_leverage", "5"))),
            
            # Concentration limits
            max_concentration_single=Decimal(str(config.get("max_concentration_single", "20"))),
            max_concentration_top5=Decimal(str(config.get("max_concentration_top5", "60")))
        )
    
    def _create_alert(
        self,
        severity: RiskAlertSeverity,
        alert_type: str,
        message: str,
        metrics: Dict[str, Any],
        recommended_action: Optional[str] = None
    ) -> RiskAlert:
        """Create a risk alert"""
        # Check cooldown
        last_alert_time = self.alert_history.get(alert_type)
        if last_alert_time:
            time_since_last = (datetime.utcnow() - last_alert_time).total_seconds()
            if time_since_last < self.alert_cooldown:
                return None  # Skip alert due to cooldown
        
        alert = RiskAlert(
            alert_id=f"{alert_type}_{get_timestamp()}",
            timestamp=datetime.utcnow(),
            severity=severity.value,
            alert_type=alert_type,
            message=message,
            metrics=metrics,
            recommended_action=recommended_action
        )
        
        # Update alert history
        self.alert_history[alert_type] = datetime.utcnow()
        
        # Log alert
        self.logger.warning(f"Risk Alert: {alert.message}")
        
        return alert
    
    async def _risk_monitoring_loop(self) -> None:
        """Continuous risk monitoring loop"""
        while True:
            try:
                if not self.is_monitoring:
                    await asyncio.sleep(1)
                    continue
                
                # Monitor active alerts
                for alert_type, alert in list(self.active_alerts.items()):
                    if not alert.resolved and self._should_auto_resolve_alert(alert):
                        alert.resolved = True
                        alert.resolved_at = datetime.utcnow()
                        self.logger.info(f"Auto-resolved alert: {alert_type}")
                
                # Calculate portfolio returns for history
                if self.current_metrics:
                    # This would calculate actual returns
                    # For now, use a placeholder
                    daily_return = Decimal("0.001")  # 0.1% placeholder
                    self.returns_history.append(daily_return)
                
                await asyncio.sleep(self.monitoring_interval)
                
            except Exception as e:
                self.logger.error(f"Error in risk monitoring loop: {e}")
                await asyncio.sleep(5)
    
    async def _alert_management_loop(self) -> None:
        """Manage risk alerts and notifications"""
        while True:
            try:
                # Clean up old resolved alerts
                cutoff_time = datetime.utcnow() - timedelta(hours=24)
                self.risk_alerts = [
                    alert for alert in self.risk_alerts
                    if not alert.resolved or alert.resolved_at > cutoff_time
                ]
                
                await asyncio.sleep(300)  # Every 5 minutes
                
            except Exception as e:
                self.logger.error(f"Error in alert management loop: {e}")
                await asyncio.sleep(300)
    
    def _should_auto_resolve_alert(self, alert: RiskAlert) -> bool:
        """Check if an alert should be auto-resolved"""
        if not self.current_metrics:
            return False
        
        # Check if the condition that triggered the alert is resolved
        if alert.alert_type == "gross_exposure_breach":
            return self.current_metrics.gross_exposure <= self.risk_limits.max_gross_exposure
        
        elif alert.alert_type == "max_drawdown_breach":
            return self.current_metrics.current_drawdown <= self.risk_limits.max_drawdown
        
        elif alert.alert_type == "var_limit_breach":
            return self.current_metrics.portfolio_var <= self.risk_limits.var_limit_95
        
        return False
    
    async def _load_historical_data(self) -> None:
        """Load historical data for risk calculations"""
        # This would load from database
        self.logger.info("Loading historical risk data...")
    
    def get_status(self) -> Dict[str, Any]:
        """Get risk engine status"""
        return {
            "monitoring": self.is_monitoring,
            "risk_level": self.get_risk_level().value,
            "active_alerts": len([a for a in self.active_alerts.values() if not a.resolved]),
            "current_metrics": {
                "drawdown": float(self.current_metrics.current_drawdown) if self.current_metrics else 0,
                "var_95": float(self.current_metrics.portfolio_var) if self.current_metrics else 0,
                "gross_exposure": float(self.current_metrics.gross_exposure) if self.current_metrics else 0,
                "sharpe_ratio": self.current_metrics.sharpe_ratio if self.current_metrics else 0
            } if self.current_metrics else None
        }