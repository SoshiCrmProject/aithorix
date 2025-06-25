"""
AITHORIX Risk Coordinator
Coordinates all risk management activities across the system

This module integrates all risk models and provides unified risk management including:
- Portfolio risk aggregation
- Dynamic hedging coordination
- Real-time risk monitoring
- Risk limit enforcement
"""

import asyncio
import logging
from typing import Dict, List, Optional, Any, Tuple, Set
from datetime import datetime, timedelta
from decimal import Decimal
from dataclasses import dataclass, field
from enum import Enum
import numpy as np

from core.engine.risk_engine import RiskEngine, RiskLevel, RiskMetrics
from core.engine.order_manager import Order
from core.engine.position_manager import Position
from models.risk.portfolio import (
    VaRCalculator, ExpectedShortfall, DrawdownPredictor,
    CorrelationMonitor, ConcentrationRisk
)
from models.risk.dynamic import (
    DeltaOptimizer, VolatilityManager, TailOptimizer,
    GammaScalper, BetaAdjuster
)
from models.risk.realtime import (
    LimitMonitor, LeverageTracker, MarginPredictor,
    FlashPredictor, CircuitBreaker
)
from utils.helpers import get_timestamp, synchronized
from monitoring.metrics import MetricsCollector


class RiskAction(Enum):
    """Risk management actions"""
    NO_ACTION = "no_action"
    REDUCE_POSITION = "reduce_position"
    HEDGE_POSITION = "hedge_position"
    CLOSE_POSITION = "close_position"
    HALT_TRADING = "halt_trading"
    ADJUST_LIMITS = "adjust_limits"
    REBALANCE = "rebalance"
    EMERGENCY_STOP = "emergency_stop"


@dataclass
class RiskAlert:
    """Risk alert information"""
    alert_id: str
    timestamp: datetime
    risk_type: str
    severity: RiskLevel
    message: str
    affected_positions: List[str] = field(default_factory=list)
    recommended_actions: List[RiskAction] = field(default_factory=list)
    metrics: Dict[str, Any] = field(default_factory=dict)
    resolved: bool = False
    resolved_at: Optional[datetime] = None


@dataclass
class RiskState:
    """Current risk state of the system"""
    timestamp: datetime
    
    # Portfolio metrics
    portfolio_var_95: Decimal
    portfolio_var_99: Decimal
    expected_shortfall: Decimal
    current_drawdown: Decimal
    max_drawdown: Decimal
    
    # Risk utilization
    risk_budget_used: Decimal
    leverage_ratio: Decimal
    concentration_score: Decimal
    
    # Market risk
    correlation_stability: float
    volatility_regime: str
    market_stress_level: float
    
    # Operational risk
    model_performance_score: float
    system_health_score: float
    
    # Active alerts
    active_alerts: List[RiskAlert] = field(default_factory=list)
    
    # Risk actions taken
    recent_actions: List[Dict[str, Any]] = field(default_factory=list)


class RiskCoordinator:
    """
    Master risk coordination system
    
    Integrates all risk models and provides unified risk management
    across the entire trading system.
    """
    
    def __init__(
        self,
        risk_engine: RiskEngine,
        config: Dict[str, Any]
    ):
        self.risk_engine = risk_engine
        self.config = config
        self.logger = logging.getLogger("AITHORIX.RiskCoordinator")
        
        # Risk models - Portfolio
        self.var_calculator = VaRCalculator(config.get("var_config", {}))
        self.expected_shortfall = ExpectedShortfall(config.get("cvar_config", {}))
        self.drawdown_predictor = DrawdownPredictor(config.get("drawdown_config", {}))
        self.correlation_monitor = CorrelationMonitor(config.get("correlation_config", {}))
        self.concentration_risk = ConcentrationRisk(config.get("concentration_config", {}))
        
        # Risk models - Dynamic Hedging
        self.delta_optimizer = DeltaOptimizer(config.get("delta_config", {}))
        self.volatility_manager = VolatilityManager(config.get("volatility_config", {}))
        self.tail_optimizer = TailOptimizer(config.get("tail_config", {}))
        self.gamma_scalper = GammaScalper(config.get("gamma_config", {}))
        self.beta_adjuster = BetaAdjuster(config.get("beta_config", {}))
        
        # Risk models - Real-time Monitoring
        self.limit_monitor = LimitMonitor(config.get("limit_config", {}))
        self.leverage_tracker = LeverageTracker(config.get("leverage_config", {}))
        self.margin_predictor = MarginPredictor(config.get("margin_config", {}))
        self.flash_predictor = FlashPredictor(config.get("flash_config", {}))
        self.circuit_breaker = CircuitBreaker(config.get("circuit_config", {}))
        
        # State tracking
        self.current_risk_state: Optional[RiskState] = None
        self.risk_history: List[RiskState] = []
        self.active_alerts: Dict[str, RiskAlert] = {}
        self.risk_actions_taken: List[Dict[str, Any]] = []
        
        # Configuration
        self.risk_check_interval = config.get("risk_check_interval", 5)  # seconds
        self.max_risk_budget = Decimal(str(config.get("max_risk_budget", "0.02")))  # 2%
        self.max_leverage = Decimal(str(config.get("max_leverage", "10")))
        self.max_concentration = Decimal(str(config.get("max_concentration", "0.2")))  # 20%
        self.emergency_stop_threshold = config.get("emergency_stop_threshold", 0.05)  # 5% loss
        
        # Metrics
        self.metrics_collector = MetricsCollector()
        
        # Control flags
        self._lock = asyncio.Lock()
        self.is_running = False
        self.emergency_stop_triggered = False
    
    async def start(self) -> None:
        """Start the risk coordinator"""
        self.logger.info("Starting Risk Coordinator...")
        self.is_running = True
        
        # Initialize all risk models
        await self._initialize_models()
        
        # Start monitoring loops
        asyncio.create_task(self._risk_monitoring_loop())
        asyncio.create_task(self._alert_processing_loop())
        asyncio.create_task(self._model_performance_loop())
    
    async def stop(self) -> None:
        """Stop the risk coordinator"""
        self.logger.info("Stopping Risk Coordinator...")
        self.is_running = False
    
    @synchronized
    async def evaluate_portfolio_risk(self, positions: List[Position]) -> RiskMetrics:
        """Evaluate overall portfolio risk"""
        # Calculate VaR
        var_95 = await self.var_calculator.calculate(positions, confidence=0.95)
        var_99 = await self.var_calculator.calculate(positions, confidence=0.99)
        
        # Calculate Expected Shortfall
        es = await self.expected_shortfall.calculate(positions)
        
        # Calculate drawdown
        current_drawdown = await self.drawdown_predictor.get_current_drawdown(positions)
        max_predicted_drawdown = await self.drawdown_predictor.predict_max_drawdown(positions)
        
        # Check correlations
        correlation_breakdown = await self.correlation_monitor.check_breakdown(positions)
        
        # Calculate concentration
        concentration = await self.concentration_risk.calculate(positions)
        
        # Create risk metrics
        metrics = RiskMetrics(
            var_95=var_95,
            var_99=var_99,
            expected_shortfall=es,
            current_drawdown=current_drawdown,
            max_drawdown=max_predicted_drawdown,
            concentration_score=concentration,
            correlation_stability=1.0 - correlation_breakdown,
            timestamp=datetime.utcnow()
        )
        
        return metrics
    
    @synchronized
    async def evaluate_order_risk(self, order: Order, current_positions: List[Position]) -> Tuple[bool, str]:
        """Evaluate risk of a new order"""
        # Check position limits
        limit_check = await self.limit_monitor.check_order(order, current_positions)
        if not limit_check[0]:
            return False, limit_check[1]
        
        # Check leverage impact
        leverage_check = await self.leverage_tracker.check_order_impact(order, current_positions)
        if not leverage_check[0]:
            return False, leverage_check[1]
        
        # Check concentration impact
        concentration_check = await self._check_concentration_impact(order, current_positions)
        if not concentration_check[0]:
            return False, concentration_check[1]
        
        # Check margin requirements
        margin_check = await self.margin_predictor.check_margin_sufficiency(order, current_positions)
        if not margin_check[0]:
            return False, margin_check[1]
        
        # Simulate portfolio with new order
        simulated_risk = await self._simulate_order_impact(order, current_positions)
        if simulated_risk > self.max_risk_budget:
            return False, f"Order would exceed risk budget: {simulated_risk:.2%} > {self.max_risk_budget:.2%}"
        
        return True, "Order approved"
    
    async def get_hedging_recommendations(self, positions: List[Position]) -> List[Order]:
        """Get hedging recommendations for current positions"""
        recommendations = []
        
        # Delta hedging
        delta_hedges = await self.delta_optimizer.get_hedges(positions)
        recommendations.extend(delta_hedges)
        
        # Volatility hedging
        vol_hedges = await self.volatility_manager.get_hedges(positions)
        recommendations.extend(vol_hedges)
        
        # Tail risk hedging
        tail_hedges = await self.tail_optimizer.get_hedges(positions)
        recommendations.extend(tail_hedges)
        
        # Beta adjustment
        beta_adjustments = await self.beta_adjuster.get_adjustments(positions)
        recommendations.extend(beta_adjustments)
        
        return recommendations
    
    async def handle_risk_alert(self, alert: RiskAlert) -> List[RiskAction]:
        """Handle a risk alert and determine actions"""
        self.logger.warning(f"Risk alert: {alert.message}")
        
        # Store alert
        self.active_alerts[alert.alert_id] = alert
        
        # Determine actions based on severity and type
        actions = []
        
        if alert.severity == RiskLevel.CRITICAL:
            if "drawdown" in alert.risk_type:
                actions.append(RiskAction.REDUCE_POSITION)
                if alert.metrics.get("drawdown", 0) > self.emergency_stop_threshold:
                    actions.append(RiskAction.EMERGENCY_STOP)
            
            elif "leverage" in alert.risk_type:
                actions.append(RiskAction.REDUCE_POSITION)
                actions.append(RiskAction.ADJUST_LIMITS)
            
            elif "correlation" in alert.risk_type:
                actions.append(RiskAction.HEDGE_POSITION)
                actions.append(RiskAction.REBALANCE)
            
            elif "flash_crash" in alert.risk_type:
                actions.append(RiskAction.HALT_TRADING)
                actions.append(RiskAction.CLOSE_POSITION)
        
        elif alert.severity == RiskLevel.HIGH:
            if "concentration" in alert.risk_type:
                actions.append(RiskAction.REBALANCE)
            elif "margin" in alert.risk_type:
                actions.append(RiskAction.REDUCE_POSITION)
            else:
                actions.append(RiskAction.HEDGE_POSITION)
        
        elif alert.severity == RiskLevel.MEDIUM:
            actions.append(RiskAction.ADJUST_LIMITS)
        
        # Execute actions
        await self._execute_risk_actions(actions, alert)
        
        return actions
    
    async def _execute_risk_actions(self, actions: List[RiskAction], alert: RiskAlert) -> None:
        """Execute risk management actions"""
        for action in actions:
            try:
                self.logger.info(f"Executing risk action: {action.value}")
                
                action_record = {
                    "timestamp": datetime.utcnow(),
                    "action": action.value,
                    "alert_id": alert.alert_id,
                    "alert_type": alert.risk_type,
                    "success": False,
                    "details": {}
                }
                
                if action == RiskAction.EMERGENCY_STOP:
                    await self._execute_emergency_stop()
                    action_record["success"] = True
                    
                elif action == RiskAction.HALT_TRADING:
                    await self._halt_trading()
                    action_record["success"] = True
                    
                elif action == RiskAction.REDUCE_POSITION:
                    reduced = await self._reduce_positions(alert.affected_positions)
                    action_record["success"] = reduced > 0
                    action_record["details"]["positions_reduced"] = reduced
                    
                elif action == RiskAction.HEDGE_POSITION:
                    hedges = await self._create_hedges(alert.affected_positions)
                    action_record["success"] = len(hedges) > 0
                    action_record["details"]["hedges_created"] = len(hedges)
                    
                elif action == RiskAction.CLOSE_POSITION:
                    closed = await self._close_positions(alert.affected_positions)
                    action_record["success"] = closed > 0
                    action_record["details"]["positions_closed"] = closed
                    
                elif action == RiskAction.ADJUST_LIMITS:
                    await self._adjust_risk_limits()
                    action_record["success"] = True
                    
                elif action == RiskAction.REBALANCE:
                    await self._rebalance_portfolio()
                    action_record["success"] = True
                
                self.risk_actions_taken.append(action_record)
                
            except Exception as e:
                self.logger.error(f"Error executing risk action {action.value}: {e}")
    
    async def _execute_emergency_stop(self) -> None:
        """Execute emergency stop - close all positions"""
        self.logger.critical("EMERGENCY STOP TRIGGERED")
        self.emergency_stop_triggered = True
        
        # Notify all components
        await self.risk_engine.trigger_emergency_stop()
        
        # Close all positions
        # This would interface with position manager
        
        # Halt all trading
        await self._halt_trading()
    
    async def _halt_trading(self) -> None:
        """Halt all trading activities"""
        self.logger.warning("Halting all trading activities")
        # This would interface with trading engine
    
    async def _reduce_positions(self, position_ids: List[str]) -> int:
        """Reduce specified positions"""
        reduced_count = 0
        # This would interface with position manager
        return reduced_count
    
    async def _create_hedges(self, position_ids: List[str]) -> List[Order]:
        """Create hedging orders"""
        hedges = []
        # This would interface with order manager
        return hedges
    
    async def _close_positions(self, position_ids: List[str]) -> int:
        """Close specified positions"""
        closed_count = 0
        # This would interface with position manager
        return closed_count
    
    async def _adjust_risk_limits(self) -> None:
        """Adjust risk limits based on current conditions"""
        # Reduce leverage limits during high volatility
        # Tighten position limits
        # Adjust risk budgets
        pass
    
    async def _rebalance_portfolio(self) -> None:
        """Rebalance portfolio to target allocations"""
        # This would interface with portfolio manager
        pass
    
    async def _risk_monitoring_loop(self) -> None:
        """Main risk monitoring loop"""
        while self.is_running:
            try:
                # Get current positions
                positions = await self._get_current_positions()
                
                # Calculate portfolio risk
                risk_metrics = await self.evaluate_portfolio_risk(positions)
                
                # Update risk state
                self.current_risk_state = RiskState(
                    timestamp=datetime.utcnow(),
                    portfolio_var_95=risk_metrics.var_95,
                    portfolio_var_99=risk_metrics.var_99,
                    expected_shortfall=risk_metrics.expected_shortfall,
                    current_drawdown=risk_metrics.current_drawdown,
                    max_drawdown=risk_metrics.max_drawdown,
                    risk_budget_used=risk_metrics.var_95 / self.max_risk_budget,
                    leverage_ratio=await self.leverage_tracker.get_current_leverage(),
                    concentration_score=risk_metrics.concentration_score,
                    correlation_stability=risk_metrics.correlation_stability,
                    volatility_regime=await self._get_volatility_regime(),
                    market_stress_level=await self._get_market_stress_level(),
                    model_performance_score=await self._get_model_performance_score(),
                    system_health_score=await self._get_system_health_score(),
                    active_alerts=list(self.active_alerts.values())
                )
                
                # Store history
                self.risk_history.append(self.current_risk_state)
                
                # Check for risk breaches
                await self._check_risk_breaches(risk_metrics)
                
                # Check for market stress
                await self._check_market_conditions()
                
                # Record metrics
                self._record_risk_metrics(risk_metrics)
                
                await asyncio.sleep(self.risk_check_interval)
                
            except Exception as e:
                self.logger.error(f"Error in risk monitoring: {e}")
                await asyncio.sleep(self.risk_check_interval)
    
    async def _alert_processing_loop(self) -> None:
        """Process risk alerts"""
        while self.is_running:
            try:
                # Check for flash crash conditions
                flash_risk = await self.flash_predictor.predict()
                if flash_risk > 0.7:
                    alert = RiskAlert(
                        alert_id=f"flash_{get_timestamp()}",
                        timestamp=datetime.utcnow(),
                        risk_type="flash_crash",
                        severity=RiskLevel.CRITICAL,
                        message=f"Flash crash risk detected: {flash_risk:.2%}",
                        recommended_actions=[RiskAction.HALT_TRADING]
                    )
                    await self.handle_risk_alert(alert)
                
                # Check circuit breaker conditions
                should_halt = await self.circuit_breaker.should_trigger()
                if should_halt:
                    alert = RiskAlert(
                        alert_id=f"circuit_{get_timestamp()}",
                        timestamp=datetime.utcnow(),
                        risk_type="circuit_breaker",
                        severity=RiskLevel.CRITICAL,
                        message="Circuit breaker triggered",
                        recommended_actions=[RiskAction.HALT_TRADING]
                    )
                    await self.handle_risk_alert(alert)
                
                # Process resolved alerts
                for alert_id, alert in list(self.active_alerts.items()):
                    if await self._is_alert_resolved(alert):
                        alert.resolved = True
                        alert.resolved_at = datetime.utcnow()
                        del self.active_alerts[alert_id]
                
                await asyncio.sleep(1)  # Check every second
                
            except Exception as e:
                self.logger.error(f"Error in alert processing: {e}")
                await asyncio.sleep(1)
    
    async def _model_performance_loop(self) -> None:
        """Monitor model performance"""
        while self.is_running:
            try:
                # Monitor all risk model performance
                # Detect model degradation
                # Adjust model weights if needed
                
                await asyncio.sleep(300)  # Check every 5 minutes
                
            except Exception as e:
                self.logger.error(f"Error in model performance monitoring: {e}")
                await asyncio.sleep(300)
    
    async def _initialize_models(self) -> None:
        """Initialize all risk models"""
        self.logger.info("Initializing risk models...")
        
        # Load model configurations
        # Initialize model states
        # Perform initial calculations
    
    async def _get_current_positions(self) -> List[Position]:
        """Get current positions from position manager"""
        # This would interface with position manager
        return []
    
    async def _simulate_order_impact(self, order: Order, positions: List[Position]) -> Decimal:
        """Simulate the risk impact of adding an order"""
        # Create simulated portfolio
        # Calculate new risk metrics
        # Return risk budget usage
        return Decimal("0")
    
    async def _check_concentration_impact(self, order: Order, positions: List[Position]) -> Tuple[bool, str]:
        """Check if order would create concentration risk"""
        # Calculate new concentration with order
        # Check against limits
        return True, "Concentration check passed"
    
    async def _check_risk_breaches(self, metrics: RiskMetrics) -> None:
        """Check for risk limit breaches"""
        # Check VaR limits
        if metrics.var_95 > self.max_risk_budget:
            alert = RiskAlert(
                alert_id=f"var_{get_timestamp()}",
                timestamp=datetime.utcnow(),
                risk_type="var_breach",
                severity=RiskLevel.HIGH,
                message=f"VaR breach: {metrics.var_95:.2%} > {self.max_risk_budget:.2%}",
                recommended_actions=[RiskAction.REDUCE_POSITION]
            )
            await self.handle_risk_alert(alert)
        
        # Check drawdown limits
        if metrics.current_drawdown > Decimal("0.03"):  # 3%
            alert = RiskAlert(
                alert_id=f"dd_{get_timestamp()}",
                timestamp=datetime.utcnow(),
                risk_type="drawdown",
                severity=RiskLevel.HIGH if metrics.current_drawdown < Decimal("0.05") else RiskLevel.CRITICAL,
                message=f"Drawdown alert: {metrics.current_drawdown:.2%}",
                metrics={"drawdown": float(metrics.current_drawdown)},
                recommended_actions=[RiskAction.REDUCE_POSITION]
            )
            await self.handle_risk_alert(alert)
    
    async def _check_market_conditions(self) -> None:
        """Check overall market conditions"""
        # Check volatility regime
        # Check correlation stability
        # Check liquidity conditions
        pass
    
    async def _get_volatility_regime(self) -> str:
        """Get current volatility regime"""
        # This would interface with volatility models
        return "normal"
    
    async def _get_market_stress_level(self) -> float:
        """Get current market stress level"""
        # This would aggregate various stress indicators
        return 0.3
    
    async def _get_model_performance_score(self) -> float:
        """Get aggregate model performance score"""
        # This would check all model performance
        return 0.95
    
    async def _get_system_health_score(self) -> float:
        """Get system health score"""
        # This would check system metrics
        return 0.98
    
    async def _is_alert_resolved(self, alert: RiskAlert) -> bool:
        """Check if an alert condition is resolved"""
        # Check if the condition that triggered the alert is resolved
        return False
    
    def _record_risk_metrics(self, metrics: RiskMetrics) -> None:
        """Record risk metrics"""
        self.metrics_collector.record_risk_metrics({
            "var_95": float(metrics.var_95),
            "var_99": float(metrics.var_99),
            "expected_shortfall": float(metrics.expected_shortfall),
            "current_drawdown": float(metrics.current_drawdown),
            "concentration_score": float(metrics.concentration_score),
            "correlation_stability": metrics.correlation_stability,
            "active_alerts": len(self.active_alerts),
            "risk_actions_taken": len(self.risk_actions_taken)
        })
    
    def get_current_risk_state(self) -> Optional[RiskState]:
        """Get current risk state"""
        return self.current_risk_state
    
    def get_risk_summary(self) -> Dict[str, Any]:
        """Get risk summary"""
        if not self.current_risk_state:
            return {"status": "initializing"}
        
        return {
            "timestamp": self.current_risk_state.timestamp.isoformat(),
            "risk_budget_used": f"{self.current_risk_state.risk_budget_used:.2%}",
            "leverage_ratio": f"{self.current_risk_state.leverage_ratio:.1f}x",
            "current_drawdown": f"{self.current_risk_state.current_drawdown:.2%}",
            "volatility_regime": self.current_risk_state.volatility_regime,
            "market_stress_level": f"{self.current_risk_state.market_stress_level:.2%}",
            "active_alerts": len(self.current_risk_state.active_alerts),
            "emergency_stop": self.emergency_stop_triggered,
            "overall_risk_level": self._calculate_overall_risk_level()
        }
    
    def _calculate_overall_risk_level(self) -> str:
        """Calculate overall risk level"""
        if not self.current_risk_state:
            return "unknown"
        
        if self.emergency_stop_triggered:
            return "emergency"
        
        if len([a for a in self.current_risk_state.active_alerts if a.severity == RiskLevel.CRITICAL]) > 0:
            return "critical"
        
        if self.current_risk_state.risk_budget_used > Decimal("0.8"):
            return "high"
        
        if self.current_risk_state.risk_budget_used > Decimal("0.5"):
            return "medium"
        
        return "low"