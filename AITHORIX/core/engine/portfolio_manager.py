"""
AITHORIX Portfolio Manager
Advanced portfolio management and optimization

This module handles:
- Portfolio construction and optimization
- Asset allocation and rebalancing
- Risk-adjusted position sizing
- Multi-strategy portfolio management
"""

import asyncio
import logging
from typing import Dict, List, Optional, Any, Set, Tuple
from datetime import datetime, timedelta
from decimal import Decimal
from dataclasses import dataclass, field
from enum import Enum
import numpy as np
import pandas as pd
from scipy.optimize import minimize
from collections import defaultdict, deque
import json

from core.engine.position_manager import Position, PositionManager
from core.engine.risk_engine import RiskMetrics
from utils.helpers import get_timestamp, synchronized
from monitoring.metrics import MetricsCollector


class AllocationMethod(Enum):
    """Portfolio allocation methods"""
    EQUAL_WEIGHT = "equal_weight"
    RISK_PARITY = "risk_parity"
    MEAN_VARIANCE = "mean_variance"
    KELLY = "kelly"
    BLACK_LITTERMAN = "black_litterman"
    HIERARCHICAL_RISK_PARITY = "hrp"


class RebalanceFrequency(Enum):
    """Rebalancing frequency options"""
    CONTINUOUS = "continuous"
    DAILY = "daily"
    WEEKLY = "weekly"
    MONTHLY = "monthly"
    THRESHOLD = "threshold"


@dataclass
class AssetAllocation:
    """Asset allocation target"""
    symbol: str
    target_weight: Decimal
    current_weight: Decimal
    deviation: Decimal
    recommended_action: str  # "BUY", "SELL", "HOLD"
    recommended_quantity: Decimal
    
    @property
    def needs_rebalance(self) -> bool:
        """Check if asset needs rebalancing"""
        return abs(self.deviation) > Decimal("0.05")  # 5% threshold


@dataclass
class PortfolioMetrics:
    """Portfolio performance metrics"""
    timestamp: datetime
    
    # Portfolio value
    total_value: Decimal
    cash_balance: Decimal
    invested_value: Decimal
    
    # Returns
    daily_return: Decimal
    cumulative_return: Decimal
    
    # Risk metrics
    volatility: Decimal
    sharpe_ratio: float
    sortino_ratio: float
    max_drawdown: Decimal
    current_drawdown: Decimal
    
    # Risk-adjusted metrics
    calmar_ratio: float
    information_ratio: float
    treynor_ratio: float
    
    # Diversification
    effective_assets: float  # Effective number of assets
    concentration_ratio: Decimal
    correlation_risk: float
    
    # Efficiency
    turnover_rate: Decimal
    transaction_costs: Decimal
    
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class PortfolioConstraints:
    """Portfolio optimization constraints"""
    # Position limits
    max_position_size: Decimal = Decimal("0.20")  # 20% max per position
    min_position_size: Decimal = Decimal("0.01")  # 1% min per position
    max_positions: int = 50
    
    # Sector/asset limits
    max_sector_weight: Decimal = Decimal("0.40")  # 40% max per sector
    max_correlated_weight: Decimal = Decimal("0.60")  # 60% max for correlated assets
    
    # Risk limits
    max_portfolio_volatility: Decimal = Decimal("0.20")  # 20% annual volatility
    max_portfolio_beta: Decimal = Decimal("1.5")
    max_concentration: Decimal = Decimal("0.70")  # HHI concentration
    
    # Trading limits
    max_daily_turnover: Decimal = Decimal("0.50")  # 50% daily turnover
    min_rebalance_amount: Decimal = Decimal("100")  # Min $100 per trade
    
    # Leverage
    max_leverage: Decimal = Decimal("2.0")
    
    # Custom constraints
    custom_constraints: List[Dict[str, Any]] = field(default_factory=list)


class PortfolioManager:
    """
    Advanced portfolio management system
    
    Handles portfolio construction, optimization, and rebalancing
    with multiple allocation strategies and risk management.
    """
    
    def __init__(self, config: Dict[str, Any], position_manager: PositionManager):
        self.config = config
        self.position_manager = position_manager
        self.logger = logging.getLogger("AITHORIX.PortfolioManager")
        
        # Portfolio configuration
        self.allocation_method = AllocationMethod(config.get("allocation_method", "risk_parity"))
        self.rebalance_frequency = RebalanceFrequency(config.get("rebalance_frequency", "daily"))
        self.constraints = self._load_constraints(config.get("constraints", {}))
        
        # Target allocations
        self.target_allocations: Dict[str, Decimal] = {}
        self.current_allocations: Dict[str, Decimal] = {}
        
        # Historical data for optimization
        self.returns_history: Dict[str, deque] = defaultdict(lambda: deque(maxlen=252))
        self.correlation_matrix: Optional[pd.DataFrame] = None
        self.covariance_matrix: Optional[pd.DataFrame] = None
        
        # Portfolio metrics
        self.portfolio_metrics: deque = deque(maxlen=1000)
        self.metrics_collector = MetricsCollector()
        
        # Rebalancing
        self.last_rebalance: Optional[datetime] = None
        self.rebalance_threshold = Decimal(str(config.get("rebalance_threshold", "0.05")))
        self.min_trade_size = Decimal(str(config.get("min_trade_size", "100")))
        
        # Strategy allocations
        self.strategy_allocations: Dict[str, Decimal] = {}
        self.strategy_performance: Dict[str, deque] = defaultdict(lambda: deque(maxlen=100))
        
        # State
        self._lock = asyncio.Lock()
        self.is_initialized = False
        
        # Optimization parameters
        self.lookback_period = config.get("lookback_period", 60)  # days
        self.risk_free_rate = Decimal(str(config.get("risk_free_rate", "0.02")))  # 2% annual
        
    async def initialize(self) -> None:
        """Initialize the portfolio manager"""
        self.logger.info("Initializing Portfolio Manager...")
        
        # Load historical data
        await self._load_historical_data()
        
        # Calculate initial allocations
        await self._calculate_current_allocations()
        
        # Start monitoring tasks
        asyncio.create_task(self._portfolio_monitoring_loop())
        asyncio.create_task(self._rebalancing_loop())
        asyncio.create_task(self._metrics_calculation_loop())
        
        self.is_initialized = True
        self.logger.info("Portfolio Manager initialized")
    
    @synchronized
    async def optimize_portfolio(
        self,
        available_assets: List[str],
        market_conditions: Dict[str, Any]
    ) -> Dict[str, AssetAllocation]:
        """Optimize portfolio allocation"""
        self.logger.info(f"Optimizing portfolio using {self.allocation_method.value} method")
        
        # Filter assets based on data availability
        valid_assets = self._filter_valid_assets(available_assets)
        
        if not valid_assets:
            self.logger.warning("No valid assets for optimization")
            return {}
        
        # Calculate optimization inputs
        returns_df = self._prepare_returns_data(valid_assets)
        
        if returns_df.empty or len(returns_df) < self.lookback_period:
            self.logger.warning("Insufficient data for optimization")
            return self._get_equal_weight_allocation(valid_assets)
        
        # Update correlation and covariance matrices
        self.correlation_matrix = returns_df.corr()
        self.covariance_matrix = returns_df.cov() * 252  # Annualized
        
        # Optimize based on method
        if self.allocation_method == AllocationMethod.EQUAL_WEIGHT:
            optimal_weights = self._optimize_equal_weight(valid_assets)
        elif self.allocation_method == AllocationMethod.RISK_PARITY:
            optimal_weights = self._optimize_risk_parity(valid_assets, returns_df)
        elif self.allocation_method == AllocationMethod.MEAN_VARIANCE:
            optimal_weights = self._optimize_mean_variance(valid_assets, returns_df)
        elif self.allocation_method == AllocationMethod.KELLY:
            optimal_weights = self._optimize_kelly(valid_assets, returns_df)
        elif self.allocation_method == AllocationMethod.BLACK_LITTERMAN:
            optimal_weights = self._optimize_black_litterman(valid_assets, returns_df, market_conditions)
        elif self.allocation_method == AllocationMethod.HIERARCHICAL_RISK_PARITY:
            optimal_weights = self._optimize_hrp(valid_assets, returns_df)
        else:
            optimal_weights = self._optimize_equal_weight(valid_assets)
        
        # Apply constraints
        optimal_weights = self._apply_constraints(optimal_weights)
        
        # Create allocation objects
        allocations = await self._create_allocations(optimal_weights)
        
        # Store target allocations
        self.target_allocations = {a.symbol: a.target_weight for a in allocations.values()}
        
        return allocations
    
    async def rebalance_portfolio(self, allocations: Dict[str, AssetAllocation]) -> List[Dict[str, Any]]:
        """Execute portfolio rebalancing"""
        rebalance_orders = []
        
        # Calculate total portfolio value
        portfolio_value = await self._calculate_portfolio_value()
        
        if portfolio_value <= 0:
            self.logger.warning("Portfolio value is zero, cannot rebalance")
            return []
        
        # Check if rebalancing is needed
        if not self._should_rebalance(allocations):
            self.logger.info("Portfolio within tolerance, no rebalancing needed")
            return []
        
        # Calculate rebalancing trades
        for symbol, allocation in allocations.items():
            if not allocation.needs_rebalance:
                continue
            
            # Calculate trade size
            target_value = portfolio_value * allocation.target_weight
            current_value = portfolio_value * allocation.current_weight
            trade_value = target_value - current_value
            
            # Check minimum trade size
            if abs(trade_value) < self.min_trade_size:
                continue
            
            # Create rebalancing order
            order = {
                "symbol": symbol,
                "side": "BUY" if trade_value > 0 else "SELL",
                "quantity": abs(allocation.recommended_quantity),
                "value": abs(trade_value),
                "reason": "portfolio_rebalance",
                "target_weight": float(allocation.target_weight),
                "current_weight": float(allocation.current_weight)
            }
            
            rebalance_orders.append(order)
        
        # Log rebalancing
        self.logger.info(f"Generated {len(rebalance_orders)} rebalancing orders")
        
        # Update last rebalance time
        self.last_rebalance = datetime.utcnow()
        
        return rebalance_orders
    
    async def update_strategy_allocation(
        self,
        strategy_id: str,
        allocation: Decimal,
        performance: Optional[float] = None
    ) -> None:
        """Update allocation for a specific strategy"""
        self.strategy_allocations[strategy_id] = allocation
        
        if performance is not None:
            self.strategy_performance[strategy_id].append({
                "timestamp": datetime.utcnow(),
                "performance": performance,
                "allocation": float(allocation)
            })
    
    async def calculate_portfolio_metrics(self) -> PortfolioMetrics:
        """Calculate comprehensive portfolio metrics"""
        # Get current positions
        positions = self.position_manager.get_open_positions()
        
        # Calculate values
        portfolio_value = await self._calculate_portfolio_value()
        cash_balance = await self._get_cash_balance()
        invested_value = portfolio_value - cash_balance
        
        # Calculate returns
        returns_data = self._calculate_portfolio_returns()
        daily_return = returns_data["daily_return"]
        cumulative_return = returns_data["cumulative_return"]
        
        # Calculate risk metrics
        risk_metrics = self._calculate_risk_metrics(returns_data["returns_series"])
        
        # Calculate diversification metrics
        diversification = self._calculate_diversification_metrics(positions)
        
        # Create metrics object
        metrics = PortfolioMetrics(
            timestamp=datetime.utcnow(),
            total_value=portfolio_value,
            cash_balance=cash_balance,
            invested_value=invested_value,
            daily_return=daily_return,
            cumulative_return=cumulative_return,
            volatility=risk_metrics["volatility"],
            sharpe_ratio=risk_metrics["sharpe_ratio"],
            sortino_ratio=risk_metrics["sortino_ratio"],
            max_drawdown=risk_metrics["max_drawdown"],
            current_drawdown=risk_metrics["current_drawdown"],
            calmar_ratio=risk_metrics["calmar_ratio"],
            information_ratio=risk_metrics["information_ratio"],
            treynor_ratio=risk_metrics["treynor_ratio"],
            effective_assets=diversification["effective_assets"],
            concentration_ratio=diversification["concentration_ratio"],
            correlation_risk=diversification["correlation_risk"],
            turnover_rate=await self._calculate_turnover_rate(),
            transaction_costs=await self._calculate_transaction_costs()
        )
        
        # Store metrics
        self.portfolio_metrics.append(metrics)
        
        # Record in metrics collector
        self.metrics_collector.record_portfolio_metrics(metrics)
        
        return metrics
    
    def _optimize_equal_weight(self, assets: List[str]) -> Dict[str, Decimal]:
        """Equal weight optimization"""
        weight = Decimal("1") / Decimal(str(len(assets)))
        return {asset: weight for asset in assets}
    
    def _optimize_risk_parity(self, assets: List[str], returns_df: pd.DataFrame) -> Dict[str, Decimal]:
        """Risk parity optimization"""
        n_assets = len(assets)
        
        # Calculate asset volatilities
        volatilities = returns_df.std() * np.sqrt(252)
        
        # Initial guess: inverse volatility weighting
        inv_vols = 1 / volatilities
        initial_weights = inv_vols / inv_vols.sum()
        
        # Optimization objective: equal risk contribution
        def risk_parity_objective(weights):
            portfolio_vol = np.sqrt(weights @ self.covariance_matrix @ weights)
            marginal_contrib = self.covariance_matrix @ weights
            contrib = weights * marginal_contrib / portfolio_vol
            
            # Minimize squared differences from equal contribution
            target_contrib = portfolio_vol / n_assets
            return np.sum((contrib - target_contrib) ** 2)
        
        # Constraints
        constraints = [
            {'type': 'eq', 'fun': lambda w: np.sum(w) - 1},  # Sum to 1
        ]
        
        bounds = [(0, float(self.constraints.max_position_size)) for _ in range(n_assets)]
        
        # Optimize
        result = minimize(
            risk_parity_objective,
            initial_weights,
            method='SLSQP',
            bounds=bounds,
            constraints=constraints
        )
        
        if result.success:
            weights = result.x
        else:
            self.logger.warning("Risk parity optimization failed, using inverse volatility")
            weights = initial_weights
        
        return {asset: Decimal(str(w)) for asset, w in zip(assets, weights)}
    
    def _optimize_mean_variance(self, assets: List[str], returns_df: pd.DataFrame) -> Dict[str, Decimal]:
        """Mean-variance optimization (Markowitz)"""
        n_assets = len(assets)
        
        # Calculate expected returns (using historical mean)
        expected_returns = returns_df.mean() * 252
        
        # Target return (e.g., 15% annual)
        target_return = 0.15
        
        # Optimization objective: minimize portfolio variance
        def portfolio_variance(weights):
            return weights @ self.covariance_matrix @ weights
        
        # Constraints
        constraints = [
            {'type': 'eq', 'fun': lambda w: np.sum(w) - 1},  # Sum to 1
            {'type': 'ineq', 'fun': lambda w: w @ expected_returns - target_return}  # Min return
        ]
        
        bounds = [(0, float(self.constraints.max_position_size)) for _ in range(n_assets)]
        
        # Initial guess
        initial_weights = np.ones(n_assets) / n_assets
        
        # Optimize
        result = minimize(
            portfolio_variance,
            initial_weights,
            method='SLSQP',
            bounds=bounds,
            constraints=constraints
        )
        
        if result.success:
            weights = result.x
        else:
            self.logger.warning("Mean-variance optimization failed, using equal weights")
            weights = initial_weights
        
        return {asset: Decimal(str(w)) for asset, w in zip(assets, weights)}
    
    def _optimize_kelly(self, assets: List[str], returns_df: pd.DataFrame) -> Dict[str, Decimal]:
        """Kelly criterion optimization"""
        # Calculate Kelly fractions for each asset
        kelly_fractions = {}
        
        for asset in assets:
            returns = returns_df[asset].values
            
            # Calculate mean and variance
            mean_return = np.mean(returns)
            variance = np.var(returns)
            
            if variance > 0:
                # Kelly fraction = mean / variance
                kelly = mean_return / variance
                
                # Apply Kelly fraction scaling (e.g., 25% of full Kelly)
                kelly_scaled = kelly * 0.25
                
                # Bound between 0 and max position size
                kelly_bounded = max(0, min(kelly_scaled, float(self.constraints.max_position_size)))
            else:
                kelly_bounded = 0
            
            kelly_fractions[asset] = kelly_bounded
        
        # Normalize to sum to 1
        total_kelly = sum(kelly_fractions.values())
        if total_kelly > 0:
            weights = {asset: Decimal(str(k / total_kelly)) for asset, k in kelly_fractions.items()}
        else:
            weights = self._optimize_equal_weight(assets)
        
        return weights
    
    def _optimize_black_litterman(
        self,
        assets: List[str],
        returns_df: pd.DataFrame,
        market_conditions: Dict[str, Any]
    ) -> Dict[str, Decimal]:
        """Black-Litterman optimization"""
        # This is a simplified implementation
        # In production, would incorporate market views and equilibrium returns
        
        # Start with market cap weights (simplified: use equal weights as proxy)
        market_weights = np.ones(len(assets)) / len(assets)
        
        # Calculate equilibrium returns
        risk_aversion = 2.5  # Risk aversion parameter
        equilibrium_returns = risk_aversion * self.covariance_matrix @ market_weights
        
        # Incorporate views (simplified: no views for now)
        # In production, would use model predictions as views
        
        # For now, return market weights
        return {asset: Decimal(str(w)) for asset, w in zip(assets, market_weights)}
    
    def _optimize_hrp(self, assets: List[str], returns_df: pd.DataFrame) -> Dict[str, Decimal]:
        """Hierarchical Risk Parity optimization"""
        # Simplified HRP implementation
        # In production, would use full clustering algorithm
        
        # For now, use risk parity as approximation
        return self._optimize_risk_parity(assets, returns_df)
    
    def _apply_constraints(self, weights: Dict[str, Decimal]) -> Dict[str, Decimal]:
        """Apply portfolio constraints to weights"""
        # Apply position size constraints
        for asset, weight in weights.items():
            weights[asset] = max(
                self.constraints.min_position_size,
                min(weight, self.constraints.max_position_size)
            )
        
        # Renormalize after constraints
        total_weight = sum(weights.values())
        if total_weight > 0:
            weights = {asset: w / total_weight for asset, w in weights.items()}
        
        return weights
    
    async def _create_allocations(self, target_weights: Dict[str, Decimal]) -> Dict[str, AssetAllocation]:
        """Create allocation objects with current weights and recommendations"""
        allocations = {}
        portfolio_value = await self._calculate_portfolio_value()
        
        # Calculate current allocations
        await self._calculate_current_allocations()
        
        for symbol, target_weight in target_weights.items():
            current_weight = self.current_allocations.get(symbol, Decimal("0"))
            deviation = target_weight - current_weight
            
            # Determine action
            if deviation > self.rebalance_threshold:
                action = "BUY"
            elif deviation < -self.rebalance_threshold:
                action = "SELL"
            else:
                action = "HOLD"
            
            # Calculate quantity (simplified - would need current price)
            if action != "HOLD":
                trade_value = portfolio_value * abs(deviation)
                # Would calculate actual quantity based on current price
                recommended_quantity = trade_value / Decimal("50000")  # Placeholder
            else:
                recommended_quantity = Decimal("0")
            
            allocations[symbol] = AssetAllocation(
                symbol=symbol,
                target_weight=target_weight,
                current_weight=current_weight,
                deviation=deviation,
                recommended_action=action,
                recommended_quantity=recommended_quantity
            )
        
        return allocations
    
    async def _calculate_current_allocations(self) -> None:
        """Calculate current portfolio allocations"""
        positions = self.position_manager.get_open_positions()
        portfolio_value = await self._calculate_portfolio_value()
        
        if portfolio_value <= 0:
            self.current_allocations = {}
            return
        
        allocations = {}
        for position in positions:
            weight = position.position_value / portfolio_value
            allocations[position.symbol] = weight
        
        self.current_allocations = allocations
    
    async def _calculate_portfolio_value(self) -> Decimal:
        """Calculate total portfolio value"""
        positions = self.position_manager.get_open_positions()
        total_value = sum(p.position_value for p in positions)
        cash_balance = await self._get_cash_balance()
        return total_value + cash_balance
    
    async def _get_cash_balance(self) -> Decimal:
        """Get current cash balance"""
        # This would interface with exchange APIs
        # For now, return placeholder
        return Decimal("10000")
    
    def _calculate_portfolio_returns(self) -> Dict[str, Any]:
        """Calculate portfolio returns"""
        if len(self.portfolio_metrics) < 2:
            return {
                "daily_return": Decimal("0"),
                "cumulative_return": Decimal("0"),
                "returns_series": []
            }
        
        # Get recent metrics
        current = self.portfolio_metrics[-1]
        previous = self.portfolio_metrics[-2]
        
        # Calculate daily return
        if previous.total_value > 0:
            daily_return = (current.total_value - previous.total_value) / previous.total_value
        else:
            daily_return = Decimal("0")
        
        # Calculate cumulative return
        if self.portfolio_metrics:
            initial_value = self.portfolio_metrics[0].total_value
            if initial_value > 0:
                cumulative_return = (current.total_value - initial_value) / initial_value
            else:
                cumulative_return = Decimal("0")
        else:
            cumulative_return = Decimal("0")
        
        # Extract returns series
        returns_series = []
        for i in range(1, len(self.portfolio_metrics)):
            prev_value = self.portfolio_metrics[i-1].total_value
            curr_value = self.portfolio_metrics[i].total_value
            if prev_value > 0:
                ret = float((curr_value - prev_value) / prev_value)
                returns_series.append(ret)
        
        return {
            "daily_return": daily_return,
            "cumulative_return": cumulative_return,
            "returns_series": returns_series
        }
    
    def _calculate_risk_metrics(self, returns: List[float]) -> Dict[str, Any]:
        """Calculate risk metrics from returns series"""
        if not returns or len(returns) < 2:
            return {
                "volatility": Decimal("0"),
                "sharpe_ratio": 0.0,
                "sortino_ratio": 0.0,
                "max_drawdown": Decimal("0"),
                "current_drawdown": Decimal("0"),
                "calmar_ratio": 0.0,
                "information_ratio": 0.0,
                "treynor_ratio": 0.0
            }
        
        returns_array = np.array(returns)
        
        # Volatility (annualized)
        volatility = Decimal(str(np.std(returns_array) * np.sqrt(252)))
        
        # Sharpe ratio
        excess_returns = returns_array - float(self.risk_free_rate) / 252
        if np.std(excess_returns) > 0:
            sharpe_ratio = np.mean(excess_returns) / np.std(excess_returns) * np.sqrt(252)
        else:
            sharpe_ratio = 0.0
        
        # Sortino ratio (downside deviation)
        downside_returns = returns_array[returns_array < 0]
        if len(downside_returns) > 0 and np.std(downside_returns) > 0:
            sortino_ratio = np.mean(returns_array) / np.std(downside_returns) * np.sqrt(252)
        else:
            sortino_ratio = 0.0
        
        # Drawdown calculation
        cumulative = (1 + returns_array).cumprod()
        running_max = np.maximum.accumulate(cumulative)
        drawdown = (cumulative - running_max) / running_max
        max_drawdown = Decimal(str(abs(np.min(drawdown))))
        current_drawdown = Decimal(str(abs(drawdown[-1])))
        
        # Calmar ratio
        annual_return = np.mean(returns_array) * 252
        if max_drawdown > 0:
            calmar_ratio = float(annual_return / float(max_drawdown))
        else:
            calmar_ratio = 0.0
        
        # Information ratio (would need benchmark)
        information_ratio = sharpe_ratio  # Simplified
        
        # Treynor ratio (would need beta)
        treynor_ratio = sharpe_ratio  # Simplified
        
        return {
            "volatility": volatility,
            "sharpe_ratio": sharpe_ratio,
            "sortino_ratio": sortino_ratio,
            "max_drawdown": max_drawdown,
            "current_drawdown": current_drawdown,
            "calmar_ratio": calmar_ratio,
            "information_ratio": information_ratio,
            "treynor_ratio": treynor_ratio
        }
    
    def _calculate_diversification_metrics(self, positions: List[Position]) -> Dict[str, Any]:
        """Calculate portfolio diversification metrics"""
        if not positions:
            return {
                "effective_assets": 0.0,
                "concentration_ratio": Decimal("0"),
                "correlation_risk": 0.0
            }
        
        # Calculate position weights
        total_value = sum(p.position_value for p in positions)
        if total_value <= 0:
            return {
                "effective_assets": 0.0,
                "concentration_ratio": Decimal("0"),
                "correlation_risk": 0.0
            }
        
        weights = [float(p.position_value / total_value) for p in positions]
        weights_array = np.array(weights)
        
        # Effective number of assets (inverse HHI)
        hhi = np.sum(weights_array ** 2)
        effective_assets = 1 / hhi if hhi > 0 else 0
        
        # Concentration ratio (top 5 positions)
        sorted_weights = sorted(weights, reverse=True)
        concentration_ratio = Decimal(str(sum(sorted_weights[:5])))
        
        # Correlation risk (average pairwise correlation)
        if self.correlation_matrix is not None and len(positions) > 1:
            # Get correlations for position symbols
            symbols = [p.symbol for p in positions]
            valid_symbols = [s for s in symbols if s in self.correlation_matrix.index]
            
            if len(valid_symbols) > 1:
                corr_subset = self.correlation_matrix.loc[valid_symbols, valid_symbols]
                # Average off-diagonal correlations
                mask = np.ones(corr_subset.shape, dtype=bool)
                np.fill_diagonal(mask, 0)
                avg_correlation = corr_subset.values[mask].mean()
                correlation_risk = float(avg_correlation)
            else:
                correlation_risk = 0.0
        else:
            correlation_risk = 0.0
        
        return {
            "effective_assets": effective_assets,
            "concentration_ratio": concentration_ratio,
            "correlation_risk": correlation_risk
        }
    
    async def _calculate_turnover_rate(self) -> Decimal:
        """Calculate portfolio turnover rate"""
        # This would calculate actual turnover from trade history
        # For now, return placeholder
        return Decimal("0.10")  # 10% daily turnover
    
    async def _calculate_transaction_costs(self) -> Decimal:
        """Calculate transaction costs"""
        # This would sum actual transaction costs
        # For now, return placeholder
        return Decimal("50")  # $50 in transaction costs
    
    def _should_rebalance(self, allocations: Dict[str, AssetAllocation]) -> bool:
        """Determine if portfolio needs rebalancing"""
        # Check frequency-based rebalancing
        if self.rebalance_frequency == RebalanceFrequency.CONTINUOUS:
            return True
        
        elif self.rebalance_frequency == RebalanceFrequency.DAILY:
            if self.last_rebalance:
                return (datetime.utcnow() - self.last_rebalance) > timedelta(days=1)
            return True
        
        elif self.rebalance_frequency == RebalanceFrequency.WEEKLY:
            if self.last_rebalance:
                return (datetime.utcnow() - self.last_rebalance) > timedelta(days=7)
            return True
        
        elif self.rebalance_frequency == RebalanceFrequency.MONTHLY:
            if self.last_rebalance:
                return (datetime.utcnow() - self.last_rebalance) > timedelta(days=30)
            return True
        
        elif self.rebalance_frequency == RebalanceFrequency.THRESHOLD:
            # Check if any allocation exceeds threshold
            return any(a.needs_rebalance for a in allocations.values())
        
        return False
    
    def _filter_valid_assets(self, assets: List[str]) -> List[str]:
        """Filter assets with sufficient data for optimization"""
        valid_assets = []
        
        for asset in assets:
            if asset in self.returns_history and len(self.returns_history[asset]) >= self.lookback_period:
                valid_assets.append(asset)
        
        return valid_assets
    
    def _prepare_returns_data(self, assets: List[str]) -> pd.DataFrame:
        """Prepare returns data for optimization"""
        returns_dict = {}
        
        for asset in assets:
            if asset in self.returns_history:
                returns_dict[asset] = list(self.returns_history[asset])
        
        if not returns_dict:
            return pd.DataFrame()
        
        # Create DataFrame
        returns_df = pd.DataFrame(returns_dict)
        
        # Ensure all series have same length
        min_length = min(len(col) for col in returns_df.columns)
        returns_df = returns_df.iloc[-min_length:]
        
        return returns_df
    
    def _get_equal_weight_allocation(self, assets: List[str]) -> Dict[str, AssetAllocation]:
        """Get equal weight allocation as fallback"""
        weight = Decimal("1") / Decimal(str(len(assets)))
        allocations = {}
        
        for asset in assets:
            allocations[asset] = AssetAllocation(
                symbol=asset,
                target_weight=weight,
                current_weight=self.current_allocations.get(asset, Decimal("0")),
                deviation=weight - self.current_allocations.get(asset, Decimal("0")),
                recommended_action="HOLD",
                recommended_quantity=Decimal("0")
            )
        
        return allocations
    
    def _load_constraints(self, config: Dict[str, Any]) -> PortfolioConstraints:
        """Load portfolio constraints from configuration"""
        return PortfolioConstraints(
            max_position_size=Decimal(str(config.get("max_position_size", "0.20"))),
            min_position_size=Decimal(str(config.get("min_position_size", "0.01"))),
            max_positions=config.get("max_positions", 50),
            max_sector_weight=Decimal(str(config.get("max_sector_weight", "0.40"))),
            max_correlated_weight=Decimal(str(config.get("max_correlated_weight", "0.60"))),
            max_portfolio_volatility=Decimal(str(config.get("max_portfolio_volatility", "0.20"))),
            max_portfolio_beta=Decimal(str(config.get("max_portfolio_beta", "1.5"))),
            max_concentration=Decimal(str(config.get("max_concentration", "0.70"))),
            max_daily_turnover=Decimal(str(config.get("max_daily_turnover", "0.50"))),
            min_rebalance_amount=Decimal(str(config.get("min_rebalance_amount", "100"))),
            max_leverage=Decimal(str(config.get("max_leverage", "2.0"))),
            custom_constraints=config.get("custom_constraints", [])
        )
    
    async def _load_historical_data(self) -> None:
        """Load historical returns data"""
        # This would load actual historical data
        # For now, generate synthetic data
        symbols = ["BTC", "ETH", "BNB", "SOL", "ADA"]
        
        for symbol in symbols:
            # Generate synthetic returns
            returns = np.random.normal(0.001, 0.02, self.lookback_period)
            for ret in returns:
                self.returns_history[symbol].append(ret)
    
    async def _portfolio_monitoring_loop(self) -> None:
        """Monitor portfolio health and performance"""
        while True:
            try:
                # Calculate and log portfolio metrics
                metrics = await self.calculate_portfolio_metrics()
                
                # Check for alerts
                if metrics.current_drawdown > Decimal("0.10"):
                    self.logger.warning(f"High drawdown detected: {metrics.current_drawdown:.2%}")
                
                if metrics.concentration_ratio > Decimal("0.50"):
                    self.logger.warning(f"High concentration detected: {metrics.concentration_ratio:.2%}")
                
                await asyncio.sleep(60)  # Every minute
                
            except Exception as e:
                self.logger.error(f"Error in portfolio monitoring: {e}")
                await asyncio.sleep(60)
    
    async def _rebalancing_loop(self) -> None:
        """Periodic portfolio rebalancing"""
        while True:
            try:
                # Wait for appropriate interval
                if self.rebalance_frequency == RebalanceFrequency.DAILY:
                    await asyncio.sleep(86400)  # 24 hours
                elif self.rebalance_frequency == RebalanceFrequency.WEEKLY:
                    await asyncio.sleep(604800)  # 7 days
                elif self.rebalance_frequency == RebalanceFrequency.MONTHLY:
                    await asyncio.sleep(2592000)  # 30 days
                else:
                    await asyncio.sleep(3600)  # 1 hour for continuous/threshold
                
                # Check if rebalancing is needed
                # This would trigger actual rebalancing logic
                
            except Exception as e:
                self.logger.error(f"Error in rebalancing loop: {e}")
                await asyncio.sleep(3600)
    
    async def _metrics_calculation_loop(self) -> None:
        """Calculate portfolio metrics periodically"""
        while True:
            try:
                # Update returns history for all positions
                positions = self.position_manager.get_open_positions()
                
                for position in positions:
                    if position.symbol not in self.returns_history:
                        self.returns_history[position.symbol] = deque(maxlen=252)
                    
                    # Calculate position return
                    if position.entry_price > 0:
                        position_return = float(
                            (position.current_price - position.entry_price) / position.entry_price
                        )
                        self.returns_history[position.symbol].append(position_return)
                
                await asyncio.sleep(300)  # Every 5 minutes
                
            except Exception as e:
                self.logger.error(f"Error in metrics calculation: {e}")
                await asyncio.sleep(300)
    
    def get_portfolio_summary(self) -> Dict[str, Any]:
        """Get portfolio summary statistics"""
        recent_metrics = self.portfolio_metrics[-1] if self.portfolio_metrics else None
        
        return {
            "total_value": float(recent_metrics.total_value) if recent_metrics else 0,
            "allocation_method": self.allocation_method.value,
            "rebalance_frequency": self.rebalance_frequency.value,
            "current_allocations": {
                symbol: float(weight)
                for symbol, weight in self.current_allocations.items()
            },
            "target_allocations": {
                symbol: float(weight)
                for symbol, weight in self.target_allocations.items()
            },
            "performance": {
                "daily_return": float(recent_metrics.daily_return) if recent_metrics else 0,
                "sharpe_ratio": recent_metrics.sharpe_ratio if recent_metrics else 0,
                "max_drawdown": float(recent_metrics.max_drawdown) if recent_metrics else 0
            }
        }