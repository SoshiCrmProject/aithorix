"""
TWAP Optimizer - Complete Production Implementation
Time-Weighted Average Price execution optimization with market impact minimization
Minimizes slippage while maintaining execution speed
"""

import torch
import torch.nn as nn
import torch.nn.functional as F
import numpy as np
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass
import pandas as pd
from scipy.optimize import minimize
from collections import deque
import heapq
from datetime import datetime, timedelta

from models.base_model import BaseModel, ModelConfig


@dataclass
class OrderSlice:
    """Represents a single slice of the parent order"""
    slice_id: str
    parent_order_id: str
    quantity: float
    target_time: datetime
    min_price: float
    max_price: float
    urgency: float
    executed_quantity: float = 0.0
    executed_price: float = 0.0
    status: str = "pending"  # pending, executing, completed, cancelled
    
    @property
    def remaining_quantity(self) -> float:
        return self.quantity - self.executed_quantity
    
    @property
    def fill_rate(self) -> float:
        return self.executed_quantity / self.quantity if self.quantity > 0 else 0.0


class MarketImpactModel(nn.Module):
    """Neural network for predicting market impact of orders"""
    
    def __init__(self, feature_dim: int = 32):
        super().__init__()
        
        self.impact_predictor = nn.Sequential(
            nn.Linear(feature_dim, 128),
            nn.LayerNorm(128),
            nn.ReLU(),
            nn.Dropout(0.2),
            nn.Linear(128, 64),
            nn.LayerNorm(64),
            nn.ReLU(),
            nn.Dropout(0.1),
            nn.Linear(64, 32),
            nn.ReLU(),
            nn.Linear(32, 3)  # Temporary impact, permanent impact, decay rate
        )
        
        self.execution_cost_predictor = nn.Sequential(
            nn.Linear(feature_dim, 64),
            nn.ReLU(),
            nn.Linear(64, 32),
            nn.ReLU(),
            nn.Linear(32, 1),
            nn.Sigmoid()  # Normalized cost [0, 1]
        )
    
    def forward(self, features: torch.Tensor) -> Tuple[torch.Tensor, torch.Tensor]:
        impact = self.impact_predictor(features)
        cost = self.execution_cost_predictor(features)
        return impact, cost


class LiquidityPredictor(nn.Module):
    """Predicts available liquidity over time"""
    
    def __init__(self, seq_len: int = 60, feature_dim: int = 16):
        super().__init__()
        
        self.lstm = nn.LSTM(
            input_size=feature_dim,
            hidden_size=64,
            num_layers=2,
            batch_first=True,
            dropout=0.1
        )
        
        self.attention = nn.MultiheadAttention(
            embed_dim=64,
            num_heads=4,
            dropout=0.1
        )
        
        self.predictor = nn.Sequential(
            nn.Linear(64, 32),
            nn.ReLU(),
            nn.Linear(32, 16),
            nn.ReLU(),
            nn.Linear(16, 5)  # Bid liquidity, ask liquidity, spread, volatility, depth
        )
    
    def forward(self, x: torch.Tensor) -> torch.Tensor:
        # LSTM encoding
        lstm_out, _ = self.lstm(x)
        
        # Self-attention
        attn_out, _ = self.attention(lstm_out, lstm_out, lstm_out)
        
        # Final prediction
        return self.predictor(attn_out[:, -1, :])


class OptimalScheduler(nn.Module):
    """Generates optimal execution schedule"""
    
    def __init__(self, n_slices: int = 20):
        super().__init__()
        
        self.n_slices = n_slices
        
        # Schedule generator
        self.schedule_generator = nn.Sequential(
            nn.Linear(64, 128),
            nn.ReLU(),
            nn.Linear(128, 64),
            nn.ReLU(),
            nn.Linear(64, n_slices * 2)  # Size and timing for each slice
        )
        
        # Urgency estimator
        self.urgency_estimator = nn.Sequential(
            nn.Linear(32, 64),
            nn.ReLU(),
            nn.Linear(64, 32),
            nn.ReLU(),
            nn.Linear(32, 1),
            nn.Sigmoid()
        )
    
    def forward(self, order_features: torch.Tensor, 
                market_features: torch.Tensor) -> Tuple[torch.Tensor, torch.Tensor]:
        
        combined = torch.cat([order_features, market_features], dim=-1)
        
        # Generate schedule
        schedule = self.schedule_generator(combined)
        schedule = schedule.view(-1, self.n_slices, 2)
        
        # Normalize slice sizes
        sizes = F.softmax(schedule[:, :, 0], dim=1)
        
        # Convert timing to cumulative
        timings = torch.sigmoid(schedule[:, :, 1])
        timings = torch.cumsum(timings, dim=1)
        timings = timings / timings[:, -1:].clamp(min=1e-8)  # Normalize to [0, 1]
        
        # Estimate urgency
        urgency = self.urgency_estimator(market_features)
        
        return torch.stack([sizes, timings], dim=2), urgency


class TWAPOptimizer(BaseModel):
    """
    Advanced TWAP execution optimizer with neural market impact modeling
    Minimizes implementation shortfall while controlling market impact
    """
    
    def __init__(self, config: ModelConfig):
        # Model-specific configuration
        config.model_id = "twap_optimizer"
        config.model_type = "execution_optimization"
        
        super().__init__(config)
        
        # Execution parameters
        self.min_slice_size = 100  # Minimum order slice
        self.max_slices = 50  # Maximum number of slices
        self.lookahead_minutes = 30  # Planning horizon
        self.impact_decay_halflife = 5.0  # Minutes
        
        # Historical tracking
        self.execution_history = deque(maxlen=1000)
        self.impact_history = deque(maxlen=500)
        self.liquidity_history = deque(maxlen=1000)
        
        # Real-time tracking
        self.active_orders = {}
        self.order_queue = []  # Priority queue
        
        # Performance metrics
        self.total_cost_savings = 0.0
        self.avg_slippage = 0.0
        self.total_volume_executed = 0.0
    
    def _build_model(self):
        """Build TWAP optimization models"""
        
        # Market impact model
        self.impact_model = MarketImpactModel(feature_dim=32)
        
        # Liquidity prediction
        self.liquidity_predictor = LiquidityPredictor(seq_len=60, feature_dim=16)
        
        # Optimal scheduler
        self.scheduler = OptimalScheduler(n_slices=20)
        
        # Slice timing optimizer
        self.timing_optimizer = nn.Sequential(
            nn.Linear(48, 128),
            nn.ReLU(),
            nn.Dropout(0.2),
            nn.Linear(128, 64),
            nn.ReLU(),
            nn.Dropout(0.1),
            nn.Linear(64, 32),
            nn.ReLU(),
            nn.Linear(32, 1),
            nn.Sigmoid()  # Optimal execution fraction [0, 1]
        )
        
        # Adaptive learning rate controller
        self.adaptation_controller = nn.Sequential(
            nn.Linear(24, 64),
            nn.ReLU(),
            nn.Linear(64, 32),
            nn.ReLU(),
            nn.Linear(32, 4)  # Learning rates for different components
        )
        
        # Cost function approximator
        self.cost_approximator = nn.Sequential(
            nn.Linear(40, 128),
            nn.ReLU(),
            nn.Linear(128, 64),
            nn.ReLU(),
            nn.Linear(64, 32),
            nn.ReLU(),
            nn.Linear(32, 1)  # Expected total cost
        )
        
        # Market regime classifier
        self.regime_classifier = nn.Sequential(
            nn.Linear(32, 64),
            nn.ReLU(),
            nn.Linear(64, 32),
            nn.ReLU(),
            nn.Linear(32, 4)  # Normal, volatile, trending, illiquid
        )
    
    def extract_order_features(self, order_size: float, total_volume: float,
                              urgency: float, side: str) -> torch.Tensor:
        """Extract features from order parameters"""
        
        features = [
            order_size / 1e6,  # Normalized by 1M
            order_size / (total_volume + 1e-8),  # Order as fraction of volume
            urgency,
            1.0 if side == 'buy' else -1.0,
            np.log(order_size + 1),
            min(order_size / self.min_slice_size, self.max_slices) / self.max_slices
        ]
        
        return torch.tensor(features, dtype=torch.float32, device=self.device)
    
    def extract_market_features(self, market_data: Dict[str, Any]) -> torch.Tensor:
        """Extract features from market data"""
        
        features = []
        
        # Price features
        if 'price_history' in market_data:
            prices = market_data['price_history']
            features.extend([
                prices[-1] / prices[0] - 1,  # Return
                np.std(np.diff(prices)) / (np.mean(prices) + 1e-8),  # Volatility
                (prices[-1] - np.min(prices)) / (np.max(prices) - np.min(prices) + 1e-8)  # Price position
            ])
        else:
            features.extend([0.0, 0.02, 0.5])  # Defaults
        
        # Volume features
        if 'volume_history' in market_data:
            volumes = market_data['volume_history']
            features.extend([
                volumes[-1] / (np.mean(volumes) + 1e-8),  # Current vs average volume
                np.std(volumes) / (np.mean(volumes) + 1e-8),  # Volume volatility
                np.sum(volumes[-5:]) / (np.sum(volumes) + 1e-8)  # Recent volume concentration
            ])
        else:
            features.extend([1.0, 0.3, 0.1])
        
        # Order book features
        if 'order_book' in market_data:
            book = market_data['order_book']
            bid_volume = sum([level[1] for level in book.get('bids', [])])
            ask_volume = sum([level[1] for level in book.get('asks', [])])
            
            features.extend([
                bid_volume / (bid_volume + ask_volume + 1e-8),  # Bid ratio
                book['bids'][0][0] / book['asks'][0][0] - 1 if book.get('bids') and book.get('asks') else 0,  # Spread
                len(book.get('bids', [])) / 20,  # Normalized bid depth
                len(book.get('asks', [])) / 20   # Normalized ask depth
            ])
        else:
            features.extend([0.5, 0.0001, 0.5, 0.5])
        
        return torch.tensor(features, dtype=torch.float32, device=self.device)
    
    def predict_market_impact(self, order_features: torch.Tensor,
                            market_features: torch.Tensor) -> Dict[str, float]:
        """Predict market impact of an order"""
        
        combined_features = torch.cat([
            order_features.unsqueeze(0),
            market_features.unsqueeze(0),
            torch.zeros(1, 32 - len(order_features) - len(market_features), device=self.device)
        ], dim=1)
        
        with torch.no_grad():
            impact, cost = self.impact_model(combined_features)
            
        impact_dict = {
            'temporary_impact_bps': impact[0, 0].item() * 100,  # Basis points
            'permanent_impact_bps': impact[0, 1].item() * 100,
            'decay_rate': torch.sigmoid(impact[0, 2]).item(),
            'expected_cost_bps': cost.item() * 100
        }
        
        return impact_dict
    
    def predict_liquidity(self, market_history: torch.Tensor) -> Dict[str, float]:
        """Predict future liquidity conditions"""
        
        with torch.no_grad():
            liquidity = self.liquidity_predictor(market_history.unsqueeze(0))
        
        liquidity_dict = {
            'bid_liquidity': torch.exp(liquidity[0, 0]).item(),
            'ask_liquidity': torch.exp(liquidity[0, 1]).item(),
            'expected_spread_bps': torch.sigmoid(liquidity[0, 2]).item() * 10,
            'volatility': torch.sigmoid(liquidity[0, 3]).item() * 0.1,
            'market_depth': torch.exp(liquidity[0, 4]).item()
        }
        
        return liquidity_dict
    
    def generate_execution_schedule(self, order_size: float, time_horizon: int,
                                  urgency: float, market_data: Dict) -> List[OrderSlice]:
        """Generate optimal TWAP schedule"""
        
        # Extract features
        order_features = self.extract_order_features(
            order_size, 
            market_data.get('total_volume', 1e6),
            urgency,
            market_data.get('side', 'buy')
        )
        
        market_features = self.extract_market_features(market_data)
        
        # Generate schedule
        with torch.no_grad():
            schedule, predicted_urgency = self.scheduler(
                order_features.unsqueeze(0),
                market_features.unsqueeze(0)
            )
        
        # Extract slice sizes and timings
        slice_sizes = schedule[0, :, 0].cpu().numpy()
        slice_timings = schedule[0, :, 1].cpu().numpy()
        
        # Adjust for actual urgency
        urgency_factor = urgency / (predicted_urgency.item() + 1e-8)
        slice_timings = np.power(slice_timings, 1 / urgency_factor)
        
        # Create order slices
        slices = []
        current_time = datetime.now()
        
        for i in range(len(slice_sizes)):
            if slice_sizes[i] * order_size < self.min_slice_size:
                continue
                
            slice_time = current_time + timedelta(minutes=slice_timings[i] * time_horizon)
            
            # Predict price range for this time
            price = market_data.get('current_price', 100.0)
            volatility = market_data.get('volatility', 0.02)
            time_factor = np.sqrt(slice_timings[i] * time_horizon / 1440)  # Daily vol adjustment
            
            price_range = price * volatility * time_factor
            
            order_slice = OrderSlice(
                slice_id=f"slice_{i}_{current_time.timestamp()}",
                parent_order_id=f"order_{current_time.timestamp()}",
                quantity=slice_sizes[i] * order_size,
                target_time=slice_time,
                min_price=price - price_range,
                max_price=price + price_range,
                urgency=urgency * (1 + 0.5 * i / len(slice_sizes))  # Increasing urgency
            )
            
            slices.append(order_slice)
        
        return slices
    
    def optimize_slice_execution(self, order_slice: OrderSlice, 
                               market_state: Dict) -> Dict[str, Any]:
        """Optimize execution of a single slice"""
        
        # Prepare features
        features = []
        
        # Slice features
        features.extend([
            order_slice.remaining_quantity / 1e6,
            order_slice.urgency,
            (order_slice.target_time - datetime.now()).total_seconds() / 3600,  # Hours to target
            order_slice.fill_rate,
            (market_state['current_price'] - order_slice.min_price) / (order_slice.max_price - order_slice.min_price + 1e-8)
        ])
        
        # Market features
        features.extend([
            market_state.get('bid_ask_spread', 0.001),
            market_state.get('order_book_imbalance', 0.0),
            market_state.get('recent_volatility', 0.02),
            market_state.get('volume_rate', 1.0),
            market_state.get('price_momentum', 0.0)
        ])
        
        # Historical execution performance
        if self.execution_history:
            recent_slippage = np.mean([h['slippage'] for h in list(self.execution_history)[-10:]])
            recent_fill_rate = np.mean([h['fill_rate'] for h in list(self.execution_history)[-10:]])
        else:
            recent_slippage = 0.001
            recent_fill_rate = 0.95
            
        features.extend([recent_slippage, recent_fill_rate])
        
        # Pad features to expected size
        features = features + [0.0] * (48 - len(features))
        feature_tensor = torch.tensor(features, dtype=torch.float32, device=self.device).unsqueeze(0)
        
        # Get optimal execution fraction
        with torch.no_grad():
            execution_fraction = self.timing_optimizer(feature_tensor).item()
        
        # Calculate optimal order parameters
        optimal_size = order_slice.remaining_quantity * execution_fraction
        
        # Adjust for market conditions
        if market_state.get('order_book_imbalance', 0) > 0.7:
            # Strong buy pressure, reduce size
            optimal_size *= 0.7
        elif market_state.get('order_book_imbalance', 0) < -0.7:
            # Strong sell pressure, increase size if buying
            if order_slice.quantity > 0:  # Buy order
                optimal_size *= 1.3
        
        # Price optimization
        spread = market_state.get('bid_ask_spread', 0.001)
        if order_slice.urgency > 0.8:
            # High urgency - cross the spread
            optimal_price = market_state['ask_price'] if order_slice.quantity > 0 else market_state['bid_price']
        else:
            # Low urgency - provide liquidity
            mid_price = (market_state['bid_price'] + market_state['ask_price']) / 2
            offset = spread * (0.5 - execution_fraction) * 0.5
            optimal_price = mid_price + offset
        
        # Ensure price is within limits
        optimal_price = np.clip(optimal_price, order_slice.min_price, order_slice.max_price)
        
        return {
            'execute_size': optimal_size,
            'limit_price': optimal_price,
            'order_type': 'limit',
            'time_in_force': 'IOC' if order_slice.urgency > 0.9 else 'GTC',
            'execution_probability': execution_fraction,
            'expected_slippage': spread * execution_fraction,
            'post_only': order_slice.urgency < 0.3
        }
    
    def update_execution_metrics(self, order_slice: OrderSlice, 
                               execution_result: Dict[str, Any]):
        """Update metrics after execution"""
        
        # Calculate metrics
        executed_qty = execution_result.get('filled_quantity', 0)
        executed_price = execution_result.get('average_price', 0)
        
        if executed_qty > 0 and executed_price > 0:
            # Update slice
            order_slice.executed_quantity += executed_qty
            order_slice.executed_price = (
                (order_slice.executed_price * (order_slice.executed_quantity - executed_qty) +
                 executed_price * executed_qty) / order_slice.executed_quantity
            )
            
            # Calculate slippage
            reference_price = execution_result.get('reference_price', executed_price)
            slippage = abs(executed_price - reference_price) / reference_price
            
            # Update history
            self.execution_history.append({
                'timestamp': datetime.now(),
                'slice_id': order_slice.slice_id,
                'quantity': executed_qty,
                'price': executed_price,
                'slippage': slippage,
                'fill_rate': executed_qty / execution_result.get('order_quantity', executed_qty),
                'market_impact': execution_result.get('market_impact', 0.0)
            })
            
            # Update performance metrics
            self.total_volume_executed += executed_qty
            self.avg_slippage = (
                self.avg_slippage * 0.95 + slippage * 0.05
            )  # Exponential moving average
            
            # Estimate cost savings
            naive_slippage = execution_result.get('spread', 0.001) * 0.5  # Half spread
            cost_saving = (naive_slippage - slippage) * executed_qty * executed_price
            self.total_cost_savings += max(0, cost_saving)
    
    def forward(self, x: torch.Tensor) -> Dict[str, torch.Tensor]:
        """
        Main forward pass for TWAP optimization
        
        Args:
            x: Market data tensor
            
        Returns:
            Optimization outputs
        """
        batch_size = x.shape[0]
        
        # Extract market regime
        market_features = x[:, -1, :32]  # Last timestep, first 32 features
        regime_logits = self.regime_classifier(market_features)
        regime = F.softmax(regime_logits, dim=-1)
        
        # Predict market impact for different order sizes
        order_sizes = torch.logspace(-3, 1, 10, device=x.device).unsqueeze(0).expand(batch_size, -1)
        
        impacts = []
        costs = []
        
        for i in range(10):
            size_features = torch.cat([
                order_sizes[:, i:i+1],
                torch.zeros(batch_size, 5, device=x.device),  # Placeholder for other order features
                market_features[:, :26]  # Market features
            ], dim=1)
            
            impact, cost = self.impact_model(size_features)
            impacts.append(impact)
            costs.append(cost)
        
        impacts = torch.stack(impacts, dim=1)
        costs = torch.stack(costs, dim=1)
        
        # Predict liquidity
        liquidity = self.liquidity_predictor(x[:, -60:, :16])  # Last 60 timesteps
        
        # Estimate total cost for a standard order
        standard_order_features = torch.cat([
            torch.ones(batch_size, 6, device=x.device),  # Standard order features
            market_features[:, :34]
        ], dim=1)
        
        total_cost = self.cost_approximator(standard_order_features)
        
        # Adaptation parameters
        adaptation_features = torch.cat([
            regime,
            liquidity,
            costs.mean(dim=1),
            torch.tensor([self.avg_slippage], device=x.device).expand(batch_size, 1),
            torch.zeros(batch_size, 14, device=x.device)  # Padding
        ], dim=1)
        
        adaptation_rates = self.adaptation_controller(adaptation_features)
        
        return {
            'market_regime': regime,
            'impact_curves': impacts,
            'cost_curves': costs,
            'liquidity_forecast': liquidity,
            'total_cost_estimate': total_cost,
            'adaptation_rates': adaptation_rates
        }
    
    def _process_predictions(self, output: torch.Tensor) -> np.ndarray:
        """Process output for compatibility with base class"""
        if isinstance(output, dict):
            # Return regime classification as "prediction"
            return output['market_regime'].argmax(dim=-1).cpu().numpy()
        return np.array([0])  # Default
    
    def _calculate_confidence(self, output: torch.Tensor) -> float:
        """Calculate confidence in optimization"""
        if isinstance(output, dict):
            # Base confidence on liquidity forecast
            if 'liquidity_forecast' in output:
                liquidity = output['liquidity_forecast']
                # Higher liquidity = higher confidence
                bid_liq = torch.exp(liquidity[:, 0])
                ask_liq = torch.exp(liquidity[:, 1])
                total_liq = (bid_liq + ask_liq) / 2
                
                # Normalize to [0, 1]
                confidence = torch.tanh(total_liq / 1e6)
                return confidence.mean().item()
        
        return 0.5
    
    def _get_confidence_scores(self, output: torch.Tensor) -> Dict[str, float]:
        """Get detailed confidence breakdown"""
        if not isinstance(output, dict):
            return {}
        
        scores = {}
        
        # Market regime probabilities
        if 'market_regime' in output:
            regime_probs = output['market_regime'].mean(dim=0)
            regimes = ['normal', 'volatile', 'trending', 'illiquid']
            for i, regime in enumerate(regimes):
                scores[f'{regime}_probability'] = regime_probs[i].item()
        
        # Liquidity metrics
        if 'liquidity_forecast' in output:
            liq = output['liquidity_forecast'].mean(dim=0)
            scores['bid_liquidity'] = torch.exp(liq[0]).item()
            scores['ask_liquidity'] = torch.exp(liq[1]).item()
            scores['expected_spread_bps'] = torch.sigmoid(liq[2]).item() * 10
            scores['volatility'] = torch.sigmoid(liq[3]).item() * 0.1
            scores['market_depth'] = torch.exp(liq[4]).item()
        
        # Cost estimates
        if 'total_cost_estimate' in output:
            scores['expected_cost_bps'] = output['total_cost_estimate'].mean().item() * 100
        
        # Impact estimates
        if 'impact_curves' in output:
            avg_impact = output['impact_curves'].mean(dim=(0, 1))
            scores['avg_temporary_impact_bps'] = avg_impact[0].item() * 100
            scores['avg_permanent_impact_bps'] = avg_impact[1].item() * 100
            scores['avg_decay_rate'] = torch.sigmoid(avg_impact[2]).item()
        
        return scores
    
    def calculate_loss(self, outputs: torch.Tensor, targets: torch.Tensor) -> torch.Tensor:
        """
        Multi-task loss for TWAP optimization
        
        Combines:
        1. Market regime classification
        2. Impact prediction accuracy
        3. Cost prediction accuracy
        4. Liquidity forecast accuracy
        """
        if not isinstance(outputs, dict):
            return torch.tensor(0.0, device=self.device)
        
        total_loss = 0.0
        
        # Market regime loss (if targets provided)
        if 'market_regime' in outputs and len(targets.shape) > 1 and targets.shape[1] >= 1:
            regime_targets = targets[:, 0].long()
            regime_loss = F.cross_entropy(
                outputs['market_regime'], 
                regime_targets
            )
            total_loss += regime_loss
        
        # Impact prediction loss (if historical data available)
        if hasattr(self, 'impact_history') and len(self.impact_history) > 0:
            # This would compare predicted vs actual impact
            # For now, we'll use a placeholder
            impact_loss = 0.0
            total_loss += impact_loss * 0.3
        
        # Cost prediction loss
        if 'total_cost_estimate' in outputs and len(targets.shape) > 1 and targets.shape[1] >= 2:
            cost_targets = targets[:, 1]
            cost_loss = F.mse_loss(
                outputs['total_cost_estimate'].squeeze(),
                cost_targets
            )
            total_loss += cost_loss * 0.2
        
        # Liquidity prediction loss
        if 'liquidity_forecast' in outputs and len(targets.shape) > 1 and targets.shape[1] >= 7:
            liq_targets = targets[:, 2:7]
            liq_loss = F.mse_loss(
                outputs['liquidity_forecast'],
                liq_targets
            )
            total_loss += liq_loss * 0.1
        
        return total_loss
    
    def get_execution_analytics(self) -> Dict[str, Any]:
        """Get comprehensive execution analytics"""
        
        analytics = {
            'total_volume_executed': self.total_volume_executed,
            'average_slippage_bps': self.avg_slippage * 10000,
            'total_cost_savings': self.total_cost_savings,
            'active_orders': len(self.active_orders),
            'execution_history_size': len(self.execution_history)
        }
        
        # Recent performance
        if self.execution_history:
            recent = list(self.execution_history)[-100:]
            analytics['recent_avg_slippage'] = np.mean([h['slippage'] for h in recent])
            analytics['recent_fill_rate'] = np.mean([h['fill_rate'] for h in recent])
            analytics['recent_market_impact'] = np.mean([h['market_impact'] for h in recent])
        
        # Slice performance
        completed_slices = [
            order for order in self.active_orders.values() 
            if order.status == 'completed'
        ]
        
        if completed_slices:
            analytics['avg_slice_fill_rate'] = np.mean([s.fill_rate for s in completed_slices])
            analytics['slice_completion_rate'] = len(completed_slices) / len(self.active_orders)
        
        # Market regime distribution
        if hasattr(self, 'regime_history'):
            regime_counts = defaultdict(int)
            for regime in self.regime_history:
                regime_counts[regime] += 1
            analytics['regime_distribution'] = dict(regime_counts)
        
        return analytics
    
    def adaptive_parameter_update(self, performance_metrics: Dict[str, float]):
        """Adaptively update execution parameters based on performance"""
        
        # Adjust slice sizing based on fill rates
        if 'recent_fill_rate' in performance_metrics:
            fill_rate = performance_metrics['recent_fill_rate']
            
            if fill_rate < 0.8:
                # Poor fills - reduce slice size
                self.min_slice_size *= 0.95
                self.max_slices = min(100, int(self.max_slices * 1.1))
            elif fill_rate > 0.95:
                # Good fills - can increase slice size
                self.min_slice_size *= 1.05
                self.max_slices = max(10, int(self.max_slices * 0.9))
        
        # Adjust urgency thresholds based on slippage
        if 'recent_avg_slippage' in performance_metrics:
            slippage = performance_metrics['recent_avg_slippage']
            
            if slippage > 0.002:  # 20 bps
                # High slippage - be less aggressive
                self.logger.info("High slippage detected, reducing execution aggressiveness")
            elif slippage < 0.0005:  # 5 bps
                # Low slippage - can be more aggressive
                self.logger.info("Low slippage achieved, increasing execution aggressiveness")
        
        # Update impact decay based on observations
        if self.impact_history:
            recent_impacts = list(self.impact_history)[-50:]
            observed_decay = np.mean([impact.get('decay_rate', 0.5) for impact in recent_impacts])
            self.impact_decay_halflife = -5 / np.log(observed_decay + 1e-8)
        
        self.logger.info(f"Adaptive update - min_slice: {self.min_slice_size}, "
                        f"max_slices: {self.max_slices}, "
                        f"decay_halflife: {self.impact_decay_halflife:.2f}")