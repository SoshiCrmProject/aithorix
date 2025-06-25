"""
TWAP Optimizer - Complete Production Implementation
File: AITHORIX/models/execution/order/twap.py
Time-Weighted Average Price execution with ML optimization
Minimizes market impact while maintaining execution schedule
"""

import torch
import torch.nn as nn
import torch.nn.functional as F
import numpy as np
from typing import Dict, Any, List, Tuple, Optional
from datetime import datetime, timedelta
from models.base_model import BaseModel, ModelConfig
import pandas as pd
from scipy.optimize import minimize
from collections import deque
import heapq


class MarketImpactModel(nn.Module):
    """Neural network for market impact prediction"""
    
    def __init__(self, input_dim: int, hidden_dim: int = 256):
        super().__init__()
        
        self.impact_predictor = nn.Sequential(
            nn.Linear(input_dim, hidden_dim),
            nn.ReLU(),
            nn.BatchNorm1d(hidden_dim),
            nn.Dropout(0.2),
            nn.Linear(hidden_dim, hidden_dim // 2),
            nn.ReLU(),
            nn.BatchNorm1d(hidden_dim // 2),
            nn.Dropout(0.2),
            nn.Linear(hidden_dim // 2, hidden_dim // 4),
            nn.ReLU(),
            nn.Linear(hidden_dim // 4, 1),
            nn.Sigmoid()  # Impact as percentage
        )
        
        # Temporary vs permanent impact split
        self.impact_splitter = nn.Sequential(
            nn.Linear(hidden_dim // 4, 32),
            nn.ReLU(),
            nn.Linear(32, 2),
            nn.Softmax(dim=-1)
        )
    
    def forward(self, x: torch.Tensor) -> Tuple[torch.Tensor, torch.Tensor]:
        # Predict total impact
        features = x
        for layer in self.impact_predictor[:-2]:  # All but last two layers
            features = layer(features)
        
        total_impact = self.impact_predictor[-1](self.impact_predictor[-2](features))
        
        # Split into temporary and permanent
        split = self.impact_splitter(features)
        temp_impact = total_impact * split[:, 0:1]
        perm_impact = total_impact * split[:, 1:2]
        
        return temp_impact, perm_impact


class AdaptiveScheduler(nn.Module):
    """ML-based adaptive execution scheduler"""
    
    def __init__(self, input_dim: int, max_slices: int = 100):
        super().__init__()
        self.max_slices = max_slices
        
        # Schedule generator
        self.schedule_generator = nn.Sequential(
            nn.Linear(input_dim, 512),
            nn.ReLU(),
            nn.BatchNorm1d(512),
            nn.Linear(512, 256),
            nn.ReLU(),
            nn.BatchNorm1d(256),
            nn.Linear(256, max_slices),
            nn.Softmax(dim=-1)  # Slice proportions sum to 1
        )
        
        # Urgency estimator
        self.urgency_estimator = nn.Sequential(
            nn.Linear(input_dim, 128),
            nn.ReLU(),
            nn.Linear(128, 64),
            nn.ReLU(),
            nn.Linear(64, 1),
            nn.Sigmoid()  # 0 = patient, 1 = urgent
        )
        
        # Adaptability scorer
        self.adaptability_scorer = nn.Sequential(
            nn.Linear(input_dim, 128),
            nn.ReLU(),
            nn.Linear(128, 64),
            nn.ReLU(),
            nn.Linear(64, 1),
            nn.Sigmoid()  # How much to adapt vs stick to schedule
        )
    
    def forward(self, x: torch.Tensor) -> Dict[str, torch.Tensor]:
        schedule = self.schedule_generator(x)
        urgency = self.urgency_estimator(x)
        adaptability = self.adaptability_scorer(x)
        
        return {
            'schedule': schedule,
            'urgency': urgency,
            'adaptability': adaptability
        }


class TWAPOptimizer(BaseModel):
    """
    Advanced TWAP execution optimizer with ML enhancements
    Minimizes market impact while maintaining execution schedule
    """
    
    def __init__(self, config: ModelConfig):
        # Model configuration
        config.model_id = "twap_optimizer"
        config.model_type = "execution_optimization"
        
        # TWAP specific parameters
        self.min_slice_size = 0.001  # 0.1% of total order
        self.max_slice_size = 0.05   # 5% of total order
        self.min_interval_seconds = 10
        self.max_participation_rate = 0.25  # 25% of volume
        
        # Performance tracking
        self.execution_history = deque(maxlen=1000)
        self.impact_history = deque(maxlen=1000)
        
        super().__init__(config)
    
    def _build_model(self):
        """Build TWAP optimization architecture"""
        
        # Feature extraction
        self.feature_extractor = nn.Sequential(
            nn.Linear(self.config.input_features, 512),
            nn.ReLU(),
            nn.BatchNorm1d(512),
            nn.Dropout(self.config.dropout_rate),
            nn.Linear(512, 256),
            nn.ReLU(),
            nn.BatchNorm1d(256)
        )
        
        # Market impact prediction
        self.impact_model = MarketImpactModel(
            input_dim=256 + 10,  # features + order characteristics
            hidden_dim=256
        )
        
        # Adaptive scheduler
        self.scheduler = AdaptiveScheduler(
            input_dim=256 + 10,
            max_slices=100
        )
        
        # Optimal timing predictor
        self.timing_predictor = nn.Sequential(
            nn.Linear(256, 128),
            nn.ReLU(),
            nn.Linear(128, 64),
            nn.ReLU(),
            nn.Linear(64, 24),  # 24 hours of timing scores
            nn.Softmax(dim=-1)
        )
        
        # Volume pattern predictor
        self.volume_predictor = nn.LSTM(
            input_size=256,
            hidden_size=128,
            num_layers=2,
            batch_first=True,
            bidirectional=True
        )
        
        # Risk estimator
        self.risk_estimator = nn.Sequential(
            nn.Linear(256 + 10, 128),
            nn.ReLU(),
            nn.Linear(128, 64),
            nn.ReLU(),
            nn.Linear(64, 3)  # execution risk, timing risk, impact risk
        )
        
        # Optimization objective predictor
        self.objective_predictor = nn.Sequential(
            nn.Linear(256 + 10, 128),
            nn.ReLU(),
            nn.Linear(128, 64),
            nn.ReLU(),
            nn.Linear(64, 1)  # Expected cost/benefit
        )
    
    def extract_order_features(self, order_size: float, total_volume: float, 
                              volatility: float, spread: float, 
                              time_constraint: float) -> torch.Tensor:
        """Extract features specific to the order"""
        features = torch.tensor([
            order_size,
            order_size / total_volume,  # Participation rate
            np.log(order_size + 1),     # Log order size
            volatility,
            spread,
            time_constraint,             # Hours to complete
            1.0 / time_constraint,       # Urgency
            order_size * volatility,     # Risk measure
            spread / volatility,         # Relative spread
            np.sqrt(order_size)          # Square root for impact modeling
        ], dtype=torch.float32)
        
        return features.unsqueeze(0)  # Add batch dimension
    
    def forward(self, x: torch.Tensor, order_info: Dict[str, float]) -> Dict[str, torch.Tensor]:
        """
        Forward pass for TWAP optimization
        
        Args:
            x: Market data tensor (batch_size, seq_len, features)
            order_info: Dictionary with order details
        
        Returns:
            Optimized execution schedule
        """
        batch_size = x.size(0)
        
        # Extract features from market data
        # Use last timestamp for current market state
        current_state = x[:, -1, :]
        market_features = self.feature_extractor(current_state)
        
        # Extract order-specific features
        order_features = self.extract_order_features(
            order_info['size'],
            order_info['avg_volume'],
            order_info['volatility'],
            order_info['spread'],
            order_info['time_constraint']
        ).to(x.device)
        
        # Expand order features to match batch size
        order_features = order_features.expand(batch_size, -1)
        
        # Combine features
        combined_features = torch.cat([market_features, order_features], dim=-1)
        
        # Predict market impact
        temp_impact, perm_impact = self.impact_model(combined_features)
        
        # Generate adaptive schedule
        schedule_output = self.scheduler(combined_features)
        
        # Predict optimal timing
        timing_scores = self.timing_predictor(market_features)
        
        # Predict volume patterns
        volume_lstm_input = market_features.unsqueeze(1).repeat(1, 24, 1)
        volume_output, _ = self.volume_predictor(volume_lstm_input)
        volume_predictions = volume_output[:, -1, :]
        
        # Estimate risks
        risks = self.risk_estimator(combined_features)
        
        # Predict optimization objective
        expected_cost = self.objective_predictor(combined_features)
        
        # Prepare outputs
        outputs = {
            'schedule': schedule_output['schedule'],
            'urgency': schedule_output['urgency'],
            'adaptability': schedule_output['adaptability'],
            'temp_impact': temp_impact,
            'perm_impact': perm_impact,
            'timing_scores': timing_scores,
            'volume_predictions': volume_predictions,
            'risks': risks,
            'expected_cost': expected_cost,
            'combined_features': combined_features
        }
        
        return outputs
    
    def optimize_schedule(self, market_data: torch.Tensor, 
                         order_info: Dict[str, float]) -> Dict[str, Any]:
        """
        Generate optimized TWAP execution schedule
        
        Args:
            market_data: Current and historical market data
            order_info: Order details including size, constraints, etc.
        
        Returns:
            Complete execution plan
        """
        self.eval()
        
        with torch.no_grad():
            # Get model predictions
            outputs = self.forward(market_data, order_info)
            
            # Extract components
            schedule = outputs['schedule'].squeeze().cpu().numpy()
            urgency = outputs['urgency'].item()
            adaptability = outputs['adaptability'].item()
            temp_impact = outputs['temp_impact'].item()
            perm_impact = outputs['perm_impact'].item()
            timing_scores = outputs['timing_scores'].squeeze().cpu().numpy()
            risks = outputs['risks'].squeeze().cpu().numpy()
            expected_cost = outputs['expected_cost'].item()
            
            # Generate execution plan
            execution_plan = self._generate_execution_plan(
                schedule, urgency, timing_scores, order_info
            )
            
            # Calculate optimal parameters
            optimal_params = self._optimize_parameters(
                execution_plan, market_data, order_info, outputs
            )
            
            # Risk-adjusted schedule
            risk_adjusted_plan = self._adjust_for_risk(
                execution_plan, risks, adaptability
            )
            
            # Final optimization
            final_plan = self._final_optimization(
                risk_adjusted_plan, market_data, order_info
            )
            
            return {
                'execution_plan': final_plan,
                'expected_impact': {
                    'temporary': temp_impact,
                    'permanent': perm_impact,
                    'total': temp_impact + perm_impact
                },
                'risk_assessment': {
                    'execution_risk': risks[0],
                    'timing_risk': risks[1],
                    'impact_risk': risks[2]
                },
                'expected_cost': expected_cost,
                'urgency_score': urgency,
                'adaptability_score': adaptability,
                'optimal_parameters': optimal_params,
                'performance_metrics': self._calculate_performance_metrics(final_plan)
            }
    
    def _generate_execution_plan(self, schedule: np.ndarray, urgency: float,
                                timing_scores: np.ndarray, 
                                order_info: Dict[str, float]) -> List[Dict]:
        """Generate detailed execution plan from model outputs"""
        
        total_size = order_info['size']
        time_constraint = order_info['time_constraint']  # in hours
        
        # Adjust schedule based on urgency
        if urgency > 0.7:
            # Front-load execution
            schedule = self._front_load_schedule(schedule, urgency)
        elif urgency < 0.3:
            # Spread more evenly
            schedule = self._smooth_schedule(schedule)
        
        # Remove negligible slices
        schedule[schedule < self.min_slice_size] = 0
        schedule = schedule / schedule.sum()  # Renormalize
        
        # Calculate time slots
        num_slices = np.sum(schedule > 0)
        time_per_slice = (time_constraint * 3600) / num_slices  # in seconds
        
        # Ensure minimum interval
        if time_per_slice < self.min_interval_seconds:
            # Reduce number of slices
            num_slices = int((time_constraint * 3600) / self.min_interval_seconds)
            schedule = self._consolidate_schedule(schedule, num_slices)
        
        # Generate execution times based on timing scores
        execution_times = self._generate_execution_times(
            timing_scores, num_slices, time_constraint
        )
        
        # Create execution plan
        plan = []
        cumulative_size = 0
        
        for i, (slice_proportion, exec_time) in enumerate(zip(schedule, execution_times)):
            if slice_proportion == 0:
                continue
                
            slice_size = slice_proportion * total_size
            
            # Ensure size constraints
            slice_size = max(self.min_slice_size * total_size, 
                           min(self.max_slice_size * total_size, slice_size))
            
            plan.append({
                'slice_id': i,
                'time': exec_time,
                'size': slice_size,
                'proportion': slice_size / total_size,
                'cumulative_proportion': (cumulative_size + slice_size) / total_size,
                'estimated_price_impact': self._estimate_slice_impact(slice_size, order_info),
                'priority': 1.0 - (i / len(execution_times)),  # Higher priority for earlier slices
                'adaptable': True  # Can be modified in real-time
            })
            
            cumulative_size += slice_size
        
        # Ensure full execution
        if cumulative_size < total_size * 0.999:
            # Add remainder to last slice
            plan[-1]['size'] += total_size - cumulative_size
        
        return plan
    
    def _front_load_schedule(self, schedule: np.ndarray, urgency: float) -> np.ndarray:
        """Adjust schedule to execute more aggressively early"""
        # Create exponential decay based on urgency
        decay_rate = 2.0 + 3.0 * urgency  # Higher urgency = faster decay
        weights = np.exp(-decay_rate * np.linspace(0, 1, len(schedule)))
        
        # Apply weights
        adjusted_schedule = schedule * weights
        
        # Renormalize
        return adjusted_schedule / adjusted_schedule.sum()
    
    def _smooth_schedule(self, schedule: np.ndarray) -> np.ndarray:
        """Smooth schedule for more even execution"""
        # Apply moving average
        kernel_size = 5
        kernel = np.ones(kernel_size) / kernel_size
        
        # Pad for convolution
        padded = np.pad(schedule, kernel_size // 2, mode='edge')
        smoothed = np.convolve(padded, kernel, mode='valid')
        
        # Renormalize
        return smoothed / smoothed.sum()
    
    def _consolidate_schedule(self, schedule: np.ndarray, target_slices: int) -> np.ndarray:
        """Consolidate schedule to target number of slices"""
        if len(schedule) <= target_slices:
            return schedule
        
        # Group slices
        group_size = len(schedule) // target_slices
        consolidated = np.zeros(target_slices)
        
        for i in range(target_slices):
            start_idx = i * group_size
            end_idx = start_idx + group_size if i < target_slices - 1 else len(schedule)
            consolidated[i] = schedule[start_idx:end_idx].sum()
        
        return consolidated / consolidated.sum()
    
    def _generate_execution_times(self, timing_scores: np.ndarray, 
                                 num_slices: int, hours: float) -> List[datetime]:
        """Generate optimal execution times"""
        now = datetime.now()
        end_time = now + timedelta(hours=hours)
        
        # Create time slots based on timing scores
        # Higher scores = preferred times
        total_minutes = int(hours * 60)
        minute_scores = np.interp(
            np.linspace(0, len(timing_scores) - 1, total_minutes),
            np.arange(len(timing_scores)),
            timing_scores
        )
        
        # Sample execution times weighted by scores
        probabilities = minute_scores / minute_scores.sum()
        selected_minutes = np.sort(np.random.choice(
            total_minutes, 
            size=num_slices, 
            replace=False,
            p=probabilities
        ))
        
        # Convert to datetime objects
        execution_times = []
        for minute in selected_minutes:
            exec_time = now + timedelta(minutes=int(minute))
            execution_times.append(exec_time)
        
        return execution_times
    
    def _estimate_slice_impact(self, slice_size: float, order_info: Dict[str, float]) -> float:
        """Estimate price impact for a single slice"""
        # Simplified square-root impact model
        avg_volume = order_info['avg_volume']
        volatility = order_info['volatility']
        spread = order_info['spread']
        
        participation_rate = slice_size / avg_volume
        
        # Temporary impact (basis points)
        temp_impact = spread + 10 * volatility * np.sqrt(participation_rate)
        
        # Permanent impact (basis points)
        perm_impact = 5 * volatility * participation_rate
        
        return (temp_impact + perm_impact) / 10000  # Convert to percentage
    
    def _optimize_parameters(self, execution_plan: List[Dict], 
                           market_data: torch.Tensor,
                           order_info: Dict[str, float],
                           model_outputs: Dict[str, torch.Tensor]) -> Dict[str, float]:
        """Optimize execution parameters using scipy optimizer"""
        
        def objective(params):
            # Extract parameters
            aggressiveness, adaptability, min_part_rate, max_part_rate = params
            
            # Calculate expected cost
            impact_cost = (model_outputs['temp_impact'].item() * aggressiveness + 
                         model_outputs['perm_impact'].item())
            
            timing_risk = model_outputs['risks'].squeeze()[1].item() * (1 - aggressiveness)
            
            # Penalty for extreme parameters
            penalty = 0
            if min_part_rate < 0.01 or max_part_rate > 0.5:
                penalty += 0.1
            
            return impact_cost + 0.5 * timing_risk + penalty
        
        # Initial guess
        x0 = [0.5, 0.5, 0.05, 0.25]
        
        # Bounds
        bounds = [
            (0.1, 1.0),    # aggressiveness
            (0.0, 1.0),    # adaptability  
            (0.01, 0.1),   # min_participation_rate
            (0.1, 0.5)     # max_participation_rate
        ]
        
        # Optimize
        result = minimize(objective, x0, bounds=bounds, method='L-BFGS-B')
        
        return {
            'aggressiveness': result.x[0],
            'adaptability': result.x[1],
            'min_participation_rate': result.x[2],
            'max_participation_rate': result.x[3],
            'optimization_success': result.success,
            'expected_cost': result.fun
        }
    
    def _adjust_for_risk(self, execution_plan: List[Dict], 
                        risks: np.ndarray, adaptability: float) -> List[Dict]:
        """Adjust execution plan based on risk assessment"""
        
        execution_risk, timing_risk, impact_risk = risks
        
        # High execution risk - reduce slice sizes
        if execution_risk > 0.7:
            for slice_info in execution_plan:
                slice_info['size'] *= 0.8
                slice_info['adaptable'] = True
        
        # High timing risk - add buffer time
        if timing_risk > 0.7:
            # Extend intervals
            for i in range(1, len(execution_plan)):
                time_diff = (execution_plan[i]['time'] - execution_plan[i-1]['time']).total_seconds()
                if time_diff < 60:  # Less than 1 minute
                    execution_plan[i]['time'] += timedelta(seconds=30)
        
        # High impact risk - further split large slices
        if impact_risk > 0.7:
            new_plan = []
            for slice_info in execution_plan:
                if slice_info['proportion'] > 0.03:  # 3% of total
                    # Split into two
                    half_size = slice_info['size'] / 2
                    
                    slice1 = slice_info.copy()
                    slice1['size'] = half_size
                    slice1['proportion'] = slice1['size'] / sum(s['size'] for s in execution_plan)
                    
                    slice2 = slice_info.copy()
                    slice2['size'] = half_size
                    slice2['proportion'] = slice2['size'] / sum(s['size'] for s in execution_plan)
                    slice2['time'] += timedelta(seconds=30)
                    
                    new_plan.extend([slice1, slice2])
                else:
                    new_plan.append(slice_info)
            
            execution_plan = new_plan
        
        # Apply adaptability factor
        for slice_info in execution_plan:
            slice_info['adaptability_score'] = adaptability
        
        return execution_plan
    
    def _final_optimization(self, execution_plan: List[Dict],
                          market_data: torch.Tensor,
                          order_info: Dict[str, float]) -> List[Dict]:
        """Final optimization pass"""
        
        # Sort by time
        execution_plan.sort(key=lambda x: x['time'])
        
        # Ensure participation rate constraints
        for slice_info in execution_plan:
            max_size = order_info['avg_volume'] * self.max_participation_rate
            if slice_info['size'] > max_size:
                slice_info['size'] = max_size
                slice_info['warning'] = 'Size capped due to participation rate limit'
        
        # Add execution instructions
        for i, slice_info in enumerate(execution_plan):
            slice_info['execution_type'] = 'limit' if slice_info['size'] < order_info['avg_volume'] * 0.01 else 'market'
            slice_info['price_limit'] = None  # Will be set at execution time
            slice_info['time_in_force'] = 'IOC' if slice_info.get('urgency', 0) > 0.8 else 'DAY'
            
        return execution_plan
    
    def _calculate_performance_metrics(self, execution_plan: List[Dict]) -> Dict[str, float]:
        """Calculate expected performance metrics"""
        
        total_slices = len(execution_plan)
        total_size = sum(s['size'] for s in execution_plan)
        
        # Time metrics
        start_time = execution_plan[0]['time']
        end_time = execution_plan[-1]['time']
        duration_hours = (end_time - start_time).total_seconds() / 3600
        
        # Size distribution metrics
        sizes = [s['size'] for s in execution_plan]
        size_variance = np.var(sizes)
        
        # Impact metrics
        estimated_impacts = [s['estimated_price_impact'] for s in execution_plan]
        avg_impact = np.mean(estimated_impacts)
        max_impact = np.max(estimated_impacts)
        
        return {
            'total_slices': total_slices,
            'duration_hours': duration_hours,
            'avg_slice_size': total_size / total_slices,
            'size_variance': size_variance,
            'avg_impact_bps': avg_impact * 10000,
            'max_impact_bps': max_impact * 10000,
            'execution_rate': total_slices / duration_hours if duration_hours > 0 else 0
        }
    
    def adapt_in_realtime(self, current_execution: Dict[str, Any],
                         market_update: torch.Tensor,
                         filled_slices: List[Dict]) -> Dict[str, Any]:
        """Adapt execution plan in real-time based on market conditions"""
        
        # Calculate execution progress
        total_filled = sum(s['filled_size'] for s in filled_slices)
        remaining_size = current_execution['total_size'] - total_filled
        
        # Get updated predictions
        updated_order_info = current_execution['order_info'].copy()
        updated_order_info['size'] = remaining_size
        
        # Recalculate optimal schedule for remaining order
        updated_plan = self.optimize_schedule(market_update, updated_order_info)
        
        # Merge with existing plan
        remaining_slices = [s for s in current_execution['execution_plan'] 
                          if s['slice_id'] not in [f['slice_id'] for f in filled_slices]]
        
        # Adjust remaining slices based on new predictions
        adaptability = updated_plan['adaptability_score']
        
        if adaptability > 0.5:  # Willing to adapt
            # Replace remaining plan with updated version
            return updated_plan
        else:
            # Minor adjustments only
            for slice_info in remaining_slices:
                # Adjust size based on market conditions
                if updated_plan['expected_impact']['total'] > current_execution['expected_impact']['total'] * 1.5:
                    slice_info['size'] *= 0.9  # Reduce size if impact increased
                    
            return {
                'execution_plan': remaining_slices,
                'updated': True,
                'adaptability_applied': adaptability
            }
    
    def _process_predictions(self, output: torch.Tensor) -> np.ndarray:
        """Process predictions (not used for TWAP but required by base class)"""
        return np.array([0])  # Dummy implementation
    
    def _calculate_confidence(self, output: torch.Tensor) -> float:
        """Calculate confidence in execution plan"""
        if isinstance(output, dict):
            # Use various factors
            urgency = output.get('urgency', torch.tensor(0.5)).item()
            risks = output.get('risks', torch.zeros(3))
            avg_risk = risks.mean().item()
            
            # Confidence is inverse of risk
            confidence = 1.0 - avg_risk
            
            # Adjust for urgency (urgent orders have less flexibility)
            confidence *= (1.0 - 0.3 * urgency)
            
            return confidence
        
        return 0.5
    
    def _get_confidence_scores(self, output: torch.Tensor) -> Dict[str, float]:
        """Get detailed confidence scores"""
        if not isinstance(output, dict):
            return {}
        
        scores = {
            'execution_confidence': 1.0 - output.get('risks', torch.zeros(3))[0].item(),
            'timing_confidence': 1.0 - output.get('risks', torch.zeros(3))[1].item(),
            'impact_confidence': 1.0 - output.get('risks', torch.zeros(3))[2].item(),
            'urgency_score': output.get('urgency', torch.tensor(0.5)).item(),
            'adaptability_score': output.get('adaptability', torch.tensor(0.5)).item()
        }
        
        return scores
    
    def calculate_loss(self, outputs: torch.Tensor, targets: torch.Tensor) -> torch.Tensor:
        """Calculate loss for TWAP optimization"""
        if not isinstance(outputs, dict):
            return torch.tensor(0.0)
        
        # Impact prediction loss
        if 'temp_impact' in outputs and 'perm_impact' in outputs:
            # Targets should contain actual impacts
            actual_temp_impact = targets[:, 0]
            actual_perm_impact = targets[:, 1]
            
            impact_loss = (
                F.mse_loss(outputs['temp_impact'].squeeze(), actual_temp_impact) +
                F.mse_loss(outputs['perm_impact'].squeeze(), actual_perm_impact)
            )
        else:
            impact_loss = 0.0
        
        # Schedule quality loss (entropy - we want decisive schedules)
        if 'schedule' in outputs:
            schedule_entropy = -torch.sum(
                outputs['schedule'] * torch.log(outputs['schedule'] + 1e-8),
                dim=-1
            ).mean()
            schedule_loss = schedule_entropy
        else:
            schedule_loss = 0.0
        
        # Risk prediction loss
        if 'risks' in outputs:
            # Targets should contain actual risk outcomes
            if targets.shape[1] > 3:
                actual_risks = targets[:, 2:5]
                risk_loss = F.mse_loss(outputs['risks'], actual_risks)
            else:
                risk_loss = 0.0
        else:
            risk_loss = 0.0
        
        # Cost prediction loss
        if 'expected_cost' in outputs:
            if targets.shape[1] > 5:
                actual_cost = targets[:, 5]
                cost_loss = F.mse_loss(outputs['expected_cost'].squeeze(), actual_cost)
            else:
                cost_loss = 0.0
        else:
            cost_loss = 0.0
        
        # Combine losses
        total_loss = (
            impact_loss + 
            0.1 * schedule_loss + 
            0.3 * risk_loss +
            0.5 * cost_loss
        )
        
        return total_loss