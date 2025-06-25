"""
AITHORIX Hyperliquid Gas Estimator
Estimates gas costs for different transaction types
"""

import logging
from typing import Dict, Any, Optional

logger = logging.getLogger(__name__)


class HyperliquidGasEstimator:
    """
    Gas estimation for Hyperliquid transactions
    """
    
    def __init__(self, client):
        self.client = client
        
        # Base gas costs for different operations (in USDC)
        self.base_costs = {
            'place_order': 0.10,
            'cancel_order': 0.08,
            'modify_order': 0.12,
            'update_leverage': 0.05,
            'update_margin': 0.15,
            'batch_order': 0.08,  # Per order in batch
            'liquidation': 0.20
        }
    
    async def estimate_transaction_cost(
        self,
        tx_type: str,
        params: Optional[Dict[str, Any]] = None
    ) -> float:
        """
        Estimate gas cost for a transaction
        """
        base_cost = self.base_costs.get(tx_type, 0.10)
        
        # Adjust based on parameters
        if tx_type == 'place_order' and params:
            # Larger orders may have slightly higher costs
            size = params.get('size', 0)
            if size > 10000:  # Large order
                base_cost *= 1.2
            
            # Post-only orders are cheaper
            if params.get('post_only'):
                base_cost *= 0.8
        
        elif tx_type == 'batch_order' and params:
            # Cost scales with batch size
            batch_size = params.get('batch_size', 1)
            base_cost = self.base_costs['batch_order'] * batch_size * 0.9  # Discount for batching
        
        # Get current gas price multiplier
        gas_optimizer = self.client.gas_optimizer if hasattr(self.client, 'gas_optimizer') else None
        if gas_optimizer:
            current_gas = await gas_optimizer._estimate_current_gas()
            multiplier = current_gas / gas_optimizer.target_gas_price
            base_cost *= multiplier
        
        return base_cost
    
    async def estimate_daily_costs(self, daily_tx_count: Dict[str, int]) -> float:
        """
        Estimate daily gas costs based on transaction counts
        """
        total_cost = 0.0
        
        for tx_type, count in daily_tx_count.items():
            cost_per_tx = await self.estimate_transaction_cost(tx_type)
            total_cost += cost_per_tx * count
        
        return total_cost
    
    def get_cost_breakdown(self, transactions: List[Dict[str, Any]]) -> Dict[str, float]:
        """
        Get cost breakdown by transaction type
        """
        breakdown = {}
        
        for tx in transactions:
            tx_type = tx.get('type', 'unknown')
            cost = tx.get('gas_cost', 0)
            
            if tx_type not in breakdown:
                breakdown[tx_type] = 0.0
            breakdown[tx_type] += cost
        
        return breakdown
    
    async def suggest_optimization(
        self,
        planned_transactions: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """
        Suggest optimizations for planned transactions
        """
        suggestions = {
            'batch_opportunities': [],
            'timing_suggestions': [],
            'estimated_savings': 0.0
        }
        
        # Check for batching opportunities
        order_txs = [tx for tx in planned_transactions if tx.get('type') == 'place_order']
        if len(order_txs) > 3:
            suggestions['batch_opportunities'].append({
                'type': 'batch_orders',
                'count': len(order_txs),
                'savings': len(order_txs) * self.base_costs['place_order'] * 0.1
            })
            suggestions['estimated_savings'] += len(order_txs) * self.base_costs['place_order'] * 0.1
        
        # Check for optimal timing
        current_hour = datetime.now().hour
        if 14 <= current_hour <= 22:  # Peak hours
            suggestions['timing_suggestions'].append({
                'suggestion': 'Consider executing non-urgent transactions during off-peak hours',
                'potential_savings': '20%'
            })
        
        return suggestions