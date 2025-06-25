"""
AITHORIX Hyperliquid Gas Optimizer
Optimizes gas usage for Hyperliquid L2 transactions
"""

import logging
from typing import Dict, Any, Optional
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)


class HyperliquidGasOptimizer:
    """
    Gas optimization for Hyperliquid L2
    """
    
    def __init__(self, client):
        self.client = client
        
        # Gas price history
        self.gas_history = []
        self.max_history = 1000
        
        # Optimization parameters
        self.target_gas_price = 0.1  # USDC
        self.max_gas_price = 2.0  # USDC
        self.urgency_multiplier = {
            'low': 0.8,
            'normal': 1.0,
            'high': 1.5,
            'urgent': 2.0
        }
    
    async def get_optimal_gas_price(self, urgency: str = 'normal') -> float:
        """
        Get optimal gas price based on current conditions
        """
        try:
            # Get current gas price from recent transactions
            current_gas = await self._estimate_current_gas()
            
            # Apply urgency multiplier
            multiplier = self.urgency_multiplier.get(urgency, 1.0)
            optimal_gas = current_gas * multiplier
            
            # Cap at maximum
            optimal_gas = min(optimal_gas, self.max_gas_price)
            
            # Record in history
            self._record_gas_price(optimal_gas)
            
            return optimal_gas
            
        except Exception as e:
            logger.error(f"Failed to get optimal gas price: {str(e)}")
            return self.target_gas_price
    
    async def should_delay_transaction(self, urgency: str = 'normal') -> bool:
        """
        Determine if transaction should be delayed for better gas prices
        """
        if urgency in ['high', 'urgent']:
            return False
        
        current_gas = await self._estimate_current_gas()
        avg_gas = self._get_average_gas_price()
        
        # Delay if current gas is >20% above average
        return current_gas > avg_gas * 1.2
    
    async def batch_transactions(self, transactions: List[Dict[str, Any]]) -> List[List[Dict[str, Any]]]:
        """
        Batch transactions for gas efficiency
        """
        # Hyperliquid supports batching certain operations
        batches = []
        current_batch = []
        
        for tx in transactions:
            tx_type = tx.get('type')
            
            # Can batch same type operations
            if not current_batch or current_batch[0].get('type') == tx_type:
                current_batch.append(tx)
            else:
                if current_batch:
                    batches.append(current_batch)
                current_batch = [tx]
            
            # Max batch size
            if len(current_batch) >= 10:
                batches.append(current_batch)
                current_batch = []
        
        if current_batch:
            batches.append(current_batch)
        
        return batches
    
    async def _estimate_current_gas(self) -> float:
        """
        Estimate current gas price from recent transactions
        """
        # For Hyperliquid L2, gas is relatively stable
        # In production, would query recent transaction costs
        base_gas = 0.1  # Base gas price in USDC
        
        # Add some variance based on time of day
        hour = datetime.now().hour
        if 14 <= hour <= 22:  # US trading hours
            base_gas *= 1.2
        elif 2 <= hour <= 10:  # Asian trading hours
            base_gas *= 1.1
        
        return base_gas
    
    def _record_gas_price(self, price: float):
        """Record gas price in history"""
        self.gas_history.append({
            'price': price,
            'timestamp': datetime.now()
        })
        
        # Trim history
        if len(self.gas_history) > self.max_history:
            self.gas_history = self.gas_history[-self.max_history:]
    
    def _get_average_gas_price(self, hours: int = 24) -> float:
        """Get average gas price over period"""
        if not self.gas_history:
            return self.target_gas_price
        
        cutoff = datetime.now() - timedelta(hours=hours)
        recent_prices = [
            h['price'] for h in self.gas_history
            if h['timestamp'] > cutoff
        ]
        
        if not recent_prices:
            return self.target_gas_price
        
        return sum(recent_prices) / len(recent_prices)
    
    def get_gas_statistics(self) -> Dict[str, float]:
        """Get gas usage statistics"""
        if not self.gas_history:
            return {
                'average': self.target_gas_price,
                'min': self.target_gas_price,
                'max': self.target_gas_price,
                'current': self.target_gas_price
            }
        
        prices = [h['price'] for h in self.gas_history]
        
        return {
            'average': sum(prices) / len(prices),
            'min': min(prices),
            'max': max(prices),
            'current': prices[-1] if prices else self.target_gas_price
        }