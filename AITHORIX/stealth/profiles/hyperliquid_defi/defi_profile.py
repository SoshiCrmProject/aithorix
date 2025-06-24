"""
Hyperliquid DeFi Trader Profile
Mimics behavior of DeFi-native traders on Hyperliquid
"""

import asyncio
import random
from datetime import datetime
from decimal import Decimal
from typing import Dict, Any

from ....core.engine.trading_engine import Order, OrderType, OrderSide


class HyperliquidDeFiProfile:
    """
    Behavioral profile for DeFi trading on Hyperliquid
    """
    
    def __init__(self):
        # DeFi characteristics
        self.gas_awareness = True
        self.mev_protection = True
        self.prefers_limit_orders = True  # Lower fees
        self.avg_position_size = Decimal("10000")  # Smaller than CEX
        
        # Trading patterns
        self.funding_arbitrage = True
        self.cross_protocol_aware = True
        self.yield_farming_integration = True
        
        # Behavioral patterns
        self.gas_price_threshold = 50  # Gwei
        self.preferred_hours = None  # 24/7 trading
        self.slippage_tolerance = Decimal("0.002")  # 0.2%
        self.use_flashloans = False  # Not for spot trading
        
        # MEV protection
        self.use_commit_reveal = True
        self.randomize_timing = True
        self.sandwich_protection = True
        
    async def pre_order_behavior(self, order: Order) -> None:
        """Apply DeFi-specific pre-order behavior"""
        # Check gas prices
        current_gas = await self._get_gas_price()
        if current_gas > self.gas_price_threshold:
            # Wait for lower gas
            await asyncio.sleep(random.uniform(30, 120))
            
        # MEV protection - randomize timing
        if self.randomize_timing:
            jitter = random.uniform(0.1, 2.0)
            await asyncio.sleep(jitter)
            
        # Sandwich attack protection
        if self.sandwich_protection and order.order_type == OrderType.MARKET:
            # Convert to limit order with slight slippage
            order.order_type = OrderType.LIMIT
            if order.side == OrderSide.BUY:
                order.price = order.price * (Decimal("1") + self.slippage_tolerance)
            else:
                order.price = order.price * (Decimal("1") - self.slippage_tolerance)
                
        # DeFi traders often use exact amounts
        if random.random() < 0.3:  # 30% chance
            # Round to nice numbers like 1000, 5000, 10000
            nice_numbers = [100, 250, 500, 1000, 2500, 5000, 10000]
            closest = min(nice_numbers, key=lambda x: abs(float(order.quantity) * float(order.price) - x))
            order.quantity = Decimal(str(closest)) / order.price
            
        # Always prefer post-only for maker fees
        if order.order_type == OrderType.LIMIT:
            order.post_only = True
            
    async def post_order_behavior(self, order: Order, result: Dict[str, Any]) -> None:
        """Apply DeFi-specific post-order behavior"""
        # Log gas costs
        gas_used = result.get("gas_fee", Decimal("0"))
        if gas_used > 0:
            logger.info(f"Gas used: {gas_used} USD")
            
        # Check for MEV
        if await self._detect_mev(order, result):
            logger.warning("Possible MEV detected")
            # Adjust future strategies
            self.randomize_timing = True
            self.sandwich_protection = True
            
    async def estimate_gas_fee(self) -> Decimal:
        """Estimate gas fee for transaction"""
        # Simplified gas estimation
        base_gas = 150000  # Gas units
        gas_price = await self._get_gas_price()  # Gwei
        eth_price = 2000  # USD
        
        gas_fee_eth = (base_gas * gas_price) / 1e9
        gas_fee_usd = Decimal(str(gas_fee_eth * eth_price))
        
        return gas_fee_usd
        
    async def _get_gas_price(self) -> float:
        """Get current gas price in Gwei"""
        # In production, this would fetch from gas oracle
        # Simulate varying gas prices
        base_gas = 30
        variance = random.uniform(0.8, 1.5)
        return base_gas * variance
        
    async def _detect_mev(self, order: Order, result: Dict[str, Any]) -> bool:
        """Detect potential MEV activity"""
        # Simple MEV detection logic
        expected_price = order.price
        actual_price = result.get("average_price", order.price)
        
        if order.side == OrderSide.BUY:
            slippage = (actual_price - expected_price) / expected_price
        else:
            slippage = (expected_price - actual_price) / expected_price
            
        # High slippage might indicate sandwich attack
        return slippage > Decimal("0.005")  # 0.5% threshold
        
    def get_defi_config(self) -> Dict[str, Any]:
        """Get DeFi-specific configuration"""
        return {
            "max_gas_price": self.gas_price_threshold,
            "mev_protection": self.mev_protection,
            "preferred_dex": ["Hyperliquid", "GMX", "dYdX"],
            "use_limit_orders": self.prefers_limit_orders,
            "funding_rate_threshold": 0.0001,  # 0.01% funding
            "leverage_preference": 5,  # Moderate leverage
            "collateral_ratio": 1.5,  # 150% collateralization
            "liquidation_buffer": 0.1  # 10% buffer
        }
