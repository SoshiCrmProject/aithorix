"""
OKX Asian Trader Profile
Mimics behavior of Asian timezone traders on OKX
"""

import asyncio
import random
from datetime import datetime, time, timezone
from decimal import Decimal
from typing import Dict, Any, Optional
import pytz

from ....core.engine.trading_engine import Order, OrderType, OrderSide


class OKXAsianProfile:
    """
    Behavioral profile for Asian timezone trading on OKX
    """
    
    def __init__(self):
        # Asian trading characteristics
        self.timezone = pytz.timezone('Asia/Shanghai')  # Beijing time
        self.preferred_sessions = {
            "tokyo": (9, 15),     # 9 AM - 3 PM JST
            "shanghai": (9, 16),  # 9 AM - 4 PM CST
            "singapore": (9, 17), # 9 AM - 5 PM SGT
            "sydney": (10, 16)    # 10 AM - 4 PM AEDT
        }
        
        # Trading preferences
        self.prefers_spot = 0.6  # 60% spot, 40% derivatives
        self.uses_grid_trading = 0.4  # 40% use grid bots
        self.uses_copy_trading = 0.2  # 20% follow others
        self.recurring_buy = 0.3  # 30% DCA strategy
        
        # Cultural trading patterns
        self.lucky_numbers = ["8", "88", "888", "168", "666"]
        self.avoids_number_4 = True  # Unlucky in Asian culture
        self.prefers_round_lots = True
        self.festival_trading = True  # Increased activity during festivals
        
        # Risk preferences
        self.conservative_approach = 0.7  # 70% conservative
        self.uses_savings_products = True
        self.staking_preference = 0.5  # 50% stake holdings
        
        # Social trading
        self.follows_kols = True  # Key Opinion Leaders
        self.group_trading = 0.3  # 30% influenced by groups
        self.news_sensitive = 0.8  # 80% react to news
        
        # Session tracking
        self.is_festival_period = False
        self.current_session = None
        
    async def pre_order_behavior(self, order: Order) -> None:
        """Apply Asian trader pre-order behavior"""
        # Check if in Asian trading hours
        current_time = datetime.now(self.timezone)
        hour = current_time.hour
        
        in_session = False
        for session, (start, end) in self.preferred_sessions.items():
            if start <= hour <= end:
                in_session = True
                self.current_session = session
                break
                
        if not in_session:
            # Outside main hours - reduce activity
            delay = random.uniform(60, 300)  # 1-5 minutes
            await asyncio.sleep(delay)
            
        # Lucky number adjustments
        if self.lucky_numbers:
            total_value = float(order.quantity * order.price)
            
            # Try to adjust to lucky numbers
            for lucky in self.lucky_numbers:
                lucky_value = float(lucky)
                if abs(total_value - lucky_value) / total_value < 0.1:  # Within 10%
                    order.quantity = Decimal(lucky) / order.price
                    break
                    
        # Avoid unlucky number 4
        if self.avoids_number_4:
            quantity_str = str(order.quantity)
            price_str = str(order.price)
            
            if "4" in quantity_str or "4" in price_str:
                # Adjust slightly to avoid 4
                if "4" in quantity_str:
                    order.quantity = order.quantity * Decimal("1.01")
                if "4" in price_str and order.order_type == OrderType.LIMIT:
                    order.price = order.price * Decimal("1.001")
                    
        # Round lot preferences
        if self.prefers_round_lots:
            # Asian traders prefer round numbers
            if order.quantity > 100:
                order.quantity = Decimal(str(round(float(order.quantity), -2)))  # Round to 100s
            elif order.quantity > 10:
                order.quantity = Decimal(str(round(float(order.quantity), -1)))  # Round to 10s
                
        # Conservative position sizing
        if random.random() < self.conservative_approach:
            order.quantity = order.quantity * Decimal("0.8")  # Reduce size by 20%
            
        # Festival period adjustments
        if self._is_festival_period():
            self.is_festival_period = True
            # Increased activity during festivals
            delay = random.uniform(5, 30)  # Shorter delays
        else:
            # Normal trading delay
            delay = random.uniform(10, 60)
            
        await asyncio.sleep(delay)
        
        # News reaction
        if random.random() < self.news_sensitive:
            # Check if there's recent news (simplified)
            # In production, would check actual news feeds
            news_delay = random.uniform(30, 180)  # 30s to 3min reaction time
            await asyncio.sleep(news_delay)
            
    async def post_order_behavior(self, order: Order, result: Dict[str, Any]) -> None:
        """Apply Asian trader post-order behavior"""
        # Grid trading setup
        if random.random() < self.uses_grid_trading:
            # Would set up grid bot for ranging markets
            logger.info("Considering grid trading setup")
            
        # Copy trading check
        if random.random() < self.uses_copy_trading:
            # Would check top traders to follow
            logger.info("Checking copy trading leaders")
            
        # Staking consideration
        if self.staking_preference and order.symbol.endswith("-USDT"):
            # Consider staking if holding spot
            logger.info(f"Consider staking {order.symbol.split('-')[0]}")
            
        # Group sharing behavior
        if random.random() < self.group_trading:
            # Would share trade in group (not actually implemented)
            logger.info("Trade shared with trading group")
            
    def _is_festival_period(self) -> bool:
        """Check if current date is during major Asian festivals"""
        current_date = datetime.now(self.timezone).date()
        
        # Simplified festival dates (would be more comprehensive)
        festivals = [
            (1, 1),    # New Year
            (2, 1),    # Spring Festival (approximate)
            (2, 14),   # Valentine's
            (5, 1),    # Labor Day
            (10, 1),   # National Day (China)
            (11, 11),  # Singles Day
            (12, 25),  # Christmas
        ]
        
        for month, day in festivals:
            if current_date.month == month and abs(current_date.day - day) <= 3:
                return True
                
        return False
        
    def should_use_grid_trading(self, symbol: str, price_range: Tuple[Decimal, Decimal]) -> bool:
        """Determine if grid trading is suitable"""
        # Check if market is ranging
        price_diff = (price_range[1] - price_range[0]) / price_range[0]
        
        # Grid trading good for 5-20% ranges
        if Decimal("0.05") <= price_diff <= Decimal("0.20"):
            return random.random() < self.uses_grid_trading
            
        return False
        
    def get_grid_parameters(self, investment: Decimal) -> Dict[str, Any]:
        """Get grid trading parameters"""
        return {
            "grid_count": random.randint(20, 50),  # Asian traders like more grids
            "profit_per_grid": Decimal("0.002"),   # 0.2% per grid
            "stop_loss": Decimal("0.05"),         # 5% stop loss
            "investment_per_grid": investment / 30  # Spread across grids
        }
        
    def get_asian_config(self) -> Dict[str, Any]:
        """Get Asian trading configuration"""
        return {
            "preferred_pairs": [
                "BTC-USDT", "ETH-USDT", "BNB-USDT",
                "TRX-USDT", "EOS-USDT",  # Popular in Asia
                "FIL-USDT", "DOT-USDT"
            ],
            "payment_coins": ["USDT", "USDC", "BUSD"],
            "max_positions": 20,  # Diversification preference
            "use_oco_orders": True,  # One-Cancels-Other
            "profit_taking_levels": [0.05, 0.10, 0.20],  # 5%, 10%, 20%
            "dca_enabled": self.recurring_buy > 0.5,
            "dca_frequency": "weekly",
            "social_trading_weight": 0.3,  # 30% decisions influenced by social
            "technical_analysis_weight": 0.5,  # 50% TA
            "fundamental_weight": 0.2  # 20% fundamentals
        }
