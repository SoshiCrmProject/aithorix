"""
Binance Institutional Trader Profile
Mimics behavior of professional institutional traders on Binance
"""

import asyncio
import random
from datetime import datetime, time
from decimal import Decimal
from typing import Optional, Dict, Any

from ....core.engine.trading_engine import Order, OrderType, OrderSide


class BinanceInstitutionalProfile:
    """
    Behavioral profile for institutional trading on Binance
    """
    
    def __init__(self):
        # Trading characteristics
        self.avg_order_size = Decimal("50000")  # USD value
        self.preferred_times = [(9, 16), (14, 22)]  # London & NY sessions
        self.order_frequency = 300  # seconds between orders
        self.use_iceberg = True
        self.preferred_pairs = [
            "BTCUSDT", "ETHUSDT", "BNBUSDT", 
            "SOLUSDT", "ADAUSDT", "AVAXUSDT"
        ]
        
        # Behavioral patterns
        self.use_round_numbers = False  # Avoid round price levels
        self.ladder_orders = True  # Split large orders
        self.post_only_preference = 0.7  # 70% maker orders
        self.cancel_ratio = 0.15  # Cancel 15% of orders
        
        # Session management
        self.session_start = None
        self.orders_this_session = 0
        self.last_order_time = None
        
    async def pre_order_behavior(self, order: Order) -> None:
        """Apply pre-order behavioral modifications"""
        # Check if we're in preferred trading time
        current_hour = datetime.now().hour
        in_session = any(
            start <= current_hour <= end 
            for start, end in self.preferred_times
        )
        
        if not in_session:
            # Add extra delay outside main sessions
            await asyncio.sleep(random.uniform(5, 15))
            
        # Avoid round numbers
        if not self.use_round_numbers and order.order_type == OrderType.LIMIT:
            # Add small random offset to price
            offset = Decimal(str(random.uniform(0.01, 0.09)))
            if order.side == OrderSide.BUY:
                order.price = order.price - offset
            else:
                order.price = order.price + offset
                
        # Implement order laddering
        if self.ladder_orders and order.quantity * order.price > self.avg_order_size * 2:
            # This is a large order, should be split
            # (In real implementation, this would create multiple orders)
            pass
            
        # Add professional delay pattern
        if self.last_order_time:
            time_since_last = (datetime.now() - self.last_order_time).seconds
            if time_since_last < 60:
                # Too fast, add delay
                delay = random.uniform(3, 8)
                await asyncio.sleep(delay)
                
        # Prefer post-only orders
        if random.random() < self.post_only_preference:
            order.post_only = True
            
        # Professional order sizing (avoid tiny decimals)
        if order.symbol in ["BTCUSDT", "ETHUSDT"]:
            # Round to 3 decimals for major pairs
            order.quantity = Decimal(str(round(float(order.quantity), 3)))
        else:
            # Round to 2 decimals for others
            order.quantity = Decimal(str(round(float(order.quantity), 2)))
            
    async def post_order_behavior(self, order: Order, result: Dict[str, Any]) -> None:
        """Apply post-order behavioral patterns"""
        self.last_order_time = datetime.now()
        self.orders_this_session += 1
        
        # Occasionally cancel orders (professional re-evaluation)
        if random.random() < self.cancel_ratio:
            # Schedule cancellation after random delay
            cancel_delay = random.uniform(30, 300)  # 30s to 5min
            # This would schedule the cancellation
            pass
            
        # Session management
        if self.orders_this_session > 50:
            # Take a break after many orders
            break_duration = random.uniform(300, 900)  # 5-15 minutes
            await asyncio.sleep(break_duration)
            self.orders_this_session = 0
            
    def get_session_config(self) -> Dict[str, Any]:
        """Get session-specific configuration"""
        return {
            "max_orders_per_hour": 20,
            "preferred_order_types": ["LIMIT", "LIMIT_MAKER"],
            "risk_per_trade": 0.02,  # 2% per trade
            "use_stop_loss": True,
            "stop_loss_percentage": 0.015,  # 1.5%
            "take_profit_percentage": 0.03,  # 3%
            "pyramiding": True,  # Add to winning positions
            "max_positions": 10
        }
        
    def should_trade_symbol(self, symbol: str) -> bool:
        """Check if we should trade this symbol"""
        # Institutional traders focus on liquid pairs
        return symbol in self.preferred_pairs or symbol.endswith("USDT")
        
    def get_order_size(self, symbol: str, account_balance: Decimal) -> Decimal:
        """Calculate appropriate order size"""
        # Risk 2% of account per trade
        risk_amount = account_balance * Decimal("0.02")
        
        # But not more than typical institutional size
        max_size = self.avg_order_size * Decimal("2")
        
        return min(risk_amount, max_size)
