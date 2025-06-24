"""
MEXC Retail Trader Profile
Mimics behavior of retail traders focusing on altcoins and new listings
"""

import asyncio
import random
from datetime import datetime, time
from decimal import Decimal
from typing import Dict, Any, Optional

from ....core.engine.trading_engine import Order, OrderType, OrderSide


class MEXCRetailProfile:
    """
    Behavioral profile for retail trading on MEXC
    """
    
    def __init__(self):
        # Retail characteristics
        self.avg_trade_size = Decimal("500")  # Smaller positions
        self.max_trade_size = Decimal("5000")
        self.min_trade_size = Decimal("50")
        
        # Trading behavior
        self.fomo_tendency = 0.3  # 30% chance of FOMO trading
        self.new_listing_interest = 0.8  # 80% interest in new listings
        self.social_influence = 0.6  # 60% influenced by social media
        self.stop_loss_usage = 0.4  # Only 40% use stop losses
        
        # Timing patterns
        self.preferred_hours = [(6, 10), (18, 23)]  # Morning and evening
        self.weekend_trading = True
        self.impulse_trading = 0.2  # 20% impulse trades
        
        # Altcoin preferences
        self.prefers_low_cap = True
        self.target_market_cap = "10M-500M"
        self.avoids_top_10 = False
        
        # Session tracking
        self.trades_today = 0
        self.daily_pnl = Decimal("0")
        self.winning_streak = 0
        self.losing_streak = 0
        
    async def pre_order_behavior(self, order: Order) -> None:
        """Apply retail trader pre-order behavior"""
        # Check if FOMO triggered
        if random.random() < self.fomo_tendency:
            # FOMO behavior - market order and larger size
            if order.order_type == OrderType.LIMIT:
                order.order_type = OrderType.MARKET
            order.quantity = order.quantity * Decimal("1.5")
            
        # Round to "retail-friendly" numbers
        total_value = order.quantity * order.price
        if total_value < 1000:
            # Round to nearest $50
            rounded_value = round(float(total_value) / 50) * 50
            order.quantity = Decimal(str(rounded_value)) / order.price
        else:
            # Round to nearest $100
            rounded_value = round(float(total_value) / 100) * 100
            order.quantity = Decimal(str(rounded_value)) / order.price
            
        # Retail traders often use round prices
        if order.order_type == OrderType.LIMIT:
            # Round to 2-4 significant figures
            price_str = str(order.price)
            if order.price > 100:
                # Round to nearest dollar
                order.price = Decimal(str(round(float(order.price))))
            elif order.price > 1:
                # Round to 2 decimals
                order.price = Decimal(str(round(float(order.price), 2)))
            else:
                # Keep 4 decimals for small prices
                order.price = Decimal(str(round(float(order.price), 4)))
                
        # Retail delay patterns
        if self.trades_today > 0:
            # Quick successive trades when excited
            delay = random.uniform(5, 30)
        else:
            # Longer initial analysis
            delay = random.uniform(30, 120)
            
        await asyncio.sleep(delay)
        
        # Emotional trading adjustments
        if self.winning_streak > 2:
            # Overconfidence - increase position size
            order.quantity = order.quantity * Decimal("1.2")
        elif self.losing_streak > 2:
            # Fear - decrease position size
            order.quantity = order.quantity * Decimal("0.8")
            
        # Ensure within retail limits
        total_value = order.quantity * order.price
        if total_value > self.max_trade_size:
            order.quantity = self.max_trade_size / order.price
        elif total_value < self.min_trade_size:
            order.quantity = self.min_trade_size / order.price
            
    async def post_order_behavior(self, order: Order, result: Dict[str, Any]) -> None:
        """Apply retail trader post-order behavior"""
        self.trades_today += 1
        
        # Emotional response to results
        if result.get("status") == "FILLED":
            # Excitement after fill
            logger.info(f"Order filled! Waiting to see what happens...")
            
            # Retail traders often watch positions closely
            watch_duration = random.uniform(60, 300)  # 1-5 minutes
            await asyncio.sleep(watch_duration)
            
        # No stop loss for many retail traders
        if random.random() > self.stop_loss_usage:
            # Skip stop loss
            logger.debug("Retail trader skipping stop loss")
            
    async def should_trade_new_listing(self, symbol: str) -> bool:
        """Determine if should trade new listing"""
        # High interest in new listings
        if random.random() < self.new_listing_interest:
            # Additional checks for scam avoidance
            
            # Avoid if name is too long (often scams)
            if len(symbol) > 10:
                return False
                
            # Avoid if contains certain keywords
            scam_keywords = ["MOON", "SAFE", "ELON", "DOGE", "SHIB", "FLOKI"]
            symbol_upper = symbol.upper()
            for keyword in scam_keywords:
                if keyword in symbol_upper and keyword != symbol_upper:
                    return False
                    
            return True
            
        return False
        
    async def adjust_for_new_listing(self, order: Order, time_since_listing: float) -> Order:
        """Adjust order for new listing dynamics"""
        if time_since_listing < 300:  # First 5 minutes
            # Very volatile - use market orders
            order.order_type = OrderType.MARKET
            # Smaller position due to high risk
            order.quantity = order.quantity * Decimal("0.5")
            
        elif time_since_listing < 3600:  # First hour
            # Still volatile - wider spreads
            if order.order_type == OrderType.LIMIT:
                if order.side == OrderSide.BUY:
                    # Pay up to get filled
                    order.price = order.price * Decimal("1.02")
                else:
                    # Sell lower to ensure fill
                    order.price = order.price * Decimal("0.98")
                    
        return order
        
    def get_retail_config(self) -> Dict[str, Any]:
        """Get retail trading configuration"""
        return {
            "position_sizing": "fixed_dollar",  # Not percentage based
            "max_positions": 10,  # Diversification
            "use_technical_analysis": True,
            "indicators": ["RSI", "MACD", "Volume"],
            "news_trading": True,
            "social_media_signals": True,
            "preferred_sources": ["Twitter", "Telegram", "Reddit"],
            "risk_per_trade": 0.05,  # 5% account risk
            "revenge_trading": self.losing_streak > 3,
            "celebration_trading": self.winning_streak > 3
        }
        
    def adjust_for_social_signal(self, order: Order, signal_source: str) -> Order:
        """Adjust order based on social media signals"""
        signal_multipliers = {
            "twitter_influencer": 1.5,
            "telegram_group": 1.2,
            "reddit_post": 1.1,
            "discord_alert": 1.3
        }
        
        multiplier = signal_multipliers.get(signal_source, 1.0)
        order.quantity = order.quantity * Decimal(str(multiplier))
        
        # FOMO usually means market orders
        if multiplier > 1.2:
            order.order_type = OrderType.MARKET
            
        return order
