"""Configuration for Bybit professional profile"""

# Professional trading parameters
RISK_PARAMETERS = {
    "max_account_risk": 0.06,      # 6% total account risk
    "max_position_risk": 0.02,      # 2% per position
    "max_correlated_risk": 0.04,    # 4% correlated positions
    "stop_loss_required": True,
    "risk_reward_minimum": 1.5,     # Minimum 1.5:1 R:R
    "kelly_fraction": 0.25          # 25% Kelly Criterion
}

# Technical analysis settings
TECHNICAL_CONFIG = {
    "primary_timeframes": ["4h", "1h", "15m"],
    "confirmation_timeframes": ["1d", "4h"],
    "volume_profile_periods": 24,    # 24 hour volume profile
    "orderflow_depth": 100,          # Order book levels
    "funding_rate_threshold": 0.0001 # 0.01% funding
}

# Position management
POSITION_MANAGEMENT = {
    "scaling_levels": 3,             # Scale in over 3 levels
    "partial_take_profits": [0.25, 0.5, 0.25],  # TP percentages
    "trailing_stop_activation": 0.02, # Activate at 2% profit
    "trailing_stop_distance": 0.01,   # 1% trailing distance
    "breakeven_move": 0.01,          # Move stop to BE at 1%
    "time_based_exits": True         # Exit if position stalls
}

# Market conditions
MARKET_CONDITIONS = {
    "avoid_high_funding": True,
    "funding_threshold": 0.0005,     # 0.05% funding rate
    "avoid_low_volume": True,
    "min_volume_usd": 10000000,      # $10M daily volume
    "avoid_high_spread": True,
    "max_spread_percent": 0.001,     # 0.1% spread
    "news_avoidance_minutes": 30     # Avoid 30min around news
}

# Order execution
EXECUTION_CONFIG = {
    "prefer_limit_orders": True,
    "post_only_default": True,
    "iceberg_threshold": 50000,      # Use iceberg above $50k
    "iceberg_display_percent": 0.2,  # Show 20% of order
    "twap_threshold": 100000,        # TWAP above $100k
    "twap_duration_minutes": 30,     # 30 minute TWAP
    "retry_attempts": 3,
    "retry_delay_ms": 500
}
