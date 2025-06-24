"""Configuration for Binance institutional profile"""

# Trading session times (UTC)
TRADING_SESSIONS = {
    "london": {"start": "08:00", "end": "16:00"},
    "new_york": {"start": "13:00", "end": "21:00"},
    "asia": {"start": "00:00", "end": "08:00"}
}

# Preferred trading pairs by market cap
TIER1_PAIRS = ["BTCUSDT", "ETHUSDT"]
TIER2_PAIRS = ["BNBUSDT", "SOLUSDT", "ADAUSDT", "AVAXUSDT", "DOTUSDT", "MATICUSDT"]
TIER3_PAIRS = ["LINKUSDT", "UNIUSDT", "ATOMUSDT", "XLMUSDT", "VETUSDT"]

# Order characteristics
ORDER_PREFERENCES = {
    "use_iceberg": True,
    "iceberg_percentage": 0.1,  # Show 10% of total
    "prefer_limit_orders": True,
    "market_order_threshold": 100000,  # Use market orders above $100k for urgency
    "slippage_tolerance": 0.001,  # 0.1%
}

# Risk management
RISK_PARAMETERS = {
    "max_position_size": 0.05,  # 5% of portfolio
    "max_daily_loss": 0.02,     # 2% daily loss limit
    "max_correlated_positions": 3,
    "correlation_threshold": 0.7,
    "var_limit": 0.015,         # 1.5% VaR
}

# Behavioral patterns
BEHAVIORAL_PATTERNS = {
    "morning_routine_duration": 900,  # 15 minutes morning analysis
    "between_trade_delay": (60, 300),  # 1-5 minutes
    "position_check_interval": 300,    # Check positions every 5 minutes
    "news_reaction_delay": (30, 120),  # React to news in 30s-2min
    "end_of_day_cleanup": True,       # Close/reduce positions before session end
}
