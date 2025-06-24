"""
AITHORIX System Constants
Production configuration values
"""

from decimal import Decimal
import os
from dotenv import load_dotenv

load_dotenv()

# Trading Constants
MAX_POSITION_SIZE = Decimal(os.getenv("MAX_POSITION_SIZE", "0.05"))
MAX_LEVERAGE = int(os.getenv("MAX_LEVERAGE", "20"))
STOP_LOSS_PERCENT = Decimal(os.getenv("STOP_LOSS_PERCENT", "0.02"))
TARGET_DAILY_RETURN = Decimal(os.getenv("TARGET_DAILY_RETURN", "0.20"))
MIN_TRADE_SIZE_USD = Decimal(os.getenv("MIN_TRADE_SIZE_USD", "50"))
MAX_DAILY_TRADES = int(os.getenv("MAX_TRADES_PER_DAY", "1200"))

# Risk Management
MAX_DRAWDOWN_PERCENT = Decimal(os.getenv("MAX_DRAWDOWN_PERCENT", "0.02"))
MAX_CORRELATION = float(os.getenv("MAX_CORRELATION", "0.7"))
MIN_SHARPE_RATIO = float(os.getenv("MIN_SHARPE_RATIO", "8.0"))
RISK_CHECK_INTERVAL = int(os.getenv("RISK_CHECK_INTERVAL", "60"))

# Model Configuration
MODEL_INFERENCE_TIMEOUT = int(os.getenv("MODEL_INFERENCE_TIMEOUT", "50"))
MODEL_BATCH_SIZE = int(os.getenv("MODEL_BATCH_SIZE", "32"))
MODEL_UPDATE_INTERVAL = int(os.getenv("MODEL_UPDATE_INTERVAL", "3600"))
MODEL_CONFIDENCE_THRESHOLD = 0.85
MODEL_COUNT = 175

# Exchange Configuration
EXCHANGES = ["binance", "hyperliquid", "mexc", "bybit", "okx"]
BINANCE_RATE_LIMIT = 1200  # requests per minute
HYPERLIQUID_RATE_LIMIT = 100  # requests per second
MEXC_RATE_LIMIT = 10  # requests per second
BYBIT_RATE_LIMIT = 600  # requests per minute
OKX_RATE_LIMIT = 300  # requests per minute

# System Configuration
REDIS_KEY_TTL = 300  # 5 minutes
DB_CONNECTION_TIMEOUT = 60
WEBSOCKET_RECONNECT_INTERVAL = 5
HEARTBEAT_INTERVAL = 30
LOG_ROTATION_SIZE = 100 * 1024 * 1024  # 100MB
MAX_LOG_FILES = 10

# Performance Targets
TARGET_WIN_RATE = 0.93
TARGET_EXECUTION_SPEED = 50  # milliseconds
TARGET_SLIPPAGE = 0.0005  # 0.05%
TARGET_UPTIME = 0.999  # 99.9%

# API Configuration
API_RATE_LIMIT = int(os.getenv("API_RATE_LIMIT", "1000"))
API_TIMEOUT = 30
JWT_ALGORITHM = "HS256"
JWT_EXPIRATION_HOURS = 24

# Monitoring
PROMETHEUS_PORT = int(os.getenv("PROMETHEUS_PORT", "9090"))
METRICS_INTERVAL = 15  # seconds
ALERT_COOLDOWN = 300  # 5 minutes

# Security
ENCRYPTION_ALGORITHM = "AES-256-GCM"
PASSWORD_MIN_LENGTH = 12
MAX_LOGIN_ATTEMPTS = 5
ACCOUNT_LOCKOUT_DURATION = 3600  # 1 hour
SESSION_TIMEOUT = 3600  # 1 hour
