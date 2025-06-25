"""
AITHORIX Configuration Management
Centralized settings using Pydantic for type safety and validation
"""

import os
import secrets
from decimal import Decimal
from typing import List, Optional, Dict, Any, Set
from functools import lru_cache
from pathlib import Path

from pydantic import BaseSettings, Field, SecretStr, validator, root_validator
from pydantic.networks import HttpUrl, PostgresDsn, RedisDsn, MongoDsn, AnyUrl


class Settings(BaseSettings):
    """Main configuration settings for AITHORIX"""
    
    # ==============================================================================
    # SYSTEM CONFIGURATION
    # ==============================================================================
    
    # Environment
    ENVIRONMENT: str = Field("development", env="ENVIRONMENT")
    DEBUG: bool = Field(False, env="DEBUG")
    LOG_LEVEL: str = Field("INFO", env="LOG_LEVEL")
    TIMEZONE: str = Field("UTC", env="TIMEZONE")
    
    # Application
    APP_NAME: str = Field("AITHORIX", env="APP_NAME")
    APP_VERSION: str = Field("1.0.0", env="APP_VERSION")
    APP_HOST: str = Field("0.0.0.0", env="APP_HOST")
    APP_PORT: int = Field(8000, env="APP_PORT")
    APP_WORKERS: int = Field(4, env="APP_WORKERS")
    
    # ==============================================================================
    # SECURITY & AUTHENTICATION
    # ==============================================================================
    
    # JWT Configuration
    JWT_SECRET_KEY: SecretStr = Field(..., env="JWT_SECRET_KEY")
    JWT_ALGORITHM: str = Field("HS256", env="JWT_ALGORITHM")
    JWT_EXPIRATION_HOURS: int = Field(24, env="JWT_EXPIRATION_HOURS")
    JWT_REFRESH_TOKEN_EXPIRES_DAYS: int = Field(30, env="JWT_REFRESH_TOKEN_EXPIRES_DAYS")
    
    # API Keys
    API_KEY_SALT: SecretStr = Field(..., env="API_KEY_SALT")
    MASTER_API_KEY: Optional[SecretStr] = Field(None, env="MASTER_API_KEY")
    
    # Encryption
    ENCRYPTION_KEY: SecretStr = Field(..., env="ENCRYPTION_KEY")
    ENCRYPTION_ALGORITHM: str = Field("AES-256-GCM", env="ENCRYPTION_ALGORITHM")
    
    # Authentication
    ENABLE_2FA: bool = Field(True, env="ENABLE_2FA")
    OTP_SECRET_KEY: SecretStr = Field(..., env="OTP_SECRET_KEY")
    PASSWORD_MIN_LENGTH: int = Field(12, env="PASSWORD_MIN_LENGTH")
    MAX_LOGIN_ATTEMPTS: int = Field(5, env="MAX_LOGIN_ATTEMPTS")
    ACCOUNT_LOCKOUT_DURATION: int = Field(3600, env="ACCOUNT_LOCKOUT_DURATION")
    SESSION_SECRET_KEY: SecretStr = Field(
        default_factory=lambda: SecretStr(secrets.token_urlsafe(32)),
        env="SESSION_SECRET_KEY"
    )
    SESSION_TIMEOUT: int = Field(3600, env="SESSION_TIMEOUT")
    
    # CORS & Security Headers
    ALLOWED_ORIGINS: List[str] = Field(
        ["http://localhost:3000", "https://app.aithorix.ai"],
        env="ALLOWED_ORIGINS"
    )
    ALLOWED_HOSTS: List[str] = Field(
        ["localhost", "127.0.0.1", "app.aithorix.ai", "api.aithorix.ai"],
        env="ALLOWED_HOSTS"
    )
    
    # SSL Configuration
    SSL_ENABLED: bool = Field(False, env="SSL_ENABLED")
    SSL_KEYFILE: Optional[str] = Field(None, env="SSL_KEYFILE")
    SSL_CERTFILE: Optional[str] = Field(None, env="SSL_CERTFILE")
    SSL_VERSION: Optional[int] = Field(None, env="SSL_VERSION")
    SSL_CIPHERS: Optional[str] = Field(None, env="SSL_CIPHERS")
    
    # ==============================================================================
    # DATABASE CONFIGURATION
    # ==============================================================================
    
    # PostgreSQL Primary
    DATABASE_URL: PostgresDsn = Field(..., env="DATABASE_URL")
    DATABASE_POOL_SIZE: int = Field(20, env="DATABASE_POOL_SIZE")
    DATABASE_POOL_TIMEOUT: int = Field(30, env="DATABASE_POOL_TIMEOUT")
    DATABASE_POOL_RECYCLE: int = Field(3600, env="DATABASE_POOL_RECYCLE")
    DATABASE_ECHO: bool = Field(False, env="DATABASE_ECHO")
    
    # PostgreSQL Read Replicas
    DATABASE_READ_REPLICAS: List[PostgresDsn] = Field([], env="DATABASE_READ_REPLICAS")
    
    # Redis
    REDIS_URL: RedisDsn = Field(..., env="REDIS_URL")
    REDIS_PASSWORD: Optional[SecretStr] = Field(None, env="REDIS_PASSWORD")
    REDIS_POOL_SIZE: int = Field(10, env="REDIS_POOL_SIZE")
    REDIS_DECODE_RESPONSES: bool = Field(True, env="REDIS_DECODE_RESPONSES")
    REDIS_SOCKET_TIMEOUT: int = Field(5, env="REDIS_SOCKET_TIMEOUT")
    REDIS_SOCKET_CONNECT_TIMEOUT: int = Field(5, env="REDIS_SOCKET_CONNECT_TIMEOUT")
    REDIS_KEY_TTL: int = Field(300, env="REDIS_KEY_TTL")
    
    # MongoDB
    MONGODB_URL: Optional[MongoDsn] = Field(None, env="MONGODB_URL")
    MONGODB_DATABASE: str = Field("aithorix", env="MONGODB_DATABASE")
    
    # ==============================================================================
    # EXCHANGE CONFIGURATION
    # ==============================================================================
    
    # Binance
    BINANCE_API_KEY: Optional[SecretStr] = Field(None, env="BINANCE_API_KEY")
    BINANCE_API_SECRET: Optional[SecretStr] = Field(None, env="BINANCE_API_SECRET")
    BINANCE_TESTNET: bool = Field(False, env="BINANCE_TESTNET")
    BINANCE_FUTURES_LEVERAGE: int = Field(20, env="BINANCE_FUTURES_LEVERAGE")
    BINANCE_SPOT_ENABLED: bool = Field(True, env="BINANCE_SPOT_ENABLED")
    BINANCE_FUTURES_ENABLED: bool = Field(True, env="BINANCE_FUTURES_ENABLED")
    BINANCE_RATE_LIMIT: int = Field(1200, description="Requests per minute")
    
    # Hyperliquid
    HYPERLIQUID_API_KEY: Optional[SecretStr] = Field(None, env="HYPERLIQUID_API_KEY")
    HYPERLIQUID_API_SECRET: Optional[SecretStr] = Field(None, env="HYPERLIQUID_API_SECRET")
    HYPERLIQUID_PRIVATE_KEY: Optional[SecretStr] = Field(None, env="HYPERLIQUID_PRIVATE_KEY")
    HYPERLIQUID_CHAIN_ID: int = Field(42161, env="HYPERLIQUID_CHAIN_ID")
    HYPERLIQUID_GAS_LIMIT: int = Field(500000, env="HYPERLIQUID_GAS_LIMIT")
    HYPERLIQUID_GAS_PRICE_GWEI: Decimal = Field(Decimal("0.1"), env="HYPERLIQUID_GAS_PRICE_GWEI")
    HYPERLIQUID_RATE_LIMIT: int = Field(100, description="Requests per second")
    
    # MEXC
    MEXC_API_KEY: Optional[SecretStr] = Field(None, env="MEXC_API_KEY")
    MEXC_API_SECRET: Optional[SecretStr] = Field(None, env="MEXC_API_SECRET")
    MEXC_PASSPHRASE: Optional[SecretStr] = Field(None, env="MEXC_PASSPHRASE")
    MEXC_SPOT_ENABLED: bool = Field(True, env="MEXC_SPOT_ENABLED")
    MEXC_FUTURES_ENABLED: bool = Field(True, env="MEXC_FUTURES_ENABLED")
    MEXC_RATE_LIMIT: int = Field(10, description="Requests per second")
    
    # Bybit
    BYBIT_API_KEY: Optional[SecretStr] = Field(None, env="BYBIT_API_KEY")
    BYBIT_API_SECRET: Optional[SecretStr] = Field(None, env="BYBIT_API_SECRET")
    BYBIT_TESTNET: bool = Field(False, env="BYBIT_TESTNET")
    BYBIT_DERIVATIVES_ENABLED: bool = Field(True, env="BYBIT_DERIVATIVES_ENABLED")
    BYBIT_RATE_LIMIT: int = Field(600, description="Requests per minute")
    
    # OKX
    OKX_API_KEY: Optional[SecretStr] = Field(None, env="OKX_API_KEY")
    OKX_API_SECRET: Optional[SecretStr] = Field(None, env="OKX_API_SECRET")
    OKX_PASSPHRASE: Optional[SecretStr] = Field(None, env="OKX_PASSPHRASE")
    OKX_TESTNET: bool = Field(False, env="OKX_TESTNET")
    OKX_RATE_LIMIT: int = Field(300, description="Requests per minute")
    
    # Exchange Selection
    ENABLED_EXCHANGES: Set[str] = Field(
        {"binance", "hyperliquid", "mexc", "bybit", "okx"},
        env="ENABLED_EXCHANGES"
    )
    
    # ==============================================================================
    # TRADING CONFIGURATION
    # ==============================================================================
    
    # Position Limits
    MAX_POSITION_SIZE: Decimal = Field(Decimal("0.05"), env="MAX_POSITION_SIZE")
    MAX_LEVERAGE: int = Field(20, env="MAX_LEVERAGE")
    MAX_OPEN_POSITIONS: int = Field(50, env="MAX_OPEN_POSITIONS")
    MAX_POSITION_AGE_HOURS: int = Field(24, env="MAX_POSITION_AGE_HOURS")
    
    # Risk Management
    MAX_DRAWDOWN_PERCENT: Decimal = Field(Decimal("0.02"), env="MAX_DRAWDOWN_PERCENT")
    STOP_LOSS_PERCENT: Decimal = Field(Decimal("0.02"), env="STOP_LOSS_PERCENT")
    TAKE_PROFIT_PERCENT: Decimal = Field(Decimal("0.10"), env="TAKE_PROFIT_PERCENT")
    MAX_DAILY_LOSS_PERCENT: Decimal = Field(Decimal("0.01"), env="MAX_DAILY_LOSS_PERCENT")
    MAX_CORRELATION: float = Field(0.7, env="MAX_CORRELATION")
    MIN_SHARPE_RATIO: float = Field(8.0, env="MIN_SHARPE_RATIO")
    RISK_CHECK_INTERVAL: int = Field(60, env="RISK_CHECK_INTERVAL")
    
    # Trading Parameters
    MIN_TRADE_SIZE_USD: Decimal = Field(Decimal("50"), env="MIN_TRADE_SIZE_USD")
    MAX_TRADE_SIZE_USD: Decimal = Field(Decimal("100000"), env="MAX_TRADE_SIZE_USD")
    MAX_DAILY_TRADES: int = Field(1200, env="MAX_DAILY_TRADES")
    TRADING_HOURS_UTC: str = Field("0-23", env="TRADING_HOURS_UTC")
    ENABLE_WEEKEND_TRADING: bool = Field(True, env="ENABLE_WEEKEND_TRADING")
    
    # Execution
    ORDER_TIMEOUT_SECONDS: int = Field(30, env="ORDER_TIMEOUT_SECONDS")
    FILL_OR_KILL_TIMEOUT: int = Field(5, env="FILL_OR_KILL_TIMEOUT")
    SLIPPAGE_TOLERANCE: Decimal = Field(Decimal("0.001"), env="SLIPPAGE_TOLERANCE")
    USE_LIMIT_ORDERS: bool = Field(True, env="USE_LIMIT_ORDERS")
    USE_REDUCE_ONLY: bool = Field(True, env="USE_REDUCE_ONLY")
    
    # Performance Targets
    TARGET_DAILY_RETURN: Decimal = Field(Decimal("0.20"), env="TARGET_DAILY_RETURN")
    TARGET_WIN_RATE: float = Field(0.93, env="TARGET_WIN_RATE")
    TARGET_EXECUTION_SPEED: int = Field(50, env="TARGET_EXECUTION_SPEED")
    TARGET_SLIPPAGE: Decimal = Field(Decimal("0.0005"), env="TARGET_SLIPPAGE")
    TARGET_UPTIME: float = Field(0.999, env="TARGET_UPTIME")
    
    # ==============================================================================
    # MODEL CONFIGURATION
    # ==============================================================================
    
    # Model Settings
    MODEL_INFERENCE_TIMEOUT: int = Field(50, env="MODEL_INFERENCE_TIMEOUT")
    MODEL_BATCH_SIZE: int = Field(32, env="MODEL_BATCH_SIZE")
    MODEL_UPDATE_INTERVAL: int = Field(3600, env="MODEL_UPDATE_INTERVAL")
    MODEL_CONFIDENCE_THRESHOLD: float = Field(0.85, env="MODEL_CONFIDENCE_THRESHOLD")
    MODEL_ENSEMBLE_VOTING: str = Field("weighted", env="MODEL_ENSEMBLE_VOTING")
    MODEL_COUNT: int = Field(175, description="Total number of models")
    
    # GPU Configuration
    CUDA_VISIBLE_DEVICES: str = Field("0,1,2,3", env="CUDA_VISIBLE_DEVICES")
    ENABLE_GPU: bool = Field(True, env="ENABLE_GPU")
    GPU_MEMORY_FRACTION: float = Field(0.8, env="GPU_MEMORY_FRACTION")
    MIXED_PRECISION_TRAINING: bool = Field(True, env="MIXED_PRECISION_TRAINING")
    
    # Model Paths
    MODEL_WEIGHTS_PATH: Path = Field(Path("/data/models/weights"), env="MODEL_WEIGHTS_PATH")
    MODEL_CONFIG_PATH: Path = Field(Path("/data/models/configs"), env="MODEL_CONFIG_PATH")
    MODEL_CHECKPOINT_PATH: Path = Field(Path("/data/models/checkpoints"), env="MODEL_CHECKPOINT_PATH")
    
    # ==============================================================================
    # MONITORING & ALERTING
    # ==============================================================================
    
    # Prometheus
    PROMETHEUS_ENABLED: bool = Field(True, env="PROMETHEUS_ENABLED")
    PROMETHEUS_PORT: int = Field(9090, env="PROMETHEUS_PORT")
    PROMETHEUS_METRICS_PATH: str = Field("/metrics", env="PROMETHEUS_METRICS_PATH")
    METRICS_INTERVAL: int = Field(15, env="METRICS_INTERVAL")
    
    # Grafana
    GRAFANA_ENABLED: bool = Field(True, env="GRAFANA_ENABLED")
    GRAFANA_PORT: int = Field(3000, env="GRAFANA_PORT")
    GRAFANA_ADMIN_PASSWORD: Optional[SecretStr] = Field(None, env="GRAFANA_ADMIN_PASSWORD")
    
    # Alerting
    ALERT_WEBHOOK_URL: Optional[HttpUrl] = Field(None, env="ALERT_WEBHOOK_URL")
    TELEGRAM_BOT_TOKEN: Optional[SecretStr] = Field(None, env="TELEGRAM_BOT_TOKEN")
    TELEGRAM_CHAT_ID: Optional[str] = Field(None, env="TELEGRAM_CHAT_ID")
    DISCORD_WEBHOOK_URL: Optional[HttpUrl] = Field(None, env="DISCORD_WEBHOOK_URL")
    EMAIL_ALERTS_ENABLED: bool = Field(True, env="EMAIL_ALERTS_ENABLED")
    SMS_ALERTS_ENABLED: bool = Field(False, env="SMS_ALERTS_ENABLED")
    ALERT_COOLDOWN: int = Field(300, env="ALERT_COOLDOWN")
    
    # Logging
    LOG_TO_FILE: bool = Field(True, env="LOG_TO_FILE")
    LOG_FILE_PATH: Path = Field(Path("/logs/aithorix.log"), env="LOG_FILE_PATH")
    LOG_ROTATION_SIZE: int = Field(104857600, env="LOG_ROTATION_SIZE")
    LOG_RETENTION_DAYS: int = Field(30, env="LOG_RETENTION_DAYS")
    LOG_FORMAT: str = Field("json", env="LOG_FORMAT")
    ENABLE_REQUEST_LOGGING: bool = Field(True, env="ENABLE_REQUEST_LOGGING")
    
    # ==============================================================================
    # PERFORMANCE & OPTIMIZATION
    # ==============================================================================
    
    # Caching
    ENABLE_CACHING: bool = Field(True, env="ENABLE_CACHING")
    CACHE_TTL_SECONDS: int = Field(300, env="CACHE_TTL_SECONDS")
    CACHE_MAX_SIZE: int = Field(1000, env="CACHE_MAX_SIZE")
    USE_MEMORY_CACHE: bool = Field(True, env="USE_MEMORY_CACHE")
    USE_REDIS_CACHE: bool = Field(True, env="USE_REDIS_CACHE")
    
    # Threading & Concurrency
    MAX_WORKERS: int = Field(16, env="MAX_WORKERS")
    THREAD_POOL_SIZE: int = Field(32, env="THREAD_POOL_SIZE")
    ASYNC_POOL_SIZE: int = Field(1000, env="ASYNC_POOL_SIZE")
    ENABLE_UVLOOP: bool = Field(True, env="ENABLE_UVLOOP")
    
    # Resource Limits
    MAX_MEMORY_MB: int = Field(32768, env="MAX_MEMORY_MB")
    MAX_CPU_PERCENT: int = Field(80, env="MAX_CPU_PERCENT")
    
    # Network
    WEBSOCKET_RECONNECT_INTERVAL: int = Field(5, env="WEBSOCKET_RECONNECT_INTERVAL")
    HEARTBEAT_INTERVAL: int = Field(30, env="HEARTBEAT_INTERVAL")
    DB_CONNECTION_TIMEOUT: int = Field(60, env="DB_CONNECTION_TIMEOUT")
    API_TIMEOUT: int = Field(30, env="API_TIMEOUT")
    API_RATE_LIMIT: int = Field(1000, env="API_RATE_LIMIT")
    
    # ==============================================================================
    # MESSAGING & QUEUES
    # ==============================================================================
    
    # Kafka
    KAFKA_BOOTSTRAP_SERVERS: str = Field("localhost:9092", env="KAFKA_BOOTSTRAP_SERVERS")
    KAFKA_SECURITY_PROTOCOL: str = Field("PLAINTEXT", env="KAFKA_SECURITY_PROTOCOL")
    KAFKA_USERNAME: Optional[str] = Field(None, env="KAFKA_USERNAME")
    KAFKA_PASSWORD: Optional[SecretStr] = Field(None, env="KAFKA_PASSWORD")
    
    # Celery
    CELERY_BROKER_URL: Optional[str] = Field(None, env="CELERY_BROKER_URL")
    CELERY_RESULT_BACKEND: Optional[str] = Field(None, env="CELERY_RESULT_BACKEND")
    
    # ==============================================================================
    # EXTERNAL SERVICES
    # ==============================================================================
    
    # AWS
    AWS_ACCESS_KEY_ID: Optional[str] = Field(None, env="AWS_ACCESS_KEY_ID")
    AWS_SECRET_ACCESS_KEY: Optional[SecretStr] = Field(None, env="AWS_SECRET_ACCESS_KEY")
    AWS_REGION: str = Field("us-east-1", env="AWS_REGION")
    S3_BUCKET_NAME: Optional[str] = Field(None, env="S3_BUCKET_NAME")
    
    # Blockchain RPCs
    ETH_RPC_URL: Optional[AnyUrl] = Field(None, env="ETH_RPC_URL")
    BSC_RPC_URL: Optional[AnyUrl] = Field(None, env="BSC_RPC_URL")
    POLYGON_RPC_URL: Optional[AnyUrl] = Field(None, env="POLYGON_RPC_URL")
    ARBITRUM_RPC_URL: Optional[AnyUrl] = Field(None, env="ARBITRUM_RPC_URL")
    
    # ==============================================================================
    # DEVELOPMENT & TESTING
    # ==============================================================================
    
    # Testing
    TEST_DATABASE_URL: Optional[PostgresDsn] = Field(None, env="TEST_DATABASE_URL")
    TEST_REDIS_URL: Optional[RedisDsn] = Field(None, env="TEST_REDIS_URL")
    ENABLE_TEST_MODE: bool = Field(False, env="ENABLE_TEST_MODE")
    
    # Feature Flags
    ENABLE_PAPER_TRADING: bool = Field(True, env="ENABLE_PAPER_TRADING")
    ENABLE_BACKTESTING: bool = Field(True, env="ENABLE_BACKTESTING")
    ENABLE_LIVE_TRADING: bool = Field(False, env="ENABLE_LIVE_TRADING")
    ENABLE_EXPERIMENTAL_MODELS: bool = Field(False, env="ENABLE_EXPERIMENTAL_MODELS")
    
    # ==============================================================================
    # STEALTH & ANTI-DETECTION
    # ==============================================================================
    
    # Behavior Simulation
    ENABLE_HUMAN_SIMULATION: bool = Field(True, env="ENABLE_HUMAN_SIMULATION")
    TYPING_SPEED_WPM_MIN: int = Field(40, env="TYPING_SPEED_WPM_MIN")
    TYPING_SPEED_WPM_MAX: int = Field(80, env="TYPING_SPEED_WPM_MAX")
    MOUSE_MOVEMENT_SPEED: str = Field("normal", env="MOUSE_MOVEMENT_SPEED")
    CLICK_DELAY_MS_MIN: int = Field(50, env="CLICK_DELAY_MS_MIN")
    CLICK_DELAY_MS_MAX: int = Field(200, env="CLICK_DELAY_MS_MAX")
    
    # Request Patterns
    REQUEST_DELAY_MS_MIN: int = Field(100, env="REQUEST_DELAY_MS_MIN")
    REQUEST_DELAY_MS_MAX: int = Field(500, env="REQUEST_DELAY_MS_MAX")
    USER_AGENT_ROTATION: bool = Field(True, env="USER_AGENT_ROTATION")
    PROXY_ROTATION: bool = Field(True, env="PROXY_ROTATION")
    PROXY_LIST_URL: Optional[HttpUrl] = Field(None, env="PROXY_LIST_URL")
    
    # Session Management
    SESSION_DURATION_MIN: int = Field(1800, env="SESSION_DURATION_MIN")
    SESSION_DURATION_MAX: int = Field(7200, env="SESSION_DURATION_MAX")
    SESSION_ROTATION_PROBABILITY: float = Field(0.1, env="SESSION_ROTATION_PROBABILITY")
    
    # ==============================================================================
    # VALIDATORS
    # ==============================================================================
    
    @validator("ENVIRONMENT")
    def validate_environment(cls, v):
        allowed = {"development", "staging", "production"}
        if v not in allowed:
            raise ValueError(f"Environment must be one of {allowed}")
        return v
    
    @validator("LOG_LEVEL")
    def validate_log_level(cls, v):
        allowed = {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}
        if v.upper() not in allowed:
            raise ValueError(f"Log level must be one of {allowed}")
        return v.upper()
    
    @validator("MODEL_ENSEMBLE_VOTING")
    def validate_ensemble_voting(cls, v):
        allowed = {"simple", "weighted", "stacked"}
        if v not in allowed:
            raise ValueError(f"Ensemble voting must be one of {allowed}")
        return v
    
    @validator("DATABASE_READ_REPLICAS", pre=True)
    def parse_read_replicas(cls, v):
        if isinstance(v, str):
            return [url.strip() for url in v.split(",") if url.strip()]
        return v
    
    @validator("ENABLED_EXCHANGES", pre=True)
    def parse_enabled_exchanges(cls, v):
        if isinstance(v, str):
            return set(ex.strip().lower() for ex in v.split(",") if ex.strip())
        return v
    
    @validator("CUDA_VISIBLE_DEVICES", pre=True)
    def validate_cuda_devices(cls, v):
        if isinstance(v, str):
            devices = v.strip()
            if devices and not all(d.isdigit() for d in devices.split(",")):
                raise ValueError("CUDA_VISIBLE_DEVICES must be comma-separated integers")
        return v
    
    @root_validator
    def validate_trading_hours(cls, values):
        trading_hours = values.get("TRADING_HOURS_UTC", "0-23")
        try:
            parts = trading_hours.split("-")
            if len(parts) == 2:
                start, end = int(parts[0]), int(parts[1])
                if not (0 <= start <= 23 and 0 <= end <= 23):
                    raise ValueError
        except:
            raise ValueError("TRADING_HOURS_UTC must be in format 'START-END' (0-23)")
        return values
    
    @root_validator
    def validate_exchange_configs(cls, values):
        """Ensure at least one exchange is properly configured"""
        enabled_exchanges = values.get("ENABLED_EXCHANGES", set())
        
        exchange_configs = {
            "binance": (values.get("BINANCE_API_KEY"), values.get("BINANCE_API_SECRET")),
            "hyperliquid": (values.get("HYPERLIQUID_API_KEY"), values.get("HYPERLIQUID_PRIVATE_KEY")),
            "mexc": (values.get("MEXC_API_KEY"), values.get("MEXC_API_SECRET")),
            "bybit": (values.get("BYBIT_API_KEY"), values.get("BYBIT_API_SECRET")),
            "okx": (values.get("OKX_API_KEY"), values.get("OKX_API_SECRET")),
        }
        
        configured = [
            exchange for exchange, (key, secret) in exchange_configs.items()
            if key and secret and exchange in enabled_exchanges
        ]
        
        if not configured and values.get("ENABLE_LIVE_TRADING"):
            raise ValueError(
                "At least one exchange must be properly configured for live trading"
            )
        
        return values
    
    @root_validator
    def validate_risk_limits(cls, values):
        """Ensure risk parameters are within safe bounds"""
        max_leverage = values.get("MAX_LEVERAGE", 20)
        max_position_size = values.get("MAX_POSITION_SIZE", Decimal("0.05"))
        
        if max_leverage > 100:
            raise ValueError("MAX_LEVERAGE cannot exceed 100x")
        
        if max_position_size > Decimal("0.10"):
            raise ValueError("MAX_POSITION_SIZE cannot exceed 10% of portfolio")
        
        return values
    
    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        case_sensitive = True
        use_enum_values = True
        
        # Custom field serialization
        json_encoders = {
            SecretStr: lambda v: v.get_secret_value() if v else None,
            Path: lambda v: str(v),
            Decimal: lambda v: str(v),
        }


@lru_cache()
def get_settings() -> Settings:
    """Get cached settings instance"""
    return Settings()


# Global settings instance
settings = get_settings()