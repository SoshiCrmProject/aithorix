"""
AITHORIX Main Application Entry Point
Production-ready FastAPI application
"""

import asyncio
import signal
import sys
from contextlib import asynccontextmanager
from typing import Dict, Any

from fastapi import FastAPI, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import JSONResponse
from prometheus_client import make_asgi_app
import structlog
import uvicorn

from .engine.trading_engine import TradingEngine
from .api.rest_api import api_router
from .websocket.ws_server import websocket_router
from .auth.middleware import AuthMiddleware
from .monitoring.metrics import setup_metrics
from .database.connection import Database
from .cache.redis_cache import RedisCache
from .exceptions import AithorixException
from .constants import PROMETHEUS_PORT


# Configure structured logging
structlog.configure(
    processors=[
        structlog.stdlib.filter_by_level,
        structlog.stdlib.add_logger_name,
        structlog.stdlib.add_log_level,
        structlog.stdlib.PositionalArgumentsFormatter(),
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
        structlog.processors.UnicodeDecoder(),
        structlog.processors.JSONRenderer()
    ],
    context_class=dict,
    logger_factory=structlog.stdlib.LoggerFactory(),
    cache_logger_on_first_use=True,
)

logger = structlog.get_logger()


class AithorixApp:
    """Main application class"""
    
    def __init__(self):
        self.trading_engine: Optional[TradingEngine] = None
        self.database: Optional[Database] = None
        self.redis: Optional[RedisCache] = None
        self.app: FastAPI = self._create_app()
        
    def _create_app(self) -> FastAPI:
        """Create FastAPI application with all middleware"""
        
        @asynccontextmanager
        async def lifespan(app: FastAPI):
            # Startup
            await self.startup()
            yield
            # Shutdown
            await self.shutdown()
        
        app = FastAPI(
            title="AITHORIX Trading System",
            description="Advanced AI Trading System with 175 ML Models",
            version="1.0.0",
            lifespan=lifespan
        )
        
        # Middleware
        app.add_middleware(
            CORSMiddleware,
            allow_origins=["https://aithorix.com"],
            allow_credentials=True,
            allow_methods=["*"],
            allow_headers=["*"],
        )
        app.add_middleware(GZipMiddleware, minimum_size=1000)
        app.add_middleware(AuthMiddleware)
        
        # Exception handler
        @app.exception_handler(AithorixException)
        async def aithorix_exception_handler(request: Request, exc: AithorixException):
            return JSONResponse(
                status_code=400,
                content={
                    "error": exc.error_code,
                    "message": str(exc),
                    "details": exc.details
                }
            )
        
        # Health check
        @app.get("/health")
        async def health_check():
            return {
                "status": "healthy",
                "version": "1.0.0",
                "engine_running": self.trading_engine.is_running if self.trading_engine else False
            }
        
        # Include routers
        app.include_router(api_router, prefix="/api/v1")
        app.include_router(websocket_router, prefix="/ws")
        
        # Metrics endpoint
        metrics_app = make_asgi_app()
        app.mount("/metrics", metrics_app)
        
        return app
        
    async def startup(self):
        """Initialize all system components"""
        logger.info("Starting AITHORIX Trading System")
        
        try:
            # Setup metrics
            setup_metrics()
            
            # Initialize database
            self.database = Database()
            await self.database.connect()
            
            # Initialize Redis
            self.redis = RedisCache()
            await self.redis.connect()
            
            # Initialize trading engine
            config = self._load_config()
            self.trading_engine = TradingEngine(config)
            await self.trading_engine.startup()
            
            logger.info("AITHORIX Trading System started successfully")
            
        except Exception as e:
            logger.error(f"Failed to start AITHORIX: {str(e)}")
            sys.exit(1)
            
    async def shutdown(self):
        """Gracefully shutdown all components"""
        logger.info("Shutting down AITHORIX Trading System")
        
        if self.trading_engine:
            await self.trading_engine.shutdown()
            
        if self.database:
            await self.database.disconnect()
            
        if self.redis:
            await self.redis.disconnect()
            
        logger.info("AITHORIX Trading System shutdown complete")
        
    def _load_config(self) -> Dict[str, Any]:
        """Load configuration from environment"""
        import os
        from dotenv import load_dotenv
        
        load_dotenv()
        
        return {
            "database_url": os.getenv("DATABASE_URL"),
            "redis_url": os.getenv("REDIS_URL"),
            "max_position_size": float(os.getenv("MAX_POSITION_SIZE", "0.05")),
            "max_leverage": int(os.getenv("MAX_LEVERAGE", "20")),
            "stop_loss_percent": float(os.getenv("STOP_LOSS_PERCENT", "0.02")),
            "target_daily_return": float(os.getenv("TARGET_DAILY_RETURN", "0.20")),
            "max_daily_trades": int(os.getenv("MAX_TRADES_PER_DAY", "1200"))
        }


# Create application instance
aithorix = AithorixApp()
app = aithorix.app


def signal_handler(sig, frame):
    """Handle shutdown signals"""
    logger.info(f"Received signal {sig}")
    asyncio.create_task(aithorix.shutdown())
    sys.exit(0)


# Register signal handlers
signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)


if __name__ == "__main__":
    uvicorn.run(
        "core.main:app",
        host="0.0.0.0",
        port=8000,
        log_config={
            "version": 1,
            "disable_existing_loggers": False,
            "formatters": {
                "default": {
                    "format": '{"time": "%(asctime)s", "level": "%(levelname)s", "message": "%(message)s"}',
                },
            },
            "handlers": {
                "default": {
                    "formatter": "default",
                    "class": "logging.StreamHandler",
                    "stream": "ext://sys.stdout",
                },
            },
            "root": {
                "level": "INFO",
                "handlers": ["default"],
            },
        }
    )
