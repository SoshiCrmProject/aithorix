"""
AITHORIX Core Trading System
Main entry point and application orchestration
"""

import asyncio
import signal
import sys
import uvloop
from contextlib import asynccontextmanager
from typing import Optional

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.middleware.trustedhost import TrustedHostMiddleware
from prometheus_fastapi_instrumentator import Instrumentator
from starlette.middleware.sessions import SessionMiddleware
import structlog

from core.api.rest_api import api_router
from core.auth.middleware import AuthenticationMiddleware, RateLimitMiddleware
from core.coordinator.strategy_coordinator import StrategyCoordinator
from core.coordinator.model_coordinator import ModelCoordinator
from core.coordinator.exchange_coordinator import ExchangeCoordinator
from core.database.connection import DatabaseManager
from core.cache.redis_manager import RedisManager
from core.engine.trading_engine import TradingEngine
from core.websocket.ws_server import WebSocketServer
from core.monitoring.metrics import MetricsCollector
from core.monitoring.health import HealthChecker
from core.config import settings
from core.constants import APP_NAME, APP_VERSION
from core.exceptions import handle_exceptions
from core.logging_config import setup_logging

# Configure structured logging
logger = structlog.get_logger(__name__)

# Use uvloop for better async performance
asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())


class AITHORIXApplication:
    """Main application class for AITHORIX trading system"""
    
    def __init__(self):
        self.app: Optional[FastAPI] = None
        self.trading_engine: Optional[TradingEngine] = None
        self.ws_server: Optional[WebSocketServer] = None
        self.db_manager: Optional[DatabaseManager] = None
        self.redis_manager: Optional[RedisManager] = None
        self.metrics_collector: Optional[MetricsCollector] = None
        self.health_checker: Optional[HealthChecker] = None
        self.strategy_coordinator: Optional[StrategyCoordinator] = None
        self.model_coordinator: Optional[ModelCoordinator] = None
        self.exchange_coordinator: Optional[ExchangeCoordinator] = None
        self._shutdown_event = asyncio.Event()
        
    async def startup(self):
        """Initialize all system components"""
        logger.info("Starting AITHORIX Trading System", version=APP_VERSION)
        
        try:
            # Initialize database connections
            self.db_manager = DatabaseManager()
            await self.db_manager.initialize()
            logger.info("Database initialized")
            
            # Initialize Redis cache
            self.redis_manager = RedisManager()
            await self.redis_manager.initialize()
            logger.info("Redis cache initialized")
            
            # Initialize metrics collector
            self.metrics_collector = MetricsCollector()
            await self.metrics_collector.initialize()
            logger.info("Metrics collector initialized")
            
            # Initialize health checker
            self.health_checker = HealthChecker(
                db_manager=self.db_manager,
                redis_manager=self.redis_manager
            )
            logger.info("Health checker initialized")
            
            # Initialize coordinators
            self.exchange_coordinator = ExchangeCoordinator(
                redis_manager=self.redis_manager
            )
            await self.exchange_coordinator.initialize()
            logger.info("Exchange coordinator initialized")
            
            self.model_coordinator = ModelCoordinator(
                redis_manager=self.redis_manager,
                metrics_collector=self.metrics_collector
            )
            await self.model_coordinator.initialize()
            logger.info("Model coordinator initialized with 175 models")
            
            self.strategy_coordinator = StrategyCoordinator(
                model_coordinator=self.model_coordinator,
                exchange_coordinator=self.exchange_coordinator,
                metrics_collector=self.metrics_collector
            )
            await self.strategy_coordinator.initialize()
            logger.info("Strategy coordinator initialized")
            
            # Initialize trading engine
            self.trading_engine = TradingEngine(
                strategy_coordinator=self.strategy_coordinator,
                exchange_coordinator=self.exchange_coordinator,
                db_manager=self.db_manager,
                redis_manager=self.redis_manager,
                metrics_collector=self.metrics_collector
            )
            await self.trading_engine.initialize()
            logger.info("Trading engine initialized")
            
            # Initialize WebSocket server
            self.ws_server = WebSocketServer(
                trading_engine=self.trading_engine,
                redis_manager=self.redis_manager
            )
            await self.ws_server.initialize()
            logger.info("WebSocket server initialized")
            
            # Start background tasks
            asyncio.create_task(self.trading_engine.run())
            asyncio.create_task(self.metrics_collector.run())
            asyncio.create_task(self.health_checker.run())
            asyncio.create_task(self.ws_server.run())
            
            logger.info(
                "AITHORIX Trading System started successfully",
                exchanges=len(self.exchange_coordinator.exchanges),
                models=len(self.model_coordinator.models),
                strategies=len(self.strategy_coordinator.strategies)
            )
            
        except Exception as e:
            logger.error("Failed to start AITHORIX", error=str(e), exc_info=True)
            await self.shutdown()
            raise
    
    async def shutdown(self):
        """Gracefully shutdown all system components"""
        logger.info("Shutting down AITHORIX Trading System")
        
        # Signal shutdown to all components
        self._shutdown_event.set()
        
        # Shutdown in reverse order of initialization
        shutdown_tasks = []
        
        if self.ws_server:
            shutdown_tasks.append(self.ws_server.shutdown())
            
        if self.trading_engine:
            shutdown_tasks.append(self.trading_engine.shutdown())
            
        if self.strategy_coordinator:
            shutdown_tasks.append(self.strategy_coordinator.shutdown())
            
        if self.model_coordinator:
            shutdown_tasks.append(self.model_coordinator.shutdown())
            
        if self.exchange_coordinator:
            shutdown_tasks.append(self.exchange_coordinator.shutdown())
            
        if self.metrics_collector:
            shutdown_tasks.append(self.metrics_collector.shutdown())
            
        if self.health_checker:
            shutdown_tasks.append(self.health_checker.shutdown())
            
        if self.redis_manager:
            shutdown_tasks.append(self.redis_manager.close())
            
        if self.db_manager:
            shutdown_tasks.append(self.db_manager.close())
        
        # Wait for all shutdowns to complete
        if shutdown_tasks:
            await asyncio.gather(*shutdown_tasks, return_exceptions=True)
        
        logger.info("AITHORIX Trading System shutdown complete")
    
    def create_app(self) -> FastAPI:
        """Create FastAPI application with all middleware and routes"""
        
        @asynccontextmanager
        async def lifespan(app: FastAPI):
            # Startup
            await self.startup()
            yield
            # Shutdown
            await self.shutdown()
        
        self.app = FastAPI(
            title=APP_NAME,
            version=APP_VERSION,
            description="Advanced AI Trading System with 175 ML Models",
            lifespan=lifespan,
            docs_url="/api/docs" if settings.DEBUG else None,
            redoc_url="/api/redoc" if settings.DEBUG else None,
            openapi_url="/api/openapi.json" if settings.DEBUG else None,
        )
        
        # Add middleware
        self._setup_middleware()
        
        # Add routes
        self._setup_routes()
        
        # Setup instrumentation
        self._setup_instrumentation()
        
        return self.app
    
    def _setup_middleware(self):
        """Configure application middleware"""
        
        # CORS middleware
        self.app.add_middleware(
            CORSMiddleware,
            allow_origins=settings.ALLOWED_ORIGINS,
            allow_credentials=True,
            allow_methods=["*"],
            allow_headers=["*"],
            expose_headers=["X-Request-ID", "X-Process-Time"],
        )
        
        # Trusted host middleware
        self.app.add_middleware(
            TrustedHostMiddleware,
            allowed_hosts=settings.ALLOWED_HOSTS
        )
        
        # GZip compression
        self.app.add_middleware(
            GZipMiddleware,
            minimum_size=1000,
            compresslevel=6
        )
        
        # Session middleware
        self.app.add_middleware(
            SessionMiddleware,
            secret_key=settings.SESSION_SECRET_KEY,
            max_age=settings.SESSION_TIMEOUT,
            same_site="lax",
            https_only=not settings.DEBUG,
        )
        
        # Custom authentication middleware
        self.app.add_middleware(AuthenticationMiddleware)
        
        # Rate limiting middleware
        self.app.add_middleware(
            RateLimitMiddleware,
            redis_manager=self.redis_manager
        )
        
        # Exception handling
        self.app.add_exception_handler(Exception, handle_exceptions)
    
    def _setup_routes(self):
        """Configure API routes"""
        
        # Health check endpoints
        @self.app.get("/health")
        async def health_check():
            return await self.health_checker.check_health()
        
        @self.app.get("/health/live")
        async def liveness_check():
            return {"status": "alive"}
        
        @self.app.get("/health/ready")
        async def readiness_check():
            return await self.health_checker.check_readiness()
        
        # Metrics endpoint
        @self.app.get("/metrics")
        async def metrics():
            return await self.metrics_collector.get_prometheus_metrics()
        
        # Include API router
        self.app.include_router(
            api_router,
            prefix="/api/v1"
        )
        
        # WebSocket endpoint
        self.app.websocket_route("/ws")(self.ws_server.websocket_endpoint)
    
    def _setup_instrumentation(self):
        """Setup monitoring instrumentation"""
        
        # Prometheus instrumentation
        instrumentator = Instrumentator(
            should_group_status_codes=True,
            should_ignore_untemplated=True,
            should_group_untemplated=False,
            should_round_latency_decimals=True,
            excluded_handlers=["/metrics", "/health.*"],
            inprogress_name="aithorix_inprogress",
            inprogress_labels=True,
        )
        
        instrumentator.instrument(self.app)
        
        # Custom metrics
        @instrumentator.add()
        async def add_custom_metrics(info: object):
            """Add custom business metrics"""
            if hasattr(info.response, "headers"):
                process_time = info.response.headers.get("X-Process-Time")
                if process_time:
                    self.metrics_collector.observe_api_latency(
                        info.request.url.path,
                        float(process_time),
                        info.response.status_code
                    )


# Global application instance
app_instance = AITHORIXApplication()
app = app_instance.create_app()


def handle_signals():
    """Setup signal handlers for graceful shutdown"""
    
    def signal_handler(sig, frame):
        logger.info(f"Received signal {sig}")
        asyncio.create_task(app_instance.shutdown())
        sys.exit(0)
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)


def run_trading():
    """Run the trading system (console script entry point)"""
    import uvicorn
    
    # Setup logging
    setup_logging()
    
    # Setup signal handlers
    handle_signals()
    
    # Run the application
    uvicorn.run(
        "core.main:app",
        host=settings.APP_HOST,
        port=settings.APP_PORT,
        workers=settings.APP_WORKERS if not settings.DEBUG else 1,
        reload=settings.DEBUG,
        access_log=settings.ENABLE_REQUEST_LOGGING,
        log_config=None,  # Use our custom logging
        server_header=False,
        date_header=False,
        limit_concurrency=1000,
        limit_max_requests=10000,
        timeout_keep_alive=5,
        ssl_keyfile=settings.SSL_KEYFILE if settings.SSL_ENABLED else None,
        ssl_certfile=settings.SSL_CERTFILE if settings.SSL_ENABLED else None,
        ssl_version=settings.SSL_VERSION if settings.SSL_ENABLED else None,
        ssl_ciphers=settings.SSL_CIPHERS if settings.SSL_ENABLED else None,
    )


if __name__ == "__main__":
    run_trading()