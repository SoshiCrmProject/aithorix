#!/usr/bin/env python3
"""
AITHORIX Main Entry Point
Advanced Trading Intelligence System

This is the main entry point for the AITHORIX trading system.
It initializes all components, starts the trading engine, and manages the system lifecycle.

Usage:
    python main.py [--config CONFIG_PATH] [--mode {production|development|testing}]
"""

import os
import sys
import signal
import asyncio
import logging
import argparse
from typing import Optional, Dict, Any
from datetime import datetime
import uvloop
import multiprocessing as mp
from pathlib import Path

# Add project root to Python path
sys.path.insert(0, str(Path(__file__).parent.parent))

# Performance: Use uvloop for faster async operations
asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

# Import core components
from core.engine.trading_engine import TradingEngine
from core.engine.market_data import MarketDataEngine
from core.engine.risk_engine import RiskEngine
from core.engine.execution_engine import ExecutionEngine
from core.coordinator.strategy_coordinator import StrategyCoordinator
from core.coordinator.model_coordinator import ModelCoordinator
from core.coordinator.exchange_coordinator import ExchangeCoordinator
from core.api.rest_api import create_app
from core.websocket.ws_server import WebSocketServer
from monitoring.health import HealthMonitor
from utils.helpers import load_config, setup_logging, get_version_info

# Global variables for graceful shutdown
trading_engine: Optional[TradingEngine] = None
shutdown_event = asyncio.Event()


class AITHORIXSystem:
    """Main system orchestrator for AITHORIX trading platform"""
    
    def __init__(self, config_path: str, mode: str = "production"):
        self.config_path = config_path
        self.mode = mode
        self.config = load_config(config_path)
        self.logger = logging.getLogger("AITHORIX.Main")
        
        # Core components
        self.trading_engine: Optional[TradingEngine] = None
        self.market_data_engine: Optional[MarketDataEngine] = None
        self.risk_engine: Optional[RiskEngine] = None
        self.execution_engine: Optional[ExecutionEngine] = None
        
        # Coordinators
        self.strategy_coordinator: Optional[StrategyCoordinator] = None
        self.model_coordinator: Optional[ModelCoordinator] = None
        self.exchange_coordinator: Optional[ExchangeCoordinator] = None
        
        # Services
        self.api_server: Optional[Any] = None
        self.ws_server: Optional[WebSocketServer] = None
        self.health_monitor: Optional[HealthMonitor] = None
        
        # System state
        self.is_running = False
        self.start_time: Optional[datetime] = None
        
    async def initialize(self) -> None:
        """Initialize all system components"""
        self.logger.info(f"Initializing AITHORIX System in {self.mode} mode...")
        
        try:
            # Initialize market data engine first (needed by others)
            self.logger.info("Initializing Market Data Engine...")
            self.market_data_engine = MarketDataEngine(self.config["market_data"])
            await self.market_data_engine.initialize()
            
            # Initialize risk engine
            self.logger.info("Initializing Risk Engine...")
            self.risk_engine = RiskEngine(self.config["risk"])
            await self.risk_engine.initialize()
            
            # Initialize execution engine
            self.logger.info("Initializing Execution Engine...")
            self.execution_engine = ExecutionEngine(self.config["execution"])
            await self.execution_engine.initialize()
            
            # Initialize coordinators
            self.logger.info("Initializing Exchange Coordinator...")
            self.exchange_coordinator = ExchangeCoordinator(self.config["exchanges"])
            await self.exchange_coordinator.initialize()
            
            self.logger.info("Initializing Model Coordinator...")
            self.model_coordinator = ModelCoordinator(self.config["models"])
            await self.model_coordinator.initialize()
            
            self.logger.info("Initializing Strategy Coordinator...")
            self.strategy_coordinator = StrategyCoordinator(
                self.config["strategies"],
                self.model_coordinator,
                self.exchange_coordinator
            )
            await self.strategy_coordinator.initialize()
            
            # Initialize trading engine with all components
            self.logger.info("Initializing Trading Engine...")
            self.trading_engine = TradingEngine(
                config=self.config["trading_engine"],
                market_data_engine=self.market_data_engine,
                risk_engine=self.risk_engine,
                execution_engine=self.execution_engine,
                strategy_coordinator=self.strategy_coordinator,
                model_coordinator=self.model_coordinator,
                exchange_coordinator=self.exchange_coordinator
            )
            await self.trading_engine.initialize()
            
            # Initialize API server
            self.logger.info("Initializing API Server...")
            self.api_server = create_app(self.trading_engine, self.config["api"])
            
            # Initialize WebSocket server
            self.logger.info("Initializing WebSocket Server...")
            self.ws_server = WebSocketServer(
                self.trading_engine,
                self.config["websocket"]
            )
            await self.ws_server.initialize()
            
            # Initialize health monitoring
            self.logger.info("Initializing Health Monitor...")
            self.health_monitor = HealthMonitor(self)
            await self.health_monitor.initialize()
            
            # Set global reference for signal handlers
            global trading_engine
            trading_engine = self.trading_engine
            
            self.logger.info("AITHORIX System initialization complete!")
            
        except Exception as e:
            self.logger.error(f"Failed to initialize system: {e}", exc_info=True)
            raise
    
    async def start(self) -> None:
        """Start all system components"""
        self.logger.info("Starting AITHORIX System...")
        self.start_time = datetime.utcnow()
        
        try:
            # Start market data feeds
            await self.market_data_engine.start()
            
            # Start exchange connections
            await self.exchange_coordinator.connect_all()
            
            # Load and validate ML models
            await self.model_coordinator.load_all_models()
            
            # Start risk monitoring
            await self.risk_engine.start()
            
            # Start strategy execution
            await self.strategy_coordinator.start()
            
            # Start trading engine
            await self.trading_engine.start()
            
            # Start API server in separate process
            api_process = mp.Process(
                target=self._run_api_server,
                args=(self.config["api"]["host"], self.config["api"]["port"])
            )
            api_process.start()
            
            # Start WebSocket server
            await self.ws_server.start()
            
            # Start health monitoring
            await self.health_monitor.start()
            
            self.is_running = True
            self.logger.info("AITHORIX System started successfully!")
            
            # Log system info
            version_info = get_version_info()
            self.logger.info(f"System Version: {version_info['version']}")
            self.logger.info(f"Trading Mode: {self.mode}")
            self.logger.info(f"Active Exchanges: {list(self.config['exchanges'].keys())}")
            self.logger.info(f"Active Models: {self.model_coordinator.get_active_model_count()}")
            self.logger.info(f"Active Strategies: {self.strategy_coordinator.get_active_strategy_count()}")
            
        except Exception as e:
            self.logger.error(f"Failed to start system: {e}", exc_info=True)
            await self.shutdown()
            raise
    
    async def run(self) -> None:
        """Main run loop"""
        await self.initialize()
        await self.start()
        
        # Wait for shutdown signal
        await shutdown_event.wait()
        
        await self.shutdown()
    
    async def shutdown(self) -> None:
        """Gracefully shutdown all components"""
        self.logger.info("Initiating graceful shutdown...")
        self.is_running = False
        
        try:
            # Stop accepting new trades
            if self.trading_engine:
                await self.trading_engine.stop_new_trades()
            
            # Close all positions if in emergency mode
            if self.mode == "emergency":
                self.logger.warning("Emergency shutdown - closing all positions...")
                if self.trading_engine:
                    await self.trading_engine.close_all_positions()
            
            # Stop components in reverse order
            if self.health_monitor:
                await self.health_monitor.stop()
                
            if self.ws_server:
                await self.ws_server.stop()
                
            if self.trading_engine:
                await self.trading_engine.stop()
                
            if self.strategy_coordinator:
                await self.strategy_coordinator.stop()
                
            if self.risk_engine:
                await self.risk_engine.stop()
                
            if self.exchange_coordinator:
                await self.exchange_coordinator.disconnect_all()
                
            if self.market_data_engine:
                await self.market_data_engine.stop()
                
            if self.model_coordinator:
                await self.model_coordinator.cleanup()
            
            # Calculate uptime
            if self.start_time:
                uptime = datetime.utcnow() - self.start_time
                self.logger.info(f"System uptime: {uptime}")
            
            self.logger.info("Graceful shutdown complete")
            
        except Exception as e:
            self.logger.error(f"Error during shutdown: {e}", exc_info=True)
    
    def _run_api_server(self, host: str, port: int) -> None:
        """Run API server in separate process"""
        import uvicorn
        uvicorn.run(
            self.api_server,
            host=host,
            port=port,
            log_level="info",
            access_log=True
        )
    
    def get_status(self) -> Dict[str, Any]:
        """Get current system status"""
        return {
            "running": self.is_running,
            "mode": self.mode,
            "start_time": self.start_time.isoformat() if self.start_time else None,
            "uptime": str(datetime.utcnow() - self.start_time) if self.start_time else None,
            "components": {
                "trading_engine": self.trading_engine.get_status() if self.trading_engine else "Not initialized",
                "market_data": self.market_data_engine.get_status() if self.market_data_engine else "Not initialized",
                "risk_engine": self.risk_engine.get_status() if self.risk_engine else "Not initialized",
                "exchanges": self.exchange_coordinator.get_status() if self.exchange_coordinator else "Not initialized",
                "models": self.model_coordinator.get_status() if self.model_coordinator else "Not initialized",
                "strategies": self.strategy_coordinator.get_status() if self.strategy_coordinator else "Not initialized",
            }
        }


def signal_handler(signum: int, frame: Any) -> None:
    """Handle system signals for graceful shutdown"""
    logger = logging.getLogger("AITHORIX.Main")
    logger.info(f"Received signal {signum}")
    
    # Trigger shutdown
    asyncio.create_task(shutdown_event.set())


def parse_arguments() -> argparse.Namespace:
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(
        description="AITHORIX Advanced Trading Intelligence System"
    )
    
    parser.add_argument(
        "--config",
        type=str,
        default="config/production.yaml",
        help="Path to configuration file"
    )
    
    parser.add_argument(
        "--mode",
        type=str,
        choices=["production", "development", "testing", "emergency"],
        default="production",
        help="System mode"
    )
    
    parser.add_argument(
        "--log-level",
        type=str,
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        default="INFO",
        help="Logging level"
    )
    
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Run in dry-run mode (no real trades)"
    )
    
    parser.add_argument(
        "--version",
        action="store_true",
        help="Show version information"
    )
    
    return parser.parse_args()


async def main() -> None:
    """Main entry point"""
    args = parse_arguments()
    
    # Show version and exit if requested
    if args.version:
        version_info = get_version_info()
        print(f"AITHORIX v{version_info['version']}")
        print(f"Build: {version_info['build']}")
        print(f"Python: {sys.version}")
        return
    
    # Setup logging
    setup_logging(args.log_level)
    logger = logging.getLogger("AITHORIX.Main")
    
    # Log startup
    logger.info("=" * 80)
    logger.info("AITHORIX Advanced Trading Intelligence System")
    logger.info(f"Version: {get_version_info()['version']}")
    logger.info(f"Mode: {args.mode}")
    logger.info(f"Config: {args.config}")
    logger.info("=" * 80)
    
    # Register signal handlers
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Create and run system
    try:
        system = AITHORIXSystem(args.config, args.mode)
        
        # Override dry-run if specified
        if args.dry_run:
            logger.info("DRY-RUN MODE ENABLED - No real trades will be executed")
            system.config["trading_engine"]["dry_run"] = True
        
        await system.run()
        
    except KeyboardInterrupt:
        logger.info("Keyboard interrupt received")
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    # Set process title
    try:
        import setproctitle
        setproctitle.setproctitle("aithorix-trading")
    except ImportError:
        pass
    
    # Run the system
    asyncio.run(main())