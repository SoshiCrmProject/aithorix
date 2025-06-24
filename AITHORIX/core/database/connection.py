"""
AITHORIX Database Connection Manager
Production PostgreSQL connection with pooling
"""

import asyncio
from typing import Optional, Dict, Any
import asyncpg
from asyncpg.pool import Pool
import structlog

from ..exceptions import ConfigurationError

logger = structlog.get_logger()


class Database:
    """Database connection manager with connection pooling"""
    
    def __init__(self, dsn: Optional[str] = None):
        self.dsn = dsn
        self.pool: Optional[Pool] = None
        self._lock = asyncio.Lock()
        
    async def connect(self, **kwargs) -> None:
        """Create connection pool"""
        if self.pool:
            return
            
        async with self._lock:
            if self.pool:  # Double check
                return
                
            try:
                import os
                dsn = self.dsn or os.getenv("DATABASE_URL")
                
                if not dsn:
                    raise ConfigurationError("database_url", "DATABASE_URL not configured")
                
                self.pool = await asyncpg.create_pool(
                    dsn,
                    min_size=kwargs.get("min_size", 10),
                    max_size=kwargs.get("max_size", 20),
                    max_queries=kwargs.get("max_queries", 50000),
                    max_inactive_connection_lifetime=kwargs.get("max_inactive_connection_lifetime", 300),
                    command_timeout=kwargs.get("command_timeout", 60),
                    server_settings={
                        'jit': 'off'  # Disable JIT for consistent performance
                    }
                )
                
                # Test connection
                async with self.pool.acquire() as conn:
                    version = await conn.fetchval("SELECT version()")
                    logger.info(f"Connected to PostgreSQL: {version}")
                    
            except Exception as e:
                logger.error(f"Failed to connect to database: {e}")
                raise
                
    async def disconnect(self) -> None:
        """Close connection pool"""
        if self.pool:
            await self.pool.close()
            self.pool = None
            logger.info("Database connection pool closed")
            
    async def execute(self, query: str, *args, timeout: Optional[float] = None) -> str:
        """Execute a query"""
        if not self.pool:
            await self.connect()
            
        async with self.pool.acquire() as conn:
            return await conn.execute(query, *args, timeout=timeout)
            
    async def executemany(self, query: str, args: list, timeout: Optional[float] = None) -> None:
        """Execute many queries"""
        if not self.pool:
            await self.connect()
            
        async with self.pool.acquire() as conn:
            await conn.executemany(query, args, timeout=timeout)
            
    async def fetch(self, query: str, *args, timeout: Optional[float] = None) -> list:
        """Fetch multiple rows"""
        if not self.pool:
            await self.connect()
            
        async with self.pool.acquire() as conn:
            return await conn.fetch(query, *args, timeout=timeout)
            
    async def fetchrow(self, query: str, *args, timeout: Optional[float] = None) -> Optional[asyncpg.Record]:
        """Fetch single row"""
        if not self.pool:
            await self.connect()
            
        async with self.pool.acquire() as conn:
            return await conn.fetchrow(query, *args, timeout=timeout)
            
    async def fetchval(self, query: str, *args, column: int = 0, timeout: Optional[float] = None) -> Any:
        """Fetch single value"""
        if not self.pool:
            await self.connect()
            
        async with self.pool.acquire() as conn:
            return await conn.fetchval(query, *args, column=column, timeout=timeout)
            
    async def transaction(self):
        """Get a transaction context"""
        if not self.pool:
            await self.connect()
            
        conn = await self.pool.acquire()
        tx = conn.transaction()
        await tx.start()
        
        try:
            yield conn
            await tx.commit()
        except Exception:
            await tx.rollback()
            raise
        finally:
            await self.pool.release(conn)
            
    async def create_tables(self) -> None:
        """Create all database tables"""
        from .models import Base
        from sqlalchemy.ext.asyncio import create_async_engine
        
        # Convert DSN for SQLAlchemy
        import os
        dsn = self.dsn or os.getenv("DATABASE_URL")
        if dsn.startswith("postgresql://"):
            dsn = dsn.replace("postgresql://", "postgresql+asyncpg://", 1)
        elif dsn.startswith("postgres://"):
            dsn = dsn.replace("postgres://", "postgresql+asyncpg://", 1)
            
        engine = create_async_engine(dsn)
        
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
            
        await engine.dispose()
        logger.info("Database tables created")
        
    async def health_check(self) -> Dict[str, Any]:
        """Check database health"""
        if not self.pool:
            return {"status": "disconnected", "pool": None}
            
        try:
            async with self.pool.acquire() as conn:
                await conn.fetchval("SELECT 1")
                
            pool_size = self.pool.get_size()
            idle_size = self.pool.get_idle_size()
            
            return {
                "status": "healthy",
                "pool_size": pool_size,
                "idle_connections": idle_size,
                "active_connections": pool_size - idle_size
            }
        except Exception as e:
            return {
                "status": "unhealthy",
                "error": str(e)
            }
