"""
AITHORIX Database Models
SQLAlchemy models for PostgreSQL
"""

from datetime import datetime
from decimal import Decimal
from typing import Optional
from uuid import uuid4

from sqlalchemy import (
    Column, String, Integer, DateTime, Numeric, Boolean, 
    ForeignKey, Index, Text, JSON, Enum as SQLEnum, 
    UniqueConstraint, CheckConstraint
)
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func

from ..engine.trading_engine import OrderType, OrderSide, OrderStatus

Base = declarative_base()


class TimestampMixin:
    """Mixin for created_at and updated_at timestamps"""
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False)


class Order(Base, TimestampMixin):
    """Order database model"""
    __tablename__ = "orders"
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid4)
    order_id = Column(String(64), unique=True, nullable=False, index=True)
    exchange_order_id = Column(String(128), index=True)
    symbol = Column(String(32), nullable=False, index=True)
    exchange = Column(String(32), nullable=False, index=True)
    side = Column(SQLEnum(OrderSide), nullable=False)
    order_type = Column(SQLEnum(OrderType), nullable=False)
    status = Column(SQLEnum(OrderStatus), nullable=False, index=True)
    
    quantity = Column(Numeric(20, 8), nullable=False)
    price = Column(Numeric(20, 8))
    stop_price = Column(Numeric(20, 8))
    filled_quantity = Column(Numeric(20, 8), default=0)
    average_price = Column(Numeric(20, 8))
    
    leverage = Column(Integer, default=1)
    reduce_only = Column(Boolean, default=False)
    post_only = Column(Boolean, default=False)
    time_in_force = Column(String(16), default="GTC")
    
    commission = Column(Numeric(20, 8), default=0)
    commission_asset = Column(String(16))
    
    signal_id = Column(UUID(as_uuid=True), ForeignKey("signals.id"))
    signal = relationship("Signal", back_populates="orders")
    
    trades = relationship("Trade", back_populates="order")
    
    __table_args__ = (
        Index("idx_orders_symbol_status", "symbol", "status"),
        Index("idx_orders_created_at", "created_at"),
    )


class Position(Base, TimestampMixin):
    """Position database model"""
    __tablename__ = "positions"
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid4)
    position_id = Column(String(64), unique=True, nullable=False, index=True)
    symbol = Column(String(32), nullable=False, index=True)
    exchange = Column(String(32), nullable=False, index=True)
    side = Column(SQLEnum(OrderSide), nullable=False)
    
    quantity = Column(Numeric(20, 8), nullable=False)
    entry_price = Column(Numeric(20, 8), nullable=False)
    mark_price = Column(Numeric(20, 8), nullable=False)
    liquidation_price = Column(Numeric(20, 8))
    
    unrealized_pnl = Column(Numeric(20, 8), default=0)
    realized_pnl = Column(Numeric(20, 8), default=0)
    
    leverage = Column(Integer, default=1)
    margin_type = Column(String(16), default="CROSS")
    
    is_active = Column(Boolean, default=True, index=True)
    closed_at = Column(DateTime(timezone=True))
    
    __table_args__ = (
        Index("idx_positions_symbol_active", "symbol", "is_active"),
        UniqueConstraint("symbol", "exchange", "is_active", 
                        name="uq_active_position_per_symbol_exchange"),
    )


class Signal(Base, TimestampMixin):
    """ML model signal database model"""
    __tablename__ = "signals"
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid4)
    model_id = Column(String(64), nullable=False, index=True)
    symbol = Column(String(32), nullable=False, index=True)
    side = Column(SQLEnum(OrderSide), nullable=False)
    
    confidence = Column(Numeric(5, 4), nullable=False)
    predicted_price = Column(Numeric(20, 8), nullable=False)
    predicted_timeframe = Column(Integer, nullable=False)  # minutes
    risk_score = Column(Numeric(5, 4), nullable=False)
    
    features = Column(JSON, nullable=False)
    
    orders = relationship("Order", back_populates="signal")
    
    __table_args__ = (
        Index("idx_signals_created_at", "created_at"),
        Index("idx_signals_model_symbol", "model_id", "symbol"),
        CheckConstraint("confidence >= 0 AND confidence <= 1", name="check_confidence_range"),
        CheckConstraint("risk_score >= 0 AND risk_score <= 1", name="check_risk_score_range"),
    )


class Trade(Base, TimestampMixin):
    """Executed trade database model"""
    __tablename__ = "trades"
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid4)
    trade_id = Column(String(128), unique=True, nullable=False, index=True)
    order_id = Column(UUID(as_uuid=True), ForeignKey("orders.id"), nullable=False)
    
    symbol = Column(String(32), nullable=False, index=True)
    exchange = Column(String(32), nullable=False)
    side = Column(SQLEnum(OrderSide), nullable=False)
    
    quantity = Column(Numeric(20, 8), nullable=False)
    price = Column(Numeric(20, 8), nullable=False)
    commission = Column(Numeric(20, 8), default=0)
    commission_asset = Column(String(16))
    
    is_maker = Column(Boolean, default=False)
    
    order = relationship("Order", back_populates="trades")
    
    __table_args__ = (
        Index("idx_trades_created_at", "created_at"),
        Index("idx_trades_symbol", "symbol"),
    )


class MarketData(Base):
    """Market data storage"""
    __tablename__ = "market_data"
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid4)
    timestamp = Column(DateTime(timezone=True), nullable=False, index=True)
    symbol = Column(String(32), nullable=False, index=True)
    exchange = Column(String(32), nullable=False)
    
    open = Column(Numeric(20, 8), nullable=False)
    high = Column(Numeric(20, 8), nullable=False)
    low = Column(Numeric(20, 8), nullable=False)
    close = Column(Numeric(20, 8), nullable=False)
    volume = Column(Numeric(20, 8), nullable=False)
    
    bid_price = Column(Numeric(20, 8))
    ask_price = Column(Numeric(20, 8))
    bid_volume = Column(Numeric(20, 8))
    ask_volume = Column(Numeric(20, 8))
    
    __table_args__ = (
        Index("idx_market_data_symbol_timestamp", "symbol", "timestamp"),
        UniqueConstraint("symbol", "exchange", "timestamp", 
                        name="uq_market_data_per_symbol_exchange_time"),
    )


class ModelPerformance(Base, TimestampMixin):
    """ML model performance tracking"""
    __tablename__ = "model_performance"
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid4)
    model_id = Column(String(64), nullable=False, index=True)
    date = Column(DateTime(timezone=True), nullable=False, index=True)
    
    predictions_count = Column(Integer, default=0)
    correct_predictions = Column(Integer, default=0)
    accuracy = Column(Numeric(5, 4))
    
    total_pnl = Column(Numeric(20, 8), default=0)
    win_rate = Column(Numeric(5, 4))
    sharpe_ratio = Column(Numeric(10, 4))
    
    avg_confidence = Column(Numeric(5, 4))
    avg_risk_score = Column(Numeric(5, 4))
    
    __table_args__ = (
        Index("idx_model_performance_model_date", "model_id", "date"),
        UniqueConstraint("model_id", "date", name="uq_model_performance_per_day"),
    )


class AccountBalance(Base, TimestampMixin):
    """Account balance tracking"""
    __tablename__ = "account_balances"
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid4)
    exchange = Column(String(32), nullable=False, index=True)
    asset = Column(String(16), nullable=False, index=True)
    
    free = Column(Numeric(20, 8), nullable=False)
    locked = Column(Numeric(20, 8), nullable=False)
    total = Column(Numeric(20, 8), nullable=False)
    
    __table_args__ = (
        UniqueConstraint("exchange", "asset", name="uq_balance_per_exchange_asset"),
    )


class RiskMetric(Base):
    """Risk metrics tracking"""
    __tablename__ = "risk_metrics"
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid4)
    timestamp = Column(DateTime(timezone=True), nullable=False, index=True)
    
    portfolio_value = Column(Numeric(20, 8), nullable=False)
    var_95 = Column(Numeric(20, 8))  # Value at Risk
    cvar_95 = Column(Numeric(20, 8))  # Conditional VaR
    max_drawdown = Column(Numeric(10, 4))
    sharpe_ratio = Column(Numeric(10, 4))
    
    position_count = Column(Integer, default=0)
    total_exposure = Column(Numeric(20, 8))
    leverage_ratio = Column(Numeric(10, 4))
    
    correlation_risk = Column(Numeric(5, 4))
    concentration_risk = Column(Numeric(5, 4))
    
    __table_args__ = (
        Index("idx_risk_metrics_timestamp", "timestamp"),
    )


class SystemLog(Base):
    """System event logging"""
    __tablename__ = "system_logs"
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid4)
    timestamp = Column(DateTime(timezone=True), nullable=False, index=True)
    level = Column(String(16), nullable=False, index=True)
    module = Column(String(64), nullable=False, index=True)
    
    message = Column(Text, nullable=False)
    details = Column(JSON)
    
    __table_args__ = (
        Index("idx_system_logs_timestamp_level", "timestamp", "level"),
        Index("idx_system_logs_module", "module"),
    )
