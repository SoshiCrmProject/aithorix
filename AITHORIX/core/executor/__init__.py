"""
AITHORIX Executor Module
High-performance execution components for order, position, and trade management

This module provides the execution layer that bridges strategy decisions
with actual exchange interactions.
"""

from core.executor.order_executor import (
    OrderExecutor,
    OrderExecutionPlan,
    OrderExecutionStatus
)
from core.executor.position_executor import (
    PositionExecutor,
    PositionAction,
    PositionExecutionPlan,
    PositionAdjustment
)
from core.executor.trade_executor import (
    TradeExecutor,
    TradeExecution,
    ExecutionQuality,
    TradeReport
)

__all__ = [
    # Order Executor
    "OrderExecutor",
    "OrderExecutionPlan", 
    "OrderExecutionStatus",
    
    # Position Executor
    "PositionExecutor",
    "PositionAction",
    "PositionExecutionPlan",
    "PositionAdjustment",
    
    # Trade Executor
    "TradeExecutor",
    "TradeExecution",
    "ExecutionQuality",
    "TradeReport"
]

# Version info
__version__ = "1.0.0"
__author__ = "AITHORIX Team"