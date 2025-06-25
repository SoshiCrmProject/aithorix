"""
AITHORIX Coordinator Module
Coordinates strategies, models, and exchanges

This module provides coordination between different system components:
- StrategyCoordinator: Manages trading strategies and signal generation
- ModelCoordinator: Manages ML models and predictions
- ExchangeCoordinator: Manages exchange connections and operations
"""

from .strategy_coordinator import StrategyCoordinator, Signal, Strategy
from .model_coordinator import ModelCoordinator, ModelPrediction, ModelInfo
from .exchange_coordinator import ExchangeCoordinator, ExchangeInfo, ExchangeStatus

__all__ = [
    "StrategyCoordinator",
    "Signal",
    "Strategy",
    "ModelCoordinator",
    "ModelPrediction",
    "ModelInfo",
    "ExchangeCoordinator",
    "ExchangeInfo",
    "ExchangeStatus",
]