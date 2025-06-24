"""Helper functions for AITHORIX"""
from decimal import Decimal
from typing import Any, Dict, List

def calculate_position_size(balance: Decimal, risk_percent: float) -> Decimal:
    """Calculate position size based on risk percentage"""
    return balance * Decimal(str(risk_percent))
