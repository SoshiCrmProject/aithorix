"""
Common utilities for exchange operations
"""

import hashlib
import hmac
import time
from decimal import Decimal
from typing import Any, Dict, List, Optional
from urllib.parse import urlencode


def create_signature(
    secret: str,
    message: str,
    algorithm: str = "sha256"
) -> str:
    """Create HMAC signature"""
    if algorithm == "sha256":
        return hmac.new(
            secret.encode('utf-8'),
            message.encode('utf-8'),
            hashlib.sha256
        ).hexdigest()
    elif algorithm == "sha512":
        return hmac.new(
            secret.encode('utf-8'),
            message.encode('utf-8'),
            hashlib.sha512
        ).hexdigest()
    else:
        raise ValueError(f"Unsupported algorithm: {algorithm}")


def create_query_string(params: Dict[str, Any]) -> str:
    """Create query string from parameters"""
    # Remove None values
    cleaned_params = {k: v for k, v in params.items() if v is not None}
    
    # Convert to strings
    str_params = {}
    for key, value in cleaned_params.items():
        if isinstance(value, bool):
            str_params[key] = "true" if value else "false"
        elif isinstance(value, (list, tuple)):
            str_params[key] = ",".join(str(v) for v in value)
        else:
            str_params[key] = str(value)
            
    return urlencode(str_params)


def get_timestamp() -> int:
    """Get current timestamp in milliseconds"""
    return int(time.time() * 1000)


def round_to_precision(value: Decimal, precision: int) -> Decimal:
    """Round decimal to specific precision"""
    if precision == 0:
        return Decimal(int(value))
    else:
        format_str = f"{{:.{precision}f}}"
        return Decimal(format_str.format(float(value)))


def calculate_required_margin(
    notional: Decimal,
    leverage: int,
    initial_margin_rate: Decimal = Decimal("0.01")
) -> Decimal:
    """Calculate required margin for leveraged position"""
    if leverage <= 0:
        leverage = 1
        
    base_margin = notional / Decimal(str(leverage))
    
    # Add initial margin requirement
    required = base_margin * (Decimal("1") + initial_margin_rate)
    
    return required


def merge_order_book_updates(
    current: Dict[Decimal, Decimal],
    updates: List[List[str]]
) -> Dict[Decimal, Decimal]:
    """Merge order book updates into current state"""
    for update in updates:
        price = Decimal(update[0])
        quantity = Decimal(update[1])
        
        if quantity == 0:
            # Remove price level
            current.pop(price, None)
        else:
            # Update price level
            current[price] = quantity
            
    return current


def calculate_vwap(trades: List[Dict[str, Any]]) -> Optional[Decimal]:
    """Calculate volume-weighted average price"""
    if not trades:
        return None
        
    total_volume = Decimal("0")
    total_value = Decimal("0")
    
    for trade in trades:
        price = Decimal(str(trade.get("price", 0)))
        quantity = Decimal(str(trade.get("quantity", 0)))
        
        total_volume += quantity
        total_value += price * quantity
        
    if total_volume == 0:
        return None
        
    return total_value / total_volume


def is_order_filled(
    order_status: str,
    filled_quantity: Decimal,
    total_quantity: Decimal
) -> bool:
    """Check if order is fully filled"""
    # Status-based check
    filled_statuses = ["FILLED", "COMPLETED", "DONE", "filled", "completed"]
    if order_status.upper() in [s.upper() for s in filled_statuses]:
        return True
        
    # Quantity-based check
    if filled_quantity >= total_quantity * Decimal("0.9999"):  # Allow tiny rounding errors
        return True
        
    return False


def normalize_order_status(exchange_status: str) -> str:
    """Normalize order status across exchanges"""
    status_upper = exchange_status.upper()
    
    # Map to standard statuses
    if status_upper in ["NEW", "OPEN", "ACTIVE", "CREATED"]:
        return "OPEN"
    elif status_upper in ["PARTIALLY_FILLED", "PARTIAL", "PART_FILLED"]:
        return "PARTIALLY_FILLED"
    elif status_upper in ["FILLED", "COMPLETED", "DONE", "EXECUTED"]:
        return "FILLED"
    elif status_upper in ["CANCELLED", "CANCELED", "CANCEL"]:
        return "CANCELLED"
    elif status_upper in ["REJECTED", "FAILED", "ERROR"]:
        return "REJECTED"
    elif status_upper in ["EXPIRED", "TIMEOUT"]:
        return "EXPIRED"
    else:
        return "UNKNOWN"
