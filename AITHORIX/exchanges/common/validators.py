"""
AITHORIX Common Exchange Validators
Input validation for exchange operations
"""

import re
from typing import Union, Optional
from decimal import Decimal

from ..base_exchange import OrderType, OrderSide
from .constants import (
    COMMON_TIMEFRAMES, 
    RISK_PARAMETERS,
    ORDER_TYPE_MAPPING,
    MARGIN_MODES
)


def validate_symbol(symbol: str) -> bool:
    """
    Validate trading symbol format
    
    Args:
        symbol: Trading symbol
        
    Returns:
        bool: True if valid
        
    Raises:
        ValueError: If invalid
    """
    if not symbol or not isinstance(symbol, str):
        raise ValueError("Symbol must be a non-empty string")
    
    # Check for valid characters
    if not re.match(r'^[A-Z0-9\-_/:]+$', symbol.upper()):
        raise ValueError(f"Invalid symbol format: {symbol}")
    
    # Check minimum length
    if len(symbol) < 3:
        raise ValueError(f"Symbol too short: {symbol}")
    
    return True


def validate_order_type(order_type: Union[str, OrderType]) -> OrderType:
    """
    Validate and normalize order type
    
    Args:
        order_type: Order type
        
    Returns:
        OrderType: Normalized order type
        
    Raises:
        ValueError: If invalid
    """
    if isinstance(order_type, OrderType):
        return order_type
    
    if not isinstance(order_type, str):
        raise ValueError("Order type must be string or OrderType enum")
    
    order_type_lower = order_type.lower()
    
    # Check against mapping
    for standard_type, variations in ORDER_TYPE_MAPPING.items():
        if order_type in variations or order_type_lower == standard_type:
            return OrderType(standard_type)
    
    raise ValueError(f"Invalid order type: {order_type}")


def validate_order_side(side: Union[str, OrderSide]) -> OrderSide:
    """
    Validate and normalize order side
    
    Args:
        side: Order side
        
    Returns:
        OrderSide: Normalized order side
        
    Raises:
        ValueError: If invalid
    """
    if isinstance(side, OrderSide):
        return side
    
    if not isinstance(side, str):
        raise ValueError("Side must be string or OrderSide enum")
    
    side_upper = side.upper()
    
    if side_upper in ['BUY', 'LONG']:
        return OrderSide.BUY
    elif side_upper in ['SELL', 'SHORT']:
        return OrderSide.SELL
    else:
        raise ValueError(f"Invalid order side: {side}")


def validate_timeframe(timeframe: str) -> str:
    """
    Validate timeframe format
    
    Args:
        timeframe: Timeframe string
        
    Returns:
        str: Valid timeframe
        
    Raises:
        ValueError: If invalid
    """
    if not timeframe or not isinstance(timeframe, str):
        raise ValueError("Timeframe must be a non-empty string")
    
    if timeframe not in COMMON_TIMEFRAMES:
        # Check if it matches pattern
        if not re.match(r'^\d+[mhdwM]$', timeframe):
            raise ValueError(f"Invalid timeframe format: {timeframe}")
    
    return timeframe


def validate_price(
    price: Union[float, Decimal, str],
    min_price: float = 0.00000001,
    max_price: float = 1000000000
) -> float:
    """
    Validate price value
    
    Args:
        price: Price value
        min_price: Minimum allowed price
        max_price: Maximum allowed price
        
    Returns:
        float: Valid price
        
    Raises:
        ValueError: If invalid
    """
    try:
        price_float = float(price)
    except (ValueError, TypeError):
        raise ValueError(f"Invalid price: {price}")
    
    if price_float <= 0:
        raise ValueError("Price must be positive")
    
    if price_float < min_price:
        raise ValueError(f"Price {price_float} below minimum {min_price}")
    
    if price_float > max_price:
        raise ValueError(f"Price {price_float} above maximum {max_price}")
    
    return price_float


def validate_quantity(
    quantity: Union[float, Decimal, str],
    min_quantity: float = 0.00001,
    max_quantity: Optional[float] = None,
    step_size: Optional[float] = None
) -> float:
    """
    Validate quantity/size value
    
    Args:
        quantity: Quantity value
        min_quantity: Minimum allowed quantity
        max_quantity: Maximum allowed quantity
        step_size: Required step size
        
    Returns:
        float: Valid quantity
        
    Raises:
        ValueError: If invalid
    """
    try:
        quantity_float = float(quantity)
    except (ValueError, TypeError):
        raise ValueError(f"Invalid quantity: {quantity}")
    
    if quantity_float <= 0:
        raise ValueError("Quantity must be positive")
    
    if quantity_float < min_quantity:
        raise ValueError(f"Quantity {quantity_float} below minimum {min_quantity}")
    
    if max_quantity and quantity_float > max_quantity:
        raise ValueError(f"Quantity {quantity_float} above maximum {max_quantity}")
    
    if step_size:
        # Check if quantity is multiple of step size
        remainder = quantity_float % step_size
        if remainder > 1e-8:  # Small tolerance for float precision
            raise ValueError(f"Quantity {quantity_float} not multiple of step size {step_size}")
    
    return quantity_float


def validate_leverage(
    leverage: Union[int, float],
    instrument_type: str = 'futures',
    max_leverage: Optional[int] = None
) -> int:
    """
    Validate leverage value
    
    Args:
        leverage: Leverage value
        instrument_type: Type of instrument
        max_leverage: Maximum allowed leverage
        
    Returns:
        int: Valid leverage
        
    Raises:
        ValueError: If invalid
    """
    try:
        leverage_int = int(leverage)
    except (ValueError, TypeError):
        raise ValueError(f"Invalid leverage: {leverage}")
    
    if leverage_int < 1:
        raise ValueError("Leverage must be at least 1")
    
    # Get max leverage for instrument type
    if not max_leverage:
        max_leverage = RISK_PARAMETERS['max_leverage'].get(
            instrument_type, 
            RISK_PARAMETERS['max_leverage']['default']
        )
    
    if leverage_int > max_leverage:
        raise ValueError(f"Leverage {leverage_int} exceeds maximum {max_leverage}")
    
    return leverage_int


def validate_margin_mode(margin_mode: str) -> str:
    """
    Validate margin mode
    
    Args:
        margin_mode: Margin mode string
        
    Returns:
        str: Valid margin mode
        
    Raises:
        ValueError: If invalid
    """
    if not margin_mode or not isinstance(margin_mode, str):
        raise ValueError("Margin mode must be a non-empty string")
    
    margin_mode_lower = margin_mode.lower()
    
    for standard_mode, variations in MARGIN_MODES.items():
        if margin_mode in variations or margin_mode_lower == standard_mode:
            return standard_mode
    
    raise ValueError(f"Invalid margin mode: {margin_mode}")


def validate_client_order_id(client_order_id: str, max_length: int = 36) -> str:
    """
    Validate client order ID
    
    Args:
        client_order_id: Client order ID
        max_length: Maximum allowed length
        
    Returns:
        str: Valid client order ID
        
    Raises:
        ValueError: If invalid
    """
    if not client_order_id or not isinstance(client_order_id, str):
        raise ValueError("Client order ID must be a non-empty string")
    
    if len(client_order_id) > max_length:
        raise ValueError(f"Client order ID too long (max {max_length} characters)")
    
    # Check for valid characters (alphanumeric, dash, underscore)
    if not re.match(r'^[a-zA-Z0-9\-_]+$', client_order_id):
        raise ValueError("Client order ID contains invalid characters")
    
    return client_order_id


def validate_datetime_range(
    start_time: Optional[datetime] = None,
    end_time: Optional[datetime] = None,
    max_range_days: int = 90
) -> Tuple[Optional[datetime], Optional[datetime]]:
    """
    Validate datetime range
    
    Args:
        start_time: Start time
        end_time: End time
        max_range_days: Maximum allowed range in days
        
    Returns:
        Tuple[Optional[datetime], Optional[datetime]]: Valid range
        
    Raises:
        ValueError: If invalid
    """
    if start_time and not isinstance(start_time, datetime):
        raise ValueError("Start time must be datetime object")
    
    if end_time and not isinstance(end_time, datetime):
        raise ValueError("End time must be datetime object")
    
    if start_time and end_time:
        if start_time >= end_time:
            raise ValueError("Start time must be before end time")
        
        # Check range
        range_days = (end_time - start_time).days
        if range_days > max_range_days:
            raise ValueError(f"Time range exceeds maximum {max_range_days} days")
    
    return start_time, end_time