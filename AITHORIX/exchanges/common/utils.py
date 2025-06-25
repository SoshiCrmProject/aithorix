"""
AITHORIX Common Exchange Utilities
Helper functions for exchange operations
"""

import re
import math
from typing import Optional, Tuple, Union
from decimal import Decimal, ROUND_DOWN, ROUND_UP
from datetime import datetime, timedelta

from .constants import FUTURES_SUFFIXES, TIMEFRAME_MAPPINGS


def normalize_symbol(symbol: str, exchange: str) -> str:
    """
    Normalize symbol to common format (e.g., BTC/USDT)
    
    Args:
        symbol: Exchange-specific symbol
        exchange: Exchange name
        
    Returns:
        Normalized symbol
    """
    # Remove exchange-specific suffixes
    symbol = symbol.upper()
    
    # Handle different exchange formats
    if exchange == 'binance':
        # BTCUSDT -> BTC/USDT
        for quote in ['USDT', 'USDC', 'BUSD', 'BTC', 'ETH', 'BNB']:
            if symbol.endswith(quote):
                base = symbol[:-len(quote)]
                return f"{base}/{quote}"
    
    elif exchange == 'hyperliquid':
        # BTC-USD-PERP -> BTC/USD:PERP
        if '-' in symbol:
            parts = symbol.split('-')
            if len(parts) >= 2:
                base, quote = parts[0], parts[1]
                suffix = f":{parts[2]}" if len(parts) > 2 else ""
                return f"{base}/{quote}{suffix}"
    
    elif exchange == 'mexc':
        # BTC_USDT -> BTC/USDT
        if '_' in symbol:
            parts = symbol.split('_')
            if len(parts) == 2:
                return f"{parts[0]}/{parts[1]}"
    
    elif exchange == 'bybit':
        # BTCUSDT, BTCUSD -> BTC/USDT, BTC/USD
        for quote in ['USDT', 'USDC', 'USD', 'PERP']:
            if symbol.endswith(quote):
                base = symbol[:-len(quote)]
                suffix = ":PERP" if quote == 'PERP' else ""
                actual_quote = 'USDT' if quote == 'PERP' else quote
                return f"{base}/{actual_quote}{suffix}"
    
    elif exchange == 'okx':
        # BTC-USDT, BTC-USDT-SWAP -> BTC/USDT, BTC/USDT:SWAP
        if '-' in symbol:
            parts = symbol.split('-')
            if len(parts) >= 2:
                base, quote = parts[0], parts[1]
                suffix = f":{parts[2]}" if len(parts) > 2 else ""
                return f"{base}/{quote}{suffix}"
    
    # Default: assume it's already normalized or simple format
    if '/' not in symbol and '-' not in symbol and '_' not in symbol:
        # Try to detect common patterns
        for quote in ['USDT', 'USDC', 'USD', 'BTC', 'ETH']:
            if symbol.endswith(quote):
                base = symbol[:-len(quote)]
                return f"{base}/{quote}"
    
    return symbol


def denormalize_symbol(symbol: str, exchange: str) -> str:
    """
    Convert normalized symbol to exchange-specific format
    
    Args:
        symbol: Normalized symbol (e.g., BTC/USDT)
        exchange: Exchange name
        
    Returns:
        Exchange-specific symbol
    """
    # Parse normalized format
    if ':' in symbol:
        pair, suffix = symbol.split(':')
    else:
        pair, suffix = symbol, None
    
    if '/' in pair:
        base, quote = pair.split('/')
    else:
        return symbol  # Can't parse, return as is
    
    # Convert to exchange format
    if exchange == 'binance':
        # BTC/USDT -> BTCUSDT
        return f"{base}{quote}"
    
    elif exchange == 'hyperliquid':
        # BTC/USD:PERP -> BTC-USD-PERP
        if suffix:
            return f"{base}-{quote}-{suffix}"
        return f"{base}-{quote}"
    
    elif exchange == 'mexc':
        # BTC/USDT -> BTC_USDT
        if suffix and suffix == 'PERP':
            return f"{base}_{quote}"
        return f"{base}_{quote}"
    
    elif exchange == 'bybit':
        # BTC/USDT:PERP -> BTCUSDT
        if suffix == 'PERP':
            return f"{base}USDT"  # Bybit uses USDT for perps
        return f"{base}{quote}"
    
    elif exchange == 'okx':
        # BTC/USDT:SWAP -> BTC-USDT-SWAP
        if suffix:
            return f"{base}-{quote}-{suffix}"
        return f"{base}-{quote}"
    
    return symbol


def round_to_precision(
    value: Union[float, Decimal],
    precision: Union[float, Decimal],
    rounding: str = 'down'
) -> float:
    """
    Round value to specified precision
    
    Args:
        value: Value to round
        precision: Precision (e.g., 0.01 for 2 decimal places)
        rounding: 'up', 'down', or 'nearest'
        
    Returns:
        Rounded value
    """
    if precision == 0:
        return float(value)
    
    value = Decimal(str(value))
    precision = Decimal(str(precision))
    
    if rounding == 'down':
        return float(value.quantize(precision, rounding=ROUND_DOWN))
    elif rounding == 'up':
        return float(value.quantize(precision, rounding=ROUND_UP))
    else:  # nearest
        return float(value.quantize(precision))


def calculate_fee(
    size: float,
    price: float,
    fee_rate: float,
    is_maker: bool = False,
    fee_currency: Optional[str] = None
) -> Tuple[float, str]:
    """
    Calculate trading fee
    
    Args:
        size: Order size
        price: Order price
        fee_rate: Fee rate (e.g., 0.001 for 0.1%)
        is_maker: Whether order is maker
        fee_currency: Currency for fee
        
    Returns:
        Tuple of (fee_amount, fee_currency)
    """
    value = size * price
    fee = value * fee_rate
    
    if not fee_currency:
        fee_currency = 'USDT'  # Default
    
    return fee, fee_currency


def is_futures_symbol(symbol: str) -> bool:
    """Check if symbol is a futures contract"""
    symbol = symbol.upper()
    
    # Check for common futures patterns
    for suffix in FUTURES_SUFFIXES:
        if suffix in symbol:
            return True
    
    # Check for date patterns (e.g., BTC-250328)
    if re.search(r'-\d{6}', symbol):
        return True
    
    return False


def is_spot_symbol(symbol: str) -> bool:
    """Check if symbol is a spot pair"""
    return not is_futures_symbol(symbol)


def parse_timeframe(timeframe: str) -> Tuple[int, str]:
    """
    Parse timeframe string into value and unit
    
    Args:
        timeframe: Timeframe string (e.g., '1h', '5m')
        
    Returns:
        Tuple of (value, unit)
    """
    match = re.match(r'^(\d+)([mhdwM])$', timeframe)
    if not match:
        raise ValueError(f"Invalid timeframe: {timeframe}")
    
    value = int(match.group(1))
    unit = match.group(2)
    
    return value, unit


def convert_timeframe(timeframe: str, to_exchange: str) -> str:
    """
    Convert timeframe to exchange-specific format
    
    Args:
        timeframe: Standard timeframe (e.g., '1h')
        to_exchange: Target exchange
        
    Returns:
        Exchange-specific timeframe
    """
    mappings = TIMEFRAME_MAPPINGS.get(to_exchange, {})
    return mappings.get(timeframe, timeframe)


def get_timeframe_seconds(timeframe: str) -> int:
    """
    Get timeframe duration in seconds
    
    Args:
        timeframe: Timeframe string
        
    Returns:
        Duration in seconds
    """
    value, unit = parse_timeframe(timeframe)
    
    unit_seconds = {
        'm': 60,
        'h': 3600,
        'd': 86400,
        'w': 604800,
        'M': 2592000  # 30 days
    }
    
    return value * unit_seconds.get(unit, 60)


def calculate_position_value(
    size: float,
    price: float,
    contract_size: float = 1.0,
    is_inverse: bool = False
) -> float:
    """
    Calculate position value
    
    Args:
        size: Position size
        price: Current price
        contract_size: Contract size/multiplier
        is_inverse: Whether it's inverse contract
        
    Returns:
        Position value
    """
    if is_inverse:
        return size * contract_size / price
    else:
        return size * contract_size * price


def calculate_pnl(
    size: float,
    entry_price: float,
    exit_price: float,
    side: str,
    contract_size: float = 1.0,
    is_inverse: bool = False
) -> float:
    """
    Calculate profit/loss
    
    Args:
        size: Position size
        entry_price: Entry price
        exit_price: Exit/current price
        side: 'long' or 'short'
        contract_size: Contract size/multiplier
        is_inverse: Whether it's inverse contract
        
    Returns:
        PnL value
    """
    if is_inverse:
        if side.lower() == 'long':
            pnl = size * contract_size * (1/entry_price - 1/exit_price)
        else:  # short
            pnl = size * contract_size * (1/exit_price - 1/entry_price)
    else:
        if side.lower() == 'long':
            pnl = size * contract_size * (exit_price - entry_price)
        else:  # short
            pnl = size * contract_size * (entry_price - exit_price)
    
    return pnl


def calculate_liquidation_price(
    size: float,
    entry_price: float,
    side: str,
    leverage: float,
    margin: float,
    maintenance_margin_rate: float = 0.005,
    is_inverse: bool = False
) -> float:
    """
    Calculate liquidation price
    
    Args:
        size: Position size
        entry_price: Entry price
        side: 'long' or 'short'
        leverage: Leverage used
        margin: Initial margin
        maintenance_margin_rate: Maintenance margin rate
        is_inverse: Whether it's inverse contract
        
    Returns:
        Liquidation price
    """
    if leverage <= 0:
        return 0
    
    # Calculate based on margin and maintenance requirements
    if is_inverse:
        if side.lower() == 'long':
            liq_price = entry_price * leverage / (leverage + 1 - maintenance_margin_rate * leverage)
        else:  # short
            liq_price = entry_price * leverage / (leverage - 1 + maintenance_margin_rate * leverage)
    else:
        if side.lower() == 'long':
            liq_price = entry_price * (1 - 1/leverage + maintenance_margin_rate)
        else:  # short
            liq_price = entry_price * (1 + 1/leverage - maintenance_margin_rate)
    
    return max(liq_price, 0)