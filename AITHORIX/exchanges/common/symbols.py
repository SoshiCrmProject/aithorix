"""
Symbol formatting utilities for different exchanges
"""

from typing import Dict, Tuple


def parse_symbol(symbol: str) -> Tuple[str, str]:
    """Parse symbol into base and quote assets"""
    # Handle different formats
    if "/" in symbol:
        return tuple(symbol.split("/"))
    elif "-" in symbol:
        return tuple(symbol.split("-"))
    else:
        # Try common patterns
        common_quotes = ["USDT", "USDC", "BUSD", "BTC", "ETH"]
        for quote in common_quotes:
            if symbol.endswith(quote):
                base = symbol[:-len(quote)]
                return base, quote
        # Default
        return symbol[:3], symbol[3:]


def format_symbol(base: str, quote: str, exchange: str) -> str:
    """Format symbol for specific exchange"""
    formatters = {
        "binance": lambda b, q: f"{b}{q}",
        "hyperliquid": lambda b, q: f"{b}-{q}",
        "mexc": lambda b, q: f"{b}_{q}",
        "bybit": lambda b, q: f"{b}{q}",
        "okx": lambda b, q: f"{b}-{q}"
    }
    
    formatter = formatters.get(exchange.lower(), lambda b, q: f"{b}/{q}")
    return formatter(base, quote)


# Common symbol mappings
SYMBOL_MAPPINGS: Dict[str, Dict[str, str]] = {
    "BTC/USDT": {
        "binance": "BTCUSDT",
        "hyperliquid": "BTC-USD",
        "mexc": "BTC_USDT",
        "bybit": "BTCUSDT",
        "okx": "BTC-USDT"
    },
    "ETH/USDT": {
        "binance": "ETHUSDT",
        "hyperliquid": "ETH-USD",
        "mexc": "ETH_USDT",
        "bybit": "ETHUSDT",
        "okx": "ETH-USDT"
    }
}


def normalize_symbol(symbol: str, from_exchange: str, to_exchange: str) -> str:
    """Convert symbol format between exchanges"""
    # Check if we have a direct mapping
    if symbol in SYMBOL_MAPPINGS and to_exchange in SYMBOL_MAPPINGS[symbol]:
        return SYMBOL_MAPPINGS[symbol][to_exchange]
        
    # Otherwise, parse and reformat
    base, quote = parse_symbol(symbol)
    return format_symbol(base, quote, to_exchange)
