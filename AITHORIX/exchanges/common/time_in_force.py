"""
Time in Force mappings for different exchanges
"""

# Standard time in force options
TIF_GTC = "GTC"  # Good Till Cancelled
TIF_IOC = "IOC"  # Immediate or Cancel
TIF_FOK = "FOK"  # Fill or Kill
TIF_GTX = "GTX"  # Good Till Crossing (Post Only)

# Exchange-specific mappings
BINANCE_TIF = {
    "GTC": "GTC",
    "IOC": "IOC",
    "FOK": "FOK",
    "GTX": "GTX"
}

HYPERLIQUID_TIF = {
    "GTC": "Gtc",
    "IOC": "Ioc",
    "FOK": "Fok",
    "GTX": "Alo"  # Add Liquidity Only
}

MEXC_TIF = {
    "GTC": "GTC",
    "IOC": "IOC",
    "FOK": "FOK",
    "GTX": "MAKER_ONLY"
}

BYBIT_TIF = {
    "GTC": "GoodTillCancel",
    "IOC": "ImmediateOrCancel",
    "FOK": "FillOrKill",
    "GTX": "PostOnly"
}

OKX_TIF = {
    "GTC": "GTC",
    "IOC": "IOC",
    "FOK": "FOK",
    "GTX": "post_only"
}
