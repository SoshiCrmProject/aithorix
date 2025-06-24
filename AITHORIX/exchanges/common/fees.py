"""
Fee structures for different exchanges
"""

from decimal import Decimal
from typing import Dict, Tuple

# Fee tiers: (maker_fee, taker_fee)
FEE_STRUCTURES: Dict[str, Dict[str, Tuple[Decimal, Decimal]]] = {
    "binance": {
        "spot": (Decimal("0.001"), Decimal("0.001")),  # 0.1%
        "futures": (Decimal("0.0002"), Decimal("0.0004")),  # 0.02% / 0.04%
        "options": (Decimal("0.0003"), Decimal("0.0003"))
    },
    "hyperliquid": {
        "perpetual": (Decimal("0.00025"), Decimal("0.0005"))  # 0.025% / 0.05%
    },
    "mexc": {
        "spot": (Decimal("0.002"), Decimal("0.002")),  # 0.2%
        "futures": (Decimal("0.0002"), Decimal("0.0006"))  # 0.02% / 0.06%
    },
    "bybit": {
        "spot": (Decimal("0.001"), Decimal("0.001")),
        "derivatives": (Decimal("0.00075"), Decimal("0.00075"))
    },
    "okx": {
        "spot": (Decimal("0.0008"), Decimal("0.001")),
        "futures": (Decimal("0.0008"), Decimal("0.001")),
        "options": (Decimal("0.0008"), Decimal("0.001"))
    }
}

# VIP discounts (simplified)
VIP_DISCOUNTS: Dict[str, Dict[int, float]] = {
    "binance": {
        1: 0.9,   # 10% discount
        2: 0.8,   # 20% discount
        3: 0.7,   # 30% discount
        4: 0.6,   # 40% discount
        5: 0.5    # 50% discount
    },
    "mexc": {
        1: 0.95,
        2: 0.9,
        3: 0.85,
        4: 0.8,
        5: 0.75
    }
}


def calculate_fee(
    exchange: str,
    market_type: str,
    is_maker: bool,
    notional: Decimal,
    vip_level: int = 0
) -> Decimal:
    """Calculate trading fee"""
    # Get base fee
    fee_structure = FEE_STRUCTURES.get(exchange, {}).get(market_type)
    if not fee_structure:
        # Default fee
        return notional * Decimal("0.001")
        
    base_fee_rate = fee_structure[0] if is_maker else fee_structure[1]
    
    # Apply VIP discount
    if vip_level > 0 and exchange in VIP_DISCOUNTS:
        discount = VIP_DISCOUNTS[exchange].get(vip_level, 1.0)
        base_fee_rate *= Decimal(str(discount))
        
    return notional * base_fee_rate
