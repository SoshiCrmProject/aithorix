"""Basic tests to verify setup"""

import pytest
from decimal import Decimal

from core.engine.trading_engine import Order, OrderType, OrderSide
from exchanges.base_exchange import MarketInfo


def test_order_creation():
    """Test order creation"""
    order = Order(
        order_id="TEST-001",
        timestamp=datetime.now(timezone.utc),
        symbol="BTC/USDT",
        side=OrderSide.BUY,
        order_type=OrderType.LIMIT,
        quantity=Decimal("0.001"),
        price=Decimal("45000"),
        status=OrderStatus.PENDING,
        exchange="binance"
    )
    
    assert order.order_id == "TEST-001"
    assert order.symbol == "BTC/USDT"
    assert order.quantity == Decimal("0.001")


def test_market_info():
    """Test market info"""
    market = MarketInfo(
        symbol="BTC/USDT",
        base_asset="BTC",
        quote_asset="USDT",
        min_quantity=Decimal("0.00001"),
        max_quantity=Decimal("10000"),
        quantity_precision=5,
        min_price=Decimal("0.01"),
        max_price=Decimal("1000000"),
        price_precision=2,
        min_notional=Decimal("10"),
        is_trading=True,
        maker_fee=Decimal("0.001"),
        taker_fee=Decimal("0.001"),
        last=Decimal("45000")
    )
    
    assert market.symbol == "BTC/USDT"
    assert market.min_quantity == Decimal("0.00001")
    assert market.is_trading is True


@pytest.mark.asyncio
async def test_basic_imports():
    """Test that all modules can be imported"""
    from core.main import app
    from exchanges.manager import ExchangeManager
    from models.ensemble import ModelEnsemble
    
    assert app is not None
