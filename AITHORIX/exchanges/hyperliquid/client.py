"""
AITHORIX Hyperliquid Exchange Implementation
DeFi perpetuals exchange with low fees and high performance
"""

import asyncio
import time
import json
from datetime import datetime, timezone
from decimal import Decimal
from typing import Dict, List, Optional, Any, AsyncGenerator
from eth_account import Account
from eth_account.messages import encode_defunct
import aiohttp

from ..base_exchange import (
    BaseExchange, ExchangeCredentials, MarketInfo, 
    OrderBook, Ticker, Balance, WebSocketManager
)
from ..common import (
    HYPERLIQUID_ORDER_TYPES, HYPERLIQUID_TIF,
    get_timestamp, normalize_order_status
)
from ...core.engine.trading_engine import Order, OrderType, OrderSide
from ...core.exceptions import OrderExecutionError, ExchangeConnectionError
from ...stealth.profiles.hyperliquid_defi import HyperliquidDeFiProfile


class HyperliquidExchange(BaseExchange):
    """
    Hyperliquid exchange implementation
    """
    
    def __init__(self, credentials: ExchangeCredentials, **kwargs):
        super().__init__(credentials, **kwargs)
        
        self.name = "hyperliquid"
        self.base_url = "https://api.hyperliquid.xyz"
        self.ws_url = "wss://api.hyperliquid.xyz/ws"
        
        # L1 chain for settlement
        self.chain_id = 421613  # Arbitrum testnet or mainnet
        
        # Ethereum account for signing
        self.account = Account.from_key(credentials.api_secret)
        self.address = getattr(credentials, 'wallet_address', self.account.address)
        
        # Rate limits
        self.rate_limits = {
            "default": 100,  # requests per second
            "orders": 20     # orders per second
        }
        
        # Apply DeFi profile
        self.profile = HyperliquidDeFiProfile()
        
    def _sign_request(self, method: str, path: str, params: Dict[str, Any]) -> Dict[str, str]:
        """Sign request for Hyperliquid API"""
        # Create message to sign
        timestamp = get_timestamp()
        message = f"{timestamp}{method}{path}{json.dumps(params, sort_keys=True)}"
        
        # Sign message
        message_hash = encode_defunct(text=message)
        signed_message = self.account.sign_message(message_hash)
        
        return {
            "X-Wallet": self.address,
            "X-Timestamp": str(timestamp),
            "X-Signature": signed_message.signature.hex()
        }
        
    async def get_markets(self) -> List[MarketInfo]:
        """Get all available markets on Hyperliquid"""
        response = await self._make_request("GET", "/info/meta")
        
        markets = []
        for asset_info in response.get("universe", []):
            symbol = asset_info["name"]
            
            market = MarketInfo(
                symbol=f"{symbol}-USD",
                base_asset=symbol,
                quote_asset="USD",
                min_quantity=Decimal(str(asset_info.get("minSize", "0.001"))),
                max_quantity=Decimal(str(asset_info.get("maxSize", "1000000"))),
                quantity_precision=asset_info.get("szDecimals", 3),
                min_price=Decimal("0.00001"),
                max_price=Decimal("1000000"),
                price_precision=asset_info.get("pxDecimals", 5),
                min_notional=Decimal("10"),  # $10 minimum
                is_trading=True,
                maker_fee=Decimal("0.00025"),  # 0.025%
                taker_fee=Decimal("0.0005"),   # 0.05%
                last=Decimal("0")
            )
            
            markets.append(market)
            
        return markets
        
    async def get_ticker(self, symbol: str) -> Ticker:
        """Get current ticker for symbol"""
        # Convert symbol format
        base = symbol.split("-")[0]
        
        response = await self._make_request(
            "POST",
            "/info/spot",
            params={"coin": base}
        )
        
        ticker_data = response.get("ticker", {})
        
        return Ticker(
            timestamp=datetime.now(timezone.utc),
            symbol=symbol,
            bid=Decimal(str(ticker_data.get("bid", "0"))),
            ask=Decimal(str(ticker_data.get("ask", "0"))),
            last=Decimal(str(ticker_data.get("last", "0"))),
            volume_24h=Decimal(str(ticker_data.get("volume24h", "0"))),
            high_24h=Decimal(str(ticker_data.get("high24h", "0"))),
            low_24h=Decimal(str(ticker_data.get("low24h", "0"))),
            change_24h=Decimal(str(ticker_data.get("change24h", "0")))
        )
        
    async def get_order_book(self, symbol: str, limit: int = 100) -> OrderBook:
        """Get order book for symbol"""
        base = symbol.split("-")[0]
        
        response = await self._make_request(
            "POST",
            "/info/orderbook",
            params={"coin": base, "nLevels": limit}
        )
        
        book_data = response.get("orderbook", {})
        
        bids = [
            (Decimal(str(level["px"])), Decimal(str(level["sz"])))
            for level in book_data.get("bids", [])
        ]
        
        asks = [
            (Decimal(str(level["px"])), Decimal(str(level["sz"])))
            for level in book_data.get("asks", [])
        ]
        
        return OrderBook(
            timestamp=datetime.now(timezone.utc),
            symbol=symbol,
            bids=bids,
            asks=asks
        )
        
    async def get_balance(self) -> List[Balance]:
        """Get account balances"""
        response = await self._make_request(
            "POST",
            "/info/clearinghouseState",
            params={"user": self.address}
        )
        
        balances = []
        
        # Get margin summary
        margin_summary = response.get("marginSummary", {})
        account_value = Decimal(str(margin_summary.get("accountValue", "0")))
        
        # Hyperliquid uses cross-margin, so we return USD balance
        balances.append(Balance(
            asset="USD",
            free=account_value,
            locked=Decimal("0"),
            total=account_value
        ))
        
        # Add positions as "locked" balance in respective assets
        for position in response.get("assetPositions", []):
            asset = position["position"]["coin"]
            size = abs(Decimal(str(position["position"]["szi"])))
            
            if size > 0:
                balances.append(Balance(
                    asset=asset,
                    free=Decimal("0"),
                    locked=size,
                    total=size
                ))
                
        return balances
        
    async def place_order(self, order: Order) -> Dict[str, Any]:
        """Place new order on Hyperliquid"""
        # Apply DeFi behavior
        await self.profile.pre_order_behavior(order)
        
        # Validate order
        valid, error = self.validate_order(order)
        if not valid:
            raise OrderExecutionError(f"Order validation failed: {error}", order.order_id, self.name)
            
        # Extract base asset
        base = order.symbol.split("-")[0]
        
        # Prepare order parameters
        order_params = {
            "coin": base,
            "is_buy": order.side == OrderSide.BUY,
            "sz": float(self.round_quantity(order.symbol, order.quantity)),
            "limit_px": float(self.round_price(order.symbol, order.price)) if order.order_type == OrderType.LIMIT else None,
            "order_type": HYPERLIQUID_ORDER_TYPES.get(order.order_type, "limit"),
            "reduce_only": order.reduce_only,
            "client_order_id": order.order_id
        }
        
        # Set time in force
        if order.order_type == OrderType.LIMIT:
            order_params["tif"] = HYPERLIQUID_TIF.get(order.time_in_force, "Gtc")
            
        # Set leverage (cross-margin by default)
        if order.leverage > 1:
            order_params["leverage"] = order.leverage
            
        # Calculate gas fee
        gas_estimate = await self.profile.estimate_gas_fee()
        
        # Place order
        try:
            response = await self._make_request(
                "POST",
                "/exchange/order",
                params={
                    "action": "order",
                    "orders": [order_params],
                    "grouping": "na"
                },
                signed=True
            )
            
            # Apply post-order behavior
            await self.profile.post_order_behavior(order, response)
            
            order_result = response.get("response", {}).get("data", {}).get("statuses", [{}])[0]
            
            return {
                "order_id": order.order_id,
                "exchange_order_id": order_result.get("oid", ""),
                "status": "OPEN" if order_result.get("filled") == "0" else "FILLED",
                "filled_quantity": Decimal(str(order_result.get("filled", "0"))),
                "gas_fee": gas_estimate,
                "created_at": datetime.now(timezone.utc)
            }
            
        except Exception as e:
            logger.error(f"Failed to place order on Hyperliquid: {e}")
            raise OrderExecutionError(str(e), order.order_id, self.name)
            
    async def cancel_order(self, order_id: str, symbol: str) -> Dict[str, Any]:
        """Cancel existing order"""
        base = symbol.split("-")[0]
        
        response = await self._make_request(
            "POST",
            "/exchange/cancel",
            params={
                "action": "cancel",
                "cancels": [{
                    "coin": base,
                    "client_order_id": order_id
                }]
            },
            signed=True
        )
        
        return {
            "order_id": order_id,
            "status": "CANCELLED",
            "cancelled_at": datetime.now(timezone.utc)
        }
        
    async def get_order_status(self, order_id: str, symbol: str) -> Dict[str, Any]:
        """Get order status"""
        response = await self._make_request(
            "POST",
            "/info/openOrders",
            params={"user": self.address},
            signed=True
        )
        
        for order_info in response.get("orders", []):
            if order_info.get("clientOrderId") == order_id:
                return {
                    "order_id": order_id,
                    "exchange_order_id": order_info.get("oid"),
                    "status": normalize_order_status(order_info.get("orderStatus", "OPEN")),
                    "filled_quantity": Decimal(str(order_info.get("filled", "0"))),
                    "remaining_quantity": Decimal(str(order_info.get("origSz", "0"))) - Decimal(str(order_info.get("filled", "0"))),
                    "average_price": Decimal(str(order_info.get("avgFillPx", "0")))
                }
                
        # Order not found in open orders, might be filled or cancelled
        return {
            "order_id": order_id,
            "status": "UNKNOWN"
        }
        
    async def get_open_orders(self, symbol: Optional[str] = None) -> List[Dict[str, Any]]:
        """Get all open orders"""
        response = await self._make_request(
            "POST",
            "/info/openOrders",
            params={"user": self.address},
            signed=True
        )
        
        orders = []
        for order_data in response.get("orders", []):
            order_symbol = f"{order_data['coin']}-USD"
            
            if symbol and order_symbol != symbol:
                continue
                
            orders.append({
                "order_id": order_data.get("clientOrderId", order_data.get("oid")),
                "exchange_order_id": order_data.get("oid"),
                "symbol": order_symbol,
                "side": "BUY" if order_data.get("side") == "B" else "SELL",
                "type": order_data.get("orderType", "LIMIT"),
                "status": normalize_order_status(order_data.get("orderStatus", "OPEN")),
                "quantity": Decimal(str(order_data.get("origSz", "0"))),
                "filled_quantity": Decimal(str(order_data.get("filled", "0"))),
                "price": Decimal(str(order_data.get("limitPx", "0"))),
                "created_at": datetime.fromtimestamp(order_data.get("timestamp", 0) / 1000, tz=timezone.utc)
            })
            
        return orders
        
    async def get_positions(self) -> List[Dict[str, Any]]:
        """Get all open positions"""
        response = await self._make_request(
            "POST",
            "/info/clearinghouseState",
            params={"user": self.address},
            signed=True
        )
        
        positions = []
        for pos_data in response.get("assetPositions", []):
            position = pos_data["position"]
            size = Decimal(str(position["szi"]))
            
            if size != 0:
                positions.append({
                    "symbol": f"{position['coin']}-USD",
                    "side": "LONG" if size > 0 else "SHORT",
                    "quantity": abs(size),
                    "entry_price": Decimal(str(position["entryPx"])),
                    "mark_price": Decimal(str(position.get("markPx", "0"))),
                    "unrealized_pnl": Decimal(str(position["unrealizedPnl"])),
                    "realized_pnl": Decimal(str(position["realizedPnl"])),
                    "margin_used": Decimal(str(position["marginUsed"])),
                    "leverage": int(position.get("leverage", 1)),
                    "liquidation_price": Decimal(str(position.get("liquidationPx", "0")))
                })
                
        return positions
        
    async def get_funding_rate(self, symbol: str) -> Dict[str, Any]:
        """Get current funding rate"""
        base = symbol.split("-")[0]
        
        response = await self._make_request(
            "POST",
            "/info/fundingHistory",
            params={"coin": base, "startTime": get_timestamp() - 86400000}  # Last 24h
        )
        
        latest_funding = response.get("fundingHistory", [{}])[-1] if response.get("fundingHistory") else {}
        
        return {
            "symbol": symbol,
            "funding_rate": Decimal(str(latest_funding.get("fundingRate", "0"))),
            "next_funding_time": datetime.fromtimestamp(latest_funding.get("time", 0) / 1000 + 28800, tz=timezone.utc),  # 8 hours
            "interval_hours": 8
        }
        
    async def subscribe_ticker(self, symbol: str) -> AsyncGenerator[Ticker, None]:
        """Subscribe to ticker updates via WebSocket"""
        base = symbol.split("-")[0]
        
        ws = WebSocketManager(self.ws_url)
        await ws.connect()
        
        # Subscribe to ticker
        await ws.send({
            "method": "subscribe",
            "subscription": {
                "type": "ticker",
                "coin": base
            }
        })
        
        async for message in ws.receive():
            if message.get("channel") == "ticker" and message.get("data", {}).get("coin") == base:
                data = message["data"]
                
                yield Ticker(
                    timestamp=datetime.now(timezone.utc),
                    symbol=symbol,
                    bid=Decimal(str(data.get("bid", "0"))),
                    ask=Decimal(str(data.get("ask", "0"))),
                    last=Decimal(str(data.get("last", "0"))),
                    volume_24h=Decimal(str(data.get("volume24h", "0"))),
                    high_24h=Decimal(str(data.get("high24h", "0"))),
                    low_24h=Decimal(str(data.get("low24h", "0"))),
                    change_24h=Decimal(str(data.get("change24h", "0")))
                )
                
    async def subscribe_order_book(self, symbol: str) -> AsyncGenerator[OrderBook, None]:
        """Subscribe to order book updates via WebSocket"""
        base = symbol.split("-")[0]
        
        ws = WebSocketManager(self.ws_url)
        await ws.connect()
        
        # Subscribe to order book
        await ws.send({
            "method": "subscribe",
            "subscription": {
                "type": "l2Book",
                "coin": base
            }
        })
        
        async for message in ws.receive():
            if message.get("channel") == "l2Book" and message.get("data", {}).get("coin") == base:
                data = message["data"]["book"]
                
                bids = [
                    (Decimal(str(level["px"])), Decimal(str(level["sz"])))
                    for level in data.get("bids", [])
                ]
                
                asks = [
                    (Decimal(str(level["px"])), Decimal(str(level["sz"])))
                    for level in data.get("asks", [])
                ]
                
                yield OrderBook(
                    timestamp=datetime.now(timezone.utc),
                    symbol=symbol,
                    bids=bids,
                    asks=asks
                )
                
    async def subscribe_trades(self, symbol: str) -> AsyncGenerator[Dict[str, Any], None]:
        """Subscribe to trade updates via WebSocket"""
        base = symbol.split("-")[0]
        
        ws = WebSocketManager(self.ws_url)
        await ws.connect()
        
        # Subscribe to trades
        await ws.send({
            "method": "subscribe",
            "subscription": {
                "type": "trades",
                "coin": base
            }
        })
        
        async for message in ws.receive():
            if message.get("channel") == "trades" and message.get("data", {}).get("coin") == base:
                for trade in message["data"]["trades"]:
                    yield {
                        "trade_id": trade.get("tid"),
                        "timestamp": datetime.fromtimestamp(trade.get("time", 0) / 1000, tz=timezone.utc),
                        "symbol": symbol,
                        "price": Decimal(str(trade.get("px", "0"))),
                        "quantity": Decimal(str(trade.get("sz", "0"))),
                        "side": trade.get("side"),
                        "is_buyer_maker": trade.get("side") == "B"
                    }
