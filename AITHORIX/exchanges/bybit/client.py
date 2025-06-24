"""
AITHORIX Bybit Exchange Implementation
Full production implementation optimized for derivatives trading
"""

import asyncio
import time
import hmac
import hashlib
from datetime import datetime, timezone
from decimal import Decimal
from typing import Dict, List, Optional, Any, AsyncGenerator
from urllib.parse import urlencode
import json

from ..base_exchange import (
    BaseExchange, ExchangeCredentials, MarketInfo, 
    OrderBook, Ticker, Balance, WebSocketManager, OrderBookManager
)
from ..common import (
    BYBIT_ORDER_TYPES, BYBIT_TIF,
    get_timestamp, normalize_order_status, create_signature
)
from ...core.engine.trading_engine import Order, OrderType, OrderSide, OrderStatus
from ...core.exceptions import (
    ExchangeConnectionError, OrderExecutionError,
    AuthenticationError, RateLimitError
)
from ...stealth.profiles.bybit_professional import BybitProfessionalProfile


class BybitExchange(BaseExchange):
    """
    Bybit exchange implementation
    Specialized for professional derivatives trading
    """
    
    def __init__(self, credentials: ExchangeCredentials, **kwargs):
        super().__init__(credentials, **kwargs)
        
        self.name = "bybit"
        self.base_url = "https://api.bybit.com"
        self.ws_url = "wss://stream.bybit.com/v5/public/spot"
        self.private_ws_url = "wss://stream.bybit.com/v5/private"
        
        # Rate limits
        self.rate_limits = {
            "default": 50,    # requests per second
            "orders": 10,     # orders per second
            "heavy": 1        # heavy endpoints per second
        }
        
        # Bybit specific features
        self.supports_unified_margin = True
        self.supports_portfolio_margin = True
        self.supports_options = True
        self.supports_copy_trading = True
        
        # WebSocket managers
        self.public_ws: Optional[WebSocketManager] = None
        self.private_ws: Optional[WebSocketManager] = None
        
        # Trading preferences
        self.preferred_margin_mode = "PORTFOLIO"  # or "REGULAR"
        self.auto_borrow = True
        
        # Apply professional trader profile
        self.profile = BybitProfessionalProfile()
        
        # Position tracking for derivatives
        self.positions: Dict[str, Dict[str, Any]] = {}
        
    async def connect(self) -> None:
        """Initialize Bybit connection"""
        await super().connect()
        
        # Initialize WebSocket connections
        self.public_ws = WebSocketManager(self.ws_url)
        await self.public_ws.connect()
        
        # Authenticate and connect private WebSocket
        await self._connect_private_ws()
        
        # Set account preferences
        await self._configure_account()
        
        # Start position monitor for derivatives
        asyncio.create_task(self._monitor_positions())
        
    async def disconnect(self) -> None:
        """Close Bybit connections"""
        if self.public_ws:
            await self.public_ws.disconnect()
        if self.private_ws:
            await self.private_ws.disconnect()
            
        await super().disconnect()
        
    def _sign_request(self, method: str, path: str, params: Dict[str, Any]) -> Dict[str, str]:
        """Sign request for Bybit API"""
        timestamp = str(get_timestamp())
        recv_window = "5000"
        
        # Prepare sign string
        if method == "GET":
            query_string = urlencode(sorted(params.items())) if params else ""
            sign_str = f"{timestamp}{self.credentials.api_key}{recv_window}{query_string}"
        else:
            # POST requests
            param_str = json.dumps(params, separators=(',', ':')) if params else ""
            sign_str = f"{timestamp}{self.credentials.api_key}{recv_window}{param_str}"
            
        # Generate signature
        signature = hmac.new(
            self.credentials.api_secret.encode('utf-8'),
            sign_str.encode('utf-8'),
            hashlib.sha256
        ).hexdigest()
        
        return {
            "X-BAPI-API-KEY": self.credentials.api_key,
            "X-BAPI-TIMESTAMP": timestamp,
            "X-BAPI-SIGN": signature,
            "X-BAPI-RECV-WINDOW": recv_window
        }
        
    async def _connect_private_ws(self) -> None:
        """Connect to private WebSocket for account updates"""
        try:
            # Generate auth for WebSocket
            expires = int((time.time() + 30) * 1000)
            sign_str = f"GET/realtime{expires}"
            signature = hmac.new(
                self.credentials.api_secret.encode('utf-8'),
                sign_str.encode('utf-8'),
                hashlib.sha256
            ).hexdigest()
            
            # Connect with auth
            auth_msg = {
                "op": "auth",
                "args": [self.credentials.api_key, expires, signature]
            }
            
            self.private_ws = WebSocketManager(self.private_ws_url)
            await self.private_ws.connect()
            await self.private_ws.send(auth_msg)
            
            # Subscribe to private channels
            await self._subscribe_private_channels()
            
            # Start processing private updates
            asyncio.create_task(self._process_private_stream())
            
        except Exception as e:
            logger.error(f"Failed to connect private WebSocket: {e}")
            
    async def _subscribe_private_channels(self) -> None:
        """Subscribe to private account channels"""
        subscriptions = {
            "op": "subscribe",
            "args": [
                "position",
                "execution", 
                "order",
                "wallet"
            ]
        }
        
        await self.private_ws.send(subscriptions)
        
    async def _process_private_stream(self) -> None:
        """Process private WebSocket updates"""
        if not self.private_ws:
            return
            
        async for message in self.private_ws.receive():
            try:
                topic = message.get("topic", "")
                data = message.get("data", [])
                
                if topic == "position":
                    await self._handle_position_update(data)
                elif topic == "execution":
                    await self._handle_execution_update(data)
                elif topic == "order":
                    await self._handle_order_update(data)
                elif topic == "wallet":
                    await self._handle_wallet_update(data)
                    
            except Exception as e:
                logger.error(f"Error processing private stream: {e}")
                
    async def _handle_position_update(self, data: List[Dict[str, Any]]) -> None:
        """Handle position updates"""
        for position in data:
            symbol = position.get("symbol")
            self.positions[symbol] = {
                "side": position.get("side"),
                "size": Decimal(position.get("size", "0")),
                "entry_price": Decimal(position.get("avgPrice", "0")),
                "mark_price": Decimal(position.get("markPrice", "0")),
                "unrealized_pnl": Decimal(position.get("unrealisedPnl", "0")),
                "leverage": int(position.get("leverage", "1")),
                "margin_mode": position.get("tradeMode"),
                "updated_at": datetime.now(timezone.utc)
            }
            
    async def _configure_account(self) -> None:
        """Configure account settings"""
        try:
            # Set margin mode if unified account
            if self.supports_unified_margin:
                await self._make_request(
                    "POST",
                    "/v5/account/set-margin-mode",
                    params={"setMarginMode": self.preferred_margin_mode},
                    signed=True
                )
                
        except Exception as e:
            logger.warning(f"Failed to configure account: {e}")
            
    async def _monitor_positions(self) -> None:
        """Monitor positions for risk management"""
        while True:
            try:
                # Update positions
                if self.positions:
                    await self._update_positions()
                    
                    # Check risk metrics
                    for symbol, position in self.positions.items():
                        # Check if position needs adjustment
                        if await self.profile.should_adjust_position(position):
                            logger.info(f"Position adjustment needed for {symbol}")
                            
                await asyncio.sleep(30)  # Check every 30 seconds
                
            except Exception as e:
                logger.error(f"Error monitoring positions: {e}")
                await asyncio.sleep(60)
                
    async def _update_positions(self) -> None:
        """Update all position information"""
        response = await self._make_request(
            "GET",
            "/v5/position/list",
            params={"category": "linear"},
            signed=True
        )
        
        for position in response.get("result", {}).get("list", []):
            symbol = position.get("symbol")
            if Decimal(position.get("size", "0")) > 0:
                self.positions[symbol] = {
                    "side": position.get("side"),
                    "size": Decimal(position.get("size", "0")),
                    "entry_price": Decimal(position.get("avgPrice", "0")),
                    "mark_price": Decimal(position.get("markPrice", "0")),
                    "unrealized_pnl": Decimal(position.get("unrealisedPnl", "0")),
                    "leverage": int(position.get("leverage", "1")),
                    "margin_mode": position.get("tradeMode"),
                    "updated_at": datetime.now(timezone.utc)
                }
            else:
                # Position closed
                self.positions.pop(symbol, None)
                
    async def get_markets(self) -> List[MarketInfo]:
        """Get all available markets on Bybit"""
        all_markets = []
        
        # Get spot markets
        spot_response = await self._make_request(
            "GET",
            "/v5/market/instruments-info",
            params={"category": "spot"}
        )
        
        for instrument in spot_response.get("result", {}).get("list", []):
            if instrument.get("status") != "Trading":
                continue
                
            market = MarketInfo(
                symbol=instrument["symbol"],
                base_asset=instrument["baseCoin"],
                quote_asset=instrument["quoteCoin"],
                min_quantity=Decimal(instrument["lotSizeFilter"]["minOrderQty"]),
                max_quantity=Decimal(instrument["lotSizeFilter"]["maxOrderQty"]),
                quantity_precision=len(instrument["lotSizeFilter"]["basePrecision"].split(".")[-1]),
                min_price=Decimal(instrument["priceFilter"]["minPrice"]),
                max_price=Decimal(instrument["priceFilter"]["maxPrice"]),
                price_precision=len(instrument["priceFilter"]["tickSize"].split(".")[-1].rstrip("0")),
                min_notional=Decimal(instrument["lotSizeFilter"].get("minOrderAmt", "1")),
                is_trading=True,
                maker_fee=Decimal("0.001"),  # 0.1% default
                taker_fee=Decimal("0.001"),  # 0.1% default
                last=Decimal("0")
            )
            
            all_markets.append(market)
            
        # Get derivatives markets
        derivatives_response = await self._make_request(
            "GET",
            "/v5/market/instruments-info",
            params={"category": "linear"}
        )
        
        for instrument in derivatives_response.get("result", {}).get("list", []):
            if instrument.get("status") != "Trading":
                continue
                
            market = MarketInfo(
                symbol=instrument["symbol"],
                base_asset=instrument["baseCoin"],
                quote_asset=instrument["quoteCoin"],
                min_quantity=Decimal(instrument["lotSizeFilter"]["minOrderQty"]),
                max_quantity=Decimal(instrument["lotSizeFilter"]["maxOrderQty"]),
                quantity_precision=len(str(instrument["lotSizeFilter"]["qtyStep"]).split(".")[-1]),
                min_price=Decimal(instrument["priceFilter"]["minPrice"]),
                max_price=Decimal(instrument["priceFilter"]["maxPrice"]),
                price_precision=len(instrument["priceFilter"]["tickSize"].split(".")[-1].rstrip("0")),
                min_notional=Decimal("5"),  # $5 minimum for derivatives
                is_trading=True,
                maker_fee=Decimal("0.00075"),  # 0.075% derivatives
                taker_fee=Decimal("0.00075"),  # 0.075% derivatives
                last=Decimal("0")
            )
            
            all_markets.append(market)
            
        return all_markets
        
    async def get_ticker(self, symbol: str) -> Ticker:
        """Get current ticker for symbol"""
        # Determine category
        category = "linear" if symbol.endswith("USDT") and "-" not in symbol else "spot"
        
        response = await self._make_request(
            "GET",
            "/v5/market/tickers",
            params={"category": category, "symbol": symbol}
        )
        
        ticker_data = response.get("result", {}).get("list", [{}])[0]
        
        return Ticker(
            timestamp=datetime.now(timezone.utc),
            symbol=symbol,
            bid=Decimal(ticker_data.get("bid1Price", "0")),
            ask=Decimal(ticker_data.get("ask1Price", "0")),
            last=Decimal(ticker_data.get("lastPrice", "0")),
            volume_24h=Decimal(ticker_data.get("volume24h", "0")),
            high_24h=Decimal(ticker_data.get("highPrice24h", "0")),
            low_24h=Decimal(ticker_data.get("lowPrice24h", "0")),
            change_24h=Decimal(ticker_data.get("price24hPcnt", "0")) * 100
        )
        
    async def get_order_book(self, symbol: str, limit: int = 100) -> OrderBook:
        """Get order book for symbol"""
        category = "linear" if symbol.endswith("USDT") and "-" not in symbol else "spot"
        
        # Bybit limits: 1, 25, 50, 100, 200, 500
        valid_limits = [1, 25, 50, 100, 200, 500]
        limit = min(valid_limits, key=lambda x: abs(x - limit) if x >= limit else float('inf'))
        
        response = await self._make_request(
            "GET",
            "/v5/market/orderbook",
            params={"category": category, "symbol": symbol, "limit": limit}
        )
        
        orderbook_data = response.get("result", {})
        
        bids = [
            (Decimal(bid[0]), Decimal(bid[1]))
            for bid in orderbook_data.get("b", [])
        ]
        
        asks = [
            (Decimal(ask[0]), Decimal(ask[1]))
            for ask in orderbook_data.get("a", [])
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
            "GET",
            "/v5/account/wallet-balance",
            params={"accountType": "UNIFIED"},
            signed=True
        )
        
        balances = []
        
        # Get coin balances
        for account in response.get("result", {}).get("list", []):
            for coin_data in account.get("coin", []):
                free = Decimal(coin_data.get("availableToWithdraw", "0"))
                locked = Decimal(coin_data.get("locked", "0"))
                
                if free > 0 or locked > 0:
                    balance = Balance(
                        asset=coin_data["coin"],
                        free=free,
                        locked=locked,
                        total=free + locked
                    )
                    balances.append(balance)
                    
        return balances
        
    async def place_order(self, order: Order) -> Dict[str, Any]:
        """Place new order on Bybit"""
        # Apply professional behavior
        await self.profile.pre_order_behavior(order)
        
        # Validate order
        valid, error = self.validate_order(order)
        if not valid:
            raise OrderExecutionError(f"Order validation failed: {error}", order.order_id, self.name)
            
        # Determine category
        if order.symbol.endswith("USDT") and "-" not in order.symbol:
            category = "linear"
        elif order.symbol.endswith("-SPOT"):
            category = "spot"
            order.symbol = order.symbol.replace("-SPOT", "")
        else:
            category = "spot"
            
        # Prepare order parameters
        params = {
            "category": category,
            "symbol": order.symbol,
            "side": "Buy" if order.side == OrderSide.BUY else "Sell",
            "orderType": BYBIT_ORDER_TYPES.get(order.order_type, "Limit"),
            "qty": str(self.round_quantity(order.symbol, order.quantity)),
            "orderLinkId": order.order_id[:36]  # Client order ID
        }
        
        # Add order type specific parameters
        if order.order_type == OrderType.LIMIT:
            params["price"] = str(self.round_price(order.symbol, order.price))
            params["timeInForce"] = BYBIT_TIF.get(order.time_in_force, "GoodTillCancel")
            
            if order.post_only:
                params["timeInForce"] = "PostOnly"
                
        elif order.order_type in [OrderType.STOP_LOSS, OrderType.TAKE_PROFIT]:
            params["triggerPrice"] = str(self.round_price(order.symbol, order.stop_price))
            params["triggerDirection"] = 1 if order.side == OrderSide.BUY else 2
            
            if order.order_type == OrderType.STOP_LOSS:
                params["orderType"] = "StopOrder"
            else:
                params["orderType"] = "TakeProfitOrder"
                
            if order.price:
                params["price"] = str(self.round_price(order.symbol, order.price))
                params["triggerBy"] = "LastPrice"
                
        # Add leverage for derivatives
        if category == "linear" and order.leverage > 1:
            params["leverage"] = str(order.leverage)
            
        # Reduce only for closing positions
        if order.reduce_only:
            params["reduceOnly"] = True
            
        # Position mode for derivatives
        if category == "linear":
            params["positionIdx"] = 0  # One-way mode
            
        # Place order
        try:
            response = await self._make_request(
                "POST",
                "/v5/order/create",
                params=params,
                signed=True
            )
            
            order_result = response.get("result", {})
            
            # Apply post-order behavior
            await self.profile.post_order_behavior(order, order_result)
            
            return {
                "order_id": order.order_id,
                "exchange_order_id": order_result.get("orderId"),
                "status": normalize_order_status(order_result.get("orderStatus", "New")),
                "created_at": datetime.now(timezone.utc)
            }
            
        except Exception as e:
            logger.error(f"Failed to place order on Bybit: {e}")
            raise OrderExecutionError(str(e), order.order_id, self.name)
            
    async def cancel_order(self, order_id: str, symbol: str) -> Dict[str, Any]:
        """Cancel existing order"""
        # Determine category
        category = "linear" if symbol.endswith("USDT") and "-" not in symbol else "spot"
        
        params = {
            "category": category,
            "symbol": symbol,
            "orderLinkId": order_id[:36]
        }
        
        response = await self._make_request(
            "POST",
            "/v5/order/cancel",
            params=params,
            signed=True
        )
        
        return {
            "order_id": order_id,
            "status": "CANCELLED",
            "cancelled_at": datetime.now(timezone.utc)
        }
        
    async def get_order_status(self, order_id: str, symbol: str) -> Dict[str, Any]:
        """Get order status"""
        category = "linear" if symbol.endswith("USDT") and "-" not in symbol else "spot"
        
        params = {
            "category": category,
            "orderLinkId": order_id[:36]
        }
        
        response = await self._make_request(
            "GET",
            "/v5/order/realtime",
            params=params,
            signed=True
        )
        
        orders = response.get("result", {}).get("list", [])
        if orders:
            order_data = orders[0]
            return {
                "order_id": order_id,
                "exchange_order_id": order_data.get("orderId"),
                "status": normalize_order_status(order_data.get("orderStatus")),
                "filled_quantity": Decimal(order_data.get("cumExecQty", "0")),
                "average_price": Decimal(order_data.get("avgPrice", "0")) if order_data.get("avgPrice") else None
            }
            
        return {
            "order_id": order_id,
            "status": "UNKNOWN"
        }
        
    async def get_open_orders(self, symbol: Optional[str] = None) -> List[Dict[str, Any]]:
        """Get all open orders"""
        params = {
            "category": "spot",  # Will need to check both spot and linear
            "settleCoin": "USDT"
        }
        
        if symbol:
            params["symbol"] = symbol
            
        # Get spot orders
        spot_response = await self._make_request(
            "GET",
            "/v5/order/realtime",
            params=params,
            signed=True
        )
        
        orders = []
        
        for order_data in spot_response.get("result", {}).get("list", []):
            orders.append({
                "order_id": order_data.get("orderLinkId", order_data.get("orderId")),
                "exchange_order_id": order_data.get("orderId"),
                "symbol": order_data.get("symbol"),
                "side": "BUY" if order_data.get("side") == "Buy" else "SELL",
                "type": order_data.get("orderType"),
                "status": normalize_order_status(order_data.get("orderStatus")),
                "quantity": Decimal(order_data.get("qty", "0")),
                "filled_quantity": Decimal(order_data.get("cumExecQty", "0")),
                "price": Decimal(order_data.get("price", "0")),
                "created_at": datetime.fromtimestamp(int(order_data.get("createdTime", 0)) / 1000, tz=timezone.utc)
            })
            
        # Also get linear/derivatives orders
        if not symbol or (symbol.endswith("USDT") and "-" not in symbol):
            params["category"] = "linear"
            linear_response = await self._make_request(
                "GET",
                "/v5/order/realtime",
                params=params,
                signed=True
            )
            
            for order_data in linear_response.get("result", {}).get("list", []):
                orders.append({
                    "order_id": order_data.get("orderLinkId", order_data.get("orderId")),
                    "exchange_order_id": order_data.get("orderId"),
                    "symbol": order_data.get("symbol"),
                    "side": "BUY" if order_data.get("side") == "Buy" else "SELL",
                    "type": order_data.get("orderType"),
                    "status": normalize_order_status(order_data.get("orderStatus")),
                    "quantity": Decimal(order_data.get("qty", "0")),
                    "filled_quantity": Decimal(order_data.get("cumExecQty", "0")),
                    "price": Decimal(order_data.get("price", "0")),
                    "created_at": datetime.fromtimestamp(int(order_data.get("createdTime", 0)) / 1000, tz=timezone.utc)
                })
                
        return orders
        
    async def get_positions(self) -> List[Dict[str, Any]]:
        """Get all open positions (derivatives)"""
        response = await self._make_request(
            "GET",
            "/v5/position/list",
            params={"category": "linear", "settleCoin": "USDT"},
            signed=True
        )
        
        positions = []
        for pos_data in response.get("result", {}).get("list", []):
            if Decimal(pos_data.get("size", "0")) > 0:
                positions.append({
                    "symbol": pos_data.get("symbol"),
                    "side": pos_data.get("side"),
                    "size": Decimal(pos_data.get("size", "0")),
                    "entry_price": Decimal(pos_data.get("avgPrice", "0")),
                    "mark_price": Decimal(pos_data.get("markPrice", "0")),
                    "unrealized_pnl": Decimal(pos_data.get("unrealisedPnl", "0")),
                    "realized_pnl": Decimal(pos_data.get("realisedPnl", "0")),
                    "leverage": int(pos_data.get("leverage", "1")),
                    "margin_mode": pos_data.get("tradeMode"),
                    "position_value": Decimal(pos_data.get("positionValue", "0"))
                })
                
        return positions
        
    async def set_leverage(self, symbol: str, leverage: int) -> Dict[str, Any]:
        """Set leverage for derivatives trading"""
        params = {
            "category": "linear",
            "symbol": symbol,
            "buyLeverage": str(leverage),
            "sellLeverage": str(leverage)
        }
        
        response = await self._make_request(
            "POST",
            "/v5/position/set-leverage",
            params=params,
            signed=True
        )
        
        return {
            "symbol": symbol,
            "leverage": leverage
        }
        
    async def subscribe_ticker(self, symbol: str) -> AsyncGenerator[Ticker, None]:
        """Subscribe to ticker updates via WebSocket"""
        # Determine stream based on symbol
        if symbol.endswith("USDT") and "-" not in symbol:
            topic = f"tickers.{symbol}"
        else:
            topic = f"tickers.{symbol}"
            
        subscribe_msg = {
            "op": "subscribe",
            "args": [topic]
        }
        
        await self.public_ws.send(subscribe_msg)
        
        async for message in self.public_ws.receive():
            if message.get("topic") == topic:
                data = message.get("data", {})
                
                yield Ticker(
                    timestamp=datetime.fromtimestamp(data.get("time", 0) / 1000, tz=timezone.utc),
                    symbol=symbol,
                    bid=Decimal(data.get("bid1Price", "0")),
                    ask=Decimal(data.get("ask1Price", "0")),
                    last=Decimal(data.get("lastPrice", "0")),
                    volume_24h=Decimal(data.get("volume24h", "0")),
                    high_24h=Decimal(data.get("highPrice24h", "0")),
                    low_24h=Decimal(data.get("lowPrice24h", "0")),
                    change_24h=Decimal(data.get("price24hPcnt", "0")) * 100
                )
                
    async def subscribe_order_book(self, symbol: str) -> AsyncGenerator[OrderBook, None]:
        """Subscribe to order book updates via WebSocket"""
        # Create order book manager
        if symbol not in self.order_books:
            self.order_books[symbol] = OrderBookManager(symbol)
            
        manager = self.order_books[symbol]
        
        # Get initial snapshot
        snapshot = await self.get_order_book(symbol, limit=50)
        manager.update_snapshot(
            [[str(p), str(q)] for p, q in snapshot.bids],
            [[str(p), str(q)] for p, q in snapshot.asks]
        )
        
        # Subscribe to depth updates
        depth_topic = f"orderbook.50.{symbol}"
        
        subscribe_msg = {
            "op": "subscribe",
            "args": [depth_topic]
        }
        
        await self.public_ws.send(subscribe_msg)
        
        async for message in self.public_ws.receive():
            if message.get("topic") == depth_topic:
                data = message.get("data", {})
                
                # Update type: snapshot or delta
                if message.get("type") == "snapshot":
                    manager.update_snapshot(
                        data.get("b", []),
                        data.get("a", []),
                        data.get("u")
                    )
                else:
                    manager.update_delta(
                        data.get("b", []),
                        data.get("a", []),
                        data.get("u")
                    )
                    
                yield manager.get_order_book()
                
    async def subscribe_trades(self, symbol: str) -> AsyncGenerator[Dict[str, Any], None]:
        """Subscribe to trade updates via WebSocket"""
        trade_topic = f"publicTrade.{symbol}"
        
        subscribe_msg = {
            "op": "subscribe",
            "args": [trade_topic]
        }
        
        await self.public_ws.send(subscribe_msg)
        
        async for message in self.public_ws.receive():
            if message.get("topic") == trade_topic:
                for trade in message.get("data", []):
                    yield {
                        "trade_id": trade.get("i"),
                        "timestamp": datetime.fromtimestamp(trade.get("T", 0) / 1000, tz=timezone.utc),
                        "symbol": symbol,
                        "price": Decimal(trade.get("p", "0")),
                        "quantity": Decimal(trade.get("v", "0")),
                        "side": trade.get("S"),
                        "is_buyer_maker": trade.get("S") == "Buy"
                    }