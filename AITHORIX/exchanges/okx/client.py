"""
AITHORIX OKX Exchange Implementation
Full production implementation with unified account and multi-product support
"""

import asyncio
import time
import hmac
import hashlib
import base64
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
    OKX_ORDER_TYPES, OKX_TIF,
    get_timestamp, normalize_order_status
)
from ...core.engine.trading_engine import Order, OrderType, OrderSide, OrderStatus
from ...core.exceptions import (
    ExchangeConnectionError, OrderExecutionError,
    AuthenticationError
)
from ...stealth.profiles.okx_asian import OKXAsianProfile


class OKXExchange(BaseExchange):
    """
    OKX exchange implementation
    Multi-product platform with unified account
    """
    
    def __init__(self, credentials: ExchangeCredentials, **kwargs):
        super().__init__(credentials, **kwargs)
        
        self.name = "okx"
        self.base_url = "https://www.okx.com"
        self.ws_url = "wss://ws.okx.com:8443/ws/v5/public"
        self.private_ws_url = "wss://ws.okx.com:8443/ws/v5/private"
        
        # OKX requires passphrase
        self.passphrase = getattr(credentials, 'passphrase', '')
        if not self.passphrase:
            raise ConfigurationError("okx", "Passphrase required for OKX")
            
        # Rate limits
        self.rate_limits = {
            "default": 20,    # requests per 2 seconds
            "orders": 60,     # orders per 2 seconds
            "heavy": 2        # heavy endpoints per 2 seconds
        }
        
        # OKX specific features
        self.account_mode = "unified"  # or "simple"
        self.position_mode = "net_mode"  # or "long_short_mode"
        self.supports_copy_trading = True
        self.supports_grid_trading = True
        self.supports_recurring_buy = True
        
        # WebSocket managers
        self.public_ws: Optional[WebSocketManager] = None
        self.private_ws: Optional[WebSocketManager] = None
        
        # Apply Asian timezone profile
        self.profile = OKXAsianProfile()
        
        # Grid trading bots tracking
        self.active_grids: Dict[str, Dict[str, Any]] = {}
        
    async def connect(self) -> None:
        """Initialize OKX connection"""
        await super().connect()
        
        # Initialize WebSocket connections
        self.public_ws = WebSocketManager(self.ws_url)
        await self.public_ws.connect()
        
        # Authenticate and connect private WebSocket
        await self._connect_private_ws()
        
        # Configure account settings
        await self._configure_account()
        
        # Start grid trading monitor if enabled
        if self.supports_grid_trading:
            asyncio.create_task(self._monitor_grid_trading())
            
    async def disconnect(self) -> None:
        """Close OKX connections"""
        if self.public_ws:
            await self.public_ws.disconnect()
        if self.private_ws:
            await self.private_ws.disconnect()
            
        await super().disconnect()
        
    def _sign_request(self, method: str, path: str, params: Dict[str, Any]) -> Dict[str, str]:
        """Sign request for OKX API"""
        timestamp = datetime.utcnow().isoformat("T", "milliseconds") + "Z"
        
        # Create sign string
        if method == "GET":
            if params:
                path = path + "?" + urlencode(sorted(params.items()))
            body = ""
        else:
            body = json.dumps(params) if params else ""
            
        message = timestamp + method + path + body
        
        # Create signature
        mac = hmac.new(
            self.credentials.api_secret.encode('utf-8'),
            message.encode('utf-8'),
            hashlib.sha256
        )
        signature = base64.b64encode(mac.digest()).decode()
        
        return {
            "OK-ACCESS-KEY": self.credentials.api_key,
            "OK-ACCESS-SIGN": signature,
            "OK-ACCESS-TIMESTAMP": timestamp,
            "OK-ACCESS-PASSPHRASE": self.passphrase,
            "Content-Type": "application/json"
        }
        
    async def _connect_private_ws(self) -> None:
        """Connect to private WebSocket for account updates"""
        try:
            # Generate auth for WebSocket
            timestamp = str(int(time.time()))
            message = timestamp + "GET" + "/users/self/verify"
            
            mac = hmac.new(
                self.credentials.api_secret.encode('utf-8'),
                message.encode('utf-8'),
                hashlib.sha256
            )
            signature = base64.b64encode(mac.digest()).decode()
            
            # Connect and authenticate
            self.private_ws = WebSocketManager(self.private_ws_url)
            await self.private_ws.connect()
            
            auth_msg = {
                "op": "login",
                "args": [{
                    "apiKey": self.credentials.api_key,
                    "passphrase": self.passphrase,
                    "timestamp": timestamp,
                    "sign": signature
                }]
            }
            
            await self.private_ws.send(auth_msg)
            
            # Wait for auth confirmation
            await asyncio.sleep(1)
            
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
                {"channel": "account"},
                {"channel": "positions", "instType": "SWAP"},
                {"channel": "orders", "instType": "ANY"},
                {"channel": "orders-algo", "instType": "ANY"}
            ]
        }
        
        await self.private_ws.send(subscriptions)
        
    async def _process_private_stream(self) -> None:
        """Process private WebSocket updates"""
        if not self.private_ws:
            return
            
        async for message in self.private_ws.receive():
            try:
                if message.get("event") == "error":
                    logger.error(f"WebSocket error: {message}")
                    continue
                    
                arg = message.get("arg", {})
                channel = arg.get("channel")
                data = message.get("data", [])
                
                if channel == "account":
                    await self._handle_account_update(data)
                elif channel == "positions":
                    await self._handle_position_update(data)
                elif channel == "orders":
                    await self._handle_order_update(data)
                elif channel == "orders-algo":
                    await self._handle_algo_order_update(data)
                    
            except Exception as e:
                logger.error(f"Error processing private stream: {e}")
                
    async def _handle_account_update(self, data: List[Dict[str, Any]]) -> None:
        """Handle account balance updates"""
        for update in data:
            # Update internal balance cache
            logger.debug(f"Account update: {update}")
            
    async def _handle_position_update(self, data: List[Dict[str, Any]]) -> None:
        """Handle position updates"""
        for position in data:
            logger.debug(f"Position update: {position}")
            
    async def _handle_order_update(self, data: List[Dict[str, Any]]) -> None:
        """Handle order updates"""
        for order in data:
            logger.info(f"Order update: {order}")
            
    async def _handle_algo_order_update(self, data: List[Dict[str, Any]]) -> None:
        """Handle algorithmic order updates"""
        for algo_order in data:
            logger.info(f"Algo order update: {algo_order}")
            
    async def _configure_account(self) -> None:
        """Configure account settings"""
        try:
            # Set account mode
            await self._make_request(
                "POST",
                "/api/v5/account/set-account-level",
                params={"acctLv": "2"},  # Unified account
                signed=True
            )
            
            # Set position mode
            await self._make_request(
                "POST",
                "/api/v5/account/set-position-mode",
                params={"posMode": self.position_mode},
                signed=True
            )
            
        except Exception as e:
            logger.warning(f"Failed to configure account: {e}")
            
    async def _monitor_grid_trading(self) -> None:
        """Monitor grid trading bots"""
        while True:
            try:
                # Get active grid bots
                response = await self._make_request(
                    "GET",
                    "/api/v5/tradingBot/grid/orders-algo-pending",
                    params={"algoOrdType": "grid"},
                    signed=True
                )
                
                for grid in response.get("data", []):
                    algo_id = grid.get("algoId")
                    self.active_grids[algo_id] = grid
                    
                await asyncio.sleep(300)  # Check every 5 minutes
                
            except Exception as e:
                logger.error(f"Error monitoring grid trading: {e}")
                await asyncio.sleep(600)
                
    async def get_markets(self) -> List[MarketInfo]:
        """Get all available markets on OKX"""
        all_markets = []
        
        # Get different instrument types
        inst_types = ["SPOT", "SWAP", "FUTURES", "OPTION"]
        
        for inst_type in inst_types:
            response = await self._make_request(
                "GET",
                "/api/v5/public/instruments",
                params={"instType": inst_type}
            )
            
            for instrument in response.get("data", []):
                if instrument.get("state") != "live":
                    continue
                    
                # Parse instrument details
                if inst_type == "SPOT":
                    base_asset = instrument["baseCcy"]
                    quote_asset = instrument["quoteCcy"]
                    symbol = f"{base_asset}-{quote_asset}"
                else:
                    symbol = instrument["instId"]
                    base_asset = instrument.get("ctValCcy", instrument.get("baseCcy", ""))
                    quote_asset = instrument.get("quoteCcy", "USDT")
                    
                market = MarketInfo(
                    symbol=symbol,
                    base_asset=base_asset,
                    quote_asset=quote_asset,
                    min_quantity=Decimal(instrument.get("minSz", "0")),
                    max_quantity=Decimal(instrument.get("maxLmtSz", "999999999")),
                    quantity_precision=len(instrument.get("lotSz", "0.00000001").split(".")[-1].rstrip("0")),
                    min_price=Decimal("0.00000001"),
                    max_price=Decimal("999999999"),
                    price_precision=len(instrument.get("tickSz", "0.00000001").split(".")[-1].rstrip("0")),
                    min_notional=Decimal("1"),  # $1 minimum
                    is_trading=True,
                    maker_fee=Decimal("0.0008"),  # 0.08% default
                    taker_fee=Decimal("0.001"),   # 0.1% default
                    last=Decimal("0")
                )
                
                # Add instrument type
                market.instrument_type = inst_type
                
                all_markets.append(market)
                
        return all_markets
        
    async def get_ticker(self, symbol: str) -> Ticker:
        """Get current ticker for symbol"""
        response = await self._make_request(
            "GET",
            "/api/v5/market/ticker",
            params={"instId": symbol}
        )
        
        ticker_data = response.get("data", [{}])[0]
        
        return Ticker(
            timestamp=datetime.fromtimestamp(int(ticker_data.get("ts", 0)) / 1000, tz=timezone.utc),
            symbol=symbol,
            bid=Decimal(ticker_data.get("bidPx", "0")),
            ask=Decimal(ticker_data.get("askPx", "0")),
            last=Decimal(ticker_data.get("last", "0")),
            volume_24h=Decimal(ticker_data.get("vol24h", "0")),
            high_24h=Decimal(ticker_data.get("high24h", "0")),
            low_24h=Decimal(ticker_data.get("low24h", "0")),
            change_24h=Decimal(ticker_data.get("instId", "0"))  # Percentage
        )
        
    async def get_order_book(self, symbol: str, limit: int = 100) -> OrderBook:
        """Get order book for symbol"""
        # OKX limits: 1-400
        limit = min(400, max(1, limit))
        
        response = await self._make_request(
            "GET",
            "/api/v5/market/books",
            params={"instId": symbol, "sz": str(limit)}
        )
        
        book_data = response.get("data", [{}])[0]
        
        bids = [
            (Decimal(bid[0]), Decimal(bid[1]))
            for bid in book_data.get("bids", [])
        ]
        
        asks = [
            (Decimal(ask[0]), Decimal(ask[1]))
            for ask in book_data.get("asks", [])
        ]
        
        return OrderBook(
            timestamp=datetime.fromtimestamp(int(book_data.get("ts", 0)) / 1000, tz=timezone.utc),
            symbol=symbol,
            bids=bids,
            asks=asks
        )
        
    async def get_balance(self) -> List[Balance]:
        """Get account balances"""
        response = await self._make_request(
            "GET",
            "/api/v5/account/balance",
            signed=True
        )
        
        balances = []
        
        for account_data in response.get("data", []):
            for detail in account_data.get("details", []):
                available = Decimal(detail.get("availBal", "0"))
                frozen = Decimal(detail.get("frozenBal", "0"))
                
                if available > 0 or frozen > 0:
                    balance = Balance(
                        asset=detail["ccy"],
                        free=available,
                        locked=frozen,
                        total=available + frozen
                    )
                    balances.append(balance)
                    
        return balances
        
    async def place_order(self, order: Order) -> Dict[str, Any]:
        """Place new order on OKX"""
        # Apply Asian trading behavior
        await self.profile.pre_order_behavior(order)
        
        # Validate order
        valid, error = self.validate_order(order)
        if not valid:
            raise OrderExecutionError(f"Order validation failed: {error}", order.order_id, self.name)
            
        # Determine trade mode (cash, cross, isolated)
        td_mode = "cash" if "-" in order.symbol and "SWAP" not in order.symbol else "cross"
        
        # Prepare order parameters
        params = {
            "instId": order.symbol,
            "tdMode": td_mode,
            "side": "buy" if order.side == OrderSide.BUY else "sell",
            "ordType": OKX_ORDER_TYPES.get(order.order_type, "limit"),
            "sz": str(self.round_quantity(order.symbol, order.quantity)),
            "clOrdId": order.order_id[:32]  # OKX limit
        }
        
        # Position side for derivatives
        if "SWAP" in order.symbol or "FUTURES" in order.symbol:
            if self.position_mode == "long_short_mode":
                params["posSide"] = "long" if order.side == OrderSide.BUY else "short"
            else:
                params["posSide"] = "net"
                
        # Add order type specific parameters
        if order.order_type == OrderType.LIMIT:
            params["px"] = str(self.round_price(order.symbol, order.price))
            
            # Time in force
            if order.post_only:
                params["ordType"] = "post_only"
            else:
                params["tgtCcy"] = ""  # Default
                
        elif order.order_type in [OrderType.STOP_LOSS, OrderType.TAKE_PROFIT]:
            # OKX conditional orders
            params["ordType"] = "conditional"
            params["triggerPx"] = str(self.round_price(order.symbol, order.stop_price))
            params["triggerPxType"] = "last"  # or "index", "mark"
            
            if order.price:
                params["px"] = str(self.round_price(order.symbol, order.price))
                
        # Reduce only
        if order.reduce_only:
            params["reduceOnly"] = True
            
        # Place order
        try:
            response = await self._make_request(
                "POST",
                "/api/v5/trade/order",
                params=params,
                signed=True
            )
            
            order_data = response.get("data", [{}])[0]
            
            # Apply post-order behavior
            await self.profile.post_order_behavior(order, order_data)
            
            return {
                "order_id": order.order_id,
                "exchange_order_id": order_data.get("ordId"),
                "status": "OPEN" if order_data.get("sCode") == "0" else "REJECTED",
                "created_at": datetime.now(timezone.utc)
            }
            
        except Exception as e:
            logger.error(f"Failed to place order on OKX: {e}")
            raise OrderExecutionError(str(e), order.order_id, self.name)
            
    async def cancel_order(self, order_id: str, symbol: str) -> Dict[str, Any]:
        """Cancel existing order"""
        params = {
            "instId": symbol,
            "clOrdId": order_id[:32]
        }
        
        response = await self._make_request(
            "POST",
            "/api/v5/trade/cancel-order",
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
        params = {
            "instId": symbol,
            "clOrdId": order_id[:32]
        }
        
        response = await self._make_request(
            "GET",
            "/api/v5/trade/order",
            params=params,
            signed=True
        )
        
        order_data = response.get("data", [{}])[0] if response.get("data") else {}
        
        if order_data:
            return {
                "order_id": order_id,
                "exchange_order_id": order_data.get("ordId"),
                "status": normalize_order_status(order_data.get("state")),
                "filled_quantity": Decimal(order_data.get("fillSz", "0")),
                "average_price": Decimal(order_data.get("avgPx", "0")) if order_data.get("avgPx") else None
            }
            
        return {
            "order_id": order_id,
            "status": "UNKNOWN"
        }
        
    async def get_open_orders(self, symbol: Optional[str] = None) -> List[Dict[str, Any]]:
        """Get all open orders"""
        params = {}
        if symbol:
            params["instId"] = symbol
            
        response = await self._make_request(
            "GET",
            "/api/v5/trade/orders-pending",
            params=params,
            signed=True
        )
        
        orders = []
        for order_data in response.get("data", []):
            orders.append({
                "order_id": order_data.get("clOrdId", order_data.get("ordId")),
                "exchange_order_id": order_data.get("ordId"),
                "symbol": order_data.get("instId"),
                "side": "BUY" if order_data.get("side") == "buy" else "SELL",
                "type": order_data.get("ordType"),
                "status": normalize_order_status(order_data.get("state")),
                "quantity": Decimal(order_data.get("sz", "0")),
                "filled_quantity": Decimal(order_data.get("fillSz", "0")),
                "price": Decimal(order_data.get("px", "0")),
                "created_at": datetime.fromtimestamp(int(order_data.get("cTime", 0)) / 1000, tz=timezone.utc)
            })
            
        return orders
        
    async def create_grid_bot(
        self,
        symbol: str,
        lower_price: Decimal,
        upper_price: Decimal,
        grid_count: int,
        investment: Decimal
    ) -> Dict[str, Any]:
        """Create grid trading bot"""
        params = {
            "instId": symbol,
            "algoOrdType": "grid",
            "maxPx": str(upper_price),
            "minPx": str(lower_price),
            "gridNum": str(grid_count),
            "runType": "1",  # Start immediately
            "sz": str(investment),
            "tdMode": "cash",
            "quoteSz": str(investment)
        }
        
        response = await self._make_request(
            "POST",
            "/api/v5/tradingBot/grid/order-algo",
            params=params,
            signed=True
        )
        
        algo_data = response.get("data", [{}])[0]
        
        return {
            "algo_id": algo_data.get("algoId"),
            "status": "active",
            "created_at": datetime.now(timezone.utc)
        }
        
    async def stop_grid_bot(self, algo_id: str) -> Dict[str, Any]:
        """Stop grid trading bot"""
        params = {
            "algoId": algo_id
        }
        
        response = await self._make_request(
            "POST",
            "/api/v5/tradingBot/grid/stop-order-algo",
            params=params,
            signed=True
        )
        
        return {
            "algo_id": algo_id,
            "status": "stopped",
            "stopped_at": datetime.now(timezone.utc)
        }
        
    async def subscribe_ticker(self, symbol: str) -> AsyncGenerator[Ticker, None]:
        """Subscribe to ticker updates via WebSocket"""
        subscribe_msg = {
            "op": "subscribe",
            "args": [{
                "channel": "tickers",
                "instId": symbol
            }]
        }
        
        await self.public_ws.send(subscribe_msg)
        
        async for message in self.public_ws.receive():
            if message.get("arg", {}).get("channel") == "tickers":
                for ticker_data in message.get("data", []):
                    if ticker_data.get("instId") == symbol:
                        yield Ticker(
                            timestamp=datetime.fromtimestamp(int(ticker_data.get("ts", 0)) / 1000, tz=timezone.utc),
                            symbol=symbol,
                            bid=Decimal(ticker_data.get("bidPx", "0")),
                            ask=Decimal(ticker_data.get("askPx", "0")),
                            last=Decimal(ticker_data.get("last", "0")),
                            volume_24h=Decimal(ticker_data.get("vol24h", "0")),
                            high_24h=Decimal(ticker_data.get("high24h", "0")),
                            low_24h=Decimal(ticker_data.get("low24h", "0")),
                            change_24h=Decimal(ticker_data.get("sodUtc8", "0"))
                        )
                        
    async def subscribe_order_book(self, symbol: str) -> AsyncGenerator[OrderBook, None]:
        """Subscribe to order book updates via WebSocket"""
        # Create order book manager
        if symbol not in self.order_books:
            self.order_books[symbol] = OrderBookManager(symbol)
            
        manager = self.order_books[symbol]
        
        # Subscribe to order book
        subscribe_msg = {
            "op": "subscribe",
            "args": [{
                "channel": "books5",  # Top 5 levels
                "instId": symbol
            }]
        }
        
        await self.public_ws.send(subscribe_msg)
        
        async for message in self.public_ws.receive():
            if message.get("arg", {}).get("channel") == "books5":
                data = message.get("data", [{}])[0]
                
                if data.get("instId") == symbol:
                    bids = [(Decimal(b[0]), Decimal(b[1])) for b in data.get("bids", [])]
                    asks = [(Decimal(a[0]), Decimal(a[1])) for a in data.get("asks", [])]
                    
                    yield OrderBook(
                        timestamp=datetime.fromtimestamp(int(data.get("ts", 0)) / 1000, tz=timezone.utc),
                        symbol=symbol,
                        bids=bids,
                        asks=asks
                    )
                    
    async def subscribe_trades(self, symbol: str) -> AsyncGenerator[Dict[str, Any], None]:
        """Subscribe to trade updates via WebSocket"""
        subscribe_msg = {
            "op": "subscribe",
            "args": [{
                "channel": "trades",
                "instId": symbol
            }]
        }
        
        await self.public_ws.send(subscribe_msg)
        
        async for message in self.public_ws.receive():
            if message.get("arg", {}).get("channel") == "trades":
                for trade in message.get("data", []):
                    if trade.get("instId") == symbol:
                        yield {
                            "trade_id": trade.get("tradeId"),
                            "timestamp": datetime.fromtimestamp(int(trade.get("ts", 0)) / 1000, tz=timezone.utc),
                            "symbol": symbol,
                            "price": Decimal(trade.get("px", "0")),
                            "quantity": Decimal(trade.get("sz", "0")),
                            "side": trade.get("side"),
                            "is_buyer_maker": trade.get("side") == "buy"
                        }
