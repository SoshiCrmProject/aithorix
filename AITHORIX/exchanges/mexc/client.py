"""
AITHORIX MEXC Exchange Implementation
Full production implementation with spot and futures support
Specialized for altcoin trading and new listings
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
    MEXC_ORDER_TYPES, MEXC_TIF,
    get_timestamp, normalize_order_status
)
from ...core.engine.trading_engine import Order, OrderType, OrderSide, OrderStatus
from ...core.exceptions import (
    ExchangeConnectionError, OrderExecutionError,
    AuthenticationError
)
from ...stealth.profiles.mexc_retail import MEXCRetailProfile


class MEXCExchange(BaseExchange):
    """
    MEXC exchange implementation
    Optimized for altcoin discovery and high-frequency retail trading
    """
    
    def __init__(self, credentials: ExchangeCredentials, **kwargs):
        super().__init__(credentials, **kwargs)
        
        self.name = "mexc"
        self.base_url = "https://api.mexc.com"
        self.ws_url = "wss://wbs.mexc.com/ws"
        self.contract_url = "https://contract.mexc.com"
        
        # Rate limits (more restrictive than others)
        self.rate_limits = {
            "default": 20,    # requests per second
            "orders": 10,     # orders per second
            "public": 10      # public endpoints per second
        }
        
        # MEXC specific features
        self.supports_margin = True
        self.supports_futures = True
        self.supports_otc = True
        self.max_open_orders = 200
        
        # WebSocket managers
        self.spot_ws: Optional[WebSocketManager] = None
        self.futures_ws: Optional[WebSocketManager] = None
        
        # Apply retail trader profile
        self.profile = MEXCRetailProfile()
        
        # New listing tracker
        self.new_listings: Dict[str, datetime] = {}
        self._last_listing_check = 0
        
    async def connect(self) -> None:
        """Initialize MEXC connection"""
        await super().connect()
        
        # Initialize WebSocket connections
        self.spot_ws = WebSocketManager(self.ws_url)
        await self.spot_ws.connect()
        
        # Start new listing monitor
        asyncio.create_task(self._monitor_new_listings())
        
    async def disconnect(self) -> None:
        """Close MEXC connections"""
        if self.spot_ws:
            await self.spot_ws.disconnect()
        if self.futures_ws:
            await self.futures_ws.disconnect()
            
        await super().disconnect()
        
    def _sign_request(self, method: str, path: str, params: Dict[str, Any]) -> Dict[str, str]:
        """Sign request for MEXC API"""
        timestamp = str(get_timestamp())
        
        # MEXC uses different signing for different endpoints
        if "/api/v3/" in path:
            # Spot API signing
            params["timestamp"] = timestamp
            params["recvWindow"] = "5000"
            
            # Sort parameters
            sorted_params = sorted(params.items())
            query_string = urlencode(sorted_params)
            
            # Create signature
            signature = hmac.new(
                self.credentials.api_secret.encode('utf-8'),
                query_string.encode('utf-8'),
                hashlib.sha256
            ).hexdigest()
            
            return {
                "X-MEXC-APIKEY": self.credentials.api_key,
                "Content-Type": "application/json"
            }
        else:
            # Contract API signing
            request_body = json.dumps(params) if params else ""
            
            sign_string = f"{self.credentials.api_key}{timestamp}{request_body}"
            signature = hmac.new(
                self.credentials.api_secret.encode('utf-8'),
                sign_string.encode('utf-8'),
                hashlib.sha256
            ).hexdigest()
            
            return {
                "ApiKey": self.credentials.api_key,
                "Request-Time": timestamp,
                "Signature": signature,
                "Content-Type": "application/json"
            }
            
    async def _monitor_new_listings(self) -> None:
        """Monitor for new token listings"""
        while True:
            try:
                current_time = time.time()
                
                # Check every 5 minutes
                if current_time - self._last_listing_check > 300:
                    await self._check_new_listings()
                    self._last_listing_check = current_time
                    
                await asyncio.sleep(60)
                
            except Exception as e:
                logger.error(f"Error monitoring new listings: {e}")
                await asyncio.sleep(300)
                
    async def _check_new_listings(self) -> None:
        """Check for newly listed tokens"""
        try:
            markets = await self.get_markets()
            
            for market in markets:
                symbol = market.symbol
                
                # Check if this is a new symbol
                if symbol not in self.new_listings:
                    # Check if trading recently started
                    # This would need additional API calls to verify
                    
                    # For now, mark as potential new listing
                    self.new_listings[symbol] = datetime.now(timezone.utc)
                    
                    # Notify profile for new listing strategy
                    if await self.profile.should_trade_new_listing(symbol):
                        logger.info(f"New listing detected: {symbol}")
                        
        except Exception as e:
            logger.error(f"Error checking new listings: {e}")
            
    async def get_markets(self) -> List[MarketInfo]:
        """Get all available markets on MEXC"""
        response = await self._make_request("GET", "/api/v3/exchangeInfo")
        
        markets = []
        for symbol_info in response.get("symbols", []):
            if symbol_info.get("status") != "ENABLED":
                continue
                
            # Check if it's a spot market
            if symbol_info.get("contractType"):
                continue
                
            # Parse trading rules
            base_asset = symbol_info["baseAsset"]
            quote_asset = symbol_info["quoteAsset"]
            
            # Extract limits
            min_quantity = Decimal(symbol_info.get("baseSizePrecision", "0.00000001"))
            max_quantity = Decimal(symbol_info.get("maxOrderAmount", "99999999"))
            quantity_precision = int(symbol_info.get("baseAssetPrecision", 8))
            
            min_price = Decimal(symbol_info.get("minOrderPrice", "0.00000001"))
            max_price = Decimal(symbol_info.get("maxOrderPrice", "99999999"))
            price_precision = int(symbol_info.get("quotePrecision", 8))
            
            min_notional = Decimal(symbol_info.get("minOrderValue", "5"))
            
            # Get current price for fee calculation
            try:
                ticker = await self.get_ticker(f"{base_asset}{quote_asset}")
                last_price = ticker.last
            except:
                last_price = Decimal("0")
                
            market = MarketInfo(
                symbol=f"{base_asset}{quote_asset}",
                base_asset=base_asset,
                quote_asset=quote_asset,
                min_quantity=min_quantity,
                max_quantity=max_quantity,
                quantity_precision=quantity_precision,
                min_price=min_price,
                max_price=max_price,
                price_precision=price_precision,
                min_notional=min_notional,
                is_trading=True,
                maker_fee=Decimal("0.002"),  # 0.2% default
                taker_fee=Decimal("0.002"),  # 0.2% default
                last=last_price
            )
            
            markets.append(market)
            
            # Check for MX fee discount
            if symbol_info.get("mxDeductEnable"):
                market.maker_fee = Decimal("0.0002")  # 0.02% with MX
                market.taker_fee = Decimal("0.0002")
                
        return markets
        
    async def get_ticker(self, symbol: str) -> Ticker:
        """Get current ticker for symbol"""
        response = await self._make_request(
            "GET",
            "/api/v3/ticker/24hr",
            params={"symbol": symbol}
        )
        
        return Ticker(
            timestamp=datetime.now(timezone.utc),
            symbol=symbol,
            bid=Decimal(response.get("bidPrice", "0")),
            ask=Decimal(response.get("askPrice", "0")),
            last=Decimal(response.get("lastPrice", "0")),
            volume_24h=Decimal(response.get("volume", "0")),
            high_24h=Decimal(response.get("highPrice", "0")),
            low_24h=Decimal(response.get("lowPrice", "0")),
            change_24h=Decimal(response.get("priceChangePercent", "0"))
        )
        
    async def get_order_book(self, symbol: str, limit: int = 100) -> OrderBook:
        """Get order book for symbol"""
        # MEXC limits: 5, 10, 20, 50, 100, 500, 1000
        valid_limits = [5, 10, 20, 50, 100, 500, 1000]
        limit = min(valid_limits, key=lambda x: abs(x - limit))
        
        response = await self._make_request(
            "GET",
            "/api/v3/depth",
            params={"symbol": symbol, "limit": limit}
        )
        
        bids = [(Decimal(b[0]), Decimal(b[1])) for b in response.get("bids", [])]
        asks = [(Decimal(a[0]), Decimal(a[1])) for a in response.get("asks", [])]
        
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
            "/api/v3/account",
            signed=True
        )
        
        balances = []
        for balance_data in response.get("balances", []):
            free = Decimal(balance_data.get("free", "0"))
            locked = Decimal(balance_data.get("locked", "0"))
            
            if free > 0 or locked > 0:
                balance = Balance(
                    asset=balance_data["asset"],
                    free=free,
                    locked=locked,
                    total=free + locked
                )
                balances.append(balance)
                
        return balances
        
    async def place_order(self, order: Order) -> Dict[str, Any]:
        """Place new order on MEXC"""
        # Apply retail behavior
        await self.profile.pre_order_behavior(order)
        
        # Check if this might be a new listing
        if order.symbol in self.new_listings:
            listing_time = self.new_listings[order.symbol]
            time_since_listing = (datetime.now(timezone.utc) - listing_time).total_seconds()
            
            # Special handling for new listings (first hour)
            if time_since_listing < 3600:
                order = await self.profile.adjust_for_new_listing(order, time_since_listing)
                
        # Validate order
        valid, error = self.validate_order(order)
        if not valid:
            raise OrderExecutionError(f"Order validation failed: {error}", order.order_id, self.name)
            
        # Prepare order parameters
        params = {
            "symbol": order.symbol,
            "side": order.side.value,
            "type": MEXC_ORDER_TYPES.get(order.order_type, "LIMIT"),
            "quantity": str(self.round_quantity(order.symbol, order.quantity))
        }
        
        # Add order type specific parameters
        if order.order_type == OrderType.LIMIT:
            params["price"] = str(self.round_price(order.symbol, order.price))
            params["timeInForce"] = MEXC_TIF.get(order.time_in_force, "GTC")
            
            if order.post_only:
                params["type"] = "LIMIT_MAKER"
                
        elif order.order_type in [OrderType.STOP_LOSS, OrderType.TAKE_PROFIT]:
            # MEXC stop orders
            params["stopPrice"] = str(self.round_price(order.symbol, order.stop_price))
            if order.price:
                params["price"] = str(self.round_price(order.symbol, order.price))
                
        # Place order
        try:
            response = await self._make_request(
                "POST",
                "/api/v3/order",
                params=params,
                signed=True
            )
            
            # Apply post-order behavior
            await self.profile.post_order_behavior(order, response)
            
            return {
                "order_id": order.order_id,
                "exchange_order_id": str(response.get("orderId")),
                "symbol": response.get("symbol"),
                "status": normalize_order_status(response.get("status")),
                "filled_quantity": Decimal(response.get("executedQty", "0")),
                "created_at": datetime.fromtimestamp(response.get("transactTime", 0) / 1000, tz=timezone.utc)
            }
            
        except Exception as e:
            logger.error(f"Failed to place order on MEXC: {e}")
            raise OrderExecutionError(str(e), order.order_id, self.name)
            
    async def cancel_order(self, order_id: str, symbol: str) -> Dict[str, Any]:
        """Cancel existing order"""
        # MEXC requires orderId, not clientOrderId
        # First, we need to find the order
        open_orders = await self.get_open_orders(symbol)
        
        exchange_order_id = None
        for open_order in open_orders:
            if open_order.get("order_id") == order_id:
                exchange_order_id = open_order.get("exchange_order_id")
                break
                
        if not exchange_order_id:
            raise OrderExecutionError(f"Order {order_id} not found", order_id, self.name)
            
        params = {
            "symbol": symbol,
            "orderId": exchange_order_id
        }
        
        response = await self._make_request(
            "DELETE",
            "/api/v3/order",
            params=params,
            signed=True
        )
        
        return {
            "order_id": order_id,
            "exchange_order_id": exchange_order_id,
            "status": "CANCELLED",
            "cancelled_at": datetime.now(timezone.utc)
        }
        
    async def get_order_status(self, order_id: str, symbol: str) -> Dict[str, Any]:
        """Get order status"""
        # Find order by client order id
        params = {
            "symbol": symbol
        }
        
        response = await self._make_request(
            "GET",
            "/api/v3/allOrders",
            params=params,
            signed=True
        )
        
        for order_data in response:
            if order_data.get("clientOrderId") == order_id:
                return {
                    "order_id": order_id,
                    "exchange_order_id": str(order_data.get("orderId")),
                    "status": normalize_order_status(order_data.get("status")),
                    "filled_quantity": Decimal(order_data.get("executedQty", "0")),
                    "average_price": Decimal(order_data.get("avgPrice", "0"))
                }
                
        return {
            "order_id": order_id,
            "status": "UNKNOWN"
        }
        
    async def get_open_orders(self, symbol: Optional[str] = None) -> List[Dict[str, Any]]:
        """Get all open orders"""
        params = {}
        if symbol:
            params["symbol"] = symbol
            
        response = await self._make_request(
            "GET",
            "/api/v3/openOrders",
            params=params,
            signed=True
        )
        
        orders = []
        for order_data in response:
            orders.append({
                "order_id": order_data.get("clientOrderId", str(order_data.get("orderId"))),
                "exchange_order_id": str(order_data.get("orderId")),
                "symbol": order_data.get("symbol"),
                "side": order_data.get("side"),
                "type": order_data.get("type"),
                "status": normalize_order_status(order_data.get("status")),
                "quantity": Decimal(order_data.get("origQty", "0")),
                "filled_quantity": Decimal(order_data.get("executedQty", "0")),
                "price": Decimal(order_data.get("price", "0")),
                "stop_price": Decimal(order_data.get("stopPrice", "0")),
                "created_at": datetime.fromtimestamp(order_data.get("time", 0) / 1000, tz=timezone.utc)
            })
            
        return orders
        
    async def get_recent_trades(self, symbol: str, limit: int = 100) -> List[Dict[str, Any]]:
        """Get recent trades for market analysis"""
        response = await self._make_request(
            "GET",
            "/api/v3/trades",
            params={"symbol": symbol, "limit": limit}
        )
        
        trades = []
        for trade in response:
            trades.append({
                "trade_id": trade.get("id"),
                "price": Decimal(trade.get("price", "0")),
                "quantity": Decimal(trade.get("qty", "0")),
                "time": datetime.fromtimestamp(trade.get("time", 0) / 1000, tz=timezone.utc),
                "is_buyer_maker": trade.get("isBuyerMaker", False)
            })
            
        return trades
        
    async def subscribe_ticker(self, symbol: str) -> AsyncGenerator[Ticker, None]:
        """Subscribe to ticker updates via WebSocket"""
        # Subscribe to ticker stream
        subscribe_msg = {
            "method": "SUBSCRIPTION",
            "params": [f"spot@public.miniTicker.v3.api@{symbol}"]
        }
        
        await self.spot_ws.send(subscribe_msg)
        
        async for message in self.spot_ws.receive():
            if message.get("c") == f"spot@public.miniTicker.v3.api@{symbol}":
                data = message.get("d", {})
                
                yield Ticker(
                    timestamp=datetime.fromtimestamp(data.get("t", 0) / 1000, tz=timezone.utc),
                    symbol=symbol,
                    bid=Decimal(data.get("b", "0")),
                    ask=Decimal(data.get("a", "0")),
                    last=Decimal(data.get("c", "0")),
                    volume_24h=Decimal(data.get("v", "0")),
                    high_24h=Decimal(data.get("h", "0")),
                    low_24h=Decimal(data.get("l", "0")),
                    change_24h=Decimal(data.get("p", "0"))
                )
                
    async def subscribe_order_book(self, symbol: str) -> AsyncGenerator[OrderBook, None]:
        """Subscribe to order book updates via WebSocket"""
        # Create order book manager
        if symbol not in self.order_books:
            self.order_books[symbol] = OrderBookManager(symbol)
            
        manager = self.order_books[symbol]
        
        # Get initial snapshot
        snapshot = await self.get_order_book(symbol)
        manager.update_snapshot(
            [[str(p), str(q)] for p, q in snapshot.bids],
            [[str(p), str(q)] for p, q in snapshot.asks]
        )
        
        # Subscribe to depth updates
        subscribe_msg = {
            "method": "SUBSCRIPTION",
            "params": [f"spot@public.bookTicker.v3.api@{symbol}"]
        }
        
        await self.spot_ws.send(subscribe_msg)
        
        async for message in self.spot_ws.receive():
            if message.get("c") == f"spot@public.bookTicker.v3.api@{symbol}":
                data = message.get("d", {})
                
                # Update best bid/ask
                # For full depth, would need to subscribe to depth stream
                yield OrderBook(
                    timestamp=datetime.now(timezone.utc),
                    symbol=symbol,
                    bids=[(Decimal(data.get("b", "0")), Decimal(data.get("B", "0")))],
                    asks=[(Decimal(data.get("a", "0")), Decimal(data.get("A", "0")))]
                )
                
    async def subscribe_trades(self, symbol: str) -> AsyncGenerator[Dict[str, Any], None]:
        """Subscribe to trade updates via WebSocket"""
        subscribe_msg = {
            "method": "SUBSCRIPTION",
            "params": [f"spot@public.trade.v3.api@{symbol}"]
        }
        
        await self.spot_ws.send(subscribe_msg)
        
        async for message in self.spot_ws.receive():
            if message.get("c") == f"spot@public.trade.v3.api@{symbol}":
                data = message.get("d", {})
                
                yield {
                    "trade_id": data.get("t"),
                    "timestamp": datetime.fromtimestamp(data.get("T", 0) / 1000, tz=timezone.utc),
                    "symbol": symbol,
                    "price": Decimal(data.get("p", "0")),
                    "quantity": Decimal(data.get("q", "0")),
                    "is_buyer_maker": data.get("m", False)
                }


class MEXCFuturesExchange(MEXCExchange):
    """
    MEXC Futures implementation
    """
    
    def __init__(self, credentials: ExchangeCredentials, **kwargs):
        super().__init__(credentials, **kwargs)
        
        self.base_url = self.contract_url
        self.ws_url = "wss://contract.mexc.com/ws"
        
    async def get_futures_markets(self) -> List[Dict[str, Any]]:
        """Get futures market information"""
        response = await self._make_request("GET", "/api/v1/contract/detail")
        
        markets = []
        for contract in response.get("data", []):
            markets.append({
                "symbol": contract["symbol"],
                "contract_size": Decimal(contract["contractSize"]),
                "price_unit": Decimal(contract["priceUnit"]),
                "vol_unit": Decimal(contract["volUnit"]),
                "min_leverage": contract["minLeverage"],
                "max_leverage": contract["maxLeverage"],
                "maintenance_margin_rate": Decimal(contract["maintainMarginRate"])
            })
            
        return markets
        
    async def set_leverage(self, symbol: str, leverage: int) -> Dict[str, Any]:
        """Set leverage for futures trading"""
        params = {
            "symbol": symbol,
            "leverage": leverage,
            "openType": 2  # Cross margin
        }
        
        response = await self._make_request(
            "POST",
            "/api/v1/private/position/change_leverage",
            params=params,
            signed=True
        )
        
        return {
            "symbol": symbol,
            "leverage": response.get("data", {}).get("leverage", leverage)
        }
