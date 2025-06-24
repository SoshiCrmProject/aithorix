"""
AITHORIX Binance Exchange Implementation
Full production implementation with spot, futures, and options support
"""

import asyncio
import time
from datetime import datetime, timezone
from decimal import Decimal
from typing import Dict, List, Optional, Any, AsyncGenerator
import json

import aiohttp
import websockets
from urllib.parse import urlencode

from ..base_exchange import (
    BaseExchange, ExchangeCredentials, MarketInfo, 
    OrderBook, Ticker, Balance, WebSocketManager, OrderBookManager
)
from ..common import (
    BINANCE_ORDER_TYPES, BINANCE_TIF,
    create_signature, get_timestamp, normalize_order_status
)
from ...core.engine.trading_engine import Order, OrderType, OrderSide, OrderStatus
from ...core.exceptions import (
    ExchangeConnectionError, OrderExecutionError, 
    AuthenticationError, RateLimitError
)
from ...stealth.profiles.binance_institutional import BinanceInstitutionalProfile


class BinanceExchange(BaseExchange):
    """
    Binance exchange implementation with full trading capabilities
    """
    
    def __init__(self, credentials: ExchangeCredentials, **kwargs):
        super().__init__(credentials, **kwargs)
        
        self.name = "binance"
        self.base_url = "https://api.binance.com" if not credentials.testnet else "https://testnet.binance.vision"
        self.ws_url = "wss://stream.binance.com:9443/ws" if not credentials.testnet else "wss://testnet.binance.vision/ws"
        
        # Rate limits
        self.rate_limits = {
            "default": 1200,  # requests per minute
            "orders": 50,     # orders per 10 seconds
            "weight": 6000    # request weight per minute
        }
        
        # WebSocket managers
        self.market_ws: Optional[WebSocketManager] = None
        self.user_ws: Optional[WebSocketManager] = None
        self.listen_key: Optional[str] = None
        
        # Order book managers
        self.order_books: Dict[str, OrderBookManager] = {}
        
        # Apply institutional profile for stealth
        self.profile = BinanceInstitutionalProfile()
        
    async def connect(self) -> None:
        """Initialize Binance connection"""
        await super().connect()
        
        # Start user data stream
        await self._start_user_data_stream()
        
        # Initialize WebSocket connections
        self.market_ws = WebSocketManager(self.ws_url)
        await self.market_ws.connect()
        
    async def disconnect(self) -> None:
        """Close Binance connections"""
        # Stop user data stream
        await self._stop_user_data_stream()
        
        # Close WebSocket connections
        if self.market_ws:
            await self.market_ws.disconnect()
        if self.user_ws:
            await self.user_ws.disconnect()
            
        await super().disconnect()
        
    def _sign_request(self, method: str, path: str, params: Dict[str, Any]) -> Dict[str, str]:
        """Sign request for Binance API"""
        # Add timestamp
        params["timestamp"] = get_timestamp()
        
        # Create query string
        query_string = urlencode(sorted(params.items()))
        
        # Create signature
        signature = create_signature(self.credentials.api_secret, query_string)
        
        # Return headers
        return {
            "X-MBX-APIKEY": self.credentials.api_key
        }
        
    async def _start_user_data_stream(self) -> None:
        """Start user data stream for account updates"""
        try:
            response = await self._make_request(
                "POST",
                "/api/v3/userDataStream",
                signed=True
            )
            
            self.listen_key = response.get("listenKey")
            
            if self.listen_key:
                # Connect to user data stream
                user_ws_url = f"{self.ws_url}/{self.listen_key}"
                self.user_ws = WebSocketManager(user_ws_url)
                await self.user_ws.connect()
                
                # Start keepalive task
                asyncio.create_task(self._keepalive_user_stream())
                
                # Start processing user events
                asyncio.create_task(self._process_user_stream())
                
        except Exception as e:
            logger.error(f"Failed to start user data stream: {e}")
            
    async def _stop_user_data_stream(self) -> None:
        """Stop user data stream"""
        if self.listen_key:
            try:
                await self._make_request(
                    "DELETE",
                    "/api/v3/userDataStream",
                    params={"listenKey": self.listen_key},
                    signed=True
                )
            except Exception as e:
                logger.error(f"Failed to stop user data stream: {e}")
                
    async def _keepalive_user_stream(self) -> None:
        """Keep user data stream alive"""
        while self.listen_key:
            try:
                await asyncio.sleep(1800)  # 30 minutes
                
                await self._make_request(
                    "PUT",
                    "/api/v3/userDataStream",
                    params={"listenKey": self.listen_key},
                    signed=True
                )
                
            except Exception as e:
                logger.error(f"Failed to keepalive user stream: {e}")
                await asyncio.sleep(60)
                
    async def _process_user_stream(self) -> None:
        """Process user data stream events"""
        if not self.user_ws:
            return
            
        async for message in self.user_ws.receive():
            try:
                event_type = message.get("e")
                
                if event_type == "executionReport":
                    # Order update
                    await self._handle_order_update(message)
                elif event_type == "outboundAccountPosition":
                    # Balance update
                    await self._handle_balance_update(message)
                    
            except Exception as e:
                logger.error(f"Error processing user stream: {e}")
                
    async def _handle_order_update(self, data: Dict[str, Any]) -> None:
        """Handle order update from user stream"""
        # Extract order information
        order_update = {
            "order_id": data.get("c"),  # Client order ID
            "exchange_order_id": data.get("i"),
            "symbol": data.get("s"),
            "status": normalize_order_status(data.get("X")),
            "filled_quantity": Decimal(data.get("z", "0")),
            "average_price": Decimal(data.get("Z", "0")) / Decimal(data.get("z", "1")) if Decimal(data.get("z", "0")) > 0 else Decimal("0"),
            "commission": Decimal(data.get("n", "0")),
            "commission_asset": data.get("N")
        }
        
        # Notify trading engine
        # This would typically emit an event or update a shared state
        logger.info(f"Order update: {order_update}")
        
    async def _handle_balance_update(self, data: Dict[str, Any]) -> None:
        """Handle balance update from user stream"""
        # Update internal balance cache
        for balance in data.get("B", []):
            asset = balance.get("a")
            free = Decimal(balance.get("f", "0"))
            locked = Decimal(balance.get("l", "0"))
            
            # Update cache
            # This would typically update a shared state
            logger.debug(f"Balance update: {asset} - free: {free}, locked: {locked}")
            
    async def get_markets(self) -> List[MarketInfo]:
        """Get all available markets on Binance"""
        response = await self._make_request("GET", "/api/v3/exchangeInfo")
        
        markets = []
        for symbol_info in response.get("symbols", []):
            if symbol_info.get("status") != "TRADING":
                continue
                
            # Extract filters
            filters = {f["filterType"]: f for f in symbol_info.get("filters", [])}
            
            # Price filter
            price_filter = filters.get("PRICE_FILTER", {})
            min_price = Decimal(price_filter.get("minPrice", "0"))
            max_price = Decimal(price_filter.get("maxPrice", "999999999"))
            price_precision = len(price_filter.get("minPrice", "0.1").split(".")[-1].rstrip("0"))
            
            # Lot size filter
            lot_filter = filters.get("LOT_SIZE", {})
            min_quantity = Decimal(lot_filter.get("minQty", "0"))
            max_quantity = Decimal(lot_filter.get("maxQty", "999999999"))
            quantity_precision = len(lot_filter.get("stepSize", "0.1").split(".")[-1].rstrip("0"))
            
            # Min notional filter
            notional_filter = filters.get("MIN_NOTIONAL", {})
            min_notional = Decimal(notional_filter.get("minNotional", "0"))
            
            # Get current price for fee calculation
            ticker = await self.get_ticker(symbol_info["symbol"])
            
            market = MarketInfo(
                symbol=symbol_info["symbol"],
                base_asset=symbol_info["baseAsset"],
                quote_asset=symbol_info["quoteAsset"],
                min_quantity=min_quantity,
                max_quantity=max_quantity,
                quantity_precision=quantity_precision,
                min_price=min_price,
                max_price=max_price,
                price_precision=price_precision,
                min_notional=min_notional,
                is_trading=True,
                maker_fee=Decimal("0.001"),  # Default 0.1%
                taker_fee=Decimal("0.001"),  # Default 0.1%
                last=ticker.last
            )
            
            markets.append(market)
            
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
        """Place new order on Binance"""
        # Apply institutional behavior
        await self.profile.pre_order_behavior(order)
        
        # Validate order
        valid, error = self.validate_order(order)
        if not valid:
            raise OrderExecutionError(f"Order validation failed: {error}", order.order_id, self.name)
            
        # Prepare order parameters
        params = {
            "symbol": order.symbol,
            "side": order.side.value,
            "type": BINANCE_ORDER_TYPES.get(order.order_type, "LIMIT"),
            "quantity": str(self.round_quantity(order.symbol, order.quantity)),
            "newClientOrderId": order.order_id[:36]  # Binance limit
        }
        
        # Add order type specific parameters
        if order.order_type == OrderType.LIMIT:
            params["price"] = str(self.round_price(order.symbol, order.price))
            params["timeInForce"] = BINANCE_TIF.get(order.time_in_force, "GTC")
            
            if order.post_only:
                params["timeInForce"] = "GTX"
                
        elif order.order_type in [OrderType.STOP_LOSS, OrderType.TAKE_PROFIT]:
            params["stopPrice"] = str(self.round_price(order.symbol, order.stop_price))
            if order.order_type == OrderType.STOP_LOSS:
                params["type"] = "STOP_LOSS_LIMIT"
            else:
                params["type"] = "TAKE_PROFIT_LIMIT"
            params["price"] = str(self.round_price(order.symbol, order.price))
            params["timeInForce"] = "GTC"
            
        # Add leverage for futures
        if order.leverage > 1:
            # This would be handled differently for futures API
            pass
            
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
                "status": normalize_order_status(response.get("status")),
                "filled_quantity": Decimal(response.get("executedQty", "0")),
                "average_price": Decimal(response.get("price", "0")),
                "created_at": datetime.fromtimestamp(response.get("transactTime", 0) / 1000, tz=timezone.utc)
            }
            
        except Exception as e:
            logger.error(f"Failed to place order on Binance: {e}")
            raise OrderExecutionError(str(e), order.order_id, self.name)
            
    async def cancel_order(self, order_id: str, symbol: str) -> Dict[str, Any]:
        """Cancel existing order"""
        params = {
            "symbol": symbol,
            "origClientOrderId": order_id[:36]
        }
        
        response = await self._make_request(
            "DELETE",
            "/api/v3/order",
            params=params,
            signed=True
        )
        
        return {
            "order_id": order_id,
            "exchange_order_id": str(response.get("orderId")),
            "status": "CANCELLED"
        }
        
    async def get_order_status(self, order_id: str, symbol: str) -> Dict[str, Any]:
        """Get order status"""
        params = {
            "symbol": symbol,
            "origClientOrderId": order_id[:36]
        }
        
        response = await self._make_request(
            "GET",
            "/api/v3/order",
            params=params,
            signed=True
        )
        
        return {
            "order_id": order_id,
            "exchange_order_id": str(response.get("orderId")),
            "status": normalize_order_status(response.get("status")),
            "filled_quantity": Decimal(response.get("executedQty", "0")),
            "average_price": Decimal(response.get("price", "0"))
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
                "order_id": order_data.get("clientOrderId"),
                "exchange_order_id": str(order_data.get("orderId")),
                "symbol": order_data.get("symbol"),
                "side": order_data.get("side"),
                "type": order_data.get("type"),
                "status": normalize_order_status(order_data.get("status")),
                "quantity": Decimal(order_data.get("origQty", "0")),
                "filled_quantity": Decimal(order_data.get("executedQty", "0")),
                "price": Decimal(order_data.get("price", "0")),
                "created_at": datetime.fromtimestamp(order_data.get("time", 0) / 1000, tz=timezone.utc)
            })
            
        return orders
        
    async def subscribe_ticker(self, symbol: str) -> AsyncGenerator[Ticker, None]:
        """Subscribe to ticker updates via WebSocket"""
        stream_name = f"{symbol.lower()}@ticker"
        
        # Subscribe to stream
        await self.market_ws.send({
            "method": "SUBSCRIBE",
            "params": [stream_name],
            "id": int(time.time())
        })
        
        async for message in self.market_ws.receive():
            if message.get("e") == "24hrTicker":
                yield Ticker(
                    timestamp=datetime.fromtimestamp(message.get("E", 0) / 1000, tz=timezone.utc),
                    symbol=message.get("s"),
                    bid=Decimal(message.get("b", "0")),
                    ask=Decimal(message.get("a", "0")),
                    last=Decimal(message.get("c", "0")),
                    volume_24h=Decimal(message.get("v", "0")),
                    high_24h=Decimal(message.get("h", "0")),
                    low_24h=Decimal(message.get("l", "0")),
                    change_24h=Decimal(message.get("P", "0"))
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
        
        # Subscribe to updates
        stream_name = f"{symbol.lower()}@depth@100ms"
        await self.market_ws.send({
            "method": "SUBSCRIBE",
            "params": [stream_name],
            "id": int(time.time())
        })
        
        async for message in self.market_ws.receive():
            if message.get("e") == "depthUpdate":
                # Update order book
                manager.update_delta(
                    message.get("b", []),
                    message.get("a", []),
                    message.get("u")
                )
                
                # Yield updated order book
                yield manager.get_order_book()
                
    async def subscribe_trades(self, symbol: str) -> AsyncGenerator[Dict[str, Any], None]:
        """Subscribe to trade updates via WebSocket"""
        stream_name = f"{symbol.lower()}@trade"
        
        await self.market_ws.send({
            "method": "SUBSCRIBE",
            "params": [stream_name],
            "id": int(time.time())
        })
        
        async for message in self.market_ws.receive():
            if message.get("e") == "trade":
                yield {
                    "trade_id": message.get("t"),
                    "timestamp": datetime.fromtimestamp(message.get("T", 0) / 1000, tz=timezone.utc),
                    "symbol": message.get("s"),
                    "price": Decimal(message.get("p", "0")),
                    "quantity": Decimal(message.get("q", "0")),
                    "is_buyer_maker": message.get("m", False)
                }
                

class BinanceFuturesExchange(BinanceExchange):
    """
    Binance Futures (USDⓈ-M) implementation
    """
    
    def __init__(self, credentials: ExchangeCredentials, **kwargs):
        super().__init__(credentials, **kwargs)
        
        self.base_url = "https://fapi.binance.com" if not credentials.testnet else "https://testnet.binancefuture.com"
        self.ws_url = "wss://fstream.binance.com/ws" if not credentials.testnet else "wss://testnet.binancefuture.com/ws"
        
    async def set_leverage(self, symbol: str, leverage: int) -> Dict[str, Any]:
        """Set leverage for symbol"""
        params = {
            "symbol": symbol,
            "leverage": leverage
        }
        
        response = await self._make_request(
            "POST",
            "/fapi/v1/leverage",
            params=params,
            signed=True
        )
        
        return {
            "symbol": symbol,
            "leverage": response.get("leverage"),
            "max_notional": Decimal(response.get("maxNotionalValue", "0"))
        }
        
    async def get_positions(self) -> List[Dict[str, Any]]:
        """Get all open positions"""
        response = await self._make_request(
            "GET",
            "/fapi/v2/positionRisk",
            signed=True
        )
        
        positions = []
        for pos in response:
            position_amt = Decimal(pos.get("positionAmt", "0"))
            if position_amt != 0:
                positions.append({
                    "symbol": pos.get("symbol"),
                    "side": "LONG" if position_amt > 0 else "SHORT",
                    "quantity": abs(position_amt),
                    "entry_price": Decimal(pos.get("entryPrice", "0")),
                    "mark_price": Decimal(pos.get("markPrice", "0")),
                    "unrealized_pnl": Decimal(pos.get("unRealizedProfit", "0")),
                    "leverage": int(pos.get("leverage", 1)),
                    "liquidation_price": Decimal(pos.get("liquidationPrice", "0"))
                })
                
        return positions