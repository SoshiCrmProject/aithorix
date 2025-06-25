"""
AITHORIX Binance Client Implementation
Main client for Binance exchange with spot, futures, and options support
"""

import asyncio
import logging
from datetime import datetime, timezone
from typing import Dict, List, Optional, Any, Callable
import time
import json

from ..base_exchange import (
    BaseExchange, ExchangeConfig, Order, Trade, Position,
    Balance, Ticker, OrderBook, Candle, OrderType, OrderSide,
    OrderStatus, TimeInForce, PositionSide
)
from .spot.spot_trading import BinanceSpotTrading
from .futures.futures_trading import BinanceFuturesTrading
from .options.options_trading import BinanceOptionsTrading
from .websocket.ws_client import BinanceWebSocketClient
from .auth.authenticator import BinanceAuthenticator

logger = logging.getLogger(__name__)


class BinanceClient(BaseExchange):
    """
    Binance exchange client implementation
    Supports spot, futures, and options trading
    """
    
    def __init__(self, config: ExchangeConfig):
        super().__init__(config)
        
        # Set Binance-specific URLs
        if config.testnet:
            self.config.rest_url = "https://testnet.binance.vision"
            self.config.ws_url = "wss://testnet.binance.vision/ws"
        else:
            self.config.rest_url = "https://api.binance.com"
            self.config.ws_url = "wss://stream.binance.com:9443/ws"
        
        # Initialize components
        self.authenticator = BinanceAuthenticator(config)
        self.spot_trading = BinanceSpotTrading(self)
        self.futures_trading = BinanceFuturesTrading(self)
        self.options_trading = BinanceOptionsTrading(self)
        self.ws_client = BinanceWebSocketClient(self)
        
        # Market info
        self.exchange_info: Dict[str, Any] = {}
        self.symbol_info: Dict[str, Any] = {}
        
        # User data stream
        self.listen_key: Optional[str] = None
        self.listen_key_task: Optional[asyncio.Task] = None
        
        logger.info("Initialized Binance client")
    
    async def _initialize_exchange(self):
        """Binance-specific initialization"""
        # Load exchange info
        await self._load_exchange_info()
        
        # Start user data stream if authenticated
        if self.config.api_key:
            await self._start_user_data_stream()
    
    async def _load_markets(self):
        """Load Binance market information"""
        await self._load_exchange_info()
    
    async def _load_exchange_info(self):
        """Load exchange trading rules and symbols"""
        try:
            # Get spot exchange info
            spot_info = await self._get("/api/v3/exchangeInfo")
            
            # Get futures exchange info
            futures_info = await self._get("/fapi/v1/exchangeInfo")
            
            # Process symbols
            self.exchange_info = {
                'spot': spot_info,
                'futures': futures_info
            }
            
            # Build symbol info cache
            for symbol_data in spot_info.get('symbols', []):
                symbol = symbol_data['symbol']
                self.symbol_info[symbol] = {
                    'type': 'spot',
                    'base': symbol_data['baseAsset'],
                    'quote': symbol_data['quoteAsset'],
                    'status': symbol_data['status'],
                    'filters': {f['filterType']: f for f in symbol_data['filters']},
                    'permissions': symbol_data['permissions']
                }
            
            for symbol_data in futures_info.get('symbols', []):
                symbol = symbol_data['symbol']
                self.symbol_info[f"{symbol}_PERP"] = {
                    'type': 'futures',
                    'base': symbol_data['baseAsset'],
                    'quote': symbol_data['quoteAsset'],
                    'status': symbol_data['status'],
                    'filters': {f['filterType']: f for f in symbol_data['filters']},
                    'contractType': symbol_data.get('contractType', 'PERPETUAL')
                }
            
            logger.info(f"Loaded {len(self.symbol_info)} symbols from Binance")
            
        except Exception as e:
            logger.error(f"Failed to load exchange info: {str(e)}")
            raise
    
    async def _check_connection(self):
        """Check Binance connection"""
        # Ping endpoint
        await self._get("/api/v3/ping")
        
        # Get server time
        response = await self._get("/api/v3/time")
        server_time = response['serverTime']
        local_time = int(time.time() * 1000)
        
        time_diff = abs(server_time - local_time)
        if time_diff > 5000:  # 5 seconds
            logger.warning(f"Time sync issue: server time differs by {time_diff}ms")
    
    async def _get_auth_headers(
        self,
        method: str,
        endpoint: str,
        params: Optional[Dict],
        data: Optional[Dict]
    ) -> Dict[str, str]:
        """Generate Binance authentication headers"""
        return await self.authenticator.get_auth_headers(method, endpoint, params, data)
    
    async def _start_user_data_stream(self):
        """Start user data stream for account updates"""
        try:
            # Create listen key
            response = await self._post("/api/v3/userDataStream", signed=True)
            self.listen_key = response['listenKey']
            
            # Start keepalive task
            self.listen_key_task = asyncio.create_task(self._keepalive_listen_key())
            
            logger.info("Started Binance user data stream")
            
        except Exception as e:
            logger.error(f"Failed to start user data stream: {str(e)}")
    
    async def _keepalive_listen_key(self):
        """Keep listen key alive"""
        while self.is_initialized:
            try:
                await asyncio.sleep(1800)  # 30 minutes
                
                if self.listen_key:
                    await self._request(
                        "PUT",
                        "/api/v3/userDataStream",
                        params={'listenKey': self.listen_key},
                        signed=True
                    )
                    logger.debug("Renewed listen key")
                    
            except Exception as e:
                logger.error(f"Failed to renew listen key: {str(e)}")
                # Try to recreate
                await self._start_user_data_stream()
    
    # Trading methods
    async def place_order(
        self,
        symbol: str,
        side: str,
        order_type: OrderType,
        size: float,
        price: Optional[float] = None,
        params: Optional[Dict] = None
    ) -> Order:
        """Place an order on Binance"""
        # Determine market type
        if symbol in self.symbol_info and self.symbol_info[symbol]['type'] == 'spot':
            return await self.spot_trading.place_order(
                symbol, side, order_type, size, price, params
            )
        elif symbol.endswith('_PERP') or (params and params.get('futures', False)):
            return await self.futures_trading.place_order(
                symbol, side, order_type, size, price, params
            )
        else:
            # Default to spot
            return await self.spot_trading.place_order(
                symbol, side, order_type, size, price, params
            )
    
    async def cancel_order(self, order_id: str, symbol: Optional[str] = None) -> bool:
        """Cancel an order"""
        if not symbol:
            raise ValueError("Symbol is required for Binance order cancellation")
        
        if symbol in self.symbol_info and self.symbol_info[symbol]['type'] == 'spot':
            return await self.spot_trading.cancel_order(order_id, symbol)
        else:
            return await self.futures_trading.cancel_order(order_id, symbol)
    
    async def get_order(self, order_id: str, symbol: Optional[str] = None) -> Order:
        """Get order details"""
        if not symbol:
            raise ValueError("Symbol is required for Binance order query")
        
        if symbol in self.symbol_info and self.symbol_info[symbol]['type'] == 'spot':
            return await self.spot_trading.get_order(order_id, symbol)
        else:
            return await self.futures_trading.get_order(order_id, symbol)
    
    async def get_open_orders(self, symbol: Optional[str] = None) -> List[Order]:
        """Get all open orders"""
        orders = []
        
        # Get spot orders
        spot_orders = await self.spot_trading.get_open_orders(symbol)
        orders.extend(spot_orders)
        
        # Get futures orders
        futures_orders = await self.futures_trading.get_open_orders(symbol)
        orders.extend(futures_orders)
        
        return orders
    
    async def get_order_history(
        self,
        symbol: Optional[str] = None,
        limit: int = 100,
        start_time: Optional[datetime] = None
    ) -> List[Order]:
        """Get order history"""
        if symbol and symbol in self.symbol_info:
            if self.symbol_info[symbol]['type'] == 'spot':
                return await self.spot_trading.get_order_history(symbol, limit, start_time)
            else:
                return await self.futures_trading.get_order_history(symbol, limit, start_time)
        else:
            # Get from both
            orders = []
            
            spot_orders = await self.spot_trading.get_order_history(symbol, limit, start_time)
            futures_orders = await self.futures_trading.get_order_history(symbol, limit, start_time)
            
            orders.extend(spot_orders)
            orders.extend(futures_orders)
            
            # Sort by timestamp
            orders.sort(key=lambda x: x.created_at, reverse=True)
            
            return orders[:limit]
    
    # Market data methods
    async def get_ticker(self, symbol: str) -> Ticker:
        """Get ticker for a symbol"""
        endpoint = "/api/v3/ticker/24hr"
        params = {'symbol': self._normalize_symbol(symbol)}
        
        data = await self._get(endpoint, params=params)
        
        return Ticker(
            symbol=symbol,
            bid=float(data['bidPrice']),
            ask=float(data['askPrice']),
            last=float(data['lastPrice']),
            volume_24h=float(data['volume']),
            quote_volume_24h=float(data['quoteVolume']),
            high_24h=float(data['highPrice']),
            low_24h=float(data['lowPrice']),
            change_24h=float(data['priceChange']),
            change_percent_24h=float(data['priceChangePercent']),
            timestamp=datetime.now(timezone.utc)
        )
    
    async def get_all_tickers(self) -> Dict[str, Ticker]:
        """Get all tickers"""
        endpoint = "/api/v3/ticker/24hr"
        
        data = await self._get(endpoint)
        
        tickers = {}
        for ticker_data in data:
            symbol = self._denormalize_symbol(ticker_data['symbol'])
            tickers[symbol] = Ticker(
                symbol=symbol,
                bid=float(ticker_data['bidPrice']),
                ask=float(ticker_data['askPrice']),
                last=float(ticker_data['lastPrice']),
                volume_24h=float(ticker_data['volume']),
                quote_volume_24h=float(ticker_data['quoteVolume']),
                high_24h=float(ticker_data['highPrice']),
                low_24h=float(ticker_data['lowPrice']),
                change_24h=float(ticker_data['priceChange']),
                change_percent_24h=float(ticker_data['priceChangePercent']),
                timestamp=datetime.now(timezone.utc)
            )
        
        return tickers
    
    async def get_order_book(self, symbol: str, limit: int = 20) -> OrderBook:
        """Get order book"""
        endpoint = "/api/v3/depth"
        params = {
            'symbol': self._normalize_symbol(symbol),
            'limit': limit
        }
        
        data = await self._get(endpoint, params=params)
        
        return OrderBook(
            symbol=symbol,
            bids=[(float(price), float(size)) for price, size in data['bids']],
            asks=[(float(price), float(size)) for price, size in data['asks']],
            timestamp=datetime.now(timezone.utc),
            sequence=data.get('lastUpdateId')
        )
    
    async def get_candles(
        self,
        symbol: str,
        timeframe: str,
        limit: int = 100,
        start_time: Optional[datetime] = None
    ) -> List[Candle]:
        """Get candlestick data"""
        endpoint = "/api/v3/klines"
        params = {
            'symbol': self._normalize_symbol(symbol),
            'interval': self._convert_timeframe(timeframe),
            'limit': limit
        }
        
        if start_time:
            params['startTime'] = int(start_time.timestamp() * 1000)
        
        data = await self._get(endpoint, params=params)
        
        candles = []
        for candle_data in data:
            candles.append(Candle(
                symbol=symbol,
                timeframe=timeframe,
                open_time=datetime.fromtimestamp(candle_data[0] / 1000, tz=timezone.utc),
                close_time=datetime.fromtimestamp(candle_data[6] / 1000, tz=timezone.utc),
                open=float(candle_data[1]),
                high=float(candle_data[2]),
                low=float(candle_data[3]),
                close=float(candle_data[4]),
                volume=float(candle_data[5]),
                quote_volume=float(candle_data[7]),
                trades=int(candle_data[8])
            ))
        
        return candles
    
    async def get_trades(self, symbol: str, limit: int = 100) -> List[Trade]:
        """Get recent trades"""
        endpoint = "/api/v3/trades"
        params = {
            'symbol': self._normalize_symbol(symbol),
            'limit': limit
        }
        
        data = await self._get(endpoint, params=params)
        
        trades = []
        for trade_data in data:
            trades.append(Trade(
                trade_id=str(trade_data['id']),
                order_id="",  # Not provided in public trades
                symbol=symbol,
                side=OrderSide.BUY if trade_data['isBuyerMaker'] else OrderSide.SELL,
                price=float(trade_data['price']),
                size=float(trade_data['qty']),
                fee=0.0,  # Not provided in public trades
                fee_currency="",
                timestamp=datetime.fromtimestamp(trade_data['time'] / 1000, tz=timezone.utc),
                is_maker=trade_data['isBuyerMaker']
            ))
        
        return trades
    
    # Account methods
    async def get_balance(self) -> Dict[str, Balance]:
        """Get account balance"""
        balances = {}
        
        # Get spot balances
        spot_balances = await self.spot_trading.get_balance()
        balances.update(spot_balances)
        
        # Get futures balances
        futures_balances = await self.futures_trading.get_balance()
        balances.update(futures_balances)
        
        return balances
    
    async def get_open_positions(self) -> List[Position]:
        """Get open positions (futures only)"""
        return await self.futures_trading.get_open_positions()
    
    async def get_position(self, symbol: str) -> Optional[Position]:
        """Get specific position"""
        return await self.futures_trading.get_position(symbol)
    
    # WebSocket methods
    async def _connect_websocket(self):
        """Connect to Binance WebSocket"""
        await self.ws_client.connect(
            on_message=self.ws_on_message,
            on_error=self.ws_on_error,
            on_close=self.ws_on_close
        )
        
        # Subscribe to user data stream if we have a listen key
        if self.listen_key:
            await self.ws_client.subscribe_user_stream(self.listen_key)
    
    async def subscribe_ticker(self, symbol: str):
        """Subscribe to ticker updates"""
        await self.ws_client.subscribe_ticker(symbol)
    
    async def subscribe_orderbook(self, symbol: str, depth: int = 20):
        """Subscribe to order book updates"""
        await self.ws_client.subscribe_orderbook(symbol, depth)
    
    async def subscribe_trades(self, symbol: str):
        """Subscribe to trade updates"""
        await self.ws_client.subscribe_trades(symbol)
    
    async def subscribe_user_orders(self):
        """Subscribe to user order updates"""
        if self.listen_key:
            # Already subscribed via user data stream
            pass
        else:
            logger.warning("Cannot subscribe to user orders without authentication")
    
    async def subscribe_user_positions(self):
        """Subscribe to user position updates"""
        if self.listen_key:
            # Already subscribed via user data stream
            pass
        else:
            logger.warning("Cannot subscribe to user positions without authentication")
    
    # Helper methods
    def _convert_timeframe(self, timeframe: str) -> str:
        """Convert standard timeframe to Binance format"""
        timeframe_map = {
            '1m': '1m',
            '3m': '3m',
            '5m': '5m',
            '15m': '15m',
            '30m': '30m',
            '1h': '1h',
            '2h': '2h',
            '4h': '4h',
            '6h': '6h',
            '8h': '8h',
            '12h': '12h',
            '1d': '1d',
            '3d': '3d',
            '1w': '1w',
            '1M': '1M'
        }
        return timeframe_map.get(timeframe, timeframe)
    
    def _convert_order_type(self, order_type: OrderType) -> str:
        """Convert OrderType to Binance format"""
        type_map = {
            OrderType.MARKET: 'MARKET',
            OrderType.LIMIT: 'LIMIT',
            OrderType.STOP: 'STOP_LOSS',
            OrderType.STOP_LIMIT: 'STOP_LOSS_LIMIT',
            OrderType.POST_ONLY: 'LIMIT_MAKER'
        }
        return type_map.get(order_type, 'LIMIT')
    
    def _convert_time_in_force(self, tif: TimeInForce) -> str:
        """Convert TimeInForce to Binance format"""
        tif_map = {
            TimeInForce.GTC: 'GTC',
            TimeInForce.IOC: 'IOC',
            TimeInForce.FOK: 'FOK',
            TimeInForce.GTX: 'GTX'
        }
        return tif_map.get(tif, 'GTC')
    
    async def shutdown(self):
        """Shutdown Binance client"""
        # Cancel listen key task
        if self.listen_key_task:
            self.listen_key_task.cancel()
        
        # Delete listen key
        if self.listen_key:
            try:
                await self._delete(
                    "/api/v3/userDataStream",
                    params={'listenKey': self.listen_key},
                    signed=True
                )
            except Exception:
                pass
        
        # Call parent shutdown
        await super().shutdown()