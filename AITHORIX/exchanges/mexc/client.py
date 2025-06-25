"""
AITHORIX MEXC Client Implementation
Exchange specializing in altcoins and new listings
"""

import asyncio
import logging
from datetime import datetime, timezone
from typing import Dict, List, Optional, Any, Callable
import time
import hmac
import hashlib
from urllib.parse import urlencode

from ..base_exchange import (
    BaseExchange, ExchangeConfig, Order, Trade, Position,
    Balance, Ticker, OrderBook, Candle, OrderType, OrderSide,
    OrderStatus, TimeInForce, PositionSide
)
from .spot.spot_trading import MEXCSpotTrading
from .futures.futures_trading import MEXCFuturesTrading
from .websocket.ws_client import MEXCWebSocketClient
from .auth.authenticator import MEXCAuthenticator

logger = logging.getLogger(__name__)


class MEXCClient(BaseExchange):
    """
    MEXC exchange client implementation
    Focus on altcoins and new token listings
    """
    
    def __init__(self, config: ExchangeConfig):
        super().__init__(config)
        
        # Set MEXC-specific URLs
        if config.testnet:
            self.config.rest_url = "https://sandbox-api.mexc.com"
            self.config.ws_url = "wss://sandbox-ws.mexc.com/ws"
        else:
            self.config.rest_url = "https://api.mexc.com"
            self.config.ws_url = "wss://wbs.mexc.com/ws"
        
        # Initialize components
        self.authenticator = MEXCAuthenticator(config)
        self.spot_trading = MEXCSpotTrading(self)
        self.futures_trading = MEXCFuturesTrading(self)
        self.ws_client = MEXCWebSocketClient(self)
        
        # Market info
        self.symbol_info: Dict[str, Any] = {}
        self.new_listings: List[Dict[str, Any]] = []
        
        logger.info("Initialized MEXC client")
    
    async def _initialize_exchange(self):
        """MEXC-specific initialization"""
        # Load market info
        await self._load_market_info()
        
        # Check for new listings
        await self._check_new_listings()
    
    async def _load_markets(self):
        """Load MEXC market information"""
        await self._load_market_info()
    
    async def _load_market_info(self):
        """Load market and symbol information"""
        try:
            # Get spot symbols
            spot_response = await self._get("/api/v3/exchangeInfo")
            
            # Get futures symbols
            futures_response = await self._get("/contract/v1/detail")
            
            # Process spot symbols
            for symbol_data in spot_response.get('symbols', []):
                if symbol_data['status'] == 'ENABLED':
                    symbol = symbol_data['symbol']
                    self.symbol_info[symbol] = {
                        'type': 'spot',
                        'base': symbol_data['baseAsset'],
                        'quote': symbol_data['quoteAsset'],
                        'status': symbol_data['status'],
                        'baseAssetPrecision': symbol_data['baseAssetPrecision'],
                        'quoteAssetPrecision': symbol_data['quoteAssetPrecision'],
                        'isSpotTradingAllowed': symbol_data.get('isSpotTradingAllowed', True),
                        'permissions': symbol_data.get('permissions', [])
                    }
            
            # Process futures symbols
            if futures_response.get('success'):
                for contract in futures_response.get('data', []):
                    symbol = contract['symbol']
                    self.symbol_info[f"{symbol}_PERP"] = {
                        'type': 'futures',
                        'base': contract.get('baseCoin'),
                        'quote': contract.get('quoteCoin'),
                        'status': 'ENABLED' if contract.get('state') == 1 else 'DISABLED',
                        'contractSize': contract.get('contractSize'),
                        'pricePrecision': contract.get('pricePrecision'),
                        'volPrecision': contract.get('volPrecision')
                    }
            
            logger.info(f"Loaded {len(self.symbol_info)} symbols from MEXC")
            
        except Exception as e:
            logger.error(f"Failed to load market info: {str(e)}")
            raise
    
    async def _check_new_listings(self):
        """Check for new token listings"""
        try:
            # MEXC provides new listing information through announcements
            # In production, would parse announcement API or websocket
            logger.info("Checking for new MEXC listings")
            
            # Store recently listed tokens (last 7 days)
            # This would be populated from announcement parsing
            self.new_listings = []
            
        except Exception as e:
            logger.error(f"Failed to check new listings: {str(e)}")
    
    async def _check_connection(self):
        """Check MEXC connection"""
        # Ping endpoint
        response = await self._get("/api/v3/ping")
        
        # Get server time
        time_response = await self._get("/api/v3/time")
        server_time = time_response['serverTime']
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
        """Generate MEXC authentication headers"""
        return await self.authenticator.get_auth_headers(method, endpoint, params, data)
    
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
        """Place an order on MEXC"""
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
            # Default to spot for MEXC (altcoin focus)
            return await self.spot_trading.place_order(
                symbol, side, order_type, size, price, params
            )
    
    async def cancel_order(self, order_id: str, symbol: Optional[str] = None) -> bool:
        """Cancel an order"""
        if not symbol:
            raise ValueError("Symbol is required for MEXC order cancellation")
        
        if symbol in self.symbol_info and self.symbol_info[symbol]['type'] == 'spot':
            return await self.spot_trading.cancel_order(order_id, symbol)
        else:
            return await self.futures_trading.cancel_order(order_id, symbol)
    
    async def get_order(self, order_id: str, symbol: Optional[str] = None) -> Order:
        """Get order details"""
        if not symbol:
            raise ValueError("Symbol is required for MEXC order query")
        
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
        
        # Get futures orders if no specific symbol or it's a futures symbol
        if not symbol or symbol.endswith('_PERP'):
            futures_orders = await self.futures_trading.get_open_orders(
                symbol.replace('_PERP', '') if symbol else None
            )
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
            # Get from both markets
            orders = []
            
            spot_orders = await self.spot_trading.get_order_history(None, limit // 2, start_time)
            futures_orders = await self.futures_trading.get_order_history(None, limit // 2, start_time)
            
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
            bid=float(data.get('bidPrice', 0)),
            ask=float(data.get('askPrice', 0)),
            last=float(data.get('lastPrice', 0)),
            volume_24h=float(data.get('volume', 0)),
            quote_volume_24h=float(data.get('quoteVolume', 0)),
            high_24h=float(data.get('highPrice', 0)),
            low_24h=float(data.get('lowPrice', 0)),
            change_24h=float(data.get('priceChange', 0)),
            change_percent_24h=float(data.get('priceChangePercent', 0)),
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
                bid=float(ticker_data.get('bidPrice', 0)),
                ask=float(ticker_data.get('askPrice', 0)),
                last=float(ticker_data.get('lastPrice', 0)),
                volume_24h=float(ticker_data.get('volume', 0)),
                quote_volume_24h=float(ticker_data.get('quoteVolume', 0)),
                high_24h=float(ticker_data.get('highPrice', 0)),
                low_24h=float(ticker_data.get('lowPrice', 0)),
                change_24h=float(ticker_data.get('priceChange', 0)),
                change_percent_24h=float(ticker_data.get('priceChangePercent', 0)),
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
            bids=[(float(price), float(size)) for price, size in data.get('bids', [])],
            asks=[(float(price), float(size)) for price, size in data.get('asks', [])],
            timestamp=datetime.now(timezone.utc)
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
                trade_id=str(trade_data.get('id', '')),
                order_id="",  # Not provided in public trades
                symbol=symbol,
                side=OrderSide.BUY if trade_data.get('isBuyerMaker') else OrderSide.SELL,
                price=float(trade_data.get('price', 0)),
                size=float(trade_data.get('qty', 0)),
                fee=0.0,  # Not provided in public trades
                fee_currency="",
                timestamp=datetime.fromtimestamp(trade_data.get('time', 0) / 1000, tz=timezone.utc),
                is_maker=trade_data.get('isBuyerMaker', False)
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
    
    # MEXC specific methods
    async def get_new_listings(self, days: int = 7) -> List[Dict[str, Any]]:
        """Get recently listed tokens"""
        # In production, this would parse announcement data
        # or use a dedicated new listings API
        return self.new_listings
    
    async def get_hot_tokens(self, limit: int = 20) -> List[str]:
        """Get trending/hot tokens on MEXC"""
        try:
            # Get all tickers
            tickers = await self.get_all_tickers()
            
            # Sort by volume and price change
            hot_tokens = sorted(
                tickers.items(),
                key=lambda x: x[1].quote_volume_24h * abs(x[1].change_percent_24h),
                reverse=True
            )
            
            return [symbol for symbol, _ in hot_tokens[:limit]]
            
        except Exception as e:
            logger.error(f"Failed to get hot tokens: {str(e)}")
            return []
    
    # WebSocket methods
    async def _connect_websocket(self):
        """Connect to MEXC WebSocket"""
        await self.ws_client.connect(
            on_message=self.ws_on_message,
            on_error=self.ws_on_error,
            on_close=self.ws_on_close
        )
    
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
        await self.ws_client.subscribe_user_data()
    
    async def subscribe_user_positions(self):
        """Subscribe to user position updates"""
        await self.ws_client.subscribe_user_data()
    
    # Helper methods
    def _convert_timeframe(self, timeframe: str) -> str:
        """Convert standard timeframe to MEXC format"""
        timeframe_map = {
            '1m': '1m',
            '5m': '5m',
            '15m': '15m',
            '30m': '30m',
            '1h': '1h',
            '4h': '4h',
            '1d': '1d',
            '1w': '1w',
            '1M': '1M'
        }
        return timeframe_map.get(timeframe, timeframe)