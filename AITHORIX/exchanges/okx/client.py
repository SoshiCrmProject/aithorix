"""
AITHORIX OKX Client Implementation
Exchange known for unified trading and copy trading features
"""

import asyncio
import logging
from datetime import datetime, timezone
from typing import Dict, List, Optional, Any, Callable
import time
import hmac
import hashlib
import base64
from urllib.parse import urlencode

from ..base_exchange import (
    BaseExchange, ExchangeConfig, Order, Trade, Position,
    Balance, Ticker, OrderBook, Candle, OrderType, OrderSide,
    OrderStatus, TimeInForce, PositionSide
)
from .unified.unified_trading import OKXUnifiedTrading
from .websocket.ws_client import OKXWebSocketClient
from .auth.authenticator import OKXAuthenticator

logger = logging.getLogger(__name__)


class OKXClient(BaseExchange):
    """
    OKX exchange client implementation
    Focus on unified trading and copy trading
    """
    
    def __init__(self, config: ExchangeConfig):
        super().__init__(config)
        
        # Set OKX-specific URLs
        if config.testnet:
            self.config.rest_url = "https://www.okx.com"  # OKX uses same URL for testnet with different headers
            self.config.ws_public_url = "wss://wspap.okx.com:8443/ws/v5/public?brokerId=9999"
            self.config.ws_private_url = "wss://wspap.okx.com:8443/ws/v5/private?brokerId=9999"
        else:
            self.config.rest_url = "https://www.okx.com"
            self.config.ws_public_url = "wss://ws.okx.com:8443/ws/v5/public"
            self.config.ws_private_url = "wss://ws.okx.com:8443/ws/v5/private"
        
        # Initialize components
        self.authenticator = OKXAuthenticator(config)
        self.unified_trading = OKXUnifiedTrading(self)
        self.ws_client = OKXWebSocketClient(self)
        
        # Market info
        self.instruments: Dict[str, Any] = {}
        self.symbol_info: Dict[str, Any] = {}
        
        # Account configuration
        self.account_level = config.params.get('account_level', 'Unified account')  # Simple, Single-currency margin, Multi-currency margin, Portfolio margin
        self.position_mode = config.params.get('position_mode', 'net_mode')  # net_mode, long_short_mode
        
        logger.info("Initialized OKX client")
    
    async def _initialize_exchange(self):
        """OKX-specific initialization"""
        # Load instruments
        await self._load_instruments()
        
        # Check account configuration
        await self._check_account_config()
    
    async def _load_markets(self):
        """Load OKX market information"""
        await self._load_instruments()
    
    async def _load_instruments(self):
        """Load all tradeable instruments"""
        try:
            # Get all instrument types
            inst_types = ['SPOT', 'SWAP', 'FUTURES', 'OPTION']
            
            for inst_type in inst_types:
                response = await self._get(
                    "/api/v5/public/instruments",
                    params={'instType': inst_type}
                )
                
                if response['code'] == '0':
                    for inst in response['data']:
                        symbol = inst['instId']
                        
                        self.instruments[symbol] = {
                            'instType': inst['instType'],
                            'instId': inst['instId'],
                            'uly': inst.get('uly'),  # Underlying
                            'baseCcy': inst.get('baseCcy'),
                            'quoteCcy': inst.get('quoteCcy'),
                            'settleCcy': inst.get('settleCcy'),
                            'ctVal': float(inst.get('ctVal', 1)),  # Contract value
                            'ctMult': float(inst.get('ctMult', 1)),  # Contract multiplier
                            'ctValCcy': inst.get('ctValCcy'),  # Contract value currency
                            'tickSz': float(inst['tickSz']),
                            'lotSz': float(inst['lotSz']),
                            'minSz': float(inst['minSz']),
                            'lever': inst.get('lever'),
                            'state': inst['state'],
                            'listTime': inst.get('listTime'),
                            'expTime': inst.get('expTime'),
                            'optType': inst.get('optType'),  # Option type
                            'stk': inst.get('stk'),  # Strike price
                            'ctType': inst.get('ctType')  # Contract type: linear, inverse
                        }
                        
                        self.symbol_info[symbol] = self.instruments[symbol]
            
            logger.info(f"Loaded {len(self.instruments)} instruments from OKX")
            
        except Exception as e:
            logger.error(f"Failed to load instruments: {str(e)}")
            raise
    
    async def _check_account_config(self):
        """Check account configuration"""
        try:
            # Get account configuration
            response = await self._get("/api/v5/account/config", signed=True)
            
            if response['code'] == '0' and response['data']:
                config_data = response['data'][0]
                self.account_level = config_data.get('acctLv', 'Simple')
                self.position_mode = config_data.get('posMode', 'net_mode')
                
                logger.info(f"OKX account level: {self.account_level}, position mode: {self.position_mode}")
                
        except Exception as e:
            logger.error(f"Failed to check account config: {str(e)}")
    
    async def _check_connection(self):
        """Check OKX connection"""
        # Get server time
        response = await self._get("/api/v5/public/time")
        
        if response['code'] == '0':
            server_time = int(response['data'][0]['ts'])
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
        """Generate OKX authentication headers"""
        headers = await self.authenticator.get_auth_headers(method, endpoint, params, data)
        
        # Add testnet flag if needed
        if self.config.testnet:
            headers['x-simulated-trading'] = '1'
        
        return headers
    
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
        """Place an order on OKX"""
        return await self.unified_trading.place_order(
            symbol, side, order_type, size, price, params
        )
    
    async def cancel_order(self, order_id: str, symbol: Optional[str] = None) -> bool:
        """Cancel an order"""
        return await self.unified_trading.cancel_order(order_id, symbol)
    
    async def get_order(self, order_id: str, symbol: Optional[str] = None) -> Optional[Order]:
        """Get order details"""
        return await self.unified_trading.get_order(order_id, symbol)
    
    async def get_open_orders(self, symbol: Optional[str] = None) -> List[Order]:
        """Get open orders"""
        return await self.unified_trading.get_open_orders(symbol)
    
    async def get_order_history(
        self,
        symbol: Optional[str] = None,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        limit: int = 100
    ) -> List[Order]:
        """Get order history"""
        return await self.unified_trading.get_order_history(
            symbol, start_time, end_time, limit
        )
    
    # Market data methods
    async def get_ticker(self, symbol: str) -> Ticker:
        """Get ticker for symbol"""
        try:
            response = await self._get(
                "/api/v5/market/ticker",
                params={'instId': symbol}
            )
            
            if response['code'] != '0':
                raise Exception(f"Failed to get ticker: {response['msg']}")
            
            ticker_data = response['data'][0]
            
            return Ticker(
                symbol=symbol,
                bid=float(ticker_data['bidPx']) if ticker_data['bidPx'] else 0,
                ask=float(ticker_data['askPx']) if ticker_data['askPx'] else 0,
                bid_size=float(ticker_data['bidSz']) if ticker_data['bidSz'] else 0,
                ask_size=float(ticker_data['askSz']) if ticker_data['askSz'] else 0,
                last=float(ticker_data['last']),
                volume_24h=float(ticker_data['vol24h']) if ticker_data['vol24h'] else 0,
                quote_volume_24h=float(ticker_data['volCcy24h']) if ticker_data['volCcy24h'] else 0,
                open_24h=float(ticker_data['open24h']) if ticker_data['open24h'] else 0,
                high_24h=float(ticker_data['high24h']) if ticker_data['high24h'] else 0,
                low_24h=float(ticker_data['low24h']) if ticker_data['low24h'] else 0,
                change_24h=float(ticker_data['last']) - float(ticker_data['open24h']) if ticker_data['open24h'] else 0,
                change_percent_24h=(float(ticker_data['last']) / float(ticker_data['open24h']) - 1) * 100 if ticker_data['open24h'] and float(ticker_data['open24h']) > 0 else 0,
                timestamp=datetime.fromtimestamp(int(ticker_data['ts']) / 1000, tz=timezone.utc)
            )
            
        except Exception as e:
            logger.error(f"Error getting ticker: {str(e)}")
            raise
    
    async def get_orderbook(self, symbol: str, depth: int = 50) -> OrderBook:
        """Get order book"""
        try:
            response = await self._get(
                "/api/v5/market/books",
                params={'instId': symbol, 'sz': str(depth)}
            )
            
            if response['code'] != '0':
                raise Exception(f"Failed to get orderbook: {response['msg']}")
            
            book_data = response['data'][0]
            
            return OrderBook(
                symbol=symbol,
                bids=[[float(p), float(s), int(c), int(o)] for p, s, c, o in book_data['bids']],
                asks=[[float(p), float(s), int(c), int(o)] for p, s, c, o in book_data['asks']],
                timestamp=datetime.fromtimestamp(int(book_data['ts']) / 1000, tz=timezone.utc)
            )
            
        except Exception as e:
            logger.error(f"Error getting orderbook: {str(e)}")
            raise
    
    async def get_trades(self, symbol: str, limit: int = 100) -> List[Trade]:
        """Get recent trades"""
        try:
            response = await self._get(
                "/api/v5/market/trades",
                params={'instId': symbol, 'limit': str(limit)}
            )
            
            if response['code'] != '0':
                raise Exception(f"Failed to get trades: {response['msg']}")
            
            trades = []
            for trade_data in response['data']:
                trades.append(Trade(
                    id=trade_data['tradeId'],
                    order_id=None,
                    symbol=symbol,
                    side=OrderSide.BUY if trade_data['side'] == 'buy' else OrderSide.SELL,
                    price=float(trade_data['px']),
                    size=float(trade_data['sz']),
                    fee=0,
                    fee_currency=None,
                    timestamp=datetime.fromtimestamp(int(trade_data['ts']) / 1000, tz=timezone.utc),
                    is_maker=False
                ))
            
            return trades
            
        except Exception as e:
            logger.error(f"Error getting trades: {str(e)}")
            raise
    
    async def get_candles(
        self,
        symbol: str,
        timeframe: str,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        limit: int = 100
    ) -> List[Candle]:
        """Get historical candles"""
        try:
            params = {
                'instId': symbol,
                'bar': self._convert_timeframe(timeframe),
                'limit': str(limit)
            }
            
            if end_time:
                params['after'] = str(int(end_time.timestamp() * 1000))
            if start_time:
                params['before'] = str(int(start_time.timestamp() * 1000))
            
            response = await self._get("/api/v5/market/candles", params=params)
            
            if response['code'] != '0':
                raise Exception(f"Failed to get candles: {response['msg']}")
            
            candles = []
            for candle_data in response['data']:
                candles.append(Candle(
                    timestamp=datetime.fromtimestamp(int(candle_data[0]) / 1000, tz=timezone.utc),
                    open=float(candle_data[1]),
                    high=float(candle_data[2]),
                    low=float(candle_data[3]),
                    close=float(candle_data[4]),
                    volume=float(candle_data[5]),
                    trades=0  # Not provided
                ))
            
            return sorted(candles, key=lambda x: x.timestamp)
            
        except Exception as e:
            logger.error(f"Error getting candles: {str(e)}")
            raise
    
    # Account methods
    async def get_balance(self) -> Dict[str, Balance]:
        """Get account balance"""
        return await self.unified_trading.get_balance()
    
    async def get_open_positions(self) -> List[Position]:
        """Get open positions"""
        return await self.unified_trading.get_open_positions()
    
    async def get_position(self, symbol: str) -> Optional[Position]:
        """Get specific position"""
        return await self.unified_trading.get_position(symbol)
    
    # WebSocket methods
    async def _connect_websocket(self):
        """Connect to OKX WebSocket"""
        await self.ws_client.connect(
            on_message=self.ws_on_message,
            on_error=self.ws_on_error,
            on_close=self.ws_on_close
        )
    
    async def subscribe_ticker(self, symbol: str):
        """Subscribe to ticker updates"""
        await self.ws_client.subscribe_ticker(symbol)
    
    async def subscribe_orderbook(self, symbol: str, depth: str = "books5"):
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
    
    # OKX specific methods
    async def get_funding_rate(self, symbol: str) -> Dict[str, Any]:
        """Get funding rate for perpetual swaps"""
        try:
            response = await self._get(
                "/api/v5/public/funding-rate",
                params={'instId': symbol}
            )
            
            if response['code'] == '0' and response['data']:
                data = response['data'][0]
                return {
                    'symbol': data['instId'],
                    'fundingRate': float(data['fundingRate']),
                    'fundingTime': datetime.fromtimestamp(int(data['fundingTime']) / 1000, tz=timezone.utc),
                    'nextFundingRate': float(data['nextFundingRate']) if data.get('nextFundingRate') else None,
                    'nextFundingTime': datetime.fromtimestamp(int(data['nextFundingTime']) / 1000, tz=timezone.utc) if data.get('nextFundingTime') else None
                }
            
            return {}
            
        except Exception as e:
            logger.error(f"Error getting funding rate: {str(e)}")
            return {}
    
    async def get_mark_price(self, symbol: str) -> float:
        """Get mark price"""
        try:
            inst_type = self.instruments.get(symbol, {}).get('instType', 'SWAP')
            
            response = await self._get(
                "/api/v5/public/mark-price",
                params={'instId': symbol, 'instType': inst_type}
            )
            
            if response['code'] == '0' and response['data']:
                return float(response['data'][0]['markPx'])
            
            return 0.0
            
        except Exception as e:
            logger.error(f"Error getting mark price: {str(e)}")
            return 0.0
    
    async def set_leverage(self, symbol: str, leverage: int, margin_mode: str = "cross") -> bool:
        """Set leverage for a symbol"""
        return await self.unified_trading.set_leverage(symbol, leverage, margin_mode)
    
    async def get_max_order_size(self, symbol: str, price: Optional[float] = None) -> Dict[str, float]:
        """Get maximum order size"""
        return await self.unified_trading.get_max_order_size(symbol, price)
    
    # Helper methods
    def _convert_timeframe(self, timeframe: str) -> str:
        """Convert standard timeframe to OKX format"""
        timeframe_map = {
            '1m': '1m',
            '3m': '3m',
            '5m': '5m',
            '15m': '15m',
            '30m': '30m',
            '1h': '1H',
            '2h': '2H',
            '4h': '4H',
            '6h': '6H',
            '12h': '12H',
            '1d': '1D',
            '2d': '2D',
            '3d': '3D',
            '1w': '1W',
            '1M': '1M',
            '3M': '3M'
        }
        return timeframe_map.get(timeframe, '1H')
    
    def _get_inst_type(self, symbol: str) -> str:
        """Get instrument type for symbol"""
        if symbol in self.instruments:
            return self.instruments[symbol]['instType']
        
        # Guess based on symbol pattern
        if '-SWAP' in symbol:
            return 'SWAP'
        elif '-' in symbol and symbol.count('-') >= 2:
            # Could be futures or options
            parts = symbol.split('-')
            if parts[-1].startswith('C') or parts[-1].startswith('P'):
                return 'OPTION'
            else:
                return 'FUTURES'
        else:
            return 'SPOT'