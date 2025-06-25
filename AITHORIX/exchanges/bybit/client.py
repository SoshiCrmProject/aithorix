"""
AITHORIX Bybit Client Implementation
Exchange known for derivatives trading and copy trading
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
from .spot.spot_trading import BybitSpotTrading
from .derivatives.derivatives_trading import BybitDerivativesTrading
from .websocket.ws_client import BybitWebSocketClient
from .auth.authenticator import BybitAuthenticator

logger = logging.getLogger(__name__)


class BybitClient(BaseExchange):
    """
    Bybit exchange client implementation
    Focus on derivatives and professional trading
    """
    
    def __init__(self, config: ExchangeConfig):
        super().__init__(config)
        
        # Set Bybit-specific URLs
        if config.testnet:
            self.config.rest_url = "https://api-testnet.bybit.com"
            self.config.ws_url = "wss://stream-testnet.bybit.com/v5/public/spot"
            self.config.ws_private_url = "wss://stream-testnet.bybit.com/v5/private"
        else:
            self.config.rest_url = "https://api.bybit.com"
            self.config.ws_url = "wss://stream.bybit.com/v5/public/spot"
            self.config.ws_private_url = "wss://stream.bybit.com/v5/private"
        
        # Initialize components
        self.authenticator = BybitAuthenticator(config)
        self.spot_trading = BybitSpotTrading(self)
        self.derivatives_trading = BybitDerivativesTrading(self)
        self.ws_client = BybitWebSocketClient(self)
        
        # Market info
        self.symbol_info: Dict[str, Any] = {}
        self.instruments: Dict[str, Any] = {}
        
        # Account type
        self.account_type = config.params.get('account_type', 'UNIFIED')  # UNIFIED, CONTRACT
        
        logger.info("Initialized Bybit client")
    
    async def _initialize_exchange(self):
        """Bybit-specific initialization"""
        # Load market info
        await self._load_instruments()
        
        # Check account info
        await self._check_account_info()
    
    async def _load_markets(self):
        """Load Bybit market information"""
        await self._load_instruments()
    
    async def _load_instruments(self):
        """Load all tradeable instruments"""
        try:
            # Load spot instruments
            spot_response = await self._get("/v5/market/instruments-info", 
                                          params={'category': 'spot'})
            
            if spot_response['retCode'] == 0:
                for inst in spot_response['result']['list']:
                    symbol = inst['symbol']
                    self.instruments[symbol] = {
                        'type': 'spot',
                        'base': inst['baseCoin'],
                        'quote': inst['quoteCoin'],
                        'status': inst['status'],
                        'minOrderQty': float(inst['lotSizeFilter']['minOrderQty']),
                        'maxOrderQty': float(inst['lotSizeFilter']['maxOrderQty']),
                        'qtyStep': float(inst['lotSizeFilter']['qtyStep']),
                        'minOrderAmt': float(inst['lotSizeFilter'].get('minOrderAmt', 0)),
                        'tickSize': float(inst['priceFilter']['tickSize'])
                    }
                    self.symbol_info[symbol] = self.instruments[symbol]
            
            # Load linear perpetuals
            linear_response = await self._get("/v5/market/instruments-info", 
                                            params={'category': 'linear'})
            
            if linear_response['retCode'] == 0:
                for inst in linear_response['result']['list']:
                    symbol = inst['symbol']
                    self.instruments[symbol] = {
                        'type': 'linear',
                        'base': inst['baseCoin'],
                        'quote': inst['quoteCoin'],
                        'settleCoin': inst['settleCoin'],
                        'status': inst['status'],
                        'contractType': inst['contractType'],
                        'minOrderQty': float(inst['lotSizeFilter']['minOrderQty']),
                        'maxOrderQty': float(inst['lotSizeFilter']['maxOrderQty']),
                        'qtyStep': float(inst['lotSizeFilter']['qtyStep']),
                        'tickSize': float(inst['priceFilter']['tickSize']),
                        'minLeverage': float(inst['leverageFilter']['minLeverage']),
                        'maxLeverage': float(inst['leverageFilter']['maxLeverage'])
                    }
                    self.symbol_info[symbol] = self.instruments[symbol]
            
            # Load inverse perpetuals
            inverse_response = await self._get("/v5/market/instruments-info", 
                                             params={'category': 'inverse'})
            
            if inverse_response['retCode'] == 0:
                for inst in inverse_response['result']['list']:
                    symbol = inst['symbol']
                    self.instruments[symbol] = {
                        'type': 'inverse',
                        'base': inst['baseCoin'],
                        'quote': inst['quoteCoin'],
                        'settleCoin': inst['settleCoin'],
                        'status': inst['status'],
                        'contractType': inst['contractType'],
                        'minOrderQty': float(inst['lotSizeFilter']['minOrderQty']),
                        'maxOrderQty': float(inst['lotSizeFilter']['maxOrderQty']),
                        'qtyStep': float(inst['lotSizeFilter']['qtyStep']),
                        'tickSize': float(inst['priceFilter']['tickSize']),
                        'minLeverage': float(inst['leverageFilter']['minLeverage']),
                        'maxLeverage': float(inst['leverageFilter']['maxLeverage'])
                    }
                    self.symbol_info[symbol] = self.instruments[symbol]
            
            # Load options if available
            try:
                options_response = await self._get("/v5/market/instruments-info", 
                                                 params={'category': 'option'})
                
                if options_response['retCode'] == 0:
                    for inst in options_response['result']['list']:
                        symbol = inst['symbol']
                        self.instruments[symbol] = {
                            'type': 'option',
                            'base': inst['baseCoin'],
                            'quote': inst['quoteCoin'],
                            'settleCoin': inst['settleCoin'],
                            'optionsType': inst['optionsType'],
                            'status': inst['status'],
                            'minOrderQty': float(inst['lotSizeFilter']['minOrderQty']),
                            'maxOrderQty': float(inst['lotSizeFilter']['maxOrderQty']),
                            'qtyStep': float(inst['lotSizeFilter']['qtyStep']),
                            'tickSize': float(inst['priceFilter']['tickSize'])
                        }
                        self.symbol_info[symbol] = self.instruments[symbol]
            except:
                logger.info("Options not available on this account")
            
            logger.info(f"Loaded {len(self.instruments)} instruments from Bybit")
            
        except Exception as e:
            logger.error(f"Failed to load instruments: {str(e)}")
            raise
    
    async def _check_account_info(self):
        """Check account configuration"""
        try:
            # Get account info
            response = await self._get("/v5/account/info", signed=True)
            
            if response['retCode'] == 0:
                account_info = response['result']
                self.account_type = account_info.get('unifiedMarginStatus', 0)
                
                logger.info(f"Bybit account type: {self.account_type}")
        except Exception as e:
            logger.error(f"Failed to check account info: {str(e)}")
    
    async def _check_connection(self):
        """Check Bybit connection"""
        # Get server time
        response = await self._get("/v5/market/time")
        
        if response['retCode'] == 0:
            server_time = int(response['result']['timeSecond']) * 1000
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
        """Generate Bybit authentication headers"""
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
        """Place an order on Bybit"""
        # Determine market type
        if symbol in self.instruments:
            inst_type = self.instruments[symbol]['type']
            
            if inst_type == 'spot':
                return await self.spot_trading.place_order(
                    symbol, side, order_type, size, price, params
                )
            else:  # linear, inverse, option
                return await self.derivatives_trading.place_order(
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
            raise ValueError("Symbol is required for Bybit order cancellation")
        
        if symbol in self.instruments:
            inst_type = self.instruments[symbol]['type']
            
            if inst_type == 'spot':
                return await self.spot_trading.cancel_order(order_id, symbol)
            else:
                return await self.derivatives_trading.cancel_order(order_id, symbol)
        else:
            return await self.spot_trading.cancel_order(order_id, symbol)
    
    async def get_order(self, order_id: str, symbol: Optional[str] = None) -> Optional[Order]:
        """Get order details"""
        if not symbol:
            raise ValueError("Symbol is required for Bybit order query")
        
        if symbol in self.instruments:
            inst_type = self.instruments[symbol]['type']
            
            if inst_type == 'spot':
                return await self.spot_trading.get_order(order_id, symbol)
            else:
                return await self.derivatives_trading.get_order(order_id, symbol)
        else:
            return await self.spot_trading.get_order(order_id, symbol)
    
    async def get_open_orders(self, symbol: Optional[str] = None) -> List[Order]:
        """Get open orders"""
        orders = []
        
        # Get spot orders
        spot_orders = await self.spot_trading.get_open_orders(symbol)
        orders.extend(spot_orders)
        
        # Get derivatives orders
        deriv_orders = await self.derivatives_trading.get_open_orders(symbol)
        orders.extend(deriv_orders)
        
        return orders
    
    async def get_order_history(
        self,
        symbol: Optional[str] = None,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        limit: int = 100
    ) -> List[Order]:
        """Get order history"""
        orders = []
        
        # Get spot history
        spot_orders = await self.spot_trading.get_order_history(
            symbol, start_time, end_time, limit
        )
        orders.extend(spot_orders)
        
        # Get derivatives history
        deriv_orders = await self.derivatives_trading.get_order_history(
            symbol, start_time, end_time, limit
        )
        orders.extend(deriv_orders)
        
        return orders
    
    # Market data methods
    async def get_ticker(self, symbol: str) -> Ticker:
        """Get ticker for symbol"""
        try:
            # Determine category
            category = self._get_category(symbol)
            
            response = await self._get(
                "/v5/market/tickers",
                params={'category': category, 'symbol': symbol}
            )
            
            if response['retCode'] != 0:
                raise Exception(f"Failed to get ticker: {response['retMsg']}")
            
            ticker_data = response['result']['list'][0]
            
            return Ticker(
                symbol=symbol,
                bid=float(ticker_data.get('bid1Price', 0)),
                ask=float(ticker_data.get('ask1Price', 0)),
                bid_size=float(ticker_data.get('bid1Size', 0)),
                ask_size=float(ticker_data.get('ask1Size', 0)),
                last=float(ticker_data.get('lastPrice', 0)),
                volume_24h=float(ticker_data.get('volume24h', 0)),
                quote_volume_24h=float(ticker_data.get('turnover24h', 0)),
                open_24h=float(ticker_data.get('prevPrice24h', 0)),
                high_24h=float(ticker_data.get('highPrice24h', 0)),
                low_24h=float(ticker_data.get('lowPrice24h', 0)),
                change_24h=float(ticker_data.get('price24hPcnt', 0)) * float(ticker_data.get('prevPrice24h', 0)),
                change_percent_24h=float(ticker_data.get('price24hPcnt', 0)) * 100,
                timestamp=datetime.now(timezone.utc)
            )
            
        except Exception as e:
            logger.error(f"Error getting ticker: {str(e)}")
            raise
    
    async def get_orderbook(self, symbol: str, depth: int = 50) -> OrderBook:
        """Get order book"""
        try:
            category = self._get_category(symbol)
            
            response = await self._get(
                "/v5/market/orderbook",
                params={'category': category, 'symbol': symbol, 'limit': depth}
            )
            
            if response['retCode'] != 0:
                raise Exception(f"Failed to get orderbook: {response['retMsg']}")
            
            book_data = response['result']
            
            return OrderBook(
                symbol=symbol,
                bids=[[float(p), float(s)] for p, s in book_data['b']],
                asks=[[float(p), float(s)] for p, s in book_data['a']],
                timestamp=datetime.fromtimestamp(int(book_data['ts']) / 1000, tz=timezone.utc)
            )
            
        except Exception as e:
            logger.error(f"Error getting orderbook: {str(e)}")
            raise
    
    async def get_trades(self, symbol: str, limit: int = 100) -> List[Trade]:
        """Get recent trades"""
        try:
            category = self._get_category(symbol)
            
            response = await self._get(
                "/v5/market/recent-trade",
                params={'category': category, 'symbol': symbol, 'limit': limit}
            )
            
            if response['retCode'] != 0:
                raise Exception(f"Failed to get trades: {response['retMsg']}")
            
            trades = []
            for trade_data in response['result']['list']:
                trades.append(Trade(
                    id=trade_data.get('execId', ''),
                    order_id=None,
                    symbol=symbol,
                    side=OrderSide.BUY if trade_data['side'] == 'Buy' else OrderSide.SELL,
                    price=float(trade_data['price']),
                    size=float(trade_data['size']),
                    fee=0,
                    fee_currency=None,
                    timestamp=datetime.fromtimestamp(int(trade_data['time']) / 1000, tz=timezone.utc),
                    is_maker=trade_data.get('isBlockTrade', False)
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
        limit: int = 200
    ) -> List[Candle]:
        """Get historical candles"""
        try:
            category = self._get_category(symbol)
            
            params = {
                'category': category,
                'symbol': symbol,
                'interval': self._convert_timeframe(timeframe),
                'limit': limit
            }
            
            if start_time:
                params['start'] = int(start_time.timestamp() * 1000)
            if end_time:
                params['end'] = int(end_time.timestamp() * 1000)
            
            response = await self._get("/v5/market/kline", params=params)
            
            if response['retCode'] != 0:
                raise Exception(f"Failed to get candles: {response['retMsg']}")
            
            candles = []
            for candle_data in response['result']['list']:
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
        balances = {}
        
        # Get spot balances
        spot_balances = await self.spot_trading.get_balance()
        balances.update(spot_balances)
        
        # Get derivatives balances
        deriv_balances = await self.derivatives_trading.get_balance()
        balances.update(deriv_balances)
        
        return balances
    
    async def get_open_positions(self) -> List[Position]:
        """Get open positions"""
        return await self.derivatives_trading.get_open_positions()
    
    async def get_position(self, symbol: str) -> Optional[Position]:
        """Get specific position"""
        return await self.derivatives_trading.get_position(symbol)
    
    # WebSocket methods
    async def _connect_websocket(self):
        """Connect to Bybit WebSocket"""
        await self.ws_client.connect(
            on_message=self.ws_on_message,
            on_error=self.ws_on_error,
            on_close=self.ws_on_close
        )
    
    async def subscribe_ticker(self, symbol: str):
        """Subscribe to ticker updates"""
        await self.ws_client.subscribe_ticker(symbol)
    
    async def subscribe_orderbook(self, symbol: str, depth: int = 50):
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
    def _get_category(self, symbol: str) -> str:
        """Get Bybit category for symbol"""
        if symbol in self.instruments:
            inst_type = self.instruments[symbol]['type']
            if inst_type == 'spot':
                return 'spot'
            elif inst_type in ['linear', 'inverse']:
                return inst_type
            elif inst_type == 'option':
                return 'option'
        
        # Default based on symbol pattern
        if symbol.endswith('USDT'):
            return 'linear'
        elif symbol.endswith('USD'):
            return 'inverse'
        else:
            return 'spot'
    
    def _convert_timeframe(self, timeframe: str) -> str:
        """Convert standard timeframe to Bybit format"""
        timeframe_map = {
            '1m': '1',
            '3m': '3',
            '5m': '5',
            '15m': '15',
            '30m': '30',
            '1h': '60',
            '2h': '120',
            '4h': '240',
            '6h': '360',
            '12h': '720',
            '1d': 'D',
            '1w': 'W',
            '1M': 'M'
        }
        return timeframe_map.get(timeframe, '60')