"""
AITHORIX Hyperliquid Client Implementation
DeFi perpetual futures exchange on Arbitrum
"""

import asyncio
import logging
from datetime import datetime, timezone
from typing import Dict, List, Optional, Any, Callable
import json
import time
from eth_account import Account
from eth_account.messages import encode_defunct
from web3 import Web3

from ..base_exchange import (
    BaseExchange, ExchangeConfig, Order, Trade, Position,
    Balance, Ticker, OrderBook, Candle, OrderType, OrderSide,
    OrderStatus, TimeInForce, PositionSide
)
from .perpetual.perpetual_trading import HyperliquidPerpetualTrading
from .websocket.ws_client import HyperliquidWebSocketClient
from .auth.authenticator import HyperliquidAuthenticator

logger = logging.getLogger(__name__)


class HyperliquidClient(BaseExchange):
    """
    Hyperliquid exchange client implementation
    Layer 2 DeFi perpetual futures
    """
    
    def __init__(self, config: ExchangeConfig):
        super().__init__(config)
        
        # Set Hyperliquid-specific URLs
        if config.testnet:
            self.config.rest_url = "https://api.hyperliquid-testnet.xyz"
            self.config.ws_url = "wss://api.hyperliquid-testnet.xyz/ws"
        else:
            self.config.rest_url = "https://api.hyperliquid.xyz"
            self.config.ws_url = "wss://api.hyperliquid.xyz/ws"
        
        # Initialize components
        self.authenticator = HyperliquidAuthenticator(config)
        self.perpetual_trading = HyperliquidPerpetualTrading(self)
        self.ws_client = HyperliquidWebSocketClient(self)
        
        # Hyperliquid specific
        self.user_address = None
        if config.api_key:
            # In Hyperliquid, API key is the private key
            account = Account.from_key(config.api_key)
            self.user_address = account.address
        
        # Market info
        self.market_info: Dict[str, Any] = {}
        self.asset_info: Dict[str, Any] = {}
        
        logger.info("Initialized Hyperliquid client")
    
    async def _initialize_exchange(self):
        """Hyperliquid-specific initialization"""
        # Load market info
        await self._load_market_info()
    
    async def _load_markets(self):
        """Load Hyperliquid market information"""
        await self._load_market_info()
    
    async def _load_market_info(self):
        """Load market and asset information"""
        try:
            # Get meta info
            meta_response = await self._post("/info", data={"type": "meta"})
            
            # Get all mids (market prices)
            mids_response = await self._post("/info", data={"type": "allMids"})
            
            # Process market info
            self.market_info = meta_response.get('universe', {})
            
            # Build asset info
            for asset_data in self.market_info.get('assets', []):
                name = asset_data['name']
                self.asset_info[name] = {
                    'name': name,
                    'szDecimals': asset_data['szDecimals'],
                    'maxLeverage': asset_data.get('maxLeverage', 50),
                    'onlyIsolated': asset_data.get('onlyIsolated', False)
                }
                
                # Add current price
                if name in mids_response:
                    self.asset_info[name]['mid'] = float(mids_response[name])
            
            logger.info(f"Loaded {len(self.asset_info)} assets from Hyperliquid")
            
        except Exception as e:
            logger.error(f"Failed to load market info: {str(e)}")
            raise
    
    async def _check_connection(self):
        """Check Hyperliquid connection"""
        # Simple meta request
        response = await self._post("/info", data={"type": "meta"})
        if not response:
            raise Exception("Failed to connect to Hyperliquid")
    
    async def _get_auth_headers(
        self,
        method: str,
        endpoint: str,
        params: Optional[Dict],
        data: Optional[Dict]
    ) -> Dict[str, str]:
        """Generate Hyperliquid authentication headers"""
        # Hyperliquid uses signature in request body, not headers
        return {}
    
    async def _request(
        self,
        method: str,
        endpoint: str,
        params: Optional[Dict] = None,
        data: Optional[Dict] = None,
        headers: Optional[Dict] = None,
        signed: bool = False
    ) -> Dict[str, Any]:
        """Override request to handle Hyperliquid's signature in body"""
        await self._rate_limit()
        
        url = f"{self.config.rest_url}{endpoint}"
        
        # Add signature to data if needed
        if signed and data:
            signature_data = await self.authenticator.sign_request(data)
            data.update(signature_data)
        
        # Hyperliquid uses POST for everything
        for attempt in range(self.config.max_retries):
            try:
                async with self.session.post(
                    url=url,
                    json=data,
                    headers=headers
                ) as response:
                    response_text = await response.text()
                    
                    if response.status >= 400:
                        raise Exception(f"API error {response.status}: {response_text}")
                    
                    # Parse response
                    try:
                        response_data = json.loads(response_text)
                    except json.JSONDecodeError:
                        # Some endpoints return plain text
                        response_data = {"result": response_text}
                    
                    return response_data
                    
            except Exception as e:
                if attempt == self.config.max_retries - 1:
                    logger.error(f"Request failed after {self.config.max_retries} attempts: {str(e)}")
                    raise
                
                await asyncio.sleep(self.config.retry_delay * (attempt + 1))
    
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
        """Place an order on Hyperliquid"""
        return await self.perpetual_trading.place_order(
            symbol, side, order_type, size, price, params
        )
    
    async def cancel_order(self, order_id: str, symbol: Optional[str] = None) -> bool:
        """Cancel an order"""
        return await self.perpetual_trading.cancel_order(order_id, symbol)
    
    async def get_order(self, order_id: str, symbol: Optional[str] = None) -> Order:
        """Get order details"""
        return await self.perpetual_trading.get_order(order_id, symbol)
    
    async def get_open_orders(self, symbol: Optional[str] = None) -> List[Order]:
        """Get all open orders"""
        return await self.perpetual_trading.get_open_orders(symbol)
    
    async def get_order_history(
        self,
        symbol: Optional[str] = None,
        limit: int = 100,
        start_time: Optional[datetime] = None
    ) -> List[Order]:
        """Get order history"""
        return await self.perpetual_trading.get_order_history(symbol, limit, start_time)
    
    # Market data methods
    async def get_ticker(self, symbol: str) -> Ticker:
        """Get ticker for a symbol"""
        # Get L2 snapshot for best bid/ask
        l2_data = await self._post("/info", data={
            "type": "l2Book",
            "coin": symbol
        })
        
        # Get recent trades for last price
        trades_data = await self._post("/info", data={
            "type": "recentTrades",
            "coin": symbol
        })
        
        # Get 24h stats
        # Hyperliquid doesn't have a direct 24h ticker endpoint
        # Would need to calculate from historical data
        
        best_bid = float(l2_data['levels'][0][0]['px']) if l2_data.get('levels') and l2_data['levels'][0] else 0
        best_ask = float(l2_data['levels'][1][0]['px']) if l2_data.get('levels') and len(l2_data['levels']) > 1 and l2_data['levels'][1] else 0
        last_price = float(trades_data[0]['px']) if trades_data else 0
        
        return Ticker(
            symbol=symbol,
            bid=best_bid,
            ask=best_ask,
            last=last_price,
            volume_24h=0.0,  # Would need to calculate
            quote_volume_24h=0.0,
            high_24h=0.0,
            low_24h=0.0,
            change_24h=0.0,
            change_percent_24h=0.0,
            timestamp=datetime.now(timezone.utc)
        )
    
    async def get_all_tickers(self) -> Dict[str, Ticker]:
        """Get all tickers"""
        # Get all mids
        mids_response = await self._post("/info", data={"type": "allMids"})
        
        tickers = {}
        for asset, mid_price in mids_response.items():
            # For each asset, we'd need to get detailed ticker info
            # For efficiency, just use mid price as last price
            tickers[asset] = Ticker(
                symbol=asset,
                bid=float(mid_price) * 0.9999,  # Approximate
                ask=float(mid_price) * 1.0001,  # Approximate
                last=float(mid_price),
                volume_24h=0.0,
                quote_volume_24h=0.0,
                high_24h=0.0,
                low_24h=0.0,
                change_24h=0.0,
                change_percent_24h=0.0,
                timestamp=datetime.now(timezone.utc)
            )
        
        return tickers
    
    async def get_order_book(self, symbol: str, limit: int = 20) -> OrderBook:
        """Get order book"""
        response = await self._post("/info", data={
            "type": "l2Book",
            "coin": symbol,
            "nSigFigs": 5  # Price precision
        })
        
        bids = []
        asks = []
        
        if 'levels' in response and len(response['levels']) >= 2:
            # First array is bids, second is asks
            for bid in response['levels'][0][:limit]:
                bids.append((float(bid['px']), float(bid['sz'])))
            
            for ask in response['levels'][1][:limit]:
                asks.append((float(ask['px']), float(ask['sz'])))
        
        return OrderBook(
            symbol=symbol,
            bids=bids,
            asks=asks,
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
        # Convert timeframe to Hyperliquid format
        interval = self._convert_timeframe(timeframe)
        
        params = {
            "type": "candleSnapshot",
            "coin": symbol,
            "interval": interval,
            "startTime": int(start_time.timestamp() * 1000) if start_time else None,
            "endTime": int(time.time() * 1000)
        }
        
        # Remove None values
        params = {k: v for k, v in params.items() if v is not None}
        
        response = await self._post("/info", data=params)
        
        candles = []
        for candle_data in response[-limit:]:
            candles.append(Candle(
                symbol=symbol,
                timeframe=timeframe,
                open_time=datetime.fromtimestamp(candle_data['t'] / 1000, tz=timezone.utc),
                close_time=datetime.fromtimestamp((candle_data['t'] + self._get_interval_ms(interval)) / 1000, tz=timezone.utc),
                open=float(candle_data['o']),
                high=float(candle_data['h']),
                low=float(candle_data['l']),
                close=float(candle_data['c']),
                volume=float(candle_data['v']),
                quote_volume=float(candle_data['v']) * float(candle_data['c']),  # Approximate
                trades=0  # Not provided
            ))
        
        return candles
    
    async def get_trades(self, symbol: str, limit: int = 100) -> List[Trade]:
        """Get recent trades"""
        response = await self._post("/info", data={
            "type": "recentTrades",
            "coin": symbol
        })
        
        trades = []
        for i, trade_data in enumerate(response[:limit]):
            trades.append(Trade(
                trade_id=str(trade_data.get('tid', i)),
                order_id="",  # Not provided
                symbol=symbol,
                side=OrderSide.BUY if trade_data['side'] == 'B' else OrderSide.SELL,
                price=float(trade_data['px']),
                size=float(trade_data['sz']),
                fee=0.0,  # Not provided in public trades
                fee_currency="USDC",
                timestamp=datetime.fromtimestamp(trade_data['time'] / 1000, tz=timezone.utc),
                is_maker=False  # Not provided
            ))
        
        return trades
    
    # Account methods
    async def get_balance(self) -> Dict[str, Balance]:
        """Get account balance"""
        return await self.perpetual_trading.get_balance()
    
    async def get_open_positions(self) -> List[Position]:
        """Get open positions"""
        return await self.perpetual_trading.get_open_positions()
    
    async def get_position(self, symbol: str) -> Optional[Position]:
        """Get specific position"""
        return await self.perpetual_trading.get_position(symbol)
    
    # WebSocket methods
    async def _connect_websocket(self):
        """Connect to Hyperliquid WebSocket"""
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
        await self.ws_client.subscribe_orderbook(symbol)
    
    async def subscribe_trades(self, symbol: str):
        """Subscribe to trade updates"""
        await self.ws_client.subscribe_trades(symbol)
    
    async def subscribe_user_orders(self):
        """Subscribe to user order updates"""
        if self.user_address:
            await self.ws_client.subscribe_user_events(self.user_address)
    
    async def subscribe_user_positions(self):
        """Subscribe to user position updates"""
        if self.user_address:
            await self.ws_client.subscribe_user_events(self.user_address)
    
    # Helper methods
    def _convert_timeframe(self, timeframe: str) -> str:
        """Convert standard timeframe to Hyperliquid format"""
        timeframe_map = {
            '1m': '1m',
            '5m': '5m',
            '15m': '15m',
            '30m': '30m',
            '1h': '1h',
            '4h': '4h',
            '1d': '1d'
        }
        return timeframe_map.get(timeframe, '1h')
    
    def _get_interval_ms(self, interval: str) -> int:
        """Get interval duration in milliseconds"""
        interval_ms = {
            '1m': 60000,
            '5m': 300000,
            '15m': 900000,
            '30m': 1800000,
            '1h': 3600000,
            '4h': 14400000,
            '1d': 86400000
        }
        return interval_ms.get(interval, 3600000)