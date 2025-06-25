"""
AITHORIX MEXC WebSocket Client
Real-time data streaming from MEXC
"""

import json
import asyncio
import logging
import time
from typing import Dict, Any, Optional, Callable, List
import websockets
import gzip

from .ws_handlers import MEXCWebSocketHandlers

logger = logging.getLogger(__name__)


class MEXCWebSocketClient:
    """
    MEXC WebSocket client for real-time data
    """
    
    def __init__(self, client):
        self.client = client
        self.handlers = MEXCWebSocketHandlers(client)
        
        # WebSocket connections
        self.public_ws = None
        self.private_ws = None
        
        # Callbacks
        self.on_message = None
        self.on_error = None
        self.on_close = None
        
        # Connection state
        self.is_connected = False
        self.reconnect_count = 0
        self.max_reconnect_attempts = 5
        
        # Subscriptions
        self.subscriptions: Dict[str, List[str]] = {
            'ticker': [],
            'depth': [],
            'trade': [],
            'kline': []
        }
        
        # Ping/Pong
        self.ping_interval = 20  # seconds
        self.last_ping = 0
        self.last_pong = 0
        
        # Tasks
        self.ping_task = None
        self.receive_task = None
        
    async def connect(self, on_message: Optional[Callable] = None,
                     on_error: Optional[Callable] = None,
                     on_close: Optional[Callable] = None):
        """Connect to MEXC WebSocket"""
        try:
            self.on_message = on_message
            self.on_error = on_error
            self.on_close = on_close
            
            # Connect to public WebSocket
            await self._connect_public()
            
            # Connect to private WebSocket if authenticated
            if self.client.config.api_key:
                await self._connect_private()
            
            self.is_connected = True
            self.reconnect_count = 0
            
            logger.info("Connected to MEXC WebSocket")
            
        except Exception as e:
            logger.error(f"Error connecting to WebSocket: {str(e)}")
            await self._handle_error(e)
    
    async def _connect_public(self):
        """Connect to public WebSocket"""
        try:
            ws_url = f"{self.client.config.ws_url}"
            
            self.public_ws = await websockets.connect(
                ws_url,
                ping_interval=None,  # We handle ping/pong manually
                close_timeout=10
            )
            
            # Start ping task
            if self.ping_task:
                self.ping_task.cancel()
            self.ping_task = asyncio.create_task(self._ping_loop())
            
            # Start receive task
            if self.receive_task:
                self.receive_task.cancel()
            self.receive_task = asyncio.create_task(self._receive_loop())
            
        except Exception as e:
            logger.error(f"Error connecting to public WebSocket: {str(e)}")
            raise
    
    async def _connect_private(self):
        """Connect to private WebSocket"""
        try:
            # MEXC uses the same WebSocket for public and private
            # Authentication is done via subscription messages
            
            # Subscribe to private channels
            await self.subscribe_user_data()
            
        except Exception as e:
            logger.error(f"Error connecting to private WebSocket: {str(e)}")
            raise
    
    async def disconnect(self):
        """Disconnect from WebSocket"""
        try:
            self.is_connected = False
            
            # Cancel tasks
            if self.ping_task:
                self.ping_task.cancel()
            if self.receive_task:
                self.receive_task.cancel()
            
            # Close connections
            if self.public_ws:
                await self.public_ws.close()
            
            logger.info("Disconnected from MEXC WebSocket")
            
        except Exception as e:
            logger.error(f"Error disconnecting: {str(e)}")
    
    async def _ping_loop(self):
        """Send periodic ping messages"""
        while self.is_connected:
            try:
                await asyncio.sleep(self.ping_interval)
                
                if self.public_ws:
                    # Send ping
                    ping_msg = {
                        "method": "ping"
                    }
                    await self.public_ws.send(json.dumps(ping_msg))
                    self.last_ping = time.time()
                    
                    # Check for pong timeout
                    if self.last_pong > 0 and time.time() - self.last_pong > 60:
                        logger.warning("Pong timeout, reconnecting...")
                        await self._reconnect()
                
            except Exception as e:
                logger.error(f"Error in ping loop: {str(e)}")
                await self._handle_error(e)
    
    async def _receive_loop(self):
        """Receive and process messages"""
        while self.is_connected:
            try:
                if self.public_ws:
                    message = await self.public_ws.recv()
                    
                    # Decompress if needed
                    if isinstance(message, bytes):
                        message = gzip.decompress(message).decode('utf-8')
                    
                    # Parse message
                    data = json.loads(message)
                    
                    # Handle different message types
                    if data.get('method') == 'pong':
                        self.last_pong = time.time()
                    else:
                        await self._handle_message(data)
                
            except websockets.exceptions.ConnectionClosed:
                logger.warning("WebSocket connection closed")
                await self._reconnect()
            except Exception as e:
                logger.error(f"Error in receive loop: {str(e)}")
                await self._handle_error(e)
    
    async def _handle_message(self, data: Dict[str, Any]):
        """Handle WebSocket message"""
        try:
            # Route to appropriate handler
            if 'channel' in data:
                channel = data['channel']
                
                if channel.startswith('spot@public.deals'):
                    await self.handlers.handle_trade(data)
                elif channel.startswith('spot@public.bookTicker'):
                    await self.handlers.handle_ticker(data)
                elif channel.startswith('spot@public.limit.depth'):
                    await self.handlers.handle_orderbook(data)
                elif channel.startswith('spot@public.kline'):
                    await self.handlers.handle_kline(data)
                elif channel.startswith('spot@private'):
                    await self.handlers.handle_user_data(data)
                elif channel.startswith('contract@'):
                    # Handle futures data
                    if 'ticker' in channel:
                        await self.handlers.handle_futures_ticker(data)
                    elif 'depth' in channel:
                        await self.handlers.handle_futures_orderbook(data)
                    elif 'deal' in channel:
                        await self.handlers.handle_futures_trade(data)
                    elif 'private' in channel:
                        await self.handlers.handle_futures_user_data(data)
            
            # Call user callback
            if self.on_message:
                await self.on_message(data)
                
        except Exception as e:
            logger.error(f"Error handling message: {str(e)}")
            await self._handle_error(e)
    
    async def _handle_error(self, error: Exception):
        """Handle WebSocket error"""
        logger.error(f"WebSocket error: {str(error)}")
        
        if self.on_error:
            await self.on_error(error)
        
        # Attempt reconnection
        if self.is_connected:
            await self._reconnect()
    
    async def _reconnect(self):
        """Reconnect to WebSocket"""
        if self.reconnect_count >= self.max_reconnect_attempts:
            logger.error("Max reconnection attempts reached")
            await self.disconnect()
            return
        
        self.reconnect_count += 1
        wait_time = min(self.reconnect_count * 5, 30)  # Max 30 seconds
        
        logger.info(f"Reconnecting in {wait_time} seconds... (attempt {self.reconnect_count})")
        await asyncio.sleep(wait_time)
        
        try:
            await self.disconnect()
            await self.connect(self.on_message, self.on_error, self.on_close)
            
            # Resubscribe to channels
            await self._resubscribe()
            
        except Exception as e:
            logger.error(f"Reconnection failed: {str(e)}")
            await self._handle_error(e)
    
    async def _resubscribe(self):
        """Resubscribe to all channels"""
        # Resubscribe to tickers
        for symbol in self.subscriptions['ticker']:
            await self.subscribe_ticker(symbol)
        
        # Resubscribe to order books
        for symbol in self.subscriptions['depth']:
            await self.subscribe_orderbook(symbol)
        
        # Resubscribe to trades
        for symbol in self.subscriptions['trade']:
            await self.subscribe_trades(symbol)
        
        # Resubscribe to user data
        if self.client.config.api_key and any(self.subscriptions.values()):
            await self.subscribe_user_data()
    
    # Subscription methods
    async def subscribe_ticker(self, symbol: str):
        """Subscribe to ticker updates"""
        try:
            # Spot ticker
            sub_msg = {
                "method": "SUBSCRIPTION",
                "params": [f"spot@public.bookTicker.v3.api@{symbol}"]
            }
            
            if self.public_ws:
                await self.public_ws.send(json.dumps(sub_msg))
                self.subscriptions['ticker'].append(symbol)
                
                logger.info(f"Subscribed to ticker: {symbol}")
                
        except Exception as e:
            logger.error(f"Error subscribing to ticker: {str(e)}")
    
    async def subscribe_orderbook(self, symbol: str, depth: int = 20):
        """Subscribe to order book updates"""
        try:
            # Spot order book
            sub_msg = {
                "method": "SUBSCRIPTION",
                "params": [f"spot@public.limit.depth.v3.api@{symbol}@{depth}"]
            }
            
            if self.public_ws:
                await self.public_ws.send(json.dumps(sub_msg))
                self.subscriptions['depth'].append(symbol)
                
                logger.info(f"Subscribed to order book: {symbol}")
                
        except Exception as e:
            logger.error(f"Error subscribing to order book: {str(e)}")
    
    async def subscribe_trades(self, symbol: str):
        """Subscribe to trade updates"""
        try:
            # Spot trades
            sub_msg = {
                "method": "SUBSCRIPTION",
                "params": [f"spot@public.deals.v3.api@{symbol}"]
            }
            
            if self.public_ws:
                await self.public_ws.send(json.dumps(sub_msg))
                self.subscriptions['trade'].append(symbol)
                
                logger.info(f"Subscribed to trades: {symbol}")
                
        except Exception as e:
            logger.error(f"Error subscribing to trades: {str(e)}")
    
    async def subscribe_kline(self, symbol: str, interval: str):
        """Subscribe to kline updates"""
        try:
            # Spot klines
            sub_msg = {
                "method": "SUBSCRIPTION",
                "params": [f"spot@public.kline.v3.api@{symbol}@{interval}"]
            }
            
            if self.public_ws:
                await self.public_ws.send(json.dumps(sub_msg))
                self.subscriptions['kline'].append(f"{symbol}@{interval}")
                
                logger.info(f"Subscribed to klines: {symbol} {interval}")
                
        except Exception as e:
            logger.error(f"Error subscribing to klines: {str(e)}")
    
    async def subscribe_user_data(self):
        """Subscribe to user data stream"""
        try:
            if not self.client.config.api_key:
                logger.warning("API key required for user data subscription")
                return
            
            # Get authentication payload
            auth_payload = self.client.authenticator.get_websocket_auth_payload()
            
            # Subscribe to spot private channel
            spot_sub = {
                "method": "SUBSCRIPTION",
                "params": ["spot@private.account.v3.api"]
            }
            
            # Send authentication
            if self.public_ws:
                await self.public_ws.send(json.dumps(auth_payload))
                await self.public_ws.send(json.dumps(spot_sub))
                
                # Subscribe to futures private channel if applicable
                futures_sub = {
                    "method": "sub.personal",
                    "param": {}
                }
                await self.public_ws.send(json.dumps(futures_sub))
                
                logger.info("Subscribed to user data stream")
                
        except Exception as e:
            logger.error(f"Error subscribing to user data: {str(e)}")
    
    async def unsubscribe(self, channel: str, symbol: str):
        """Unsubscribe from a channel"""
        try:
            unsub_msg = {
                "method": "UNSUBSCRIPTION",
                "params": [f"{channel}@{symbol}"]
            }
            
            if self.public_ws:
                await self.public_ws.send(json.dumps(unsub_msg))
                
                # Remove from subscriptions
                if channel in self.subscriptions and symbol in self.subscriptions[channel]:
                    self.subscriptions[channel].remove(symbol)
                
                logger.info(f"Unsubscribed from {channel}: {symbol}")
                
        except Exception as e:
            logger.error(f"Error unsubscribing: {str(e)}")