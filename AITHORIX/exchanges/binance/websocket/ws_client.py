"""
AITHORIX Binance WebSocket Client
Manages WebSocket connections to Binance streams
"""

import asyncio
import json
import logging
from typing import Dict, List, Optional, Any, Callable
import aiohttp
from datetime import datetime

logger = logging.getLogger(__name__)


class BinanceWebSocketClient:
    """
    Binance WebSocket client for real-time data
    """
    
    def __init__(self, client):
        self.client = client
        self.ws_url = client.config.ws_url
        self.ws_connection = None
        self.subscriptions = set()
        self.handlers = BinanceWebSocketHandlers(client)
        
        # Callbacks
        self.on_message: Optional[Callable] = None
        self.on_error: Optional[Callable] = None
        self.on_close: Optional[Callable] = None
        
        # Connection state
        self.is_connected = False
        self.reconnect_count = 0
        self.max_reconnects = 10
        
        # Stream management
        self.stream_id_counter = 1
        self.stream_mapping = {}
        
    async def connect(
        self,
        on_message: Callable,
        on_error: Optional[Callable] = None,
        on_close: Optional[Callable] = None
    ):
        """Connect to Binance WebSocket"""
        self.on_message = on_message
        self.on_error = on_error
        self.on_close = on_close
        
        await self._connect()
    
    async def _connect(self):
        """Internal connection method"""
        try:
            # Create connection
            self.ws_connection = await self.client.ws_session.ws_connect(
                self.ws_url,
                heartbeat=30
            )
            
            self.is_connected = True
            self.reconnect_count = 0
            
            logger.info("Connected to Binance WebSocket")
            
            # Start message handler
            asyncio.create_task(self._handle_messages())
            
            # Resubscribe to streams
            if self.subscriptions:
                await self._resubscribe()
                
        except Exception as e:
            logger.error(f"Failed to connect to Binance WebSocket: {str(e)}")
            await self._handle_disconnect()
    
    async def _handle_messages(self):
        """Handle incoming WebSocket messages"""
        try:
            async for msg in self.ws_connection:
                if msg.type == aiohttp.WSMsgType.TEXT:
                    await self._process_message(json.loads(msg.data))
                elif msg.type == aiohttp.WSMsgType.ERROR:
                    logger.error(f"WebSocket error: {msg.data}")
                    if self.on_error:
                        await self.on_error(msg.data)
                elif msg.type == aiohttp.WSMsgType.CLOSED:
                    logger.warning("WebSocket connection closed")
                    break
                    
        except Exception as e:
            logger.error(f"Error in message handler: {str(e)}")
            if self.on_error:
                await self.on_error(e)
        finally:
            await self._handle_disconnect()
    
    async def _process_message(self, data: Dict[str, Any]):
        """Process WebSocket message"""
        try:
            # Check if it's a response to subscription
            if 'id' in data and 'result' in data:
                stream_id = data['id']
                if stream_id in self.stream_mapping:
                    logger.debug(f"Subscription confirmed for stream {self.stream_mapping[stream_id]}")
                return
            
            # Handle stream data
            if 'stream' in data:
                stream_name = data['stream']
                stream_data = data['data']
                
                # Parse stream type and handle accordingly
                parsed_data = await self.handlers.handle_stream_data(stream_name, stream_data)
                
                if parsed_data and self.on_message:
                    await self.on_message(parsed_data)
                    
            elif 'e' in data:
                # Direct stream data (user data stream)
                parsed_data = await self.handlers.handle_user_data(data)
                
                if parsed_data and self.on_message:
                    await self.on_message(parsed_data)
                    
        except Exception as e:
            logger.error(f"Error processing message: {str(e)}")
            if self.on_error:
                await self.on_error(e)
    
    async def _handle_disconnect(self):
        """Handle disconnection and reconnection"""
        self.is_connected = False
        
        if self.on_close:
            await self.on_close()
        
        # Attempt reconnection
        if self.reconnect_count < self.max_reconnects:
            self.reconnect_count += 1
            wait_time = min(self.reconnect_count * 2, 60)
            
            logger.info(f"Reconnecting in {wait_time} seconds... (attempt {self.reconnect_count})")
            await asyncio.sleep(wait_time)
            
            await self._connect()
        else:
            logger.error("Max reconnection attempts reached")
    
    async def _resubscribe(self):
        """Resubscribe to all streams after reconnection"""
        temp_subs = list(self.subscriptions)
        self.subscriptions.clear()
        
        for stream in temp_subs:
            await self._subscribe_stream(stream)
    
    async def _subscribe_stream(self, stream: str):
        """Subscribe to a single stream"""
        if not self.is_connected:
            logger.warning("Cannot subscribe - not connected")
            return
        
        stream_id = self.stream_id_counter
        self.stream_id_counter += 1
        
        subscribe_message = {
            "method": "SUBSCRIBE",
            "params": [stream],
            "id": stream_id
        }
        
        self.stream_mapping[stream_id] = stream
        self.subscriptions.add(stream)
        
        await self.ws_connection.send_str(json.dumps(subscribe_message))
        logger.debug(f"Subscribed to stream: {stream}")
    
    async def _unsubscribe_stream(self, stream: str):
        """Unsubscribe from a single stream"""
        if not self.is_connected:
            return
        
        if stream not in self.subscriptions:
            return
        
        stream_id = self.stream_id_counter
        self.stream_id_counter += 1
        
        unsubscribe_message = {
            "method": "UNSUBSCRIBE",
            "params": [stream],
            "id": stream_id
        }
        
        self.subscriptions.discard(stream)
        
        await self.ws_connection.send_str(json.dumps(unsubscribe_message))
        logger.debug(f"Unsubscribed from stream: {stream}")
    
    # Public subscription methods
    async def subscribe_ticker(self, symbol: str):
        """Subscribe to ticker updates"""
        stream = f"{self.client._normalize_symbol(symbol).lower()}@ticker"
        await self._subscribe_stream(stream)
    
    async def subscribe_orderbook(self, symbol: str, depth: int = 20):
        """Subscribe to order book updates"""
        levels = "5" if depth <= 5 else "10" if depth <= 10 else "20"
        stream = f"{self.client._normalize_symbol(symbol).lower()}@depth{levels}@100ms"
        await self._subscribe_stream(stream)
    
    async def subscribe_trades(self, symbol: str):
        """Subscribe to trade updates"""
        stream = f"{self.client._normalize_symbol(symbol).lower()}@trade"
        await self._subscribe_stream(stream)
    
    async def subscribe_klines(self, symbol: str, interval: str):
        """Subscribe to kline/candlestick updates"""
        stream = f"{self.client._normalize_symbol(symbol).lower()}@kline_{interval}"
        await self._subscribe_stream(stream)
    
    async def subscribe_user_stream(self, listen_key: str):
        """Subscribe to user data stream"""
        # User data stream doesn't need subscription message
        # Just connect to the listen key URL
        if self.is_connected:
            user_stream_url = f"{self.ws_url}/{listen_key}"
            # This would need a separate connection in practice
            logger.info(f"User data stream would connect to: {user_stream_url}")
    
    async def close(self):
        """Close WebSocket connection"""
        self.is_connected = False
        
        if self.ws_connection:
            await self.ws_connection.close()
            
        self.subscriptions.clear()
        logger.info("Binance WebSocket connection closed")