"""
AITHORIX Hyperliquid WebSocket Client
Manages WebSocket connections to Hyperliquid
"""

import asyncio
import json
import logging
from typing import Dict, List, Optional, Any, Callable
import aiohttp
from datetime import datetime

logger = logging.getLogger(__name__)


class HyperliquidWebSocketClient:
    """
    Hyperliquid WebSocket client for real-time data
    """
    
    def __init__(self, client):
        self.client = client
        self.ws_url = client.config.ws_url
        self.ws_connection = None
        self.subscriptions = {}
        self.handlers = HyperliquidWebSocketHandlers(client)
        
        # Callbacks
        self.on_message: Optional[Callable] = None
        self.on_error: Optional[Callable] = None
        self.on_close: Optional[Callable] = None
        
        # Connection state
        self.is_connected = False
        self.reconnect_count = 0
        self.max_reconnects = 10
        
        # Subscription ID counter
        self.subscription_id = 1
        
    async def connect(
        self,
        on_message: Callable,
        on_error: Optional[Callable] = None,
        on_close: Optional[Callable] = None
    ):
        """Connect to Hyperliquid WebSocket"""
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
            
            logger.info("Connected to Hyperliquid WebSocket")
            
            # Start message handler
            asyncio.create_task(self._handle_messages())
            
            # Authenticate if we have credentials
            if self.client.user_address:
                await self._authenticate()
            
            # Resubscribe to channels
            await self._resubscribe()
                
        except Exception as e:
            logger.error(f"Failed to connect to Hyperliquid WebSocket: {str(e)}")
            await self._handle_disconnect()
    
    async def _authenticate(self):
        """Authenticate WebSocket connection"""
        try:
            auth_data = self.client.authenticator.sign_websocket_auth()
            await self.ws_connection.send_str(json.dumps(auth_data))
            logger.info("Sent WebSocket authentication")
        except Exception as e:
            logger.error(f"WebSocket authentication failed: {str(e)}")
    
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
            # Check message type
            channel = data.get('channel')
            
            if channel == 'subscriptionResponse':
                # Subscription confirmation
                method = data.get('method')
                subscription = data.get('subscription')
                logger.debug(f"Subscription {method} for {subscription}")
                return
            
            # Handle data messages
            parsed_data = await self.handlers.handle_message(data)
            
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
        """Resubscribe to all channels after reconnection"""
        for sub_id, subscription in list(self.subscriptions.items()):
            await self._send_subscription(subscription['channel'], subscription['params'])
    
    async def _subscribe(self, channel: str, params: Dict[str, Any]):
        """Subscribe to a channel"""
        subscription = {
            "method": "subscribe",
            "subscription": {
                "type": channel,
                **params
            }
        }
        
        sub_id = self.subscription_id
        self.subscription_id += 1
        
        self.subscriptions[sub_id] = {
            'channel': channel,
            'params': params
        }
        
        await self._send_subscription(channel, params)
    
    async def _send_subscription(self, channel: str, params: Dict[str, Any]):
        """Send subscription message"""
        if not self.is_connected:
            return
        
        subscription = {
            "method": "subscribe",
            "subscription": {
                "type": channel,
                **params
            }
        }
        
        await self.ws_connection.send_str(json.dumps(subscription))
        logger.debug(f"Subscribed to {channel}: {params}")
    
    async def _unsubscribe(self, channel: str, params: Dict[str, Any]):
        """Unsubscribe from a channel"""
        if not self.is_connected:
            return
        
        subscription = {
            "method": "unsubscribe",
            "subscription": {
                "type": channel,
                **params
            }
        }
        
        # Remove from subscriptions
        for sub_id, sub in list(self.subscriptions.items()):
            if sub['channel'] == channel and sub['params'] == params:
                del self.subscriptions[sub_id]
                break
        
        await self.ws_connection.send_str(json.dumps(subscription))
        logger.debug(f"Unsubscribed from {channel}: {params}")
    
    # Public subscription methods
    async def subscribe_ticker(self, symbol: str):
        """Subscribe to ticker updates"""
        await self._subscribe("l2Book", {"coin": symbol})
    
    async def subscribe_orderbook(self, symbol: str, depth: int = 20):
        """Subscribe to order book updates"""
        await self._subscribe("l2Book", {"coin": symbol})
    
    async def subscribe_trades(self, symbol: str):
        """Subscribe to trade updates"""
        await self._subscribe("trades", {"coin": symbol})
    
    async def subscribe_candles(self, symbol: str, interval: str):
        """Subscribe to candle updates"""
        await self._subscribe("candle", {"coin": symbol, "interval": interval})
    
    async def subscribe_user_events(self, user: str):
        """Subscribe to user events (orders, fills, etc.)"""
        await self._subscribe("userEvents", {"user": user})
    
    async def subscribe_funding(self, symbol: str):
        """Subscribe to funding rate updates"""
        await self._subscribe("activeAssetCtx", {"coin": symbol})
    
    async def close(self):
        """Close WebSocket connection"""
        self.is_connected = False
        
        if self.ws_connection:
            await self.ws_connection.close()
            
        self.subscriptions.clear()
        logger.info("Hyperliquid WebSocket connection closed")