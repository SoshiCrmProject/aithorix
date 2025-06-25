"""
AITHORIX OKX WebSocket Client
Real-time data streaming from OKX
"""

import json
import asyncio
import logging
import time
from typing import Dict, Any, Optional, Callable, List, Set
import websockets
from datetime import datetime

from .ws_handlers import OKXWebSocketHandlers

logger = logging.getLogger(__name__)


class OKXWebSocketClient:
    """
    OKX WebSocket client for real-time data
    """
    
    def __init__(self, client):
        self.client = client
        self.handlers = OKXWebSocketHandlers(client)
        
        # WebSocket connections
        self.public_ws = None
        self.private_ws = None
        
        # Callbacks
        self.on_message = None
        self.on_error = None
        self.on_close = None
        
        # Connection state
        self.is_connected = False
        self.is_authenticated = False
        self.reconnect_count = 0
        self.max_reconnect_attempts = 5
        
        # Subscriptions
        self.public_subscriptions: Set[str] = set()
        self.private_subscriptions: Set[str] = set()
        
        # Ping/Pong
        self.ping_interval = 25  # seconds (OKX requires ping every 30s)
        self.last_ping_time = 0
        self.last_pong_time = 0
        
        # Tasks
        self.ping_task = None
        self.public_receive_task = None
        self.private_receive_task = None
        
    async def connect(self, on_message: Optional[Callable] = None,
                     on_error: Optional[Callable] = None,
                     on_close: Optional[Callable] = None):
        """Connect to OKX WebSocket"""
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
            
            logger.info("Connected to OKX WebSocket")
            
        except Exception as e:
            logger.error(f"Error connecting to WebSocket: {str(e)}")
            await self._handle_error(e)
    
    async def _connect_public(self):
        """Connect to public WebSocket"""
        try:
            self.public_ws = await websockets.connect(
                self.client.config.ws_public_url,
                ping_interval=None,  # We handle ping/pong manually
                close_timeout=10
            )
            
            # Start tasks
            self.public_receive_task = asyncio.create_task(self._receive_loop('public'))
            
            logger.info("Connected to OKX public WebSocket")
            
        except Exception as e:
            logger.error(f"Error connecting to public WebSocket: {str(e)}")
            raise
    
    async def _connect_private(self):
        """Connect to private WebSocket"""
        try:
            self.private_ws = await websockets.connect(
                self.client.config.ws_private_url,
                ping_interval=None,
                close_timeout=10
            )
            
            # Authenticate
            auth_payload = self.client.authenticator.get_websocket_auth_payload()
            await self.private_ws.send(json.dumps(auth_payload))
            
            # Wait for auth response
            auth_response = await self.private_ws.recv()
            response_data = json.loads(auth_response)
            
            if response_data.get('event') == 'login' and response_data.get('code') == '0':
                self.is_authenticated = True
                logger.info("Authenticated to OKX private WebSocket")
            else:
                raise Exception(f"Authentication failed: {response_data}")
            
            # Start tasks
            self.private_receive_task = asyncio.create_task(self._receive_loop('private'))
            
            # Start shared ping task (OKX uses same ping for both connections)
            if not self.ping_task:
                self.ping_task = asyncio.create_task(self._ping_loop())
            
            logger.info("Connected to OKX private WebSocket")
            
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
            if self.public_receive_task:
                self.public_receive_task.cancel()
            if self.private_receive_task:
                self.private_receive_task.cancel()
            
            # Close connections
            if self.public_ws:
                await self.public_ws.close()
            if self.private_ws:
                await self.private_ws.close()
            
            self.public_ws = None
            self.private_ws = None
            self.is_authenticated = False
            
            logger.info("Disconnected from OKX WebSocket")
            
        except Exception as e:
            logger.error(f"Error disconnecting: {str(e)}")
    
    async def _ping_loop(self):
        """Send periodic ping messages"""
        while self.is_connected:
            try:
                await asyncio.sleep(self.ping_interval)
                
                # Send ping to both connections
                ping_msg = "ping"
                
                if self.public_ws:
                    await self.public_ws.send(ping_msg)
                    self.last_ping_time = time.time()
                
                if self.private_ws:
                    await self.private_ws.send(ping_msg)
                
                logger.debug("Sent ping to OKX")
                
                # Check for pong timeout
                if self.last_ping_time > 0 and time.time() - self.last_ping_time > 60:
                    logger.warning("Pong timeout, reconnecting...")
                    await self._reconnect('all')
                
            except Exception as e:
                logger.error(f"Error in ping loop: {str(e)}")
                await self._handle_error(e)
    
    async def _receive_loop(self, connection_type: str):
        """Receive and process messages"""
        while self.is_connected:
            try:
                ws = self.public_ws if connection_type == 'public' else self.private_ws
                
                if ws:
                    message = await ws.recv()
                    
                    # Handle pong
                    if message == "pong":
                        self.last_pong_time = time.time()
                        logger.debug(f"Received pong from {connection_type}")
                        continue
                    
                    # Parse JSON message
                    data = json.loads(message)
                    
                    # Handle different message types
                    if data.get('event'):
                        await self._handle_event(data, connection_type)
                    else:
                        await self._handle_message(data, connection_type)
                
            except websockets.exceptions.ConnectionClosed:
                logger.warning(f"WebSocket connection closed ({connection_type})")
                await self._reconnect(connection_type)
            except Exception as e:
                logger.error(f"Error in receive loop ({connection_type}): {str(e)}")
                await self._handle_error(e)
    
    async def _handle_event(self, data: Dict[str, Any], connection_type: str):
        """Handle WebSocket events"""
        try:
            event = data.get('event')
            
            if event == 'subscribe':
                if data.get('code') == '0':
                    logger.info(f"Successfully subscribed: {data}")
                else:
                    logger.error(f"Subscription failed: {data}")
            elif event == 'unsubscribe':
                if data.get('code') == '0':
                    logger.info(f"Successfully unsubscribed: {data}")
                else:
                    logger.error(f"Unsubscription failed: {data}")
            elif event == 'error':
                logger.error(f"WebSocket error: {data}")
                
        except Exception as e:
            logger.error(f"Error handling event: {str(e)}")
    
    async def _handle_message(self, data: Dict[str, Any], connection_type: str):
        """Handle WebSocket message"""
        try:
            # Get channel from arg
            arg = data.get('arg', {})
            channel = arg.get('channel', '')
            
            # Route to appropriate handler
            if channel == 'tickers':
                await self.handlers.handle_ticker(data)
            elif channel.startswith('books'):
                await self.handlers.handle_orderbook(data)
            elif channel == 'trades':
                await self.handlers.handle_trade(data)
            elif channel.startswith('candle'):
                await self.handlers.handle_kline(data)
            elif channel == 'mark-price':
                await self.handlers.handle_mark_price(data)
            elif channel == 'funding-rate':
                await self.handlers.handle_funding_rate(data)
            elif channel == 'instruments':
                await self.handlers.handle_instruments(data)
            elif channel == 'open-interest':
                await self.handlers.handle_open_interest(data)
            elif channel == 'liquidation-orders':
                await self.handlers.handle_liquidation(data)
            # Private channels
            elif channel == 'account':
                await self.handlers.handle_account(data)
            elif channel == 'positions':
                await self.handlers.handle_positions(data)
            elif channel == 'balance_and_position':
                await self.handlers.handle_balance_and_position(data)
            elif channel == 'orders':
                await self.handlers.handle_orders(data)
            elif channel == 'orders-algo':
                await self.handlers.handle_algo_orders(data)
            
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
        
        # Attempt reconnection for connection errors
        if isinstance(error, (websockets.exceptions.WebSocketException, ConnectionError)):
            await self._reconnect('all')
    
    async def _reconnect(self, connection_type: str):
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
            if connection_type == 'all':
                await self.disconnect()
                await self.connect(self.on_message, self.on_error, self.on_close)
            elif connection_type == 'public':
                if self.public_ws:
                    await self.public_ws.close()
                await self._connect_public()
            elif connection_type == 'private':
                if self.private_ws:
                    await self.private_ws.close()
                await self._connect_private()
            
            # Resubscribe to channels
            await self._resubscribe()
            
        except Exception as e:
            logger.error(f"Reconnection failed: {str(e)}")
            await self._handle_error(e)
    
    async def _resubscribe(self):
        """Resubscribe to all channels"""
        # Resubscribe to public channels
        for sub in self.public_subscriptions:
            args = json.loads(sub)
            await self._subscribe_public(args)
        
        # Resubscribe to private channels
        if self.is_authenticated:
            for sub in self.private_subscriptions:
                args = json.loads(sub)
                await self._subscribe_private(args)
    
    # Subscription methods
    async def subscribe_ticker(self, symbol: str):
        """Subscribe to ticker updates"""
        try:
            args = {
                "channel": "tickers",
                "instId": symbol
            }
            
            await self._subscribe_public(args)
            
            logger.info(f"Subscribed to ticker: {symbol}")
            
        except Exception as e:
            logger.error(f"Error subscribing to ticker: {str(e)}")
    
    async def subscribe_orderbook(self, symbol: str, depth: str = "books5"):
        """Subscribe to order book updates"""
        try:
            # depth can be: books, books5, books-l2-tbt, books50-l2-tbt
            args = {
                "channel": depth,
                "instId": symbol
            }
            
            await self._subscribe_public(args)
            
            logger.info(f"Subscribed to order book: {symbol}")
            
        except Exception as e:
            logger.error(f"Error subscribing to order book: {str(e)}")
    
    async def subscribe_trades(self, symbol: str):
        """Subscribe to trade updates"""
        try:
            args = {
                "channel": "trades",
                "instId": symbol
            }
            
            await self._subscribe_public(args)
            
            logger.info(f"Subscribed to trades: {symbol}")
            
        except Exception as e:
            logger.error(f"Error subscribing to trades: {str(e)}")
    
    async def subscribe_kline(self, symbol: str, interval: str):
        """Subscribe to kline updates"""
        try:
            # Convert interval to OKX format
            okx_interval = self.client._convert_timeframe(interval)
            
            args = {
                "channel": f"candle{okx_interval}",
                "instId": symbol
            }
            
            await self._subscribe_public(args)
            
            logger.info(f"Subscribed to klines: {symbol} {interval}")
            
        except Exception as e:
            logger.error(f"Error subscribing to klines: {str(e)}")
    
    async def subscribe_mark_price(self, symbol: str):
        """Subscribe to mark price updates"""
        try:
            args = {
                "channel": "mark-price",
                "instId": symbol
            }
            
            await self._subscribe_public(args)
            
            logger.info(f"Subscribed to mark price: {symbol}")
            
        except Exception as e:
            logger.error(f"Error subscribing to mark price: {str(e)}")
    
    async def subscribe_funding_rate(self, symbol: str):
        """Subscribe to funding rate updates"""
        try:
            args = {
                "channel": "funding-rate",
                "instId": symbol
            }
            
            await self._subscribe_public(args)
            
            logger.info(f"Subscribed to funding rate: {symbol}")
            
        except Exception as e:
            logger.error(f"Error subscribing to funding rate: {str(e)}")
    
    async def subscribe_user_data(self):
        """Subscribe to user data stream"""
        try:
            if not self.is_authenticated:
                logger.warning("Not authenticated, cannot subscribe to user data")
                return
            
            # Subscribe to all user data channels
            channels = [
                {"channel": "account"},
                {"channel": "positions", "instType": "ANY"},
                {"channel": "balance_and_position"},
                {"channel": "orders", "instType": "ANY"},
                {"channel": "orders-algo", "instType": "ANY"}
            ]
            
            for args in channels:
                await self._subscribe_private(args)
            
            logger.info("Subscribed to user data stream")
            
        except Exception as e:
            logger.error(f"Error subscribing to user data: {str(e)}")
    
    async def _subscribe_public(self, args: Dict[str, Any]):
        """Subscribe to public channel"""
        if not self.public_ws:
            return
        
        sub_msg = {
            "op": "subscribe",
            "args": [args]
        }
        
        await self.public_ws.send(json.dumps(sub_msg))
        self.public_subscriptions.add(json.dumps(args))
    
    async def _subscribe_private(self, args: Dict[str, Any]):
        """Subscribe to private channel"""
        if not self.private_ws or not self.is_authenticated:
            return
        
        sub_msg = {
            "op": "subscribe",
            "args": [args]
        }
        
        await self.private_ws.send(json.dumps(sub_msg))
        self.private_subscriptions.add(json.dumps(args))
    
    async def unsubscribe(self, channel: str, symbol: str):
        """Unsubscribe from a channel"""
        try:
            args = {
                "channel": channel,
                "instId": symbol
            }
            
            unsub_msg = {
                "op": "unsubscribe",
                "args": [args]
            }
            
            # Determine which connection to use
            if channel in ['account', 'positions', 'balance_and_position', 'orders', 'orders-algo']:
                if self.private_ws:
                    await self.private_ws.send(json.dumps(unsub_msg))
                    self.private_subscriptions.discard(json.dumps(args))
            else:
                if self.public_ws:
                    await self.public_ws.send(json.dumps(unsub_msg))
                    self.public_subscriptions.discard(json.dumps(args))
            
            logger.info(f"Unsubscribed from {channel}: {symbol}")
            
        except Exception as e:
            logger.error(f"Error unsubscribing: {str(e)}")