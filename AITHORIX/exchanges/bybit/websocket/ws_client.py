"""
AITHORIX Bybit WebSocket Client
Real-time data streaming from Bybit
"""

import json
import asyncio
import logging
import time
from typing import Dict, Any, Optional, Callable, List, Set
import websockets
from datetime import datetime

from .ws_handlers import BybitWebSocketHandlers

logger = logging.getLogger(__name__)


class BybitWebSocketClient:
    """
    Bybit WebSocket client for real-time data
    """
    
    def __init__(self, client):
        self.client = client
        self.handlers = BybitWebSocketHandlers(client)
        
        # WebSocket connections
        self.public_ws: Dict[str, Any] = {}  # category -> connection
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
        self.subscriptions: Dict[str, Set[str]] = {
            'orderbook': set(),
            'trade': set(),
            'ticker': set(),
            'kline': set(),
            'liquidation': set()
        }
        
        # Ping/Pong
        self.ping_interval = 20  # seconds
        self.ping_tasks = {}
        self.receive_tasks = {}
        
    async def connect(self, on_message: Optional[Callable] = None,
                     on_error: Optional[Callable] = None,
                     on_close: Optional[Callable] = None):
        """Connect to Bybit WebSocket"""
        try:
            self.on_message = on_message
            self.on_error = on_error
            self.on_close = on_close
            
            # Connect to public WebSocket streams by category
            await self._connect_public('spot')
            await self._connect_public('linear')
            await self._connect_public('inverse')
            
            # Connect to private WebSocket if authenticated
            if self.client.config.api_key:
                await self._connect_private()
            
            self.is_connected = True
            self.reconnect_count = 0
            
            logger.info("Connected to Bybit WebSocket")
            
        except Exception as e:
            logger.error(f"Error connecting to WebSocket: {str(e)}")
            await self._handle_error(e)
    
    async def _connect_public(self, category: str):
        """Connect to public WebSocket for a category"""
        try:
            # Get appropriate WebSocket URL
            if category == 'spot':
                ws_url = self.client.config.ws_url
            elif category == 'linear':
                ws_url = self.client.config.ws_url.replace('/spot', '/linear')
            elif category == 'inverse':
                ws_url = self.client.config.ws_url.replace('/spot', '/inverse')
            elif category == 'option':
                ws_url = self.client.config.ws_url.replace('/spot', '/option')
            else:
                return
            
            ws = await websockets.connect(
                ws_url,
                ping_interval=None,  # We handle ping/pong manually
                close_timeout=10
            )
            
            self.public_ws[category] = ws
            
            # Start ping task
            ping_task = asyncio.create_task(self._ping_loop(category))
            self.ping_tasks[category] = ping_task
            
            # Start receive task
            receive_task = asyncio.create_task(self._receive_loop(category))
            self.receive_tasks[category] = receive_task
            
            logger.info(f"Connected to Bybit {category} WebSocket")
            
        except Exception as e:
            logger.error(f"Error connecting to {category} WebSocket: {str(e)}")
            raise
    
    async def _connect_private(self):
        """Connect to private WebSocket"""
        try:
            ws_url = self.client.config.ws_private_url
            
            self.private_ws = await websockets.connect(
                ws_url,
                ping_interval=None,
                close_timeout=10
            )
            
            # Authenticate
            auth_payload = self.client.authenticator.get_websocket_auth_payload()
            await self.private_ws.send(json.dumps(auth_payload))
            
            # Wait for auth response
            auth_response = await self.private_ws.recv()
            response_data = json.loads(auth_response)
            
            if not response_data.get('success'):
                raise Exception(f"Authentication failed: {response_data}")
            
            # Start ping task
            self.ping_tasks['private'] = asyncio.create_task(self._ping_loop('private'))
            
            # Start receive task
            self.receive_tasks['private'] = asyncio.create_task(self._receive_loop('private'))
            
            logger.info("Connected to Bybit private WebSocket")
            
        except Exception as e:
            logger.error(f"Error connecting to private WebSocket: {str(e)}")
            raise
    
    async def disconnect(self):
        """Disconnect from WebSocket"""
        try:
            self.is_connected = False
            
            # Cancel all tasks
            for task in self.ping_tasks.values():
                if task:
                    task.cancel()
            
            for task in self.receive_tasks.values():
                if task:
                    task.cancel()
            
            # Close all connections
            for ws in self.public_ws.values():
                if ws:
                    await ws.close()
            
            if self.private_ws:
                await self.private_ws.close()
            
            self.public_ws.clear()
            self.private_ws = None
            
            logger.info("Disconnected from Bybit WebSocket")
            
        except Exception as e:
            logger.error(f"Error disconnecting: {str(e)}")
    
    async def _ping_loop(self, connection_type: str):
        """Send periodic ping messages"""
        while self.is_connected:
            try:
                await asyncio.sleep(self.ping_interval)
                
                ws = self.private_ws if connection_type == 'private' else self.public_ws.get(connection_type)
                
                if ws:
                    # Send ping
                    ping_msg = {"op": "ping"}
                    await ws.send(json.dumps(ping_msg))
                    logger.debug(f"Sent ping to {connection_type}")
                
            except Exception as e:
                logger.error(f"Error in ping loop ({connection_type}): {str(e)}")
                await self._handle_error(e)
    
    async def _receive_loop(self, connection_type: str):
        """Receive and process messages"""
        while self.is_connected:
            try:
                ws = self.private_ws if connection_type == 'private' else self.public_ws.get(connection_type)
                
                if ws:
                    message = await ws.recv()
                    data = json.loads(message)
                    
                    # Handle different message types
                    if data.get('op') == 'pong':
                        logger.debug(f"Received pong from {connection_type}")
                    elif data.get('success') is not None:
                        # Subscription response
                        logger.info(f"Subscription response: {data}")
                    else:
                        await self._handle_message(data, connection_type)
                
            except websockets.exceptions.ConnectionClosed:
                logger.warning(f"WebSocket connection closed ({connection_type})")
                await self._reconnect(connection_type)
            except Exception as e:
                logger.error(f"Error in receive loop ({connection_type}): {str(e)}")
                await self._handle_error(e)
    
    async def _handle_message(self, data: Dict[str, Any], connection_type: str):
        """Handle WebSocket message"""
        try:
            # Route to appropriate handler
            topic = data.get('topic', '')
            
            if 'orderbook' in topic:
                await self.handlers.handle_orderbook(data)
            elif 'trade' in topic:
                await self.handlers.handle_trade(data)
            elif 'ticker' in topic or 'tickers' in topic:
                await self.handlers.handle_ticker(data)
            elif 'kline' in topic:
                await self.handlers.handle_kline(data)
            elif 'liquidation' in topic:
                await self.handlers.handle_liquidation(data)
            elif 'position' in topic:
                await self.handlers.handle_position(data)
            elif 'execution' in topic:
                await self.handlers.handle_execution(data)
            elif 'order' in topic:
                await self.handlers.handle_order(data)
            elif 'wallet' in topic:
                await self.handlers.handle_wallet(data)
            
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
            elif connection_type == 'private':
                if self.private_ws:
                    await self.private_ws.close()
                await self._connect_private()
            else:
                if connection_type in self.public_ws:
                    await self.public_ws[connection_type].close()
                await self._connect_public(connection_type)
            
            # Resubscribe to channels
            await self._resubscribe()
            
        except Exception as e:
            logger.error(f"Reconnection failed: {str(e)}")
            await self._handle_error(e)
    
    async def _resubscribe(self):
        """Resubscribe to all channels"""
        # Resubscribe to public channels
        for topic, symbols in self.subscriptions.items():
            for symbol in symbols:
                if topic == 'orderbook':
                    await self.subscribe_orderbook(symbol)
                elif topic == 'trade':
                    await self.subscribe_trades(symbol)
                elif topic == 'ticker':
                    await self.subscribe_ticker(symbol)
                elif topic == 'kline':
                    # Need to parse interval from subscription
                    parts = symbol.split('.')
                    if len(parts) >= 2:
                        await self.subscribe_kline(parts[0], parts[1])
                elif topic == 'liquidation':
                    await self.subscribe_liquidation(symbol)
        
        # Resubscribe to private channels
        if self.client.config.api_key and self.private_ws:
            await self.subscribe_user_data()
    
    # Subscription methods
    async def subscribe_ticker(self, symbol: str):
        """Subscribe to ticker updates"""
        try:
            category = self._get_category(symbol)
            ws = self.public_ws.get(category)
            
            if not ws:
                logger.warning(f"No WebSocket connection for {category}")
                return
            
            sub_msg = {
                "op": "subscribe",
                "args": [f"tickers.{symbol}"]
            }
            
            await ws.send(json.dumps(sub_msg))
            self.subscriptions['ticker'].add(symbol)
            
            logger.info(f"Subscribed to ticker: {symbol}")
            
        except Exception as e:
            logger.error(f"Error subscribing to ticker: {str(e)}")
    
    async def subscribe_orderbook(self, symbol: str, depth: int = 50):
        """Subscribe to order book updates"""
        try:
            category = self._get_category(symbol)
            ws = self.public_ws.get(category)
            
            if not ws:
                logger.warning(f"No WebSocket connection for {category}")
                return
            
            sub_msg = {
                "op": "subscribe",
                "args": [f"orderbook.{depth}.{symbol}"]
            }
            
            await ws.send(json.dumps(sub_msg))
            self.subscriptions['orderbook'].add(symbol)
            
            logger.info(f"Subscribed to order book: {symbol}")
            
        except Exception as e:
            logger.error(f"Error subscribing to order book: {str(e)}")
    
    async def subscribe_trades(self, symbol: str):
        """Subscribe to trade updates"""
        try:
            category = self._get_category(symbol)
            ws = self.public_ws.get(category)
            
            if not ws:
                logger.warning(f"No WebSocket connection for {category}")
                return
            
            sub_msg = {
                "op": "subscribe",
                "args": [f"publicTrade.{symbol}"]
            }
            
            await ws.send(json.dumps(sub_msg))
            self.subscriptions['trade'].add(symbol)
            
            logger.info(f"Subscribed to trades: {symbol}")
            
        except Exception as e:
            logger.error(f"Error subscribing to trades: {str(e)}")
    
    async def subscribe_kline(self, symbol: str, interval: str):
        """Subscribe to kline updates"""
        try:
            category = self._get_category(symbol)
            ws = self.public_ws.get(category)
            
            if not ws:
                logger.warning(f"No WebSocket connection for {category}")
                return
            
            # Convert interval to Bybit format
            interval_map = {
                '1m': '1', '3m': '3', '5m': '5', '15m': '15', '30m': '30',
                '1h': '60', '2h': '120', '4h': '240', '6h': '360', '12h': '720',
                '1d': 'D', '1w': 'W', '1M': 'M'
            }
            bybit_interval = interval_map.get(interval, interval)
            
            sub_msg = {
                "op": "subscribe",
                "args": [f"kline.{bybit_interval}.{symbol}"]
            }
            
            await ws.send(json.dumps(sub_msg))
            self.subscriptions['kline'].add(f"{symbol}.{interval}")
            
            logger.info(f"Subscribed to klines: {symbol} {interval}")
            
        except Exception as e:
            logger.error(f"Error subscribing to klines: {str(e)}")
    
    async def subscribe_liquidation(self, symbol: str):
        """Subscribe to liquidation updates"""
        try:
            category = self._get_category(symbol)
            ws = self.public_ws.get(category)
            
            if not ws:
                logger.warning(f"No WebSocket connection for {category}")
                return
            
            sub_msg = {
                "op": "subscribe",
                "args": [f"liquidation.{symbol}"]
            }
            
            await ws.send(json.dumps(sub_msg))
            self.subscriptions['liquidation'].add(symbol)
            
            logger.info(f"Subscribed to liquidations: {symbol}")
            
        except Exception as e:
            logger.error(f"Error subscribing to liquidations: {str(e)}")
    
    async def subscribe_user_data(self):
        """Subscribe to user data stream"""
        try:
            if not self.private_ws:
                logger.warning("Private WebSocket not connected")
                return
            
            # Subscribe to all private topics
            sub_msg = {
                "op": "subscribe",
                "args": [
                    "position",
                    "execution",
                    "order",
                    "wallet"
                ]
            }
            
            await self.private_ws.send(json.dumps(sub_msg))
            
            logger.info("Subscribed to user data stream")
            
        except Exception as e:
            logger.error(f"Error subscribing to user data: {str(e)}")
    
    async def unsubscribe(self, topic: str, symbol: str):
        """Unsubscribe from a topic"""
        try:
            category = self._get_category(symbol)
            ws = self.public_ws.get(category)
            
            if not ws:
                return
            
            # Build topic string based on type
            if topic == 'orderbook':
                topic_str = f"orderbook.50.{symbol}"
            elif topic == 'trade':
                topic_str = f"publicTrade.{symbol}"
            elif topic == 'ticker':
                topic_str = f"tickers.{symbol}"
            else:
                topic_str = f"{topic}.{symbol}"
            
            unsub_msg = {
                "op": "unsubscribe",
                "args": [topic_str]
            }
            
            await ws.send(json.dumps(unsub_msg))
            
            # Remove from subscriptions
            if topic in self.subscriptions:
                self.subscriptions[topic].discard(symbol)
            
            logger.info(f"Unsubscribed from {topic}: {symbol}")
            
        except Exception as e:
            logger.error(f"Error unsubscribing: {str(e)}")
    
    def _get_category(self, symbol: str) -> str:
        """Get category for symbol"""
        if symbol in self.client.instruments:
            inst_type = self.client.instruments[symbol]['type']
            return inst_type if inst_type != 'option' else 'option'
        
        # Guess based on symbol
        if symbol.endswith('USDT') or symbol.endswith('PERP'):
            return 'linear'
        elif symbol.endswith('USD'):
            return 'inverse'
        else:
            return 'spot'