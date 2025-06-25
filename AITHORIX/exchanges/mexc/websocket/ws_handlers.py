"""
AITHORIX MEXC WebSocket Handlers
Process WebSocket messages from MEXC
"""

import logging
from datetime import datetime, timezone
from typing import Dict, Any

from ...base_exchange import (
    Ticker, OrderBook, Trade, Order, Position,
    OrderSide, OrderStatus, OrderType, PositionSide
)

logger = logging.getLogger(__name__)


class MEXCWebSocketHandlers:
    """
    MEXC WebSocket message handlers
    """
    
    def __init__(self, client):
        self.client = client
    
    async def handle_ticker(self, data: Dict[str, Any]):
        """Handle ticker updates"""
        try:
            if 'data' not in data:
                return
            
            ticker_data = data['data']
            symbol = ticker_data.get('symbol', '').replace('@', '')
            
            # Create ticker object
            ticker = Ticker(
                symbol=symbol,
                bid=float(ticker_data.get('bidPrice', 0)),
                ask=float(ticker_data.get('askPrice', 0)),
                bid_size=float(ticker_data.get('bidQty', 0)),
                ask_size=float(ticker_data.get('askQty', 0)),
                last=float(ticker_data.get('lastPrice', 0)),
                volume_24h=float(ticker_data.get('volume', 0)),
                quote_volume_24h=float(ticker_data.get('quoteVolume', 0)),
                open_24h=float(ticker_data.get('openPrice', 0)),
                high_24h=float(ticker_data.get('highPrice', 0)),
                low_24h=float(ticker_data.get('lowPrice', 0)),
                change_24h=float(ticker_data.get('priceChange', 0)),
                change_percent_24h=float(ticker_data.get('priceChangePercent', 0)),
                timestamp=datetime.now(timezone.utc)
            )
            
            # Update client ticker cache
            if hasattr(self.client, '_ticker_cache'):
                self.client._ticker_cache[symbol] = ticker
            
            # Call user callback
            if hasattr(self.client, 'ws_on_ticker'):
                await self.client.ws_on_ticker(ticker)
            
        except Exception as e:
            logger.error(f"Error handling ticker: {str(e)}")
    
    async def handle_orderbook(self, data: Dict[str, Any]):
        """Handle order book updates"""
        try:
            if 'data' not in data:
                return
            
            book_data = data['data']
            symbol = data.get('symbol', '').split('@')[0]
            
            # Create order book object
            orderbook = OrderBook(
                symbol=symbol,
                bids=[[float(p), float(s)] for p, s in book_data.get('bids', [])],
                asks=[[float(p), float(s)] for p, s in book_data.get('asks', [])],
                timestamp=datetime.now(timezone.utc)
            )
            
            # Update client order book cache
            if hasattr(self.client, '_orderbook_cache'):
                self.client._orderbook_cache[symbol] = orderbook
            
            # Call user callback
            if hasattr(self.client, 'ws_on_orderbook'):
                await self.client.ws_on_orderbook(orderbook)
            
        except Exception as e:
            logger.error(f"Error handling order book: {str(e)}")
    
    async def handle_trade(self, data: Dict[str, Any]):
        """Handle trade updates"""
        try:
            if 'data' not in data:
                return
            
            trades_data = data['data'].get('deals', [])
            symbol = data.get('symbol', '').split('@')[0]
            
            for trade_data in trades_data:
                # Create trade object
                trade = Trade(
                    id=str(trade_data.get('t', '')),
                    order_id=None,
                    symbol=symbol,
                    side=OrderSide.BUY if trade_data.get('S') == 1 else OrderSide.SELL,
                    price=float(trade_data.get('p', 0)),
                    size=float(trade_data.get('v', 0)),
                    fee=0,
                    fee_currency=None,
                    timestamp=datetime.fromtimestamp(trade_data.get('t', 0) / 1000, tz=timezone.utc),
                    is_maker=False
                )
                
                # Call user callback
                if hasattr(self.client, 'ws_on_trade'):
                    await self.client.ws_on_trade(trade)
            
        except Exception as e:
            logger.error(f"Error handling trade: {str(e)}")
    
    async def handle_kline(self, data: Dict[str, Any]):
        """Handle kline updates"""
        try:
            if 'data' not in data:
                return
            
            kline_data = data['data']
            symbol = data.get('symbol', '').split('@')[0]
            
            # Call user callback with raw kline data
            if hasattr(self.client, 'ws_on_kline'):
                await self.client.ws_on_kline({
                    'symbol': symbol,
                    'interval': kline_data.get('interval'),
                    'open': float(kline_data.get('o', 0)),
                    'high': float(kline_data.get('h', 0)),
                    'low': float(kline_data.get('l', 0)),
                    'close': float(kline_data.get('c', 0)),
                    'volume': float(kline_data.get('v', 0)),
                    'timestamp': datetime.fromtimestamp(kline_data.get('t', 0) / 1000, tz=timezone.utc)
                })
            
        except Exception as e:
            logger.error(f"Error handling kline: {str(e)}")
    
    async def handle_user_data(self, data: Dict[str, Any]):
        """Handle user data updates"""
        try:
            event_type = data.get('e')
            
            if event_type == 'ACCOUNT_UPDATE':
                await self._handle_account_update(data)
            elif event_type == 'ORDER_UPDATE':
                await self._handle_order_update(data)
            elif event_type == 'TRADE_UPDATE':
                await self._handle_trade_update(data)
            
        except Exception as e:
            logger.error(f"Error handling user data: {str(e)}")
    
    async def _handle_account_update(self, data: Dict[str, Any]):
        """Handle account updates"""
        try:
            # Update balances
            balances_data = data.get('data', {}).get('balances', [])
            
            for balance_data in balances_data:
                currency = balance_data.get('asset')
                
                # Update balance cache
                if hasattr(self.client, '_balance_cache'):
                    if currency not in self.client._balance_cache:
                        self.client._balance_cache[currency] = {}
                    
                    self.client._balance_cache[currency].update({
                        'free': float(balance_data.get('free', 0)),
                        'locked': float(balance_data.get('locked', 0))
                    })
            
            # Call user callback
            if hasattr(self.client, 'ws_on_balance_update'):
                await self.client.ws_on_balance_update(data)
            
        except Exception as e:
            logger.error(f"Error handling account update: {str(e)}")
    
    async def _handle_order_update(self, data: Dict[str, Any]):
        """Handle order updates"""
        try:
            order_data = data.get('data', {})
            
            # Create order object
            order = Order(
                id=str(order_data.get('orderId')),
                client_order_id=order_data.get('clientOrderId'),
                exchange='MEXC',
                symbol=order_data.get('symbol'),
                type=self._parse_order_type(order_data.get('type')),
                side=OrderSide.BUY if order_data.get('side') == 'BUY' else OrderSide.SELL,
                size=float(order_data.get('quantity', 0)),
                price=float(order_data.get('price', 0)) if order_data.get('price') else None,
                status=self._parse_order_status(order_data.get('status')),
                filled_size=float(order_data.get('executedQty', 0)),
                average_price=float(order_data.get('avgPrice', 0)) if order_data.get('avgPrice') else None,
                fee=0,
                fee_currency=None,
                timestamp=datetime.now(timezone.utc),
                raw_data=order_data
            )
            
            # Call user callback
            if hasattr(self.client, 'ws_on_order_update'):
                await self.client.ws_on_order_update(order)
            
        except Exception as e:
            logger.error(f"Error handling order update: {str(e)}")
    
    async def _handle_trade_update(self, data: Dict[str, Any]):
        """Handle trade execution updates"""
        try:
            trade_data = data.get('data', {})
            
            # Create trade object
            trade = Trade(
                id=str(trade_data.get('tradeId')),
                order_id=str(trade_data.get('orderId')),
                symbol=trade_data.get('symbol'),
                side=OrderSide.BUY if trade_data.get('side') == 'BUY' else OrderSide.SELL,
                price=float(trade_data.get('price', 0)),
                size=float(trade_data.get('quantity', 0)),
                fee=float(trade_data.get('fee', 0)),
                fee_currency=trade_data.get('feeCurrency'),
                timestamp=datetime.now(timezone.utc),
                is_maker=trade_data.get('isMaker', False)
            )
            
            # Call user callback
            if hasattr(self.client, 'ws_on_trade_update'):
                await self.client.ws_on_trade_update(trade)
            
        except Exception as e:
            logger.error(f"Error handling trade update: {str(e)}")
    
    # Futures handlers
    async def handle_futures_ticker(self, data: Dict[str, Any]):
        """Handle futures ticker updates"""
        try:
            ticker_data = data.get('data', {})
            
            # Create ticker object
            ticker = Ticker(
                symbol=ticker_data.get('symbol'),
                bid=float(ticker_data.get('bid1', 0)),
                ask=float(ticker_data.get('ask1', 0)),
                bid_size=0,  # Not provided
                ask_size=0,  # Not provided
                last=float(ticker_data.get('lastPrice', 0)),
                volume_24h=float(ticker_data.get('volume24', 0)),
                quote_volume_24h=float(ticker_data.get('turnover24', 0)),
                open_24h=float(ticker_data.get('open24', 0)),
                high_24h=float(ticker_data.get('high24', 0)),
                low_24h=float(ticker_data.get('low24', 0)),
                change_24h=float(ticker_data.get('change24', 0)),
                change_percent_24h=float(ticker_data.get('changePercent24', 0)),
                timestamp=datetime.now(timezone.utc)
            )
            
            # Update client ticker cache
            if hasattr(self.client, '_ticker_cache'):
                self.client._ticker_cache[ticker.symbol] = ticker
            
            # Call user callback
            if hasattr(self.client, 'ws_on_ticker'):
                await self.client.ws_on_ticker(ticker)
            
        except Exception as e:
            logger.error(f"Error handling futures ticker: {str(e)}")
    
    async def handle_futures_orderbook(self, data: Dict[str, Any]):
        """Handle futures order book updates"""
        try:
            book_data = data.get('data', {})
            
            # Create order book object
            orderbook = OrderBook(
                symbol=book_data.get('symbol'),
                bids=[[float(p), float(s)] for p, s in book_data.get('bids', [])],
                asks=[[float(p), float(s)] for p, s in book_data.get('asks', [])],
                timestamp=datetime.now(timezone.utc)
            )
            
            # Update client order book cache
            if hasattr(self.client, '_orderbook_cache'):
                self.client._orderbook_cache[orderbook.symbol] = orderbook
            
            # Call user callback
            if hasattr(self.client, 'ws_on_orderbook'):
                await self.client.ws_on_orderbook(orderbook)
            
        except Exception as e:
            logger.error(f"Error handling futures order book: {str(e)}")
    
    async def handle_futures_trade(self, data: Dict[str, Any]):
        """Handle futures trade updates"""
        try:
            trades_data = data.get('data', [])
            
            for trade_data in trades_data:
                # Create trade object
                trade = Trade(
                    id=str(trade_data.get('t', '')),
                    order_id=None,
                    symbol=trade_data.get('symbol'),
                    side=OrderSide.BUY if trade_data.get('side') == 1 else OrderSide.SELL,
                    price=float(trade_data.get('price', 0)),
                    size=float(trade_data.get('volume', 0)),
                    fee=0,
                    fee_currency=None,
                    timestamp=datetime.fromtimestamp(trade_data.get('ts', 0) / 1000, tz=timezone.utc),
                    is_maker=False
                )
                
                # Call user callback
                if hasattr(self.client, 'ws_on_trade'):
                    await self.client.ws_on_trade(trade)
            
        except Exception as e:
            logger.error(f"Error handling futures trade: {str(e)}")
    
    async def handle_futures_user_data(self, data: Dict[str, Any]):
        """Handle futures user data updates"""
        try:
            channel = data.get('channel', '')
            
            if 'asset' in channel:
                await self._handle_futures_balance_update(data)
            elif 'order' in channel:
                await self._handle_futures_order_update(data)
            elif 'position' in channel:
                await self._handle_futures_position_update(data)
            
        except Exception as e:
            logger.error(f"Error handling futures user data: {str(e)}")
    
    async def _handle_futures_balance_update(self, data: Dict[str, Any]):
        """Handle futures balance updates"""
        try:
            balance_data = data.get('data', {})
            
            # Update balance cache
            if hasattr(self.client, '_balance_cache'):
                currency = balance_data.get('currency', 'USDT')
                
                if currency not in self.client._balance_cache:
                    self.client._balance_cache[currency] = {}
                
                self.client._balance_cache[currency].update({
                    'free': float(balance_data.get('availableBalance', 0)),
                    'locked': float(balance_data.get('frozenBalance', 0)),
                    'total': float(balance_data.get('balance', 0))
                })
            
            # Call user callback
            if hasattr(self.client, 'ws_on_balance_update'):
                await self.client.ws_on_balance_update(data)
            
        except Exception as e:
            logger.error(f"Error handling futures balance update: {str(e)}")
    
    async def _handle_futures_order_update(self, data: Dict[str, Any]):
        """Handle futures order updates"""
        try:
            order_data = data.get('data', {})
            
            # Map side values
            side_map = {1: 'BUY', 2: 'SELL', 3: 'SELL', 4: 'BUY'}
            side = side_map.get(order_data.get('side'), 'BUY')
            
            # Create order object
            order = Order(
                id=str(order_data.get('orderId')),
                client_order_id=order_data.get('externalOid'),
                exchange='MEXC',
                symbol=order_data.get('symbol'),
                type=self._parse_futures_order_type(order_data.get('orderType')),
                side=OrderSide.BUY if side == 'BUY' else OrderSide.SELL,
                size=float(order_data.get('volume', 0)),
                price=float(order_data.get('price', 0)) if order_data.get('price') else None,
                status=self._parse_futures_order_status(order_data.get('state')),
                filled_size=float(order_data.get('dealVolume', 0)),
                average_price=float(order_data.get('dealAvgPrice', 0)) if order_data.get('dealAvgPrice') else None,
                fee=float(order_data.get('fee', 0)),
                fee_currency='USDT',
                timestamp=datetime.fromtimestamp(order_data.get('createTime', 0) / 1000, tz=timezone.utc),
                raw_data=order_data
            )
            
            # Call user callback
            if hasattr(self.client, 'ws_on_order_update'):
                await self.client.ws_on_order_update(order)
            
        except Exception as e:
            logger.error(f"Error handling futures order update: {str(e)}")
    
    async def _handle_futures_position_update(self, data: Dict[str, Any]):
        """Handle futures position updates"""
        try:
            position_data = data.get('data', {})
            
            # Create position object
            position = Position(
                symbol=position_data.get('symbol'),
                side=PositionSide.LONG if position_data.get('positionType') == 1 else PositionSide.SHORT,
                size=float(position_data.get('volume', 0)),
                entry_price=float(position_data.get('openAvgPrice', 0)),
                mark_price=float(position_data.get('markPrice', 0)),
                unrealized_pnl=float(position_data.get('unrealisedPnl', 0)),
                realized_pnl=float(position_data.get('realisedPnl', 0)),
                margin=float(position_data.get('im', 0)),
                leverage=float(position_data.get('leverage', 1)),
                liquidation_price=float(position_data.get('liquidationPrice', 0)),
                exchange='MEXC',
                raw_data=position_data
            )
            
            # Update position cache
            if hasattr(self.client, '_position_cache'):
                self.client._position_cache[position.symbol] = position
            
            # Call user callback
            if hasattr(self.client, 'ws_on_position_update'):
                await self.client.ws_on_position_update(position)
            
        except Exception as e:
            logger.error(f"Error handling futures position update: {str(e)}")
    
    # Helper methods
    def _parse_order_type(self, type_str: str) -> OrderType:
        """Parse order type"""
        type_map = {
            'MARKET': OrderType.MARKET,
            'LIMIT': OrderType.LIMIT,
            'STOP': OrderType.STOP,
            'STOP_LIMIT': OrderType.STOP_LIMIT
        }
        return type_map.get(type_str.upper(), OrderType.LIMIT)
    
    def _parse_order_status(self, status: str) -> OrderStatus:
        """Parse order status"""
        status_map = {
            'NEW': OrderStatus.OPEN,
            'PARTIALLY_FILLED': OrderStatus.PARTIALLY_FILLED,
            'FILLED': OrderStatus.FILLED,
            'CANCELED': OrderStatus.CANCELLED,
            'REJECTED': OrderStatus.REJECTED,
            'EXPIRED': OrderStatus.EXPIRED
        }
        return status_map.get(status.upper(), OrderStatus.OPEN)
    
    def _parse_futures_order_type(self, type_code: int) -> OrderType:
        """Parse futures order type"""
        type_map = {
            1: OrderType.LIMIT,
            2: OrderType.IOC,
            3: OrderType.STOP,
            4: OrderType.STOP_LIMIT,
            5: OrderType.MARKET,
            6: OrderType.FOK
        }
        return type_map.get(type_code, OrderType.LIMIT)
    
    def _parse_futures_order_status(self, status_code: int) -> OrderStatus:
        """Parse futures order status"""
        status_map = {
            1: OrderStatus.OPEN,  # Not triggered
            2: OrderStatus.OPEN,  # New
            3: OrderStatus.OPEN,  # Partially filled
            4: OrderStatus.FILLED,
            5: OrderStatus.CANCELLED,
            6: OrderStatus.OPEN,  # Partially cancelled
            7: OrderStatus.CANCELLED  # Cancelled by system
        }
        return status_map.get(status_code, OrderStatus.OPEN)