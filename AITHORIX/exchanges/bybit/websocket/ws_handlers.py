"""
AITHORIX Bybit WebSocket Handlers
Process WebSocket messages from Bybit
"""

import logging
from datetime import datetime, timezone
from typing import Dict, Any

from ...base_exchange import (
    Ticker, OrderBook, Trade, Order, Position,
    OrderSide, OrderStatus, OrderType, PositionSide
)

logger = logging.getLogger(__name__)


class BybitWebSocketHandlers:
    """
    Bybit WebSocket message handlers
    """
    
    def __init__(self, client):
        self.client = client
    
    async def handle_ticker(self, data: Dict[str, Any]):
        """Handle ticker updates"""
        try:
            topic = data.get('topic', '')
            ticker_data = data.get('data', {})
            
            # Extract symbol from topic
            symbol = topic.split('.')[-1]
            
            # Create ticker object
            ticker = Ticker(
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
            topic = data.get('topic', '')
            book_data = data.get('data', {})
            
            # Extract symbol from topic
            parts = topic.split('.')
            symbol = parts[-1]
            
            # Determine update type
            update_type = data.get('type', 'snapshot')
            
            if update_type == 'snapshot':
                # Full order book snapshot
                orderbook = OrderBook(
                    symbol=symbol,
                    bids=[[float(p), float(s)] for p, s in book_data.get('b', [])],
                    asks=[[float(p), float(s)] for p, s in book_data.get('a', [])],
                    timestamp=datetime.fromtimestamp(int(book_data.get('ts', 0)) / 1000, tz=timezone.utc)
                )
                
                # Update client order book cache
                if hasattr(self.client, '_orderbook_cache'):
                    self.client._orderbook_cache[symbol] = orderbook
            else:
                # Delta update
                if hasattr(self.client, '_orderbook_cache') and symbol in self.client._orderbook_cache:
                    orderbook = self.client._orderbook_cache[symbol]
                    
                    # Apply bid updates
                    for bid in book_data.get('b', []):
                        price = float(bid[0])
                        size = float(bid[1])
                        
                        if size == 0:
                            # Remove level
                            orderbook.bids = [b for b in orderbook.bids if b[0] != price]
                        else:
                            # Update or add level
                            found = False
                            for i, b in enumerate(orderbook.bids):
                                if b[0] == price:
                                    orderbook.bids[i] = [price, size]
                                    found = True
                                    break
                            if not found:
                                orderbook.bids.append([price, size])
                    
                    # Apply ask updates
                    for ask in book_data.get('a', []):
                        price = float(ask[0])
                        size = float(ask[1])
                        
                        if size == 0:
                            # Remove level
                            orderbook.asks = [a for a in orderbook.asks if a[0] != price]
                        else:
                            # Update or add level
                            found = False
                            for i, a in enumerate(orderbook.asks):
                                if a[0] == price:
                                    orderbook.asks[i] = [price, size]
                                    found = True
                                    break
                            if not found:
                                orderbook.asks.append([price, size])
                    
                    # Sort order book
                    orderbook.bids.sort(key=lambda x: x[0], reverse=True)
                    orderbook.asks.sort(key=lambda x: x[0])
                    
                    # Update timestamp
                    orderbook.timestamp = datetime.fromtimestamp(int(book_data.get('ts', 0)) / 1000, tz=timezone.utc)
                else:
                    return
            
            # Call user callback
            if hasattr(self.client, 'ws_on_orderbook'):
                await self.client.ws_on_orderbook(orderbook)
            
        except Exception as e:
            logger.error(f"Error handling order book: {str(e)}")
    
    async def handle_trade(self, data: Dict[str, Any]):
        """Handle trade updates"""
        try:
            topic = data.get('topic', '')
            trades_data = data.get('data', [])
            
            # Extract symbol from topic
            symbol = topic.split('.')[-1]
            
            for trade_data in trades_data:
                # Create trade object
                trade = Trade(
                    id=trade_data.get('i', ''),
                    order_id=None,
                    symbol=symbol,
                    side=OrderSide.BUY if trade_data.get('S') == 'Buy' else OrderSide.SELL,
                    price=float(trade_data.get('p', 0)),
                    size=float(trade_data.get('v', 0)),
                    fee=0,
                    fee_currency=None,
                    timestamp=datetime.fromtimestamp(int(trade_data.get('T', 0)) / 1000, tz=timezone.utc),
                    is_maker=trade_data.get('BT', False)
                )
                
                # Call user callback
                if hasattr(self.client, 'ws_on_trade'):
                    await self.client.ws_on_trade(trade)
            
        except Exception as e:
            logger.error(f"Error handling trade: {str(e)}")
    
    async def handle_kline(self, data: Dict[str, Any]):
        """Handle kline updates"""
        try:
            topic = data.get('topic', '')
            kline_data = data.get('data', [])
            
            # Extract symbol and interval from topic
            parts = topic.split('.')
            interval = parts[1]
            symbol = parts[2]
            
            for candle_data in kline_data:
                # Call user callback with raw kline data
                if hasattr(self.client, 'ws_on_kline'):
                    await self.client.ws_on_kline({
                        'symbol': symbol,
                        'interval': interval,
                        'start': candle_data.get('start'),
                        'end': candle_data.get('end'),
                        'open': float(candle_data.get('open', 0)),
                        'high': float(candle_data.get('high', 0)),
                        'low': float(candle_data.get('low', 0)),
                        'close': float(candle_data.get('close', 0)),
                        'volume': float(candle_data.get('volume', 0)),
                        'turnover': float(candle_data.get('turnover', 0)),
                        'confirm': candle_data.get('confirm', False),
                        'timestamp': datetime.fromtimestamp(int(candle_data.get('timestamp', 0)) / 1000, tz=timezone.utc)
                    })
            
        except Exception as e:
            logger.error(f"Error handling kline: {str(e)}")
    
    async def handle_liquidation(self, data: Dict[str, Any]):
        """Handle liquidation updates"""
        try:
            topic = data.get('topic', '')
            liq_data = data.get('data', {})
            
            # Extract symbol from topic
            symbol = topic.split('.')[-1]
            
            # Call user callback with liquidation data
            if hasattr(self.client, 'ws_on_liquidation'):
                await self.client.ws_on_liquidation({
                    'symbol': symbol,
                    'side': liq_data.get('side'),
                    'price': float(liq_data.get('price', 0)),
                    'size': float(liq_data.get('size', 0)),
                    'timestamp': datetime.fromtimestamp(int(liq_data.get('updatedTime', 0)) / 1000, tz=timezone.utc)
                })
            
        except Exception as e:
            logger.error(f"Error handling liquidation: {str(e)}")
    
    # Private data handlers
    async def handle_position(self, data: Dict[str, Any]):
        """Handle position updates"""
        try:
            positions_data = data.get('data', [])
            
            for pos_data in positions_data:
                # Create position object
                position = Position(
                    symbol=pos_data.get('symbol'),
                    side=PositionSide.LONG if pos_data.get('side') == 'Buy' else PositionSide.SHORT,
                    size=abs(float(pos_data.get('size', 0))),
                    entry_price=float(pos_data.get('avgPrice', 0)),
                    mark_price=float(pos_data.get('markPrice', 0)),
                    unrealized_pnl=float(pos_data.get('unrealisedPnl', 0)),
                    realized_pnl=float(pos_data.get('cumRealisedPnl', 0)),
                    margin=float(pos_data.get('positionIM', 0)),
                    leverage=float(pos_data.get('leverage', 1)),
                    liquidation_price=float(pos_data.get('liqPrice', 0)),
                    exchange='Bybit',
                    raw_data=pos_data
                )
                
                # Update position cache
                if hasattr(self.client, '_position_cache'):
                    self.client._position_cache[position.symbol] = position
                
                # Call user callback
                if hasattr(self.client, 'ws_on_position_update'):
                    await self.client.ws_on_position_update(position)
            
        except Exception as e:
            logger.error(f"Error handling position update: {str(e)}")
    
    async def handle_execution(self, data: Dict[str, Any]):
        """Handle execution (trade) updates"""
        try:
            executions_data = data.get('data', [])
            
            for exec_data in executions_data:
                # Create trade object
                trade = Trade(
                    id=exec_data.get('execId'),
                    order_id=exec_data.get('orderId'),
                    symbol=exec_data.get('symbol'),
                    side=OrderSide.BUY if exec_data.get('side') == 'Buy' else OrderSide.SELL,
                    price=float(exec_data.get('execPrice', 0)),
                    size=float(exec_data.get('execQty', 0)),
                    fee=float(exec_data.get('execFee', 0)),
                    fee_currency=exec_data.get('feeCurrency'),
                    timestamp=datetime.fromtimestamp(int(exec_data.get('execTime', 0)) / 1000, tz=timezone.utc),
                    is_maker=exec_data.get('isMaker', False)
                )
                
                # Call user callback
                if hasattr(self.client, 'ws_on_trade_update'):
                    await self.client.ws_on_trade_update(trade)
            
        except Exception as e:
            logger.error(f"Error handling execution update: {str(e)}")
    
    async def handle_order(self, data: Dict[str, Any]):
        """Handle order updates"""
        try:
            orders_data = data.get('data', [])
            
            for order_data in orders_data:
                # Create order object
                order = Order(
                    id=order_data.get('orderId'),
                    client_order_id=order_data.get('orderLinkId'),
                    exchange='Bybit',
                    symbol=order_data.get('symbol'),
                    type=self._parse_order_type(order_data.get('orderType'), order_data.get('stopOrderType')),
                    side=OrderSide.BUY if order_data.get('side') == 'Buy' else OrderSide.SELL,
                    size=float(order_data.get('qty', 0)),
                    price=float(order_data.get('price', 0)) if order_data.get('price') else None,
                    status=self._parse_order_status(order_data.get('orderStatus')),
                    filled_size=float(order_data.get('cumExecQty', 0)),
                    average_price=float(order_data.get('avgPrice', 0)) if order_data.get('avgPrice') else None,
                    fee=float(order_data.get('cumExecFee', 0)),
                    fee_currency=order_data.get('feeCurrency'),
                    timestamp=datetime.fromtimestamp(int(order_data.get('createdTime', 0)) / 1000, tz=timezone.utc),
                    raw_data=order_data
                )
                
                # Call user callback
                if hasattr(self.client, 'ws_on_order_update'):
                    await self.client.ws_on_order_update(order)
            
        except Exception as e:
            logger.error(f"Error handling order update: {str(e)}")
    
    async def handle_wallet(self, data: Dict[str, Any]):
        """Handle wallet balance updates"""
        try:
            wallet_data = data.get('data', [])
            
            for balance_data in wallet_data:
                # Update balance cache
                if hasattr(self.client, '_balance_cache'):
                    for coin_data in balance_data.get('coin', []):
                        currency = coin_data.get('coin')
                        
                        if currency not in self.client._balance_cache:
                            self.client._balance_cache[currency] = {}
                        
                        self.client._balance_cache[currency].update({
                            'free': float(coin_data.get('free', 0)),
                            'locked': float(coin_data.get('locked', 0)),
                            'total': float(coin_data.get('walletBalance', 0))
                        })
                
                # Call user callback
                if hasattr(self.client, 'ws_on_balance_update'):
                    await self.client.ws_on_balance_update(balance_data)
            
        except Exception as e:
            logger.error(f"Error handling wallet update: {str(e)}")
    
    # Helper methods
    def _parse_order_type(self, type_str: str, stop_type: Optional[str] = None) -> OrderType:
        """Parse order type"""
        if stop_type:
            if type_str == 'Market':
                return OrderType.STOP
            else:
                return OrderType.STOP_LIMIT
        
        type_map = {
            'Market': OrderType.MARKET,
            'Limit': OrderType.LIMIT
        }
        return type_map.get(type_str, OrderType.LIMIT)
    
    def _parse_order_status(self, status: str) -> OrderStatus:
        """Parse order status"""
        status_map = {
            'Created': OrderStatus.OPEN,
            'New': OrderStatus.OPEN,
            'PartiallyFilled': OrderStatus.PARTIALLY_FILLED,
            'Filled': OrderStatus.FILLED,
            'Cancelled': OrderStatus.CANCELLED,
            'PartiallyFilledCanceled': OrderStatus.CANCELLED,
            'Rejected': OrderStatus.REJECTED,
            'Triggered': OrderStatus.OPEN,
            'Active': OrderStatus.OPEN
        }
        return status_map.get(status, OrderStatus.OPEN)