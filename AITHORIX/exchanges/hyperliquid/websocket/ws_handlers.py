"""
AITHORIX Hyperliquid WebSocket Handlers
Processes WebSocket messages from Hyperliquid
"""

import logging
from datetime import datetime, timezone
from typing import Dict, Any, Optional

from ...base_exchange import OrderSide, OrderStatus

logger = logging.getLogger(__name__)


class HyperliquidWebSocketHandlers:
    """
    Handlers for different types of Hyperliquid WebSocket messages
    """
    
    def __init__(self, client):
        self.client = client
    
    async def handle_message(self, data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Handle WebSocket message based on type
        """
        try:
            channel = data.get('channel')
            
            if channel == 'l2Book':
                return self._handle_orderbook(data)
            elif channel == 'trades':
                return self._handle_trades(data)
            elif channel == 'candle':
                return self._handle_candle(data)
            elif channel == 'userEvents':
                return self._handle_user_events(data)
            elif channel == 'activeAssetCtx':
                return self._handle_asset_context(data)
            elif channel == 'notification':
                return self._handle_notification(data)
            else:
                logger.debug(f"Unknown channel: {channel}")
                return None
                
        except Exception as e:
            logger.error(f"Error handling message: {str(e)}")
            return None
    
    def _handle_orderbook(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Handle order book update"""
        book_data = data.get('data', {})
        coin = book_data.get('coin', '')
        
        levels = book_data.get('levels', [])
        bids = []
        asks = []
        
        if len(levels) >= 2:
            # First array is bids, second is asks
            for bid in levels[0]:
                bids.append((float(bid['px']), float(bid['sz'])))
            
            for ask in levels[1]:
                asks.append((float(ask['px']), float(ask['sz'])))
        
        return {
            'type': 'orderbook',
            'symbol': coin,
            'bids': bids,
            'asks': asks,
            'timestamp': datetime.fromtimestamp(book_data.get('time', 0) / 1000, tz=timezone.utc)
        }
    
    def _handle_trades(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Handle trades update"""
        trades_data = data.get('data', [])
        
        trades = []
        for trade in trades_data:
            trades.append({
                'trade_id': str(trade.get('tid', '')),
                'price': float(trade['px']),
                'size': float(trade['sz']),
                'side': 'buy' if trade['side'] == 'B' else 'sell',
                'timestamp': datetime.fromtimestamp(trade['time'] / 1000, tz=timezone.utc)
            })
        
        if trades:
            return {
                'type': 'trades',
                'symbol': data.get('data', [{}])[0].get('coin', '') if data.get('data') else '',
                'trades': trades
            }
        
        return None
    
    def _handle_candle(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Handle candle update"""
        candle_data = data.get('data', {})
        
        return {
            'type': 'candle',
            'symbol': candle_data.get('coin', ''),
            'interval': candle_data.get('interval', ''),
            'open': float(candle_data.get('o', 0)),
            'high': float(candle_data.get('h', 0)),
            'low': float(candle_data.get('l', 0)),
            'close': float(candle_data.get('c', 0)),
            'volume': float(candle_data.get('v', 0)),
            'timestamp': datetime.fromtimestamp(candle_data.get('t', 0) / 1000, tz=timezone.utc)
        }
    
    def _handle_user_events(self, data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Handle user events (orders, fills, liquidations)"""
        events = data.get('data', [])
        
        for event in events:
            if 'fills' in event:
                return self._handle_fills(event['fills'])
            elif 'order' in event:
                return self._handle_order_update(event['order'])
            elif 'liquidation' in event:
                return self._handle_liquidation(event['liquidation'])
        
        return None
    
    def _handle_fills(self, fills: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Handle fill events"""
        fill_list = []
        
        for fill in fills:
            fill_list.append({
                'order_id': str(fill['oid']),
                'trade_id': str(fill.get('tid', '')),
                'symbol': fill['coin'],
                'side': 'buy' if fill['side'] == 'B' else 'sell',
                'price': float(fill['px']),
                'size': float(fill['sz']),
                'fee': float(fill.get('fee', 0)),
                'timestamp': datetime.fromtimestamp(fill['time'] / 1000, tz=timezone.utc)
            })
        
        return {
            'type': 'fills',
            'fills': fill_list
        }
    
    def _handle_order_update(self, order: Dict[str, Any]) -> Dict[str, Any]:
        """Handle order update"""
        return {
            'type': 'order_update',
            'order_id': str(order['oid']),
            'symbol': order['coin'],
            'side': 'buy' if order['side'] == 'B' else 'sell',
            'price': float(order['limitPx']),
            'size': float(order['sz']),
            'filled_size': float(order.get('filledSz', 0)),
            'status': self._convert_order_status(order.get('orderStatus', 'open')),
            'timestamp': datetime.fromtimestamp(order.get('timestamp', 0) / 1000, tz=timezone.utc)
        }
    
    def _handle_liquidation(self, liquidation: Dict[str, Any]) -> Dict[str, Any]:
        """Handle liquidation event"""
        return {
            'type': 'liquidation',
            'symbol': liquidation['coin'],
            'side': liquidation['side'],
            'price': float(liquidation['px']),
            'size': float(liquidation['sz']),
            'timestamp': datetime.fromtimestamp(liquidation['time'] / 1000, tz=timezone.utc)
        }
    
    def _handle_asset_context(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Handle asset context update (funding rates, etc.)"""
        ctx_data = data.get('data', {})
        
        return {
            'type': 'asset_context',
            'symbol': ctx_data.get('coin', ''),
            'funding_rate': float(ctx_data.get('funding', 0)),
            'open_interest': float(ctx_data.get('openInterest', 0)),
            'mark_price': float(ctx_data.get('markPx', 0)),
            'oracle_price': float(ctx_data.get('oraclePx', 0)),
            'timestamp': datetime.now(timezone.utc)
        }
    
    def _handle_notification(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Handle notification messages"""
        return {
            'type': 'notification',
            'message': data.get('data', {}).get('notification', ''),
            'timestamp': datetime.now(timezone.utc)
        }
    
    def _convert_order_status(self, status: str) -> str:
        """Convert Hyperliquid order status to internal format"""
        status_map = {
            'open': 'open',
            'filled': 'filled',
            'canceled': 'cancelled',
            'cancelled': 'cancelled',
            'rejected': 'rejected',
            'partial': 'partially_filled'
        }
        return status_map.get(status.lower(), status.lower())