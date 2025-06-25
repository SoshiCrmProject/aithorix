"""
AITHORIX Binance WebSocket Handlers
Processes WebSocket messages from Binance
"""

import logging
from datetime import datetime, timezone
from typing import Dict, Any, Optional

from ...base_exchange import OrderSide, OrderStatus

logger = logging.getLogger(__name__)


class BinanceWebSocketHandlers:
    """
    Handlers for different types of Binance WebSocket messages
    """
    
    def __init__(self, client):
        self.client = client
    
    async def handle_stream_data(self, stream_name: str, data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Handle stream data based on stream type
        """
        try:
            # Parse stream type
            parts = stream_name.split('@')
            if len(parts) < 2:
                return None
            
            symbol = parts[0].upper()
            stream_type = parts[1]
            
            # Route to appropriate handler
            if stream_type == 'ticker':
                return self._handle_ticker(symbol, data)
            elif stream_type.startswith('depth'):
                return self._handle_orderbook(symbol, data)
            elif stream_type == 'trade':
                return self._handle_trade(symbol, data)
            elif stream_type.startswith('kline'):
                return self._handle_kline(symbol, data)
            else:
                logger.warning(f"Unknown stream type: {stream_type}")
                return None
                
        except Exception as e:
            logger.error(f"Error handling stream data: {str(e)}")
            return None
    
    async def handle_user_data(self, data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Handle user data stream messages
        """
        try:
            event_type = data.get('e')
            
            if event_type == 'executionReport':
                return self._handle_order_update(data)
            elif event_type == 'outboundAccountPosition':
                return self._handle_account_update(data)
            elif event_type == 'balanceUpdate':
                return self._handle_balance_update(data)
            elif event_type == 'ACCOUNT_UPDATE':  # Futures
                return self._handle_futures_account_update(data)
            elif event_type == 'ORDER_TRADE_UPDATE':  # Futures
                return self._handle_futures_order_update(data)
            else:
                logger.debug(f"Unhandled user event type: {event_type}")
                return None
                
        except Exception as e:
            logger.error(f"Error handling user data: {str(e)}")
            return None
    
    def _handle_ticker(self, symbol: str, data: Dict[str, Any]) -> Dict[str, Any]:
        """Handle ticker update"""
        return {
            'type': 'ticker',
            'symbol': self.client._denormalize_symbol(symbol),
            'bid': float(data['b']),
            'ask': float(data['a']),
            'last': float(data['c']),
            'volume': float(data['v']),
            'quote_volume': float(data['q']),
            'change_percent': float(data['P']),
            'timestamp': datetime.fromtimestamp(data['E'] / 1000, tz=timezone.utc)
        }
    
    def _handle_orderbook(self, symbol: str, data: Dict[str, Any]) -> Dict[str, Any]:
        """Handle order book update"""
        return {
            'type': 'orderbook',
            'symbol': self.client._denormalize_symbol(symbol),
            'bids': [(float(price), float(size)) for price, size in data.get('bids', [])],
            'asks': [(float(price), float(size)) for price, size in data.get('asks', [])],
            'timestamp': datetime.fromtimestamp(data.get('E', 0) / 1000, tz=timezone.utc) if 'E' in data else datetime.now(timezone.utc)
        }
    
    def _handle_trade(self, symbol: str, data: Dict[str, Any]) -> Dict[str, Any]:
        """Handle trade update"""
        return {
            'type': 'trade',
            'symbol': self.client._denormalize_symbol(symbol),
            'trade_id': str(data['t']),
            'price': float(data['p']),
            'size': float(data['q']),
            'side': 'sell' if data['m'] else 'buy',  # m = true means seller is maker
            'timestamp': datetime.fromtimestamp(data['T'] / 1000, tz=timezone.utc)
        }
    
    def _handle_kline(self, symbol: str, data: Dict[str, Any]) -> Dict[str, Any]:
        """Handle kline/candlestick update"""
        kline = data['k']
        return {
            'type': 'kline',
            'symbol': self.client._denormalize_symbol(symbol),
            'interval': kline['i'],
            'open': float(kline['o']),
            'high': float(kline['h']),
            'low': float(kline['l']),
            'close': float(kline['c']),
            'volume': float(kline['v']),
            'close_time': datetime.fromtimestamp(kline['T'] / 1000, tz=timezone.utc),
            'is_closed': kline['x']
        }
    
    def _handle_order_update(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Handle spot order update"""
        return {
            'type': 'order_update',
            'order_id': str(data['i']),
            'client_order_id': data['c'],
            'symbol': self.client._denormalize_symbol(data['s']),
            'side': data['S'].lower(),
            'order_type': data['o'].lower(),
            'status': self._convert_order_status(data['X']),
            'price': float(data['p']),
            'size': float(data['q']),
            'filled_size': float(data['z']),
            'avg_price': float(data['Z']) / float(data['z']) if float(data['z']) > 0 else 0,
            'timestamp': datetime.fromtimestamp(data['T'] / 1000, tz=timezone.utc)
        }
    
    def _handle_account_update(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Handle spot account update"""
        balances = {}
        for balance in data.get('B', []):
            asset = balance['a']
            free = float(balance['f'])
            locked = float(balance['l'])
            if free > 0 or locked > 0:
                balances[asset] = {
                    'free': free,
                    'locked': locked,
                    'total': free + locked
                }
        
        return {
            'type': 'account_update',
            'balances': balances,
            'timestamp': datetime.fromtimestamp(data['E'] / 1000, tz=timezone.utc)
        }
    
    def _handle_balance_update(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Handle balance update"""
        return {
            'type': 'balance_update',
            'asset': data['a'],
            'delta': float(data['d']),
            'timestamp': datetime.fromtimestamp(data['E'] / 1000, tz=timezone.utc)
        }
    
    def _handle_futures_account_update(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Handle futures account update"""
        account = data.get('a', {})
        
        # Extract positions
        positions = []
        for pos_data in account.get('P', []):
            if float(pos_data['pa']) != 0:  # Position amount
                positions.append({
                    'symbol': self.client._denormalize_symbol(pos_data['s']) + '_PERP',
                    'side': 'long' if float(pos_data['pa']) > 0 else 'short',
                    'size': abs(float(pos_data['pa'])),
                    'entry_price': float(pos_data['ep']),
                    'unrealized_pnl': float(pos_data['up']),
                    'margin_type': pos_data['mt'],
                    'isolated_margin': float(pos_data.get('iw', 0))
                })
        
        # Extract balances
        balances = {}
        for bal_data in account.get('B', []):
            asset = bal_data['a']
            wallet_balance = float(bal_data['wb'])
            cross_wallet = float(bal_data['cw'])
            if wallet_balance > 0:
                balances[f"{asset}_FUTURES"] = {
                    'total': wallet_balance,
                    'available': cross_wallet
                }
        
        return {
            'type': 'futures_account_update',
            'positions': positions,
            'balances': balances,
            'timestamp': datetime.fromtimestamp(data['E'] / 1000, tz=timezone.utc)
        }
    
    def _handle_futures_order_update(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Handle futures order update"""
        order = data.get('o', {})
        
        return {
            'type': 'order_update',
            'order_id': str(order['i']),
            'client_order_id': order['c'],
            'symbol': self.client._denormalize_symbol(order['s']) + '_PERP',
            'side': order['S'].lower(),
            'order_type': order['o'].lower(),
            'status': self._convert_order_status(order['X']),
            'price': float(order['p']),
            'size': float(order['q']),
            'filled_size': float(order['z']),
            'avg_price': float(order['ap']),
            'position_side': order.get('ps'),
            'reduce_only': order.get('R', False),
            'timestamp': datetime.fromtimestamp(order['T'] / 1000, tz=timezone.utc)
        }
    
    def _convert_order_status(self, status: str) -> str:
        """Convert Binance order status to internal format"""
        status_map = {
            'NEW': 'open',
            'PARTIALLY_FILLED': 'partially_filled',
            'FILLED': 'filled',
            'CANCELED': 'cancelled',
            'PENDING_CANCEL': 'pending_cancel',
            'REJECTED': 'rejected',
            'EXPIRED': 'expired'
        }
        return status_map.get(status, status.lower())