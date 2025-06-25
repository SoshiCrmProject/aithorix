"""
AITHORIX OKX Unified Trading Implementation
High-level unified trading functionality for OKX
"""

import logging
from datetime import datetime, timezone
from typing import Dict, List, Optional, Any
import asyncio
import uuid

from ...base_exchange import (
    Order, Trade, Position, Balance, OrderType, OrderSide,
    OrderStatus, TimeInForce, PositionSide
)
from .unified_api import OKXUnifiedAPI

logger = logging.getLogger(__name__)


class OKXUnifiedTrading:
    """
    OKX unified trading implementation
    """
    
    def __init__(self, client):
        self.client = client
        self.api = OKXUnifiedAPI(client)
        self._position_cache: Dict[str, Position] = {}
        self._balance_cache: Dict[str, Balance] = {}
        self._last_balance_update = 0
        
    async def place_order(
        self,
        symbol: str,
        side: str,
        order_type: OrderType,
        size: float,
        price: Optional[float] = None,
        params: Optional[Dict] = None
    ) -> Order:
        """Place an order"""
        try:
            params = params or {}
            
            # Get instrument info
            inst_info = self.client.instruments.get(symbol, {})
            inst_type = inst_info.get('instType', 'SPOT')
            
            # Generate client order ID if not provided
            client_order_id = params.get('client_order_id', str(uuid.uuid4())[:32])
            
            # Build order parameters
            order_params = {
                'instId': symbol,
                'tdMode': self._get_trade_mode(inst_type, params.get('margin_mode')),
                'side': side.lower(),
                'ordType': self._get_order_type(order_type),
                'sz': self._format_quantity(symbol, size),
                'clOrdId': client_order_id
            }
            
            # Add price for limit orders
            if order_type in [OrderType.LIMIT, OrderType.STOP_LIMIT, OrderType.POST_ONLY]:
                if not price:
                    raise ValueError("Price required for limit orders")
                order_params['px'] = self._format_price(symbol, price)
            
            # Position side (for hedge mode)
            if self.client.position_mode == 'long_short_mode':
                if params.get('position_side'):
                    order_params['posSide'] = params['position_side'].value.lower()
                else:
                    # Auto-determine position side
                    order_params['posSide'] = 'long' if side.upper() == 'BUY' else 'short'
            
            # Reduce only
            if params.get('reduce_only'):
                order_params['reduceOnly'] = True
            
            # Order tag
            if params.get('tag'):
                order_params['tag'] = params['tag']
            
            # Stop orders
            if order_type in [OrderType.STOP, OrderType.STOP_LIMIT]:
                order_params['ordType'] = 'conditional'
                order_params['triggerPx'] = params.get('stop_price', price)
                order_params['triggerPxType'] = params.get('trigger_type', 'last')
                
                # For stop limit, add actual order type
                if order_type == OrderType.STOP_LIMIT:
                    order_params['orderPx'] = price
                    order_params['orderType'] = 'limit'
                else:
                    order_params['orderType'] = 'market'
            
            # Take profit / Stop loss
            if params.get('take_profit'):
                order_params['tpTriggerPx'] = str(params['take_profit'])
                order_params['tpTriggerPxType'] = params.get('tp_trigger_type', 'last')
                order_params['tpOrdPx'] = params.get('tp_order_price', '-1')  # -1 for market
            
            if params.get('stop_loss'):
                order_params['slTriggerPx'] = str(params['stop_loss'])
                order_params['slTriggerPxType'] = params.get('sl_trigger_type', 'last')
                order_params['slOrdPx'] = params.get('sl_order_price', '-1')  # -1 for market
            
            # Place order
            response = await self.api.place_order(order_params)
            
            if response['code'] != '0':
                raise Exception(f"Order failed: {response['msg']}")
            
            order_data = response['data'][0]
            
            # Get order status
            status = OrderStatus.OPEN
            if order_data['sCode'] != '0':
                status = OrderStatus.REJECTED
            
            # Create order object
            return Order(
                id=order_data['ordId'],
                client_order_id=order_data['clOrdId'],
                exchange='OKX',
                symbol=symbol,
                type=order_type,
                side=OrderSide.BUY if side.upper() == 'BUY' else OrderSide.SELL,
                size=size,
                price=price,
                status=status,
                filled_size=0.0,
                average_price=None,
                fee=0.0,
                fee_currency=self._get_fee_currency(symbol),
                timestamp=datetime.now(timezone.utc),
                raw_data=order_data
            )
            
        except Exception as e:
            logger.error(f"Error placing order: {str(e)}")
            raise
    
    async def cancel_order(self, order_id: str, symbol: Optional[str] = None) -> bool:
        """Cancel an order"""
        try:
            if not symbol:
                raise ValueError("Symbol is required for OKX order cancellation")
            
            params = {
                'instId': symbol,
                'ordId': order_id
            }
            
            response = await self.api.cancel_order(params)
            
            if response['code'] == '0':
                return True
            
            logger.error(f"Failed to cancel order: {response['msg']}")
            return False
            
        except Exception as e:
            logger.error(f"Error canceling order: {str(e)}")
            return False
    
    async def get_order(self, order_id: str, symbol: Optional[str] = None) -> Optional[Order]:
        """Get order details"""
        try:
            if not symbol:
                raise ValueError("Symbol is required for OKX order query")
            
            params = {
                'instId': symbol,
                'ordId': order_id
            }
            
            response = await self.api.get_order(params)
            
            if response['code'] != '0' or not response['data']:
                return None
            
            order_data = response['data'][0]
            return self._parse_order(order_data)
            
        except Exception as e:
            logger.error(f"Error getting order: {str(e)}")
            return None
    
    async def get_open_orders(self, symbol: Optional[str] = None) -> List[Order]:
        """Get all open orders"""
        try:
            params = {}
            if symbol:
                params['instId'] = symbol
            
            response = await self.api.get_order_list(params)
            
            if response['code'] != '0':
                return []
            
            orders = []
            for order_data in response['data']:
                order = self._parse_order(order_data)
                if order:
                    orders.append(order)
            
            return orders
            
        except Exception as e:
            logger.error(f"Error getting open orders: {str(e)}")
            return []
    
    async def get_order_history(
        self,
        symbol: Optional[str] = None,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        limit: int = 100
    ) -> List[Order]:
        """Get order history"""
        try:
            params = {'limit': str(limit)}
            
            if symbol:
                params['instId'] = symbol
                
                # Determine instrument type
                inst_type = self.client.instruments.get(symbol, {}).get('instType', 'SPOT')
                params['instType'] = inst_type
            
            if start_time:
                params['after'] = str(int(start_time.timestamp() * 1000))
            if end_time:
                params['before'] = str(int(end_time.timestamp() * 1000))
            
            response = await self.api.get_orders_history(params)
            
            if response['code'] != '0':
                return []
            
            orders = []
            for order_data in response['data']:
                order = self._parse_order(order_data)
                if order:
                    orders.append(order)
            
            return orders
            
        except Exception as e:
            logger.error(f"Error getting order history: {str(e)}")
            return []
    
    async def get_balance(self) -> Dict[str, Balance]:
        """Get account balance"""
        try:
            # Cache balance for 1 second
            current_time = asyncio.get_event_loop().time()
            if current_time - self._last_balance_update < 1:
                return self._balance_cache
            
            params = {}
            response = await self.api.get_balance(params)
            
            if response['code'] != '0':
                return self._balance_cache
            
            balances = {}
            
            for account_data in response['data']:
                # Process each currency detail
                for detail in account_data['details']:
                    currency = detail['ccy']
                    
                    # Calculate available and used
                    available = float(detail.get('availBal', 0))
                    equity = float(detail.get('eq', 0))
                    used = equity - available
                    
                    balance = Balance(
                        currency=currency,
                        free=available,
                        used=used,
                        total=equity,
                        exchange='OKX'
                    )
                    
                    balances[currency] = balance
            
            self._balance_cache = balances
            self._last_balance_update = current_time
            
            return balances
            
        except Exception as e:
            logger.error(f"Error getting balance: {str(e)}")
            return self._balance_cache
    
    async def get_open_positions(self) -> List[Position]:
        """Get all open positions"""
        try:
            params = {}
            response = await self.api.get_positions(params)
            
            if response['code'] != '0':
                return []
            
            positions = []
            for pos_data in response['data']:
                if float(pos_data['pos']) != 0:
                    position = self._parse_position(pos_data)
                    if position:
                        positions.append(position)
                        self._position_cache[pos_data['instId']] = position
            
            return positions
            
        except Exception as e:
            logger.error(f"Error getting positions: {str(e)}")
            return []
    
    async def get_position(self, symbol: str) -> Optional[Position]:
        """Get specific position"""
        try:
            # Try cache first
            if symbol in self._position_cache:
                return self._position_cache[symbol]
            
            params = {'instId': symbol}
            response = await self.api.get_positions(params)
            
            if response['code'] != '0' or not response['data']:
                return None
            
            pos_data = response['data'][0]
            if float(pos_data['pos']) != 0:
                position = self._parse_position(pos_data)
                if position:
                    self._position_cache[symbol] = position
                return position
            
            return None
            
        except Exception as e:
            logger.error(f"Error getting position: {str(e)}")
            return None
    
    async def close_position(self, symbol: str, size: Optional[float] = None) -> Order:
        """Close a position"""
        try:
            position = await self.get_position(symbol)
            if not position:
                raise ValueError(f"No position found for {symbol}")
            
            # Determine close size
            close_size = size or position.size
            
            # Determine close side
            close_side = 'SELL' if position.side == PositionSide.LONG else 'BUY'
            
            # Use close position endpoint for full position close
            if not size:
                inst_info = self.client.instruments.get(symbol, {})
                params = {
                    'instId': symbol,
                    'mgnMode': self._get_margin_mode(position.raw_data.get('mgnMode', 'cross')),
                    'posSide': position.raw_data.get('posSide', 'net')
                }
                
                response = await self.api.close_position(params)
                
                if response['code'] != '0':
                    raise Exception(f"Failed to close position: {response['msg']}")
                
                # Return a dummy order
                return Order(
                    id=response['data'][0].get('ordId', str(uuid.uuid4())),
                    client_order_id=response['data'][0].get('clOrdId', ''),
                    exchange='OKX',
                    symbol=symbol,
                    type=OrderType.MARKET,
                    side=OrderSide.SELL if close_side == 'SELL' else OrderSide.BUY,
                    size=position.size,
                    price=None,
                    status=OrderStatus.OPEN,
                    filled_size=0.0,
                    average_price=None,
                    fee=0.0,
                    fee_currency=self._get_fee_currency(symbol),
                    timestamp=datetime.now(timezone.utc)
                )
            else:
                # Place regular order for partial close
                return await self.place_order(
                    symbol=symbol,
                    side=close_side,
                    order_type=OrderType.MARKET,
                    size=close_size,
                    params={'reduce_only': True}
                )
            
        except Exception as e:
            logger.error(f"Error closing position: {str(e)}")
            raise
    
    async def set_leverage(self, symbol: str, leverage: int, margin_mode: str = "cross") -> bool:
        """Set leverage for a symbol"""
        try:
            inst_info = self.client.instruments.get(symbol, {})
            
            params = {
                'instId': symbol,
                'lever': str(leverage),
                'mgnMode': margin_mode
            }
            
            # Add position side for hedge mode
            if self.client.position_mode == 'long_short_mode':
                # Set for both sides
                params['posSide'] = 'long'
                response1 = await self.api.set_leverage(params)
                
                params['posSide'] = 'short'
                response2 = await self.api.set_leverage(params)
                
                return response1['code'] == '0' and response2['code'] == '0'
            else:
                response = await self.api.set_leverage(params)
                return response['code'] == '0'
            
        except Exception as e:
            logger.error(f"Error setting leverage: {str(e)}")
            return False
    
    async def get_max_order_size(self, symbol: str, price: Optional[float] = None) -> Dict[str, float]:
        """Get maximum order size"""
        try:
            params = {
                'instId': symbol,
                'tdMode': 'cash'  # Will be adjusted based on instrument
            }
            
            # Add price for more accurate calculation
            if price:
                params['px'] = str(price)
            
            # Determine trade mode
            inst_info = self.client.instruments.get(symbol, {})
            if inst_info.get('instType') != 'SPOT':
                params['tdMode'] = 'cross'  # or 'isolated'
            
            response = await self.api.get_max_avail_size(params)
            
            if response['code'] != '0':
                return {'maxBuy': 0.0, 'maxSell': 0.0}
            
            data = response['data'][0]
            return {
                'maxBuy': float(data.get('availBuy', 0)),
                'maxSell': float(data.get('availSell', 0))
            }
            
        except Exception as e:
            logger.error(f"Error getting max order size: {str(e)}")
            return {'maxBuy': 0.0, 'maxSell': 0.0}
    
    async def get_trades(
        self,
        symbol: Optional[str] = None,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        limit: int = 100
    ) -> List[Trade]:
        """Get trade history"""
        try:
            params = {'limit': str(limit)}
            
            if symbol:
                params['instId'] = symbol
                inst_type = self.client.instruments.get(symbol, {}).get('instType', 'SPOT')
                params['instType'] = inst_type
            
            if start_time:
                params['after'] = str(int(start_time.timestamp() * 1000))
            if end_time:
                params['before'] = str(int(end_time.timestamp() * 1000))
            
            response = await self.api.get_fills(params)
            
            if response['code'] != '0':
                return []
            
            trades = []
            for trade_data in response['data']:
                trades.append(Trade(
                    id=trade_data['tradeId'],
                    order_id=trade_data['ordId'],
                    symbol=trade_data['instId'],
                    side=OrderSide.BUY if trade_data['side'] == 'buy' else OrderSide.SELL,
                    price=float(trade_data['fillPx']),
                    size=float(trade_data['fillSz']),
                    fee=abs(float(trade_data['fee'])),
                    fee_currency=trade_data['feeCcy'],
                    timestamp=datetime.fromtimestamp(int(trade_data['fillTime']) / 1000, tz=timezone.utc),
                    is_maker=trade_data.get('execType', 'T') == 'M'
                ))
            
            return trades
            
        except Exception as e:
            logger.error(f"Error getting trades: {str(e)}")
            return []
    
    # Helper methods
    def _parse_order(self, order_data: Dict[str, Any]) -> Optional[Order]:
        """Parse order data into Order object"""
        try:
            # Determine order type
            ord_type = order_data['ordType']
            if ord_type == 'conditional':
                # Check if it's stop or stop limit
                order_type = OrderType.STOP_LIMIT if order_data.get('px') else OrderType.STOP
            else:
                order_type = self._parse_order_type(ord_type)
            
            return Order(
                id=order_data['ordId'],
                client_order_id=order_data.get('clOrdId'),
                exchange='OKX',
                symbol=order_data['instId'],
                type=order_type,
                side=OrderSide.BUY if order_data['side'] == 'buy' else OrderSide.SELL,
                size=float(order_data['sz']),
                price=float(order_data['px']) if order_data['px'] else None,
                status=self._parse_order_status(order_data['state']),
                filled_size=float(order_data.get('fillSz', 0)),
                average_price=float(order_data['avgPx']) if order_data.get('avgPx') else None,
                fee=abs(float(order_data.get('fee', 0))),
                fee_currency=order_data.get('feeCcy', self._get_fee_currency(order_data['instId'])),
                timestamp=datetime.fromtimestamp(int(order_data['cTime']) / 1000, tz=timezone.utc),
                raw_data=order_data
            )
        except Exception as e:
            logger.error(f"Error parsing order: {str(e)}")
            return None
    
    def _parse_position(self, pos_data: Dict[str, Any]) -> Optional[Position]:
        """Parse position data into Position object"""
        try:
            # Determine position side
            pos_side = pos_data.get('posSide', 'net')
            if pos_side == 'net':
                # Net mode - side determined by position sign
                side = PositionSide.LONG if float(pos_data['pos']) > 0 else PositionSide.SHORT
            else:
                side = PositionSide.LONG if pos_side == 'long' else PositionSide.SHORT
            
            return Position(
                symbol=pos_data['instId'],
                side=side,
                size=abs(float(pos_data['pos'])),
                entry_price=float(pos_data['avgPx']) if pos_data['avgPx'] else 0,
                mark_price=float(pos_data['markPx']) if pos_data['markPx'] else 0,
                unrealized_pnl=float(pos_data['upl']) if pos_data['upl'] else 0,
                realized_pnl=float(pos_data.get('realizedPnl', 0)),
                margin=float(pos_data.get('margin', 0)),
                leverage=float(pos_data.get('lever', 1)),
                liquidation_price=float(pos_data['liqPx']) if pos_data.get('liqPx') else 0,
                exchange='OKX',
                raw_data=pos_data
            )
        except Exception as e:
            logger.error(f"Error parsing position: {str(e)}")
            return None
    
    def _get_order_type(self, order_type: OrderType) -> str:
        """Convert order type to OKX format"""
        type_map = {
            OrderType.MARKET: 'market',
            OrderType.LIMIT: 'limit',
            OrderType.STOP: 'market',  # With trigger
            OrderType.STOP_LIMIT: 'limit',  # With trigger
            OrderType.POST_ONLY: 'post_only',
            OrderType.FOK: 'fok',
            OrderType.IOC: 'ioc'
        }
        return type_map.get(order_type, 'limit')
    
    def _parse_order_type(self, type_str: str) -> OrderType:
        """Parse OKX order type"""
        type_map = {
            'market': OrderType.MARKET,
            'limit': OrderType.LIMIT,
            'post_only': OrderType.POST_ONLY,
            'fok': OrderType.FOK,
            'ioc': OrderType.IOC,
            'optimal_limit_ioc': OrderType.IOC
        }
        return type_map.get(type_str, OrderType.LIMIT)
    
    def _parse_order_status(self, status: str) -> OrderStatus:
        """Parse OKX order status"""
        status_map = {
            'live': OrderStatus.OPEN,
            'partially_filled': OrderStatus.PARTIALLY_FILLED,
            'filled': OrderStatus.FILLED,
            'canceled': OrderStatus.CANCELLED,
            'cancelled': OrderStatus.CANCELLED,  # Alternative spelling
            'mmp_canceled': OrderStatus.CANCELLED,
            'expired': OrderStatus.EXPIRED
        }
        return status_map.get(status, OrderStatus.OPEN)
    
    def _get_trade_mode(self, inst_type: str, margin_mode: Optional[str] = None) -> str:
        """Get trade mode based on instrument type"""
        if inst_type == 'SPOT':
            return 'cash'
        elif inst_type == 'MARGIN':
            return margin_mode or 'cross'
        else:  # SWAP, FUTURES, OPTION
            return margin_mode or 'cross'
    
    def _get_margin_mode(self, mode_str: str) -> str:
        """Convert margin mode string"""
        if mode_str in ['cross', 'isolated']:
            return mode_str
        return 'cross'
    
    def _get_fee_currency(self, symbol: str) -> str:
        """Get fee currency for symbol"""
        inst_info = self.client.instruments.get(symbol, {})
        
        if inst_info.get('instType') == 'SPOT':
            return inst_info.get('quoteCcy', 'USDT')
        else:
            return inst_info.get('settleCcy', 'USDT')
    
    def _format_quantity(self, symbol: str, quantity: float) -> str:
        """Format quantity according to symbol rules"""
        inst_info = self.client.instruments.get(symbol, {})
        lot_sz = inst_info.get('lotSz', 1)
        
        # Round to lot size
        if lot_sz < 1:
            precision = len(str(lot_sz).split('.')[-1])
            return f"{quantity:.{precision}f}"
        else:
            return str(int(quantity / lot_sz) * lot_sz)
    
    def _format_price(self, symbol: str, price: float) -> str:
        """Format price according to symbol rules"""
        inst_info = self.client.instruments.get(symbol, {})
        tick_sz = inst_info.get('tickSz', 0.0001)
        
        # Round to tick size
        if tick_sz < 1:
            precision = len(str(tick_sz).split('.')[-1])
            return f"{price:.{precision}f}"
        else:
            return str(int(price / tick_sz) * tick_sz)