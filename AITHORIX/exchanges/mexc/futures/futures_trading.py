"""
AITHORIX MEXC Futures Trading Implementation
High-level futures trading functionality for MEXC contracts
"""

import logging
from datetime import datetime, timezone
from typing import Dict, List, Optional, Any
import asyncio

from ...base_exchange import (
    Order, Trade, Position, Balance, OrderType, OrderSide,
    OrderStatus, TimeInForce, PositionSide
)
from .futures_api import MEXCFuturesAPI

logger = logging.getLogger(__name__)


class MEXCFuturesTrading:
    """
    MEXC futures trading implementation
    """
    
    def __init__(self, client):
        self.client = client
        self.api = MEXCFuturesAPI(client)
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
        """Place a futures order"""
        try:
            params = params or {}
            
            # Build order parameters
            order_params = {
                'symbol': self._normalize_symbol(symbol),
                'price': price if order_type == OrderType.LIMIT else 0,
                'vol': self._format_quantity(symbol, size),
                'side': 1 if side.upper() == 'BUY' else 3,  # MEXC specific: 1=open long, 2=close short, 3=open short, 4=close long
                'type': self._get_order_type(order_type),
                'openType': params.get('margin_mode', 2),  # 1=isolated, 2=cross
                'leverage': params.get('leverage', 10),
                'externalOid': params.get('client_order_id')
            }
            
            # Handle position side
            if params.get('position_side'):
                position_side = params['position_side']
                if position_side == PositionSide.LONG:
                    order_params['side'] = 1 if side.upper() == 'BUY' else 4
                else:  # SHORT
                    order_params['side'] = 3 if side.upper() == 'SELL' else 2
            
            # Add optional parameters
            if params.get('reduce_only'):
                order_params['reduceOnly'] = 1
            
            if params.get('time_in_force'):
                order_params['timeInForce'] = self._convert_time_in_force(params['time_in_force'])
            
            if order_type in [OrderType.STOP, OrderType.STOP_LIMIT]:
                order_params['triggerPrice'] = params.get('stop_price', price)
                order_params['triggerType'] = params.get('trigger_type', 1)  # 1=last price, 2=index price
            
            # Place order
            response = await self.api.place_order(order_params)
            
            if not response.get('success'):
                raise Exception(f"Order failed: {response.get('message', 'Unknown error')}")
            
            order_data = response['data']
            
            # Create order object
            return Order(
                id=str(order_data['orderId']),
                client_order_id=order_data.get('externalOid'),
                exchange='MEXC',
                symbol=symbol,
                type=order_type,
                side=OrderSide.BUY if side.upper() == 'BUY' else OrderSide.SELL,
                size=size,
                price=price,
                status=self._parse_order_status(order_data.get('state', 2)),
                filled_size=0.0,
                average_price=None,
                fee=0.0,
                fee_currency='USDT',
                timestamp=datetime.now(timezone.utc),
                raw_data=order_data
            )
            
        except Exception as e:
            logger.error(f"Error placing futures order: {str(e)}")
            raise
    
    async def cancel_order(self, order_id: str, symbol: Optional[str] = None) -> bool:
        """Cancel a futures order"""
        try:
            response = await self.api.cancel_order([order_id])
            
            if response.get('success'):
                return True
            
            logger.error(f"Failed to cancel order: {response.get('message')}")
            return False
            
        except Exception as e:
            logger.error(f"Error canceling order: {str(e)}")
            return False
    
    async def get_order(self, order_id: str, symbol: Optional[str] = None) -> Optional[Order]:
        """Get order details"""
        try:
            response = await self.api.get_order(order_id)
            
            if not response.get('success'):
                return None
            
            order_data = response['data']
            
            # Determine side
            side_map = {1: 'BUY', 2: 'SELL', 3: 'SELL', 4: 'BUY'}
            side = side_map.get(order_data['side'], 'BUY')
            
            return Order(
                id=str(order_data['orderId']),
                client_order_id=order_data.get('externalOid'),
                exchange='MEXC',
                symbol=order_data['symbol'],
                type=self._parse_order_type(order_data['orderType']),
                side=OrderSide.BUY if side == 'BUY' else OrderSide.SELL,
                size=float(order_data['volume']),
                price=float(order_data['price']) if order_data['price'] else None,
                status=self._parse_order_status(order_data['state']),
                filled_size=float(order_data.get('dealVolume', 0)),
                average_price=float(order_data['dealAvgPrice']) if order_data.get('dealAvgPrice') else None,
                fee=self._calculate_fee(order_data),
                fee_currency='USDT',
                timestamp=datetime.fromtimestamp(order_data['createTime'] / 1000, tz=timezone.utc),
                raw_data=order_data
            )
            
        except Exception as e:
            logger.error(f"Error getting order: {str(e)}")
            return None
    
    async def get_open_orders(self, symbol: Optional[str] = None) -> List[Order]:
        """Get all open orders"""
        try:
            response = await self.api.get_open_orders(symbol)
            
            if not response.get('success'):
                return []
            
            orders = []
            for order_data in response['data']:
                side_map = {1: 'BUY', 2: 'SELL', 3: 'SELL', 4: 'BUY'}
                side = side_map.get(order_data['side'], 'BUY')
                
                orders.append(Order(
                    id=str(order_data['orderId']),
                    client_order_id=order_data.get('externalOid'),
                    exchange='MEXC',
                    symbol=order_data['symbol'],
                    type=self._parse_order_type(order_data['orderType']),
                    side=OrderSide.BUY if side == 'BUY' else OrderSide.SELL,
                    size=float(order_data['volume']),
                    price=float(order_data['price']) if order_data['price'] else None,
                    status=OrderStatus.OPEN,
                    filled_size=float(order_data.get('dealVolume', 0)),
                    average_price=float(order_data['dealAvgPrice']) if order_data.get('dealAvgPrice') else None,
                    fee=0.0,
                    fee_currency='USDT',
                    timestamp=datetime.fromtimestamp(order_data['createTime'] / 1000, tz=timezone.utc),
                    raw_data=order_data
                ))
            
            return orders
            
        except Exception as e:
            logger.error(f"Error getting open orders: {str(e)}")
            return []
    
    async def get_balance(self) -> Dict[str, Balance]:
        """Get futures account balance"""
        try:
            # Cache balance for 1 second to avoid rate limits
            current_time = asyncio.get_event_loop().time()
            if current_time - self._last_balance_update < 1:
                return self._balance_cache
            
            response = await self.api.get_account_info()
            
            if not response.get('success'):
                return self._balance_cache
            
            balances = {}
            for asset_data in response['data']:
                currency = asset_data['currency']
                
                balance = Balance(
                    currency=currency,
                    free=float(asset_data.get('availableBalance', 0)),
                    used=float(asset_data.get('frozenBalance', 0)),
                    total=float(asset_data.get('balance', 0)),
                    exchange='MEXC'
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
            response = await self.api.get_positions()
            
            if not response.get('success'):
                return []
            
            positions = []
            for pos_data in response['data']:
                position = Position(
                    symbol=pos_data['symbol'],
                    side=PositionSide.LONG if pos_data['positionType'] == 1 else PositionSide.SHORT,
                    size=float(pos_data['volume']),
                    entry_price=float(pos_data['openAvgPrice']),
                    mark_price=float(pos_data.get('markPrice', pos_data['openAvgPrice'])),
                    unrealized_pnl=float(pos_data.get('unrealisedPnl', 0)),
                    realized_pnl=float(pos_data.get('realisedPnl', 0)),
                    margin=float(pos_data.get('im', 0)),
                    leverage=float(pos_data.get('leverage', 1)),
                    liquidation_price=float(pos_data.get('liquidationPrice', 0)),
                    exchange='MEXC',
                    raw_data=pos_data
                )
                positions.append(position)
                self._position_cache[pos_data['symbol']] = position
            
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
            
            # Get all positions
            positions = await self.get_open_positions()
            
            for position in positions:
                if position.symbol == symbol:
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
            
            # Place close order
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
    
    async def adjust_leverage(self, symbol: str, leverage: int) -> bool:
        """Adjust position leverage"""
        try:
            # Get current position to determine type
            position = await self.get_position(symbol)
            position_type = 1 if position and position.margin > 0 else 2  # 1=isolated, 2=cross
            
            response = await self.api.adjust_leverage(symbol, leverage, position_type)
            return response.get('success', False)
            
        except Exception as e:
            logger.error(f"Error adjusting leverage: {str(e)}")
            return False
    
    async def get_trades(self, symbol: Optional[str] = None, 
                        start_time: Optional[datetime] = None,
                        end_time: Optional[datetime] = None,
                        limit: int = 100) -> List[Trade]:
        """Get trade history"""
        try:
            # Convert times to milliseconds
            start_ms = int(start_time.timestamp() * 1000) if start_time else None
            end_ms = int(end_time.timestamp() * 1000) if end_time else None
            
            response = await self.api.get_trade_history(
                symbol=symbol,
                start_time=start_ms,
                end_time=end_ms,
                page_size=limit
            )
            
            if not response.get('success'):
                return []
            
            trades = []
            for trade_data in response['data']:
                side_map = {1: 'BUY', 2: 'SELL', 3: 'SELL', 4: 'BUY'}
                side = side_map.get(trade_data['side'], 'BUY')
                
                trades.append(Trade(
                    id=str(trade_data['tradeId']),
                    order_id=str(trade_data['orderId']),
                    symbol=trade_data['symbol'],
                    side=OrderSide.BUY if side == 'BUY' else OrderSide.SELL,
                    price=float(trade_data['price']),
                    size=float(trade_data['volume']),
                    fee=float(trade_data.get('fee', 0)),
                    fee_currency=trade_data.get('feeCurrency', 'USDT'),
                    timestamp=datetime.fromtimestamp(trade_data['createTime'] / 1000, tz=timezone.utc),
                    is_maker=trade_data.get('isMaker', False)
                ))
            
            return trades
            
        except Exception as e:
            logger.error(f"Error getting trades: {str(e)}")
            return []
    
    # Helper methods
    def _normalize_symbol(self, symbol: str) -> str:
        """Normalize symbol for MEXC futures"""
        # MEXC uses format like BTC_USDT for futures
        if '_' not in symbol:
            # Convert BTCUSDT to BTC_USDT
            if symbol.endswith('USDT'):
                base = symbol[:-4]
                return f"{base}_USDT"
            elif symbol.endswith('PERP'):
                # Remove PERP suffix
                return symbol[:-4]
        return symbol
    
    def _get_order_type(self, order_type: OrderType) -> int:
        """Convert order type to MEXC format"""
        type_map = {
            OrderType.LIMIT: 1,
            OrderType.MARKET: 5,
            OrderType.STOP: 3,
            OrderType.STOP_LIMIT: 4,
            OrderType.POST_ONLY: 1,  # Use limit with post-only flag
            OrderType.IOC: 2,
            OrderType.FOK: 6
        }
        return type_map.get(order_type, 1)
    
    def _parse_order_type(self, type_code: int) -> OrderType:
        """Parse MEXC order type"""
        type_map = {
            1: OrderType.LIMIT,
            2: OrderType.IOC,
            3: OrderType.STOP,
            4: OrderType.STOP_LIMIT,
            5: OrderType.MARKET,
            6: OrderType.FOK
        }
        return type_map.get(type_code, OrderType.LIMIT)
    
    def _parse_order_status(self, status_code: int) -> OrderStatus:
        """Parse MEXC order status"""
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
    
    def _convert_time_in_force(self, tif: str) -> int:
        """Convert time in force to MEXC format"""
        tif_map = {
            'GTC': 1,
            'IOC': 2,
            'FOK': 3
        }
        return tif_map.get(tif.upper(), 1)
    
    def _calculate_fee(self, order_data: Dict[str, Any]) -> float:
        """Calculate order fee"""
        return float(order_data.get('fee', 0))
    
    def _format_quantity(self, symbol: str, quantity: float) -> float:
        """Format quantity according to symbol rules"""
        # Get symbol info from client
        symbol_info = self.client.symbol_info.get(self._normalize_symbol(symbol), {})
        precision = symbol_info.get('volPrecision', 8)
        
        # Round to precision
        return round(quantity, precision)