"""
AITHORIX Binance Futures Trading
High-level futures trading interface for Binance
"""

import logging
from datetime import datetime, timezone
from typing import Dict, List, Optional, Any
import uuid

from ...base_exchange import (
    Order, Trade, Balance, Position, OrderType, OrderSide,
    OrderStatus, TimeInForce, PositionSide
)
from .futures_api import BinanceFuturesAPI

logger = logging.getLogger(__name__)


class BinanceFuturesTrading:
    """
    Binance futures trading implementation
    """
    
    def __init__(self, client):
        self.client = client
        self.api = BinanceFuturesAPI(client)
        self._position_mode = None
    
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
            # Prepare order parameters
            order_params = {
                'symbol': self._normalize_futures_symbol(symbol),
                'side': side.upper(),
                'type': self.client._convert_order_type(order_type),
                'quantity': self._format_quantity(symbol, size),
                'newClientOrderId': str(uuid.uuid4())
            }
            
            # Add position side for hedge mode
            if await self._is_hedge_mode():
                position_side = params.get('position_side') if params else None
                if not position_side:
                    # Infer from order side
                    position_side = 'LONG' if side.upper() == 'BUY' else 'SHORT'
                order_params['positionSide'] = position_side
            
            # Add price for limit orders
            if order_type in [OrderType.LIMIT, OrderType.STOP_LIMIT, OrderType.POST_ONLY]:
                if not price:
                    raise ValueError(f"Price required for {order_type.value} orders")
                order_params['price'] = self._format_price(symbol, price)
                order_params['timeInForce'] = 'GTC'
            
            # Add stop price for stop orders
            if order_type in [OrderType.STOP, OrderType.STOP_LIMIT]:
                stop_price = params.get('stop_price') if params else None
                if not stop_price:
                    raise ValueError("Stop price required for stop orders")
                order_params['stopPrice'] = self._format_price(symbol, stop_price)
                order_params['workingType'] = params.get('working_type', 'CONTRACT_PRICE') if params else 'CONTRACT_PRICE'
            
            # Handle reduce-only orders
            if params and params.get('reduce_only'):
                order_params['reduceOnly'] = 'true'
            
            # Handle post-only orders
            if order_type == OrderType.POST_ONLY:
                order_params['type'] = 'LIMIT'
                order_params['timeInForce'] = 'GTX'
            
            # Add additional parameters
            if params:
                if 'time_in_force' in params:
                    order_params['timeInForce'] = params['time_in_force']
                if 'activation_price' in params:
                    order_params['activationPrice'] = self._format_price(
                        symbol, params['activation_price']
                    )
                if 'callback_rate' in params:
                    order_params['callbackRate'] = params['callback_rate']
            
            # Place order
            response = await self.api.new_order(order_params)
            
            # Convert to Order object
            return self._parse_order(response)
            
        except Exception as e:
            logger.error(f"Failed to place futures order: {str(e)}")
            raise
    
    async def cancel_order(self, order_id: str, symbol: str) -> bool:
        """Cancel a futures order"""
        try:
            normalized_symbol = self._normalize_futures_symbol(symbol)
            
            # Try to cancel by order ID first
            try:
                response = await self.api.cancel_order(
                    symbol=normalized_symbol,
                    order_id=order_id
                )
                return response.get('status') == 'CANCELED'
            except Exception:
                # Try with client order ID
                response = await self.api.cancel_order(
                    symbol=normalized_symbol,
                    orig_client_order_id=order_id
                )
                return response.get('status') == 'CANCELED'
                
        except Exception as e:
            logger.error(f"Failed to cancel futures order: {str(e)}")
            return False
    
    async def cancel_all_orders(self, symbol: str) -> bool:
        """Cancel all open orders for a symbol"""
        try:
            normalized_symbol = self._normalize_futures_symbol(symbol)
            response = await self.api.cancel_all_orders(normalized_symbol)
            return response.get('code') == 200
        except Exception as e:
            logger.error(f"Failed to cancel all orders: {str(e)}")
            return False
    
    async def get_order(self, order_id: str, symbol: str) -> Order:
        """Get futures order details"""
        try:
            normalized_symbol = self._normalize_futures_symbol(symbol)
            
            # Try to get by order ID first
            try:
                response = await self.api.get_order(
                    symbol=normalized_symbol,
                    order_id=order_id
                )
            except Exception:
                # Try with client order ID
                response = await self.api.get_order(
                    symbol=normalized_symbol,
                    orig_client_order_id=order_id
                )
            
            return self._parse_order(response)
            
        except Exception as e:
            logger.error(f"Failed to get futures order: {str(e)}")
            raise
    
    async def get_open_orders(self, symbol: Optional[str] = None) -> List[Order]:
        """Get open futures orders"""
        try:
            if symbol:
                symbol = self._normalize_futures_symbol(symbol)
            
            response = await self.api.get_open_orders(symbol)
            
            orders = []
            for order_data in response:
                orders.append(self._parse_order(order_data))
            
            return orders
            
        except Exception as e:
            logger.error(f"Failed to get open futures orders: {str(e)}")
            return []
    
    async def get_order_history(
        self,
        symbol: Optional[str] = None,
        limit: int = 100,
        start_time: Optional[datetime] = None
    ) -> List[Order]:
        """Get futures order history"""
        try:
            if not symbol:
                # Get orders for all active positions
                positions = await self.get_open_positions()
                orders = []
                
                for position in positions:
                    pos_orders = await self._get_symbol_order_history(
                        position.symbol, limit // max(len(positions), 1), start_time
                    )
                    orders.extend(pos_orders)
                
                # Sort by timestamp
                orders.sort(key=lambda x: x.created_at, reverse=True)
                return orders[:limit]
            else:
                return await self._get_symbol_order_history(symbol, limit, start_time)
                
        except Exception as e:
            logger.error(f"Failed to get futures order history: {str(e)}")
            return []
    
    async def _get_symbol_order_history(
        self,
        symbol: str,
        limit: int,
        start_time: Optional[datetime]
    ) -> List[Order]:
        """Get order history for specific symbol"""
        normalized_symbol = self._normalize_futures_symbol(symbol)
        
        params = {
            'symbol': normalized_symbol,
            'limit': min(limit, 500)
        }
        
        if start_time:
            params['startTime'] = int(start_time.timestamp() * 1000)
        
        response = await self.api.get_all_orders(**params)
        
        orders = []
        for order_data in response:
            orders.append(self._parse_order(order_data))
        
        return orders
    
    async def get_balance(self) -> Dict[str, Balance]:
        """Get futures account balance"""
        try:
            response = await self.api.get_balance()
            
            balances = {}
            for balance_data in response:
                currency = balance_data['asset']
                balance = float(balance_data['balance'])
                available = float(balance_data['availableBalance'])
                
                # Only include non-zero balances
                if balance > 0:
                    balances[f"{currency}_FUTURES"] = Balance(
                        currency=currency,
                        free=available,
                        used=balance - available,
                        total=balance
                    )
            
            return balances
            
        except Exception as e:
            logger.error(f"Failed to get futures balance: {str(e)}")
            return {}
    
    async def get_open_positions(self) -> List[Position]:
        """Get open futures positions"""
        try:
            response = await self.api.get_position_risk()
            
            positions = []
            for pos_data in response:
                position_amt = float(pos_data['positionAmt'])
                if position_amt != 0:
                    positions.append(self._parse_position(pos_data))
            
            return positions
            
        except Exception as e:
            logger.error(f"Failed to get open positions: {str(e)}")
            return []
    
    async def get_position(self, symbol: str) -> Optional[Position]:
        """Get specific position"""
        try:
            normalized_symbol = self._normalize_futures_symbol(symbol)
            response = await self.api.get_position_risk(normalized_symbol)
            
            for pos_data in response:
                position_amt = float(pos_data['positionAmt'])
                if position_amt != 0:
                    return self._parse_position(pos_data)
            
            return None
            
        except Exception as e:
            logger.error(f"Failed to get position: {str(e)}")
            return None
    
    async def change_leverage(self, symbol: str, leverage: int) -> bool:
        """Change leverage for a symbol"""
        try:
            normalized_symbol = self._normalize_futures_symbol(symbol)
            response = await self.api.change_leverage(normalized_symbol, leverage)
            return response.get('leverage') == leverage
        except Exception as e:
            logger.error(f"Failed to change leverage: {str(e)}")
            return False
    
    async def change_margin_type(self, symbol: str, margin_type: str) -> bool:
        """Change margin type (ISOLATED/CROSSED)"""
        try:
            normalized_symbol = self._normalize_futures_symbol(symbol)
            response = await self.api.change_margin_type(normalized_symbol, margin_type.upper())
            return response.get('code') == 200
        except Exception as e:
            logger.error(f"Failed to change margin type: {str(e)}")
            return False
    
    async def _is_hedge_mode(self) -> bool:
        """Check if account is in hedge mode"""
        if self._position_mode is None:
            response = await self.api.get_position_mode()
            self._position_mode = response.get('dualSidePosition', False)
        return self._position_mode
    
    def _normalize_futures_symbol(self, symbol: str) -> str:
        """Normalize symbol for futures"""
        # Remove _PERP suffix if present
        if symbol.endswith('_PERP'):
            symbol = symbol[:-5]
        return self.client._normalize_symbol(symbol)
    
    def _parse_order(self, data: Dict[str, Any]) -> Order:
        """Parse Binance futures order response to Order object"""
        return Order(
            order_id=str(data['orderId']),
            client_order_id=data.get('clientOrderId'),
            symbol=self.client._denormalize_symbol(data['symbol']) + '_PERP',
            side=OrderSide.BUY if data['side'] == 'BUY' else OrderSide.SELL,
            order_type=self.client._parse_order_type(data['type']),
            status=self._parse_order_status(data['status']),
            price=float(data['price']) if data.get('price') else None,
            size=float(data['origQty']),
            filled_size=float(data['executedQty']),
            average_price=float(data.get('avgPrice', 0)) if float(data.get('avgPrice', 0)) > 0 else None,
            fee=self._calculate_fee(data),
            fee_currency='USDT',
            time_in_force=self._parse_time_in_force(data.get('timeInForce', 'GTC')),
            created_at=datetime.fromtimestamp(data['time'] / 1000, tz=timezone.utc),
            updated_at=datetime.fromtimestamp(data.get('updateTime', data['time']) / 1000, tz=timezone.utc),
            metadata={
                'position_side': data.get('positionSide'),
                'reduce_only': data.get('reduceOnly', False),
                'close_position': data.get('closePosition', False),
                'working_type': data.get('workingType')
            }
        )
    
    def _parse_position(self, data: Dict[str, Any]) -> Position:
        """Parse position data"""
        position_amt = float(data['positionAmt'])
        entry_price = float(data['entryPrice'])
        mark_price = float(data['markPrice'])
        
        # Calculate PnL
        if position_amt != 0:
            unrealized_pnl = float(data['unRealizedProfit'])
        else:
            unrealized_pnl = 0.0
        
        return Position(
            symbol=self.client._denormalize_symbol(data['symbol']) + '_PERP',
            side=PositionSide.LONG if position_amt > 0 else PositionSide.SHORT,
            size=abs(position_amt),
            entry_price=entry_price,
            mark_price=mark_price,
            liquidation_price=float(data['liquidationPrice']) if data.get('liquidationPrice') else None,
            unrealized_pnl=unrealized_pnl,
            realized_pnl=0.0,  # Not provided in position risk
            margin=float(data['isolatedMargin']) if data.get('marginType') == 'isolated' else 
                   abs(position_amt) * mark_price / float(data.get('leverage', 1)),
            leverage=int(data.get('leverage', 1)),
            created_at=datetime.now(timezone.utc),  # Not provided
            updated_at=datetime.now(timezone.utc),
            metadata={
                'margin_type': data.get('marginType'),
                'position_side': data.get('positionSide'),
                'maint_margin': float(data.get('maintMargin', 0)),
                'margin_ratio': float(data.get('marginRatio', 0))
            }
        )
    
    def _parse_order_status(self, status: str) -> OrderStatus:
        """Parse Binance futures order status"""
        status_map = {
            'NEW': OrderStatus.OPEN,
            'PARTIALLY_FILLED': OrderStatus.PARTIALLY_FILLED,
            'FILLED': OrderStatus.FILLED,
            'CANCELED': OrderStatus.CANCELLED,
            'REJECTED': OrderStatus.REJECTED,
            'EXPIRED': OrderStatus.EXPIRED,
            'EXPIRED_IN_MATCH': OrderStatus.EXPIRED
        }
        return status_map.get(status, OrderStatus.OPEN)
    
    def _parse_time_in_force(self, tif: str) -> TimeInForce:
        """Parse time in force"""
        tif_map = {
            'GTC': TimeInForce.GTC,
            'IOC': TimeInForce.IOC,
            'FOK': TimeInForce.FOK,
            'GTX': TimeInForce.GTX
        }
        return tif_map.get(tif, TimeInForce.GTC)
    
    def _calculate_fee(self, order_data: Dict[str, Any]) -> float:
        """Calculate order fee (estimate)"""
        if float(order_data['executedQty']) == 0:
            return 0.0
        
        # For futures, fee is in USDT
        # Estimate based on executed quantity and average price
        executed_value = float(order_data['executedQty']) * float(order_data.get('avgPrice', 0))
        is_maker = order_data['type'] == 'LIMIT' and order_data.get('timeInForce') != 'IOC'
        fee_rate = self.client.config.maker_fee if is_maker else self.client.config.taker_fee
        
        return executed_value * fee_rate
    
    def _format_quantity(self, symbol: str, quantity: float) -> str:
        """Format quantity according to symbol rules"""
        # Get symbol info
        norm_symbol = self._normalize_futures_symbol(symbol)
        symbol_info = self.client.symbol_info.get(f"{norm_symbol}_PERP", {})
        filters = symbol_info.get('filters', {})
        
        # Get LOT_SIZE filter
        lot_size = filters.get('LOT_SIZE', {})
        step_size = float(lot_size.get('stepSize', 0.001))
        
        # Round to step size
        precision = len(str(step_size).rstrip('0').split('.')[-1])
        return f"{quantity:.{precision}f}".rstrip('0').rstrip('.')
    
    def _format_price(self, symbol: str, price: float) -> str:
        """Format price according to symbol rules"""
        # Get symbol info
        norm_symbol = self._normalize_futures_symbol(symbol)
        symbol_info = self.client.symbol_info.get(f"{norm_symbol}_PERP", {})
        filters = symbol_info.get('filters', {})
        
        # Get PRICE_FILTER
        price_filter = filters.get('PRICE_FILTER', {})
        tick_size = float(price_filter.get('tickSize', 0.01))
        
        # Round to tick size
        precision = len(str(tick_size).rstrip('0').split('.')[-1])
        return f"{price:.{precision}f}".rstrip('0').rstrip('.')