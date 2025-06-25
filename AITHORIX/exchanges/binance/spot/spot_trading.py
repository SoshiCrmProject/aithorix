"""
AITHORIX Binance Spot Trading
High-level spot trading interface for Binance
"""

import logging
from datetime import datetime, timezone
from typing import Dict, List, Optional, Any
import uuid

from ...base_exchange import (
    Order, Trade, Balance, OrderType, OrderSide,
    OrderStatus, TimeInForce
)
from .spot_api import BinanceSpotAPI

logger = logging.getLogger(__name__)


class BinanceSpotTrading:
    """
    Binance spot trading implementation
    """
    
    def __init__(self, client):
        self.client = client
        self.api = BinanceSpotAPI(client)
    
    async def place_order(
        self,
        symbol: str,
        side: str,
        order_type: OrderType,
        size: float,
        price: Optional[float] = None,
        params: Optional[Dict] = None
    ) -> Order:
        """Place a spot order"""
        try:
            # Prepare order parameters
            order_params = {
                'symbol': self.client._normalize_symbol(symbol),
                'side': side.upper(),
                'type': self.client._convert_order_type(order_type),
                'quantity': self._format_quantity(symbol, size),
                'newClientOrderId': str(uuid.uuid4())
            }
            
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
            
            # Handle post-only orders
            if order_type == OrderType.POST_ONLY:
                order_params['type'] = 'LIMIT_MAKER'
            
            # Add additional parameters
            if params:
                if 'time_in_force' in params:
                    order_params['timeInForce'] = params['time_in_force']
                if 'iceberg_qty' in params:
                    order_params['icebergQty'] = self._format_quantity(
                        symbol, params['iceberg_qty']
                    )
            
            # Place order
            response = await self.api.new_order(order_params)
            
            # Convert to Order object
            return self._parse_order(response)
            
        except Exception as e:
            logger.error(f"Failed to place spot order: {str(e)}")
            raise
    
    async def cancel_order(self, order_id: str, symbol: str) -> bool:
        """Cancel a spot order"""
        try:
            normalized_symbol = self.client._normalize_symbol(symbol)
            
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
            logger.error(f"Failed to cancel spot order: {str(e)}")
            return False
    
    async def get_order(self, order_id: str, symbol: str) -> Order:
        """Get spot order details"""
        try:
            normalized_symbol = self.client._normalize_symbol(symbol)
            
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
            logger.error(f"Failed to get spot order: {str(e)}")
            raise
    
    async def get_open_orders(self, symbol: Optional[str] = None) -> List[Order]:
        """Get open spot orders"""
        try:
            if symbol:
                symbol = self.client._normalize_symbol(symbol)
            
            response = await self.api.get_open_orders(symbol)
            
            orders = []
            for order_data in response:
                orders.append(self._parse_order(order_data))
            
            return orders
            
        except Exception as e:
            logger.error(f"Failed to get open spot orders: {str(e)}")
            return []
    
    async def get_order_history(
        self,
        symbol: Optional[str] = None,
        limit: int = 100,
        start_time: Optional[datetime] = None
    ) -> List[Order]:
        """Get spot order history"""
        try:
            if not symbol:
                # Binance requires symbol for order history
                # Get orders for top symbols
                orders = []
                top_symbols = await self.client.get_top_symbols(10)
                
                for sym in top_symbols:
                    sym_orders = await self._get_symbol_order_history(
                        sym, limit // 10, start_time
                    )
                    orders.extend(sym_orders)
                
                # Sort by timestamp
                orders.sort(key=lambda x: x.created_at, reverse=True)
                return orders[:limit]
            else:
                return await self._get_symbol_order_history(symbol, limit, start_time)
                
        except Exception as e:
            logger.error(f"Failed to get spot order history: {str(e)}")
            return []
    
    async def _get_symbol_order_history(
        self,
        symbol: str,
        limit: int,
        start_time: Optional[datetime]
    ) -> List[Order]:
        """Get order history for specific symbol"""
        normalized_symbol = self.client._normalize_symbol(symbol)
        
        params = {
            'symbol': normalized_symbol,
            'limit': min(limit, 500)  # Binance max is 500
        }
        
        if start_time:
            params['startTime'] = int(start_time.timestamp() * 1000)
        
        response = await self.api.get_all_orders(**params)
        
        orders = []
        for order_data in response:
            orders.append(self._parse_order(order_data))
        
        return orders
    
    async def get_balance(self) -> Dict[str, Balance]:
        """Get spot account balance"""
        try:
            response = await self.api.get_account()
            
            balances = {}
            for balance_data in response.get('balances', []):
                currency = balance_data['asset']
                free = float(balance_data['free'])
                locked = float(balance_data['locked'])
                
                # Only include non-zero balances
                if free > 0 or locked > 0:
                    balances[currency] = Balance(
                        currency=currency,
                        free=free,
                        used=locked,
                        total=free + locked
                    )
            
            return balances
            
        except Exception as e:
            logger.error(f"Failed to get spot balance: {str(e)}")
            return {}
    
    async def place_oco_order(
        self,
        symbol: str,
        side: str,
        size: float,
        price: float,
        stop_price: float,
        stop_limit_price: Optional[float] = None,
        params: Optional[Dict] = None
    ) -> Dict[str, Any]:
        """Place OCO (One-Cancels-Other) order"""
        try:
            order_params = {
                'symbol': self.client._normalize_symbol(symbol),
                'side': side.upper(),
                'quantity': self._format_quantity(symbol, size),
                'price': self._format_price(symbol, price),
                'stopPrice': self._format_price(symbol, stop_price),
                'listClientOrderId': str(uuid.uuid4()),
                'limitClientOrderId': str(uuid.uuid4()),
                'stopClientOrderId': str(uuid.uuid4())
            }
            
            if stop_limit_price:
                order_params['stopLimitPrice'] = self._format_price(symbol, stop_limit_price)
                order_params['stopLimitTimeInForce'] = 'GTC'
            
            if params:
                order_params.update(params)
            
            response = await self.api.new_oco_order(order_params)
            return response
            
        except Exception as e:
            logger.error(f"Failed to place OCO order: {str(e)}")
            raise
    
    def _parse_order(self, data: Dict[str, Any]) -> Order:
        """Parse Binance order response to Order object"""
        return Order(
            order_id=str(data['orderId']),
            client_order_id=data.get('clientOrderId'),
            symbol=self.client._denormalize_symbol(data['symbol']),
            side=OrderSide.BUY if data['side'] == 'BUY' else OrderSide.SELL,
            order_type=self.client._parse_order_type(data['type']),
            status=self._parse_order_status(data['status']),
            price=float(data['price']) if data.get('price') else None,
            size=float(data['origQty']),
            filled_size=float(data['executedQty']),
            average_price=float(data.get('cummulativeQuoteQty', 0)) / float(data['executedQty']) 
                         if float(data['executedQty']) > 0 else None,
            fee=self._calculate_fee(data),
            fee_currency='BNB',  # Default, actual fee currency in trades
            time_in_force=self._parse_time_in_force(data.get('timeInForce', 'GTC')),
            created_at=datetime.fromtimestamp(data['time'] / 1000, tz=timezone.utc),
            updated_at=datetime.fromtimestamp(data.get('updateTime', data['time']) / 1000, tz=timezone.utc)
        )
    
    def _parse_order_status(self, status: str) -> OrderStatus:
        """Parse Binance order status"""
        status_map = {
            'NEW': OrderStatus.OPEN,
            'PARTIALLY_FILLED': OrderStatus.PARTIALLY_FILLED,
            'FILLED': OrderStatus.FILLED,
            'CANCELED': OrderStatus.CANCELLED,
            'PENDING_CANCEL': OrderStatus.OPEN,
            'REJECTED': OrderStatus.REJECTED,
            'EXPIRED': OrderStatus.EXPIRED
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
        
        # Estimate fee based on executed quantity and fee rate
        executed_value = float(order_data.get('cummulativeQuoteQty', 0))
        is_maker = order_data['type'] in ['LIMIT', 'LIMIT_MAKER']
        fee_rate = self.client.config.maker_fee if is_maker else self.client.config.taker_fee
        
        return executed_value * fee_rate
    
    def _format_quantity(self, symbol: str, quantity: float) -> str:
        """Format quantity according to symbol rules"""
        # Get symbol info
        symbol_info = self.client.symbol_info.get(self.client._normalize_symbol(symbol), {})
        filters = symbol_info.get('filters', {})
        
        # Get LOT_SIZE filter
        lot_size = filters.get('LOT_SIZE', {})
        step_size = float(lot_size.get('stepSize', 0.00000001))
        
        # Round to step size
        precision = len(str(step_size).rstrip('0').split('.')[-1])
        return f"{quantity:.{precision}f}".rstrip('0').rstrip('.')
    
    def _format_price(self, symbol: str, price: float) -> str:
        """Format price according to symbol rules"""
        # Get symbol info
        symbol_info = self.client.symbol_info.get(self.client._normalize_symbol(symbol), {})
        filters = symbol_info.get('filters', {})
        
        # Get PRICE_FILTER
        price_filter = filters.get('PRICE_FILTER', {})
        tick_size = float(price_filter.get('tickSize', 0.00000001))
        
        # Round to tick size
        precision = len(str(tick_size).rstrip('0').split('.')[-1])
        return f"{price:.{precision}f}".rstrip('0').rstrip('.')