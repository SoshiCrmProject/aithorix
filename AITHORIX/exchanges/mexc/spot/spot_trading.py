"""
AITHORIX MEXC Spot Trading
High-level spot trading interface for MEXC
"""

import logging
from datetime import datetime, timezone
from typing import Dict, List, Optional, Any
import uuid

from ...base_exchange import (
    Order, Trade, Balance, OrderType, OrderSide,
    OrderStatus, TimeInForce
)
from .spot_api import MEXCSpotAPI

logger = logging.getLogger(__name__)


class MEXCSpotTrading:
    """
    MEXC spot trading implementation
    """
    
    def __init__(self, client):
        self.client = client
        self.api = MEXCSpotAPI(client)
    
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
                'type': self._convert_order_type(order_type),
                'quantity': self._format_quantity(symbol, size),
                'newClientOrderId': str(uuid.uuid4())
            }
            
            # Add price for limit orders
            if order_type in [OrderType.LIMIT, OrderType.STOP_LIMIT]:
                if not price:
                    raise ValueError(f"Price required for {order_type.value} orders")
                order_params['price'] = self._format_price(symbol, price)
                order_params['timeInForce'] = 'GTC'
            
            # Handle additional parameters
            if params:
                if 'time_in_force' in params:
                    order_params['timeInForce'] = self._convert_time_in_force(params['time_in_force'])
                if 'client_order_id' in params:
                    order_params['newClientOrderId'] = params['client_order_id']
                if 'stop_price' in params:
                    order_params['stopPrice'] = self._format_price(symbol, params['stop_price'])
            
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
                    orderId=int(order_id) if order_id.isdigit() else None,
                    origClientOrderId=order_id if not order_id.isdigit() else None
                )
                return response.get('status') == 'CANCELED'
            except Exception as e:
                logger.error(f"Failed to cancel order: {str(e)}")
                return False
                
        except Exception as e:
            logger.error(f"Failed to cancel spot order: {str(e)}")
            return False
    
    async def get_order(self, order_id: str, symbol: str) -> Order:
        """Get spot order details"""
        try:
            normalized_symbol = self.client._normalize_symbol(symbol)
            
            response = await self.api.get_order(
                symbol=normalized_symbol,
                orderId=int(order_id) if order_id.isdigit() else None,
                origClientOrderId=order_id if not order_id.isdigit() else None
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
                # MEXC requires symbol for order history
                # Get orders for most active symbols
                orders = []
                active_symbols = await self._get_active_symbols()
                
                for sym in active_symbols[:5]:  # Top 5 symbols
                    sym_orders = await self._get_symbol_order_history(
                        sym, limit // 5, start_time
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
        
        start_ts = int(start_time.timestamp() * 1000) if start_time else None
        
        response = await self.api.get_all_orders(
            symbol=normalized_symbol,
            startTime=start_ts,
            limit=min(limit, 500)  # MEXC max is 500
        )
        
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
    
    async def _get_active_symbols(self) -> List[str]:
        """Get most active trading symbols"""
        try:
            # Get 24hr tickers
            tickers = await self.api.get_ticker_24hr()
            
            # Sort by volume
            sorted_tickers = sorted(
                tickers,
                key=lambda x: float(x.get('quoteVolume', 0)),
                reverse=True
            )
            
            # Return top symbols
            return [t['symbol'] for t in sorted_tickers[:20]]
            
        except Exception as e:
            logger.error(f"Failed to get active symbols: {str(e)}")
            return []
    
    def _parse_order(self, data: Dict[str, Any]) -> Order:
        """Parse MEXC order response to Order object"""
        return Order(
            order_id=str(data['orderId']),
            client_order_id=data.get('clientOrderId'),
            symbol=self.client._denormalize_symbol(data['symbol']),
            side=OrderSide.BUY if data['side'] == 'BUY' else OrderSide.SELL,
            order_type=self._parse_order_type(data['type']),
            status=self._parse_order_status(data['status']),
            price=float(data.get('price', 0)) if data.get('price') else None,
            size=float(data['origQty']),
            filled_size=float(data['executedQty']),
            average_price=float(data.get('cummulativeQuoteQty', 0)) / float(data['executedQty']) 
                         if float(data['executedQty']) > 0 else None,
            fee=self._calculate_fee(data),
            fee_currency='USDT',  # Default fee currency
            time_in_force=self._parse_time_in_force(data.get('timeInForce', 'GTC')),
            created_at=datetime.fromtimestamp(data.get('time', 0) / 1000, tz=timezone.utc),
            updated_at=datetime.fromtimestamp(data.get('updateTime', data.get('time', 0)) / 1000, tz=timezone.utc)
        )
    
    def _convert_order_type(self, order_type: OrderType) -> str:
        """Convert OrderType to MEXC format"""
        type_map = {
            OrderType.MARKET: 'MARKET',
            OrderType.LIMIT: 'LIMIT',
            OrderType.STOP: 'STOP_LOSS',
            OrderType.STOP_LIMIT: 'STOP_LOSS_LIMIT'
        }
        return type_map.get(order_type, 'LIMIT')
    
    def _parse_order_type(self, type_str: str) -> OrderType:
        """Parse MEXC order type"""
        type_map = {
            'MARKET': OrderType.MARKET,
            'LIMIT': OrderType.LIMIT,
            'STOP_LOSS': OrderType.STOP,
            'STOP_LOSS_LIMIT': OrderType.STOP_LIMIT
        }
        return type_map.get(type_str, OrderType.LIMIT)
    
    def _parse_order_status(self, status: str) -> OrderStatus:
        """Parse MEXC order status"""
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
    
    def _convert_time_in_force(self, tif: str) -> str:
        """Convert time in force to MEXC format"""
        tif_map = {
            'GTC': 'GTC',
            'IOC': 'IOC',
            'FOK': 'FOK'
        }
        return tif_map.get(tif.upper(), 'GTC')
    
    def _parse_time_in_force(self, tif: str) -> TimeInForce:
        """Parse time in force"""
        tif_map = {
            'GTC': TimeInForce.GTC,
            'IOC': TimeInForce.IOC,
            'FOK': TimeInForce.FOK
        }
        return tif_map.get(tif, TimeInForce.GTC)
    
    def _calculate_fee(self, order_data: Dict[str, Any]) -> float:
        """Calculate order fee (estimate)"""
        if float(order_data['executedQty']) == 0:
            return 0.0
        
        # MEXC spot trading fees
        # Standard: 0.2%, with MX: 0.02%
        executed_value = float(order_data.get('cummulativeQuoteQty', 0))
        fee_rate = 0.002  # 0.2% default
        
        return executed_value * fee_rate
    
    def _format_quantity(self, symbol: str, quantity: float) -> str:
        """Format quantity according to symbol rules"""
        # Get symbol info
        symbol_info = self.client.symbol_info.get(self.client._normalize_symbol(symbol), {})
        precision = symbol_info.get('baseAssetPrecision', 8)
        
        # Format with precision
        return f"{quantity:.{precision}f}".rstrip('0').rstrip('.')
    
    def _format_price(self, symbol: str, price: float) -> str:
        """Format price according to symbol rules"""
        # Get symbol info
        symbol_info = self.client.symbol_info.get(self.client._normalize_symbol(symbol), {})
        precision = symbol_info.get('quoteAssetPrecision', 8)
        
        # Format with precision
        return f"{price:.{precision}f}".rstrip('0').rstrip('.')