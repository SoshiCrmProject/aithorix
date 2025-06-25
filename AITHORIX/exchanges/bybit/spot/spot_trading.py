"""
AITHORIX Bybit Spot Trading Implementation
High-level spot trading functionality for Bybit
"""

import logging
from datetime import datetime, timezone
from typing import Dict, List, Optional, Any
import asyncio
import uuid

from ...base_exchange import (
    Order, Trade, Balance, OrderType, OrderSide,
    OrderStatus, TimeInForce
)
from .spot_api import BybitSpotAPI

logger = logging.getLogger(__name__)


class BybitSpotTrading:
    """
    Bybit spot trading implementation
    """
    
    def __init__(self, client):
        self.client = client
        self.api = BybitSpotAPI(client)
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
        """Place a spot order"""
        try:
            params = params or {}
            
            # Generate client order ID if not provided
            client_order_id = params.get('client_order_id', str(uuid.uuid4()))
            
            # Build order parameters
            order_params = {
                'category': 'spot',
                'symbol': symbol,
                'orderType': self._get_order_type(order_type),
                'side': side.capitalize(),
                'qty': self._format_quantity(symbol, size),
                'orderLinkId': client_order_id,
                'isLeverage': 0  # Spot trading, no leverage
            }
            
            # Add price for limit orders
            if order_type == OrderType.LIMIT:
                if not price:
                    raise ValueError("Price required for limit orders")
                order_params['price'] = self._format_price(symbol, price)
            
            # Time in force
            if params.get('time_in_force'):
                order_params['timeInForce'] = self._convert_time_in_force(params['time_in_force'])
            elif order_type == OrderType.LIMIT:
                order_params['timeInForce'] = 'GTC'
            
            # Post-only orders
            if order_type == OrderType.POST_ONLY or params.get('post_only'):
                order_params['orderType'] = 'Limit'
                order_params['timeInForce'] = 'PostOnly'
            
            # Stop orders
            if order_type in [OrderType.STOP, OrderType.STOP_LIMIT]:
                order_params['triggerPrice'] = params.get('stop_price', price)
                order_params['triggerBy'] = params.get('trigger_by', 'LastPrice')
                
                if order_type == OrderType.STOP:
                    order_params['orderType'] = 'Market'
                else:
                    order_params['orderType'] = 'Limit'
            
            # Place order
            response = await self.api.place_order(order_params)
            
            if response['retCode'] != 0:
                raise Exception(f"Order failed: {response['retMsg']}")
            
            order_data = response['result']
            
            # Create order object
            return Order(
                id=order_data['orderId'],
                client_order_id=order_data['orderLinkId'],
                exchange='Bybit',
                symbol=symbol,
                type=order_type,
                side=OrderSide.BUY if side.upper() == 'BUY' else OrderSide.SELL,
                size=size,
                price=price,
                status=self._parse_order_status(order_data['orderStatus']),
                filled_size=0.0,
                average_price=None,
                fee=0.0,
                fee_currency='USDT',
                timestamp=datetime.now(timezone.utc),
                raw_data=order_data
            )
            
        except Exception as e:
            logger.error(f"Error placing spot order: {str(e)}")
            raise
    
    async def cancel_order(self, order_id: str, symbol: str) -> bool:
        """Cancel a spot order"""
        try:
            params = {
                'category': 'spot',
                'symbol': symbol,
                'orderId': order_id
            }
            
            response = await self.api.cancel_order(params)
            
            if response['retCode'] == 0:
                return True
            
            logger.error(f"Failed to cancel order: {response['retMsg']}")
            return False
            
        except Exception as e:
            logger.error(f"Error canceling order: {str(e)}")
            return False
    
    async def cancel_all_orders(self, symbol: Optional[str] = None) -> int:
        """Cancel all orders"""
        try:
            params = {'category': 'spot'}
            if symbol:
                params['symbol'] = symbol
            
            response = await self.api.cancel_all_orders(params)
            
            if response['retCode'] == 0:
                # Return number of cancelled orders
                return len(response['result']['list'])
            
            logger.error(f"Failed to cancel all orders: {response['retMsg']}")
            return 0
            
        except Exception as e:
            logger.error(f"Error canceling all orders: {str(e)}")
            return 0
    
    async def get_order(self, order_id: str, symbol: str) -> Optional[Order]:
        """Get order details"""
        try:
            # First check open orders
            params = {
                'category': 'spot',
                'symbol': symbol,
                'orderId': order_id
            }
            
            response = await self.api.get_open_orders(params)
            
            if response['retCode'] == 0 and response['result']['list']:
                order_data = response['result']['list'][0]
                return self._parse_order(order_data)
            
            # Check order history
            history_params = {
                'category': 'spot',
                'symbol': symbol,
                'orderId': order_id
            }
            
            history_response = await self.api.get_order_history(history_params)
            
            if history_response['retCode'] == 0 and history_response['result']['list']:
                order_data = history_response['result']['list'][0]
                return self._parse_order(order_data)
            
            return None
            
        except Exception as e:
            logger.error(f"Error getting order: {str(e)}")
            return None
    
    async def get_open_orders(self, symbol: Optional[str] = None) -> List[Order]:
        """Get all open orders"""
        try:
            params = {'category': 'spot'}
            if symbol:
                params['symbol'] = symbol
            else:
                params['limit'] = 50  # Max per request
            
            response = await self.api.get_open_orders(params)
            
            if response['retCode'] != 0:
                return []
            
            orders = []
            for order_data in response['result']['list']:
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
        limit: int = 50
    ) -> List[Order]:
        """Get order history"""
        try:
            params = {
                'category': 'spot',
                'limit': min(limit, 50)  # Max 50 per request
            }
            
            if symbol:
                params['symbol'] = symbol
            if start_time:
                params['startTime'] = int(start_time.timestamp() * 1000)
            if end_time:
                params['endTime'] = int(end_time.timestamp() * 1000)
            
            response = await self.api.get_order_history(params)
            
            if response['retCode'] != 0:
                return []
            
            orders = []
            for order_data in response['result']['list']:
                order = self._parse_order(order_data)
                if order:
                    orders.append(order)
            
            return orders
            
        except Exception as e:
            logger.error(f"Error getting order history: {str(e)}")
            return []
    
    async def get_balance(self) -> Dict[str, Balance]:
        """Get spot account balance"""
        try:
            # Cache balance for 1 second to avoid rate limits
            current_time = asyncio.get_event_loop().time()
            if current_time - self._last_balance_update < 1:
                return self._balance_cache
            
            params = {
                'accountType': 'SPOT'
            }
            
            response = await self.api.get_wallet_balance(params)
            
            if response['retCode'] != 0:
                return self._balance_cache
            
            balances = {}
            for coin_data in response['result']['list'][0]['coin']:
                currency = coin_data['coin']
                
                balance = Balance(
                    currency=currency,
                    free=float(coin_data['free']),
                    used=float(coin_data['locked']),
                    total=float(coin_data['walletBalance']),
                    exchange='Bybit'
                )
                
                balances[currency] = balance
            
            self._balance_cache = balances
            self._last_balance_update = current_time
            
            return balances
            
        except Exception as e:
            logger.error(f"Error getting balance: {str(e)}")
            return self._balance_cache
    
    async def get_trades(
        self,
        symbol: Optional[str] = None,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        limit: int = 50
    ) -> List[Trade]:
        """Get trade history"""
        try:
            params = {
                'category': 'spot',
                'limit': min(limit, 50)
            }
            
            if symbol:
                params['symbol'] = symbol
            if start_time:
                params['startTime'] = int(start_time.timestamp() * 1000)
            if end_time:
                params['endTime'] = int(end_time.timestamp() * 1000)
            
            response = await self.api.get_trade_history(params)
            
            if response['retCode'] != 0:
                return []
            
            trades = []
            for trade_data in response['result']['list']:
                trades.append(Trade(
                    id=trade_data['execId'],
                    order_id=trade_data['orderId'],
                    symbol=trade_data['symbol'],
                    side=OrderSide.BUY if trade_data['side'] == 'Buy' else OrderSide.SELL,
                    price=float(trade_data['execPrice']),
                    size=float(trade_data['execQty']),
                    fee=float(trade_data['execFee']),
                    fee_currency=trade_data['feeCurrency'],
                    timestamp=datetime.fromtimestamp(int(trade_data['execTime']) / 1000, tz=timezone.utc),
                    is_maker=trade_data.get('isMaker', False)
                ))
            
            return trades
            
        except Exception as e:
            logger.error(f"Error getting trades: {str(e)}")
            return []
    
    # Helper methods
    def _parse_order(self, order_data: Dict[str, Any]) -> Optional[Order]:
        """Parse order data into Order object"""
        try:
            return Order(
                id=order_data['orderId'],
                client_order_id=order_data.get('orderLinkId'),
                exchange='Bybit',
                symbol=order_data['symbol'],
                type=self._parse_order_type(order_data['orderType']),
                side=OrderSide.BUY if order_data['side'] == 'Buy' else OrderSide.SELL,
                size=float(order_data['qty']),
                price=float(order_data['price']) if order_data['price'] else None,
                status=self._parse_order_status(order_data['orderStatus']),
                filled_size=float(order_data.get('cumExecQty', 0)),
                average_price=float(order_data['avgPrice']) if order_data.get('avgPrice') else None,
                fee=float(order_data.get('cumExecFee', 0)),
                fee_currency='USDT',
                timestamp=datetime.fromtimestamp(int(order_data['createdTime']) / 1000, tz=timezone.utc),
                raw_data=order_data
            )
        except Exception as e:
            logger.error(f"Error parsing order: {str(e)}")
            return None
    
    def _get_order_type(self, order_type: OrderType) -> str:
        """Convert order type to Bybit format"""
        type_map = {
            OrderType.MARKET: 'Market',
            OrderType.LIMIT: 'Limit',
            OrderType.STOP: 'Market',  # With trigger price
            OrderType.STOP_LIMIT: 'Limit',  # With trigger price
            OrderType.POST_ONLY: 'Limit',
            OrderType.FOK: 'Limit',
            OrderType.IOC: 'Limit'
        }
        return type_map.get(order_type, 'Limit')
    
    def _parse_order_type(self, type_str: str) -> OrderType:
        """Parse Bybit order type"""
        type_map = {
            'Market': OrderType.MARKET,
            'Limit': OrderType.LIMIT
        }
        return type_map.get(type_str, OrderType.LIMIT)
    
    def _parse_order_status(self, status: str) -> OrderStatus:
        """Parse Bybit order status"""
        status_map = {
            'Created': OrderStatus.OPEN,
            'New': OrderStatus.OPEN,
            'PartiallyFilled': OrderStatus.PARTIALLY_FILLED,
            'Filled': OrderStatus.FILLED,
            'Cancelled': OrderStatus.CANCELLED,
            'PartiallyFilledCanceled': OrderStatus.CANCELLED,
            'Rejected': OrderStatus.REJECTED,
            'Deactivated': OrderStatus.EXPIRED
        }
        return status_map.get(status, OrderStatus.OPEN)
    
    def _convert_time_in_force(self, tif: str) -> str:
        """Convert time in force to Bybit format"""
        tif_map = {
            'GTC': 'GTC',
            'IOC': 'IOC',
            'FOK': 'FOK',
            'POST_ONLY': 'PostOnly'
        }
        return tif_map.get(tif.upper(), 'GTC')
    
    def _format_quantity(self, symbol: str, quantity: float) -> str:
        """Format quantity according to symbol rules"""
        # Get symbol info
        if symbol in self.client.instruments:
            qty_step = self.client.instruments[symbol].get('qtyStep', 0.000001)
            precision = len(str(qty_step).split('.')[-1]) if '.' in str(qty_step) else 0
            return f"{quantity:.{precision}f}"
        
        return str(quantity)
    
    def _format_price(self, symbol: str, price: float) -> str:
        """Format price according to symbol rules"""
        # Get symbol info
        if symbol in self.client.instruments:
            tick_size = self.client.instruments[symbol].get('tickSize', 0.01)
            precision = len(str(tick_size).split('.')[-1]) if '.' in str(tick_size) else 0
            return f"{price:.{precision}f}"
        
        return str(price)