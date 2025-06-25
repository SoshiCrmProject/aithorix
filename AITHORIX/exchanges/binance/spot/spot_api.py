"""
AITHORIX Binance Spot API
Low-level API wrapper for Binance spot trading
"""

import logging
from typing import Dict, List, Optional, Any
from datetime import datetime

logger = logging.getLogger(__name__)


class BinanceSpotAPI:
    """
    Binance Spot API wrapper
    """
    
    def __init__(self, client):
        self.client = client
    
    # Order endpoints
    async def new_order(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """Place new order"""
        return await self.client._post("/api/v3/order", data=params, signed=True)
    
    async def cancel_order(self, symbol: str, order_id: Optional[str] = None, 
                          orig_client_order_id: Optional[str] = None) -> Dict[str, Any]:
        """Cancel order"""
        params = {'symbol': symbol}
        if order_id:
            params['orderId'] = order_id
        if orig_client_order_id:
            params['origClientOrderId'] = orig_client_order_id
            
        return await self.client._delete("/api/v3/order", params=params, signed=True)
    
    async def get_order(self, symbol: str, order_id: Optional[str] = None,
                       orig_client_order_id: Optional[str] = None) -> Dict[str, Any]:
        """Query order"""
        params = {'symbol': symbol}
        if order_id:
            params['orderId'] = order_id
        if orig_client_order_id:
            params['origClientOrderId'] = orig_client_order_id
            
        return await self.client._get("/api/v3/order", params=params, signed=True)
    
    async def get_open_orders(self, symbol: Optional[str] = None) -> List[Dict[str, Any]]:
        """Get open orders"""
        params = {}
        if symbol:
            params['symbol'] = symbol
            
        return await self.client._get("/api/v3/openOrders", params=params, signed=True)
    
    async def get_all_orders(self, symbol: str, order_id: Optional[int] = None,
                            start_time: Optional[int] = None, end_time: Optional[int] = None,
                            limit: int = 500) -> List[Dict[str, Any]]:
        """Get all orders"""
        params = {
            'symbol': symbol,
            'limit': limit
        }
        if order_id:
            params['orderId'] = order_id
        if start_time:
            params['startTime'] = start_time
        if end_time:
            params['endTime'] = end_time
            
        return await self.client._get("/api/v3/allOrders", params=params, signed=True)
    
    # Account endpoints
    async def get_account(self) -> Dict[str, Any]:
        """Get account information"""
        return await self.client._get("/api/v3/account", signed=True)
    
    async def get_my_trades(self, symbol: str, order_id: Optional[int] = None,
                           start_time: Optional[int] = None, end_time: Optional[int] = None,
                           from_id: Optional[int] = None, limit: int = 500) -> List[Dict[str, Any]]:
        """Get account trades"""
        params = {
            'symbol': symbol,
            'limit': limit
        }
        if order_id:
            params['orderId'] = order_id
        if start_time:
            params['startTime'] = start_time
        if end_time:
            params['endTime'] = end_time
        if from_id:
            params['fromId'] = from_id
            
        return await self.client._get("/api/v3/myTrades", params=params, signed=True)
    
    # OCO endpoints
    async def new_oco_order(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """Place new OCO order"""
        return await self.client._post("/api/v3/order/oco", data=params, signed=True)
    
    async def cancel_oco_order(self, symbol: str, order_list_id: Optional[int] = None,
                              list_client_order_id: Optional[str] = None,
                              new_client_order_id: Optional[str] = None) -> Dict[str, Any]:
        """Cancel OCO order"""
        params = {'symbol': symbol}
        if order_list_id:
            params['orderListId'] = order_list_id
        if list_client_order_id:
            params['listClientOrderId'] = list_client_order_id
        if new_client_order_id:
            params['newClientOrderId'] = new_client_order_id
            
        return await self.client._delete("/api/v3/orderList", params=params, signed=True)
    
    async def get_oco_order(self, order_list_id: Optional[int] = None,
                           orig_client_order_id: Optional[str] = None) -> Dict[str, Any]:
        """Query OCO order"""
        params = {}
        if order_list_id:
            params['orderListId'] = order_list_id
        if orig_client_order_id:
            params['origClientOrderId'] = orig_client_order_id
            
        return await self.client._get("/api/v3/orderList", params=params, signed=True)
    
    async def get_open_oco_orders(self) -> List[Dict[str, Any]]:
        """Get open OCO orders"""
        return await self.client._get("/api/v3/openOrderList", signed=True)