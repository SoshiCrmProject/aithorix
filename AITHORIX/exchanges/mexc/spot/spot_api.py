"""
AITHORIX MEXC Spot API
Low-level API wrapper for MEXC spot trading
"""

import logging
from typing import Dict, List, Optional, Any
from datetime import datetime

logger = logging.getLogger(__name__)


class MEXCSpotAPI:
    """
    MEXC Spot API wrapper
    """
    
    def __init__(self, client):
        self.client = client
    
    # Market Data Endpoints (Public)
    async def get_exchange_info(self) -> Dict[str, Any]:
        """Get exchange trading rules and symbol information"""
        return await self.client._get("/api/v3/exchangeInfo")
    
    async def get_ticker_24hr(self, symbol: Optional[str] = None) -> Any:
        """Get 24hr ticker price change statistics"""
        params = {}
        if symbol:
            params['symbol'] = symbol
        return await self.client._get("/api/v3/ticker/24hr", params=params)
    
    async def get_depth(self, symbol: str, limit: int = 100) -> Dict[str, Any]:
        """Get order book"""
        params = {
            'symbol': symbol,
            'limit': limit
        }
        return await self.client._get("/api/v3/depth", params=params)
    
    async def get_trades(self, symbol: str, limit: int = 500) -> List[Dict[str, Any]]:
        """Get recent trades"""
        params = {
            'symbol': symbol,
            'limit': limit
        }
        return await self.client._get("/api/v3/trades", params=params)
    
    async def get_klines(self, symbol: str, interval: str, 
                        startTime: Optional[int] = None,
                        endTime: Optional[int] = None,
                        limit: int = 500) -> List[List]:
        """Get kline/candlestick data"""
        params = {
            'symbol': symbol,
            'interval': interval,
            'limit': limit
        }
        if startTime:
            params['startTime'] = startTime
        if endTime:
            params['endTime'] = endTime
        return await self.client._get("/api/v3/klines", params=params)
    
    # Account Endpoints (Private)
    async def get_account(self) -> Dict[str, Any]:
        """Get current account information"""
        return await self.client._get("/api/v3/account", signed=True)
    
    async def get_my_trades(self, symbol: str, orderId: Optional[int] = None,
                           startTime: Optional[int] = None,
                           endTime: Optional[int] = None,
                           limit: int = 500) -> List[Dict[str, Any]]:
        """Get trades for a specific account and symbol"""
        params = {
            'symbol': symbol,
            'limit': limit
        }
        if orderId:
            params['orderId'] = orderId
        if startTime:
            params['startTime'] = startTime
        if endTime:
            params['endTime'] = endTime
        return await self.client._get("/api/v3/myTrades", params=params, signed=True)
    
    # Order Endpoints (Private)
    async def new_order(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """Send in a new order"""
        return await self.client._post("/api/v3/order", data=params, signed=True)
    
    async def cancel_order(self, symbol: str, orderId: Optional[int] = None,
                          origClientOrderId: Optional[str] = None) -> Dict[str, Any]:
        """Cancel an active order"""
        params = {'symbol': symbol}
        if orderId:
            params['orderId'] = orderId
        if origClientOrderId:
            params['origClientOrderId'] = origClientOrderId
        return await self.client._delete("/api/v3/order", params=params, signed=True)
    
    async def cancel_all_orders(self, symbol: str) -> List[Dict[str, Any]]:
        """Cancel all open orders on a symbol"""
        params = {'symbol': symbol}
        return await self.client._delete("/api/v3/openOrders", params=params, signed=True)
    
    async def get_order(self, symbol: str, orderId: Optional[int] = None,
                       origClientOrderId: Optional[str] = None) -> Dict[str, Any]:
        """Check an order's status"""
        params = {'symbol': symbol}
        if orderId:
            params['orderId'] = orderId
        if origClientOrderId:
            params['origClientOrderId'] = origClientOrderId
        return await self.client._get("/api/v3/order", params=params, signed=True)
    
    async def get_open_orders(self, symbol: Optional[str] = None) -> List[Dict[str, Any]]:
        """Get all open orders"""
        params = {}
        if symbol:
            params['symbol'] = symbol
        return await self.client._get("/api/v3/openOrders", params=params, signed=True)
    
    async def get_all_orders(self, symbol: str, orderId: Optional[int] = None,
                            startTime: Optional[int] = None,
                            endTime: Optional[int] = None,
                            limit: int = 500) -> List[Dict[str, Any]]:
        """Get all account orders; active, canceled, or filled"""
        params = {
            'symbol': symbol,
            'limit': limit
        }
        if orderId:
            params['orderId'] = orderId
        if startTime:
            params['startTime'] = startTime
        if endTime:
            params['endTime'] = endTime
        return await self.client._get("/api/v3/allOrders", params=params, signed=True)
    
    # User Data Stream Endpoints
    async def create_listen_key(self) -> Dict[str, Any]:
        """Start a new user data stream"""
        return await self.client._post("/api/v3/userDataStream", signed=True)
    
    async def keepalive_listen_key(self, listenKey: str) -> Dict[str, Any]:
        """Keepalive a user data stream"""
        params = {'listenKey': listenKey}
        return await self.client._put("/api/v3/userDataStream", params=params, signed=True)
    
    async def close_listen_key(self, listenKey: str) -> Dict[str, Any]:
        """Close a user data stream"""
        params = {'listenKey': listenKey}
        return await self.client._delete("/api/v3/userDataStream", params=params, signed=True)