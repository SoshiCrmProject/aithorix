"""
AITHORIX Binance Options API
Low-level API wrapper for Binance options trading
"""

import logging
from typing import Dict, List, Optional, Any

logger = logging.getLogger(__name__)


class BinanceOptionsAPI:
    """
    Binance Options API wrapper
    Note: Binance options (COIN-M) have limited API support
    """
    
    def __init__(self, client):
        self.client = client
        self.base_url = "/dapi/v1"  # COIN-M futures (used for options-like trading)
    
    # Order endpoints
    async def new_order(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """Place new options order"""
        return await self.client._post(f"{self.base_url}/order", data=params, signed=True)
    
    async def cancel_order(self, symbol: str, order_id: Optional[str] = None,
                          orig_client_order_id: Optional[str] = None) -> Dict[str, Any]:
        """Cancel options order"""
        params = {'symbol': symbol}
        if order_id:
            params['orderId'] = order_id
        if orig_client_order_id:
            params['origClientOrderId'] = orig_client_order_id
            
        return await self.client._delete(f"{self.base_url}/order", params=params, signed=True)
    
    async def get_order(self, symbol: str, order_id: Optional[str] = None,
                       orig_client_order_id: Optional[str] = None) -> Dict[str, Any]:
        """Query options order"""
        params = {'symbol': symbol}
        if order_id:
            params['orderId'] = order_id
        if orig_client_order_id:
            params['origClientOrderId'] = orig_client_order_id
            
        return await self.client._get(f"{self.base_url}/order", params=params, signed=True)
    
    async def get_open_orders(self, symbol: Optional[str] = None) -> List[Dict[str, Any]]:
        """Get open options orders"""
        params = {}
        if symbol:
            params['symbol'] = symbol
            
        return await self.client._get(f"{self.base_url}/openOrders", params=params, signed=True)
    
    # Position endpoints
    async def get_position_risk(self, symbol: Optional[str] = None) -> List[Dict[str, Any]]:
        """Get current position information"""
        params = {}
        if symbol:
            params['symbol'] = symbol
            
        return await self.client._get(f"{self.base_url}/positionRisk", params=params, signed=True)
    
    # Account endpoints
    async def get_account(self) -> Dict[str, Any]:
        """Get options account information"""
        return await self.client._get(f"{self.base_url}/account", signed=True)
    
    async def get_balance(self) -> List[Dict[str, Any]]:
        """Get options account balance"""
        return await self.client._get(f"{self.base_url}/balance", signed=True)
    
    # Options specific endpoints (European style)
    async def get_option_info(self) -> Dict[str, Any]:
        """Get options contract information"""
        return await self.client._get("/eapi/v1/exchangeInfo")
    
    async def get_option_mark_price(self, symbol: Optional[str] = None) -> List[Dict[str, Any]]:
        """Get options mark price"""
        params = {}
        if symbol:
            params['symbol'] = symbol
            
        return await self.client._get("/eapi/v1/mark", params=params)
    
    async def get_option_ticker(self, symbol: Optional[str] = None) -> List[Dict[str, Any]]:
        """Get options ticker"""
        params = {}
        if symbol:
            params['symbol'] = symbol
            
        return await self.client._get("/eapi/v1/ticker", params=params)