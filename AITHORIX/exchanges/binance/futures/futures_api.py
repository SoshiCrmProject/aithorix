"""
AITHORIX Binance Futures API
Low-level API wrapper for Binance futures trading
"""

import logging
from typing import Dict, List, Optional, Any

logger = logging.getLogger(__name__)


class BinanceFuturesAPI:
    """
    Binance Futures API wrapper
    """
    
    def __init__(self, client):
        self.client = client
        self.base_url = "/fapi/v1"  # USD-M futures
    
    # Order endpoints
    async def new_order(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """Place new futures order"""
        return await self.client._post(f"{self.base_url}/order", data=params, signed=True)
    
    async def cancel_order(self, symbol: str, order_id: Optional[str] = None,
                          orig_client_order_id: Optional[str] = None) -> Dict[str, Any]:
        """Cancel futures order"""
        params = {'symbol': symbol}
        if order_id:
            params['orderId'] = order_id
        if orig_client_order_id:
            params['origClientOrderId'] = orig_client_order_id
            
        return await self.client._delete(f"{self.base_url}/order", params=params, signed=True)
    
    async def cancel_all_orders(self, symbol: str) -> Dict[str, Any]:
        """Cancel all open orders for symbol"""
        params = {'symbol': symbol}
        return await self.client._delete(f"{self.base_url}/allOpenOrders", params=params, signed=True)
    
    async def get_order(self, symbol: str, order_id: Optional[str] = None,
                       orig_client_order_id: Optional[str] = None) -> Dict[str, Any]:
        """Query futures order"""
        params = {'symbol': symbol}
        if order_id:
            params['orderId'] = order_id
        if orig_client_order_id:
            params['origClientOrderId'] = orig_client_order_id
            
        return await self.client._get(f"{self.base_url}/order", params=params, signed=True)
    
    async def get_open_orders(self, symbol: Optional[str] = None) -> List[Dict[str, Any]]:
        """Get open futures orders"""
        params = {}
        if symbol:
            params['symbol'] = symbol
            
        return await self.client._get(f"{self.base_url}/openOrders", params=params, signed=True)
    
    async def get_all_orders(self, symbol: str, order_id: Optional[int] = None,
                            start_time: Optional[int] = None, end_time: Optional[int] = None,
                            limit: int = 500) -> List[Dict[str, Any]]:
        """Get all futures orders"""
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
            
        return await self.client._get(f"{self.base_url}/allOrders", params=params, signed=True)
    
    # Position endpoints
    async def get_position_risk(self, symbol: Optional[str] = None) -> List[Dict[str, Any]]:
        """Get current position information"""
        params = {}
        if symbol:
            params['symbol'] = symbol
            
        return await self.client._get(f"{self.base_url}/positionRisk", params=params, signed=True)
    
    async def change_position_mode(self, dual_side_position: bool) -> Dict[str, Any]:
        """Change position mode (hedge/one-way)"""
        params = {'dualSidePosition': 'true' if dual_side_position else 'false'}
        return await self.client._post(f"{self.base_url}/positionSide/dual", data=params, signed=True)
    
    async def get_position_mode(self) -> Dict[str, Any]:
        """Get current position mode"""
        return await self.client._get(f"{self.base_url}/positionSide/dual", signed=True)
    
    # Leverage endpoints
    async def change_leverage(self, symbol: str, leverage: int) -> Dict[str, Any]:
        """Change leverage"""
        params = {
            'symbol': symbol,
            'leverage': leverage
        }
        return await self.client._post(f"{self.base_url}/leverage", data=params, signed=True)
    
    async def get_leverage_bracket(self, symbol: Optional[str] = None) -> List[Dict[str, Any]]:
        """Get leverage bracket"""
        params = {}
        if symbol:
            params['symbol'] = symbol
            
        return await self.client._get(f"{self.base_url}/leverageBracket", params=params, signed=True)
    
    # Margin type endpoints
    async def change_margin_type(self, symbol: str, margin_type: str) -> Dict[str, Any]:
        """Change margin type (ISOLATED/CROSSED)"""
        params = {
            'symbol': symbol,
            'marginType': margin_type
        }
        return await self.client._post(f"{self.base_url}/marginType", data=params, signed=True)
    
    # Account endpoints
    async def get_account(self) -> Dict[str, Any]:
        """Get futures account information"""
        return await self.client._get(f"{self.base_url}/account", signed=True)
    
    async def get_balance(self) -> List[Dict[str, Any]]:
        """Get futures account balance"""
        return await self.client._get(f"{self.base_url}/balance", signed=True)
    
    async def get_income_history(self, symbol: Optional[str] = None, income_type: Optional[str] = None,
                                 start_time: Optional[int] = None, end_time: Optional[int] = None,
                                 limit: int = 100) -> List[Dict[str, Any]]:
        """Get income history"""
        params = {'limit': limit}
        if symbol:
            params['symbol'] = symbol
        if income_type:
            params['incomeType'] = income_type
        if start_time:
            params['startTime'] = start_time
        if end_time:
            params['endTime'] = end_time
            
        return await self.client._get(f"{self.base_url}/income", params=params, signed=True)