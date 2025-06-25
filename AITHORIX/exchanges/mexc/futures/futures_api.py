"""
AITHORIX MEXC Futures API
Low-level API wrapper for MEXC contract trading
"""

import logging
from typing import Dict, List, Optional, Any

logger = logging.getLogger(__name__)


class MEXCFuturesAPI:
    """
    MEXC Futures API wrapper for contract trading
    """
    
    def __init__(self, client):
        self.client = client
        self.base_url = "/contract/v1"
    
    # Contract information
    async def get_contract_detail(self) -> Dict[str, Any]:
        """Get all contract details"""
        return await self.client._get(f"{self.base_url}/detail")
    
    async def get_contract_depth(self, symbol: str, limit: int = 20) -> Dict[str, Any]:
        """Get contract order book"""
        params = {
            'symbol': symbol,
            'limit': limit
        }
        return await self.client._get(f"{self.base_url}/depth/{symbol}", params=params)
    
    async def get_contract_ticker(self, symbol: Optional[str] = None) -> Dict[str, Any]:
        """Get contract ticker"""
        if symbol:
            return await self.client._get(f"{self.base_url}/ticker/{symbol}")
        return await self.client._get(f"{self.base_url}/ticker")
    
    async def get_funding_rate(self, symbol: str) -> Dict[str, Any]:
        """Get funding rate"""
        return await self.client._get(f"{self.base_url}/funding_rate/{symbol}")
    
    async def get_klines(self, symbol: str, interval: str, 
                        start_time: Optional[int] = None,
                        end_time: Optional[int] = None,
                        limit: int = 500) -> Dict[str, Any]:
        """Get contract klines"""
        params = {
            'symbol': symbol,
            'interval': interval,
            'limit': limit
        }
        
        if start_time:
            params['start'] = start_time
        if end_time:
            params['end'] = end_time
            
        return await self.client._get(f"{self.base_url}/kline/{symbol}", params=params)
    
    # Trading endpoints
    async def place_order(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """Place contract order"""
        return await self.client._post(f"{self.base_url}/order/submit", data=params, signed=True)
    
    async def cancel_order(self, order_ids: List[str]) -> Dict[str, Any]:
        """Cancel contract orders"""
        data = {
            'order_ids': ','.join(order_ids) if isinstance(order_ids, list) else order_ids
        }
        return await self.client._post(f"{self.base_url}/order/cancel", data=data, signed=True)
    
    async def cancel_all_orders(self, symbol: Optional[str] = None) -> Dict[str, Any]:
        """Cancel all orders"""
        data = {}
        if symbol:
            data['symbol'] = symbol
        return await self.client._post(f"{self.base_url}/order/cancel_all", data=data, signed=True)
    
    async def get_order(self, order_id: str) -> Dict[str, Any]:
        """Get order details"""
        params = {'order_id': order_id}
        return await self.client._get(f"{self.base_url}/order/get/{order_id}", params=params, signed=True)
    
    async def get_open_orders(self, symbol: Optional[str] = None,
                             page_num: int = 1, page_size: int = 50) -> Dict[str, Any]:
        """Get open orders"""
        params = {
            'page_num': page_num,
            'page_size': page_size
        }
        if symbol:
            params['symbol'] = symbol
            
        return await self.client._get(f"{self.base_url}/order/list/open_orders/{page_num}/{page_size}", 
                                     params=params, signed=True)
    
    async def get_order_history(self, symbol: Optional[str] = None,
                               states: Optional[str] = None,
                               start_time: Optional[int] = None,
                               end_time: Optional[int] = None,
                               page_num: int = 1, page_size: int = 50) -> Dict[str, Any]:
        """Get historical orders"""
        params = {
            'page_num': page_num,
            'page_size': page_size
        }
        
        if symbol:
            params['symbol'] = symbol
        if states:
            params['states'] = states
        if start_time:
            params['start_time'] = start_time
        if end_time:
            params['end_time'] = end_time
            
        return await self.client._get(f"{self.base_url}/order/list/history_orders/{page_num}/{page_size}",
                                     params=params, signed=True)
    
    # Position endpoints
    async def get_positions(self, symbol: Optional[str] = None) -> Dict[str, Any]:
        """Get current positions"""
        params = {}
        if symbol:
            params['symbol'] = symbol
            
        return await self.client._get(f"{self.base_url}/position/list/open_positions", 
                                     params=params, signed=True)
    
    async def get_position_history(self, symbol: Optional[str] = None,
                                  page_num: int = 1, page_size: int = 50) -> Dict[str, Any]:
        """Get position history"""
        params = {
            'page_num': page_num,
            'page_size': page_size
        }
        if symbol:
            params['symbol'] = symbol
            
        return await self.client._get(f"{self.base_url}/position/list/history_positions/{page_num}/{page_size}",
                                     params=params, signed=True)
    
    async def adjust_leverage(self, symbol: str, leverage: int, position_type: int) -> Dict[str, Any]:
        """Adjust position leverage"""
        data = {
            'symbol': symbol,
            'leverage': leverage,
            'openType': position_type  # 1: isolated, 2: cross
        }
        return await self.client._post(f"{self.base_url}/position/change_leverage", data=data, signed=True)
    
    async def adjust_margin(self, symbol: str, margin: float, position_id: str,
                           adjust_type: int) -> Dict[str, Any]:
        """Adjust position margin"""
        data = {
            'symbol': symbol,
            'margin': margin,
            'positionId': position_id,
            'type': adjust_type  # 1: increase, 2: decrease
        }
        return await self.client._post(f"{self.base_url}/position/change_margin", data=data, signed=True)
    
    # Account endpoints
    async def get_account_info(self) -> Dict[str, Any]:
        """Get contract account info"""
        return await self.client._get(f"{self.base_url}/private/account/assets", signed=True)
    
    async def get_trade_history(self, symbol: Optional[str] = None,
                               start_time: Optional[int] = None,
                               end_time: Optional[int] = None,
                               page_num: int = 1, page_size: int = 50) -> Dict[str, Any]:
        """Get trade history"""
        params = {
            'page_num': page_num,
            'page_size': page_size
        }
        
        if symbol:
            params['symbol'] = symbol
        if start_time:
            params['start_time'] = start_time
        if end_time:
            params['end_time'] = end_time
            
        return await self.client._get(f"{self.base_url}/order/list/trades/{page_num}/{page_size}",
                                     params=params, signed=True)