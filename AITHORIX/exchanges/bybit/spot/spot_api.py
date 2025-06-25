"""
AITHORIX Bybit Spot API
Low-level API wrapper for Bybit spot trading
"""

import logging
from typing import Dict, List, Optional, Any

logger = logging.getLogger(__name__)


class BybitSpotAPI:
    """
    Bybit Spot API wrapper using unified V5 API
    """
    
    def __init__(self, client):
        self.client = client
        self.base_url = "/v5"
    
    # Order endpoints
    async def place_order(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """Place spot order"""
        return await self.client._post(f"{self.base_url}/order/create", data=params, signed=True)
    
    async def cancel_order(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """Cancel spot order"""
        return await self.client._post(f"{self.base_url}/order/cancel", data=params, signed=True)
    
    async def cancel_all_orders(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """Cancel all orders"""
        return await self.client._post(f"{self.base_url}/order/cancel-all", data=params, signed=True)
    
    async def amend_order(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """Amend order"""
        return await self.client._post(f"{self.base_url}/order/amend", data=params, signed=True)
    
    async def get_open_orders(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """Get open orders"""
        return await self.client._get(f"{self.base_url}/order/realtime", params=params, signed=True)
    
    async def get_order_history(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """Get order history"""
        return await self.client._get(f"{self.base_url}/order/history", params=params, signed=True)
    
    async def get_trade_history(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """Get trade history"""
        return await self.client._get(f"{self.base_url}/execution/list", params=params, signed=True)
    
    # Account endpoints
    async def get_wallet_balance(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """Get wallet balance"""
        return await self.client._get(f"{self.base_url}/account/wallet-balance", params=params, signed=True)
    
    async def get_account_info(self) -> Dict[str, Any]:
        """Get account info"""
        return await self.client._get(f"{self.base_url}/account/info", signed=True)
    
    async def get_fee_rate(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """Get fee rates"""
        return await self.client._get(f"{self.base_url}/account/fee-rate", params=params, signed=True)
    
    # Market data endpoints
    async def get_instruments(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """Get instrument info"""
        return await self.client._get(f"{self.base_url}/market/instruments-info", params=params)
    
    async def get_orderbook(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """Get order book"""
        return await self.client._get(f"{self.base_url}/market/orderbook", params=params)
    
    async def get_tickers(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """Get tickers"""
        return await self.client._get(f"{self.base_url}/market/tickers", params=params)
    
    async def get_kline(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """Get kline data"""
        return await self.client._get(f"{self.base_url}/market/kline", params=params)
    
    async def get_recent_trades(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """Get recent trades"""
        return await self.client._get(f"{self.base_url}/market/recent-trade", params=params)