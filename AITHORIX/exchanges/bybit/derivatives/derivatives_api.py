"""
AITHORIX Bybit Derivatives API
Low-level API wrapper for Bybit derivatives trading (linear, inverse, options)
"""

import logging
from typing import Dict, List, Optional, Any

logger = logging.getLogger(__name__)


class BybitDerivativesAPI:
    """
    Bybit Derivatives API wrapper using unified V5 API
    """
    
    def __init__(self, client):
        self.client = client
        self.base_url = "/v5"
    
    # Order endpoints
    async def place_order(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """Place derivatives order"""
        return await self.client._post(f"{self.base_url}/order/create", data=params, signed=True)
    
    async def cancel_order(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """Cancel derivatives order"""
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
    
    # Position endpoints
    async def get_positions(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """Get positions"""
        return await self.client._get(f"{self.base_url}/position/list", params=params, signed=True)
    
    async def set_leverage(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """Set leverage"""
        return await self.client._post(f"{self.base_url}/position/set-leverage", data=params, signed=True)
    
    async def switch_margin_mode(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """Switch margin mode (isolated/cross)"""
        return await self.client._post(f"{self.base_url}/position/switch-isolated", data=params, signed=True)
    
    async def set_trading_stop(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """Set trading stop (TP/SL)"""
        return await self.client._post(f"{self.base_url}/position/trading-stop", data=params, signed=True)
    
    async def set_risk_limit(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """Set risk limit"""
        return await self.client._post(f"{self.base_url}/position/set-risk-limit", data=params, signed=True)
    
    async def add_margin(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """Add/reduce margin"""
        return await self.client._post(f"{self.base_url}/position/add-margin", data=params, signed=True)
    
    # Account endpoints
    async def get_wallet_balance(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """Get wallet balance"""
        return await self.client._get(f"{self.base_url}/account/wallet-balance", params=params, signed=True)
    
    async def get_fee_rate(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """Get fee rates"""
        return await self.client._get(f"{self.base_url}/account/fee-rate", params=params, signed=True)
    
    async def get_position_info(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """Get position P&L records"""
        return await self.client._get(f"{self.base_url}/position/closed-pnl", params=params, signed=True)
    
    # Execution endpoints
    async def get_trade_history(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """Get trade history"""
        return await self.client._get(f"{self.base_url}/execution/list", params=params, signed=True)
    
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
    
    async def get_funding_rate(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """Get funding rate history"""
        return await self.client._get(f"{self.base_url}/market/funding/history", params=params)
    
    async def get_mark_price_kline(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """Get mark price kline"""
        return await self.client._get(f"{self.base_url}/market/mark-price-kline", params=params)
    
    async def get_index_price_kline(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """Get index price kline"""
        return await self.client._get(f"{self.base_url}/market/index-price-kline", params=params)
    
    async def get_open_interest(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """Get open interest"""
        return await self.client._get(f"{self.base_url}/market/open-interest", params=params)
    
    # Option specific endpoints
    async def get_option_delivery_price(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """Get option delivery price"""
        return await self.client._get(f"{self.base_url}/market/delivery-price", params=params)
    
    async def get_volatility_index(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """Get historical volatility"""
        return await self.client._get(f"{self.base_url}/market/historical-volatility", params=params)