"""
AITHORIX OKX Unified API
Low-level API wrapper for OKX unified trading
"""

import logging
from typing import Dict, List, Optional, Any

logger = logging.getLogger(__name__)


class OKXUnifiedAPI:
    """
    OKX Unified API wrapper for all trading types
    """
    
    def __init__(self, client):
        self.client = client
        
    # Trading endpoints
    async def place_order(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """Place order"""
        return await self.client._post("/api/v5/trade/order", data=params, signed=True)
    
    async def place_multiple_orders(self, params: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Place multiple orders"""
        return await self.client._post("/api/v5/trade/batch-orders", data=params, signed=True)
    
    async def cancel_order(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """Cancel order"""
        return await self.client._post("/api/v5/trade/cancel-order", data=params, signed=True)
    
    async def cancel_multiple_orders(self, params: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Cancel multiple orders"""
        return await self.client._post("/api/v5/trade/cancel-batch-orders", data=params, signed=True)
    
    async def amend_order(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """Amend order"""
        return await self.client._post("/api/v5/trade/amend-order", data=params, signed=True)
    
    async def amend_multiple_orders(self, params: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Amend multiple orders"""
        return await self.client._post("/api/v5/trade/amend-batch-orders", data=params, signed=True)
    
    async def close_position(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """Close position"""
        return await self.client._post("/api/v5/trade/close-position", data=params, signed=True)
    
    async def get_order(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """Get order details"""
        return await self.client._get("/api/v5/trade/order", params=params, signed=True)
    
    async def get_order_list(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """Get order list"""
        return await self.client._get("/api/v5/trade/orders-pending", params=params, signed=True)
    
    async def get_orders_history(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """Get orders history (7 days)"""
        return await self.client._get("/api/v5/trade/orders-history", params=params, signed=True)
    
    async def get_orders_history_archive(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """Get orders history archive (3 months)"""
        return await self.client._get("/api/v5/trade/orders-history-archive", params=params, signed=True)
    
    async def get_fills(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """Get transaction details"""
        return await self.client._get("/api/v5/trade/fills", params=params, signed=True)
    
    async def get_fills_history(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """Get transaction details history"""
        return await self.client._get("/api/v5/trade/fills-history", params=params, signed=True)
    
    # Account endpoints
    async def get_balance(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """Get balance"""
        return await self.client._get("/api/v5/account/balance", params=params, signed=True)
    
    async def get_positions(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """Get positions"""
        return await self.client._get("/api/v5/account/positions", params=params, signed=True)
    
    async def get_positions_history(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """Get positions history"""
        return await self.client._get("/api/v5/account/positions-history", params=params, signed=True)
    
    async def get_account_position_risk(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """Get account and position risk"""
        return await self.client._get("/api/v5/account/account-position-risk", params=params, signed=True)
    
    async def get_bills(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """Get bills details"""
        return await self.client._get("/api/v5/account/bills", params=params, signed=True)
    
    async def get_bills_archive(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """Get bills details archive"""
        return await self.client._get("/api/v5/account/bills-archive", params=params, signed=True)
    
    async def get_config(self) -> Dict[str, Any]:
        """Get account configuration"""
        return await self.client._get("/api/v5/account/config", signed=True)
    
    async def set_position_mode(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """Set position mode"""
        return await self.client._post("/api/v5/account/set-position-mode", data=params, signed=True)
    
    async def set_leverage(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """Set leverage"""
        return await self.client._post("/api/v5/account/set-leverage", data=params, signed=True)
    
    async def get_max_size(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """Get maximum buy/sell amount or open amount"""
        return await self.client._get("/api/v5/account/max-size", params=params, signed=True)
    
    async def get_max_avail_size(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """Get maximum available tradable amount"""
        return await self.client._get("/api/v5/account/max-avail-size", params=params, signed=True)
    
    async def adjust_margin(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """Increase/decrease margin"""
        return await self.client._post("/api/v5/account/position/margin-balance", data=params, signed=True)
    
    async def get_leverage(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """Get leverage"""
        return await self.client._get("/api/v5/account/leverage-info", params=params, signed=True)
    
    async def get_trade_fee(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """Get fee rates"""
        return await self.client._get("/api/v5/account/trade-fee", params=params, signed=True)
    
    async def get_interest_accrued(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """Get interest accrued"""
        return await self.client._get("/api/v5/account/interest-accrued", params=params, signed=True)
    
    async def get_interest_rate(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """Get interest rate"""
        return await self.client._get("/api/v5/public/interest-rate-loan-quota", params=params, signed=True)
    
    async def set_greeks(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """Set Greeks (PA/BS)"""
        return await self.client._post("/api/v5/account/set-greeks", data=params, signed=True)
    
    async def set_isolated_mode(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """Set isolated mode"""
        return await self.client._post("/api/v5/account/set-isolated-mode", data=params, signed=True)
    
    async def get_max_loan(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """Get maximum loan"""
        return await self.client._get("/api/v5/account/max-loan", params=params, signed=True)
    
    # Market data endpoints
    async def get_instruments(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """Get instruments"""
        return await self.client._get("/api/v5/public/instruments", params=params)
    
    async def get_delivery_exercise_history(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """Get delivery/exercise history"""
        return await self.client._get("/api/v5/public/delivery-exercise-history", params=params)
    
    async def get_open_interest(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """Get open interest"""
        return await self.client._get("/api/v5/public/open-interest", params=params)
    
    async def get_funding_rate(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """Get funding rate"""
        return await self.client._get("/api/v5/public/funding-rate", params=params)
    
    async def get_funding_rate_history(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """Get funding rate history"""
        return await self.client._get("/api/v5/public/funding-rate-history", params=params)
    
    async def get_price_limit(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """Get price limit"""
        return await self.client._get("/api/v5/public/price-limit", params=params)
    
    async def get_opt_summary(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """Get option market data"""
        return await self.client._get("/api/v5/public/opt-summary", params=params)
    
    async def get_estimated_price(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """Get estimated delivery/exercise price"""
        return await self.client._get("/api/v5/public/estimated-price", params=params)
    
    async def get_discount_rate_interest_free_quota(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """Get discount rate and interest-free quota"""
        return await self.client._get("/api/v5/public/discount-rate-interest-free-quota", params=params)
    
    async def get_time(self) -> Dict[str, Any]:
        """Get system time"""
        return await self.client._get("/api/v5/public/time")
    
    async def get_liquidation_orders(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """Get liquidation orders"""
        return await self.client._get("/api/v5/public/liquidation-orders", params=params)
    
    async def get_mark_price(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """Get mark price"""
        return await self.client._get("/api/v5/public/mark-price", params=params)
    
    async def get_position_tiers(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """Get position tiers"""
        return await self.client._get("/api/v5/public/position-tiers", params=params)
    
    async def get_interest_rate_loan_quota(self) -> Dict[str, Any]:
        """Get interest rate and loan quota"""
        return await self.client._get("/api/v5/public/interest-rate-loan-quota")
    
    async def get_underlying(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """Get underlying"""
        return await self.client._get("/api/v5/public/underlying", params=params)
    
    async def get_insurance_fund(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """Get insurance fund"""
        return await self.client._get("/api/v5/public/insurance-fund", params=params)
    
    async def unit_convert(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """Unit convert"""
        return await self.client._get("/api/v5/public/convert-contract-coin", params=params)