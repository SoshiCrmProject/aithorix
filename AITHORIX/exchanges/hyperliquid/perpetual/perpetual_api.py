"""
AITHORIX Hyperliquid Perpetual API
Low-level API wrapper for Hyperliquid perpetual trading
"""

import logging
from typing import Dict, List, Optional, Any

logger = logging.getLogger(__name__)


class HyperliquidPerpetualAPI:
    """
    Hyperliquid Perpetual API wrapper
    """
    
    def __init__(self, client):
        self.client = client
    
    # Trading endpoints
    async def place_order(self, order_request: Dict[str, Any]) -> Dict[str, Any]:
        """Place perpetual order"""
        request_data = {
            "action": {
                "type": "order",
                "orders": [order_request]
            }
        }
        return await self.client._post("/exchange", data=request_data, signed=True)
    
    async def cancel_order(self, asset: str, oid: int) -> Dict[str, Any]:
        """Cancel perpetual order"""
        request_data = {
            "action": {
                "type": "cancel",
                "cancels": [{
                    "a": asset,
                    "o": oid
                }]
            }
        }
        return await self.client._post("/exchange", data=request_data, signed=True)
    
    async def cancel_all_orders(self, asset: Optional[str] = None) -> Dict[str, Any]:
        """Cancel all orders"""
        cancels = []
        if asset:
            # Cancel for specific asset
            open_orders = await self.get_open_orders(self.client.user_address)
            for order in open_orders:
                if order['coin'] == asset:
                    cancels.append({
                        "a": order['coin'],
                        "o": order['oid']
                    })
        else:
            # Cancel all
            open_orders = await self.get_open_orders(self.client.user_address)
            for order in open_orders:
                cancels.append({
                    "a": order['coin'],
                    "o": order['oid']
                })
        
        if not cancels:
            return {"status": "ok", "response": {"data": {"statuses": []}}}
        
        request_data = {
            "action": {
                "type": "cancel",
                "cancels": cancels
            }
        }
        return await self.client._post("/exchange", data=request_data, signed=True)
    
    async def modify_order(self, asset: str, oid: int, new_size: float, 
                          new_price: float) -> Dict[str, Any]:
        """Modify existing order"""
        request_data = {
            "action": {
                "type": "modify",
                "modifies": [{
                    "a": asset,
                    "o": oid,
                    "sz": new_size,
                    "px": new_price
                }]
            }
        }
        return await self.client._post("/exchange", data=request_data, signed=True)
    
    async def update_leverage(self, asset: str, leverage: int, 
                             is_cross: bool = True) -> Dict[str, Any]:
        """Update leverage for asset"""
        request_data = {
            "action": {
                "type": "updateLeverage",
                "asset": asset,
                "isCross": is_cross,
                "leverage": leverage
            }
        }
        return await self.client._post("/exchange", data=request_data, signed=True)
    
    async def update_isolated_margin(self, asset: str, amount: float) -> Dict[str, Any]:
        """Update isolated margin"""
        request_data = {
            "action": {
                "type": "updateIsolatedMargin",
                "asset": asset,
                "amount": amount
            }
        }
        return await self.client._post("/exchange", data=request_data, signed=True)
    
    # Info endpoints
    async def get_user_state(self, user: str) -> Dict[str, Any]:
        """Get user account state"""
        request_data = {
            "type": "clearinghouseState",
            "user": user
        }
        return await self.client._post("/info", data=request_data)
    
    async def get_open_orders(self, user: str) -> List[Dict[str, Any]]:
        """Get open orders"""
        request_data = {
            "type": "openOrders",
            "user": user
        }
        return await self.client._post("/info", data=request_data)
    
    async def get_user_fills(self, user: str, start_time: Optional[int] = None,
                            end_time: Optional[int] = None) -> List[Dict[str, Any]]:
        """Get user fills/trades"""
        request_data = {
            "type": "userFills",
            "user": user
        }
        if start_time:
            request_data["startTime"] = start_time
        if end_time:
            request_data["endTime"] = end_time
            
        return await self.client._post("/info", data=request_data)
    
    async def get_user_funding(self, user: str, start_time: Optional[int] = None,
                              end_time: Optional[int] = None) -> List[Dict[str, Any]]:
        """Get user funding payments"""
        request_data = {
            "type": "userFunding",
            "user": user
        }
        if start_time:
            request_data["startTime"] = start_time
        if end_time:
            request_data["endTime"] = end_time
            
        return await self.client._post("/info", data=request_data)
    
    async def get_asset_contexts(self) -> List[Dict[str, Any]]:
        """Get asset contexts (market info)"""
        request_data = {"type": "assetContexts"}
        return await self.client._post("/info", data=request_data)
    
    async def get_funding_history(self, asset: str, start_time: Optional[int] = None,
                                 end_time: Optional[int] = None) -> List[Dict[str, Any]]:
        """Get funding rate history"""
        request_data = {
            "type": "fundingHistory",
            "coin": asset
        }
        if start_time:
            request_data["startTime"] = start_time
        if end_time:
            request_data["endTime"] = end_time
            
        return await self.client._post("/info", data=request_data)