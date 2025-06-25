"""
AITHORIX Hyperliquid Perpetual Trading
High-level perpetual trading interface for Hyperliquid
"""

import logging
from datetime import datetime, timezone
from typing import Dict, List, Optional, Any
from decimal import Decimal

from ...base_exchange import (
    Order, Trade, Balance, Position, OrderType, OrderSide,
    OrderStatus, TimeInForce, PositionSide
)
from .perpetual_api import HyperliquidPerpetualAPI

logger = logging.getLogger(__name__)


class HyperliquidPerpetualTrading:
    """
    Hyperliquid perpetual trading implementation
    """
    
    def __init__(self, client):
        self.client = client
        self.api = HyperliquidPerpetualAPI(client)
    
    async def place_order(
        self,
        symbol: str,
        side: str,
        order_type: OrderType,
        size: float,
        price: Optional[float] = None,
        params: Optional[Dict] = None
    ) -> Order:
        """Place a perpetual order"""
        try:
            # Get asset info for decimals
            asset_info = self.client.asset_info.get(symbol, {})
            sz_decimals = asset_info.get('szDecimals', 3)
            
            # Build order request
            order_request = {
                "a": symbol,  # Asset
                "b": side.lower() == "buy",  # Is buy
                "p": str(price) if price else None,  # Price
                "s": self._format_size(size, sz_decimals),  # Size
                "r": params.get('reduce_only', False) if params else False,  # Reduce only
                "t": self._get_order_type(order_type, params),  # Order type
            }
            
            # Add limit order specific fields
            if order_type == OrderType.LIMIT:
                order_request["t"]["limit"] = {
                    "tif": params.get('time_in_force', 'Gtc') if params else 'Gtc'
                }
            
            # Add trigger order fields
            if order_type in [OrderType.STOP, OrderType.STOP_LIMIT]:
                trigger_price = params.get('trigger_price') if params else None
                if not trigger_price:
                    raise ValueError("Trigger price required for stop orders")
                
                order_request["t"]["trigger"] = {
                    "triggerPx": str(trigger_price),
                    "isMarket": order_type == OrderType.STOP,
                    "tpsl": params.get('tpsl', 'tp') if params else 'tp'  # tp or sl
                }
            
            # Place order
            response = await self.api.place_order(order_request)
            
            # Parse response
            if response.get('status') == 'ok':
                statuses = response.get('response', {}).get('data', {}).get('statuses', [])
                if statuses and statuses[0].get('error'):
                    raise Exception(f"Order failed: {statuses[0].get('error')}")
                
                # Get order details
                if statuses and 'resting' in statuses[0]:
                    order_info = statuses[0]['resting']
                    return self._parse_order(order_info, symbol, side, order_type, size, price)
                elif statuses and 'filled' in statuses[0]:
                    # Immediate fill
                    fill_info = statuses[0]['filled']
                    return self._parse_filled_order(fill_info, symbol, side, order_type, size)
                else:
                    raise Exception("Unknown order response format")
            else:
                raise Exception(f"Order failed: {response}")
                
        except Exception as e:
            logger.error(f"Failed to place perpetual order: {str(e)}")
            raise
    
    async def cancel_order(self, order_id: str, symbol: Optional[str] = None) -> bool:
        """Cancel a perpetual order"""
        try:
            if not symbol:
                # Try to find symbol from open orders
                open_orders = await self.get_open_orders()
                for order in open_orders:
                    if str(order.order_id) == order_id:
                        symbol = order.symbol
                        break
                
                if not symbol:
                    raise ValueError("Symbol required for order cancellation")
            
            response = await self.api.cancel_order(symbol, int(order_id))
            
            if response.get('status') == 'ok':
                statuses = response.get('response', {}).get('data', {}).get('statuses', [])
                return len(statuses) > 0 and not statuses[0].get('error')
            
            return False
            
        except Exception as e:
            logger.error(f"Failed to cancel order: {str(e)}")
            return False
    
    async def get_order(self, order_id: str, symbol: Optional[str] = None) -> Order:
        """Get order details"""
        # Hyperliquid doesn't have a direct get order endpoint
        # Need to search in open orders or order history
        open_orders = await self.get_open_orders(symbol)
        
        for order in open_orders:
            if str(order.order_id) == order_id:
                return order
        
        # If not found in open orders, check recent fills
        if self.client.user_address:
            fills = await self.api.get_user_fills(self.client.user_address)
            for fill in fills:
                if str(fill.get('oid')) == order_id:
                    return self._parse_fill_to_order(fill)
        
        raise Exception(f"Order {order_id} not found")
    
    async def get_open_orders(self, symbol: Optional[str] = None) -> List[Order]:
        """Get open orders"""
        try:
            if not self.client.user_address:
                return []
            
            response = await self.api.get_open_orders(self.client.user_address)
            
            orders = []
            for order_data in response:
                if symbol and order_data['coin'] != symbol:
                    continue
                
                order = self._parse_open_order(order_data)
                orders.append(order)
            
            return orders
            
        except Exception as e:
            logger.error(f"Failed to get open orders: {str(e)}")
            return []
    
    async def get_order_history(
        self,
        symbol: Optional[str] = None,
        limit: int = 100,
        start_time: Optional[datetime] = None
    ) -> List[Order]:
        """Get order history from fills"""
        try:
            if not self.client.user_address:
                return []
            
            # Get fills
            start_ts = int(start_time.timestamp() * 1000) if start_time else None
            fills = await self.api.get_user_fills(
                self.client.user_address,
                start_time=start_ts
            )
            
            orders = []
            seen_order_ids = set()
            
            for fill in fills[:limit]:
                if symbol and fill['coin'] != symbol:
                    continue
                
                order_id = fill['oid']
                if order_id not in seen_order_ids:
                    seen_order_ids.add(order_id)
                    order = self._parse_fill_to_order(fill)
                    orders.append(order)
            
            return orders
            
        except Exception as e:
            logger.error(f"Failed to get order history: {str(e)}")
            return []
    
    async def get_balance(self) -> Dict[str, Balance]:
        """Get account balance"""
        try:
            if not self.client.user_address:
                return {}
            
            # Get user state
            user_state = await self.api.get_user_state(self.client.user_address)
            
            balances = {}
            
            # Get margin summary
            margin_summary = user_state.get('marginSummary', {})
            account_value = float(margin_summary.get('accountValue', 0))
            total_margin_used = float(margin_summary.get('totalMarginUsed', 0))
            
            # USDC is the only currency on Hyperliquid
            balances['USDC'] = Balance(
                currency='USDC',
                free=account_value - total_margin_used,
                used=total_margin_used,
                total=account_value
            )
            
            return balances
            
        except Exception as e:
            logger.error(f"Failed to get balance: {str(e)}")
            return {}
    
    async def get_open_positions(self) -> List[Position]:
        """Get open positions"""
        try:
            if not self.client.user_address:
                return []
            
            # Get user state
            user_state = await self.api.get_user_state(self.client.user_address)
            
            positions = []
            asset_positions = user_state.get('assetPositions', [])
            
            for pos_data in asset_positions:
                position = pos_data.get('position')
                if position and float(position.get('szi', 0)) != 0:
                    positions.append(self._parse_position(pos_data))
            
            return positions
            
        except Exception as e:
            logger.error(f"Failed to get open positions: {str(e)}")
            return []
    
    async def get_position(self, symbol: str) -> Optional[Position]:
        """Get specific position"""
        positions = await self.get_open_positions()
        
        for position in positions:
            if position.symbol == symbol:
                return position
        
        return None
    
    async def update_leverage(self, symbol: str, leverage: int, is_cross: bool = True) -> bool:
        """Update leverage for a symbol"""
        try:
            response = await self.api.update_leverage(symbol, leverage, is_cross)
            return response.get('status') == 'ok'
        except Exception as e:
            logger.error(f"Failed to update leverage: {str(e)}")
            return False
    
    def _get_order_type(self, order_type: OrderType, params: Optional[Dict]) -> Dict[str, Any]:
        """Convert order type to Hyperliquid format"""
        if order_type == OrderType.MARKET:
            return {"limit": {"tif": "Ioc"}}  # Market orders are IOC in Hyperliquid
        elif order_type == OrderType.LIMIT:
            tif = "Gtc"  # Good till cancelled
            if params:
                if params.get('post_only'):
                    tif = "Alo"  # Add liquidity only (post-only)
                elif params.get('time_in_force') == 'IOC':
                    tif = "Ioc"
            return {"limit": {"tif": tif}}
        elif order_type in [OrderType.STOP, OrderType.STOP_LIMIT]:
            return {"trigger": {}}  # Trigger details added separately
        else:
            return {"limit": {"tif": "Gtc"}}
    
    def _format_size(self, size: float, sz_decimals: int) -> str:
        """Format size according to asset decimals"""
        return f"{size:.{sz_decimals}f}".rstrip('0').rstrip('.')
    
    def _parse_order(self, order_info: Dict[str, Any], symbol: str, side: str, 
                    order_type: OrderType, size: float, price: Optional[float]) -> Order:
        """Parse order response"""
        return Order(
            order_id=str(order_info['oid']),
            client_order_id=None,
            symbol=symbol,
            side=OrderSide.BUY if side.lower() == 'buy' else OrderSide.SELL,
            order_type=order_type,
            status=OrderStatus.OPEN,
            price=price,
            size=size,
            filled_size=0.0,
            average_price=None,
            fee=0.0,
            fee_currency='USDC',
            time_in_force=TimeInForce.GTC,
            created_at=datetime.now(timezone.utc),
            updated_at=datetime.now(timezone.utc)
        )
    
    def _parse_filled_order(self, fill_info: Dict[str, Any], symbol: str, side: str,
                           order_type: OrderType, size: float) -> Order:
        """Parse immediately filled order"""
        total_size = float(fill_info['totalSz'])
        avg_price = float(fill_info['avgPx'])
        
        return Order(
            order_id=str(fill_info['oid']),
            client_order_id=None,
            symbol=symbol,
            side=OrderSide.BUY if side.lower() == 'buy' else OrderSide.SELL,
            order_type=order_type,
            status=OrderStatus.FILLED,
            price=avg_price,
            size=size,
            filled_size=total_size,
            average_price=avg_price,
            fee=0.0,  # Would need to calculate from fills
            fee_currency='USDC',
            time_in_force=TimeInForce.IOC if order_type == OrderType.MARKET else TimeInForce.GTC,
            created_at=datetime.now(timezone.utc),
            updated_at=datetime.now(timezone.utc)
        )
    
    def _parse_open_order(self, order_data: Dict[str, Any]) -> Order:
        """Parse open order data"""
        return Order(
            order_id=str(order_data['oid']),
            client_order_id=None,
            symbol=order_data['coin'],
            side=OrderSide.BUY if order_data['side'] == 'B' else OrderSide.SELL,
            order_type=OrderType.LIMIT,  # Hyperliquid open orders are limit orders
            status=OrderStatus.OPEN,
            price=float(order_data['limitPx']),
            size=float(order_data['origSz']),
            filled_size=float(order_data['origSz']) - float(order_data['sz']),
            average_price=None,
            fee=0.0,
            fee_currency='USDC',
            time_in_force=TimeInForce.GTC,
            created_at=datetime.fromtimestamp(order_data['timestamp'] / 1000, tz=timezone.utc),
            updated_at=datetime.now(timezone.utc)
        )
    
    def _parse_fill_to_order(self, fill: Dict[str, Any]) -> Order:
        """Convert fill to order"""
        return Order(
            order_id=str(fill['oid']),
            client_order_id=None,
            symbol=fill['coin'],
            side=OrderSide.BUY if fill['side'] == 'B' else OrderSide.SELL,
            order_type=OrderType.LIMIT if fill.get('crossed') else OrderType.MARKET,
            status=OrderStatus.FILLED,
            price=float(fill['px']),
            size=float(fill['sz']),
            filled_size=float(fill['sz']),
            average_price=float(fill['px']),
            fee=float(fill['fee']),
            fee_currency='USDC',
            time_in_force=TimeInForce.GTC,
            created_at=datetime.fromtimestamp(fill['time'] / 1000, tz=timezone.utc),
            updated_at=datetime.fromtimestamp(fill['time'] / 1000, tz=timezone.utc)
        )
    
    def _parse_position(self, pos_data: Dict[str, Any]) -> Position:
        """Parse position data"""
        position = pos_data['position']
        
        size = float(position['szi'])
        entry_price = float(position['entryPx'])
        mark_price = float(pos_data.get('markPx', entry_price))
        
        # Calculate PnL
        unrealized_pnl = float(position.get('unrealizedPnl', 0))
        realized_pnl = float(position.get('returnOnEquity', 0)) * abs(size) * entry_price
        
        return Position(
            symbol=pos_data['coin'],
            side=PositionSide.LONG if size > 0 else PositionSide.SHORT,
            size=abs(size),
            entry_price=entry_price,
            mark_price=mark_price,
            liquidation_price=float(position.get('liquidationPx', 0)),
            unrealized_pnl=unrealized_pnl,
            realized_pnl=realized_pnl,
            margin=float(position.get('marginUsed', 0)),
            leverage=int(pos_data.get('leverage', {}).get('value', 1)),
            created_at=datetime.now(timezone.utc),
            updated_at=datetime.now(timezone.utc),
            metadata={
                'return_on_equity': float(position.get('returnOnEquity', 0)),
                'funding_accrued': float(position.get('cumulativeFunding', {}).get('sinceOpen', 0))
            }
        )