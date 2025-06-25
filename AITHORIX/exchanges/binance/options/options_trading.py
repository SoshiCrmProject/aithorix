"""
AITHORIX Binance Options Trading
High-level options trading interface for Binance
Note: Limited implementation as Binance options are still developing
"""

import logging
from datetime import datetime, timezone
from typing import Dict, List, Optional, Any
import uuid

from ...base_exchange import (
    Order, Trade, Balance, Position, OrderType, OrderSide,
    OrderStatus, TimeInForce, PositionSide
)
from .options_api import BinanceOptionsAPI

logger = logging.getLogger(__name__)


class BinanceOptionsTrading:
    """
    Binance options trading implementation
    Currently limited as Binance options API is still evolving
    """
    
    def __init__(self, client):
        self.client = client
        self.api = BinanceOptionsAPI(client)
        self._options_info = None
    
    async def place_order(
        self,
        symbol: str,
        side: str,
        order_type: OrderType,
        size: float,
        price: Optional[float] = None,
        params: Optional[Dict] = None
    ) -> Order:
        """Place an options order"""
        try:
            # Check if options trading is available
            if not await self._check_options_available():
                raise NotImplementedError("Options trading not fully available on Binance")
            
            # For now, route to COIN-M futures as alternative
            logger.warning("Binance options API limited, using COIN-M futures as alternative")
            
            # Prepare order parameters
            order_params = {
                'symbol': symbol,
                'side': side.upper(),
                'type': self.client._convert_order_type(order_type),
                'quantity': str(size),
                'newClientOrderId': str(uuid.uuid4())
            }
            
            if price:
                order_params['price'] = str(price)
                order_params['timeInForce'] = 'GTC'
            
            # Place order
            response = await self.api.new_order(order_params)
            
            # Convert to Order object
            return self._parse_order(response)
            
        except Exception as e:
            logger.error(f"Failed to place options order: {str(e)}")
            raise
    
    async def get_option_chains(self, underlying: str) -> Dict[str, Any]:
        """Get available option chains for an underlying asset"""
        try:
            # Get option info
            if not self._options_info:
                self._options_info = await self.api.get_option_info()
            
            # Filter by underlying
            chains = {}
            for symbol_info in self._options_info.get('symbols', []):
                if symbol_info.get('underlying') == underlying:
                    expiry = symbol_info.get('expiryDate')
                    strike = symbol_info.get('strikePrice')
                    option_type = symbol_info.get('optionType')  # CALL or PUT
                    
                    if expiry not in chains:
                        chains[expiry] = {'calls': {}, 'puts': {}}
                    
                    if option_type == 'CALL':
                        chains[expiry]['calls'][strike] = symbol_info
                    else:
                        chains[expiry]['puts'][strike] = symbol_info
            
            return chains
            
        except Exception as e:
            logger.error(f"Failed to get option chains: {str(e)}")
            return {}
    
    async def get_option_greeks(self, symbol: str) -> Dict[str, float]:
        """Get option Greeks (if available)"""
        try:
            # Binance doesn't provide Greeks directly
            # Would need to calculate them
            logger.warning("Option Greeks not directly available from Binance API")
            
            # Get mark price and calculate basic Greeks
            mark_data = await self.api.get_option_mark_price(symbol)
            
            if mark_data:
                return {
                    'delta': 0.0,  # Would need to calculate
                    'gamma': 0.0,
                    'theta': 0.0,
                    'vega': 0.0,
                    'rho': 0.0,
                    'iv': float(mark_data[0].get('iv', 0))  # Implied volatility if available
                }
            
            return {}
            
        except Exception as e:
            logger.error(f"Failed to get option Greeks: {str(e)}")
            return {}
    
    async def _check_options_available(self) -> bool:
        """Check if options trading is available"""
        try:
            # Try to get options info
            info = await self.api.get_option_info()
            return len(info.get('symbols', [])) > 0
        except Exception:
            return False
    
    def _parse_order(self, data: Dict[str, Any]) -> Order:
        """Parse options order response"""
        return Order(
            order_id=str(data['orderId']),
            client_order_id=data.get('clientOrderId'),
            symbol=data['symbol'],
            side=OrderSide.BUY if data['side'] == 'BUY' else OrderSide.SELL,
            order_type=self.client._parse_order_type(data['type']),
            status=self._parse_order_status(data['status']),
            price=float(data['price']) if data.get('price') else None,
            size=float(data['origQty']),
            filled_size=float(data['executedQty']),
            average_price=float(data.get('avgPrice', 0)) if float(data.get('avgPrice', 0)) > 0 else None,
            fee=0.0,  # Calculate from trades
            fee_currency='BTC',  # COIN-M uses BTC
            time_in_force=TimeInForce.GTC,
            created_at=datetime.fromtimestamp(data['time'] / 1000, tz=timezone.utc),
            updated_at=datetime.fromtimestamp(data.get('updateTime', data['time']) / 1000, tz=timezone.utc)
        )
    
    def _parse_order_status(self, status: str) -> OrderStatus:
        """Parse order status"""
        status_map = {
            'NEW': OrderStatus.OPEN,
            'PARTIALLY_FILLED': OrderStatus.PARTIALLY_FILLED,
            'FILLED': OrderStatus.FILLED,
            'CANCELED': OrderStatus.CANCELLED,
            'REJECTED': OrderStatus.REJECTED,
            'EXPIRED': OrderStatus.EXPIRED
        }
        return status_map.get(status, OrderStatus.OPEN)