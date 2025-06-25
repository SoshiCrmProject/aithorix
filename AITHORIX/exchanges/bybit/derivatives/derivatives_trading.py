"""
AITHORIX Bybit Derivatives Trading Implementation
High-level derivatives trading functionality for Bybit (linear, inverse, options)
"""

import logging
from datetime import datetime, timezone
from typing import Dict, List, Optional, Any
import asyncio
import uuid

from ...base_exchange import (
    Order, Trade, Position, Balance, OrderType, OrderSide,
    OrderStatus, TimeInForce, PositionSide
)
from .derivatives_api import BybitDerivativesAPI

logger = logging.getLogger(__name__)


class BybitDerivativesTrading:
    """
    Bybit derivatives trading implementation
    """
    
    def __init__(self, client):
        self.client = client
        self.api = BybitDerivativesAPI(client)
        self._position_cache: Dict[str, Position] = {}
        self._balance_cache: Dict[str, Balance] = {}
        self._last_balance_update = 0
        
    async def place_order(
        self,
        symbol: str,
        side: str,
        order_type: OrderType,
        size: float,
        price: Optional[float] = None,
        params: Optional[Dict] = None
    ) -> Order:
        """Place a derivatives order"""
        try:
            params = params or {}
            
            # Determine category
            category = self._get_category(symbol)
            
            # Generate client order ID if not provided
            client_order_id = params.get('client_order_id', str(uuid.uuid4()))
            
            # Build order parameters
            order_params = {
                'category': category,
                'symbol': symbol,
                'orderType': self._get_order_type(order_type),
                'side': side.capitalize(),
                'qty': self._format_quantity(symbol, size),
                'orderLinkId': client_order_id
            }
            
            # Add price for limit orders
            if order_type == OrderType.LIMIT:
                if not price:
                    raise ValueError("Price required for limit orders")
                order_params['price'] = self._format_price(symbol, price)
            
            # Time in force
            if params.get('time_in_force'):
                order_params['timeInForce'] = self._convert_time_in_force(params['time_in_force'])
            elif order_type == OrderType.LIMIT:
                order_params['timeInForce'] = 'GTC'
            
            # Position parameters
            if params.get('position_idx') is not None:
                order_params['positionIdx'] = params['position_idx']  # 0: one-way, 1: hedge-buy, 2: hedge-sell
            else:
                order_params['positionIdx'] = 0  # Default to one-way mode
            
            # Reduce only
            if params.get('reduce_only'):
                order_params['reduceOnly'] = True
            
            # Close on trigger
            if params.get('close_on_trigger'):
                order_params['closeOnTrigger'] = True
            
            # Stop orders
            if order_type in [OrderType.STOP, OrderType.STOP_LIMIT]:
                order_params['triggerPrice'] = params.get('stop_price', price)
                order_params['triggerBy'] = params.get('trigger_by', 'LastPrice')
                order_params['triggerDirection'] = 1 if side.upper() == 'BUY' else 2
                
                if order_type == OrderType.STOP:
                    order_params['orderType'] = 'Market'
                else:
                    order_params['orderType'] = 'Limit'
            
            # Take profit / Stop loss
            if params.get('take_profit'):
                order_params['takeProfit'] = str(params['take_profit'])
                order_params['tpTriggerBy'] = params.get('tp_trigger_by', 'LastPrice')
            
            if params.get('stop_loss'):
                order_params['stopLoss'] = str(params['stop_loss'])
                order_params['slTriggerBy'] = params.get('sl_trigger_by', 'LastPrice')
            
            # Place order
            response = await self.api.place_order(order_params)
            
            if response['retCode'] != 0:
                raise Exception(f"Order failed: {response['retMsg']}")
            
            order_data = response['result']
            
            # Create order object
            return Order(
                id=order_data['orderId'],
                client_order_id=order_data['orderLinkId'],
                exchange='Bybit',
                symbol=symbol,
                type=order_type,
                side=OrderSide.BUY if side.upper() == 'BUY' else OrderSide.SELL,
                size=size,
                price=price,
                status=self._parse_order_status(order_data['orderStatus']),
                filled_size=0.0,
                average_price=None,
                fee=0.0,
                fee_currency=self._get_settle_coin(symbol),
                timestamp=datetime.now(timezone.utc),
                raw_data=order_data
            )
            
        except Exception as e:
            logger.error(f"Error placing derivatives order: {str(e)}")
            raise
    
    async def cancel_order(self, order_id: str, symbol: str) -> bool:
        """Cancel a derivatives order"""
        try:
            category = self._get_category(symbol)
            
            params = {
                'category': category,
                'symbol': symbol,
                'orderId': order_id
            }
            
            response = await self.api.cancel_order(params)
            
            if response['retCode'] == 0:
                return True
            
            logger.error(f"Failed to cancel order: {response['retMsg']}")
            return False
            
        except Exception as e:
            logger.error(f"Error canceling order: {str(e)}")
            return False
    
    async def get_order(self, order_id: str, symbol: str) -> Optional[Order]:
        """Get order details"""
        try:
            category = self._get_category(symbol)
            
            # Check open orders first
            params = {
                'category': category,
                'symbol': symbol,
                'orderId': order_id
            }
            
            response = await self.api.get_open_orders(params)
            
            if response['retCode'] == 0 and response['result']['list']:
                order_data = response['result']['list'][0]
                return self._parse_order(order_data)
            
            # Check order history
            history_response = await self.api.get_order_history(params)
            
            if history_response['retCode'] == 0 and history_response['result']['list']:
                order_data = history_response['result']['list'][0]
                return self._parse_order(order_data)
            
            return None
            
        except Exception as e:
            logger.error(f"Error getting order: {str(e)}")
            return None
    
    async def get_open_orders(self, symbol: Optional[str] = None) -> List[Order]:
        """Get all open orders"""
        try:
            orders = []
            
            # Get orders for each category
            for category in ['linear', 'inverse', 'option']:
                params = {'category': category}
                if symbol:
                    params['symbol'] = symbol
                else:
                    params['limit'] = 50
                
                try:
                    response = await self.api.get_open_orders(params)
                    
                    if response['retCode'] == 0:
                        for order_data in response['result']['list']:
                            order = self._parse_order(order_data)
                            if order:
                                orders.append(order)
                except:
                    # Category might not be available
                    continue
            
            return orders
            
        except Exception as e:
            logger.error(f"Error getting open orders: {str(e)}")
            return []
    
    async def get_balance(self) -> Dict[str, Balance]:
        """Get derivatives account balance"""
        try:
            # Cache balance for 1 second
            current_time = asyncio.get_event_loop().time()
            if current_time - self._last_balance_update < 1:
                return self._balance_cache
            
            balances = {}
            
            # Get unified account balance
            params = {'accountType': 'UNIFIED'}
            
            response = await self.api.get_wallet_balance(params)
            
            if response['retCode'] == 0:
                for account in response['result']['list']:
                    for coin_data in account['coin']:
                        currency = coin_data['coin']
                        
                        balance = Balance(
                            currency=currency,
                            free=float(coin_data.get('availableToWithdraw', 0)),
                            used=float(coin_data.get('locked', 0)),
                            total=float(coin_data['walletBalance']),
                            exchange='Bybit'
                        )
                        
                        balances[currency] = balance
            
            # Also try contract account if available
            try:
                contract_params = {'accountType': 'CONTRACT'}
                contract_response = await self.api.get_wallet_balance(contract_params)
                
                if contract_response['retCode'] == 0:
                    for account in contract_response['result']['list']:
                        for coin_data in account['coin']:
                            currency = f"{coin_data['coin']}_CONTRACT"
                            
                            balance = Balance(
                                currency=currency,
                                free=float(coin_data.get('availableBalance', 0)),
                                used=float(coin_data['walletBalance']) - float(coin_data.get('availableBalance', 0)),
                                total=float(coin_data['walletBalance']),
                                exchange='Bybit'
                            )
                            
                            balances[currency] = balance
            except:
                pass
            
            self._balance_cache = balances
            self._last_balance_update = current_time
            
            return balances
            
        except Exception as e:
            logger.error(f"Error getting balance: {str(e)}")
            return self._balance_cache
    
    async def get_open_positions(self) -> List[Position]:
        """Get all open positions"""
        try:
            positions = []
            
            # Get positions for each category
            for category in ['linear', 'inverse', 'option']:
                params = {
                    'category': category,
                    'settleCoin': 'USDT' if category == 'linear' else 'BTC'
                }
                
                try:
                    response = await self.api.get_positions(params)
                    
                    if response['retCode'] == 0:
                        for pos_data in response['result']['list']:
                            if float(pos_data['size']) > 0:
                                position = self._parse_position(pos_data, category)
                                if position:
                                    positions.append(position)
                                    self._position_cache[pos_data['symbol']] = position
                except:
                    continue
            
            return positions
            
        except Exception as e:
            logger.error(f"Error getting positions: {str(e)}")
            return []
    
    async def get_position(self, symbol: str) -> Optional[Position]:
        """Get specific position"""
        try:
            # Try cache first
            if symbol in self._position_cache:
                return self._position_cache[symbol]
            
            category = self._get_category(symbol)
            
            params = {
                'category': category,
                'symbol': symbol
            }
            
            response = await self.api.get_positions(params)
            
            if response['retCode'] == 0 and response['result']['list']:
                pos_data = response['result']['list'][0]
                if float(pos_data['size']) > 0:
                    position = self._parse_position(pos_data, category)
                    if position:
                        self._position_cache[symbol] = position
                    return position
            
            return None
            
        except Exception as e:
            logger.error(f"Error getting position: {str(e)}")
            return None
    
    async def close_position(self, symbol: str, size: Optional[float] = None) -> Order:
        """Close a position"""
        try:
            position = await self.get_position(symbol)
            if not position:
                raise ValueError(f"No position found for {symbol}")
            
            # Determine close size
            close_size = size or position.size
            
            # Determine close side
            close_side = 'SELL' if position.side == PositionSide.LONG else 'BUY'
            
            # Place close order
            return await self.place_order(
                symbol=symbol,
                side=close_side,
                order_type=OrderType.MARKET,
                size=close_size,
                params={'reduce_only': True}
            )
            
        except Exception as e:
            logger.error(f"Error closing position: {str(e)}")
            raise
    
    async def set_leverage(self, symbol: str, leverage: int) -> bool:
        """Set position leverage"""
        try:
            category = self._get_category(symbol)
            
            params = {
                'category': category,
                'symbol': symbol,
                'buyLeverage': str(leverage),
                'sellLeverage': str(leverage)
            }
            
            response = await self.api.set_leverage(params)
            return response['retCode'] == 0
            
        except Exception as e:
            logger.error(f"Error setting leverage: {str(e)}")
            return False
    
    async def set_margin_mode(self, symbol: str, isolated: bool) -> bool:
        """Set margin mode (isolated/cross)"""
        try:
            category = self._get_category(symbol)
            
            params = {
                'category': category,
                'symbol': symbol,
                'tradeMode': 1 if isolated else 0,  # 0: cross, 1: isolated
                'buyLeverage': '10',
                'sellLeverage': '10'
            }
            
            response = await self.api.switch_margin_mode(params)
            return response['retCode'] == 0
            
        except Exception as e:
            logger.error(f"Error setting margin mode: {str(e)}")
            return False
    
    async def get_order_history(
        self,
        symbol: Optional[str] = None,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        limit: int = 50
    ) -> List[Order]:
        """Get order history"""
        try:
            orders = []
            
            # Get orders for each category
            for category in ['linear', 'inverse', 'option']:
                params = {
                    'category': category,
                    'limit': min(limit, 50)
                }
                
                if symbol:
                    params['symbol'] = symbol
                if start_time:
                    params['startTime'] = int(start_time.timestamp() * 1000)
                if end_time:
                    params['endTime'] = int(end_time.timestamp() * 1000)
                
                try:
                    response = await self.api.get_order_history(params)
                    
                    if response['retCode'] == 0:
                        for order_data in response['result']['list']:
                            order = self._parse_order(order_data)
                            if order:
                                orders.append(order)
                except:
                    continue
            
            return orders
            
        except Exception as e:
            logger.error(f"Error getting order history: {str(e)}")
            return []
    
    async def get_trades(
        self,
        symbol: Optional[str] = None,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        limit: int = 50
    ) -> List[Trade]:
        """Get trade history"""
        try:
            trades = []
            
            # Determine category
            if symbol:
                category = self._get_category(symbol)
                categories = [category]
            else:
                categories = ['linear', 'inverse', 'option']
            
            for category in categories:
                params = {
                    'category': category,
                    'limit': min(limit, 50)
                }
                
                if symbol:
                    params['symbol'] = symbol
                if start_time:
                    params['startTime'] = int(start_time.timestamp() * 1000)
                if end_time:
                    params['endTime'] = int(end_time.timestamp() * 1000)
                
                try:
                    response = await self.api.get_trade_history(params)
                    
                    if response['retCode'] == 0:
                        for trade_data in response['result']['list']:
                            trades.append(Trade(
                                id=trade_data['execId'],
                                order_id=trade_data['orderId'],
                                symbol=trade_data['symbol'],
                                side=OrderSide.BUY if trade_data['side'] == 'Buy' else OrderSide.SELL,
                                price=float(trade_data['execPrice']),
                                size=float(trade_data['execQty']),
                                fee=float(trade_data['execFee']),
                                fee_currency=trade_data.get('feeCurrency', self._get_settle_coin(trade_data['symbol'])),
                                timestamp=datetime.fromtimestamp(int(trade_data['execTime']) / 1000, tz=timezone.utc),
                                is_maker=trade_data.get('isMaker', False)
                            ))
                except:
                    continue
            
            return trades
            
        except Exception as e:
            logger.error(f"Error getting trades: {str(e)}")
            return []
    
    # Helper methods
    def _get_category(self, symbol: str) -> str:
        """Get category for symbol"""
        if symbol in self.client.instruments:
            return self.client.instruments[symbol]['type']
        
        # Guess based on symbol
        if symbol.endswith('USDT') or symbol.endswith('PERP'):
            return 'linear'
        elif symbol.endswith('USD'):
            return 'inverse'
        else:
            return 'option'
    
    def _get_settle_coin(self, symbol: str) -> str:
        """Get settlement coin for symbol"""
        if symbol in self.client.instruments:
            return self.client.instruments[symbol].get('settleCoin', 'USDT')
        
        if symbol.endswith('USDT'):
            return 'USDT'
        elif symbol.endswith('USD'):
            return 'USD'
        else:
            return 'USDC'
    
    def _parse_order(self, order_data: Dict[str, Any]) -> Optional[Order]:
        """Parse order data into Order object"""
        try:
            return Order(
                id=order_data['orderId'],
                client_order_id=order_data.get('orderLinkId'),
                exchange='Bybit',
                symbol=order_data['symbol'],
                type=self._parse_order_type(order_data['orderType'], order_data.get('stopOrderType')),
                side=OrderSide.BUY if order_data['side'] == 'Buy' else OrderSide.SELL,
                size=float(order_data['qty']),
                price=float(order_data['price']) if order_data['price'] else None,
                status=self._parse_order_status(order_data['orderStatus']),
                filled_size=float(order_data.get('cumExecQty', 0)),
                average_price=float(order_data['avgPrice']) if order_data.get('avgPrice') else None,
                fee=float(order_data.get('cumExecFee', 0)),
                fee_currency=self._get_settle_coin(order_data['symbol']),
                timestamp=datetime.fromtimestamp(int(order_data['createdTime']) / 1000, tz=timezone.utc),
                raw_data=order_data
            )
        except Exception as e:
            logger.error(f"Error parsing order: {str(e)}")
            return None
    
    def _parse_position(self, pos_data: Dict[str, Any], category: str) -> Optional[Position]:
        """Parse position data into Position object"""
        try:
            # Determine position side
            if category in ['linear', 'inverse']:
                if pos_data.get('positionIdx') == 1:
                    side = PositionSide.LONG
                elif pos_data.get('positionIdx') == 2:
                    side = PositionSide.SHORT
                else:
                    # One-way mode
                    side = PositionSide.LONG if pos_data['side'] == 'Buy' else PositionSide.SHORT
            else:
                # Options
                side = PositionSide.LONG if float(pos_data['size']) > 0 else PositionSide.SHORT
            
            return Position(
                symbol=pos_data['symbol'],
                side=side,
                size=abs(float(pos_data['size'])),
                entry_price=float(pos_data['avgPrice']),
                mark_price=float(pos_data['markPrice']),
                unrealized_pnl=float(pos_data['unrealisedPnl']),
                realized_pnl=float(pos_data.get('realisedPnl', 0)),
                margin=float(pos_data.get('positionIM', 0)),
                leverage=float(pos_data.get('leverage', 1)),
                liquidation_price=float(pos_data.get('liqPrice', 0)),
                exchange='Bybit',
                raw_data=pos_data
            )
        except Exception as e:
            logger.error(f"Error parsing position: {str(e)}")
            return None
    
    def _get_order_type(self, order_type: OrderType) -> str:
        """Convert order type to Bybit format"""
        type_map = {
            OrderType.MARKET: 'Market',
            OrderType.LIMIT: 'Limit',
            OrderType.STOP: 'Market',
            OrderType.STOP_LIMIT: 'Limit',
            OrderType.POST_ONLY: 'Limit',
            OrderType.FOK: 'Limit',
            OrderType.IOC: 'Limit'
        }
        return type_map.get(order_type, 'Limit')
    
    def _parse_order_type(self, type_str: str, stop_type: Optional[str] = None) -> OrderType:
        """Parse Bybit order type"""
        if stop_type:
            if type_str == 'Market':
                return OrderType.STOP
            else:
                return OrderType.STOP_LIMIT
        
        type_map = {
            'Market': OrderType.MARKET,
            'Limit': OrderType.LIMIT
        }
        return type_map.get(type_str, OrderType.LIMIT)
    
    def _parse_order_status(self, status: str) -> OrderStatus:
        """Parse Bybit order status"""
        status_map = {
            'Created': OrderStatus.OPEN,
            'New': OrderStatus.OPEN,
            'PartiallyFilled': OrderStatus.PARTIALLY_FILLED,
            'Filled': OrderStatus.FILLED,
            'Cancelled': OrderStatus.CANCELLED,
            'PartiallyFilledCanceled': OrderStatus.CANCELLED,
            'Rejected': OrderStatus.REJECTED,
            'Deactivated': OrderStatus.EXPIRED,
            'Triggered': OrderStatus.OPEN,
            'Active': OrderStatus.OPEN
        }
        return status_map.get(status, OrderStatus.OPEN)
    
    def _convert_time_in_force(self, tif: str) -> str:
        """Convert time in force to Bybit format"""
        tif_map = {
            'GTC': 'GTC',
            'IOC': 'IOC',
            'FOK': 'FOK',
            'POST_ONLY': 'PostOnly'
        }
        return tif_map.get(tif.upper(), 'GTC')
    
    def _format_quantity(self, symbol: str, quantity: float) -> str:
        """Format quantity according to symbol rules"""
        if symbol in self.client.instruments:
            qty_step = self.client.instruments[symbol].get('qtyStep', 0.001)
            precision = len(str(qty_step).split('.')[-1]) if '.' in str(qty_step) else 0
            return f"{quantity:.{precision}f}"
        
        return str(quantity)
    
    def _format_price(self, symbol: str, price: float) -> str:
        """Format price according to symbol rules"""
        if symbol in self.client.instruments:
            tick_size = self.client.instruments[symbol].get('tickSize', 0.01)
            precision = len(str(tick_size).split('.')[-1]) if '.' in str(tick_size) else 0
            return f"{price:.{precision}f}"
        
        return str(price)