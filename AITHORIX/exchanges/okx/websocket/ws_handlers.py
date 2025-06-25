"""
AITHORIX OKX WebSocket Handlers
Process WebSocket messages from OKX
"""

import logging
from datetime import datetime, timezone
from typing import Dict, Any, List

from ...base_exchange import (
    Ticker, OrderBook, Trade, Order, Position, Balance,
    OrderSide, OrderStatus, OrderType, PositionSide
)

logger = logging.getLogger(__name__)


class OKXWebSocketHandlers:
    """
    OKX WebSocket message handlers
    """
    
    def __init__(self, client):
        self.client = client
    
    async def handle_ticker(self, data: Dict[str, Any]):
        """Handle ticker updates"""
        try:
            ticker_list = data.get('data', [])
            
            for ticker_data in ticker_list:
                symbol = ticker_data['instId']
                
                # Create ticker object
                ticker = Ticker(
                    symbol=symbol,
                    bid=float(ticker_data['bidPx']) if ticker_data.get('bidPx') else 0,
                    ask=float(ticker_data['askPx']) if ticker_data.get('askPx') else 0,
                    bid_size=float(ticker_data['bidSz']) if ticker_data.get('bidSz') else 0,
                    ask_size=float(ticker_data['askSz']) if ticker_data.get('askSz') else 0,
                    last=float(ticker_data['last']) if ticker_data.get('last') else 0,
                    volume_24h=float(ticker_data['vol24h']) if ticker_data.get('vol24h') else 0,
                    quote_volume_24h=float(ticker_data['volCcy24h']) if ticker_data.get('volCcy24h') else 0,
                    open_24h=float(ticker_data['open24h']) if ticker_data.get('open24h') else 0,
                    high_24h=float(ticker_data['high24h']) if ticker_data.get('high24h') else 0,
                    low_24h=float(ticker_data['low24h']) if ticker_data.get('low24h') else 0,
                    change_24h=float(ticker_data['last']) - float(ticker_data['open24h']) if ticker_data.get('open24h') else 0,
                    change_percent_24h=(float(ticker_data['last']) / float(ticker_data['open24h']) - 1) * 100 if ticker_data.get('open24h') and float(ticker_data['open24h']) > 0 else 0,
                    timestamp=datetime.fromtimestamp(int(ticker_data['ts']) / 1000, tz=timezone.utc)
                )
                
                # Update client ticker cache
                if hasattr(self.client, '_ticker_cache'):
                    self.client._ticker_cache[symbol] = ticker
                
                # Call user callback
                if hasattr(self.client, 'ws_on_ticker'):
                    await self.client.ws_on_ticker(ticker)
            
        except Exception as e:
            logger.error(f"Error handling ticker: {str(e)}")
    
    async def handle_orderbook(self, data: Dict[str, Any]):
        """Handle order book updates"""
        try:
            book_list = data.get('data', [])
            action = data.get('action', 'snapshot')  # snapshot or update
            
            for book_data in book_list:
                symbol = data['arg']['instId']
                
                if action == 'snapshot':
                    # Full order book snapshot
                    orderbook = OrderBook(
                        symbol=symbol,
                        bids=[[float(p), float(s), int(c), int(o)] for p, s, c, o in book_data.get('bids', [])],
                        asks=[[float(p), float(s), int(c), int(o)] for p, s, c, o in book_data.get('asks', [])],
                        timestamp=datetime.fromtimestamp(int(book_data['ts']) / 1000, tz=timezone.utc)
                    )
                    
                    # Update client order book cache
                    if hasattr(self.client, '_orderbook_cache'):
                        self.client._orderbook_cache[symbol] = orderbook
                else:
                    # Update existing order book
                    if hasattr(self.client, '_orderbook_cache') and symbol in self.client._orderbook_cache:
                        orderbook = self.client._orderbook_cache[symbol]
                        
                        # Apply bid updates
                        self._update_orderbook_side(orderbook.bids, book_data.get('bids', []), True)
                        
                        # Apply ask updates
                        self._update_orderbook_side(orderbook.asks, book_data.get('asks', []), False)
                        
                        # Update timestamp
                        orderbook.timestamp = datetime.fromtimestamp(int(book_data['ts']) / 1000, tz=timezone.utc)
                    else:
                        continue
                
                # Call user callback
                if hasattr(self.client, 'ws_on_orderbook'):
                    await self.client.ws_on_orderbook(orderbook)
            
        except Exception as e:
            logger.error(f"Error handling order book: {str(e)}")
    
    def _update_orderbook_side(self, levels: List[List[float]], updates: List[List], is_bid: bool):
        """Update order book side with new levels"""
        for update in updates:
            price = float(update[0])
            size = float(update[1])
            
            if size == 0:
                # Remove level
                levels[:] = [level for level in levels if level[0] != price]
            else:
                # Update or add level
                found = False
                for i, level in enumerate(levels):
                    if level[0] == price:
                        levels[i] = [price, size, int(update[2]), int(update[3])]
                        found = True
                        break
                
                if not found:
                    levels.append([price, size, int(update[2]), int(update[3])])
        
        # Sort levels
        levels.sort(key=lambda x: x[0], reverse=is_bid)
    
    async def handle_trade(self, data: Dict[str, Any]):
        """Handle trade updates"""
        try:
            trades_list = data.get('data', [])
            
            for trade_data in trades_list:
                symbol = trade_data['instId']
                
                # Create trade object
                trade = Trade(
                    id=trade_data['tradeId'],
                    order_id=None,
                    symbol=symbol,
                    side=OrderSide.BUY if trade_data['side'] == 'buy' else OrderSide.SELL,
                    price=float(trade_data['px']),
                    size=float(trade_data['sz']),
                    fee=0,
                    fee_currency=None,
                    timestamp=datetime.fromtimestamp(int(trade_data['ts']) / 1000, tz=timezone.utc),
                    is_maker=False
                )
                
                # Call user callback
                if hasattr(self.client, 'ws_on_trade'):
                    await self.client.ws_on_trade(trade)
            
        except Exception as e:
            logger.error(f"Error handling trade: {str(e)}")
    
    async def handle_kline(self, data: Dict[str, Any]):
        """Handle kline updates"""
        try:
            kline_list = data.get('data', [])
            arg = data.get('arg', {})
            
            # Extract interval from channel
            channel = arg.get('channel', '')
            interval = channel.replace('candle', '')
            symbol = arg.get('instId')
            
            for kline_data in kline_list:
                # Call user callback with raw kline data
                if hasattr(self.client, 'ws_on_kline'):
                    await self.client.ws_on_kline({
                        'symbol': symbol,
                        'interval': interval,
                        'timestamp': datetime.fromtimestamp(int(kline_data[0]) / 1000, tz=timezone.utc),
                        'open': float(kline_data[1]),
                        'high': float(kline_data[2]),
                        'low': float(kline_data[3]),
                        'close': float(kline_data[4]),
                        'volume': float(kline_data[5]),
                        'volume_ccy': float(kline_data[6]) if len(kline_data) > 6 else 0,
                        'volume_ccy_quote': float(kline_data[7]) if len(kline_data) > 7 else 0,
                        'confirm': kline_data[8] if len(kline_data) > 8 else True
                    })
            
        except Exception as e:
            logger.error(f"Error handling kline: {str(e)}")
    
    async def handle_mark_price(self, data: Dict[str, Any]):
        """Handle mark price updates"""
        try:
            mark_price_list = data.get('data', [])
            
            for price_data in mark_price_list:
                # Call user callback
                if hasattr(self.client, 'ws_on_mark_price'):
                    await self.client.ws_on_mark_price({
                        'symbol': price_data['instId'],
                        'markPx': float(price_data['markPx']),
                        'timestamp': datetime.fromtimestamp(int(price_data['ts']) / 1000, tz=timezone.utc)
                    })
            
        except Exception as e:
            logger.error(f"Error handling mark price: {str(e)}")
    
    async def handle_funding_rate(self, data: Dict[str, Any]):
        """Handle funding rate updates"""
        try:
            funding_list = data.get('data', [])
            
            for funding_data in funding_list:
                # Call user callback
                if hasattr(self.client, 'ws_on_funding_rate'):
                    await self.client.ws_on_funding_rate({
                        'symbol': funding_data['instId'],
                        'fundingRate': float(funding_data['fundingRate']),
                        'fundingTime': datetime.fromtimestamp(int(funding_data['fundingTime']) / 1000, tz=timezone.utc),
                        'nextFundingRate': float(funding_data['nextFundingRate']) if funding_data.get('nextFundingRate') else None,
                        'nextFundingTime': datetime.fromtimestamp(int(funding_data['nextFundingTime']) / 1000, tz=timezone.utc) if funding_data.get('nextFundingTime') else None
                    })
            
        except Exception as e:
            logger.error(f"Error handling funding rate: {str(e)}")
    
    async def handle_instruments(self, data: Dict[str, Any]):
        """Handle instruments updates"""
        try:
            inst_list = data.get('data', [])
            
            for inst_data in inst_list:
                # Update client instruments cache
                symbol = inst_data['instId']
                if hasattr(self.client, 'instruments'):
                    self.client.instruments[symbol] = inst_data
                
                # Call user callback
                if hasattr(self.client, 'ws_on_instrument_update'):
                    await self.client.ws_on_instrument_update(inst_data)
            
        except Exception as e:
            logger.error(f"Error handling instruments: {str(e)}")
    
    async def handle_open_interest(self, data: Dict[str, Any]):
        """Handle open interest updates"""
        try:
            oi_list = data.get('data', [])
            
            for oi_data in oi_list:
                # Call user callback
                if hasattr(self.client, 'ws_on_open_interest'):
                    await self.client.ws_on_open_interest({
                        'symbol': oi_data['instId'],
                        'oi': float(oi_data['oi']),
                        'oiCcy': float(oi_data['oiCcy']),
                        'timestamp': datetime.fromtimestamp(int(oi_data['ts']) / 1000, tz=timezone.utc)
                    })
            
        except Exception as e:
            logger.error(f"Error handling open interest: {str(e)}")
    
    async def handle_liquidation(self, data: Dict[str, Any]):
        """Handle liquidation updates"""
        try:
            liq_list = data.get('data', [])
            
            for liq_data in liq_list:
                # Call user callback
                if hasattr(self.client, 'ws_on_liquidation'):
                    await self.client.ws_on_liquidation({
                        'symbol': liq_data['instId'],
                        'side': liq_data.get('side'),
                        'posSide': liq_data.get('posSide'),
                        'price': float(liq_data['bkPx']),
                        'size': float(liq_data['sz']),
                        'markPrice': float(liq_data['markPx']),
                        'timestamp': datetime.fromtimestamp(int(liq_data['ts']) / 1000, tz=timezone.utc)
                    })
            
        except Exception as e:
            logger.error(f"Error handling liquidation: {str(e)}")
    
    # Private data handlers
    async def handle_account(self, data: Dict[str, Any]):
        """Handle account updates"""
        try:
            account_list = data.get('data', [])
            
            for account_data in account_list:
                # Update balance cache
                if hasattr(self.client, '_balance_cache'):
                    for detail in account_data.get('details', []):
                        currency = detail['ccy']
                        
                        available = float(detail.get('availBal', 0))
                        equity = float(detail.get('eq', 0))
                        used = equity - available
                        
                        balance = Balance(
                            currency=currency,
                            free=available,
                            used=used,
                            total=equity,
                            exchange='OKX'
                        )
                        
                        self.client._balance_cache[currency] = balance
                
                # Call user callback
                if hasattr(self.client, 'ws_on_balance_update'):
                    await self.client.ws_on_balance_update(account_data)
            
        except Exception as e:
            logger.error(f"Error handling account update: {str(e)}")
    
    async def handle_positions(self, data: Dict[str, Any]):
        """Handle position updates"""
        try:
            position_list = data.get('data', [])
            
            for pos_data in position_list:
                if float(pos_data.get('pos', 0)) != 0:
                    # Parse position
                    position = self.client.unified_trading._parse_position(pos_data)
                    
                    if position:
                        # Update position cache
                        if hasattr(self.client, '_position_cache'):
                            self.client._position_cache[position.symbol] = position
                        
                        # Call user callback
                        if hasattr(self.client, 'ws_on_position_update'):
                            await self.client.ws_on_position_update(position)
                else:
                    # Position closed
                    symbol = pos_data['instId']
                    if hasattr(self.client, '_position_cache') and symbol in self.client._position_cache:
                        del self.client._position_cache[symbol]
                    
                    # Call user callback for closed position
                    if hasattr(self.client, 'ws_on_position_closed'):
                        await self.client.ws_on_position_closed(symbol)
            
        except Exception as e:
            logger.error(f"Error handling position update: {str(e)}")
    
    async def handle_balance_and_position(self, data: Dict[str, Any]):
        """Handle combined balance and position updates"""
        try:
            update_list = data.get('data', [])
            
            for update_data in update_list:
                # Handle balance updates
                if 'balData' in update_data:
                    for bal_data in update_data['balData']:
                        currency = bal_data['ccy']
                        
                        balance = Balance(
                            currency=currency,
                            free=float(bal_data.get('cashBal', 0)),
                            used=0,  # Calculate from positions
                            total=float(bal_data.get('cashBal', 0)),
                            exchange='OKX'
                        )
                        
                        if hasattr(self.client, '_balance_cache'):
                            self.client._balance_cache[currency] = balance
                
                # Handle position updates
                if 'posData' in update_data:
                    for pos_data in update_data['posData']:
                        if float(pos_data.get('pos', 0)) != 0:
                            position = self.client.unified_trading._parse_position(pos_data)
                            
                            if position and hasattr(self.client, '_position_cache'):
                                self.client._position_cache[position.symbol] = position
                
                # Call user callback
                if hasattr(self.client, 'ws_on_balance_position_update'):
                    await self.client.ws_on_balance_position_update(update_data)
            
        except Exception as e:
            logger.error(f"Error handling balance and position update: {str(e)}")
    
    async def handle_orders(self, data: Dict[str, Any]):
        """Handle order updates"""
        try:
            order_list = data.get('data', [])
            
            for order_data in order_list:
                # Parse order
                order = self.client.unified_trading._parse_order(order_data)
                
                if order:
                    # Call user callback
                    if hasattr(self.client, 'ws_on_order_update'):
                        await self.client.ws_on_order_update(order)
            
        except Exception as e:
            logger.error(f"Error handling order update: {str(e)}")
    
    async def handle_algo_orders(self, data: Dict[str, Any]):
        """Handle algo order updates"""
        try:
            order_list = data.get('data', [])
            
            for order_data in order_list:
                # Call user callback with raw data
                if hasattr(self.client, 'ws_on_algo_order_update'):
                    await self.client.ws_on_algo_order_update(order_data)
            
        except Exception as e:
            logger.error(f"Error handling algo order update: {str(e)}")