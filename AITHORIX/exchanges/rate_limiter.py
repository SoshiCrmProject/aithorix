"""
AITHORIX Rate Limiter
Manages API rate limits across all exchanges
"""

import asyncio
import time
import logging
from typing import Dict, Optional, Any, Callable
from collections import deque, defaultdict
from datetime import datetime, timedelta
import aiohttp

logger = logging.getLogger(__name__)


class RateLimiter:
    """
    Universal rate limiter for exchange API calls
    """
    
    def __init__(self):
        # Exchange-specific rate limits
        self.rate_limits = {
            'binance': {
                'spot': {'requests_per_minute': 1200, 'weight_per_minute': 6000},
                'futures': {'requests_per_minute': 2400, 'weight_per_minute': 12000},
                'orders_per_second': 10,
                'orders_per_day': 200000
            },
            'hyperliquid': {
                'requests_per_second': 100,
                'burst_limit': 200,
                'websocket_messages_per_second': 50
            },
            'mexc': {
                'spot': {'public_per_second': 20, 'private_per_second': 10},
                'futures': {'public_per_second': 100, 'private_per_second': 50},
                'orders_per_second': 10
            },
            'bybit': {
                'requests_per_minute': 600,
                'requests_per_second': 10,
                'orders_per_minute': 100
            },
            'okx': {
                'requests_per_second': 20,
                'orders_per_second': 60,
                'websocket_connections': 20,
                'websocket_subscriptions_per_connection': 240
            }
        }
        
        # Request tracking
        self.request_history: Dict[str, deque] = defaultdict(lambda: deque())
        self.weight_history: Dict[str, deque] = defaultdict(lambda: deque())
        self.order_history: Dict[str, deque] = defaultdict(lambda: deque())
        
        # Locks for thread safety
        self.locks: Dict[str, asyncio.Lock] = defaultdict(asyncio.Lock)
        
        # Rate limit status
        self.rate_limit_status: Dict[str, Dict[str, Any]] = defaultdict(dict)
        
    async def check_rate_limit(
        self,
        exchange: str,
        endpoint: str,
        request_weight: int = 1,
        is_order: bool = False
    ) -> bool:
        """
        Check if request can be made within rate limits
        
        Returns:
            bool: True if request can proceed, False if rate limited
        """
        async with self.locks[exchange]:
            current_time = time.time()
            
            # Clean old entries
            self._clean_old_entries(exchange, current_time)
            
            # Check exchange-specific limits
            if exchange == 'binance':
                return await self._check_binance_limits(endpoint, request_weight, is_order, current_time)
            elif exchange == 'hyperliquid':
                return await self._check_hyperliquid_limits(current_time)
            elif exchange == 'mexc':
                return await self._check_mexc_limits(endpoint, is_order, current_time)
            elif exchange == 'bybit':
                return await self._check_bybit_limits(is_order, current_time)
            elif exchange == 'okx':
                return await self._check_okx_limits(is_order, current_time)
            else:
                # Default rate limiting
                return await self._check_default_limits(exchange, current_time)
    
    async def wait_if_needed(
        self,
        exchange: str,
        endpoint: str,
        request_weight: int = 1,
        is_order: bool = False
    ):
        """
        Wait if rate limited before making request
        """
        while not await self.check_rate_limit(exchange, endpoint, request_weight, is_order):
            wait_time = self._get_wait_time(exchange)
            logger.warning(f"Rate limited on {exchange}, waiting {wait_time:.2f}s")
            await asyncio.sleep(wait_time)
    
    def record_request(
        self,
        exchange: str,
        endpoint: str,
        request_weight: int = 1,
        is_order: bool = False
    ):
        """
        Record a completed request for rate limiting
        """
        current_time = time.time()
        
        # Record request
        self.request_history[exchange].append(current_time)
        
        # Record weight
        if request_weight > 1:
            self.weight_history[exchange].append((current_time, request_weight))
        
        # Record order
        if is_order:
            self.order_history[exchange].append(current_time)
        
        # Update status
        self._update_rate_limit_status(exchange)
    
    def update_from_headers(self, exchange: str, headers: Dict[str, str]):
        """
        Update rate limit status from response headers
        """
        if exchange == 'binance':
            self._update_binance_headers(headers)
        elif exchange == 'bybit':
            self._update_bybit_headers(headers)
        elif exchange == 'okx':
            self._update_okx_headers(headers)
    
    async def _check_binance_limits(
        self,
        endpoint: str,
        request_weight: int,
        is_order: bool,
        current_time: float
    ) -> bool:
        """Check Binance-specific rate limits"""
        # Determine if spot or futures
        is_futures = '/fapi/' in endpoint or '/dapi/' in endpoint
        limits = self.rate_limits['binance']['futures' if is_futures else 'spot']
        
        # Check requests per minute
        minute_ago = current_time - 60
        recent_requests = sum(1 for t in self.request_history['binance'] if t > minute_ago)
        if recent_requests >= limits['requests_per_minute']:
            return False
        
        # Check weight per minute
        recent_weight = sum(
            w for t, w in self.weight_history['binance']
            if t > minute_ago
        )
        if recent_weight + request_weight > limits['weight_per_minute']:
            return False
        
        # Check orders per second
        if is_order:
            second_ago = current_time - 1
            recent_orders = sum(1 for t in self.order_history['binance'] if t > second_ago)
            if recent_orders >= self.rate_limits['binance']['orders_per_second']:
                return False
        
        return True
    
    async def _check_hyperliquid_limits(self, current_time: float) -> bool:
        """Check Hyperliquid-specific rate limits"""
        limits = self.rate_limits['hyperliquid']
        
        # Check requests per second
        second_ago = current_time - 1
        recent_requests = sum(1 for t in self.request_history['hyperliquid'] if t > second_ago)
        
        # Allow burst up to burst_limit
        if recent_requests >= limits['burst_limit']:
            return False
        
        # Check sustained rate
        ten_seconds_ago = current_time - 10
        recent_sustained = sum(1 for t in self.request_history['hyperliquid'] if t > ten_seconds_ago)
        if recent_sustained >= limits['requests_per_second'] * 10:
            return False
        
        return True
    
    async def _check_mexc_limits(
        self,
        endpoint: str,
        is_order: bool,
        current_time: float
    ) -> bool:
        """Check MEXC-specific rate limits"""
        # Determine endpoint type
        is_public = '/api/v3/ticker' in endpoint or '/api/v3/depth' in endpoint
        is_futures = '/contract/' in endpoint
        
        limits = self.rate_limits['mexc']['futures' if is_futures else 'spot']
        limit_key = 'public_per_second' if is_public else 'private_per_second'
        
        # Check requests per second
        second_ago = current_time - 1
        recent_requests = sum(1 for t in self.request_history['mexc'] if t > second_ago)
        if recent_requests >= limits[limit_key]:
            return False
        
        # Check orders per second
        if is_order:
            recent_orders = sum(1 for t in self.order_history['mexc'] if t > second_ago)
            if recent_orders >= self.rate_limits['mexc']['orders_per_second']:
                return False
        
        return True
    
    async def _check_bybit_limits(self, is_order: bool, current_time: float) -> bool:
        """Check Bybit-specific rate limits"""
        limits = self.rate_limits['bybit']
        
        # Check requests per minute
        minute_ago = current_time - 60
        recent_requests = sum(1 for t in self.request_history['bybit'] if t > minute_ago)
        if recent_requests >= limits['requests_per_minute']:
            return False
        
        # Check requests per second
        second_ago = current_time - 1
        recent_requests_second = sum(1 for t in self.request_history['bybit'] if t > second_ago)
        if recent_requests_second >= limits['requests_per_second']:
            return False
        
        # Check orders per minute
        if is_order:
            recent_orders = sum(1 for t in self.order_history['bybit'] if t > minute_ago)
            if recent_orders >= limits['orders_per_minute']:
                return False
        
        return True
    
    async def _check_okx_limits(self, is_order: bool, current_time: float) -> bool:
        """Check OKX-specific rate limits"""
        limits = self.rate_limits['okx']
        
        # Check requests per second
        second_ago = current_time - 1
        recent_requests = sum(1 for t in self.request_history['okx'] if t > second_ago)
        if recent_requests >= limits['requests_per_second']:
            return False
        
        # Check orders per second
        if is_order:
            recent_orders = sum(1 for t in self.order_history['okx'] if t > second_ago)
            if recent_orders >= limits['orders_per_second']:
                return False
        
        return True
    
    async def _check_default_limits(self, exchange: str, current_time: float) -> bool:
        """Default rate limiting (10 requests per second)"""
        second_ago = current_time - 1
        recent_requests = sum(1 for t in self.request_history[exchange] if t > second_ago)
        return recent_requests < 10
    
    def _clean_old_entries(self, exchange: str, current_time: float):
        """Remove old entries from tracking"""
        # Keep last 5 minutes of data
        cutoff_time = current_time - 300
        
        # Clean request history
        while self.request_history[exchange] and self.request_history[exchange][0] < cutoff_time:
            self.request_history[exchange].popleft()
        
        # Clean weight history
        while self.weight_history[exchange] and self.weight_history[exchange][0][0] < cutoff_time:
            self.weight_history[exchange].popleft()
        
        # Clean order history
        while self.order_history[exchange] and self.order_history[exchange][0] < cutoff_time:
            self.order_history[exchange].popleft()
    
    def _get_wait_time(self, exchange: str) -> float:
        """Calculate how long to wait before next request"""
        current_time = time.time()
        
        if exchange == 'binance':
            # Wait until oldest request expires from 1-minute window
            if self.request_history[exchange]:
                oldest = self.request_history[exchange][0]
                return max(0.1, 60 - (current_time - oldest) + 0.1)
        elif exchange in ['hyperliquid', 'mexc', 'okx']:
            # Wait until oldest request expires from 1-second window
            if self.request_history[exchange]:
                oldest = self.request_history[exchange][0]
                return max(0.1, 1 - (current_time - oldest) + 0.1)
        elif exchange == 'bybit':
            # Check both per-second and per-minute limits
            if self.request_history[exchange]:
                # Check per-second
                second_ago = current_time - 1
                recent = [t for t in self.request_history[exchange] if t > second_ago]
                if len(recent) >= self.rate_limits['bybit']['requests_per_second']:
                    return 1.1
                
                # Check per-minute
                minute_ago = current_time - 60
                recent = [t for t in self.request_history[exchange] if t > minute_ago]
                if len(recent) >= self.rate_limits['bybit']['requests_per_minute']:
                    oldest = min(t for t in self.request_history[exchange] if t > minute_ago)
                    return 60 - (current_time - oldest) + 0.1
        
        return 0.5  # Default wait time
    
    def _update_rate_limit_status(self, exchange: str):
        """Update rate limit status for monitoring"""
        current_time = time.time()
        
        # Calculate current usage
        if exchange == 'binance':
            minute_ago = current_time - 60
            requests = sum(1 for t in self.request_history[exchange] if t > minute_ago)
            weight = sum(w for t, w in self.weight_history[exchange] if t > minute_ago)
            
            self.rate_limit_status[exchange] = {
                'requests_per_minute': requests,
                'weight_per_minute': weight,
                'requests_limit': self.rate_limits[exchange]['spot']['requests_per_minute'],
                'weight_limit': self.rate_limits[exchange]['spot']['weight_per_minute'],
                'updated_at': current_time
            }
        else:
            second_ago = current_time - 1
            requests = sum(1 for t in self.request_history[exchange] if t > second_ago)
            
            self.rate_limit_status[exchange] = {
                'requests_per_second': requests,
                'limit': self.rate_limits.get(exchange, {}).get('requests_per_second', 10),
                'updated_at': current_time
            }
    
    def _update_binance_headers(self, headers: Dict[str, str]):
        """Update Binance rate limit info from headers"""
        if 'X-MBX-USED-WEIGHT-1M' in headers:
            used_weight = int(headers['X-MBX-USED-WEIGHT-1M'])
            self.rate_limit_status['binance']['used_weight'] = used_weight
        
        if 'X-MBX-ORDER-COUNT-1S' in headers:
            order_count = int(headers['X-MBX-ORDER-COUNT-1S'])
            self.rate_limit_status['binance']['order_count_1s'] = order_count
    
    def _update_bybit_headers(self, headers: Dict[str, str]):
        """Update Bybit rate limit info from headers"""
        if 'X-Bapi-Limit-Status' in headers:
            limit_status = headers['X-Bapi-Limit-Status']
            self.rate_limit_status['bybit']['limit_status'] = limit_status
        
        if 'X-Bapi-Limit' in headers:
            limit = headers['X-Bapi-Limit']
            self.rate_limit_status['bybit']['limit'] = limit
    
    def _update_okx_headers(self, headers: Dict[str, str]):
        """Update OKX rate limit info from headers"""
        if 'x-ratelimit-limit' in headers:
            limit = headers['x-ratelimit-limit']
            self.rate_limit_status['okx']['limit'] = limit
        
        if 'x-ratelimit-remaining' in headers:
            remaining = headers['x-ratelimit-remaining']
            self.rate_limit_status['okx']['remaining'] = remaining
    
    def get_status(self, exchange: Optional[str] = None) -> Dict[str, Any]:
        """Get current rate limit status"""
        if exchange:
            return self.rate_limit_status.get(exchange, {})
        return dict(self.rate_limit_status)
    
    def reset(self, exchange: Optional[str] = None):
        """Reset rate limit tracking"""
        if exchange:
            self.request_history[exchange].clear()
            self.weight_history[exchange].clear()
            self.order_history[exchange].clear()
            self.rate_limit_status[exchange].clear()
        else:
            self.request_history.clear()
            self.weight_history.clear()
            self.order_history.clear()
            self.rate_limit_status.clear()


# Global rate limiter instance
rate_limiter = RateLimiter()