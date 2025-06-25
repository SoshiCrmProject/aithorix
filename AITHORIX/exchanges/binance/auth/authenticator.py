"""
AITHORIX Binance Authenticator
Handles authentication for Binance API requests
"""

import time
import logging
from typing import Dict, Optional
from urllib.parse import urlencode

from .signer import BinanceSigner
from ...base_exchange import ExchangeConfig

logger = logging.getLogger(__name__)


class BinanceAuthenticator:
    """
    Binance authentication handler
    """
    
    def __init__(self, config: ExchangeConfig):
        self.config = config
        self.signer = BinanceSigner(config.api_secret)
        
    async def get_auth_headers(
        self,
        method: str,
        endpoint: str,
        params: Optional[Dict],
        data: Optional[Dict]
    ) -> Dict[str, str]:
        """
        Generate authentication headers for Binance
        
        Binance uses:
        - API key in header
        - Signature in query params
        """
        headers = {
            'X-MBX-APIKEY': self.config.api_key
        }
        
        # Add timestamp to params
        timestamp = int(time.time() * 1000)
        
        if params is None:
            params = {}
        
        params['timestamp'] = timestamp
        
        # Create query string
        if data:
            # For POST requests, data is included in signature
            query_string = urlencode(sorted(data.items()))
            if params:
                query_string = urlencode(sorted(params.items())) + '&' + query_string
        else:
            query_string = urlencode(sorted(params.items()))
        
        # Generate signature
        signature = self.signer.sign(query_string)
        params['signature'] = signature
        
        return headers
    
    def get_listen_key_headers(self) -> Dict[str, str]:
        """Get headers for listen key endpoints"""
        return {
            'X-MBX-APIKEY': self.config.api_key
        }