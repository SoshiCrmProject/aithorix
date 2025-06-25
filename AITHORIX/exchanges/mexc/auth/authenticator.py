"""
AITHORIX MEXC Authenticator
Handles authentication for MEXC API requests
"""

import time
import logging
from typing import Dict, Optional, Any
from urllib.parse import urlencode
import json

from .signer import MEXCSigner
from ...base_exchange import ExchangeConfig

logger = logging.getLogger(__name__)


class MEXCAuthenticator:
    """
    MEXC authentication handler
    """
    
    def __init__(self, config: ExchangeConfig):
        self.config = config
        self.signer = MEXCSigner(config.api_secret)
        
    async def get_auth_headers(
        self,
        method: str,
        endpoint: str,
        params: Optional[Dict],
        data: Optional[Dict]
    ) -> Dict[str, str]:
        """
        Generate authentication headers for MEXC
        
        MEXC uses:
        - API key in header
        - Signature in header
        - Timestamp in header
        """
        headers = {
            'X-MEXC-APIKEY': self.config.api_key,
            'Content-Type': 'application/json'
        }
        
        # Add timestamp
        timestamp = str(int(time.time() * 1000))
        headers['Request-Time'] = timestamp
        
        # Create signature payload
        signature_payload = self._create_signature_payload(
            method, endpoint, params, data, timestamp
        )
        
        # Generate signature
        signature = self.signer.sign(signature_payload)
        headers['Signature'] = signature
        
        return headers
    
    def _create_signature_payload(
        self,
        method: str,
        endpoint: str,
        params: Optional[Dict],
        data: Optional[Dict],
        timestamp: str
    ) -> str:
        """
        Create payload for signature
        
        MEXC signature format:
        timestamp + api_key + recv_window + query_string + body
        """
        recv_window = "5000"  # 5 second window
        
        # Build components
        components = [
            timestamp,
            self.config.api_key,
            recv_window
        ]
        
        # Add query string for GET requests
        if method == "GET" and params:
            # Sort params by key
            sorted_params = sorted(params.items())
            query_string = urlencode(sorted_params)
            components.append(query_string)
        elif method in ["POST", "PUT", "DELETE"]:
            # For other methods, add empty string if no params
            if params:
                sorted_params = sorted(params.items())
                query_string = urlencode(sorted_params)
                components.append(query_string)
            else:
                components.append("")
            
            # Add body for POST/PUT/DELETE
            if data:
                body_string = json.dumps(data, separators=(',', ':'), sort_keys=True)
                components.append(body_string)
            else:
                components.append("")
        
        # Join all components
        return "".join(components)
    
    def get_websocket_auth_payload(self) -> Dict[str, Any]:
        """
        Create WebSocket authentication payload
        """
        timestamp = int(time.time() * 1000)
        
        # Create auth params
        auth_params = {
            "apiKey": self.config.api_key,
            "reqTime": timestamp,
            "op": "sub.personal"
        }
        
        # Create signature
        signature_string = f"{auth_params['apiKey']}{auth_params['reqTime']}"
        signature = self.signer.sign(signature_string)
        
        auth_params["signature"] = signature
        
        return auth_params