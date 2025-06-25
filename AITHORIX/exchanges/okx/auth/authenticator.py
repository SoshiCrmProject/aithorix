"""
AITHORIX OKX Authenticator
Handles authentication for OKX API requests
"""

import time
import logging
from typing import Dict, Optional, Any
import json
from datetime import datetime

from .signer import OKXSigner
from ...base_exchange import ExchangeConfig

logger = logging.getLogger(__name__)


class OKXAuthenticator:
    """
    OKX authentication handler
    """
    
    def __init__(self, config: ExchangeConfig):
        self.config = config
        self.signer = OKXSigner(config.api_secret)
        
        # OKX requires passphrase in addition to API key/secret
        self.passphrase = config.params.get('passphrase', '')
        
    async def get_auth_headers(
        self,
        method: str,
        endpoint: str,
        params: Optional[Dict] = None,
        data: Optional[Dict] = None
    ) -> Dict[str, str]:
        """
        Generate authentication headers for OKX API
        
        OKX uses:
        - OK-ACCESS-KEY: API key
        - OK-ACCESS-SIGN: Base64 encoded HMAC signature
        - OK-ACCESS-TIMESTAMP: Request timestamp
        - OK-ACCESS-PASSPHRASE: API key passphrase
        """
        timestamp = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
        
        # Create signature payload
        sign_payload = self._create_sign_payload(
            timestamp, method, endpoint, params, data
        )
        
        # Generate signature
        signature = self.signer.sign(sign_payload)
        
        # Create headers
        headers = {
            "OK-ACCESS-KEY": self.config.api_key,
            "OK-ACCESS-SIGN": signature,
            "OK-ACCESS-TIMESTAMP": timestamp,
            "OK-ACCESS-PASSPHRASE": self.passphrase,
            "Content-Type": "application/json"
        }
        
        return headers
    
    def _create_sign_payload(
        self,
        timestamp: str,
        method: str,
        endpoint: str,
        params: Optional[Dict],
        data: Optional[Dict]
    ) -> str:
        """
        Create payload for signature
        
        OKX signature format:
        timestamp + method + requestPath + body
        """
        # Build request path
        request_path = endpoint
        
        # Add query string for GET requests
        if method == "GET" and params:
            # Sort params and create query string
            sorted_params = sorted(params.items())
            query_string = "&".join([f"{k}={v}" for k, v in sorted_params])
            request_path = f"{endpoint}?{query_string}"
        
        # Build body string
        body = ""
        if method in ["POST", "PUT", "DELETE"] and data:
            body = json.dumps(data, separators=(',', ':'))
        
        # Combine all parts
        sign_payload = timestamp + method + request_path + body
        
        return sign_payload
    
    def get_websocket_auth_payload(self) -> Dict[str, Any]:
        """
        Create WebSocket authentication payload
        """
        timestamp = str(int(time.time()))
        
        # Create signature for WebSocket
        sign_payload = timestamp + "GET" + "/users/self/verify"
        signature = self.signer.sign(sign_payload)
        
        return {
            "op": "login",
            "args": [{
                "apiKey": self.config.api_key,
                "passphrase": self.passphrase,
                "timestamp": timestamp,
                "sign": signature
            }]
        }
    
    def verify_webhook(self, headers: Dict[str, str], body: str) -> bool:
        """
        Verify webhook signature from OKX
        """
        try:
            timestamp = headers.get('OK-ACCESS-TIMESTAMP')
            received_signature = headers.get('OK-ACCESS-SIGN')
            
            if not timestamp or not received_signature:
                return False
            
            # Create expected signature
            sign_payload = timestamp + body
            expected_signature = self.signer.sign(sign_payload)
            
            return expected_signature == received_signature
            
        except Exception as e:
            logger.error(f"Error verifying webhook: {str(e)}")
            return False