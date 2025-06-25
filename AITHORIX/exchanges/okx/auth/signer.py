"""
AITHORIX OKX Request Signer
Signs requests for OKX API authentication
"""

import hmac
import hashlib
import base64
from typing import Union
import logging

logger = logging.getLogger(__name__)


class OKXSigner:
    """
    OKX request signer using HMAC-SHA256 with Base64 encoding
    """
    
    def __init__(self, api_secret: str):
        self.api_secret = api_secret.encode('utf-8')
    
    def sign(self, payload: Union[str, bytes]) -> str:
        """
        Sign payload using HMAC-SHA256 and encode with Base64
        
        Args:
            payload: String or bytes to sign
            
        Returns:
            Base64-encoded signature
        """
        if isinstance(payload, str):
            payload = payload.encode('utf-8')
        
        # Create HMAC signature
        signature = hmac.new(
            self.api_secret,
            payload,
            hashlib.sha256
        ).digest()
        
        # Base64 encode
        return base64.b64encode(signature).decode('utf-8')
    
    def sign_websocket(self, timestamp: str, method: str, path: str) -> str:
        """
        Sign WebSocket authentication request
        
        Args:
            timestamp: Request timestamp
            method: HTTP method (GET for WebSocket)
            path: WebSocket path
            
        Returns:
            Base64-encoded signature
        """
        payload = timestamp + method + path
        return self.sign(payload)