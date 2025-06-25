"""
AITHORIX Bybit Request Signer
Signs requests for Bybit API authentication
"""

import hmac
import hashlib
from typing import Union
import logging

logger = logging.getLogger(__name__)


class BybitSigner:
    """
    Bybit request signer using HMAC-SHA256
    """
    
    def __init__(self, api_secret: str):
        self.api_secret = api_secret.encode('utf-8')
    
    def sign(self, payload: Union[str, bytes]) -> str:
        """
        Sign payload using HMAC-SHA256
        
        Args:
            payload: String or bytes to sign
            
        Returns:
            Hex-encoded signature
        """
        if isinstance(payload, str):
            payload = payload.encode('utf-8')
        
        signature = hmac.new(
            self.api_secret,
            payload,
            hashlib.sha256
        ).hexdigest()
        
        return signature
    
    def sign_websocket(self, method: str, path: str, expires: int) -> str:
        """
        Sign WebSocket authentication request
        
        Args:
            method: HTTP method (GET for WebSocket)
            path: WebSocket path
            expires: Expiration timestamp
            
        Returns:
            Hex-encoded signature
        """
        payload = f"{method}{path}{expires}"
        return self.sign(payload)