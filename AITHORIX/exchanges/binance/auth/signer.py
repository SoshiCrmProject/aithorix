"""
AITHORIX Binance Request Signer
Signs requests for Binance API authentication
"""

import hmac
import hashlib
from typing import Union


class BinanceSigner:
    """
    Binance request signer using HMAC-SHA256
    """
    
    def __init__(self, api_secret: str):
        self.api_secret = api_secret.encode('utf-8')
    
    def sign(self, message: Union[str, bytes]) -> str:
        """
        Sign a message using HMAC-SHA256
        
        Args:
            message: Message to sign
            
        Returns:
            Hex signature
        """
        if isinstance(message, str):
            message = message.encode('utf-8')
        
        signature = hmac.new(
            self.api_secret,
            message,
            hashlib.sha256
        ).hexdigest()
        
        return signature