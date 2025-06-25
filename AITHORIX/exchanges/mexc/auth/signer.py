"""
AITHORIX MEXC Request Signer
Signs requests for MEXC API authentication
"""

import hmac
import hashlib
from typing import Union
import logging

logger = logging.getLogger(__name__)


class MEXCSigner:
    """
    MEXC request signer using HMAC-SHA256
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
    
    def verify_signature(self, message: Union[str, bytes], signature: str) -> bool:
        """
        Verify a signature
        
        Args:
            message: Original message
            signature: Signature to verify
            
        Returns:
            True if valid, False otherwise
        """
        try:
            expected_signature = self.sign(message)
            return hmac.compare_digest(expected_signature, signature)
        except Exception as e:
            logger.error(f"Signature verification failed: {str(e)}")
            return False