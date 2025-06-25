"""
AITHORIX Hyperliquid Authenticator
Handles authentication for Hyperliquid L2 DeFi exchange
"""

import time
import logging
from typing import Dict, Optional, Any
from eth_account import Account
from eth_account.messages import encode_defunct

from .signer import HyperliquidSigner
from ...base_exchange import ExchangeConfig

logger = logging.getLogger(__name__)


class HyperliquidAuthenticator:
    """
    Hyperliquid authentication handler
    Uses Ethereum-style signatures for authentication
    """
    
    def __init__(self, config: ExchangeConfig):
        self.config = config
        
        # In Hyperliquid, API key is the private key
        if config.api_key:
            self.account = Account.from_key(config.api_key)
            self.address = self.account.address
            self.signer = HyperliquidSigner(self.account)
        else:
            self.account = None
            self.address = None
            self.signer = None
            
        logger.info(f"Initialized Hyperliquid authenticator for address: {self.address}")
    
    async def sign_request(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Sign request data for Hyperliquid
        
        Returns additional fields to be added to request body
        """
        if not self.signer:
            return {}
        
        # Create signature payload
        timestamp = int(time.time() * 1000)
        
        # Hyperliquid uses specific signature format
        action = data.get('action', {})
        nonce = timestamp  # Use timestamp as nonce
        
        # Create signature data
        signature_data = {
            'action': action,
            'nonce': nonce,
            'vaultAddress': None  # Only for vault trading
        }
        
        # Sign the payload
        signature = self.signer.sign_l1_action(signature_data)
        
        # Return fields to add to request
        return {
            'signature': signature,
            'nonce': nonce,
            'vaultAddress': None
        }
    
    def sign_websocket_auth(self) -> Dict[str, Any]:
        """
        Create WebSocket authentication message
        """
        if not self.signer:
            return {}
        
        timestamp = int(time.time() * 1000)
        
        # Create auth message
        auth_data = {
            'method': 'auth',
            'user': self.address,
            'timestamp': timestamp
        }
        
        # Sign the auth message
        message = f"{auth_data['method']}:{auth_data['user']}:{auth_data['timestamp']}"
        signature = self.signer.sign_message(message)
        
        auth_data['signature'] = signature
        
        return auth_data