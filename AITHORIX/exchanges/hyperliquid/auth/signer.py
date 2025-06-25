"""
AITHORIX Hyperliquid Request Signer
Signs requests for Hyperliquid L2 authentication
"""

import json
import logging
from typing import Dict, Any, Union
from eth_account import Account
from eth_account.messages import encode_defunct
from eth_utils import keccak
from web3 import Web3

logger = logging.getLogger(__name__)


class HyperliquidSigner:
    """
    Hyperliquid request signer using Ethereum signatures
    """
    
    def __init__(self, account: Account):
        self.account = account
        self.address = account.address
        self.w3 = Web3()
    
    def sign_l1_action(self, action: Dict[str, Any]) -> str:
        """
        Sign L1 action for Hyperliquid
        
        This follows Hyperliquid's specific signing format
        """
        # Serialize action to bytes
        action_bytes = self._serialize_action(action)
        
        # Create hash
        message_hash = keccak(action_bytes)
        
        # Sign hash
        signature = self.account.signHash(message_hash)
        
        # Format signature
        r = signature.r
        s = signature.s
        v = signature.v
        
        # Hyperliquid expects signature as hex string
        sig_hex = f"0x{r:064x}{s:064x}{v:02x}"
        
        return sig_hex
    
    def sign_message(self, message: str) -> str:
        """
        Sign a simple message
        """
        # Create signable message
        signable_message = encode_defunct(text=message)
        
        # Sign message
        signed = self.account.sign_message(signable_message)
        
        # Return hex signature
        return signed.signature.hex()
    
    def _serialize_action(self, action: Dict[str, Any]) -> bytes:
        """
        Serialize action dictionary to bytes for signing
        
        Hyperliquid has specific serialization requirements
        """
        # Sort keys and create deterministic JSON
        sorted_json = json.dumps(action, sort_keys=True, separators=(',', ':'))
        
        # Convert to bytes
        return sorted_json.encode('utf-8')
    
    def verify_signature(self, message: Union[str, bytes], signature: str) -> bool:
        """
        Verify a signature matches this account
        """
        try:
            if isinstance(message, str):
                signable_message = encode_defunct(text=message)
            else:
                signable_message = encode_defunct(message)
            
            # Recover address from signature
            recovered = self.w3.eth.account.recover_message(
                signable_message,
                signature=signature
            )
            
            return recovered.lower() == self.address.lower()
            
        except Exception as e:
            logger.error(f"Signature verification failed: {str(e)}")
            return False