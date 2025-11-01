import os
from typing import List

class AuthManager:
    """Manage YouTube API authentication and key rotation"""
    
    def __init__(self, config: Dict):
        self.config = config
        self.api_keys = self._load_api_keys()
        self.current_key_index = 0
    
    def _load_api_keys(self) -> List[str]:
        """Load API keys from environment variables or config"""
        # First try environment variable
        api_key = os.getenv('YOUTUBE_API_KEY')
        if api_key:
            return [api_key]
        
        # Try multiple keys from environment
        keys = []
        i = 1
        while True:
            key = os.getenv(f'YOUTUBE_API_KEY_{i}')
            if not key:
                break
            keys.append(key)
            i += 1
        
        if keys:
            return keys
        
        # Fallback to config file
        return self.config.get('api_keys', [])
    
    def get_api_key(self) -> str:
        """Get current API key with rotation"""
        if not self.api_keys:
            raise ValueError("No YouTube API keys configured")
        
        key = self.api_keys[self.current_key_index]
        self.current_key_index = (self.current_key_index + 1) % len(self.api_keys)
        return key
    
    def validate_keys(self) -> bool:
        """Validate all API keys"""
        # Implementation for key validation
        return len(self.api_keys) > 0