import time
import threading
from typing import Optional

class RateLimiter:
    """Manage API rate limiting and quota management"""
    
    def __init__(self, config: Dict):
        self.config = config
        self.requests_per_minute = config.get('extraction', {}).get('requests_per_minute', 60)
        self.last_request_time = 0
        self.lock = threading.Lock()
    
    def wait_if_needed(self):
        """Wait if needed to respect rate limits"""
        with self.lock:
            current_time = time.time()
            time_since_last_request = current_time - self.last_request_time
            
            min_interval = 60.0 / self.requests_per_minute
            if time_since_last_request < min_interval:
                sleep_time = min_interval - time_since_last_request
                time.sleep(sleep_time)
            
            self.last_request_time = time.time()
    
    def get_quota_usage(self) -> Dict:
        """Get current quota usage information"""
        # Implementation for quota tracking
        return {
            'requests_made': 0,  # Placeholder
            'quota_limit': 10000,  # YouTube default
            'percentage_used': 0.0
        }