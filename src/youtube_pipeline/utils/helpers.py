import hashlib
import time
from datetime import datetime, timedelta
from typing import Any, Dict, List
from urllib.parse import urlencode

def generate_id(*args) -> str:
    """Generate a unique ID from given arguments"""
    input_string = ''.join(str(arg) for arg in args)
    return hashlib.md5(input_string.encode()).hexdigest()[:16]

def format_duration(seconds: int) -> str:
    """Format duration in seconds to human-readable string"""
    if seconds < 60:
        return f"{seconds}s"
    elif seconds < 3600:
        minutes = seconds // 60
        return f"{minutes}m {seconds % 60}s"
    else:
        hours = seconds // 3600
        minutes = (seconds % 3600) // 60
        return f"{hours}h {minutes}m"

def safe_get(data: Dict, keys: List[str], default: Any = None) -> Any:
    """Safely get nested dictionary values"""
    current = data
    for key in keys:
        if isinstance(current, dict) and key in current:
            current = current[key]
        else:
            return default
    return current

def build_url(base_url: str, params: Dict) -> str:
    """Build URL with query parameters"""
    if not params:
        return base_url
    
    query_string = urlencode(params)
    return f"{base_url}?{query_string}"

def retry_on_exception(max_retries: int = 3, delay: int = 1, 
                      exceptions: tuple = (Exception,)):
    """Decorator for retrying function on exception"""
    def decorator(func):
        def wrapper(*args, **kwargs):
            for attempt in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    if attempt == max_retries - 1:
                        raise e
                    time.sleep(delay * (2 ** attempt))  # Exponential backoff
            return None
        return wrapper
    return decorator

def chunk_list(lst: List, chunk_size: int) -> List[List]:
    """Split list into chunks of specified size"""
    return [lst[i:i + chunk_size] for i in range(0, len(lst), chunk_size)]

def format_number(number: int) -> str:
    """Format large numbers for display (e.g., 1.5K, 2.3M)"""
    if number >= 1_000_000:
        return f"{number/1_000_000:.1f}M"
    elif number >= 1_000:
        return f"{number/1_000:.1f}K"
    else:
        return str(number)