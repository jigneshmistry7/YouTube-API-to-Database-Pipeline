import re
from datetime import datetime
from typing import Dict, List, Tuple
from ..utils.logger import setup_logger

class DataValidator:
    """Validate and clean extracted YouTube data"""
    
    def __init__(self, config: Dict):
        self.config = config
        self.logger = setup_logger(__name__)
        self.validation_errors = []
    
    def validate_channel_data(self, channel_data: Dict) -> Tuple[bool, Dict]:
        """Validate channel data structure and content"""
        errors = []
        validated_data = channel_data.copy()
        
        # Required fields validation
        required_fields = ['id', 'title', 'published_at']
        for field in required_fields:
            if not channel_data.get(field):
                errors.append(f"Missing required field: {field}")
        
        # ID format validation
        if channel_data.get('id') and not self._is_valid_channel_id(channel_data['id']):
            errors.append(f"Invalid channel ID format: {channel_data['id']}")
        
        # Date validation
        if channel_data.get('published_at'):
            if not self._is_valid_date(channel_data['published_at']):
                errors.append(f"Invalid date format: {channel_data['published_at']}")
        
        # Numeric field validation
        numeric_fields = ['view_count', 'subscriber_count', 'video_count']
        for field in numeric_fields:
            value = channel_data.get(field, 0)
            if not isinstance(value, (int, float)) or value < 0:
                errors.append(f"Invalid {field}: {value}")
                validated_data[field] = 0
        
        if errors:
            self.validation_errors.extend(errors)
            self.logger.warning(f"Channel validation errors: {errors}")
            return False, validated_data
        
        return True, validated_data
    
    def validate_video_data(self, video_data: Dict) -> Tuple[bool, Dict]:
        """Validate video data structure and content"""
        errors = []
        validated_data = video_data.copy()
        
        # Required fields validation
        required_fields = ['id', 'title', 'channel_id', 'published_at']
        for field in required_fields:
            if not video_data.get(field):
                errors.append(f"Missing required field: {field}")
        
        # Video ID validation
        if video_data.get('id') and not self._is_valid_video_id(video_data['id']):
            errors.append(f"Invalid video ID format: {video_data['id']}")
        
        # Text field cleaning
        if video_data.get('title'):
            validated_data['title'] = self._clean_text(video_data['title'])
        
        if video_data.get('description'):
            validated_data['description'] = self._clean_text(video_data['description'])
        
        # Duration validation
        if video_data.get('duration'):
            if not self._is_valid_duration(video_data['duration']):
                errors.append(f"Invalid duration format: {video_data['duration']}")
        
        if errors:
            self.validation_errors.extend(errors)
            self.logger.warning(f"Video validation errors: {errors}")
            return False, validated_data
        
        return True, validated_data
    
    def _is_valid_channel_id(self, channel_id: str) -> bool:
        """Validate channel ID format"""
        # YouTube channel IDs start with UC followed by 22 characters
        return bool(re.match(r'^UC[\w-]{22}$', channel_id))
    
    def _is_valid_video_id(self, video_id: str) -> bool:
        """Validate video ID format"""
        # YouTube video IDs are 11 characters
        return bool(re.match(r'^[\w-]{11}$', video_id))
    
    def _is_valid_date(self, date_string: str) -> bool:
        """Validate ISO date format"""
        try:
            datetime.fromisoformat(date_string.replace('Z', '+00:00'))
            return True
        except ValueError:
            return False
    
    def _is_valid_duration(self, duration: str) -> bool:
        """Validate ISO 8601 duration format"""
        return bool(re.match(r'^PT(\d+H)?(\d+M)?(\d+S)?$', duration))
    
    def _clean_text(self, text: str) -> str:
        """Clean and normalize text data"""
        if not text:
            return ""
        
        # Remove extra whitespace
        text = ' '.join(text.split())
        
        # Remove problematic characters (optional)
        text = text.replace('\x00', '')  # Remove null bytes
        
        return text
    
    def get_validation_report(self) -> Dict:
        """Get comprehensive validation report"""
        return {
            'total_errors': len(self.validation_errors),
            'errors': self.validation_errors,
            'timestamp': datetime.now().isoformat()
        }