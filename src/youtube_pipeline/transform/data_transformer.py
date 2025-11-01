import json
from datetime import datetime, timedelta
from typing import Dict, List, Any
from ..utils.logger import setup_logger

class DataTransformer:
    """Transform raw YouTube data into structured format for database"""
    
    def __init__(self, config: Dict):
        self.config = config
        self.logger = setup_logger(__name__)
    
    def transform_channel_data(self, raw_channels: List[Dict]) -> List[Dict]:
        """Transform raw channel data to database schema"""
        transformed = []
        
        for channel in raw_channels:
            try:
                transformed_channel = {
                    'channel_id': channel['id'],
                    'channel_name': channel.get('title', '').strip(),
                    'description': channel.get('description', '')[:1000],  # Limit length
                    'published_at': self._parse_datetime(channel.get('published_at')),
                    'country': channel.get('country', ''),
                    'view_count': channel.get('view_count', 0),
                    'subscriber_count': channel.get('subscriber_count', 0),
                    'video_count': channel.get('video_count', 0),
                    'custom_url': channel.get('custom_url', ''),
                    'thumbnails': json.dumps(channel.get('thumbnails', {})),
                    'created_date': datetime.now(),
                    'last_updated': datetime.now()
                }
                transformed.append(transformed_channel)
                
            except Exception as e:
                self.logger.error(f"Error transforming channel {channel.get('id')}: {e}")
                continue
        
        self.logger.info(f"Transformed {len(transformed)} channels")
        return transformed
    
    def transform_video_data(self, raw_videos: List[Dict]) -> List[Dict]:
        """Transform raw video data to database schema"""
        transformed = []
        
        for video in raw_videos:
            try:
                # Calculate engagement rate if possible
                view_count = video.get('view_count', 0)
                like_count = video.get('like_count', 0)
                engagement_rate = 0.0
                
                if view_count > 0:
                    engagement_rate = (like_count / view_count) * 100
                
                transformed_video = {
                    'video_id': video['video_id'],
                    'channel_id': video.get('channel_id', ''),
                    'title': video.get('title', '').strip()[:500],  # Limit length
                    'description': video.get('description', '')[:2000],
                    'published_at': self._parse_datetime(video.get('published_at')),
                    'duration': video.get('duration', ''),
                    'category_id': video.get('category_id', 0),
                    'tags': video.get('tags', []),
                    'default_language': video.get('default_audio_language', ''),
                    'view_count': view_count,
                    'like_count': like_count,
                    'comment_count': video.get('comment_count', 0),
                    'favorite_count': video.get('favorite_count', 0),
                    'engagement_rate': round(engagement_rate, 2),
                    'created_date': datetime.now(),
                    'last_updated': datetime.now()
                }
                transformed.append(transformed_video)
                
            except Exception as e:
                self.logger.error(f"Error transforming video {video.get('video_id')}: {e}")
                continue
        
        self.logger.info(f"Transformed {len(transformed)} videos")
        return transformed
    
    def transform_video_stats(self, raw_stats: List[Dict]) -> List[Dict]:
        """Transform video statistics for fact table"""
        transformed = []
        current_date = datetime.now().date()
        
        for stats in raw_stats:
            try:
                transformed_stats = {
                    'video_id': stats['video_id'],
                    'date_id': self._date_to_id(current_date),
                    'view_count': stats.get('view_count', 0),
                    'like_count': stats.get('like_count', 0),
                    'comment_count': stats.get('comment_count', 0),
                    'favorite_count': stats.get('favorite_count', 0),
                    'updated_at': datetime.now()
                }
                transformed.append(transformed_stats)
                
            except Exception as e:
                self.logger.error(f"Error transforming stats for video {stats.get('video_id')}: {e}")
                continue
        
        return transformed
    
    def create_date_dimension(self, start_date: str = "2020-01-01") -> List[Dict]:
        """Create date dimension table data"""
        dates = []
        start = datetime.fromisoformat(start_date).date()
        end = datetime.now().date()
        current = start
        
        while current <= end:
            dates.append({
                'date_id': self._date_to_id(current),
                'full_date': current,
                'day_name': current.strftime('%A'),
                'month_name': current.strftime('%B'),
                'year': current.year,
                'quarter': (current.month - 1) // 3 + 1,
                'week_number': current.isocalendar()[1],
                'is_weekend': current.weekday() >= 5
            })
            current += timedelta(days=1)
        
        return dates
    
    def _parse_datetime(self, date_string: str) -> datetime:
        """Parse ISO datetime string to Python datetime"""
        if not date_string:
            return datetime.now()
        
        try:
            # Handle different datetime formats
            if 'Z' in date_string:
                date_string = date_string.replace('Z', '+00:00')
            return datetime.fromisoformat(date_string)
        except ValueError:
            self.logger.warning(f"Invalid date format: {date_string}")
            return datetime.now()
    
    def _date_to_id(self, date: datetime.date) -> int:
        """Convert date to integer ID (YYYYMMDD)"""
        return int(date.strftime('%Y%m%d'))