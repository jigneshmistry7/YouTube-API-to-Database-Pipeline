import requests
import time
import json
from typing import Dict, List, Optional
from ..utils.logger import setup_logger
from .auth_manager import AuthManager
from .rate_limiter import RateLimiter

class YouTubeClient:
    """Client for interacting with YouTube Data API v3"""
    
    BASE_URL = "https://www.googleapis.com/youtube/v3"
    
    def __init__(self, config: Dict):
        self.config = config
        self.auth_manager = AuthManager(config)
        self.rate_limiter = RateLimiter(config)
        self.logger = setup_logger(__name__)
        self.session = requests.Session()
    
    def fetch_data(self, channel_ids: Optional[List[str]] = None) -> Dict:
        """Fetch data from YouTube APIs"""
        self.logger.info("Fetching data from YouTube APIs")
        
        data = {
            'channels': [],
            'videos': [],
            'comments': [],
            'search_results': []
        }
        
        try:
            # Fetch channel data
            if channel_ids:
                for channel_id in channel_ids:
                    channel_data = self.get_channel_data(channel_id)
                    if channel_data:
                        data['channels'].append(channel_data)
            
            # Fetch videos for channels
            for channel in data['channels']:
                videos = self.get_channel_videos(channel['id'])
                data['videos'].extend(videos)
            
            # Fetch video statistics
            video_ids = [video['id'] for video in data['videos']]
            if video_ids:
                video_stats = self.get_video_statistics(video_ids)
                data['video_stats'] = video_stats
            
            self.logger.info(f"Fetched {len(data['channels'])} channels, {len(data['videos'])} videos")
            
        except Exception as e:
            self.logger.error(f"Error fetching data: {e}")
            raise
        
        return data
    
    def get_channel_data(self, channel_id: str) -> Optional[Dict]:
        """Get channel information and statistics"""
        self.rate_limiter.wait_if_needed()
        
        params = {
            'part': 'snippet,statistics,contentDetails,brandingSettings',
            'id': channel_id,
            'key': self.auth_manager.get_api_key()
        }
        
        try:
            response = self.session.get(
                f"{self.BASE_URL}/channels",
                params=params,
                timeout=self.config['extraction']['timeout']
            )
            response.raise_for_status()
            
            data = response.json()
            if data.get('items'):
                return self._parse_channel_data(data['items'][0])
            
        except requests.RequestException as e:
            self.logger.error(f"Error fetching channel data for {channel_id}: {e}")
            return None
    
    def get_channel_videos(self, channel_id: str, max_results: int = 50) -> List[Dict]:
        """Get videos from a channel"""
        self.rate_limiter.wait_if_needed()
        
        params = {
            'part': 'snippet',
            'channelId': channel_id,
            'maxResults': min(max_results, 50),
            'order': 'date',
            'type': 'video',
            'key': self.auth_manager.get_api_key()
        }
        
        try:
            response = self.session.get(
                f"{self.BASE_URL}/search",
                params=params,
                timeout=self.config['extraction']['timeout']
            )
            response.raise_for_status()
            
            data = response.json()
            return [self._parse_video_data(item) for item in data.get('items', [])]
            
        except requests.RequestException as e:
            self.logger.error(f"Error fetching videos for channel {channel_id}: {e}")
            return []
    
    def get_video_statistics(self, video_ids: List[str]) -> List[Dict]:
        """Get statistics for multiple videos"""
        if not video_ids:
            return []
        
        self.rate_limiter.wait_if_needed()
        
        # YouTube API allows max 50 videos per request
        batch_size = 50
        all_stats = []
        
        for i in range(0, len(video_ids), batch_size):
            batch = video_ids[i:i + batch_size]
            
            params = {
                'part': 'snippet,statistics,contentDetails',
                'id': ','.join(batch),
                'key': self.auth_manager.get_api_key()
            }
            
            try:
                response = self.session.get(
                    f"{self.BASE_URL}/videos",
                    params=params,
                    timeout=self.config['extraction']['timeout']
                )
                response.raise_for_status()
                
                data = response.json()
                batch_stats = [self._parse_video_stats(item) for item in data.get('items', [])]
                all_stats.extend(batch_stats)
                
            except requests.RequestException as e:
                self.logger.error(f"Error fetching video statistics: {e}")
                continue
        
        return all_stats
    
    def _parse_channel_data(self, raw_data: Dict) -> Dict:
        """Parse raw channel data into structured format"""
        snippet = raw_data.get('snippet', {})
        stats = raw_data.get('statistics', {})
        
        return {
            'id': raw_data['id'],
            'title': snippet.get('title', ''),
            'description': snippet.get('description', ''),
            'published_at': snippet.get('publishedAt', ''),
            'country': snippet.get('country', ''),
            'view_count': int(stats.get('viewCount', 0)),
            'subscriber_count': int(stats.get('subscriberCount', 0)),
            'video_count': int(stats.get('videoCount', 0)),
            'custom_url': snippet.get('customUrl', ''),
            'thumbnails': snippet.get('thumbnails', {}),
            'fetched_at': time.time()
        }
    
    def _parse_video_data(self, raw_data: Dict) -> Dict:
        """Parse raw video data into structured format"""
        snippet = raw_data.get('snippet', {})
        
        return {
            'id': raw_data['id']['videoId'],
            'title': snippet.get('title', ''),
            'description': snippet.get('description', ''),
            'published_at': snippet.get('publishedAt', ''),
            'channel_id': snippet.get('channelId', ''),
            'channel_title': snippet.get('channelTitle', ''),
            'thumbnails': snippet.get('thumbnails', {}),
            'live_broadcast_content': snippet.get('liveBroadcastContent', 'none')
        }
    
    def _parse_video_stats(self, raw_data: Dict) -> Dict:
        """Parse raw video statistics into structured format"""
        snippet = raw_data.get('snippet', {})
        stats = raw_data.get('statistics', {})
        content_details = raw_data.get('contentDetails', {})
        
        return {
            'video_id': raw_data['id'],
            'title': snippet.get('title', ''),
            'description': snippet.get('description', ''),
            'published_at': snippet.get('publishedAt', ''),
            'channel_id': snippet.get('channelId', ''),
            'category_id': snippet.get('categoryId', ''),
            'tags': snippet.get('tags', []),
            'duration': content_details.get('duration', ''),
            'view_count': int(stats.get('viewCount', 0)),
            'like_count': int(stats.get('likeCount', 0)),
            'comment_count': int(stats.get('commentCount', 0)),
            'favorite_count': int(stats.get('favoriteCount', 0)),
            'fetched_at': time.time()
        }