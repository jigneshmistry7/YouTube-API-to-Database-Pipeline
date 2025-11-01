import re
from typing import Dict, List
from datetime import datetime
from ..utils.logger import setup_logger

class DataEnricher:
    """Enrich data with derived metrics and additional insights"""
    
    def __init__(self, config: Dict):
        self.config = config
        self.logger = setup_logger(__name__)
    
    def enrich_channel_data(self, channels: List[Dict]) -> List[Dict]:
        """Enrich channel data with derived metrics"""
        enriched_channels = []
        
        for channel in channels:
            try:
                enriched_channel = channel.copy()
                
                # Calculate growth metrics
                subscriber_count = channel.get('subscriber_count', 0)
                view_count = channel.get('view_count', 0)
                video_count = channel.get('video_count', 0)
                
                # Average views per video
                avg_views_per_video = 0
                if video_count > 0:
                    avg_views_per_video = view_count / video_count
                
                # Subscriber engagement ratio
                engagement_ratio = 0
                if subscriber_count > 0:
                    engagement_ratio = view_count / subscriber_count
                
                # Channel age (if published_at is available)
                channel_age_days = 0
                if channel.get('published_at'):
                    published = channel['published_at']
                    if isinstance(published, str):
                        published = datetime.fromisoformat(published.replace('Z', '+00:00'))
                    channel_age_days = (datetime.now() - published).days
                
                # Videos per day
                videos_per_day = 0
                if channel_age_days > 0:
                    videos_per_day = video_count / channel_age_days
                
                # Add enriched fields
                enriched_channel.update({
                    'avg_views_per_video': round(avg_views_per_video, 2),
                    'engagement_ratio': round(engagement_ratio, 2),
                    'channel_age_days': channel_age_days,
                    'videos_per_day': round(videos_per_day, 2),
                    'content_quality_score': self._calculate_content_quality(channel),
                    'growth_tier': self._classify_growth_tier(subscriber_count)
                })
                
                enriched_channels.append(enriched_channel)
                
            except Exception as e:
                self.logger.error(f"Error enriching channel {channel.get('channel_id')}: {e}")
                enriched_channels.append(channel)  # Keep original data
        
        return enriched_channels
    
    def enrich_video_data(self, videos: List[Dict]) -> List[Dict]:
        """Enrich video data with derived metrics"""
        enriched_videos = []
        
        for video in videos:
            try:
                enriched_video = video.copy()
                
                # Calculate performance metrics
                view_count = video.get('view_count', 0)
                like_count = video.get('like_count', 0)
                comment_count = video.get('comment_count', 0)
                
                # Engagement rate
                engagement_rate = 0.0
                if view_count > 0:
                    engagement_rate = ((like_count + comment_count) / view_count) * 100
                
                # Like to comment ratio
                like_comment_ratio = 0.0
                if comment_count > 0:
                    like_comment_ratio = like_count / comment_count
                
                # Video performance score
                performance_score = self._calculate_performance_score(video)
                
                # Content type classification
                content_type = self._classify_content_type(video)
                
                # Estimated duration in minutes
                duration_minutes = self._parse_duration_minutes(video.get('duration', ''))
                
                # Add enriched fields
                enriched_video.update({
                    'engagement_rate': round(engagement_rate, 2),
                    'like_comment_ratio': round(like_comment_ratio, 2),
                    'performance_score': performance_score,
                    'content_type': content_type,
                    'duration_minutes': duration_minutes,
                    'title_length': len(video.get('title', '')),
                    'has_description': bool(video.get('description', '').strip()),
                    'tag_count': len(video.get('tags', []))
                })
                
                enriched_videos.append(enriched_video)
                
            except Exception as e:
                self.logger.error(f"Error enriching video {video.get('video_id')}: {e}")
                enriched_videos.append(video)  # Keep original data
        
        return enriched_videos
    
    def _calculate_content_quality(self, channel: Dict) -> float:
        """Calculate content quality score based on various metrics"""
        score = 0.0
        
        # Based on subscriber to view ratio
        subscriber_count = channel.get('subscriber_count', 0)
        view_count = channel.get('view_count', 0)
        
        if subscriber_count > 0 and view_count > 0:
            view_ratio = view_count / subscriber_count
            if view_ratio > 10:
                score += 3.0
            elif view_ratio > 5:
                score += 2.0
            elif view_ratio > 2:
                score += 1.0
        
        # Based on video frequency
        video_count = channel.get('video_count', 0)
        if channel.get('published_at'):
            published = channel['published_at']
            if isinstance(published, str):
                published = datetime.fromisoformat(published.replace('Z', '+00:00'))
            channel_age_days = (datetime.now() - published).days
            
            if channel_age_days > 0:
                videos_per_week = (video_count / channel_age_days) * 7
                if 0.5 <= videos_per_week <= 3:  # Optimal posting frequency
                    score += 2.0
        
        return min(score, 5.0)  # Cap at 5
    
    def _classify_growth_tier(self, subscriber_count: int) -> str:
        """Classify channel growth tier"""
        if subscriber_count >= 1000000:
            return "Mega"
        elif subscriber_count >= 100000:
            return "Macro"
        elif subscriber_count >= 10000:
            return "Mid-tier"
        elif subscriber_count >= 1000:
            return "Micro"
        else:
            return "Nano"
    
    def _calculate_performance_score(self, video: Dict) -> float:
        """Calculate video performance score (0-100)"""
        score = 0.0
        
        # View-based scoring
        view_count = video.get('view_count', 0)
        if view_count >= 1000000:
            score += 40
        elif view_count >= 100000:
            score += 30
        elif view_count >= 10000:
            score += 20
        elif view_count >= 1000:
            score += 10
        
        # Engagement-based scoring
        engagement_rate = video.get('engagement_rate', 0)
        if engagement_rate >= 10:
            score += 30
        elif engagement_rate >= 5:
            score += 20
        elif engagement_rate >= 2:
            score += 10
        
        # Like ratio scoring
        view_count = video.get('view_count', 1)  # Avoid division by zero
        like_ratio = video.get('like_count', 0) / view_count
        if like_ratio >= 0.1:
            score += 30
        elif like_ratio >= 0.05:
            score += 20
        elif like_ratio >= 0.02:
            score += 10
        
        return min(score, 100.0)
    
    def _classify_content_type(self, video: Dict) -> str:
        """Classify video content type based on title and description"""
        title = (video.get('title', '') + ' ' + video.get('description', '')).lower()
        
        content_keywords = {
            'tutorial': ['tutorial', 'how to', 'guide', 'learn', 'step by step'],
            'review': ['review', 'unboxing', 'test', 'compared', 'vs'],
            'entertainment': ['funny', 'prank', 'challenge', 'comedy', 'music'],
            'news': ['news', 'update', 'announcement', 'breaking'],
            'educational': ['explained', 'science', 'history', 'documentary'],
            'gaming': ['gameplay', 'walkthrough', 'gaming', 'stream']
        }
        
        for content_type, keywords in content_keywords.items():
            if any(keyword in title for keyword in keywords):
                return content_type
        
        return 'other'
    
    def _parse_duration_minutes(self, duration: str) -> float:
        """Parse ISO 8601 duration to minutes"""
        if not duration or not duration.startswith('PT'):
            return 0.0
        
        try:
            # Parse PT1H30M15S format
            hours = 0
            minutes = 0
            seconds = 0
            
            # Extract hours
            if 'H' in duration:
                hours_str = duration.split('H')[0].replace('PT', '')
                hours = int(hours_str)
                duration = duration.split('H', 1)[1]
            
            # Extract minutes
            if 'M' in duration:
                minutes_str = duration.split('M')[0]
                minutes = int(minutes_str)
                duration = duration.split('M', 1)[1]
            
            # Extract seconds
            if 'S' in duration:
                seconds_str = duration.split('S')[0]
                seconds = int(seconds_str)
            
            total_minutes = hours * 60 + minutes + seconds / 60
            return round(total_minutes, 2)
            
        except (ValueError, IndexError):
            return 0.0