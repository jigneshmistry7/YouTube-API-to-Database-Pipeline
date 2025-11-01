import psycopg2
from psycopg2.extras import execute_batch
from typing import Dict, List, Any, Optional
import json
from datetime import datetime
from ..utils.logger import setup_logger

class DatabaseManager:
    """Manage database connections and operations for YouTube data"""
    
    def __init__(self, config: Dict):
        self.config = config
        self.logger = setup_logger(__name__)
        self.connection = None
        self.schema_manager = SchemaManager(config)
    
    def connect(self) -> bool:
        """Establish database connection"""
        try:
            db_config = self.config['storage']['database']
            self.connection = psycopg2.connect(
                host=db_config.get('host', 'localhost'),
                port=db_config.get('port', 5432),
                database=db_config.get('database', 'youtube_analytics'),
                user=db_config.get('username', 'postgres'),
                password=db_config.get('password', ''),
                connect_timeout=10
            )
            self.logger.info("Database connection established successfully")
            return True
            
        except psycopg2.Error as e:
            self.logger.error(f"Database connection failed: {e}")
            return False
    
    def disconnect(self):
        """Close database connection"""
        if self.connection:
            self.connection.close()
            self.logger.info("Database connection closed")
    
    def load_data(self, processed_data: Dict) -> bool:
        """Load processed data into database"""
        if not self.connection:
            if not self.connect():
                return False
        
        try:
            # Ensure tables exist
            self.schema_manager.create_tables(self.connection)
            
            # Load data in transaction
            with self.connection:
                with self.connection.cursor() as cursor:
                    # Load date dimension if needed
                    if 'date_dimension' in processed_data:
                        self._load_date_dimension(cursor, processed_data['date_dimension'])
                    
                    # Load channels
                    if 'channels' in processed_data:
                        self._load_channels(cursor, processed_data['channels'])
                    
                    # Load videos
                    if 'videos' in processed_data:
                        self._load_videos(cursor, processed_data['videos'])
                    
                    # Load video statistics
                    if 'video_stats' in processed_data:
                        self._load_video_stats(cursor, processed_data['video_stats'])
                    
                    # Load comments if available
                    if 'comments' in processed_data:
                        self._load_comments(cursor, processed_data['comments'])
            
            self.logger.info("Data loaded successfully into database")
            return True
            
        except psycopg2.Error as e:
            self.logger.error(f"Error loading data: {e}")
            self.connection.rollback()
            return False
    
    def _load_channels(self, cursor, channels: List[Dict]):
        """Load channel data into dim_channels table"""
        query = """
        INSERT INTO dim_channels (
            channel_id, channel_name, description, published_at, country,
            view_count, subscriber_count, video_count, custom_url, thumbnails,
            created_date, last_updated
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (channel_id) DO UPDATE SET
            channel_name = EXCLUDED.channel_name,
            description = EXCLUDED.description,
            view_count = EXCLUDED.view_count,
            subscriber_count = EXCLUDED.subscriber_count,
            video_count = EXCLUDED.video_count,
            custom_url = EXCLUDED.custom_url,
            thumbnails = EXCLUDED.thumbnails,
            last_updated = EXCLUDED.last_updated
        """
        
        data_tuples = []
        for channel in channels:
            data_tuples.append((
                channel['channel_id'],
                channel['channel_name'],
                channel['description'],
                channel['published_at'],
                channel['country'],
                channel['view_count'],
                channel['subscriber_count'],
                channel['video_count'],
                channel['custom_url'],
                channel['thumbnails'],
                channel['created_date'],
                channel['last_updated']
            ))
        
        execute_batch(cursor, query, data_tuples)
        self.logger.info(f"Loaded/Updated {len(channels)} channels")
    
    def _load_videos(self, cursor, videos: List[Dict]):
        """Load video data into dim_videos table"""
        query = """
        INSERT INTO dim_videos (
            video_id, channel_id, title, description, published_at,
            duration, category_id, tags, default_language, view_count,
            like_count, comment_count, favorite_count, engagement_rate,
            created_date, last_updated
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (video_id) DO UPDATE SET
            title = EXCLUDED.title,
            description = EXCLUDED.description,
            view_count = EXCLUDED.view_count,
            like_count = EXCLUDED.like_count,
            comment_count = EXCLUDED.comment_count,
            favorite_count = EXCLUDED.favorite_count,
            engagement_rate = EXCLUDED.engagement_rate,
            last_updated = EXCLUDED.last_updated
        """
        
        data_tuples = []
        for video in videos:
            data_tuples.append((
                video['video_id'],
                video['channel_id'],
                video['title'],
                video['description'],
                video['published_at'],
                video['duration'],
                video['category_id'],
                video['tags'],
                video['default_language'],
                video['view_count'],
                video['like_count'],
                video['comment_count'],
                video['favorite_count'],
                video['engagement_rate'],
                video['created_date'],
                video['last_updated']
            ))
        
        execute_batch(cursor, query, data_tuples)
        self.logger.info(f"Loaded/Updated {len(videos)} videos")
    
    def _load_video_stats(self, cursor, video_stats: List[Dict]):
        """Load video statistics into fact_video_stats table"""
        query = """
        INSERT INTO fact_video_stats (
            video_id, date_id, view_count, like_count, 
            comment_count, favorite_count, updated_at
        ) VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (video_id, date_id) DO UPDATE SET
            view_count = EXCLUDED.view_count,
            like_count = EXCLUDED.like_count,
            comment_count = EXCLUDED.comment_count,
            favorite_count = EXCLUDED.favorite_count,
            updated_at = EXCLUDED.updated_at
        """
        
        data_tuples = []
        for stats in video_stats:
            data_tuples.append((
                stats['video_id'],
                stats['date_id'],
                stats['view_count'],
                stats['like_count'],
                stats['comment_count'],
                stats['favorite_count'],
                stats['updated_at']
            ))
        
        execute_batch(cursor, query, data_tuples)
        self.logger.info(f"Loaded/Updated {len(video_stats)} video statistics")
    
    def _load_date_dimension(self, cursor, dates: List[Dict]):
        """Load date dimension data"""
        query = """
        INSERT INTO dim_dates (
            date_id, full_date, day_name, month_name, year, 
            quarter, week_number, is_weekend
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (date_id) DO NOTHING
        """
        
        data_tuples = []
        for date in dates:
            data_tuples.append((
                date['date_id'],
                date['full_date'],
                date['day_name'],
                date['month_name'],
                date['year'],
                date['quarter'],
                date['week_number'],
                date['is_weekend']
            ))
        
        execute_batch(cursor, query, data_tuples)
        self.logger.info(f"Loaded {len(dates)} date records")
    
    def _load_comments(self, cursor, comments: List[Dict]):
        """Load comment data into fact_comments table"""
        query = """
        INSERT INTO fact_comments (
            comment_id, video_id, author, comment_text, likes_count,
            published_at, sentiment_score, updated_at
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (comment_id) DO UPDATE SET
            likes_count = EXCLUDED.likes_count,
            sentiment_score = EXCLUDED.sentiment_score,
            updated_at = EXCLUDED.updated_at
        """
        
        data_tuples = []
        for comment in comments:
            data_tuples.append((
                comment['comment_id'],
                comment['video_id'],
                comment['author'],
                comment['comment_text'],
                comment['likes_count'],
                comment['published_at'],
                comment.get('sentiment_score'),
                comment['updated_at']
            ))
        
        execute_batch(cursor, query, data_tuples)
        self.logger.info(f"Loaded {len(comments)} comments")
    
    def execute_query(self, query: str, params: tuple = None) -> List[Dict]:
        """Execute a read query and return results"""
        if not self.connection:
            if not self.connect():
                return []
        
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(query, params)
                if cursor.description:
                    columns = [desc[0] for desc in cursor.description]
                    return [dict(zip(columns, row)) for row in cursor.fetchall()]
                return []
        except psycopg2.Error as e:
            self.logger.error(f"Query execution failed: {e}")
            return []
    
    def get_database_stats(self) -> Dict:
        """Get database statistics"""
        stats_queries = {
            'total_channels': "SELECT COUNT(*) FROM dim_channels",
            'total_videos': "SELECT COUNT(*) FROM dim_videos",
            'total_stats_records': "SELECT COUNT(*) FROM fact_video_stats",
            'latest_update': "SELECT MAX(updated_at) FROM fact_video_stats"
        }
        
        stats = {}
        for key, query in stats_queries.items():
            result = self.execute_query(query)
            if result:
                stats[key] = list(result[0].values())[0]
        
        return stats