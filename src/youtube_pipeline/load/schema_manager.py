import psycopg2
from typing import List
from ..utils.logger import setup_logger

class SchemaManager:
    """Manage database schema creation and migrations"""
    
    def __init__(self, config: Dict):
        self.config = config
        self.logger = setup_logger(__name__)
        self.table_definitions = self._get_table_definitions()
    
    def create_tables(self, connection) -> bool:
        """Create all necessary tables if they don't exist"""
        try:
            with connection.cursor() as cursor:
                for table_name, create_sql in self.table_definitions.items():
                    cursor.execute(create_sql)
                    self.logger.info(f"Table {table_name} ensured")
            
            connection.commit()
            self.logger.info("All database tables created successfully")
            return True
            
        except psycopg2.Error as e:
            self.logger.error(f"Error creating tables: {e}")
            connection.rollback()
            return False
    
    def _get_table_definitions(self) -> Dict[str, str]:
        """Get SQL definitions for all tables"""
        return {
            'dim_channels': """
                CREATE TABLE IF NOT EXISTS dim_channels (
                    channel_id VARCHAR(255) PRIMARY KEY,
                    channel_name VARCHAR(255) NOT NULL,
                    description TEXT,
                    published_at TIMESTAMP,
                    country VARCHAR(100),
                    view_count BIGINT DEFAULT 0,
                    subscriber_count BIGINT DEFAULT 0,
                    video_count INTEGER DEFAULT 0,
                    custom_url VARCHAR(255),
                    thumbnails JSONB,
                    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """,
            
            'dim_videos': """
                CREATE TABLE IF NOT EXISTS dim_videos (
                    video_id VARCHAR(255) PRIMARY KEY,
                    channel_id VARCHAR(255) REFERENCES dim_channels(channel_id),
                    title VARCHAR(500) NOT NULL,
                    description TEXT,
                    published_at TIMESTAMP,
                    duration VARCHAR(50),
                    category_id INTEGER,
                    tags TEXT[],
                    default_language VARCHAR(10),
                    view_count BIGINT DEFAULT 0,
                    like_count BIGINT DEFAULT 0,
                    comment_count BIGINT DEFAULT 0,
                    favorite_count BIGINT DEFAULT 0,
                    engagement_rate DECIMAL(5,2),
                    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """,
            
            'dim_dates': """
                CREATE TABLE IF NOT EXISTS dim_dates (
                    date_id INTEGER PRIMARY KEY,
                    full_date DATE NOT NULL UNIQUE,
                    day_name VARCHAR(10) NOT NULL,
                    month_name VARCHAR(10) NOT NULL,
                    year INTEGER NOT NULL,
                    quarter INTEGER NOT NULL,
                    week_number INTEGER NOT NULL,
                    is_weekend BOOLEAN NOT NULL
                )
            """,
            
            'fact_video_stats': """
                CREATE TABLE IF NOT EXISTS fact_video_stats (
                    stat_id SERIAL PRIMARY KEY,
                    video_id VARCHAR(255) REFERENCES dim_videos(video_id),
                    date_id INTEGER REFERENCES dim_dates(date_id),
                    view_count BIGINT DEFAULT 0,
                    like_count BIGINT DEFAULT 0,
                    comment_count BIGINT DEFAULT 0,
                    favorite_count BIGINT DEFAULT 0,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(video_id, date_id)
                )
            """,
            
            'fact_comments': """
                CREATE TABLE IF NOT EXISTS fact_comments (
                    comment_id VARCHAR(255) PRIMARY KEY,
                    video_id VARCHAR(255) REFERENCES dim_videos(video_id),
                    author VARCHAR(255),
                    comment_text TEXT,
                    likes_count INTEGER DEFAULT 0,
                    published_at TIMESTAMP,
                    sentiment_score DECIMAL(3,2),
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """
        }
    
    def create_indexes(self, connection) -> bool:
        """Create performance indexes"""
        indexes = [
            "CREATE INDEX IF NOT EXISTS idx_videos_channel_id ON dim_videos(channel_id)",
            "CREATE INDEX IF NOT EXISTS idx_videos_published_at ON dim_videos(published_at)",
            "CREATE INDEX IF NOT EXISTS idx_stats_video_id ON fact_video_stats(video_id)",
            "CREATE INDEX IF NOT EXISTS idx_stats_date_id ON fact_video_stats(date_id)",
            "CREATE INDEX IF NOT EXISTS idx_comments_video_id ON fact_comments(video_id)",
            "CREATE INDEX IF NOT EXISTS idx_channels_subscribers ON dim_channels(subscriber_count)",
            "CREATE INDEX IF NOT EXISTS idx_videos_views ON dim_videos(view_count)"
        ]
        
        try:
            with connection.cursor() as cursor:
                for index_sql in indexes:
                    cursor.execute(index_sql)
            
            connection.commit()
            self.logger.info("All indexes created successfully")
            return True
            
        except psycopg2.Error as e:
            self.logger.error(f"Error creating indexes: {e}")
            connection.rollback()
            return False
    
    def validate_schema(self, connection) -> Dict:
        """Validate that all required tables and columns exist"""
        validation_query = """
        SELECT 
            table_name,
            column_name,
            data_type,
            is_nullable
        FROM information_schema.columns 
        WHERE table_schema = 'public'
        ORDER BY table_name, ordinal_position
        """
        
        try:
            with connection.cursor() as cursor:
                cursor.execute(validation_query)
                columns = cursor.fetchall()
            
            # Group by table
            table_columns = {}
            for table, column, data_type, nullable in columns:
                if table not in table_columns:
                    table_columns[table] = []
                table_columns[table].append({
                    'column': column,
                    'type': data_type,
                    'nullable': nullable == 'YES'
                })
            
            # Check required tables
            required_tables = set(self.table_definitions.keys())
            existing_tables = set(table_columns.keys())
            missing_tables = required_tables - existing_tables
            
            return {
                'is_valid': len(missing_tables) == 0,
                'existing_tables': list(existing_tables),
                'missing_tables': list(missing_tables),
                'table_details': table_columns
            }
            
        except psycopg2.Error as e:
            self.logger.error(f"Error validating schema: {e}")
            return {'is_valid': False, 'error': str(e)}