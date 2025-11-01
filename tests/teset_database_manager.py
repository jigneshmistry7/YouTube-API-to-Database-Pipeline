import pytest
import psycopg2
from unittest.mock import Mock, patch, MagicMock
import sys
import os

# Add the project root to Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from youtube_pipeline.load.database_manager import DatabaseManager
from youtube_pipeline.load.schema_manager import SchemaManager

class TestDatabaseManager:
    """Test database manager functionality"""
    
    @pytest.fixture
    def mock_config(self):
        """Mock configuration"""
        return {
            'storage': {
                'database': {
                    'host': 'localhost',
                    'port': 5432,
                    'database': 'test_youtube_analytics',
                    'username': 'test_user',
                    'password': 'test_password'
                }
            }
        }
    
    @pytest.fixture
    def mock_connection(self):
        """Mock database connection"""
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_conn.cursor.return_value = mock_cursor
        return mock_conn
    
    @pytest.fixture
    def db_manager(self, mock_config):
        """Create database manager instance"""
        return DatabaseManager(mock_config)
    
    def test_connect_success(self, db_manager, mock_config):
        """Test successful database connection"""
        with patch('psycopg2.connect') as mock_connect:
            mock_connect.return_value = Mock()
            
            result = db_manager.connect()
            
            assert result == True
            mock_connect.assert_called_once()
    
    def test_connect_failure(self, db_manager):
        """Test database connection failure"""
        with patch('psycopg2.connect') as mock_connect:
            mock_connect.side_effect = psycopg2.Error("Connection failed")
            
            result = db_manager.connect()
            
            assert result == False
    
    def test_load_channels(self, db_manager, mock_connection):
        """Test channel data loading"""
        channel_data = [{
            'channel_id': 'UC_test_1',
            'channel_name': 'Test Channel 1',
            'description': 'Test Description 1',
            'published_at': '2020-01-01T00:00:00Z',
            'country': 'US',
            'view_count': 1000000,
            'subscriber_count': 50000,
            'video_count': 200,
            'custom_url': '@testchannel1',
            'thumbnails': '{}',
            'created_date': '2023-01-01T00:00:00Z',
            'last_updated': '2023-01-01T00:00:00Z'
        }]
        
        processed_data = {
            'channels': channel_data
        }
        
        db_manager.connection = mock_connection
        
        with patch.object(db_manager, '_load_channels') as mock_load:
            result = db_manager.load_data(processed_data)
            
            assert result == True
            mock_load.assert_called_once()
    
    def test_execute_query_success(self, db_manager, mock_connection):
        """Test query execution success"""
        mock_cursor = Mock()
        mock_cursor.description = [('column1',), ('column2',)]
        mock_cursor.fetchall.return_value = [('value1', 'value2')]
        mock_connection.cursor.return_value = mock_cursor
        
        db_manager.connection = mock_connection
        
        result = db_manager.execute_query("SELECT * FROM test_table")
        
        assert len(result) == 1
        assert result[0]['column1'] == 'value1'
        assert result[0]['column2'] == 'value2'
    
    def test_execute_query_failure(self, db_manager, mock_connection):
        """Test query execution failure"""
        mock_connection.cursor.side_effect = psycopg2.Error("Query failed")
        
        db_manager.connection = mock_connection
        
        result = db_manager.execute_query("SELECT * FROM test_table")
        
        assert result == []

class TestSchemaManager:
    """Test schema manager functionality"""
    
    @pytest.fixture
    def schema_manager(self):
        """Create schema manager instance"""
        return SchemaManager({})
    
    def test_create_tables_success(self, schema_manager, mock_connection):
        """Test successful table creation"""
        mock_cursor = Mock()
        mock_connection.cursor.return_value = mock_cursor
        
        result = schema_manager.create_tables(mock_connection)
        
        assert result == True
        assert mock_cursor.execute.call_count > 0
    
    def test_create_tables_failure(self, schema_manager, mock_connection):
        """Test table creation failure"""
        mock_cursor = Mock()
        mock_cursor.execute.side_effect = psycopg2.Error("Table creation failed")
        mock_connection.cursor.return_value = mock_cursor
        
        result = schema_manager.create_tables(mock_connection)
        
        assert result == False
    
    def test_validate_schema_success(self, schema_manager, mock_connection):
        """Test schema validation success"""
        mock_cursor = Mock()
        mock_cursor.fetchall.return_value = [
            ('dim_channels', 'channel_id', 'character varying', 'NO'),
            ('dim_channels', 'channel_name', 'character varying', 'NO')
        ]
        mock_connection.cursor.return_value = mock_cursor
        
        result = schema_manager.validate_schema(mock_connection)
        
        assert result['is_valid'] == True
        assert 'dim_channels' in result['existing_tables']

if __name__ == "__main__":
    pytest.main([__file__, "-v"])