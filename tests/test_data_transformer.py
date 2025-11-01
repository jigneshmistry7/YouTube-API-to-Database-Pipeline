import pytest
from datetime import datetime
import sys
import os

# Add the project root to Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from youtube_pipeline.transform.data_transformer import DataTransformer
from youtube_pipeline.transform.data_validator import DataValidator

class TestDataTransformer:
    """Test data transformation functionality"""
    
    @pytest.fixture
    def mock_config(self):
        """Mock configuration"""
        return {
            'processing': {
                'calculate_engagement_rate': True
            }
        }
    
    @pytest.fixture
    def transformer(self, mock_config):
        """Create data transformer instance"""
        return DataTransformer(mock_config)
    
    @pytest.fixture
    def sample_channel_data(self):
        """Sample raw channel data for testing"""
        return [{
            "id": "UC_test_channel",
            "title": "Test Channel",
            "description": "Test Description",
            "published_at": "2020-01-01T00:00:00Z",
            "country": "US",
            "view_count": 1000000,
            "subscriber_count": 50000,
            "video_count": 200,
            "custom_url": "@testchannel",
            "thumbnails": {"default": {"url": "http://test.com/thumb.jpg"}}
        }]
    
    @pytest.fixture
    def sample_video_data(self):
        """Sample raw video data for testing"""
        return [{
            "video_id": "test_video_123",
            "channel_id": "UC_test_channel",
            "title": "Test Video",
            "description": "Test Video Description",
            "published_at": "2023-01-01T00:00:00Z",
            "duration": "PT10M30S",
            "category_id": 27,
            "tags": ["test", "python", "tutorial"],
            "view_count": 50000,
            "like_count": 2500,
            "comment_count": 300,
            "favorite_count": 100
        }]
    
    def test_transform_channel_data(self, transformer, sample_channel_data):
        """Test channel data transformation"""
        transformed = transformer.transform_channel_data(sample_channel_data)
        
        assert len(transformed) == 1
        channel = transformed[0]
        
        assert channel['channel_id'] == "UC_test_channel"
        assert channel['channel_name'] == "Test Channel"
        assert channel['view_count'] == 1000000
        assert channel['subscriber_count'] == 50000
        assert channel['video_count'] == 200
        assert isinstance(channel['created_date'], datetime)
        assert isinstance(channel['last_updated'], datetime)
    
    def test_transform_video_data(self, transformer, sample_video_data):
        """Test video data transformation"""
        transformed = transformer.transform_video_data(sample_video_data)
        
        assert len(transformed) == 1
        video = transformed[0]
        
        assert video['video_id'] == "test_video_123"
        assert video['channel_id'] == "UC_test_channel"
        assert video['title'] == "Test Video"
        assert video['view_count'] == 50000
        assert video['like_count'] == 2500
        assert video['comment_count'] == 300
        assert video['engagement_rate'] == 5.0  # (2500 + 300) / 50000 * 100
    
    def test_transform_video_data_zero_views(self, transformer):
        """Test video transformation with zero views"""
        video_data = [{
            "video_id": "test_video_123",
            "channel_id": "UC_test_channel",
            "title": "Test Video",
            "published_at": "2023-01-01T00:00:00Z",
            "view_count": 0,
            "like_count": 0,
            "comment_count": 0
        }]
        
        transformed = transformer.transform_video_data(video_data)
        
        assert transformed[0]['engagement_rate'] == 0.0
    
    def test_create_date_dimension(self, transformer):
        """Test date dimension creation"""
        dates = transformer.create_date_dimension("2023-01-01")
        
        assert len(dates) > 0
        first_date = dates[0]
        
        assert 'date_id' in first_date
        assert 'full_date' in first_date
        assert 'day_name' in first_date
        assert 'month_name' in first_date
        assert 'year' in first_date
        assert 'quarter' in first_date
        assert 'week_number' in first_date
        assert 'is_weekend' in first_date
        
        # Check date_id format (YYYYMMDD)
        assert first_date['date_id'] == 20230101

class TestDataValidator:
    """Test data validation functionality"""
    
    @pytest.fixture
    def validator(self):
        """Create data validator instance"""
        return DataValidator({})
    
    def test_validate_channel_data_valid(self, validator):
        """Test validation of valid channel data"""
        channel_data = {
            "id": "UCtestchannel12345678901234",
            "title": "Test Channel",
            "published_at": "2020-01-01T00:00:00Z",
            "view_count": 1000000,
            "subscriber_count": 50000,
            "video_count": 200
        }
        
        is_valid, validated_data = validator.validate_channel_data(channel_data)
        
        assert is_valid == True
        assert validated_data['id'] == "UCtestchannel12345678901234"
    
    def test_validate_channel_data_invalid_id(self, validator):
        """Test validation of channel data with invalid ID"""
        channel_data = {
            "id": "invalid_channel_id",
            "title": "Test Channel",
            "published_at": "2020-01-01T00:00:00Z"
        }
        
        is_valid, validated_data = validator.validate_channel_data(channel_data)
        
        assert is_valid == False
        assert len(validator.validation_errors) > 0
    
    def test_validate_channel_data_missing_required(self, validator):
        """Test validation of channel data with missing required fields"""
        channel_data = {
            "id": "UCtestchannel12345678901234"
            # Missing title and published_at
        }
        
        is_valid, validated_data = validator.validate_channel_data(channel_data)
        
        assert is_valid == False
        assert any("Missing required field" in error for error in validator.validation_errors)

if __name__ == "__main__":
    pytest.main([__file__, "-v"])