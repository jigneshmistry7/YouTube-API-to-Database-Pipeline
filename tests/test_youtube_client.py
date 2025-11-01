import pytest
import responses
import os
from unittest.mock import Mock, patch
import sys

# Add the project root to Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from youtube_pipeline.extract.youtube_client import YouTubeClient

class TestYouTubeClient:
    """Test YouTube API client functionality"""
    
    @pytest.fixture
    def mock_config(self):
        """Mock configuration"""
        return {
            'extraction': {
                'timeout': 30,
                'batch_size': 50
            },
            'api_keys': ['test_api_key']
        }
    
    @pytest.fixture
    def youtube_client(self, mock_config):
        """Create YouTube client instance"""
        return YouTubeClient(mock_config)
    
    @responses.activate
    def test_get_channel_data_success(self, youtube_client):
        """Test successful channel data retrieval"""
        # Mock API response
        mock_response = {
            "items": [{
                "id": "UC_test_channel",
                "snippet": {
                    "title": "Test Channel",
                    "description": "Test Description",
                    "publishedAt": "2020-01-01T00:00:00Z",
                    "country": "US",
                    "customUrl": "@testchannel"
                },
                "statistics": {
                    "viewCount": "1000000",
                    "subscriberCount": "50000",
                    "videoCount": "200"
                }
            }]
        }
        
        responses.add(
            responses.GET,
            "https://www.googleapis.com/youtube/v3/channels",
            json=mock_response,
            status=200
        )
        
        # Test method
        result = youtube_client.get_channel_data("UC_test_channel")
        
        # Assertions
        assert result is not None
        assert result['id'] == "UC_test_channel"
        assert result['title'] == "Test Channel"
        assert result['view_count'] == 1000000
        assert result['subscriber_count'] == 50000
    
    @responses.activate
    def test_get_channel_data_not_found(self, youtube_client):
        """Test channel data retrieval when channel not found"""
        # Mock empty response
        mock_response = {"items": []}
        
        responses.add(
            responses.GET,
            "https://www.googleapis.com/youtube/v3/channels",
            json=mock_response,
            status=200
        )
        
        result = youtube_client.get_channel_data("UC_nonexistent")
        
        assert result is None
    
    @responses.activate
    def test_get_channel_data_api_error(self, youtube_client):
        """Test channel data retrieval with API error"""
        responses.add(
            responses.GET,
            "https://www.googleapis.com/youtube/v3/channels",
            json={"error": "API Error"},
            status=400
        )
        
        result = youtube_client.get_channel_data("UC_test_channel")
        
        assert result is None
    
    def test_parse_channel_data(self, youtube_client):
        """Test channel data parsing"""
        raw_data = {
            "id": "UC_test",
            "snippet": {
                "title": "Test Channel",
                "description": "Test Description",
                "publishedAt": "2020-01-01T00:00:00Z",
                "country": "US",
                "customUrl": "@testchannel",
                "thumbnails": {"default": {"url": "http://test.com/thumb.jpg"}}
            },
            "statistics": {
                "viewCount": "1000000",
                "subscriberCount": "50000",
                "videoCount": "200"
            }
        }
        
        parsed_data = youtube_client._parse_channel_data(raw_data)
        
        assert parsed_data['id'] == "UC_test"
        assert parsed_data['title'] == "Test Channel"
        assert parsed_data['view_count'] == 1000000
        assert parsed_data['subscriber_count'] == 50000
        assert parsed_data['video_count'] == 200
        assert 'fetched_at' in parsed_data
    
    def test_parse_channel_data_missing_fields(self, youtube_client):
        """Test channel data parsing with missing fields"""
        raw_data = {
            "id": "UC_test",
            "snippet": {
                "title": "Test Channel"
                # Missing other fields
            },
            "statistics": {
                # Missing statistics
            }
        }
        
        parsed_data = youtube_client._parse_channel_data(raw_data)
        
        assert parsed_data['id'] == "UC_test"
        assert parsed_data['title'] == "Test Channel"
        assert parsed_data['view_count'] == 0
        assert parsed_data['subscriber_count'] == 0
        assert parsed_data['video_count'] == 0

if __name__ == "__main__":
    pytest.main([__file__, "-v"])