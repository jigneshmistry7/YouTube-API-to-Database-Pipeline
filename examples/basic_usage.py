#!/usr/bin/env python3
"""
Basic usage example for YouTube Data Pipeline
"""

import sys
import os
from pathlib import Path

# Add the project root to Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.youtube_pipeline import YouTubeDataPipeline
from src.youtube_pipeline.utils.config_loader import ConfigLoader

def basic_example():
    """Basic example of using the YouTube Data Pipeline"""
    print("🎬 YouTube Data Pipeline - Basic Usage Example")
    print("=" * 50)
    
    try:
        # Load configuration
        config = ConfigLoader.load_yaml_config("config/pipeline.yaml")
        
        # Initialize pipeline
        pipeline = YouTubeDataPipeline(config)
        
        # Define channels to monitor
        channel_ids = [
            "UC_x5XG1OV2P6uZZ5FSM9Ttw",  # Google Developers
            "UCBJycsmduvYEL83R_U4JriQ",  # Marques Brownlee
        ]
        
        print(f"📺 Monitoring {len(channel_ids)} channels")
        print("⏳ Running pipeline...")
        
        # Run pipeline
        pipeline.run(channel_ids)
        
        # Get database statistics
        stats = pipeline.db_manager.get_database_stats()
        
        print("✅ Pipeline completed successfully!")
        print("\n📊 Database Statistics:")
        print(f"   Channels: {stats.get('total_channels', 0)}")
        print(f"   Videos: {stats.get('total_videos', 0)}")
        print(f"   Statistics Records: {stats.get('total_stats_records', 0)}")
        
        # Get pipeline health
        health = pipeline.monitor.get_health_status()
        print(f"\n❤️  Pipeline Health: {health['status']}")
        print(f"   Success Rate: {health['success_rate']}%")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        return False
    
    return True

def advanced_example():
    """Advanced example with custom configuration"""
    print("\n🚀 YouTube Data Pipeline - Advanced Usage")
    print("=" * 50)
    
    try:
        # Custom configuration
        custom_config = {
            'extraction': {
                'batch_size': 25,
                'max_retries': 5,
                'timeout': 45
            },
            'processing': {
                'calculate_engagement_rate': True,
                'detect_anomalies': True
            },
            'monitoring': {
                'log_level': 'DEBUG',
                'enable_health_checks': True
            }
        }
        
        # Initialize with custom config
        pipeline = YouTubeDataPipeline(custom_config)
        
        # Run for specific channel
        channel_id = "UC_x5XG1OV2P6uZZ5FSM9Ttw"
        print(f"📺 Processing channel: {channel_id}")
        
        pipeline.run([channel_id])
        
        # Example query: Get top videos by engagement
        query = """
        SELECT 
            v.video_id,
            v.title,
            v.view_count,
            v.like_count,
            v.engagement_rate
        FROM dim_videos v
        WHERE v.channel_id = %s
        ORDER BY v.engagement_rate DESC
        LIMIT 5
        """
        
        results = pipeline.db_manager.execute_query(query, (channel_id,))
        
        print("\n🏆 Top 5 Videos by Engagement Rate:")
        for i, video in enumerate(results, 1):
            print(f"   {i}. {video['title'][:50]}...")
            print(f"      Views: {video['view_count']:,} | "
                  f"Likes: {video['like_count']:,} | "
                  f"Engagement: {video['engagement_rate']}%")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        return False
    
    return True

if __name__ == "__main__":
    print("YouTube Data Pipeline Examples")
    print("=" * 30)
    
    # Run basic example
    if basic_example():
        print("\n" + "=" * 50)
        
        # Run advanced example if basic succeeded
        advanced_example()
    
    print("\n🎉 Examples completed!")