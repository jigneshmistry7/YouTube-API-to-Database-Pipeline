Collect Debug Information
When reporting issues, include:

Configuration:

Redacted version of your .env file

config/pipeline.yaml contents

Error Logs:
tail -n 100 logs/pipeline.log

System Information:

Python version: python --version

PostgreSQL version: psql --version

Operating system

Reproduction Steps:

Exact command that caused the issue

Channel IDs being processed

Time of occurrence

Community Support
Check existing GitHub Issues

Create a new issue with the debug information

Join the discussion forum

Prevention Best Practices
Regular Maintenance
Monitor API Quota: Set up alerts for high usage

Database Backups: Schedule regular backups

Log Rotation: Configure log rotation to prevent disk space issues

Update Dependencies: Regularly update Python packages

Monitoring Setup
Health Checks: Implement comprehensive health monitoring

Performance Metrics: Track pipeline performance over time

Alerting: Configure alerts for critical issues

Dashboard: Set up monitoring dashboard

Configuration Management
Version Control: Keep configuration files in version control

Environment Separation: Use different configurations for dev/test/prod

Sensitive Data: Never commit API keys or passwords

Documentation: Keep configuration documentation updated


## 13. Main Application Files

### `main.py`
```python
#!/usr/bin/env python3
"""
YouTube Data Pipeline - Main Entry Point
A comprehensive ETL pipeline for YouTube data analytics
"""

import argparse
import sys
import os
from pathlib import Path

# Add the src directory to Python path
sys.path.insert(0, str(Path(__file__).parent / "src"))

from youtube_pipeline import YouTubeDataPipeline
from youtube_pipeline.utils.config_loader import ConfigLoader
from youtube_pipeline.utils.logger import setup_logger

def main():
    """Main entry point for the YouTube Data Pipeline"""
    parser = argparse.ArgumentParser(
        description="YouTube Data Pipeline - Extract, transform, and load YouTube data for analytics",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run with default configuration
  python main.py

  # Run for specific channels
  python main.py --channels UC_x5XG1OV2P6uZZ5FSM9Ttw UCBJycsmduvYEL83R_U4JriQ

  # Run with custom config and monitoring
  python main.py --config config/production.yaml --monitor

  # Run in debug mode
  python main.py --debug --log-level DEBUG
        """
    )
    
    parser.add_argument(
        "--config",
        default="config/pipeline.yaml",
        help="Path to pipeline configuration file (default: config/pipeline.yaml)"
    )
    
    parser.add_argument(
        "--channels",
        nargs="+",
        help="Specific channel IDs to process (default: all active channels from config)"
    )
    
    parser.add_argument(
        "--full-refresh",
        action="store_true",
        help="Force full data refresh instead of incremental update"
    )
    
    parser.add_argument(
        "--monitor",
        action="store_true",
        help="Show monitoring dashboard after pipeline execution"
    )
    
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Enable debug mode with verbose logging"
    )
    
    parser.add_argument(
        "--log-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        default="INFO",
        help="Set logging level (default: INFO)"
    )
    
    parser.add_argument(
        "--setup-db",
        action="store_true",
        help="Setup database schema before running pipeline"
    )
    
    parser.add_argument(
        "--version",
        action="version",
        version="YouTube Data Pipeline 1.0.0"
    )
    
    args = parser.parse_args()
    
    # Set up logging
    log_level = "DEBUG" if args.debug else args.log_level
    logger = setup_logger("main", log_level, "logs/pipeline.log")
    
    print("🚀 YouTube Data Pipeline")
    print("=" * 50)
    
    try:
        # Validate configuration file exists
        if not os.path.exists(args.config):
            print(f"❌ Configuration file not found: {args.config}")
            sys.exit(1)
        
        # Load configuration
        config = ConfigLoader.load_yaml_config(args.config)
        
        # Setup database if requested
        if args.setup_db:
            from scripts.setup_database import setup_database
            print("🗄️  Setting up database...")
            if not setup_database(args.config):
                print("❌ Database setup failed!")
                sys.exit(1)
            print("✅ Database setup completed")
        
        # Initialize pipeline
        print("🔧 Initializing pipeline...")
        pipeline = YouTubeDataPipeline(config)
        
        # Load channel configuration
        channel_ids = args.channels
        if not channel_ids:
            channels_config_path = "config/channels.json"
            if os.path.exists(channels_config_path):
                channels_config = ConfigLoader.load_json_config(channels_config_path)
                channel_ids = [
                    channel['channel_id'] 
                    for channel in channels_config.get('channels', []) 
                    if channel.get('active', True)
                ]
                print(f"📺 Loaded {len(channel_ids)} active channels from config")
        
        if channel_ids:
            print(f"🎯 Processing {len(channel_ids)} channels:")
            for i, channel_id in enumerate(channel_ids[:5], 1):  # Show first 5
                print(f"   {i}. {channel_id}")
            if len(channel_ids) > 5:
                print(f"   ... and {len(channel_ids) - 5} more")
        else:
            print("⚠️  No channels specified for processing")
        
        print("⏳ Starting pipeline execution...")
        print("-" * 50)
        
        # Run pipeline
        success = pipeline.run(channel_ids)
        
        print("-" * 50)
        if success:
            print("✅ Pipeline execution completed successfully!")
            
            # Show database statistics
            stats = pipeline.db_manager.get_database_stats()
            print(f"📊 Database Statistics:")
            print(f"   📺 Channels: {stats.get('total_channels', 0)}")
            print(f"   🎬 Videos: {stats.get('total_videos', 0)}")
            print(f"   📈 Stats Records: {stats.get('total_stats_records', 0)}")
            
            # Show pipeline health
            health = pipeline.monitor.get_health_status()
            print(f"❤️  Pipeline Health: {health['status']}")
            print(f"   Success Rate: {health['success_rate']}%")
            print(f"   Total Runs: {health['total_runs']}")
            
        else:
            print("❌ Pipeline execution failed!")
            sys.exit(1)
        
        # Show monitoring dashboard if requested
        if args.monitor:
            print("\n📊 Monitoring Dashboard")
            print("=" * 30)
            
            health = pipeline.monitor.get_health_status()
            print(f"Overall Status: {health['status']}")
            print(f"Success Rate: {health['success_rate']}%")
            print(f"Successful Runs: {health['successful_runs']}")
            print(f"Failed Runs: {health['failure_count']}")
            print(f"Average Processing Time: {health['average_processing_time']:.2f}s")
            
            # System health
            system_health = health['system_health']
            print(f"\nSystem Resources:")
            print(f"  CPU Usage: {system_health['cpu_usage_percent']}%")
            print(f"  Memory Usage: {system_health['memory_usage_percent']}%")
            print(f"  Disk Usage: {system_health['disk_usage_percent']}%")
            
            # Data freshness
            freshness = pipeline.monitor.check_data_freshness(pipeline.db_manager)
            print(f"\nData Freshness: {freshness['status']}")
            print(f"  Last Update: {freshness['hours_since_update']:.1f} hours ago")
            print(f"  Total Records: {freshness['total_records']}")
            
            # Recent errors
            if health['recent_errors']:
                print(f"\nRecent Errors ({len(health['recent_errors'])}):")
                for error in health['recent_errors'][-3:]:  # Last 3 errors
                    print(f"  - {error['timestamp']}: {error['message']}")
        
    except KeyboardInterrupt:
        print("\n⏹️  Pipeline execution interrupted by user")
        sys.exit(1)
        
    except Exception as e:
        print(f"💥 Unexpected error: {e}")
        if args.debug:
            import traceback
            traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()