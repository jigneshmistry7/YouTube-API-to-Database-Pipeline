#!/usr/bin/env python3
"""
Main pipeline execution script
"""

import sys
import os
import argparse
import time
from pathlib import Path
from datetime import datetime

# Add the project root to Python path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from youtube_pipeline import YouTubeDataPipeline
from youtube_pipeline.utils.config_loader import ConfigLoader
from youtube_pipeline.utils.logger import setup_logger

def run_pipeline(config_path: str, channel_ids: list = None, full_refresh: bool = False) -> bool:
    """Run the YouTube data pipeline"""
    logger = setup_logger("run_pipeline")
    
    try:
        # Load configuration
        config = ConfigLoader.load_yaml_config(config_path)
        
        # Load channel configuration
        channels_config_path = "config/channels.json"
        if os.path.exists(channels_config_path):
            channels_config = ConfigLoader.load_json_config(channels_config_path)
            if not channel_ids:
                # Use all active channels from config
                channel_ids = [
                    channel['channel_id'] 
                    for channel in channels_config.get('channels', []) 
                    if channel.get('active', True)
                ]
        
        logger.info(f"Starting pipeline for {len(channel_ids) if channel_ids else 'all'} channels")
        
        # Initialize pipeline
        pipeline = YouTubeDataPipeline(config_path)
        
        # Run pipeline
        start_time = time.time()
        pipeline.run(channel_ids)
        processing_time = time.time() - start_time
        
        # Record success
        pipeline.monitor.record_success(processing_time)
        
        # Generate report
        health_status = pipeline.monitor.get_health_status()
        logger.info(f"Pipeline completed in {processing_time:.2f} seconds")
        logger.info(f"Success rate: {health_status['success_rate']}%")
        
        return True
        
    except Exception as e:
        logger.error(f"Pipeline execution failed: {e}")
        
        # Record failure
        if 'pipeline' in locals():
            pipeline.monitor.record_failure(str(e))
        
        return False

def main():
    """Main function for pipeline execution"""
    parser = argparse.ArgumentParser(description="Run YouTube Data Pipeline")
    parser.add_argument(
        "--config", 
        default="config/pipeline.yaml",
        help="Path to pipeline configuration file"
    )
    parser.add_argument(
        "--channels", 
        nargs="+",
        help="Specific channel IDs to process"
    )
    parser.add_argument(
        "--full-refresh", 
        action="store_true",
        help="Force full data refresh"
    )
    parser.add_argument(
        "--monitor", 
        action="store_true",
        help="Show monitoring dashboard after run"
    )
    
    args = parser.parse_args()
    
    print("🚀 YouTube Data Pipeline - Execution")
    print("=" * 50)
    print(f"📅 Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"📁 Config: {args.config}")
    print(f"📺 Channels: {args.channels if args.channels else 'All active channels'}")
    print(f"🔄 Full refresh: {args.full_refresh}")
    print("-" * 50)
    
    start_time = time.time()
    success = run_pipeline(args.config, args.channels, args.full_refresh)
    execution_time = time.time() - start_time
    
    print(f"⏱️  Execution time: {execution_time:.2f} seconds")
    
    if success:
        print("✅ Pipeline execution completed successfully!")
    else:
        print("❌ Pipeline execution failed!")
        sys.exit(1)
    
    if args.monitor:
        # Show monitoring dashboard
        from youtube_pipeline.monitor.pipeline_monitor import PipelineMonitor
        config = ConfigLoader.load_yaml_config(args.config)
        monitor = PipelineMonitor(config)
        health = monitor.get_health_status()
        
        print("\n📊 Pipeline Health Dashboard")
        print("=" * 30)
        print(f"Status: {health['status']}")
        print(f"Success Rate: {health['success_rate']}%")
        print(f"Total Runs: {health['total_runs']}")
        print(f"Last Run: {health['last_run']}")

if __name__ == "__main__":
    main()