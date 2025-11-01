#!/usr/bin/env python3
"""
Scheduled pipeline execution example
"""

import sys
import os
import time
import schedule
from datetime import datetime, timedelta
from pathlib import Path

# Add the project root to Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.youtube_pipeline import YouTubeDataPipeline
from src.youtube_pipeline.utils.config_loader import ConfigLoader
from src.youtube_pipeline.utils.logger import setup_logger

class ScheduledPipeline:
    """Scheduled pipeline execution manager"""
    
    def __init__(self, config_path: str):
        self.config_path = config_path
        self.config = ConfigLoader.load_yaml_config(config_path)
        self.pipeline = YouTubeDataPipeline(self.config)
        self.logger = setup_logger("scheduled_pipeline")
        self.is_running = False
    
    def run_daily_pipeline(self):
        """Run pipeline on daily schedule"""
        if self.is_running:
            self.logger.warning("Pipeline is already running, skipping...")
            return
        
        self.is_running = True
        start_time = time.time()
        
        try:
            self.logger.info("Starting scheduled daily pipeline run")
            
            # Run the pipeline
            self.pipeline.run()
            
            # Record success
            processing_time = time.time() - start_time
            self.pipeline.monitor.record_success(processing_time)
            
            self.logger.info(f"Daily pipeline completed in {processing_time:.2f} seconds")
            
        except Exception as e:
            self.logger.error(f"Daily pipeline failed: {e}")
            self.pipeline.monitor.record_failure(str(e))
        
        finally:
            self.is_running = False
    
    def run_hourly_stats_update(self):
        """Run hourly statistics update"""
        if self.is_running:
            self.logger.warning("Pipeline is already running, skipping hourly update...")
            return
        
        self.is_running = True
        
        try:
            self.logger.info("Starting hourly statistics update")
            
            # For hourly updates, we might only update video statistics
            # rather than full channel data extraction
            # This is a simplified example - implement based on your needs
            
            self.logger.info("Hourly statistics update completed")
            
        except Exception as e:
            self.logger.error(f"Hourly update failed: {e}")
        
        finally:
            self.is_running = False
    
    def start_scheduler(self):
        """Start the scheduled pipeline"""
        self.logger.info("Starting pipeline scheduler")
        
        # Schedule daily full pipeline run at 2 AM
        schedule.every().day.at("02:00").do(self.run_daily_pipeline)
        
        # Schedule hourly statistics updates
        schedule.every().hour.do(self.run_hourly_stats_update)
        
        # Schedule health check every 6 hours
        schedule.every(6).hours.do(self.check_pipeline_health)
        
        self.logger.info("Scheduler started with the following jobs:")
        for job in schedule.get_jobs():
            self.logger.info(f"  - {job}")
        
        # Keep the scheduler running
        try:
            while True:
                schedule.run_pending()
                time.sleep(60)  # Check every minute
                
        except KeyboardInterrupt:
            self.logger.info("Scheduler stopped by user")
    
    def check_pipeline_health(self):
        """Check and report pipeline health"""
        try:
            health = self.pipeline.monitor.get_health_status()
            
            if health['status'] == 'UNHEALTHY':
                self.logger.warning(f"Pipeline health check: UNHEALTHY (Success rate: {health['success_rate']}%)")
            else:
                self.logger.info(f"Pipeline health check: {health['status']} (Success rate: {health['success_rate']}%)")
                
        except Exception as e:
            self.logger.error(f"Health check failed: {e}")

def main():
    """Main function for scheduled pipeline"""
    print("⏰ YouTube Data Pipeline - Scheduled Execution")
    print("=" * 50)
    print("This will run the pipeline on a schedule:")
    print("  - Daily full run at 2:00 AM")
    print("  - Hourly statistics updates")
    print("  - Health checks every 6 hours")
    print("\nPress Ctrl+C to stop the scheduler")
    print("-" * 50)
    
    # Initialize scheduled pipeline
    scheduler = ScheduledPipeline("config/pipeline.yaml")
    
    # Run one immediate execution
    print("🚀 Running initial pipeline execution...")
    scheduler.run_daily_pipeline()
    
    # Start scheduler
    print("⏰ Starting scheduler...")
    scheduler.start_scheduler()

if __name__ == "__main__":
    main()