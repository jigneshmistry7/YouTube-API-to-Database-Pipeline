Common Issues
Database Connection Failed

Check if PostgreSQL is running

Verify credentials in .env

Ensure database exists

YouTube API Quota Exceeded

Use multiple API keys

Reduce request frequency

Enable caching

Permission Errors

Check file permissions

Ensure database user has correct privileges

Getting Help
Check the Troubleshooting Guide

Open an Issue on GitHub

Review the API Reference


### `docs/api_reference.md`
```markdown
# YouTube Data Pipeline - API Reference

## Overview

This document provides detailed information about the YouTube Data Pipeline's classes, methods, and configuration options.

## Core Classes

### YouTubeDataPipeline

The main pipeline class that orchestrates the ETL process.

#### Initialization
```python
from youtube_pipeline import YouTubeDataPipeline

# With config file
pipeline = YouTubeDataPipeline("config/pipeline.yaml")

# With config dictionary
pipeline = YouTubeDataPipeline(config_dict)

Methods
run(channel_ids=None)
Runs the complete ETL pipeline.

channel_ids: List of specific channel IDs to process. If None, processes all active channels from config.

Returns: True if successful, False otherwise

Example:

# Run for all configured channels
pipeline.run()

# Run for specific channels
pipeline.run(["UC_x5XG1OV2P6uZZ5FSM9Ttw", "UCBJycsmduvYEL83R_U4JriQ"])

YouTubeClient
Handles interactions with YouTube Data API.

Methods
fetch_data(channel_ids=None)
Fetches data from YouTube APIs.

channel_ids: List of channel IDs to fetch

Returns: Dictionary with channel, video, and statistics data

get_channel_data(channel_id)
Gets detailed information for a specific channel.

get_channel_videos(channel_id, max_results=50)
Gets videos from a channel.

get_video_statistics(video_ids)
Gets statistics for multiple videos.

DatabaseManager
Manages database operations.

Methods
connect()
Establishes database connection.

load_data(processed_data)
Loads processed data into database.

execute_query(query, params=None)
Executes a read query.

get_database_stats()
Gets database statistics.

Configuration Reference
Pipeline Configuration (pipeline.yaml)
Extraction Section

extraction:
  batch_size: 50           # Number of items per API request
  max_retries: 3           # Maximum retry attempts
  retry_delay: 60          # Delay between retries (seconds)
  timeout: 30              # API request timeout
  enable_cache: true       # Enable response caching
  cache_ttl: 3600          # Cache time-to-live (seconds)

Processing Section

processing:
  enable_sentiment_analysis: false  # Analyze comment sentiment
  calculate_engagement_rate: true   # Calculate engagement metrics
  detect_anomalies: true            # Detect data anomalies
  clean_html_tags: true             # Remove HTML from text
  normalize_text: true              # Normalize text data

Storage Section

storage:
  database_type: "postgresql"       # Database type
  keep_raw_data: false              # Store raw API responses
  backup_frequency: "daily"         # Backup schedule
  retention_days: 365               # Data retention period

Monitoring Section

monitoring:
  log_level: "INFO"                 # Logging level
  enable_health_checks: true        # Enable health monitoring
  metrics_port: 9090                # Metrics endpoint port
  alert_channels: ["log"]           # Alert channels


Channel Configuration (channels.json)

{
  "channels": [
    {
      "channel_id": "UC_x5XG1OV2P6uZZ5FSM9Ttw",
      "channel_name": "Google Developers",
      "active": true,
      "update_frequency": 3600,
      "collect_comments": true,
      "max_videos": 100
    }
  ],
  "search_queries": [
    "python tutorial",
    "data science"
  ]
}

❌ "API Key Not Valid" Error
Solutions:

Verify API key in .env file

Ensure YouTube Data API v3 is enabled

Check if API key has restrictions

Regenerate API key if necessary

2. Database Issues
❌ "Database Connection Failed"
Symptoms:

psycopg2.OperationalError exceptions

"Connection refused" errors

Pipeline fails to start

Solutions:

Check Database Status:

# For local PostgreSQL
sudo systemctl status postgresql

# For Docker
docker ps | grep postgres

Verify Connection Settings:
# In .env file
DB_HOST=localhost
DB_PORT=5432
DB_NAME=youtube_analytics
DB_USER=youtube_user
DB_PASSWORD=your_password

Test Connection Manually:
psql -h localhost -U youtube_user -d youtube_analytics

Check Firewall Settings:
# Allow PostgreSQL port
sudo ufw allow 5432

❌ "Table Does Not Exist" Error
Solutions:

Run database setup:
python src/scripts/setup_database.py

Check schema:
from youtube_pipeline.load.schema_manager import SchemaManager
schema_manager = SchemaManager(config)
validation = schema_manager.validate_schema(connection)
print(validation['missing_tables'])

Data Quality Issues
❌ "Missing Data" or "Incomplete Records"
Symptoms:

Some channels/videos not being collected

Partial data in database

Null values in important fields

Solutions:

Enable Debug Logging:
monitoring:
  log_level: "DEBUG"


Check Data Validation:
from youtube_pipeline.transform.data_validator import DataValidator
validator = DataValidator(config)
report = validator.get_validation_report()
print(report['issues'])

Verify Channel IDs:
# Test individual channel
channel_data = youtube_client.get_channel_data("UC_CHANNEL_ID")
if not channel_data:
    print("Channel not found or inaccessible")

❌ "Data Freshness" Warnings
Solutions:

Increase update frequency:
extraction:
  update_frequency: 1800  # 30 minutes


Check pipeline scheduling

Verify API quota isn't exhausted

4. Performance Issues
❌ "Pipeline Running Slowly"
Symptoms:

Long execution times

High memory usage

Database timeouts

Solutions:

Optimize Batch Sizes:

extraction:
  batch_size: 25  # Reduce if memory issues

Enable Query Optimization:
# After database setup
schema_manager.create_indexes(connection)

Monitor System Resources:
from youtube_pipeline.monitor.pipeline_monitor import PipelineMonitor
monitor = PipelineMonitor(config)
system_health = monitor._check_system_health()
print(system_health)

Use Connection Pooling:
storage:
  database:
    pool_size: 20
    max_overflow: 10

Configuration Issues
❌ "Invalid Configuration" Errors
Solutions:

Validate YAML syntax:
python -c "import yaml; yaml.safe_load(open('config/pipeline.yaml'))"

Check required fields:
from youtube_pipeline.utils.config_loader import ConfigLoader
config = ConfigLoader.load_yaml_config('config/pipeline.yaml')
required_sections = ['extraction', 'processing', 'storage']
for section in required_sections:
    if section not in config:
        print(f"Missing section: {section}")

Permission Issues
❌ "Permission Denied" Errors
Solutions:

Check file permissions:
chmod 644 config/*.yaml config/*.json
chmod 755 src/scripts/*.py

Verify database user privileges:

GRANT ALL PRIVILEGES ON DATABASE youtube_analytics TO youtube_user;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO youtube_user;

