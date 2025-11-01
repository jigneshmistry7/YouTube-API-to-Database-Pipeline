Monitoring API
PipelineMonitor
get_health_status()
Returns pipeline health information.

Example Response:
{
  "status": "HEALTHY",
  "success_rate": 95.5,
  "total_runs": 100,
  "successful_runs": 95,
  "failed_runs": 5,
  "average_processing_time": 120.5
}

check_data_freshness(db_manager)
Checks if data is up-to-date.

get_performance_metrics(hours=24)
Gets performance metrics.

AlertManager
send_alert(alert_type, message, severity)
Sends alert through configured channels.

check_and_alert(monitor, db_manager)
Checks conditions and sends alerts.

Utility Functions
ConfigLoader
load_yaml_config(file_path)
Loads YAML configuration file.

load_json_config(file_path)
Loads JSON configuration file.

load_environment_variables(prefix)
Loads environment variables.

Helpers
retry_on_exception(max_retries, delay, exceptions)
Decorator for retrying functions.

chunk_list(lst, chunk_size)
Splits list into chunks.

format_number(number)
Formats large numbers for display.

Error Handling
Common Exceptions
YouTubeAPIError: YouTube API related errors

DatabaseConnectionError: Database connection issues

DataValidationError: Data validation failures

ConfigurationError: Configuration problems

Custom Exception Handling

from youtube_pipeline.exceptions import YouTubeAPIError

try:
    pipeline.run()
except YouTubeAPIError as e:
    print(f"YouTube API error: {e}")
except DatabaseConnectionError as e:
    print(f"Database error: {e}")


Extension Points
Custom Data Enrichers

from youtube_pipeline.transform.data_enricher import DataEnricher

class CustomEnricher(DataEnricher):
    def enrich_video_data(self, videos):
        # Custom enrichment logic
        return super().enrich_video_data(videos)

Custom Alert Channels

from youtube_pipeline.monitor.alert_manager import AlertManager

class CustomAlertManager(AlertManager):
    def _send_custom_alert(self, alert_type, message, severity):
        # Custom alert implementation
        pass


### `docs/troubleshooting.md`
```markdown
# YouTube Data Pipeline - Troubleshooting Guide

## Common Issues and Solutions

### 1. YouTube API Issues

#### ❌ "API Quota Exceeded" Error

**Symptoms:**
- `403 Forbidden` errors from YouTube API
- "quotaExceeded" in error messages
- Pipeline fails with quota-related errors

**Solutions:**

1. **Use Multiple API Keys:**
   ```yaml
   # In your .env file
   YOUTUBE_API_KEY=your_primary_key
   YOUTUBE_API_KEY_2=your_secondary_key
   YOUTUBE_API_KEY_3=your_tertiary_key

2. **Reduce Request Frequency:**
# In config/pipeline.yaml
extraction:
  batch_size: 25  # Reduce from 50
  requests_per_minute: 50  # Add rate limiting

3. **Enable Caching:**
extraction:
  enable_cache: true
  cache_ttl: 7200  # 2 hours

4. **Monitor Quota Usage:**
from youtube_pipeline.extract.youtube_client import YouTubeClient
client = YouTubeClient(config)
quota_usage = client.rate_limiter.get_quota_usage()
print(f"Quota used: {quota_usage['percentage_used']}%")

