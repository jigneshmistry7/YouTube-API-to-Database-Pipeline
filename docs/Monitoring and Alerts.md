Set Up Alerts for Common Issues

monitoring:
  alerts:
    api_quota:
      threshold: 80%
      channels: ["email", "slack"]
    data_freshness:
      threshold: "2 hours"
      channels: ["email"]
    system_resources:
      threshold: "90%"
      channels: ["slack"]


Check Pipeline Health

from youtube_pipeline.monitor.pipeline_monitor import PipelineMonitor
monitor = PipelineMonitor(config)
health = monitor.get_health_status()

if health['status'] != 'HEALTHY':
    print(f"Issues detected: {health['recent_errors']}")