import smtplib
from email.mime.text import MimeText
from email.mime.multipart import MimeMultipart
import requests
import json
from typing import Dict, List
from ..utils.logger import setup_logger

class AlertManager:
    """Manage alerts and notifications for pipeline issues"""
    
    def __init__(self, config: Dict):
        self.config = config
        self.logger = setup_logger(__name__)
        self.alert_config = config.get('monitoring', {}).get('alerts', {})
    
    def send_alert(self, alert_type: str, message: str, severity: str = 'WARNING') -> bool:
        """Send alert through configured channels"""
        channels = self.alert_config.get(alert_type, {}).get('channels', [])
        
        if not channels:
            self.logger.warning(f"No alert channels configured for {alert_type}")
            return False
        
        success_count = 0
        for channel in channels:
            try:
                if channel == 'email':
                    self._send_email_alert(alert_type, message, severity)
                elif channel == 'slack':
                    self._send_slack_alert(alert_type, message, severity)
                elif channel == 'log':
                    self._send_log_alert(alert_type, message, severity)
                
                success_count += 1
                self.logger.info(f"Alert sent via {channel}: {message}")
                
            except Exception as e:
                self.logger.error(f"Failed to send alert via {channel}: {e}")
        
        return success_count > 0
    
    def _send_email_alert(self, alert_type: str, message: str, severity: str):
        """Send email alert"""
        email_config = self.alert_config.get('email', {})
        
        if not email_config.get('enabled', False):
            return
        
        # Email configuration
        smtp_server = email_config.get('smtp_server')
        smtp_port = email_config.get('smtp_port', 587)
        username = email_config.get('username')
        password = email_config.get('password')
        from_addr = email_config.get('from_address')
        to_addrs = email_config.get('to_addresses', [])
        
        if not all([smtp_server, username, password, from_addr, to_addrs]):
            self.logger.warning("Email alert configuration incomplete")
            return
        
        # Create email message
        msg = MimeMultipart()
        msg['From'] = from_addr
        msg['To'] = ', '.join(to_addrs)
        msg['Subject'] = f"[{severity}] YouTube Pipeline Alert: {alert_type}"
        
        body = f"""
        YouTube Data Pipeline Alert
        
        Type: {alert_type}
        Severity: {severity}
        Message: {message}
        
        Timestamp: {datetime.now().isoformat()}
        
        Please check the pipeline logs for more details.
        """
        
        msg.attach(MimeText(body, 'plain'))
        
        # Send email
        with smtplib.SMTP(smtp_server, smtp_port) as server:
            server.starttls()
            server.login(username, password)
            server.send_message(msg)
    
    def _send_slack_alert(self, alert_type: str, message: str, severity: str):
        """Send Slack alert"""
        slack_config = self.alert_config.get('slack', {})
        
        if not slack_config.get('enabled', False):
            return
        
        webhook_url = slack_config.get('webhook_url')
        if not webhook_url:
            self.logger.warning("Slack webhook URL not configured")
            return
        
        # Determine color based on severity
        color_map = {
            'CRITICAL': '#FF0000',  # Red
            'ERROR': '#FF0000',     # Red
            'WARNING': '#FFA500',   # Orange
            'INFO': '#00FF00'       # Green
        }
        
        color = color_map.get(severity, '#808080')  # Default to gray
        
        # Create Slack message
        slack_message = {
            'attachments': [
                {
                    'color': color,
                    'title': f'YouTube Pipeline Alert: {alert_type}',
                    'text': message,
                    'fields': [
                        {
                            'title': 'Severity',
                            'value': severity,
                            'short': True
                        },
                        {
                            'title': 'Timestamp',
                            'value': datetime.now().isoformat(),
                            'short': True
                        }
                    ]
                }
            ]
        }
        
        # Send to Slack
        response = requests.post(
            webhook_url,
            data=json.dumps(slack_message),
            headers={'Content-Type': 'application/json'}
        )
        
        if response.status_code != 200:
            raise Exception(f"Slack API returned {response.status_code}: {response.text}")
    
    def _send_log_alert(self, alert_type: str, message: str, severity: str):
        """Log alert to application logs"""
        log_method = {
            'CRITICAL': self.logger.critical,
            'ERROR': self.logger.error,
            'WARNING': self.logger.warning,
            'INFO': self.logger.info
        }.get(severity, self.logger.info)
        
        log_method(f"ALERT [{alert_type}]: {message}")
    
    def check_and_alert(self, monitor, db_manager):
        """Check various conditions and send alerts if needed"""
        alerts_sent = []
        
        # Check pipeline health
        health_status = monitor.get_health_status()
        if health_status['status'] == 'UNHEALTHY':
            message = f"Pipeline health is UNHEALTHY. Success rate: {health_status['success_rate']}%"
            if self.send_alert('pipeline_health', message, 'CRITICAL'):
                alerts_sent.append('pipeline_health')
        
        # Check data freshness
        freshness = monitor.check_data_freshness(db_manager)
        if freshness['status'] in ['STALE', 'VERY_STALE']:
            message = f"Data is {freshness['status']}. Last update: {freshness['hours_since_update']} hours ago"
            if self.send_alert('data_freshness', message, 'WARNING'):
                alerts_sent.append('data_freshness')
        
        # Check system resources
        system_health = health_status['system_health']
        if system_health['status'] == 'CRITICAL':
            message = f"System resources critical. CPU: {system_health['cpu_usage_percent']}%, Memory: {system_health['memory_usage_percent']}%"
            if self.send_alert('system_resources', message, 'CRITICAL'):
                alerts_sent.append('system_resources')
        
        return alerts_sent