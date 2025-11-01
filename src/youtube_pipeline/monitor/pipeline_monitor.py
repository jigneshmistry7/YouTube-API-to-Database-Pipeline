import time
import psutil
from datetime import datetime, timedelta
from typing import Dict, List, Optional
from ..utils.logger import setup_logger

class PipelineMonitor:
    """Monitor pipeline health, performance, and errors"""
    
    def __init__(self, config: Dict):
        self.config = config
        self.logger = setup_logger(__name__)
        self.metrics = {
            'run_count': 0,
            'success_count': 0,
            'failure_count': 0,
            'total_processing_time': 0,
            'last_run': None,
            'errors': []
        }
    
    def record_success(self, processing_time: float = None):
        """Record a successful pipeline run"""
        self.metrics['run_count'] += 1
        self.metrics['success_count'] += 1
        self.metrics['last_run'] = datetime.now()
        
        if processing_time:
            self.metrics['total_processing_time'] += processing_time
        
        self.logger.info(f"Pipeline run #{self.metrics['run_count']} completed successfully")
    
    def record_failure(self, error_message: str, processing_time: float = None):
        """Record a failed pipeline run"""
        self.metrics['run_count'] += 1
        self.metrics['failure_count'] += 1
        self.metrics['last_run'] = datetime.now()
        
        if processing_time:
            self.metrics['total_processing_time'] += processing_time
        
        error_record = {
            'timestamp': datetime.now(),
            'message': error_message,
            'run_number': self.metrics['run_count']
        }
        self.metrics['errors'].append(error_record)
        
        # Keep only last 100 errors
        if len(self.metrics['errors']) > 100:
            self.metrics['errors'] = self.metrics['errors'][-100:]
        
        self.logger.error(f"Pipeline run #{self.metrics['run_count']} failed: {error_message}")
    
    def get_health_status(self) -> Dict:
        """Get overall pipeline health status"""
        total_runs = self.metrics['run_count']
        success_rate = 0
        
        if total_runs > 0:
            success_rate = (self.metrics['success_count'] / total_runs) * 100
        
        avg_processing_time = 0
        if total_runs > 0:
            avg_processing_time = self.metrics['total_processing_time'] / total_runs
        
        # System health checks
        system_health = self._check_system_health()
        
        # Determine overall status
        if success_rate >= 95 and system_health['status'] == 'HEALTHY':
            status = 'HEALTHY'
        elif success_rate >= 80:
            status = 'DEGRADED'
        else:
            status = 'UNHEALTHY'
        
        return {
            'status': status,
            'success_rate': round(success_rate, 2),
            'total_runs': total_runs,
            'successful_runs': self.metrics['success_count'],
            'failed_runs': self.metrics['failure_count'],
            'average_processing_time': round(avg_processing_time, 2),
            'last_run': self.metrics['last_run'],
            'system_health': system_health,
            'recent_errors': self.metrics['errors'][-5:]  # Last 5 errors
        }
    
    def _check_system_health(self) -> Dict:
        """Check system resource health"""
        try:
            # CPU usage
            cpu_percent = psutil.cpu_percent(interval=1)
            
            # Memory usage
            memory = psutil.virtual_memory()
            memory_percent = memory.percent
            
            # Disk usage
            disk = psutil.disk_usage('/')
            disk_percent = disk.percent
            
            # Determine system status
            if cpu_percent < 80 and memory_percent < 80 and disk_percent < 90:
                system_status = 'HEALTHY'
            elif cpu_percent < 90 and memory_percent < 90 and disk_percent < 95:
                system_status = 'WARNING'
            else:
                system_status = 'CRITICAL'
            
            return {
                'status': system_status,
                'cpu_usage_percent': round(cpu_percent, 2),
                'memory_usage_percent': round(memory_percent, 2),
                'disk_usage_percent': round(disk_percent, 2),
                'memory_available_gb': round(memory.available / (1024**3), 2),
                'disk_free_gb': round(disk.free / (1024**3), 2)
            }
            
        except Exception as e:
            self.logger.error(f"Error checking system health: {e}")
            return {
                'status': 'UNKNOWN',
                'error': str(e)
            }
    
    def get_performance_metrics(self, hours: int = 24) -> Dict:
        """Get performance metrics for the specified time period"""
        # This would typically query a metrics database
        # For now, return simulated data
        current_time = datetime.now()
        time_points = []
        metrics_data = []
        
        for i in range(hours):
            point_time = current_time - timedelta(hours=i)
            time_points.append(point_time)
            
            # Simulate metrics (replace with actual data collection)
            metrics_data.append({
                'timestamp': point_time,
                'processing_time': 120 + (i % 10) * 5,  # Varying processing time
                'data_volume': 1000 + (i % 20) * 50,    # Varying data volume
                'success': i % 20 != 0  # Simulate occasional failures
            })
        
        return {
            'time_period_hours': hours,
            'metrics': metrics_data
        }
    
    def check_data_freshness(self, db_manager) -> Dict:
        """Check if data is up-to-date"""
        try:
            # Get the latest update timestamp from database
            freshness_query = """
            SELECT 
                MAX(updated_at) as latest_update,
                COUNT(*) as total_records
            FROM fact_video_stats
            """
            
            result = db_manager.execute_query(freshness_query)
            if not result:
                return {'status': 'UNKNOWN', 'error': 'No data available'}
            
            latest_update = result[0]['latest_update']
            total_records = result[0]['total_records']
            
            if not latest_update:
                return {'status': 'STALE', 'message': 'No data updates found'}
            
            current_time = datetime.now()
            time_diff = current_time - latest_update
            hours_diff = time_diff.total_seconds() / 3600
            
            if hours_diff < 1:
                status = 'FRESH'
            elif hours_diff < 6:
                status = 'SLIGHTLY_STALE'
            elif hours_diff < 24:
                status = 'STALE'
            else:
                status = 'VERY_STALE'
            
            return {
                'status': status,
                'latest_update': latest_update,
                'hours_since_update': round(hours_diff, 2),
                'total_records': total_records
            }
            
        except Exception as e:
            self.logger.error(f"Error checking data freshness: {e}")
            return {'status': 'UNKNOWN', 'error': str(e)}