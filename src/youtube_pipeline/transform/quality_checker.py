import pandas as pd
from typing import Dict, List, Tuple
from datetime import datetime
from ..utils.logger import setup_logger

class QualityChecker:
    """Perform data quality checks and generate quality reports"""
    
    def __init__(self, config: Dict):
        self.config = config
        self.logger = setup_logger(__name__)
        self.quality_issues = []
    
    def check_data_quality(self, data: Dict) -> Dict:
        """Perform comprehensive data quality checks"""
        quality_report = {
            'timestamp': datetime.now().isoformat(),
            'summary': {},
            'details': {},
            'issues': []
        }
        
        try:
            # Check channel data quality
            if 'channels' in data:
                channel_quality = self._check_channel_quality(data['channels'])
                quality_report['details']['channels'] = channel_quality
                quality_report['issues'].extend(channel_quality.get('issues', []))
            
            # Check video data quality
            if 'videos' in data:
                video_quality = self._check_video_quality(data['videos'])
                quality_report['details']['videos'] = video_quality
                quality_report['issues'].extend(video_quality.get('issues', []))
            
            # Check video stats quality
            if 'video_stats' in data:
                stats_quality = self._check_stats_quality(data['video_stats'])
                quality_report['details']['video_stats'] = stats_quality
                quality_report['issues'].extend(stats_quality.get('issues', []))
            
            # Generate summary
            quality_report['summary'] = self._generate_quality_summary(quality_report)
            
            self.logger.info(f"Quality check completed: {len(quality_report['issues'])} issues found")
            
        except Exception as e:
            self.logger.error(f"Error during quality check: {e}")
            quality_report['error'] = str(e)
        
        return quality_report
    
    def _check_channel_quality(self, channels: List[Dict]) -> Dict:
        """Check quality of channel data"""
        if not channels:
            return {'status': 'EMPTY', 'issues': ['No channel data available']}
        
        issues = []
        df = pd.DataFrame(channels)
        
        # Completeness checks
        missing_ids = df['id'].isna().sum()
        if missing_ids > 0:
            issues.append(f"{missing_ids} channels missing ID")
        
        missing_titles = df['title'].isna().sum()
        if missing_titles > 0:
            issues.append(f"{missing_titles} channels missing title")
        
        # Validity checks
        zero_subscribers = (df['subscriber_count'] == 0).sum()
        if zero_subscribers > len(channels) * 0.5:  # If more than 50% have 0 subscribers
            issues.append(f"Suspicious: {zero_subscribers} channels have 0 subscribers")
        
        # Consistency checks
        duplicate_ids = df['id'].duplicated().sum()
        if duplicate_ids > 0:
            issues.append(f"{duplicate_ids} duplicate channel IDs")
        
        return {
            'total_channels': len(channels),
            'completeness_score': self._calculate_completeness(df, ['id', 'title', 'subscriber_count']),
            'validity_score': self._calculate_validity_score(df),
            'issues': issues,
            'status': 'PASS' if len(issues) == 0 else 'WARNING'
        }
    
    def _check_video_quality(self, videos: List[Dict]) -> Dict:
        """Check quality of video data"""
        if not videos:
            return {'status': 'EMPTY', 'issues': ['No video data available']}
        
        issues = []
        df = pd.DataFrame(videos)
        
        # Completeness checks
        required_fields = ['id', 'title', 'channel_id']
        for field in required_fields:
            missing_count = df[field].isna().sum()
            if missing_count > 0:
                issues.append(f"{missing_count} videos missing {field}")
        
        # Validity checks
        zero_views = (df['view_count'] == 0).sum()
        if zero_views > len(videos) * 0.3:  # If more than 30% have 0 views
            issues.append(f"Suspicious: {zero_views} videos have 0 views")
        
        # Uniqueness checks
        duplicate_videos = df['id'].duplicated().sum()
        if duplicate_videos > 0:
            issues.append(f"{duplicate_videos} duplicate video IDs")
        
        return {
            'total_videos': len(videos),
            'completeness_score': self._calculate_completeness(df, required_fields),
            'validity_score': self._calculate_validity_score(df),
            'issues': issues,
            'status': 'PASS' if len(issues) == 0 else 'WARNING'
        }
    
    def _check_stats_quality(self, stats: List[Dict]) -> Dict:
        """Check quality of video statistics"""
        if not stats:
            return {'status': 'EMPTY', 'issues': ['No video statistics available']}
        
        issues = []
        df = pd.DataFrame(stats)
        
        # Data freshness check
        if 'fetched_at' in df.columns:
            current_time = datetime.now().timestamp()
            time_threshold = 3600  # 1 hour
            stale_data = (current_time - df['fetched_at']) > time_threshold
            if stale_data.any():
                issues.append("Some data is stale (older than 1 hour)")
        
        # Statistical outliers
        numeric_columns = ['view_count', 'like_count', 'comment_count']
        for column in numeric_columns:
            if column in df.columns:
                outliers = self._detect_outliers(df[column])
                if len(outliers) > 0:
                    issues.append(f"Potential outliers detected in {column}")
        
        return {
            'total_stats': len(stats),
            'data_freshness': self._calculate_freshness_score(df),
            'outlier_count': len(outliers) if 'outliers' in locals() else 0,
            'issues': issues,
            'status': 'PASS' if len(issues) == 0 else 'WARNING'
        }
    
    def _calculate_completeness(self, df: pd.DataFrame, columns: List[str]) -> float:
        """Calculate data completeness score (0-100)"""
        if df.empty:
            return 0.0
        
        total_cells = len(df) * len(columns)
        if total_cells == 0:
            return 100.0
        
        missing_cells = sum(df[col].isna().sum() for col in columns)
        completeness = ((total_cells - missing_cells) / total_cells) * 100
        return round(completeness, 2)
    
    def _calculate_validity_score(self, df: pd.DataFrame) -> float:
        """Calculate data validity score (0-100)"""
        if df.empty:
            return 0.0
        
        # Count valid rows (no nulls in critical columns)
        critical_columns = [col for col in ['id', 'title'] if col in df.columns]
        if not critical_columns:
            return 100.0
        
        valid_rows = df[critical_columns].notna().all(axis=1).sum()
        validity = (valid_rows / len(df)) * 100
        return round(validity, 2)
    
    def _calculate_freshness_score(self, df: pd.DataFrame) -> float:
        """Calculate data freshness score (0-100)"""
        if 'fetched_at' not in df.columns or df.empty:
            return 0.0
        
        current_time = datetime.now().timestamp()
        time_diffs = current_time - df['fetched_at']
        
        # Score based on how recent the data is (within 1 hour = 100%)
        freshness_scores = [max(0, 100 - (diff / 3600) * 100) for diff in time_diffs]
        avg_freshness = sum(freshness_scores) / len(freshness_scores)
        return round(avg_freshness, 2)
    
    def _detect_outliers(self, series: pd.Series) -> pd.Series:
        """Detect outliers using IQR method"""
        Q1 = series.quantile(0.25)
        Q3 = series.quantile(0.75)
        IQR = Q3 - Q1
        lower_bound = Q1 - 1.5 * IQR
        upper_bound = Q3 + 1.5 * IQR
        return series[(series < lower_bound) | (series > upper_bound)]
    
    def _generate_quality_summary(self, quality_report: Dict) -> Dict:
        """Generate quality summary from detailed report"""
        total_issues = len(quality_report.get('issues', []))
        
        # Calculate overall score (0-100)
        completeness_scores = []
        validity_scores = []
        
        for category, details in quality_report.get('details', {}).items():
            if 'completeness_score' in details:
                completeness_scores.append(details['completeness_score'])
            if 'validity_score' in details:
                validity_scores.append(details['validity_score'])
        
        avg_completeness = sum(completeness_scores) / len(completeness_scores) if completeness_scores else 0
        avg_validity = sum(validity_scores) / len(validity_scores) if validity_scores else 0
        
        # Penalize for issues
        issue_penalty = min(total_issues * 5, 30)  # Max 30% penalty
        overall_score = (avg_completeness + avg_validity) / 2 - issue_penalty
        overall_score = max(0, min(100, overall_score))
        
        return {
            'overall_score': round(overall_score, 2),
            'total_issues': total_issues,
            'data_categories': list(quality_report.get('details', {}).keys()),
            'status': 'EXCELLENT' if overall_score >= 90 else
                     'GOOD' if overall_score >= 75 else
                     'FAIR' if overall_score >= 60 else 'POOR'
        }