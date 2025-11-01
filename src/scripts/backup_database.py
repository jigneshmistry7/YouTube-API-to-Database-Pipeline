#!/usr/bin/env python3
"""
Database backup script for YouTube Data Pipeline
"""

import sys
import os
import argparse
import subprocess
from pathlib import Path
from datetime import datetime

# Add the project root to Python path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from youtube_pipeline.utils.config_loader import ConfigLoader
from youtube_pipeline.utils.logger import setup_logger

def backup_database(config_path: str, backup_dir: str = "backups") -> bool:
    """Create database backup using pg_dump"""
    logger = setup_logger("backup_database")
    
    try:
        # Load configuration
        config = ConfigLoader.load_yaml_config(config_path)
        db_config = config['storage']['database']
        
        # Create backup directory
        backup_path = Path(backup_dir)
        backup_path.mkdir(parents=True, exist_ok=True)
        
        # Generate backup filename
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_file = backup_path / f"youtube_analytics_backup_{timestamp}.sql"
        
        # Build pg_dump command
        pg_dump_cmd = [
            "pg_dump",
            "-h", db_config.get('host', 'localhost'),
            "-p", str(db_config.get('port', 5432)),
            "-U", db_config.get('username', 'postgres'),
            "-d", db_config.get('database', 'youtube_analytics'),
            "-f", str(backup_file),
            "--verbose"
        ]
        
        # Set password environment variable
        env = os.environ.copy()
        env['PGPASSWORD'] = db_config.get('password', '')
        
        logger.info(f"Creating database backup: {backup_file}")
        
        # Execute backup command
        result = subprocess.run(
            pg_dump_cmd,
            env=env,
            capture_output=True,
            text=True
        )
        
        if result.returncode == 0:
            # Compress backup file
            compressed_file = backup_file.with_suffix('.sql.gz')
            compress_cmd = ["gzip", str(backup_file)]
            subprocess.run(compress_cmd, check=True)
            
            file_size = compressed_file.stat().st_size / (1024 * 1024)  # MB
            logger.info(f"Backup completed successfully: {compressed_file} ({file_size:.2f} MB)")
            
            # Clean up old backups (keep last 10)
            cleanup_old_backups(backup_path, keep_count=10)
            
            return True
        else:
            logger.error(f"Backup failed: {result.stderr}")
            return False
            
    except Exception as e:
        logger.error(f"Backup process failed: {e}")
        return False

def cleanup_old_backups(backup_path: Path, keep_count: int = 10):
    """Clean up old backup files, keeping only the most recent ones"""
    backup_files = sorted(
        backup_path.glob("youtube_analytics_backup_*.sql.gz"),
        key=os.path.getmtime,
        reverse=True
    )
    
    for old_backup in backup_files[keep_count:]:
        try:
            old_backup.unlink()
            logger.info(f"Removed old backup: {old_backup}")
        except Exception as e:
            logger.warning(f"Failed to remove old backup {old_backup}: {e}")

def main():
    """Main function for database backup"""
    parser = argparse.ArgumentParser(description="Backup YouTube Data Pipeline Database")
    parser.add_argument(
        "--config", 
        default="config/pipeline.yaml",
        help="Path to pipeline configuration file"
    )
    parser.add_argument(
        "--backup-dir", 
        default="backups",
        help="Directory to store backups"
    )
    parser.add_argument(
        "--keep-count", 
        type=int,
        default=10,
        help="Number of backups to keep"
    )
    
    args = parser.parse_args()
    
    print("💾 YouTube Data Pipeline - Database Backup")
    print("=" * 50)
    print(f"📅 Backup time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"📁 Config: {args.config}")
    print(f"📂 Backup directory: {args.backup_dir}")
    print(f"💿 Keep count: {args.keep_count}")
    print("-" * 50)
    
    success = backup_database(args.config, args.backup_dir)
    
    if success:
        print("✅ Database backup completed successfully!")
    else:
        print("❌ Database backup failed!")
        sys.exit(1)

if __name__ == "__main__":
    main()