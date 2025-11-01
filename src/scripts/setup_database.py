#!/usr/bin/env python3
"""
Database setup script for YouTube Data Pipeline
Creates database schema and initializes tables
"""

import sys
import os
import argparse
from pathlib import Path

# Add the project root to Python path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from youtube_pipeline.load.database_manager import DatabaseManager
from youtube_pipeline.load.schema_manager import SchemaManager
from youtube_pipeline.utils.config_loader import ConfigLoader
from youtube_pipeline.utils.logger import setup_logger

def setup_database(config_path: str, create_indexes: bool = True) -> bool:
    """Setup database schema and tables"""
    logger = setup_logger("setup_database")
    
    try:
        # Load configuration
        config = ConfigLoader.load_yaml_config(config_path)
        db_config = config['storage']['database']
        
        logger.info(f"Setting up database: {db_config.get('database')}")
        
        # Initialize database manager
        db_manager = DatabaseManager(config)
        
        if not db_manager.connect():
            logger.error("Failed to connect to database")
            return False
        
        # Create schema manager and setup tables
        schema_manager = SchemaManager(config)
        
        # Create tables
        if not schema_manager.create_tables(db_manager.connection):
            logger.error("Failed to create tables")
            return False
        
        # Create indexes if requested
        if create_indexes:
            if not schema_manager.create_indexes(db_manager.connection):
                logger.error("Failed to create indexes")
                return False
        
        # Validate schema
        validation_result = schema_manager.validate_schema(db_manager.connection)
        
        if validation_result['is_valid']:
            logger.info("Database schema validation passed")
            logger.info(f"Existing tables: {', '.join(validation_result['existing_tables'])}")
        else:
            logger.warning("Database schema validation failed")
            logger.warning(f"Missing tables: {', '.join(validation_result['missing_tables'])}")
        
        # Get database statistics
        stats = db_manager.get_database_stats()
        logger.info(f"Database setup complete. Stats: {stats}")
        
        db_manager.disconnect()
        return True
        
    except Exception as e:
        logger.error(f"Database setup failed: {e}")
        return False

def main():
    """Main function for database setup"""
    parser = argparse.ArgumentParser(description="Setup YouTube Data Pipeline Database")
    parser.add_argument(
        "--config", 
        default="config/pipeline.yaml",
        help="Path to pipeline configuration file"
    )
    parser.add_argument(
        "--no-indexes", 
        action="store_true",
        help="Skip creating indexes"
    )
    parser.add_argument(
        "--env", 
        choices=["development", "production", "testing"],
        default="development",
        help="Environment to setup"
    )
    
    args = parser.parse_args()
    
    print("🚀 YouTube Data Pipeline - Database Setup")
    print("=" * 50)
    
    if not os.path.exists(args.config):
        print(f"❌ Configuration file not found: {args.config}")
        sys.exit(1)
    
    print(f"📁 Using config: {args.config}")
    print(f"🌍 Environment: {args.env}")
    print(f"📊 Create indexes: {not args.no_indexes}")
    print("-" * 50)
    
    success = setup_database(args.config, create_indexes=not args.no_indexes)
    
    if success:
        print("✅ Database setup completed successfully!")
    else:
        print("❌ Database setup failed!")
        sys.exit(1)

if __name__ == "__main__":
    main()