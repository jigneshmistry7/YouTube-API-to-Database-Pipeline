Enable Detailed Logging

import logging
logging.basicConfig(level=logging.DEBUG)

Test Individual Components

# Test YouTube client
from youtube_pipeline.extract.youtube_client import YouTubeClient
client = YouTubeClient(config)
channel_data = client.get_channel_data("UC_x5XG1OV2P6uZZ5FSM9Ttw")

# Test database connection
from youtube_pipeline.load.database_manager import DatabaseManager
db = DatabaseManager(config)
if db.connect():
    stats = db.get_database_stats()
    print(stats)

Use the Debug Script
python scripts/debug_pipeline.py --test-all