# YouTube Data Pipeline - Setup Guide

## Quick Start

### Prerequisites
- Python 3.8+
- PostgreSQL 12+
- YouTube API Key

### 1. Clone the Repository

```bash
git clone https://github.com/yourusername/youtube-data-pipeline.git
cd youtube-data-pipeline

### 2. Set up Python Environment

# Create virtual environment
python -m venv youtube_env
source youtube_env/bin/activate  # On Windows: youtube_env\Scripts\activate

# Install dependencies
pip install -e .

### 3. Configure Environment

# Copy environment template
cp .env.example .env

# Edit .env with your settings
nano .env  # or use your favorite editor

### 4. Set up Database

# Create database (using PostgreSQL CLI)
createdb youtube_analytics

# Or use the setup script
python src/scripts/setup_database.py

### 5. Get YouTube API Key

Go to Google Cloud Console

Create a new project

Enable YouTube Data API v3

Create credentials (API Key)

Add the key to your .env file

### 6. Run the Pipeline

# Basic run
python main.py

# With specific channels
python main.py --channels UC_x5XG1OV2P6uZZ5FSM9Ttw UCBJycsmduvYEL83R_U4JriQ

# With monitoring dashboard
python main.py --monitor


### Docker Setup
### Using Docker Compose (Recommended)

# Copy and configure environment
cp .env.example .env
# Edit .env with your settings

# Build and start services
docker-compose up -d

# View logs
docker-compose logs -f youtube-pipeline

# Stop services
docker-compose down

### Manual Docker Setup

# Build the image
docker build -t youtube-data-pipeline .

# Run the container
docker run -d \
  --name youtube-pipeline \
  --env-file .env \
  -v $(pwd)/config:/app/config \
  -v $(pwd)/data:/app/data \
  youtube-data-pipeline

### Verification
### Test Database Connection

python -c "
from src.youtube_pipeline.load.database_manager import DatabaseManager
from src.youtube_pipeline.utils.config_loader import ConfigLoader

config = ConfigLoader.load_yaml_config('config/pipeline.yaml')
db = DatabaseManager(config)
if db.connect():
    print('✅ Database connection successful')
    stats = db.get_database_stats()
    print(f'Database stats: {stats}')
else:
    print('❌ Database connection failed')
"

### Test YouTube API

python -c "
from src.youtube_pipeline.extract.youtube_client import YouTubeClient
from src.youtube_pipeline.utils.config_loader import ConfigLoader
import os

config = ConfigLoader.load_yaml_config('config/pipeline.yaml')
client = YouTubeClient(config)

# Test with a public channel
channel_data = client.get_channel_data('UC_x5XG1OV2P6uZZ5FSM9Ttw')
if channel_data:
    print('✅ YouTube API connection successful')
    print(f'Channel: {channel_data[\\\"title\\\"]}')
else:
    print('❌ YouTube API connection failed')
"

