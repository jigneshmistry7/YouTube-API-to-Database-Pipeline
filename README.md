# YouTube API to Database Pipeline 🚀

A comprehensive data pipeline that extracts data from YouTube APIs, processes it, and loads it into a PostgreSQL database for analytics and reporting.

![Pipeline Diagram](https://drive.google.com/file/d/1DUcqnDsgogjDZOqb8SBo7Wi6np_Yb2Zw/view?usp=sharing)
*Architecture Diagram - Replace with your actual diagram*

## 📋 Table of Contents
- [Overview](#overview)
- [Features](#features)
- [Architecture](#architecture)
- [Quick Start](#quick-start)
- [Installation](#installation)
- [Configuration](#configuration)
- [Usage](#usage)
- [Database Schema](#database-schema)
- [API Endpoints](#api-endpoints)
- [Monitoring](#monitoring)
- [Troubleshooting](#troubleshooting)
- [Contributing](#contributing)
- [License](#license)

## 🎯 Overview

This project solves the problem of **collecting and analyzing YouTube data systematically**. Instead of manually checking YouTube analytics, this pipeline automatically:

- 📊 **Tracks video performance** (views, likes, comments)
- 🔍 **Monitors channel growth** (subscribers, view counts)
- 💬 **Analyzes user engagement** (comment sentiment, engagement rates)
- 📈 **Provides historical data** for trend analysis

**Perfect for**: Content creators, marketing agencies, data analysts, and anyone who wants to make data-driven decisions about YouTube content.

## ✨ Features

| Feature | Description | Benefit |
|---------|-------------|---------|
| **Automated Data Collection** | Scheduled YouTube API calls | Saves time, ensures data consistency |
| **Comprehensive Analytics** | Tracks 20+ metrics per video | Deep insights into performance |
| **Error Handling** | Automatic retries and failure recovery | Reliable data pipeline |
| **Rate Limit Management** | Smart API quota management | Prevents service interruptions |
| **Real-time Monitoring** | Live pipeline health dashboard | Quick issue identification |
| **Scalable Architecture** | Handles multiple channels/videos | Grows with your needs |

## 🏗️ Architecture

### Pipeline Flow
```
YouTube APIs → Extraction Layer → Processing Layer → Database → Analytics
```

### Component Details

#### 1. **Data Sources Layer** 🔗
- **YouTube Data API v3**: Official Google API
- **Supported Endpoints**:
  - `Search API`: Find videos by keywords
  - `Videos API`: Get video details and statistics
  - `Channels API`: Channel information and stats
  - `Comments API`: User comments and replies
  - `Statistics API`: Real-time metrics

#### 2. **Extraction Layer** ⬇️
- **API Client**: Handles HTTP requests to YouTube
- **Authentication**: Manages API keys securely
- **Rate Limit Manager**: Prevents hitting API quotas
- **Data Fetcher**: Retrieves specific data endpoints

#### 3. **Processing Layer** 🔄
- **Data Validator**: Ensures data quality and completeness
- **Data Transformer**: Converts JSON to structured format
- **Data Enricher**: Calculates derived metrics (engagement rate, etc.)
- **Quality Checker**: Validates data before storage

#### 4. **Storage Layer** 💾
- **PostgreSQL Database**: Reliable, SQL-based storage
- **Star Schema**: Optimized for analytics queries
- **Historical Tracking**: Time-series data storage

## 🚀 Quick Start

### Prerequisites
- Python 3.8+
- PostgreSQL 12+
- YouTube API Key

### 5-Minute Setup
```bash
# 1. Clone the repository
git clone https://github.com/yourusername/youtube-data-pipeline.git
cd youtube-data-pipeline

# 2. Install dependencies
pip install -r requirements.txt

# 3. Setup environment variables
cp .env.example .env
# Edit .env with your credentials

# 4. Run the pipeline
python main.py --channel-id UC_x5XG1OV2P6uZZ5FSM9Ttw
```

## 📥 Installation

### Step 1: Get YouTube API Key
1. Go to [Google Cloud Console](https://console.cloud.google.com/)
2. Create a new project or select existing one
3. Enable **YouTube Data API v3**
4. Create **API Credentials**
5. Copy your API key

### Step 2: Database Setup
```sql
-- Create database
CREATE DATABASE youtube_analytics;

-- Create user (optional)
CREATE USER youtube_user WITH PASSWORD 'your_password';
GRANT ALL PRIVILEGES ON DATABASE youtube_analytics TO youtube_user;
```

### Step 3: Project Setup
```bash
# Create virtual environment
python -m venv youtube_env
source youtube_env/bin/activate  # On Windows: youtube_env\Scripts\activate

# Install package
pip install -e .

# Or install from requirements
pip install -r requirements.txt
```

### Step 4: Configuration
Create `.env` file:
```env
# YouTube API Configuration
YOUTUBE_API_KEY=your_api_key_here

# Database Configuration
DB_HOST=localhost
DB_PORT=5432
DB_NAME=youtube_analytics
DB_USER=youtube_user
DB_PASSWORD=your_password

# Pipeline Settings
BATCH_SIZE=50
UPDATE_FREQUENCY=3600  # 1 hour in seconds
```

## ⚙️ Configuration

### Channel Configuration
Add channels to monitor in `config/channels.json`:
```json
{
  "channels": [
    {
      "channel_id": "UC_x5XG1OV2P6uZZ5FSM9Ttw",
      "channel_name": "Google Developers",
      "active": true,
      "update_frequency": 3600
    }
  ]
}
```

### Pipeline Settings
Modify `config/pipeline.yaml`:
```yaml
extraction:
  batch_size: 50
  max_retries: 3
  retry_delay: 60

processing:
  enable_sentiment_analysis: true
  calculate_engagement_rate: true
  detect_anomalies: true

storage:
  keep_raw_data: false
  backup_frequency: "daily"
```

## 📊 Usage

### Basic Data Collection
```python
from pipeline import YouTubeDataPipeline

# Initialize pipeline
pipeline = YouTubeDataPipeline()

# Collect data for a channel
channel_data = pipeline.collect_channel_data("UC_x5XG1OV2P6uZZ5FSM9Ttw")

# Collect data for specific videos
video_data = pipeline.collect_video_data(["video_id_1", "video_id_2"])

# Run complete pipeline
pipeline.run()
```

### Scheduled Collection
```python
# Run every 6 hours
from apscheduler.schedulers.blocking import BlockingScheduler

scheduler = BlockingScheduler()
scheduler.add_job(pipeline.run, 'interval', hours=6)
scheduler.start()
```

### Custom Queries
```sql
-- Get top performing videos
SELECT 
    video_id,
    title,
    view_count,
    like_count,
    (like_count::FLOAT / NULLIF(view_count, 0)) * 100 as engagement_rate
FROM fact_video_stats 
JOIN dim_videos USING (video_id)
ORDER BY engagement_rate DESC
LIMIT 10;
```

## 🗃️ Database Schema

### Tables Overview

#### 📺 Dimension Tables (Descriptive Data)
| Table | Description | Key Fields |
|-------|-------------|------------|
| `dim_channels` | Channel information | `channel_id`, `channel_name`, `subscriber_count` |
| `dim_videos` | Video metadata | `video_id`, `title`, `description`, `published_at` |
| `dim_dates` | Date dimensions | `date_id`, `full_date`, `day_name`, `month_name` |

#### 📈 Fact Tables (Measurable Data)
| Table | Description | Metrics |
|-------|-------------|---------|
| `fact_video_stats` | Video performance | `views`, `likes`, `comments`, `engagement_rate` |
| `fact_comments` | Comment analysis | `comment_count`, `sentiment_score`, `topics` |

### Schema Details

#### dim_channels
```sql
CREATE TABLE dim_channels (
    channel_id VARCHAR(255) PRIMARY KEY,
    channel_name VARCHAR(255) NOT NULL,
    description TEXT,
    published_at TIMESTAMP,
    country VARCHAR(100),
    view_count BIGINT DEFAULT 0,
    subscriber_count BIGINT DEFAULT 0,
    video_count INT DEFAULT 0,
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

#### fact_video_stats
```sql
CREATE TABLE fact_video_stats (
    stat_id SERIAL PRIMARY KEY,
    video_id VARCHAR(255) REFERENCES dim_videos(video_id),
    date_id INTEGER REFERENCES dim_dates(date_id),
    view_count BIGINT DEFAULT 0,
    like_count BIGINT DEFAULT 0,
    comment_count BIGINT DEFAULT 0,
    favorite_count BIGINT DEFAULT 0,
    engagement_rate DECIMAL(5,2),
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

## 🔌 API Endpoints

### Supported YouTube APIs

| Endpoint | Method | Data Collected |
|----------|--------|----------------|
| `channels.list` | GET | Channel statistics, branding |
| `videos.list` | GET | Video metadata, statistics |
| `search.list` | GET | Video search results |
| `commentThreads.list` | GET | Comments, replies, likes |

### Example API Usage
```python
from youtube_client import YouTubeClient

client = YouTubeClient(api_key="your_key")

# Get channel statistics
channel_info = client.get_channel_stats("UC_x5XG1OV2P6uZZ5FSM9Ttw")

# Get video details
video_info = client.get_video_details(["video_id_1", "video_id_2"])

# Search for videos
search_results = client.search_videos("python tutorial", max_results=50)
```

## 📊 Monitoring

### Dashboard Features
- **Real-time Pipeline Status**
- **API Quota Usage**
- **Data Freshness Indicators**
- **Error Rate Monitoring**
- **Performance Metrics**

### Setting Up Monitoring
```python
from monitoring import PipelineMonitor

monitor = PipelineMonitor()

# Check pipeline health
health = monitor.check_health()

# Get API quota status
quota = monitor.get_quota_usage()

# View recent errors
errors = monitor.get_recent_errors()
```

### Alert Configuration
```yaml
alerts:
  api_quota:
    threshold: 80%
    channels: ["email", "slack"]
  
  data_freshness:
    threshold: "2 hours"
    channels: ["email"]
  
  error_rate:
    threshold: "5%"
    channels: ["pagerduty", "slack"]
```

## 🐛 Troubleshooting

### Common Issues

#### ❌ "API Quota Exceeded"
**Solution**: 
```python
# Reduce request frequency or use multiple API keys
pipeline = YouTubeDataPipeline(
    api_keys=['key1', 'key2', 'key3'],
    requests_per_minute=50
)
```

#### ❌ "Database Connection Failed"
**Solution**:
```python
# Check connection settings
import psycopg2
try:
    conn = psycopg2.connect(
        host="localhost",
        database="youtube_analytics",
        user="your_username"
    )
except Exception as e:
    print(f"Connection failed: {e}")
```

#### ❌ "Missing Video Data"
**Solution**:
```python
# Enable debug mode for detailed logs
pipeline = YouTubeDataPipeline(debug=True)
pipeline.collect_video_data(video_ids, force_refresh=True)
```

### Debug Mode
```bash
# Run with verbose logging
python main.py --debug --log-level DEBUG

# Test specific component
python -m pytest tests/test_youtube_client.py -v
```

## 🤝 Contributing

We love contributions! Here's how to help:

### Reporting Bugs
1. Check existing [issues](https://github.com/yourusername/youtube-data-pipeline/issues)
2. Create new issue with:
   - Error logs
   - Steps to reproduce
   - Expected vs actual behavior

### Suggesting Features
1. Check [feature requests](https://github.com/yourusername/youtube-data-pipeline/issues?q=is%3Aissue+label%3Aenhancement)
2. Create new issue with:
   - Use case description
   - Proposed implementation
   - Benefits

### Code Contributions
```bash
# 1. Fork repository
# 2. Create feature branch
git checkout -b feature/amazing-feature

# 3. Commit changes
git commit -m "Add amazing feature"

# 4. Push to branch
git push origin feature/amazing-feature

# 5. Create Pull Request
```

### Development Setup
```bash
# Install development dependencies
pip install -r requirements-dev.txt

# Run tests
pytest

# Check code quality
flake8
black --check .
```

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🙏 Acknowledgments

- **YouTube Data API** - For providing comprehensive data access
- **PostgreSQL** - Robust database solution
- **Python Community** - Amazing libraries and tools

## 📞 Support

- **Documentation**: [Full Docs](https://github.com/yourusername/youtube-data-pipeline/wiki)
- **Issues**: [GitHub Issues](https://github.com/yourusername/youtube-data-pipeline/issues)
- **Discussions**: [GitHub Discussions](https://github.com/yourusername/youtube-data-pipeline/discussions)
- **Email**: support@youremail.com

---

**⭐ If this project helped you, please give it a star on GitHub!**

---

<div align="center">

### 🚀 Ready to start analyzing your YouTube data?

[Get Started](#quick-start) • [View Examples](examples/) • [Report Bug](https://github.com/yourusername/youtube-data-pipeline/issues)

</div>
