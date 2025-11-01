Database Schema
dim_channels
Channel dimension table.

Column	Type	Description
channel_id	VARCHAR(255)	Primary key
channel_name	VARCHAR(255)	Channel title
description	TEXT	Channel description
published_at	TIMESTAMP	Channel creation date
view_count	BIGINT	Total views
subscriber_count	BIGINT	Subscriber count
video_count	INTEGER	Total videos


dim_videos
Video dimension table.

Column	Type	Description
video_id	VARCHAR(255)	Primary key
channel_id	VARCHAR(255)	Foreign key to dim_channels
title	VARCHAR(500)	Video title
description	TEXT	Video description
published_at	TIMESTAMP	Video publish date
view_count	BIGINT	View count
like_count	BIGINT	Like count
engagement_rate	DECIMAL(5,2)	Engagement percentage


fact_video_stats
Video statistics fact table.

Column	Type	Description
stat_id	SERIAL	Primary key
video_id	VARCHAR(255)	Foreign key to dim_videos
date_id	INTEGER	Foreign key to dim_dates
view_count	BIGINT	Views on this date
like_count	BIGINT	Likes on this date
updated_at	TIMESTAMP	Last update timestamp