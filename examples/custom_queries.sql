-- YouTube Data Pipeline - Useful Analytical Queries
-- Save this file as custom_queries.sql

-- =============================================================================
-- CHANNEL ANALYTICS QUERIES
-- =============================================================================

-- 1. Channel Performance Overview
SELECT 
    channel_id,
    channel_name,
    subscriber_count,
    view_count,
    video_count,
    ROUND(view_count::DECIMAL / NULLIF(subscriber_count, 0), 2) as views_per_subscriber,
    ROUND(view_count::DECIMAL / NULLIF(video_count, 0), 0) as avg_views_per_video
FROM dim_channels
WHERE subscriber_count > 0
ORDER BY subscriber_count DESC;

-- 2. Channel Growth Tracking (requires historical data)
SELECT 
    c.channel_name,
    s.date_id,
    s.view_count,
    s.like_count,
    s.comment_count,
    LAG(s.view_count) OVER (PARTITION BY s.video_id ORDER BY s.date_id) as previous_views,
    s.view_count - LAG(s.view_count) OVER (PARTITION BY s.video_id ORDER BY s.date_id) as view_growth
FROM fact_video_stats s
JOIN dim_videos v ON s.video_id = v.video_id
JOIN dim_channels c ON v.channel_id = c.channel_id
WHERE c.channel_id = 'YOUR_CHANNEL_ID'
ORDER BY s.date_id DESC;

-- =============================================================================
-- VIDEO PERFORMANCE QUERIES
-- =============================================================================

-- 3. Top Performing Videos by Engagement Rate
SELECT 
    v.video_id,
    v.title,
    c.channel_name,
    v.view_count,
    v.like_count,
    v.comment_count,
    v.engagement_rate,
    v.published_at
FROM dim_videos v
JOIN dim_channels c ON v.channel_id = c.channel_id
WHERE v.view_count > 1000  -- Minimum views threshold
ORDER BY v.engagement_rate DESC
LIMIT 20;

-- 4. Video Performance Over Time
SELECT 
    d.full_date,
    COUNT(DISTINCT s.video_id) as videos_tracked,
    SUM(s.view_count) as total_views,
    SUM(s.like_count) as total_likes,
    AVG(s.view_count) as avg_views_per_video
FROM fact_video_stats s
JOIN dim_dates d ON s.date_id = d.date_id
WHERE s.date_id >= 20230101  -- Start date (YYYYMMDD)
GROUP BY d.full_date
ORDER BY d.full_date DESC;

-- 5. Best Performing Video Categories
SELECT 
    v.category_id,
    COUNT(*) as video_count,
    AVG(v.view_count) as avg_views,
    AVG(v.engagement_rate) as avg_engagement,
    SUM(v.view_count) as total_views
FROM dim_videos v
WHERE v.category_id IS NOT NULL
GROUP BY v.category_id
ORDER BY avg_engagement DESC;

-- =============================================================================
-- CONTENT STRATEGY QUERIES
-- =============================================================================

-- 6. Optimal Video Length Analysis
SELECT 
    CASE 
        WHEN duration LIKE 'PT%H%' THEN 'Long (>1 hour)'
        WHEN duration LIKE 'PT%M%' AND duration NOT LIKE '%H%' THEN
            CASE 
                WHEN CAST(SUBSTRING(duration FROM 'PT([0-9]+)M') AS INTEGER) < 5 THEN 'Short (<5 min)'
                WHEN CAST(SUBSTRING(duration FROM 'PT([0-9]+)M') AS INTEGER) <= 15 THEN 'Medium (5-15 min)'
                ELSE 'Long (15+ min)'
            END
        ELSE 'Unknown'
    END as duration_category,
    COUNT(*) as video_count,
    AVG(view_count) as avg_views,
    AVG(engagement_rate) as avg_engagement
FROM dim_videos
WHERE duration IS NOT NULL
GROUP BY duration_category
ORDER BY avg_engagement DESC;

-- 7. Title Length vs Performance
SELECT 
    CASE 
        WHEN LENGTH(title) < 30 THEN 'Short (<30 chars)'
        WHEN LENGTH(title) <= 60 THEN 'Medium (30-60 chars)'
        ELSE 'Long (>60 chars)'
    END as title_length,
    COUNT(*) as video_count,
    AVG(view_count) as avg_views,
    AVG(engagement_rate) as avg_engagement
FROM dim_videos
GROUP BY title_length
ORDER BY avg_engagement DESC;

-- 8. Tag Usage Analysis
SELECT 
    UNNEST(tags) as tag,
    COUNT(*) as usage_count,
    AVG(view_count) as avg_views,
    AVG(engagement_rate) as avg_engagement
FROM dim_videos
WHERE tags IS NOT NULL AND array_length(tags, 1) > 0
GROUP BY tag
HAVING COUNT(*) >= 5  -- Only tags used in at least 5 videos
ORDER BY avg_engagement DESC
LIMIT 20;

-- =============================================================================
-- TREND ANALYSIS QUERIES
-- =============================================================================

-- 9. Weekly Performance Trends
SELECT 
    EXTRACT(YEAR FROM d.full_date) as year,
    EXTRACT(WEEK FROM d.full_date) as week,
    COUNT(DISTINCT s.video_id) as active_videos,
    SUM(s.view_count) as weekly_views,
    SUM(s.like_count) as weekly_likes,
    AVG(s.view_count) as avg_views_per_video
FROM fact_video_stats s
JOIN dim_dates d ON s.date_id = d.date_id
GROUP BY year, week
ORDER BY year DESC, week DESC;

-- 10. Monthly Growth Analysis
WITH monthly_stats AS (
    SELECT 
        EXTRACT(YEAR FROM d.full_date) as year,
        EXTRACT(MONTH FROM d.full_date) as month,
        SUM(s.view_count) as monthly_views
    FROM fact_video_stats s
    JOIN dim_dates d ON s.date_id = d.date_id
    GROUP BY year, month
)
SELECT 
    year,
    month,
    monthly_views,
    LAG(monthly_views) OVER (ORDER BY year, month) as previous_month_views,
    ROUND(
        (monthly_views - LAG(monthly_views) OVER (ORDER BY year, month)) * 100.0 / 
        NULLIF(LAG(monthly_views) OVER (ORDER BY year, month), 0), 
        2
    ) as growth_percentage
FROM monthly_stats
ORDER BY year DESC, month DESC;

-- =============================================================================
-- COMPARATIVE ANALYSIS QUERIES
-- =============================================================================

-- 11. Channel Comparison
SELECT 
    c1.channel_name as channel_1,
    c2.channel_name as channel_2,
    c1.subscriber_count as subs_1,
    c2.subscriber_count as subs_2,
    ROUND(c1.view_count::DECIMAL / NULLIF(c1.subscriber_count, 0), 2) as engagement_1,
    ROUND(c2.view_count::DECIMAL / NULLIF(c2.subscriber_count, 0), 2) as engagement_2
FROM dim_channels c1
CROSS JOIN dim_channels c2
WHERE c1.channel_id != c2.channel_id
AND c1.subscriber_count > 10000
AND c2.subscriber_count > 10000
LIMIT 10;

-- 12. Performance Benchmarks by Channel Size
SELECT 
    CASE 
        WHEN subscriber_count < 1000 THEN 'Nano (<1K)'
        WHEN subscriber_count < 10000 THEN 'Micro (1K-10K)'
        WHEN subscriber_count < 100000 THEN 'Small (10K-100K)'
        WHEN subscriber_count < 1000000 THEN 'Medium (100K-1M)'
        ELSE 'Large (>1M)'
    END as channel_size,
    COUNT(*) as channel_count,
    AVG(view_count) as avg_views,
    AVG(video_count) as avg_videos,
    ROUND(AVG(view_count::DECIMAL / NULLIF(subscriber_count, 0)), 2) as avg_engagement_ratio
FROM dim_channels
WHERE subscriber_count > 0
GROUP BY channel_size
ORDER BY channel_size;