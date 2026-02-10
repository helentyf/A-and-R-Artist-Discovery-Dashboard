-- Artist Scoring and Analysis Query
-- Creates a scored table identifying high-potential underrated artists
-- Based on Instagram engagement relative to Last.fm listening performance

-- Project: project-3455313a-b3a3-461c-861
-- Dataset: artist_discovery

CREATE OR REPLACE TABLE `project-3455313a-b3a3-461c-861.artist_discovery.artist_scores` AS

SELECT 
  artist_id,
  artist_name,
  primary_genre,
  collection_date,
  
  -- Raw metrics
  lastfm_listeners,
  lastfm_playcount,
  lastfm_playcount_per_listener,
  instagram_handle,
  instagram_followers,
  instagram_engagement_rate,
  instagram_avg_likes,
  youtube_channel_id,
  youtube_subscribers,
  youtube_total_views,
  
  -- SCORING COMPONENTS
  
  -- 1. Underrated Ratio
  -- Higher ratio = more Instagram presence relative to listening data
  -- Indicates artist has social media momentum but hasn't converted to listeners yet
  SAFE_DIVIDE(instagram_followers, NULLIF(lastfm_listeners, 0)) AS underrated_ratio,
  
  -- 2. Engagement Quality Score (0-10 scale)
  -- High engagement = real, active fanbase
  LEAST(instagram_engagement_rate * 2, 10) AS engagement_score,
  
  -- 3. Platform Presence Score
  -- Artists active on multiple platforms show broader appeal
  (CASE WHEN lastfm_listeners > 5000 THEN 1 ELSE 0 END +
   CASE WHEN instagram_followers > 10000 THEN 1 ELSE 0 END +
   CASE WHEN youtube_subscribers > 1000 THEN 1 ELSE 0 END) AS platform_count,
  
  -- 4. Growth Potential Indicator
  -- Categorizes artists based on Instagram-to-Last.fm ratio
  -- Lowered thresholds to identify more High Potential artists
  CASE 
    WHEN instagram_followers > lastfm_listeners * 1.5 THEN 'High'
    WHEN instagram_followers > lastfm_listeners * 0.8 THEN 'Medium'
    ELSE 'Low'
  END AS growth_potential,
  
  -- COMPOSITE A&R PRIORITY SCORE
  -- Weighted formula emphasizing Instagram strength vs Last.fm listener saturation
  -- Higher score = better candidate for label signing
  (
    (instagram_followers * 0.3) +
    (instagram_engagement_rate * instagram_followers * 0.4) +
    (youtube_subscribers * 0.2) +
    (lastfm_playcount_per_listener * 1000 * 0.1)
  ) / NULLIF(GREATEST(lastfm_listeners, 1), 0) AS priority_score,
  
  -- Additional metrics for analysis
  instagram_followers - lastfm_listeners AS follower_gap,
  youtube_avg_views_per_video,
  data_quality_score

FROM `project-3455313a-b3a3-461c-861.artist_discovery.artist_raw_data`

WHERE 
  -- Filter criteria for emerging artists
  primary_genre IN ('jazz', 'folk', 'alternative', 'soul', 'indie', 'indie folk', 'blues', 'rnb', 'nu jazz', 'funk', 'piano', '80s', 'unknown', 'indie rock', 'singer-songwriter', 'british indie', 'acoustic', 'neo soul', 'jazz fusion')
  AND lastfm_listeners BETWEEN 5000 AND 200000
  AND (instagram_followers IS NULL OR instagram_followers BETWEEN 5000 AND 100000)
  AND (instagram_engagement_rate IS NULL OR instagram_engagement_rate > 1.0)
  AND data_quality_score >= 0.3  -- Lower threshold to include artists without Instagram data
  AND lastfm_listeners > 0  -- Must have Last.fm presence

ORDER BY priority_score DESC;

-- Summary statistics query (run separately)
-- SELECT 
--   primary_genre,
--   COUNT(*) as artist_count,
--   AVG(priority_score) as avg_priority_score,
--   AVG(underrated_ratio) as avg_underrated_ratio,
--   AVG(engagement_score) as avg_engagement,
--   COUNTIF(growth_potential = 'High') as high_growth_count
-- FROM `project-3455313a-b3a3-461c-861.artist_discovery.artist_scores`
-- GROUP BY primary_genre
-- ORDER BY avg_priority_score DESC;
