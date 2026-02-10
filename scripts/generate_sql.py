"""
SQL Generation Helper

Generates the create_artist_scores.sql file with the correct BigQuery project ID
from environment variables.
"""

import os
from dotenv import load_dotenv
from pathlib import Path

load_dotenv()


def generate_sql_file():
    """Generate SQL file with project ID from environment."""
    project_id = os.getenv('BIGQUERY_PROJECT_ID')
    dataset_id = os.getenv('BIGQUERY_DATASET_ID', 'artist_discovery')
    
    if not project_id or project_id.startswith('your'):
        print("ERROR: BIGQUERY_PROJECT_ID not set in .env file")
        print("Please set your BigQuery project ID in .env")
        return False
    
    sql_template = f"""-- Artist Scoring and Analysis Query
-- Creates a scored table identifying high-potential underrated artists
-- Based on Instagram engagement relative to Last.fm listening performance

-- Project: {project_id}
-- Dataset: {dataset_id}

CREATE OR REPLACE TABLE `{project_id}.{dataset_id}.artist_scores` AS

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
  CASE 
    WHEN instagram_followers > lastfm_listeners * 2 THEN 'High'
    WHEN instagram_followers > lastfm_listeners THEN 'Medium'
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

FROM `{project_id}.{dataset_id}.artist_raw_data`

WHERE 
  -- Filter criteria for emerging artists
  primary_genre IN ('jazz', 'folk', 'alternative', 'soul', 'indie', 'indie folk', 'blues', 'rnb', 'nu jazz', 'funk', 'piano', '80s', 'unknown')
  AND lastfm_listeners BETWEEN 5000 AND 200000
  AND (instagram_followers IS NULL OR instagram_followers BETWEEN 1000 AND 100000)
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
-- FROM `{project_id}.{dataset_id}.artist_scores`
-- GROUP BY primary_genre
-- ORDER BY avg_priority_score DESC;
"""
    
    output_path = Path('sql/create_artist_scores.sql')
    output_path.write_text(sql_template)
    
    print(f"Generated SQL file: {output_path}")
    print(f"Project ID: {project_id}")
    print(f"Dataset ID: {dataset_id}")
    print("\nYou can now run this SQL in BigQuery Console or via bq command line tool.")
    
    return True


if __name__ == "__main__":
    generate_sql_file()
