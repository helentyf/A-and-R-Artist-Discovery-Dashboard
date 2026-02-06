"""
Data Merging and BigQuery Upload Script

Merges data from Last.fm, Instagram, and YouTube sources into a unified dataset
and uploads to Google BigQuery for analysis and scoring.
"""

import pandas as pd
from google.cloud import bigquery
from datetime import datetime
import os
from dotenv import load_dotenv
from pathlib import Path

load_dotenv()

# Set up BigQuery client
project_id = os.getenv('BIGQUERY_PROJECT_ID')
dataset_id = os.getenv('BIGQUERY_DATASET_ID', 'artist_discovery')

# Set service account credentials path
credentials_path = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')
if credentials_path and os.path.exists(credentials_path):
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credentials_path
else:
    print("Warning: GOOGLE_APPLICATION_CREDENTIALS not set or file not found")
    print("Attempting to use default credentials...")

client = bigquery.Client(project=project_id)


def load_data_files():
    """
    Load all data files from data collection scripts.
    
    Returns:
        Tuple of DataFrames: (lastfm_df, instagram_df, youtube_df)
    """
    lastfm_path = 'data/raw/lastfm_artists_raw.csv'
    instagram_path = 'data/raw/instagram_data_raw.csv'
    youtube_path = 'data/raw/youtube_data_raw.csv'
    
    # Load Last.fm data (required)
    if not os.path.exists(lastfm_path):
        raise FileNotFoundError(f"Last.fm data not found: {lastfm_path}")
    
    lastfm_df = pd.read_csv(lastfm_path)
    print(f"Loaded Last.fm data: {len(lastfm_df)} artists")
    
    # Load Instagram data (optional)
    instagram_df = None
    if os.path.exists(instagram_path):
        instagram_df = pd.read_csv(instagram_path)
        print(f"Loaded Instagram data: {len(instagram_df)} artists")
    else:
        print("Instagram data not found (optional)")
    
    # Load YouTube data (optional)
    youtube_df = None
    if os.path.exists(youtube_path):
        youtube_df = pd.read_csv(youtube_path)
        print(f"Loaded YouTube data: {len(youtube_df)} artists")
    else:
        print("YouTube data not found (optional)")
    
    return lastfm_df, instagram_df, youtube_df


def merge_all_data(lastfm_df, instagram_df, youtube_df):
    """
    Merge all data sources on artist_id.
    
    Args:
        lastfm_df: DataFrame with Last.fm data
        instagram_df: DataFrame with Instagram data (can be None)
        youtube_df: DataFrame with YouTube data (can be None)
        
    Returns:
        Merged DataFrame
    """
    merged = lastfm_df.copy()
    
    # Merge Instagram data
    if instagram_df is not None and len(instagram_df) > 0:
        # Select only Instagram-specific columns to avoid conflicts
        ig_cols = [col for col in instagram_df.columns 
                  if col not in ['artist_id', 'artist_name'] or col.startswith('instagram_')]
        ig_cols = ['artist_id'] + ig_cols
        
        merged = merged.merge(
            instagram_df[ig_cols], 
            on='artist_id', 
            how='left',
            suffixes=('', '_ig')
        )
        print(f"Merged Instagram data: {merged['instagram_followers'].notna().sum()} artists have Instagram data")
    else:
        # Add empty Instagram columns
        merged['instagram_handle'] = None
        merged['instagram_followers'] = None
        merged['instagram_following'] = None
        merged['instagram_posts'] = None
        merged['instagram_avg_likes'] = None
        merged['instagram_avg_comments'] = None
        merged['instagram_engagement_rate'] = None
        merged['instagram_reels_count'] = None
        merged['instagram_verified'] = None
    
    # Merge YouTube data
    if youtube_df is not None and len(youtube_df) > 0:
        # Select only YouTube-specific columns
        yt_cols = [col for col in youtube_df.columns 
                  if col not in ['artist_id', 'artist_name'] or col.startswith('youtube_')]
        yt_cols = ['artist_id'] + yt_cols
        
        merged = merged.merge(
            youtube_df[yt_cols], 
            on='artist_id', 
            how='left',
            suffixes=('', '_yt')
        )
        print(f"Merged YouTube data: {merged['youtube_subscribers'].notna().sum()} artists have YouTube data")
    else:
        # Add empty YouTube columns
        merged['youtube_channel_id'] = None
        merged['youtube_subscribers'] = None
        merged['youtube_total_views'] = None
        merged['youtube_video_count'] = None
        merged['youtube_avg_views_per_video'] = None
    
    # Add metadata
    merged['collection_date'] = datetime.now().date()
    merged['last_updated'] = datetime.now()
    
    # Calculate data quality score (0-1 based on completeness)
    # Weight: Last.fm 30%, Instagram 40%, YouTube 30%
    merged['data_quality_score'] = (
        merged['lastfm_listeners'].notna().astype(int) * 0.3 +
        merged['instagram_followers'].notna().astype(int) * 0.4 +
        merged['youtube_subscribers'].notna().astype(int) * 0.3
    )
    
    # Standardize column names for BigQuery
    merged = merged.rename(columns={
        'collection_timestamp': 'collection_timestamp'
    })
    
    return merged


def create_bigquery_schema():
    """
    Create BigQuery dataset and table schema if they don't exist.
    """
    # Create dataset if it doesn't exist
    dataset_ref = client.dataset(dataset_id)
    try:
        client.get_dataset(dataset_ref)
        print(f"Dataset {dataset_id} already exists")
    except Exception:
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = "US"
        dataset = client.create_dataset(dataset, exists_ok=True)
        print(f"Created dataset {dataset_id}")
    
    # Define table schema
    schema = [
        bigquery.SchemaField("artist_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("artist_name", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("collection_date", "DATE", mode="REQUIRED"),
        
        # Last.fm fields
        bigquery.SchemaField("lastfm_url", "STRING"),
        bigquery.SchemaField("lastfm_listeners", "INT64"),
        bigquery.SchemaField("lastfm_playcount", "INT64"),
        bigquery.SchemaField("lastfm_playcount_per_listener", "FLOAT64"),
        bigquery.SchemaField("genres", "STRING"),
        bigquery.SchemaField("primary_genre", "STRING"),
        
        # Instagram fields
        bigquery.SchemaField("instagram_handle", "STRING"),
        bigquery.SchemaField("instagram_followers", "INT64"),
        bigquery.SchemaField("instagram_following", "INT64"),
        bigquery.SchemaField("instagram_posts", "INT64"),
        bigquery.SchemaField("instagram_avg_likes", "FLOAT64"),
        bigquery.SchemaField("instagram_avg_comments", "FLOAT64"),
        bigquery.SchemaField("instagram_engagement_rate", "FLOAT64"),
        bigquery.SchemaField("instagram_reels_count", "INT64"),
        bigquery.SchemaField("instagram_verified", "BOOLEAN"),
        
        # YouTube fields
        bigquery.SchemaField("youtube_channel_id", "STRING"),
        bigquery.SchemaField("youtube_subscribers", "INT64"),
        bigquery.SchemaField("youtube_total_views", "INT64"),
        bigquery.SchemaField("youtube_video_count", "INT64"),
        bigquery.SchemaField("youtube_avg_views_per_video", "FLOAT64"),
        
        # Metadata
        bigquery.SchemaField("data_quality_score", "FLOAT64"),
        bigquery.SchemaField("last_updated", "TIMESTAMP"),
    ]
    
    table_id = f"{project_id}.{dataset_id}.artist_raw_data"
    table_ref = bigquery.Table(table_id, schema=schema)
    
    try:
        client.get_table(table_ref)
        print(f"Table artist_raw_data already exists")
    except Exception:
        table = client.create_table(table_ref)
        print(f"Created table {table_id}")
    
    return table_id


def upload_to_bigquery(df, table_id):
    """
    Upload DataFrame to BigQuery.
    
    Args:
        df: DataFrame to upload
        table_id: Full table ID (project.dataset.table)
    """
    # Prepare data types for BigQuery
    df_upload = df.copy()
    
    # Convert date columns
    if 'collection_date' in df_upload.columns:
        df_upload['collection_date'] = pd.to_datetime(df_upload['collection_date']).dt.date
    
    # Convert timestamp columns
    if 'last_updated' in df_upload.columns:
        df_upload['last_updated'] = pd.to_datetime(df_upload['last_updated'])
    
    # Convert boolean columns
    if 'instagram_verified' in df_upload.columns:
        df_upload['instagram_verified'] = df_upload['instagram_verified'].fillna(False)
    
    # Fill NaN values appropriately
    int_cols = ['lastfm_listeners', 'lastfm_playcount', 'instagram_followers', 
                'instagram_following', 'instagram_posts', 'youtube_subscribers',
                'youtube_total_views', 'youtube_video_count']
    for col in int_cols:
        if col in df_upload.columns:
            df_upload[col] = df_upload[col].fillna(0).astype('Int64')
    
    float_cols = ['lastfm_playcount_per_listener', 'instagram_avg_likes', 
                  'instagram_avg_comments', 'instagram_engagement_rate', 
                  'youtube_avg_views_per_video', 'data_quality_score']
    for col in float_cols:
        if col in df_upload.columns:
            df_upload[col] = df_upload[col].fillna(0.0)
    
    # Configure load job
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",  # Overwrite existing data
        autodetect=False,  # Use explicit schema
        schema=client.get_table(table_id).schema
    )
    
    print(f"\nUploading {len(df_upload)} rows to BigQuery...")
    job = client.load_table_from_dataframe(df_upload, table_id, job_config=job_config)
    job.result()  # Wait for completion
    
    print(f"Successfully uploaded {len(df_upload)} rows to {table_id}")
    
    # Print summary
    table = client.get_table(table_id)
    print(f"Table now contains {table.num_rows} rows")


def main():
    """
    Main execution function.
    """
    print("=" * 60)
    print("Data Merging and BigQuery Upload")
    print("=" * 60)
    
    try:
        # Load data files
        lastfm_df, instagram_df, youtube_df = load_data_files()
        
        # Merge all data
        print("\nMerging data sources...")
        merged_df = merge_all_data(lastfm_df, instagram_df, youtube_df)
        
        # Save merged data locally
        output_path = 'data/raw/artist_raw_data_merged.csv'
        merged_df.to_csv(output_path, index=False)
        print(f"\nMerged data saved to: {output_path}")
        
        # Create BigQuery schema
        print("\nSetting up BigQuery...")
        table_id = create_bigquery_schema()
        
        # Upload to BigQuery
        upload_to_bigquery(merged_df, table_id)
        
        print("\n" + "=" * 60)
        print("Data pipeline complete!")
        print("=" * 60)
        print(f"\nNext steps:")
        print(f"1. Run SQL script: sql/create_artist_scores.sql")
        print(f"2. Generate insights: python scripts/generate_insights.py")
        print(f"3. Launch dashboard: streamlit run dashboard/app.py")
        
    except Exception as e:
        print(f"\nError: {e}")
        raise


if __name__ == "__main__":
    main()
