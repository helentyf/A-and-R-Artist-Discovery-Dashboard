"""
Insights Generation Script

Queries BigQuery to generate insights and identify top underrated artists
for A&R discovery purposes.
"""

from google.cloud import bigquery
import pandas as pd
import os
from dotenv import load_dotenv

load_dotenv()

# Initialize BigQuery client
project_id = os.getenv('BIGQUERY_PROJECT_ID')
dataset_id = os.getenv('BIGQUERY_DATASET_ID', 'artist_discovery')

credentials_path = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')
if credentials_path and os.path.exists(credentials_path):
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credentials_path

client = bigquery.Client(project=project_id)


def get_top_underrated_artists(limit=50):
    """
    Get top ranked underrated artists.
    
    Args:
        limit: Number of artists to return
        
    Returns:
        DataFrame with top artists
    """
    query = f"""
    SELECT 
      artist_name,
      primary_genre,
      priority_score,
      underrated_ratio,
      engagement_score,
      instagram_followers,
      lastfm_listeners,
      youtube_subscribers,
      growth_potential
    FROM `{project_id}.{dataset_id}.artist_scores`
    ORDER BY priority_score DESC
    LIMIT {limit}
    """
    
    try:
        df = client.query(query).to_dataframe()
        return df
    except Exception as e:
        print(f"Error querying top artists: {e}")
        return pd.DataFrame()


def generate_summary_stats():
    """
    Generate summary statistics by genre.
    
    Returns:
        DataFrame with genre-level statistics
    """
    query = f"""
    SELECT 
      primary_genre,
      COUNT(*) as artist_count,
      AVG(priority_score) as avg_priority_score,
      AVG(underrated_ratio) as avg_underrated_ratio,
      AVG(engagement_score) as avg_engagement,
      COUNTIF(growth_potential = 'High') as high_growth_count,
      AVG(instagram_followers) as avg_instagram_followers,
      AVG(lastfm_listeners) as avg_lastfm_listeners
    FROM `{project_id}.{dataset_id}.artist_scores`
    GROUP BY primary_genre
    ORDER BY avg_priority_score DESC
    """
    
    try:
        df = client.query(query).to_dataframe()
        return df
    except Exception as e:
        print(f"Error generating summary stats: {e}")
        return pd.DataFrame()


def get_high_growth_artists():
    """
    Get artists with high growth potential.
    
    Returns:
        DataFrame with high growth artists
    """
    query = f"""
    SELECT 
      artist_name,
      primary_genre,
      priority_score,
      underrated_ratio,
      instagram_followers,
      lastfm_listeners,
      follower_gap,
      engagement_score
    FROM `{project_id}.{dataset_id}.artist_scores`
    WHERE growth_potential = 'High'
    ORDER BY priority_score DESC
    LIMIT 30
    """
    
    try:
        df = client.query(query).to_dataframe()
        return df
    except Exception as e:
        print(f"Error querying high growth artists: {e}")
        return pd.DataFrame()


def generate_insights_report():
    """
    Generate comprehensive insights report.
    """
    print("=" * 80)
    print("UNDERRATED ARTIST RADAR - INSIGHTS REPORT")
    print("=" * 80)
    
    # Top artists
    print("\nTOP 20 UNDERRATED ARTISTS")
    print("-" * 80)
    top_artists = get_top_underrated_artists(limit=20)
    
    if len(top_artists) > 0:
        display_cols = ['artist_name', 'primary_genre', 'priority_score', 
                       'instagram_followers', 'lastfm_listeners', 'growth_potential']
        print(top_artists[display_cols].to_string(index=False))
        
        # Save to CSV
        top_artists.to_csv('data/top_underrated_artists.csv', index=False)
        print(f"\nFull results saved to: data/top_underrated_artists.csv")
    else:
        print("No artists found. Ensure artist_scores table exists in BigQuery.")
        return
    
    # Genre analysis
    print("\n\nGENRE ANALYSIS")
    print("-" * 80)
    genre_stats = generate_summary_stats()
    
    if len(genre_stats) > 0:
        print(genre_stats.to_string(index=False))
        genre_stats.to_csv('data/genre_analysis.csv', index=False)
        print(f"\nGenre analysis saved to: data/genre_analysis.csv")
    
    # High growth artists
    print("\n\nHIGH GROWTH POTENTIAL ARTISTS")
    print("-" * 80)
    high_growth = get_high_growth_artists()
    
    if len(high_growth) > 0:
        display_cols = ['artist_name', 'primary_genre', 'priority_score',
                       'follower_gap', 'underrated_ratio']
        print(high_growth[display_cols].to_string(index=False))
        high_growth.to_csv('data/high_growth_artists.csv', index=False)
        print(f"\nHigh growth artists saved to: data/high_growth_artists.csv")
    
    # Key insights
    print("\n\nKEY INSIGHTS")
    print("-" * 80)
    
    if len(top_artists) > 0:
        avg_ratio = top_artists['underrated_ratio'].mean()
        max_ratio = top_artists['underrated_ratio'].max()
        top_artist = top_artists.iloc[0]
        
        print(f"1. Average underrated ratio (top 20): {avg_ratio:.2f}")
        print(f"   (Instagram followers / Last.fm listeners)")
        print(f"\n2. Highest underrated ratio: {max_ratio:.2f}")
        print(f"   Artist: {top_artists.loc[top_artists['underrated_ratio'].idxmax(), 'artist_name']}")
        print(f"\n3. Top priority artist: {top_artist['artist_name']}")
        print(f"   Genre: {top_artist['primary_genre']}")
        print(f"   Instagram: {top_artist['instagram_followers']:,} followers")
        print(f"   Last.fm: {top_artist['lastfm_listeners']:,} listeners")
        print(f"   Growth potential: {top_artist['growth_potential']}")
        
        if len(genre_stats) > 0:
            top_genre = genre_stats.iloc[0]
            print(f"\n4. Highest potential genre: {top_genre['primary_genre']}")
            print(f"   Artists analyzed: {top_genre['artist_count']}")
            print(f"   Average priority score: {top_genre['avg_priority_score']:.2f}")
            print(f"   High growth artists: {top_genre['high_growth_count']}")
    
    print("\n" + "=" * 80)
    print("Report generation complete")
    print("=" * 80)


def main():
    """
    Main execution function.
    """
    try:
        generate_insights_report()
    except Exception as e:
        print(f"\nError generating insights: {e}")
        print("\nTroubleshooting:")
        print("1. Ensure BigQuery project ID is set in .env file")
        print("2. Ensure artist_scores table exists (run sql/create_artist_scores.sql)")
        print("3. Check Google Cloud credentials")
        raise


if __name__ == "__main__":
    main()
