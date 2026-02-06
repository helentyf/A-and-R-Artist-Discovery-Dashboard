"""
Underrated Artist Radar Dashboard

Interactive Streamlit dashboard for exploring underrated artists discovered
through cross-platform analysis of Last.fm, Instagram, and YouTube metrics.
"""

import streamlit as st
from google.cloud import bigquery
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import os
import subprocess
import sys
from dotenv import load_dotenv

# Load environment variables for local development
# On Streamlit Cloud, secrets are automatically available via st.secrets
load_dotenv()

# Page configuration
st.set_page_config(
    page_title="Underrated Artist Radar",
    page_icon=None,
    layout="wide",
    initial_sidebar_state="expanded"
)

# Initialize BigQuery client with error handling and delays
@st.cache_resource
def init_bigquery_client():
    """Initialize BigQuery client with caching, error handling, and delays."""
    import time
    import json
    time.sleep(0.5)  # Delay to prevent rapid initialization issues
    
    # Get project ID from environment variables (local development)
    # For Streamlit Cloud, secrets are handled separately
    project_id = os.getenv('BIGQUERY_PROJECT_ID')
    
    if not project_id:
        st.warning("BIGQUERY_PROJECT_ID not set in environment variables")
        return None
    
    # Local: use Application Default Credentials or JSON file
    # For Streamlit Cloud, secrets are handled via .streamlit/secrets.toml
    credentials_path = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')
    
    if credentials_path and os.path.exists(credentials_path):
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credentials_path
    
    try:
        time.sleep(0.3)  # Additional delay before client creation
        client = bigquery.Client(project=project_id)
        return client
    except Exception as e:
        st.error(f"Error initializing BigQuery client: {e}")
        return None


@st.cache_data(ttl=60)
def load_artist_scores(_client, project_id, dataset_id):
    """
    Load artist scores from BigQuery with error handling and delays.
    
    Args:
        _client: BigQuery client (underscore prefix for cache)
        project_id: BigQuery project ID
        dataset_id: BigQuery dataset ID
        
    Returns:
        DataFrame with artist scores
    """
    import time
    time.sleep(0.5)  # Delay to prevent rapid queries
    
    query = f"""
    SELECT * 
    FROM `{project_id}.{dataset_id}.artist_scores`
    ORDER BY priority_score DESC
    """
    
    try:
        time.sleep(0.3)  # Additional delay before query
        df = _client.query(query).to_dataframe()
        time.sleep(0.2)  # Delay after query
        return df
    except Exception as e:
        st.error(f"Error loading data: {e}")
        return pd.DataFrame()


def main():
    """Main dashboard function with error handling and delays."""
    import time
    
    # Add initial delay to prevent rapid startup issues
    time.sleep(0.5)
    
    # Header
    st.title("Underrated Artist Radar")
    st.subheader("A&R Discovery Tool - Identifying High-Potential Emerging Artists")
    st.markdown("---")
    
    # Initialize BigQuery with delays
    # Get project ID from environment variables (local development)
    # For Streamlit Cloud, secrets are handled via .streamlit/secrets.toml
    project_id = os.getenv('BIGQUERY_PROJECT_ID')
    dataset_id = os.getenv('BIGQUERY_DATASET_ID', 'artist_discovery')
    
    time.sleep(0.3)
    client = init_bigquery_client()
    
    if client is None:
        st.warning("BigQuery client initialization failed. Please check your credentials.")
        st.stop()
    
    # Load data with delay
    time.sleep(0.2)
    df = load_artist_scores(client, project_id, dataset_id)
    
    if df.empty:
        st.warning("No data available. Please ensure:")
        st.markdown("1. Data has been collected and uploaded to BigQuery")
        st.markdown("2. artist_scores table has been created (run sql/create_artist_scores.sql)")
        st.stop()
    
    # Sidebar
    st.sidebar.header("Data Collection")
    
    # Refresh button at the top
    if st.sidebar.button("Refresh Dashboard", use_container_width=True):
        st.sidebar.success("Dashboard refreshed!")
        st.rerun()
    
    st.sidebar.markdown("**Current Dataset:**")
    st.sidebar.metric("Total Artists", len(df))
    st.sidebar.metric("With Instagram", df['instagram_followers'].notna().sum())
    st.sidebar.metric("With YouTube", df['youtube_subscribers'].notna().sum())
    
    st.sidebar.markdown("---")
    st.sidebar.subheader("Collect More Data")
    
    # Main button to discover and scrape more artists
    st.sidebar.caption("- Click to refresh current artist's metrics and discover new artists")
    if st.sidebar.button("Refresh Artist Database", use_container_width=True, type="primary"):
        project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        progress_container = st.sidebar.container()
        
        with progress_container:
            st.info("Starting data collection pipeline...")
            
            # Step 1: Collect Last.fm artists
            with st.spinner("Step 1/4: Collecting Last.fm data (30-60 min)..."):
                try:
                    result1 = subprocess.run(
                        [sys.executable, "scripts/collect_lastfm_artists.py"],
                        capture_output=True,
                        text=True,
                        timeout=3600,
                        cwd=project_root
                    )
                    if result1.returncode == 0:
                        st.success("Last.fm collection complete!")
                    else:
                        st.error(f"Last.fm Error: {result1.stderr[:300] if result1.stderr else 'Unknown error'}")
                        st.stop()
                except subprocess.TimeoutExpired:
                    st.warning("Last.fm collection timed out. Check terminal.")
                    st.stop()
                except Exception as e:
                    st.error(f"Last.fm Error: {str(e)[:200]}")
                    st.stop()
            
            # Step 2: Collect Instagram data
            with st.spinner("Step 2/4: Collecting Instagram data (10-20 min)..."):
                try:
                    result2 = subprocess.run(
                        [sys.executable, "scripts/collect_instagram_data.py"],
                        capture_output=True,
                        text=True,
                        timeout=1200,
                        cwd=project_root
                    )
                    if result2.returncode == 0:
                        st.success("Instagram collection complete!")
                    else:
                        st.warning(f"Instagram warning: {result2.stderr[:300] if result2.stderr else 'Some artists may be missing Instagram data'}")
                except subprocess.TimeoutExpired:
                    st.warning("Instagram collection timed out. Continuing with available data...")
                except Exception as e:
                    st.warning(f"Instagram Error: {str(e)[:200]}. Continuing...")
            
            # Step 3: Collect YouTube data
            with st.spinner("Step 3/4: Collecting YouTube data (5-10 min)..."):
                try:
                    result3 = subprocess.run(
                        [sys.executable, "scripts/collect_youtube_data.py"],
                        capture_output=True,
                        text=True,
                        timeout=600,
                        cwd=project_root
                    )
                    if result3.returncode == 0:
                        st.success("YouTube collection complete!")
                    else:
                        st.warning(f"YouTube warning: {result3.stderr[:300] if result3.stderr else 'Some artists may be missing YouTube data'}")
                except subprocess.TimeoutExpired:
                    st.warning("YouTube collection timed out. Continuing...")
                except Exception as e:
                    st.warning(f"YouTube Error: {str(e)[:200]}. Continuing...")
            
            # Step 4: Merge and upload to BigQuery
            with st.spinner("Step 4/4: Merging data and uploading to BigQuery..."):
                try:
                    result4 = subprocess.run(
                        [sys.executable, "scripts/merge_and_upload_bigquery.py"],
                        capture_output=True,
                        text=True,
                        timeout=300,
                        cwd=project_root
                    )
                    if result4.returncode == 0:
                        st.success("Data merged and uploaded to BigQuery!")
                        st.info("⚠️ Next: Run SQL query in BigQuery Console to refresh artist_scores table, then refresh this dashboard.")
                    else:
                        st.error(f"Merge Error: {result4.stderr[:300] if result4.stderr else 'Unknown error'}")
                except Exception as e:
                    st.error(f"Merge Error: {str(e)[:200]}")
        
        st.sidebar.success("Data collection pipeline complete! Refresh dashboard to see new artists.")
    
    st.sidebar.markdown("---")
    
    # Separate button for Instagram verification only
    if st.sidebar.button("Verify Instagram Handles (YouTube)", use_container_width=True):
        project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        with st.sidebar:
            with st.spinner("Verifying Instagram handles from YouTube... This may take 10-20 minutes."):
                try:
                    result = subprocess.run(
                        [sys.executable, "scripts/find_instagram_from_youtube.py"],
                        capture_output=True,
                        text=True,
                        timeout=1200,
                        cwd=project_root
                    )
                    if result.returncode == 0:
                        st.success("Instagram verification complete!")
                        st.info("Run merge script to update data.")
                    else:
                        st.error(f"Error: {result.stderr[:500] if result.stderr else 'Unknown error'}")
                except subprocess.TimeoutExpired:
                    st.warning("Verification still running. Check terminal for progress.")
                except Exception as e:
                    st.error(f"Error: {str(e)[:200]}")
    
    st.sidebar.markdown("---")
    st.sidebar.header("Filters")
    
    # Two-level dropdown filter system
    # First dropdown: Filter by Potential Level
    potential_options = ["All Artists", "High Potential", "Medium Potential", "Low Potential"]
    potential_filter = st.sidebar.selectbox(
        "Show Artists:",
        options=potential_options,
        index=0
    )
    
    # Apply potential filter first
    if potential_filter == "All Artists":
        filtered_df = df.copy()
    else:
        growth_value = potential_filter.replace(" Potential", "")
        filtered_df = df[df['growth_potential'] == growth_value].copy()
    
    # Second dropdown: Select Artist for Detail View (updates based on first filter)
    artist_names = sorted(filtered_df['artist_name'].unique())
    
    # Find best default artist (highest priority_score with complete data)
    if len(filtered_df) > 0:
        complete_data = filtered_df[
            filtered_df['instagram_followers'].notna() & 
            filtered_df['youtube_subscribers'].notna() &
            filtered_df['lastfm_listeners'].notna()
        ].sort_values('priority_score', ascending=False)
        
        if len(complete_data) > 0:
            default_artist_name = complete_data.iloc[0]['artist_name']
        else:
            default_artist_name = filtered_df.sort_values('priority_score', ascending=False).iloc[0]['artist_name']
        
        default_index = artist_names.index(default_artist_name) if default_artist_name in artist_names else 0
    else:
        default_index = 0
    
    selected_artist_detail = st.sidebar.selectbox(
        "View Artist Details:",
        options=artist_names,
        index=default_index if len(artist_names) > 0 else 0,
        key="detail_artist_selector"
    )
    
    # Genre filter
    available_genres = sorted(filtered_df['primary_genre'].unique())
    selected_genres = st.sidebar.multiselect(
        "Select Genres",
        available_genres,
        default=available_genres[:min(3, len(available_genres))]
    )
    
    # Apply genre filter
    if selected_genres:
        filtered_df = filtered_df[filtered_df['primary_genre'].isin(selected_genres)]
    
    # Follower range filter (only if Instagram data exists)
    if filtered_df['instagram_followers'].notna().any():
        min_instagram = st.sidebar.slider(
            "Minimum Instagram Followers",
            int(filtered_df['instagram_followers'].min()),
            int(filtered_df['instagram_followers'].max()),
            int(filtered_df['instagram_followers'].min())
        )
        
        max_instagram = st.sidebar.slider(
            "Maximum Instagram Followers",
            int(filtered_df['instagram_followers'].min()),
            int(filtered_df['instagram_followers'].max()),
            int(filtered_df['instagram_followers'].max())
        )
        
        filtered_df = filtered_df[
            (filtered_df['instagram_followers'].isna()) |
            ((filtered_df['instagram_followers'] >= min_instagram) &
             (filtered_df['instagram_followers'] <= max_instagram))
        ]
    
    # Key metrics
    st.header("Overview Metrics")
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.metric("Total Artists Analyzed", len(filtered_df))
    
    with col2:
        high_growth = len(filtered_df[filtered_df['growth_potential'] == 'High']) if len(filtered_df) > 0 else 0
        st.metric("High Potential Artists", high_growth)
    
    with col3:
        genres_covered = len(filtered_df['primary_genre'].unique()) if len(filtered_df) > 0 else 0
        st.metric("Genres Covered", genres_covered)
    
    st.markdown("---")
    
    # Top artists table
    st.header("Top Underrated Artists")
    st.markdown("Artists ranked by A&R Priority Score (higher = more underrated)")
    
    # Prepare display dataframe with formatted columns
    display_df = filtered_df.copy().sort_values('priority_score', ascending=False)
    
    # Add rank column
    display_df['Rank'] = range(1, len(display_df) + 1)
    
    # Format artist name with Last.fm listeners
    def format_artist_name(row):
        artist_name = row['artist_name']
        lastfm_listeners = int(row['lastfm_listeners']) if pd.notna(row.get('lastfm_listeners')) else 0
        return f"{artist_name} ({lastfm_listeners:,} Last.fm Listeners)"
    
    # Format Instagram metrics
    def format_instagram_metrics(row):
        if pd.notna(row.get('instagram_handle')):
            insta_followers = int(row['instagram_followers']) if pd.notna(row.get('instagram_followers')) else 0
            return f"@{row['instagram_handle']} ({insta_followers:,} Followers)"
        return "N/A"
    
    # Format growth potential with text labels
    def format_growth_potential(growth):
        if pd.isna(growth):
            return "N/A"
        return growth
    
    display_df['Rank'] = display_df['Rank']
    display_df['Artist Name (Last.fm)'] = display_df.apply(format_artist_name, axis=1)
    display_df['Instagram Metrics'] = display_df.apply(format_instagram_metrics, axis=1)
    display_df['Genre'] = display_df['primary_genre']
    display_df['Priority Score'] = display_df['priority_score'].round(2)
    display_df['Growth Potential'] = display_df['growth_potential'].apply(format_growth_potential)
    display_df['Underrated Ratio'] = display_df['underrated_ratio'].round(2)
    
    display_cols = ['Rank', 'Artist Name (Last.fm)', 'Instagram Metrics', 'Genre', 'Priority Score', 'Growth Potential', 'Underrated Ratio']
    
    display_df = display_df[display_cols].head(50)
    
    # Highlight top 3 artists
    def highlight_top3(row):
        if row['Rank'] <= 3:
            return ['background-color: #fff9e6'] * len(row)
        return [''] * len(row)
    
    st.dataframe(display_df.style.apply(highlight_top3, axis=1), use_container_width=True, hide_index=True)
    
    # Featured Artist Analysis
    st.markdown("---")
    st.header("Featured Artist Analysis")
    
    # Artist Comparison Chart - Uses selected artist from sidebar dropdown
    st.subheader("Artist Platform Comparison")
    st.markdown("Compare artist performance across Last.fm, Instagram, and YouTube platforms. Metrics are normalized (0-100 scale) for fair comparison.")
    
    # Use artist selected from sidebar dropdown
    # Ensure selected artist is still in filtered dataset after all filters
    if len(filtered_df) > 0:
        if selected_artist_detail in filtered_df['artist_name'].values:
            selected_artist = selected_artist_detail
        else:
            # If selected artist was filtered out, use first artist in filtered dataset
            selected_artist = filtered_df.iloc[0]['artist_name']
        
        artist_data = filtered_df[filtered_df['artist_name'] == selected_artist].iloc[0]
        
        # Normalize metrics for comparison (0-100 scale)
        max_listeners = filtered_df['lastfm_listeners'].max()
        max_followers = filtered_df['instagram_followers'].max() if filtered_df['instagram_followers'].notna().any() else 1
        max_subscribers = filtered_df['youtube_subscribers'].max() if filtered_df['youtube_subscribers'].notna().any() else 1
        
        # Calculate normalized scores
        lastfm_score = (artist_data['lastfm_listeners'] / max_listeners * 100) if pd.notna(artist_data['lastfm_listeners']) else 0
        instagram_score = (artist_data['instagram_followers'] / max_followers * 100) if pd.notna(artist_data['instagram_followers']) else 0
        youtube_score = (artist_data['youtube_subscribers'] / max_subscribers * 100) if pd.notna(artist_data['youtube_subscribers']) else 0
        
        # Create comparison chart
        platforms = ['Last.fm', 'Instagram', 'YouTube']
        scores = [lastfm_score, instagram_score, youtube_score]
        colors = ['#1db954', '#E4405F', '#FF0000']
        
        fig_comparison = go.Figure()
        
        fig_comparison.add_trace(go.Bar(
            x=platforms,
            y=scores,
            marker_color=colors,
            text=[f"{score:.1f}%" for score in scores],
            textposition='outside',
            name='Normalized Score'
        ))
        
        fig_comparison.update_layout(
            title=f"Platform Performance Comparison: {selected_artist}",
            yaxis_title="Normalized Score (0-100)",
            xaxis_title="Platform",
            height=450,
            showlegend=False,
            yaxis=dict(range=[0, 110])
        )
        
        st.plotly_chart(fig_comparison, use_container_width=True)
        
        # Show raw metrics
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Last.fm Listeners", f"{int(artist_data['lastfm_listeners']):,}" if pd.notna(artist_data['lastfm_listeners']) else "N/A")
            st.metric("Last.fm Play Count", f"{int(artist_data['lastfm_playcount']):,}" if pd.notna(artist_data['lastfm_playcount']) else "N/A")
        with col2:
            st.metric("Instagram Followers", f"{int(artist_data['instagram_followers']):,}" if pd.notna(artist_data['instagram_followers']) else "N/A")
            # Format engagement rate more beautifully
            if pd.notna(artist_data['instagram_engagement_rate']):
                engagement = artist_data['instagram_engagement_rate']
                if engagement < 1:
                    engagement_display = f"{engagement:.3f}%"
                elif engagement < 10:
                    engagement_display = f"{engagement:.2f}%"
                else:
                    engagement_display = f"{engagement:.1f}%"
                st.metric("Instagram Engagement Rate", engagement_display)
            else:
                st.metric("Instagram Engagement Rate", "N/A")
        with col3:
            st.metric("YouTube Subscribers", f"{int(artist_data['youtube_subscribers']):,}" if pd.notna(artist_data['youtube_subscribers']) else "N/A")
            st.metric("YouTube Total Views", f"{int(artist_data['youtube_total_views']):,}" if pd.notna(artist_data['youtube_total_views']) else "N/A")
    else:
        st.info("No artists available for comparison.")
    
    # Additional Analysis Section
    st.markdown("---")
    st.header("Additional Analysis")
    
    # Genre Distribution Chart (if we have enough genres)
    if len(filtered_df['primary_genre'].unique()) > 2:
        st.subheader("Artists by Genre")
        genre_counts = filtered_df['primary_genre'].value_counts()
        genre_df = pd.DataFrame({
            'Genre': genre_counts.index,
            'Count': genre_counts.values
        })
        fig_genre = px.bar(
            genre_df,
            x='Genre',
            y='Count',
            title="Genre Distribution",
            labels={'Genre': 'Genre', 'Count': 'Number of Artists'},
            color='Count',
            color_continuous_scale='Blues'
        )
        fig_genre.update_layout(height=400, showlegend=False)
        st.plotly_chart(fig_genre, use_container_width=True)
        st.markdown("---")
    
    # Growth Potential Summary (as a table, not chart)
    if filtered_df['growth_potential'].notna().any():
        st.subheader("Growth Potential Summary")
        growth_summary = filtered_df.groupby('growth_potential').agg({
            'artist_name': 'count',
            'priority_score': 'mean',
            'underrated_ratio': 'mean'
        }).reset_index()
        growth_summary.columns = ['Growth Potential', 'Artist Count', 'Avg Priority Score', 'Avg Underrated Ratio']
        growth_summary = growth_summary.sort_values('Avg Priority Score', ascending=False)
        growth_summary['Avg Priority Score'] = growth_summary['Avg Priority Score'].round(2)
        growth_summary['Avg Underrated Ratio'] = growth_summary['Avg Underrated Ratio'].round(2)
        st.dataframe(growth_summary, use_container_width=True, hide_index=True)
        st.markdown("---")
    
    # Underrated Ratio Distribution (histogram)
    ratio_df = filtered_df[filtered_df['underrated_ratio'].notna()]
    if len(ratio_df) > 0:
        st.subheader("Underrated Ratio Distribution")
        st.markdown("Shows distribution of Instagram Followers to Last.fm Listeners ratio. Higher ratios indicate artists with strong social media presence relative to streaming performance.")
        fig_ratio = px.histogram(
            ratio_df,
            x='underrated_ratio',
            nbins=20,
            title="Distribution of Underrated Ratios",
            labels={
                'underrated_ratio': 'Underrated Ratio (Instagram Followers / Last.fm Listeners)', 
                'count': 'Number of Artists'
            },
            color_discrete_sequence=['#1f77b4']
        )
        fig_ratio.add_vline(x=1.0, line_dash="dash", line_color="red", 
                           annotation_text="Ratio = 1.0 (Equal presence)", 
                           annotation_position="top")
        fig_ratio.update_layout(height=500)
        st.plotly_chart(fig_ratio, use_container_width=True)
    
    # Footer
    st.markdown("---")
    
    # Data collection section
    with st.expander("Guide for Manually Collect More Artist"):
        st.markdown("""
        **Note:** Data collection scripts are available in the `scripts/` folder.
        
        To add more artists:
        1. Run `python3 scripts/collect_lastfm_artists.py` to discover more artists
        2. Run `python3 scripts/collect_instagram_data.py` for Instagram data
        3. Run `python3 scripts/collect_youtube_data.py` for YouTube data
        4. Run `python3 scripts/merge_and_upload_bigquery.py` to update BigQuery
        5. Re-run the SQL query in BigQuery Console to refresh scores
        6. Refresh this dashboard to see new data
        
        **API Rate Limits:**
        - Instagram: ~10 seconds per artist
        - YouTube: 10,000 units/day
        - Last.fm: No strict limits
        """)
    
    st.markdown("---")
    st.markdown("**Data Sources:** Last.fm API, Instagram (via Instaloader), YouTube Data API v3")
    st.markdown("**Last Updated:** " + str(df['collection_date'].max() if 'collection_date' in df.columns else 'N/A'))


if __name__ == "__main__":
    main()
