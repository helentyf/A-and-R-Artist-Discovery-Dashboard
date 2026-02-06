# Underrated Artist Radar - A&R Discovery Tool

A data pipeline and interactive dashboard that identifies high-potential unsigned and emerging musical artists by analyzing their performance across Last.fm, Instagram, and YouTube. The tool flags artists who demonstrate strong social media engagement but are underrepresented in listening data, indicating they are undervalued and prime candidates for label signing.

# Main Aims

How can A&R teams discover emerging talent before mainstream breakthrough?
Which artists show strong social media momentum relative to streaming performance?
What metrics indicate high growth potential for label signing?

# Interactive Dashboard

**Live Dashboard:** [**Click here to view the interactive dashboard**](http://localhost:8501)

**Note:** This is a localhost URL - only accessible on your computer. To share with others, deploy to Streamlit Cloud (see SHARING_GUIDE.md) or use ngrok for temporary sharing.

The dashboard provides:
- Real-time filtering by genre, followers, engagement, and growth potential
- Top underrated artists table with comprehensive metrics
- Interactive visualizations: scatter plots, genre distribution, growth potential analysis
- One-click data collection to discover more artists
- Instagram handle verification via YouTube channel analysis

**To launch the dashboard:**
```bash
streamlit run dashboard/app.py
```
Then open [http://localhost:8501](http://localhost:8501) in your browser.

# Tools Used

1. **Python 3.10+** (pandas, requests, google-cloud-bigquery) - Data collection, processing, and analysis
2. **Google BigQuery** - Data warehouse for storing and querying artist metrics
3. **Streamlit** - Interactive web dashboard for visualization and exploration
4. **APIs:**
   - Last.fm API - Artist discovery and listening metrics
   - Instagram (via Instaloader) - Social media engagement data
   - YouTube Data API v3 - Channel statistics and video metrics
5. **SQL** - Scoring algorithms and analytical queries

# Project Summary

This project builds a complete data pipeline from collection to visualization, identifying underrated artists for A&R discovery. The pipeline collects data from multiple platforms, merges and scores artists based on cross-platform performance, and presents findings through an interactive dashboard.

Key outputs:
- Raw data files (CSV) for Last.fm, Instagram, and YouTube metrics
- BigQuery tables (`artist_raw_data`, `artist_scores`) with calculated metrics
- Interactive Streamlit dashboard with filtering and visualization capabilities
- Automated data collection pipeline with one-click execution

# Why Last.fm?

Last.fm is ideal for discovering emerging and underrated artists because:
- **Better coverage of small artists:** Last.fm's scrobbling system captures listening data from users across all platforms (Spotify, Apple Music, YouTube, etc.), providing broader visibility into emerging talent
- **Rich genre tagging:** Community-driven tags offer more granular genre classification than commercial platforms, perfect for niche discovery
- **Engagement depth metrics:** Play-per-listener ratio reveals how deeply fans engage with an artist's catalog, not just surface-level popularity
- **Free, unrestricted API access:** No approval processes or rate limit restrictions that delay data collection
- **Historical data:** Long-term listening trends help identify artists with sustained growth versus temporary spikes

# Data Sources

**Last.fm API:**
- Artist discovery via genre tags and geographic searches (UK focus)
- Listener counts (total unique listeners)
- Play counts (total scrobbles/plays)
- Play-per-listener ratio (engagement depth metric)
- Genre tags and classifications

**Instagram (Instaloader):**
- Follower counts and engagement metrics
- Average likes and comments per post
- Engagement rate calculation
- Profile verification via YouTube channel descriptions

**YouTube Data API v3:**
- Channel subscriber counts
- Total view counts
- Average views per video
- Channel description scraping for Instagram handle discovery

# Setup and Installation

**Prerequisites:** Python 3.10+, Google Cloud account, Last.fm API key (free), YouTube Data API key

1. Clone repository and install dependencies: `pip install -r requirements.txt`
2. Copy `.env.example` to `.env` and add your API credentials (Last.fm, YouTube, BigQuery project ID)
3. Set up BigQuery: Create project and dataset `artist_discovery`, authenticate with `gcloud auth application-default login`
4. Launch dashboard: `streamlit run dashboard/app.py` and open [http://localhost:8501](http://localhost:8501)

# Usage

**Quick Start:** Launch the dashboard (`streamlit run dashboard/app.py`) and use the "Discover & Scrape More Underrated Artists" button to run the complete pipeline automatically.

**Manual Execution:** Run scripts sequentially: `collect_lastfm_artists.py` → `collect_instagram_data.py` → `collect_youtube_data.py` → `merge_and_upload_bigquery.py`. Then run `generate_sql.py` and execute the SQL query in BigQuery Console to create the scored table.

# Methodology

## Scoring Algorithm

The composite A&R priority score combines multiple factors:

**Formula:**
```
Priority Score = (
    (Instagram Followers × 0.3) +
    (Instagram Engagement Rate × Instagram Followers × 0.4) +
    (YouTube Subscribers × 0.2) +
    (Last.fm Play-per-Listener × 1000 × 0.1)
) / Last.fm Listeners
```

Higher scores indicate artists with strong social media presence relative to listening data performance.

## Growth Potential Calculation

Artists are categorized based on Instagram-to-Last.fm ratio:

- **High:** `instagram_followers > lastfm_listeners * 2` (strong social media momentum)
- **Medium:** `instagram_followers > lastfm_listeners` (moderate growth potential)
- **Low:** `instagram_followers ≤ lastfm_listeners` (established streaming presence)

## Filtering Criteria

Artists are filtered to:
- Genres: jazz, folk, alternative, soul, indie, blues, rnb, nu jazz, funk
- Last.fm listeners: 5,000 - 200,000 (emerging range)
- Instagram followers: 1,000 - 100,000 (optional, for artists with Instagram data)
- Engagement rate: > 1.0% (filters fake followers)
- Data quality score: >= 0.3 (minimum completeness)

# Project Structure

```
UniversalMusic_DataAnalyst_project/
├── README.md
├── requirements.txt
├── .env.example
├── data/
│   └── raw/                    # Raw data files (CSV)
├── scripts/
│   ├── collect_lastfm_artists.py
│   ├── test_lastfm_api.py
│   ├── collect_instagram_data.py
│   ├── collect_youtube_data.py
│   ├── merge_and_upload_bigquery.py
│   ├── generate_sql.py
│   └── generate_insights.py
├── sql/
│   └── create_artist_scores.sql
├── dashboard/
│   └── app.py                  # Streamlit dashboard
└── .gitignore
```

# Key Features

- **Multi-platform data collection:** Automated scraping from Last.fm, Instagram, and YouTube
- **Cross-platform analysis:** Identifies artists with social media momentum but low streaming presence
- **Interactive dashboard:** Real-time filtering, visualization, and exploration
- **One-click data collection:** Automated pipeline execution from dashboard
- **Instagram verification:** YouTube channel description analysis for accurate handle matching
- **Scalable architecture:** BigQuery for efficient querying and analysis

# Limitations and Considerations

1. **Instagram Rate Limits:** Aggressive rate limiting requires 10+ second delays between requests
2. **Data Matching:** Artist name to Instagram handle matching uses YouTube verification; manual verification recommended for top candidates
3. **YouTube API Quota:** 10,000 units/day limit requires careful management
4. **Data Completeness:** Not all artists have data across all platforms (30-40% may have incomplete data)
5. **Geographic Focus:** Currently optimized for UK artists; global expansion requires additional filtering

# Future Enhancements

- Historical tracking: Weekly data collection to track growth over time
- Predictive modeling: BigQuery ML to predict artist growth trajectories
- TikTok integration: Add TikTok virality metrics
- Playlist analysis: Track which Last.fm tags and charts feature emerging artists
- Geographic analysis: Location-based discovery for regional UK scenes
- Sentiment analysis: Social media sentiment from Reddit/Twitter mentions

# Troubleshooting

## Dashboard Not Loading

- Ensure Streamlit is installed: `pip install streamlit`
- Check that BigQuery credentials are configured correctly
- Verify project ID matches Google Cloud Console

## Instagram Collection Issues

- **Rate Limiting:** Script includes automatic delays; increase if needed
- **Profile Not Found:** Artist name to handle matching may fail; use YouTube verification feature
- **Login Required:** Some profiles require login; update script with credentials if needed

## BigQuery Upload Errors

- **Credentials:** Ensure Application Default Credentials are set: `gcloud auth application-default login`
- **Project ID:** Verify project ID matches Google Cloud Console
- **Permissions:** Service account needs BigQuery Data Editor and Job User roles

## YouTube API Quota

- **Quota Exceeded:** Wait 24 hours or upgrade API quota
- **Optimization:** Filter to artists with existing Last.fm/Instagram data before YouTube collection

# License

This project is for portfolio and demonstration purposes.

# Contact

For questions or contributions, please open an issue in the repository.
