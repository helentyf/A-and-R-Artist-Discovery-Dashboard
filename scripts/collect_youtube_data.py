"""
YouTube Data Collection Script

Collects YouTube channel statistics for artists discovered via Last.fm. Uses YouTube
Data API v3 to find channels and retrieve subscriber counts, view counts, and video
statistics.

Note: YouTube API has quota limits (10,000 units/day). This script prioritizes
artists with existing Last.fm/Instagram data to maximize value.
"""

from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import pandas as pd
from dotenv import load_dotenv
import os
import time
from datetime import datetime

load_dotenv()

# Initialize YouTube API client
youtube_api_key = os.getenv('YOUTUBE_API_KEY')
if not youtube_api_key:
    raise ValueError("YOUTUBE_API_KEY not found in environment variables")

youtube = build('youtube', 'v3', developerKey=youtube_api_key)


def search_youtube_channel(artist_name):
    """
    Search for artist's YouTube channel with better verification.
    
    Args:
        artist_name: Name of the artist
        
    Returns:
        Channel ID if found and verified, None otherwise
    """
    try:
        # Search for channels
        request = youtube.search().list(
            part='snippet',
            q=f"{artist_name} music official",
            type='channel',
            maxResults=10
        )
        response = request.execute()
        
        if not response['items']:
            return None
        
        # Get channel IDs and their subscriber counts
        channel_candidates = []
        channel_ids = [item['id']['channelId'] for item in response['items']]
        
        # Get statistics for all candidates
        request = youtube.channels().list(
            part='statistics,snippet',
            id=','.join(channel_ids)
        )
        channels_response = request.execute()
        
        artist_name_lower = artist_name.lower()
        artist_words = [w for w in artist_name_lower.split() if w not in ['the', 'a', 'an', 'and'] and len(w) > 2]
        
        for channel in channels_response['items']:
            channel_id = channel['id']
            channel_title = channel['snippet']['title'].lower()
            channel_desc = channel['snippet'].get('description', '').lower()
            subscribers = int(channel['statistics'].get('subscriberCount', 0))
            
            # Skip channels with very low subscribers (likely wrong match)
            if subscribers < 500:
                continue
            
            # Score channels based on how well they match
            score = 0
            
            # High score if "official" in title
            if 'official' in channel_title:
                score += 10
            
            # High score if artist name words appear in channel title
            name_matches = sum(1 for word in artist_words if word in channel_title)
            score += name_matches * 5
            
            # Medium score if artist name words appear in description
            desc_matches = sum(1 for word in artist_words if word in channel_desc)
            score += desc_matches * 2
            
            # Prefer channels with more subscribers (but not too many - avoid major labels)
            if 500 <= subscribers <= 500000:
                score += 1
            
            channel_candidates.append({
                'id': channel_id,
                'subscribers': subscribers,
                'score': score,
                'title': channel_title
            })
        
        if not channel_candidates:
            return None
        
        # Sort by score (highest first), then by subscribers
        channel_candidates.sort(key=lambda x: (x['score'], x['subscribers']), reverse=True)
        
        # Return best match only if score is reasonable (at least some name match)
        best_match = channel_candidates[0]
        if best_match['score'] >= 5:  # Require at least some matching
            return best_match['id']
        
        return None
    except HttpError as e:
        if e.resp.status == 403:
            print(f"API quota exceeded or access denied")
        else:
            print(f"Error searching for {artist_name}: {e}")
        return None
    except Exception as e:
        print(f"Error searching for {artist_name}: {e}")
        return None


def get_channel_stats(channel_id):
    """
    Get comprehensive channel statistics.
    
    Args:
        channel_id: YouTube channel ID
        
    Returns:
        Dictionary with channel statistics or None if error
    """
    try:
        request = youtube.channels().list(
            part='statistics,contentDetails',
            id=channel_id
        )
        response = request.execute()
        
        if response['items']:
            stats = response['items'][0]['statistics']
            content_details = response['items'][0].get('contentDetails', {})
            
            subscriber_count = int(stats.get('subscriberCount', 0))
            view_count = int(stats.get('viewCount', 0))
            video_count = int(stats.get('videoCount', 0))
            
            # Filter out channels with very low subscribers (likely incorrect matches)
            # Also filter out channels with too many subscribers (likely major label/established artists)
            if subscriber_count < 500 or subscriber_count > 500000:
                return None
            
            # Calculate average views per video
            avg_views = view_count / video_count if video_count > 0 else 0
            
            return {
                'youtube_channel_id': channel_id,
                'youtube_subscribers': subscriber_count,
                'youtube_total_views': view_count,
                'youtube_video_count': video_count,
                'youtube_avg_views_per_video': round(avg_views, 0) if avg_views > 0 else 0
            }
        return None
    except HttpError as e:
        if e.resp.status == 403:
            print(f"API quota exceeded or access denied")
        else:
            print(f"Error fetching channel stats: {e}")
        return None
    except Exception as e:
        print(f"Error fetching channel stats: {e}")
        return None


def get_recent_video_stats(channel_id, max_videos=10):
    """
    Get statistics for recent videos to calculate average views.
    
    Args:
        channel_id: YouTube channel ID
        max_videos: Maximum number of recent videos to analyze
        
    Returns:
        Average views per video
    """
    try:
        # Get uploads playlist ID
        request = youtube.channels().list(
            part='contentDetails',
            id=channel_id
        )
        response = request.execute()
        
        if not response['items']:
            return 0
        
        uploads_playlist_id = response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
        
        # Get videos from uploads playlist
        request = youtube.playlistItems().list(
            part='contentDetails',
            playlistId=uploads_playlist_id,
            maxResults=max_videos
        )
        response = request.execute()
        
        video_ids = [item['contentDetails']['videoId'] for item in response['items']]
        
        if not video_ids:
            return 0
        
        # Get video statistics
        request = youtube.videos().list(
            part='statistics',
            id=','.join(video_ids)
        )
        response = request.execute()
        
        total_views = sum(int(video['statistics'].get('viewCount', 0)) for video in response['items'])
        avg_views = total_views / len(response['items']) if response['items'] else 0
        
        return round(avg_views, 0)
    except Exception as e:
        print(f"Error fetching recent video stats: {e}")
        return 0


def main():
    """
    Main execution function to collect YouTube data for artists.
    """
    # Load Last.fm artists
    lastfm_path = 'data/raw/lastfm_artists_raw.csv'
    
    if not os.path.exists(lastfm_path):
        print(f"Error: {lastfm_path} not found.")
        print("Please run collect_lastfm_artists.py first.")
        return
    
    df = pd.read_csv(lastfm_path)
    print(f"Loaded {len(df)} artists from Spotify data")
    
    # Optionally filter to artists with minimum Last.fm listeners to conserve quota
    # Uncomment the line below to only process artists with 5k+ Last.fm listeners
    # df = df[df['lastfm_listeners'] >= 5000]
    
    print(f"Processing {len(df)} artists")
    print("-" * 60)
    print("Note: YouTube API has quota limits (10,000 units/day)")
    print("Each search costs 100 units, each channel detail costs 1 unit")
    print(f"Estimated quota usage: {len(df) * 101} units")
    print("-" * 60)
    
    youtube_data = []
    quota_used = 0
    
    for idx, row in df.iterrows():
        artist_name = row['artist_name']
        artist_id = row['artist_id']
        
        if (idx + 1) % 10 == 0:
            print(f"Processing {idx + 1}/{len(df)}: {artist_name}")
        
        # Search for channel (costs 100 units)
        channel_id = search_youtube_channel(artist_name)
        quota_used += 100
        
        if channel_id:
            # Get channel stats (costs 1 unit)
            stats = get_channel_stats(channel_id)
            quota_used += 1
            
            if not stats:
                print(f"  Channel found but filtered out (low subscribers or verification failed)")
                continue
            
            if stats:
                # Get recent video stats for better average calculation
                avg_views = get_recent_video_stats(channel_id)
                stats['youtube_avg_views_per_video'] = avg_views
                quota_used += 2  # Approximate cost for video stats
                
                stats['artist_id'] = artist_id
                stats['artist_name'] = artist_name
                youtube_data.append(stats)
                print(f"  Verified channel: {stats['youtube_subscribers']:,} subscribers")
            else:
                print(f"  Channel found but filtered out (subscribers < 500 or verification failed)")
        else:
            print(f"  No YouTube channel found")
        
        # Rate limit protection
        time.sleep(1)
        
        # Warn if approaching quota limit
        if quota_used > 9000:
            print(f"\nWARNING: Approaching API quota limit ({quota_used} units used)")
            print("Consider stopping and resuming tomorrow, or upgrade API quota")
            break
    
    # Final save - append to existing data
    if youtube_data:
        final_df = pd.DataFrame(youtube_data)
        output_path = 'data/raw/youtube_data_raw.csv'
        
        # Append to existing file instead of replacing
        if os.path.exists(output_path):
            existing_df = pd.read_csv(output_path)
            # Combine and deduplicate by artist_id
            combined_df = pd.concat([existing_df, final_df], ignore_index=True)
            combined_df = combined_df.drop_duplicates(subset='artist_id', keep='last')
            combined_df.to_csv(output_path, index=False)
            print(f"\nAppended {len(final_df)} new YouTube channels to existing {len(existing_df)} channels")
            print(f"Total unique channels after deduplication: {len(combined_df)}")
        else:
            final_df.to_csv(output_path, index=False)
        
        print("\n" + "=" * 60)
        print("YouTube data collection complete")
        print("=" * 60)
        print(f"Total artists processed: {len(df)}")
        print(f"Artists with YouTube data: {len(final_df)}")
        print(f"Success rate: {len(final_df)/len(df)*100:.1f}%")
        print(f"API quota used: ~{quota_used} units")
        print(f"\nData saved to: {output_path}")
        
        # Summary statistics
        print("\nSummary Statistics:")
        print(f"  Average subscribers: {final_df['youtube_subscribers'].mean():.0f}")
        print(f"  Average total views: {final_df['youtube_total_views'].mean():,.0f}")
        print(f"  Average views per video: {final_df['youtube_avg_views_per_video'].mean():,.0f}")
    else:
        print("\nNo YouTube data collected. Check:")
        print("  1. YOUTUBE_API_KEY in .env file")
        print("  2. API quota limits")
        print("  3. Network connection")


if __name__ == "__main__":
    main()
