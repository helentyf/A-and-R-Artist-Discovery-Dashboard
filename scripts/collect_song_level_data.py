"""
Song-Level Data Collection

Collects track-level data from Last.fm, YouTube video data, and Instagram post data
for comprehensive song performance analysis.
"""

import requests
import pandas as pd
import os
import time
from datetime import datetime
from dotenv import load_dotenv
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import instaloader
import re

load_dotenv()

LASTFM_API_KEY = os.getenv('LASTFM_API_KEY')
YOUTUBE_API_KEY = os.getenv('YOUTUBE_API_KEY')
BASE_URL = 'http://ws.audioscrobbler.com/2.0/'

# Initialize YouTube client
youtube = build('youtube', 'v3', developerKey=YOUTUBE_API_KEY)

# Initialize Instagram loader
L = instaloader.Instaloader(
    download_pictures=False,
    download_videos=False,
    download_video_thumbnails=False,
    download_geotags=False,
    download_comments=False,
    save_metadata=False,
    compress_json=False
)


def get_lastfm_top_tracks(artist_name, limit=10):
    """
    Get top tracks for an artist from Last.fm.
    
    Args:
        artist_name: Name of the artist
        limit: Number of top tracks to fetch
        
    Returns:
        List of dictionaries with track data
    """
    params = {
        'method': 'artist.gettoptracks',
        'artist': artist_name,
        'api_key': LASTFM_API_KEY,
        'limit': limit,
        'format': 'json'
    }
    
    try:
        response = requests.get(BASE_URL, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()
        
        if 'toptracks' not in data or 'track' not in data['toptracks']:
            return []
        
        tracks = []
        for track in data['toptracks']['track']:
            tracks.append({
                'track_name': track['name'],
                'lastfm_playcount': int(track.get('playcount', 0)),
                'lastfm_listeners': int(track.get('listeners', 0)),
                'lastfm_url': track.get('url', ''),
                'lastfm_rank': int(track.get('@attr', {}).get('rank', 0))
            })
        
        return tracks
    except Exception as e:
        print(f"Error fetching Last.fm tracks for {artist_name}: {e}")
        return []


def search_youtube_video(artist_name, track_name):
    """
    Search for YouTube video matching artist and track name.
    
    Args:
        artist_name: Name of the artist
        track_name: Name of the track
        
    Returns:
        Dictionary with video data or None
    """
    query = f"{artist_name} {track_name} official"
    
    try:
        search_response = youtube.search().list(
            q=query,
            part='id,snippet',
            type='video',
            maxResults=1,
            order='relevance'
        ).execute()
        
        if not search_response.get('items'):
            return None
        
        video = search_response['items'][0]
        video_id = video['id']['videoId']
        
        # Get video statistics
        video_response = youtube.videos().list(
            part='statistics,snippet,contentDetails',
            id=video_id
        ).execute()
        
        if not video_response.get('items'):
            return None
        
        video_data = video_response['items'][0]
        stats = video_data.get('statistics', {})
        snippet = video_data.get('snippet', {})
        
        # Parse published date
        published_at = snippet.get('publishedAt', '')
        published_date = None
        if published_at:
            try:
                published_date = datetime.fromisoformat(published_at.replace('Z', '+00:00')).date()
            except:
                pass
        
        return {
            'youtube_video_id': video_id,
            'youtube_video_title': snippet.get('title', ''),
            'youtube_views': int(stats.get('viewCount', 0)),
            'youtube_likes': int(stats.get('likeCount', 0)),
            'youtube_comments': int(stats.get('commentCount', 0)),
            'youtube_published_date': published_date,
            'youtube_url': f"https://www.youtube.com/watch?v={video_id}"
        }
    except HttpError as e:
        print(f"YouTube API error for {artist_name} - {track_name}: {e}")
        return None
    except Exception as e:
        print(f"Error fetching YouTube video for {artist_name} - {track_name}: {e}")
        return None


def get_instagram_posts_for_song(instagram_handle, track_name, published_date=None):
    """
    Get Instagram posts related to a song release.
    
    Args:
        instagram_handle: Instagram handle of the artist
        track_name: Name of the track
        published_date: YouTube video publish date to search around
        
    Returns:
        Dictionary with post data or None
    """
    if not instagram_handle:
        return None
    
    try:
        profile = instaloader.Profile.from_username(L.context, instagram_handle)
        
        # Search posts for track name in caption
        posts_found = []
        post_count = 0
        max_posts_to_check = 50
        
        for post in profile.get_posts():
            if post_count >= max_posts_to_check:
                break
            
            caption = post.caption.lower() if post.caption else ""
            track_name_lower = track_name.lower()
            
            # Check if post mentions track name
            if track_name_lower in caption or any(word in caption for word in track_name_lower.split() if len(word) > 3):
                # Check date proximity if published_date provided
                if published_date:
                    post_date = post.date.date()
                    days_diff = abs((post_date - published_date).days)
                    if days_diff > 30:  # Only consider posts within 30 days of release
                        post_count += 1
                        continue
                
                # Get follower count at time of post (approximate)
                # Note: Instagram API doesn't provide historical follower counts
                # We'll use current follower count as approximation
                posts_found.append({
                    'instagram_post_id': post.shortcode,
                    'instagram_post_date': post.date.date(),
                    'instagram_likes': post.likes,
                    'instagram_comments': post.comments,
                    'instagram_caption': post.caption[:200] if post.caption else '',
                    'instagram_url': f"https://www.instagram.com/p/{post.shortcode}/"
                })
                
                # Only get the most recent/relevant post
                if len(posts_found) >= 1:
                    break
            
            post_count += 1
            time.sleep(1)  # Rate limiting
        
        if posts_found:
            return posts_found[0]  # Return most relevant post
        return None
        
    except Exception as e:
        print(f"Error fetching Instagram posts for {instagram_handle} - {track_name}: {e}")
        return None


def collect_song_data_for_artist(artist_name, instagram_handle=None):
    """
    Collect song-level data for a specific artist.
    
    Args:
        artist_name: Name of the artist
        instagram_handle: Instagram handle (optional)
        
    Returns:
        List of dictionaries with song-level data
    """
    print(f"\nCollecting song data for: {artist_name}")
    
    # Get top tracks from Last.fm
    try:
        tracks = get_lastfm_top_tracks(artist_name, limit=10)
        if not tracks:
            print(f"  No tracks found for {artist_name}")
            return []
    except Exception as e:
        print(f"  Error getting tracks for {artist_name}: {e}")
        return []
    
    song_data = []
    
    for track in tracks:
        track_name = track['track_name']
        print(f"  Processing: {track_name}")
        
        # Get YouTube video data
        try:
            youtube_data = search_youtube_video(artist_name, track_name)
            time.sleep(0.5)  # Rate limiting
        except Exception as e:
            print(f"    Error getting YouTube data: {e}")
            youtube_data = None
        
        # Get Instagram post data if handle provided (skip for now due to rate limits)
        instagram_data = None
        # Temporarily disabled Instagram scraping to speed up collection
        # if instagram_handle and youtube_data and youtube_data.get('youtube_published_date'):
        #     instagram_data = get_instagram_posts_for_song(
        #         instagram_handle,
        #         track_name,
        #         youtube_data['youtube_published_date']
        #     )
        #     time.sleep(2)  # Instagram rate limiting
        
        # Combine all data
        song_record = {
            'artist_name': artist_name,
            'track_name': track_name,
            'lastfm_playcount': track['lastfm_playcount'],
            'lastfm_listeners': track['lastfm_listeners'],
            'lastfm_url': track['lastfm_url'],
            'lastfm_rank': track['lastfm_rank']
        }
        
        if youtube_data:
            song_record.update({
                'youtube_video_id': youtube_data.get('youtube_video_id'),
                'youtube_views': youtube_data.get('youtube_views', 0),
                'youtube_likes': youtube_data.get('youtube_likes', 0),
                'youtube_comments': youtube_data.get('youtube_comments', 0),
                'youtube_published_date': youtube_data.get('youtube_published_date'),
                'youtube_url': youtube_data.get('youtube_url')
            })
        else:
            song_record.update({
                'youtube_video_id': None,
                'youtube_views': None,
                'youtube_likes': None,
                'youtube_comments': None,
                'youtube_published_date': None,
                'youtube_url': None
            })
        
        if instagram_data:
            song_record.update({
                'instagram_post_id': instagram_data.get('instagram_post_id'),
                'instagram_post_date': instagram_data.get('instagram_post_date'),
                'instagram_likes': instagram_data.get('instagram_likes', 0),
                'instagram_comments': instagram_data.get('instagram_comments', 0),
                'instagram_url': instagram_data.get('instagram_url')
            })
        else:
            song_record.update({
                'instagram_post_id': None,
                'instagram_post_date': None,
                'instagram_likes': None,
                'instagram_comments': None,
                'instagram_url': None
            })
        
        song_record['collection_date'] = datetime.now().date()
        song_data.append(song_record)
    
    return song_data


def main():
    """Main function to collect song-level data."""
    # Load artist data to get list of artists with Instagram handles
    lastfm_path = 'data/raw/lastfm_artists_raw.csv'
    instagram_path = 'data/raw/instagram_data_raw.csv'
    
    if not os.path.exists(lastfm_path):
        print(f"Error: {lastfm_path} not found. Run collect_lastfm_artists.py first.")
        return
    
    try:
        lastfm_df = pd.read_csv(lastfm_path)
    except Exception as e:
        print(f"Error reading Last.fm data: {e}")
        return
    
    instagram_df = None
    if os.path.exists(instagram_path):
        try:
            instagram_df = pd.read_csv(instagram_path)
        except Exception as e:
            print(f"Warning: Could not load Instagram data: {e}")
    
    # Merge to get Instagram handles
    if instagram_df is not None and 'artist_id' in instagram_df.columns and 'instagram_handle' in instagram_df.columns:
        merged_df = lastfm_df.merge(
            instagram_df[['artist_id', 'instagram_handle']],
            on='artist_id',
            how='left'
        )
    else:
        merged_df = lastfm_df.copy()
        merged_df['instagram_handle'] = None
    
    # Limit to first 10 artists for faster initial collection
    print(f"\nCollecting song data for {min(len(merged_df), 10)} artists...")
    print("Note: This process may take 15-30 minutes due to API rate limits.")
    print("-" * 60)
    
    # Limit to first 10 artists for testing
    merged_df = merged_df.head(10)
    
    # Collect song data for each artist
    all_song_data = []
    
    for idx, row in merged_df.iterrows():
        artist_name = row['artist_name']
        instagram_handle = row.get('instagram_handle')
        
        if pd.notna(instagram_handle) and isinstance(instagram_handle, str):
            instagram_handle = instagram_handle.replace('@', '')
        else:
            instagram_handle = None
        
        print(f"\n[{idx + 1}/{len(merged_df)}] Processing: {artist_name}")
        songs = collect_song_data_for_artist(artist_name, instagram_handle)
        all_song_data.extend(songs)
        
        # Rate limiting between artists
        time.sleep(2)
        
        # Save progress every 3 artists
        if (idx + 1) % 3 == 0:
            if all_song_data:
                df = pd.DataFrame(all_song_data)
                output_path = 'data/raw/song_level_data_raw.csv'
                os.makedirs(os.path.dirname(output_path), exist_ok=True)
                df.to_csv(output_path, index=False)
                print(f"\nProgress saved: {len(all_song_data)} songs collected")
    
    # Save final data
    if all_song_data:
        df = pd.DataFrame(all_song_data)
        output_path = 'data/raw/song_level_data_raw.csv'
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        df.to_csv(output_path, index=False)
        print(f"\n{'='*60}")
        print(f"Song-level data collection complete!")
        print(f"Total songs collected: {len(all_song_data)}")
        print(f"Data saved to: {output_path}")
        print(f"{'='*60}")
    else:
        print("\nNo song data collected.")


if __name__ == "__main__":
    main()
