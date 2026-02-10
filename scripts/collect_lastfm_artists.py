"""
Last.fm Artist Discovery Script

Discovers UK artists in target genres (jazz, folk, alternative, soul) using
Last.fm API. Collects listener counts, play counts, and genre classifications
for emerging artists.

Target: Artists with 10k-200k Last.fm listeners (emerging to mid-tier range)
"""

import requests
import pandas as pd
from dotenv import load_dotenv
import os
import time
from datetime import datetime
import hashlib

load_dotenv()

LASTFM_API_KEY = os.getenv('LASTFM_API_KEY')
BASE_URL = 'http://ws.audioscrobbler.com/2.0/'


def search_artists_by_genre(genre, limit=100):
    """
    Search for artists by genre tag using Last.fm API.
    
    Args:
        genre: Genre tag to search for
        limit: Maximum number of artists to return
        
    Returns:
        List of artist dictionaries with name and listener count
    """
    params = {
        'method': 'tag.gettopartists',
        'tag': genre,
        'api_key': LASTFM_API_KEY,
        'format': 'json',
        'limit': limit
    }
    
    try:
        response = requests.get(BASE_URL, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()
        
        if 'topartists' in data and 'artist' in data['topartists']:
            return data['topartists']['artist']
        return []
    except requests.exceptions.RequestException as e:
        print(f"Error searching genre {genre}: {e}")
        return []
    except KeyError as e:
        print(f"Unexpected API response format for {genre}: {e}")
        return []


def get_uk_artists(limit=200):
    """
    Get top artists from United Kingdom.
    
    Args:
        limit: Maximum number of artists to return
        
    Returns:
        List of artist dictionaries
    """
    params = {
        'method': 'geo.gettopartists',
        'country': 'united kingdom',
        'api_key': LASTFM_API_KEY,
        'format': 'json',
        'limit': limit
    }
    
    try:
        response = requests.get(BASE_URL, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()
        
        if 'topartists' in data and 'artist' in data['topartists']:
            return data['topartists']['artist']
        return []
    except requests.exceptions.RequestException as e:
        print(f"Error fetching UK artists: {e}")
        return []
    except KeyError:
        return []


def get_artist_details(artist_name):
    """
    Get detailed information for a specific artist.
    
    Args:
        artist_name: Name of the artist
        
    Returns:
        Dictionary with artist details or None if error
    """
    params = {
        'method': 'artist.getinfo',
        'artist': artist_name,
        'api_key': LASTFM_API_KEY,
        'format': 'json'
    }
    
    try:
        response = requests.get(BASE_URL, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()
        
        if 'artist' not in data:
            return None
        
        artist = data['artist']
        stats = artist.get('stats', {})
        
        listeners = int(stats.get('listeners', 0))
        playcount = int(stats.get('playcount', 0))
        
        tags = artist.get('tags', {}).get('tag', [])
        genres_list = [tag['name'] for tag in tags[:5]] if isinstance(tags, list) else []
        primary_genre = genres_list[0] if genres_list else 'unknown'
        
        playcount_per_listener = playcount / listeners if listeners > 0 else 0.0
        
        artist_id = hashlib.md5(artist_name.encode()).hexdigest()
        
        return {
            'artist_id': artist_id,
            'artist_name': artist['name'],
            'lastfm_listeners': listeners,
            'lastfm_playcount': playcount,
            'lastfm_playcount_per_listener': round(playcount_per_listener, 2),
            'lastfm_url': artist.get('url', ''),
            'genres': ', '.join(genres_list),
            'primary_genre': primary_genre
        }
    except requests.exceptions.RequestException as e:
        print(f"Error fetching artist {artist_name}: {e}")
        return None
    except (KeyError, ValueError, TypeError) as e:
        print(f"Error parsing data for {artist_name}: {e}")
        return None


def main():
    """
    Main execution function to discover and collect UK artists from Last.fm.
    """
    target_genres = ['jazz', 'folk', 'alternative', 'soul', 'indie', 'indie folk', 'british soul', 'uk jazz', 
                     'indie rock', 'singer-songwriter', 'british indie', 'acoustic', 'neo soul', 'jazz fusion']
    all_artists = []
    
    print("Starting Last.fm artist discovery")
    print(f"Target genres: {', '.join(target_genres)}")
    print(f"Geographic focus: United Kingdom")
    print("-" * 60)
    
    if not LASTFM_API_KEY:
        print("Error: LASTFM_API_KEY not found in environment variables")
        print("Please set LASTFM_API_KEY in your .env file")
        return
    
    for genre in target_genres:
        print(f"\nSearching artists for genre: {genre}")
        artists = search_artists_by_genre(genre, limit=100)
        
        if not artists:
            print(f"  No artists found for {genre}")
            continue
        
        print(f"  Found {len(artists)} artists")
        
        for artist in artists:
            artist_name = artist.get('name', '')
            listeners = int(artist.get('listeners', 0))
            
            # Collect all artists initially (no filter), filter later after getting detailed info
            # This ensures we get accurate listener counts from detailed API calls
            all_artists.append({
                'artist_name': artist_name,
                'lastfm_listeners': listeners,  # Initial count, will be updated with detailed info
                'source': f'genre:{genre}'
            })
        
        time.sleep(1)
    
    print("\nFetching UK top artists")
    uk_artists = get_uk_artists(limit=200)
    
    for artist in uk_artists:
        artist_name = artist.get('name', '')
        listeners = int(artist.get('listeners', 0))
        
        # Collect all UK artists, filter later
        all_artists.append({
            'artist_name': artist_name,
            'lastfm_listeners': listeners,  # Initial count, will be updated with detailed info
            'source': 'uk_top'
        })
    
    if not all_artists:
        print("No artists found matching criteria")
        return
    
    df = pd.DataFrame(all_artists)
    df = df.drop_duplicates(subset='artist_name')
    
    print(f"\nTotal unique artists found: {len(df)}")
    print("Fetching detailed artist information")
    
    detailed_artists = []
    
    for idx, row in df.iterrows():
        if (idx + 1) % 50 == 0:
            print(f"  Progress: {idx + 1}/{len(df)} artists processed")
        
        details = get_artist_details(row['artist_name'])
        
        if details:
            details['source'] = row['source']
            detailed_artists.append(details)
        
        time.sleep(0.5)
    
    if not detailed_artists:
        print("No detailed artist data collected")
        return
    
    final_df = pd.DataFrame(detailed_artists)
    
    initial_count = len(final_df)
    
    # Debug: Show listener count distribution
    if len(final_df) > 0:
        print(f"\nListener count range: {final_df['lastfm_listeners'].min():,} - {final_df['lastfm_listeners'].max():,}")
        print(f"Artists in 5k-150k range: {len(final_df[(final_df['lastfm_listeners'] >= 5000) & (final_df['lastfm_listeners'] <= 150000)])}")
        print(f"Artists above 150k: {len(final_df[final_df['lastfm_listeners'] > 150000])}")
        print(f"Artists below 5k: {len(final_df[final_df['lastfm_listeners'] < 5000])}")
    
    # Filter to emerging artists range (5k-200k listeners)
    # Increased back to 200k to include artists like Old Man Canyon
    final_df = final_df[
        (final_df['lastfm_listeners'] >= 5000) &
        (final_df['lastfm_listeners'] <= 200000)
    ]
    
    # Filter out known established/major label artists (but keep Old Man Canyon and similar)
    established_artists = ['coldplay', 'radiohead', 'adele', 'ed sheeran', 'arctic monkeys', 'the 1975',
                          'david bowie', 'pink floyd', 'the beatles', 'rolling stones']
    
    final_df = final_df[
        ~final_df['artist_name'].str.lower().isin([a.lower() for a in established_artists])
    ]
    
    print(f"\nFiltered from {initial_count} to {len(final_df)} artists (5k-200k listeners)")
    
    final_df['collection_date'] = datetime.now().date()
    final_df['collection_timestamp'] = datetime.now()
    
    output_path = 'data/raw/lastfm_artists_raw.csv'
    
    # Append to existing file instead of replacing
    if os.path.exists(output_path):
        existing_df = pd.read_csv(output_path)
        # Combine and deduplicate by artist_id
        combined_df = pd.concat([existing_df, final_df], ignore_index=True)
        combined_df = combined_df.drop_duplicates(subset='artist_id', keep='last')
        combined_df.to_csv(output_path, index=False)
        print(f"\nAppended {len(final_df)} new artists to existing {len(existing_df)} artists")
        print(f"Total unique artists after deduplication: {len(combined_df)}")
    else:
        final_df.to_csv(output_path, index=False)
    
    print(f"\nData saved to: {output_path}")
    print(f"Total artists collected: {len(final_df)}")
    
    print("\nSummary Statistics:")
    print(f"  Average listeners: {final_df['lastfm_listeners'].mean():.0f}")
    print(f"  Average play count: {final_df['lastfm_playcount'].mean():,.0f}")
    print(f"  Average plays per listener: {final_df['lastfm_playcount_per_listener'].mean():.1f}")
    print("\nGenre distribution:")
    genre_counts = final_df['primary_genre'].value_counts().head(10)
    for genre, count in genre_counts.items():
        print(f"  {genre}: {count}")


if __name__ == "__main__":
    main()
