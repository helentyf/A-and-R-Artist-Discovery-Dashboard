"""
Last.fm API Connection Test

Tests Last.fm API connection and verifies credentials are working correctly.
"""

import requests
import os
from dotenv import load_dotenv

load_dotenv()

LASTFM_API_KEY = os.getenv('LASTFM_API_KEY')
BASE_URL = 'http://ws.audioscrobbler.com/2.0/'


def test_api_connection():
    """
    Test Last.fm API connection with a known artist.
    """
    if not LASTFM_API_KEY:
        print("Error: LASTFM_API_KEY not found in environment variables")
        print("Please set LASTFM_API_KEY in your .env file")
        return False
    
    print("Testing Last.fm API connection")
    print("-" * 60)
    
    test_artist = 'Aurora'
    
    params = {
        'method': 'artist.getinfo',
        'artist': test_artist,
        'api_key': LASTFM_API_KEY,
        'format': 'json'
    }
    
    try:
        response = requests.get(BASE_URL, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()
        
        if 'artist' in data:
            artist = data['artist']
            stats = artist.get('stats', {})
            
            print("API connection successful")
            print(f"\nTest Artist: {artist['name']}")
            print(f"Listeners: {stats.get('listeners', 'N/A'):,}")
            print(f"Play count: {stats.get('playcount', 'N/A'):,}")
            
            tags = artist.get('tags', {}).get('tag', [])
            if tags:
                print(f"Genres: {', '.join([tag['name'] for tag in tags[:5]])}")
            
            return True
        else:
            print("Error: Unexpected API response format")
            print(f"Response: {data}")
            return False
            
    except requests.exceptions.RequestException as e:
        print(f"Error connecting to Last.fm API: {e}")
        return False
    except Exception as e:
        print(f"Unexpected error: {e}")
        return False


def test_genre_search():
    """
    Test genre-based artist search.
    """
    if not LASTFM_API_KEY:
        return False
    
    print("\nTesting genre search")
    print("-" * 60)
    
    params = {
        'method': 'tag.gettopartists',
        'tag': 'jazz',
        'api_key': LASTFM_API_KEY,
        'format': 'json',
        'limit': 5
    }
    
    try:
        response = requests.get(BASE_URL, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()
        
        if 'topartists' in data and 'artist' in data['topartists']:
            artists = data['topartists']['artist'][:5]
            print(f"Found {len(artists)} artists for 'jazz' tag:")
            for artist in artists:
                print(f"  - {artist['name']}: {artist.get('listeners', 'N/A')} listeners")
            return True
        else:
            print("Error: No artists found in response")
            return False
            
    except Exception as e:
        print(f"Error testing genre search: {e}")
        return False


def main():
    """
    Run all API tests.
    """
    print("=" * 60)
    print("Last.fm API Connection Test")
    print("=" * 60)
    
    test1 = test_api_connection()
    test2 = test_genre_search()
    
    print("\n" + "=" * 60)
    if test1 and test2:
        print("All tests passed. API is ready to use.")
        return 0
    else:
        print("Some tests failed. Please check your API key and network connection.")
        return 1


if __name__ == "__main__":
    exit(main())
