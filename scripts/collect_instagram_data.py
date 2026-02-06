"""
Instagram Data Collection Script

Scrapes Instagram metrics for artists discovered via Last.fm. Collects follower counts,
engagement rates, and post statistics to identify artists with strong social media
presence relative to their listening data performance.

Uses YouTube channel/video descriptions to find Instagram handles, then verifies
matches by checking if artist name appears in Instagram profile name.

Note: Instagram rate limits are aggressive. This script includes delays and checkpoint
saving to handle interruptions gracefully.
"""

import instaloader
import pandas as pd
import time
from datetime import datetime
import os
from pathlib import Path
import re
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from dotenv import load_dotenv

load_dotenv()


class InstagramCollector:
    """
    Handles Instagram data collection with rate limiting and error handling.
    """
    
    def __init__(self):
        """Initialize Instaloader instance."""
        self.L = instaloader.Instaloader(
            download_pictures=False,
            download_videos=False,
            download_video_thumbnails=False,
            download_geotags=False,
            download_comments=False,
            save_metadata=False,
            compress_json=False
        )
        
        # Initialize YouTube API for finding Instagram handles
        youtube_api_key = os.getenv('YOUTUBE_API_KEY')
        if youtube_api_key:
            self.youtube = build('youtube', 'v3', developerKey=youtube_api_key)
        else:
            self.youtube = None
        
        # Optional: Login for better rate limits (use throwaway account)
        # Uncomment and add credentials if needed:
        # self.L.login('username', 'password')
    
    def find_best_youtube_channel(self, artist_name):
        """
        Find the best YouTube channel for an artist (most subscribers).
        
        Args:
            artist_name: Name of the artist
            
        Returns:
            Tuple of (channel_id, subscribers) or (None, None)
        """
        if not self.youtube:
            return None, None
        
        try:
            # Search for channels
            request = self.youtube.search().list(
                part='snippet',
                q=f"{artist_name} music",
                type='channel',
                maxResults=10
            )
            response = request.execute()
            
            if not response['items']:
                return None, None
            
            # Get subscriber counts for all channels
            channel_ids = [item['id']['channelId'] for item in response['items']]
            
            request = self.youtube.channels().list(
                part='statistics',
                id=','.join(channel_ids)
            )
            channels_response = request.execute()
            
            channel_candidates = []
            for channel in channels_response['items']:
                channel_candidates.append({
                    'id': channel['id'],
                    'subscribers': int(channel['statistics'].get('subscriberCount', 0))
                })
            
            # Return channel with most subscribers
            if channel_candidates:
                best = max(channel_candidates, key=lambda x: x['subscribers'])
                return best['id'], best['subscribers']
            
            return None, None
        except Exception as e:
            return None, None
    
    def get_channel_description(self, channel_id):
        """Get YouTube channel description."""
        if not self.youtube:
            return None
        try:
            request = self.youtube.channels().list(part='snippet', id=channel_id)
            response = request.execute()
            if response['items']:
                return response['items'][0]['snippet'].get('description', '')
        except:
            pass
        return None
    
    def get_top_videos(self, channel_id, max_videos=5):
        """Get top videos from a channel."""
        if not self.youtube:
            return []
        try:
            request = self.youtube.channels().list(part='contentDetails', id=channel_id)
            response = request.execute()
            if not response['items']:
                return []
            uploads_playlist_id = response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
            request = self.youtube.playlistItems().list(
                part='contentDetails',
                playlistId=uploads_playlist_id,
                maxResults=max_videos
            )
            response = request.execute()
            return [item['contentDetails']['videoId'] for item in response['items']]
        except:
            return []
    
    def get_video_description(self, video_id):
        """Get video description."""
        if not self.youtube:
            return None
        try:
            request = self.youtube.videos().list(part='snippet', id=video_id)
            response = request.execute()
            if response['items']:
                return response['items'][0]['snippet'].get('description', '')
        except:
            pass
        return None
    
    def extract_instagram_handles(self, text):
        """Extract Instagram handles from text."""
        if not text:
            return []
        handles = set()
        # Pattern 1: instagram.com/username
        pattern1 = r'instagram\.com/([a-zA-Z0-9_.]+)'
        matches = re.findall(pattern1, text, re.IGNORECASE)
        for match in matches:
            handle = match.split('/')[0].split('?')[0]
            if handle and len(handle) > 1:
                handles.add(handle.lower())
        # Pattern 2: @username
        pattern2 = r'(?<![@\w])@([a-zA-Z0-9_.]+)(?![@\w])'
        matches = re.findall(pattern2, text)
        for match in matches:
            if len(match) > 1 and '.' not in match:
                handles.add(match.lower())
        # Pattern 3: "Instagram: username"
        pattern3 = r'(?:instagram|ig)[\s:]+([a-zA-Z0-9_.]+)'
        matches = re.findall(pattern3, text, re.IGNORECASE)
        for match in matches:
            if len(match) > 1:
                handles.add(match.lower())
        return list(handles)
    
    def verify_instagram_match(self, handle, artist_name):
        """
        Verify if Instagram handle belongs to the artist.
        Checks if artist name appears in Instagram profile name or bio.
        
        Args:
            handle: Instagram handle
            artist_name: Artist name to match
            
        Returns:
            True if verified, False otherwise
        """
        try:
            profile = instaloader.Profile.from_username(self.L.context, handle)
            profile_name = (profile.full_name or '').lower()
            profile_bio = (profile.biography or '').lower()
            artist_lower = artist_name.lower()
            artist_words = [w for w in artist_lower.split() if w not in ['the', 'a', 'an', 'and', 'or'] and len(w) > 2]
            name_match = any(word in profile_name for word in artist_words)
            bio_match = any(word in profile_bio for word in artist_words)
            music_keywords = ['music', 'artist', 'band', 'musician', 'singer', 'songwriter']
            has_music_keyword = any(keyword in profile_name or keyword in profile_bio for keyword in music_keywords)
            has_reasonable_followers = profile.followers >= 5000
            return (name_match or bio_match or has_music_keyword) and has_reasonable_followers
        except:
            return False
    
    def find_instagram_handle_from_youtube(self, artist_name):
        """
        Find Instagram handle from YouTube channel/video descriptions.
        
        Args:
            artist_name: Name of the artist
            
        Returns:
            Instagram handle if found and verified, None otherwise
        """
        if not self.youtube:
            return None
        
        try:
            # Find best YouTube channel
            channel_id, subscribers = self.find_best_youtube_channel(artist_name)
            if not channel_id:
                return None
            
            # Check channel description
            channel_desc = self.get_channel_description(channel_id)
            if channel_desc:
                handles = self.extract_instagram_handles(channel_desc)
                for handle in handles:
                    if self.verify_instagram_match(handle, artist_name):
                        return handle
            
            # Check top videos
            video_ids = self.get_top_videos(channel_id, max_videos=5)
            for video_id in video_ids:
                video_desc = self.get_video_description(video_id)
                if video_desc:
                    handles = self.extract_instagram_handles(video_desc)
                    for handle in handles:
                        if self.verify_instagram_match(handle, artist_name):
                            return handle
                time.sleep(0.5)
        except Exception as e:
            pass
        
        return None
    
    def find_instagram_handle(self, artist_name):
        """
        Attempt to find Instagram handle using multiple methods.
        
        Priority:
        1. YouTube channel/video descriptions (most reliable)
        2. Name-based matching (fallback)
        
        Args:
            artist_name: Name of the artist
            
        Returns:
            Instagram username (handle) or None
        """
        # Method 1: Try YouTube descriptions first
        handle = self.find_instagram_handle_from_youtube(artist_name)
        if handle:
            return handle
        
        # Method 2: Fallback to name-based matching
        handle_base = artist_name.lower().replace(' ', '').replace('.', '').replace('-', '')
        variations = [
            handle_base,
            handle_base.replace('the', ''),
            f"{handle_base}music",
            f"{handle_base}official"
        ]
        
        for variation in variations:
            if self._profile_exists(variation):
                # Verify the match
                if self.verify_instagram_match(variation, artist_name):
                    return variation
        
        return None
    
    def _profile_exists(self, username):
        """
        Check if an Instagram profile exists.
        
        Args:
            username: Instagram username to check
            
        Returns:
            True if profile exists, False otherwise
        """
        try:
            profile = instaloader.Profile.from_username(self.L.context, username)
            return profile is not None
        except:
            return False
    
    def get_instagram_metrics(self, username):
        """
        Collect comprehensive Instagram metrics for a profile.
        
        Args:
            username: Instagram username (handle)
            
        Returns:
            Dictionary with Instagram metrics or None if error
        """
        try:
            profile = instaloader.Profile.from_username(self.L.context, username)
            
            # Get last 12 posts for engagement calculation
            posts = []
            post_count = 0
            max_posts = 12
            
            for post in profile.get_posts():
                posts.append(post)
                post_count += 1
                if post_count >= max_posts:
                    break
            
            if len(posts) == 0:
                return None
            
            # Calculate engagement metrics
            total_likes = sum(post.likes for post in posts)
            total_comments = sum(post.comments for post in posts)
            avg_likes = total_likes / len(posts)
            avg_comments = total_comments / len(posts)
            
            # Engagement rate: (avg likes + avg comments) / followers * 100
            if profile.followers > 0:
                engagement_rate = ((avg_likes + avg_comments) / profile.followers) * 100
            else:
                engagement_rate = 0.0
            
            # Count reels (videos)
            reels_count = sum(1 for post in posts if post.is_video)
            
            # Filter out suspiciously low-follower accounts (likely fake matches)
            # Real artist accounts should have at least 1000 followers
            if profile.followers < 5000:
                return None
            
            return {
                'instagram_handle': username,
                'instagram_followers': profile.followers,
                'instagram_following': profile.followees,
                'instagram_posts': profile.mediacount,
                'instagram_avg_likes': round(avg_likes, 2),
                'instagram_avg_comments': round(avg_comments, 2),
                'instagram_engagement_rate': round(engagement_rate, 2),
                'instagram_reels_count': reels_count,
                'instagram_verified': profile.is_verified
            }
        except instaloader.exceptions.ProfileNotExistsException:
            return None
        except instaloader.exceptions.LoginRequiredException:
            print("Login required for this profile. Consider logging in.")
            return None
        except Exception as e:
            print(f"Error fetching Instagram data for {username}: {e}")
            return None


def main():
    """
    Main execution function to collect Instagram data for Spotify artists.
    """
    # Load Last.fm artists
    lastfm_path = 'data/raw/lastfm_artists_raw.csv'
    
    if not os.path.exists(lastfm_path):
        print(f"Error: {lastfm_path} not found.")
        print("Please run collect_lastfm_artists.py first.")
        return
    
    df = pd.read_csv(lastfm_path)
    print(f"Loaded {len(df)} artists from Spotify data")
    print("-" * 60)
    
    # Initialize collector
    collector = InstagramCollector()
    
    instagram_data = []
    checkpoint_interval = 50
    
    print("\nStarting Instagram data collection...")
    print("Note: This process is slow due to Instagram rate limits.")
    print(f"Estimated time: {len(df) * 10 / 3600:.1f} hours")
    print("-" * 60)
    
    for idx, row in df.iterrows():
        artist_name = row['artist_name']
        artist_id = row['artist_id']
        
        print(f"\n[{idx + 1}/{len(df)}] Processing: {artist_name}")
        
        # Attempt to find Instagram handle (tries YouTube first, then name matching)
        print(f"  Searching for Instagram handle...")
        handle = collector.find_instagram_handle(artist_name)
        
        if handle:
            print(f"  Found handle: @{handle}")
            metrics = collector.get_instagram_metrics(handle)
            if metrics:
                metrics['artist_id'] = artist_id
                metrics['artist_name'] = artist_name
                instagram_data.append(metrics)
                print(f"  Collected data: @{handle} ({metrics['instagram_followers']:,} followers, {metrics['instagram_engagement_rate']:.1f}% engagement)")
            else:
                print(f"  Handle found but data collection failed (may be low followers or private)")
        else:
            print(f"  Could not find verified Instagram handle")
        
        # IMPORTANT: Respect rate limits - wait 10 seconds between requests
        # (YouTube API calls add extra time, so this is sufficient)
        time.sleep(10)
        
        # Save checkpoint every N artists
        if (idx + 1) % checkpoint_interval == 0:
            checkpoint_df = pd.DataFrame(instagram_data)
            checkpoint_path = f'data/raw/instagram_checkpoint_{idx + 1}.csv'
            checkpoint_df.to_csv(checkpoint_path, index=False)
            print(f"\n  Checkpoint saved: {checkpoint_path}")
            print(f"  Artists with Instagram data: {len(instagram_data)}/{idx + 1}\n")
    
    # Final save - append to existing data
    if instagram_data:
        final_df = pd.DataFrame(instagram_data)
        output_path = 'data/raw/instagram_data_raw.csv'
        
        # Append to existing file instead of replacing
        if os.path.exists(output_path):
            existing_df = pd.read_csv(output_path)
            # Combine and deduplicate by artist_id
            combined_df = pd.concat([existing_df, final_df], ignore_index=True)
            combined_df = combined_df.drop_duplicates(subset='artist_id', keep='last')
            combined_df.to_csv(output_path, index=False)
            print(f"\nAppended {len(final_df)} new Instagram profiles to existing {len(existing_df)} profiles")
            print(f"Total unique profiles after deduplication: {len(combined_df)}")
        else:
            final_df.to_csv(output_path, index=False)
        
        print("\n" + "=" * 60)
        print("Instagram data collection complete")
        print("=" * 60)
        print(f"Total artists processed: {len(df)}")
        print(f"Artists with Instagram data: {len(final_df)}")
        print(f"Success rate: {len(final_df)/len(df)*100:.1f}%")
        print(f"\nData saved to: {output_path}")
        
        # Summary statistics
        print("\nSummary Statistics:")
        print(f"  Average followers: {final_df['instagram_followers'].mean():.0f}")
        print(f"  Average engagement rate: {final_df['instagram_engagement_rate'].mean():.2f}%")
        print(f"  Artists in target range (10k-100k): {len(final_df[(final_df['instagram_followers'] >= 10000) & (final_df['instagram_followers'] <= 100000)])}")
    else:
        print("\nNo Instagram data collected. Check:")
        print("  1. Internet connection")
        print("  2. Instagram rate limits (may need to wait)")
        print("  3. Consider logging in for better access")


if __name__ == "__main__":
    main()
