"""
Automated Data Collection Script

Runs all data collection scripts in sequence with automatic rate limit handling,
retries, and progress tracking. Designed to run unattended.

Handles:
- Last.fm artist collection
- Instagram data collection (with rate limit handling)
- YouTube data collection (with quota management)
- Automatic retries and delays
- Progress logging
"""

import subprocess
import sys
import time
import os
from datetime import datetime
from pathlib import Path
import pandas as pd
import json

# Configuration
SCRIPT_DIR = Path(__file__).parent
PROJECT_ROOT = SCRIPT_DIR.parent
LOG_FILE = PROJECT_ROOT / 'data' / 'logs' / 'automated_collection.log'
CHECKPOINT_FILE = PROJECT_ROOT / 'data' / 'logs' / 'checkpoint.json'

# Rate limit configurations (in seconds)
INSTAGRAM_RATE_LIMIT_WAIT = 1320  # 22 minutes (1320 seconds) + buffer
INSTAGRAM_RETRY_DELAY = 60  # Wait 1 minute between retries
YOUTUBE_QUOTA_CHECK_INTERVAL = 300  # Check quota every 5 minutes
GENERAL_RETRY_DELAY = 30  # General retry delay

# Create logs directory if it doesn't exist
LOG_FILE.parent.mkdir(parents=True, exist_ok=True)


def log_message(message, level="INFO"):
    """Log message to file and console."""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log_entry = f"[{timestamp}] [{level}] {message}\n"
    
    with open(LOG_FILE, 'a') as f:
        f.write(log_entry)
    
    print(f"[{timestamp}] {message}")


def load_checkpoint():
    """Load checkpoint data."""
    if CHECKPOINT_FILE.exists():
        try:
            with open(CHECKPOINT_FILE, 'r') as f:
                return json.load(f)
        except:
            return {}
    return {}


def save_checkpoint(data):
    """Save checkpoint data."""
    CHECKPOINT_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(CHECKPOINT_FILE, 'w') as f:
        json.dump(data, f, indent=2)


def run_script(script_name, max_retries=3, retry_delay=GENERAL_RETRY_DELAY):
    """
    Run a Python script with retry logic.
    
    Args:
        script_name: Name of script to run (relative to scripts/)
        max_retries: Maximum number of retries
        retry_delay: Delay between retries (seconds)
        
    Returns:
        True if successful, False otherwise
    """
    script_path = SCRIPT_DIR / script_name
    
    for attempt in range(1, max_retries + 1):
        log_message(f"Running {script_name} (attempt {attempt}/{max_retries})")
        
        try:
            result = subprocess.run(
                [sys.executable, str(script_path)],
                cwd=PROJECT_ROOT,
                capture_output=True,
                text=True,
                timeout=7200  # 2 hour timeout per script
            )
            
            if result.returncode == 0:
                log_message(f"Successfully completed {script_name}")
                if result.stdout:
                    log_message(f"Output: {result.stdout[-500:]}")  # Last 500 chars
                return True
            else:
                error_output = result.stderr or result.stdout
                log_message(f"Error in {script_name}: {error_output[-500:]}", "ERROR")
                
                # Check for specific rate limit errors
                if "rate limit" in error_output.lower() or "401" in error_output or "429" in error_output:
                    if "instagram" in script_name.lower():
                        log_message(f"Instagram rate limit detected. Waiting {INSTAGRAM_RATE_LIMIT_WAIT} seconds...")
                        time.sleep(INSTAGRAM_RATE_LIMIT_WAIT)
                        continue
                    elif "youtube" in script_name.lower():
                        log_message(f"YouTube quota may be exhausted. Waiting {YOUTUBE_QUOTA_CHECK_INTERVAL} seconds...")
                        time.sleep(YOUTUBE_QUOTA_CHECK_INTERVAL)
                        continue
                
                if attempt < max_retries:
                    log_message(f"Retrying {script_name} in {retry_delay} seconds...")
                    time.sleep(retry_delay)
                else:
                    log_message(f"Failed to complete {script_name} after {max_retries} attempts", "ERROR")
                    return False
                    
        except subprocess.TimeoutExpired:
            log_message(f"{script_name} timed out after 2 hours", "ERROR")
            if attempt < max_retries:
                log_message(f"Retrying {script_name} in {retry_delay} seconds...")
                time.sleep(retry_delay)
            else:
                return False
                
        except Exception as e:
            log_message(f"Exception running {script_name}: {str(e)}", "ERROR")
            if attempt < max_retries:
                log_message(f"Retrying {script_name} in {retry_delay} seconds...")
                time.sleep(retry_delay)
            else:
                return False
    
    return False


def check_instagram_rate_limit():
    """Check if we need to wait for Instagram rate limit."""
    # Check if there's a recent rate limit error in logs
    if LOG_FILE.exists():
        with open(LOG_FILE, 'r') as f:
            recent_logs = f.readlines()[-50:]  # Last 50 lines
        
        for line in recent_logs:
            if "rate limit" in line.lower() or "22 minutes" in line.lower() or "401" in line:
                # Extract timestamp if possible
                log_message("Instagram rate limit detected in recent logs. Waiting...")
                return True
    return False


def get_current_stats():
    """Get current statistics about collected data."""
    stats = {
        'lastfm_artists': 0,
        'instagram_data': 0,
        'youtube_data': 0,
        'merged_data': 0
    }
    
    try:
        lastfm_path = PROJECT_ROOT / 'data' / 'raw' / 'lastfm_artists_raw.csv'
        if lastfm_path.exists():
            df = pd.read_csv(lastfm_path)
            stats['lastfm_artists'] = len(df)
    except:
        pass
    
    try:
        instagram_path = PROJECT_ROOT / 'data' / 'raw' / 'instagram_data_raw.csv'
        if instagram_path.exists():
            df = pd.read_csv(instagram_path)
            stats['instagram_data'] = len(df)
    except:
        pass
    
    try:
        youtube_path = PROJECT_ROOT / 'data' / 'raw' / 'youtube_data_raw.csv'
        if youtube_path.exists():
            df = pd.read_csv(youtube_path)
            stats['youtube_data'] = len(df)
    except:
        pass
    
    try:
        merged_path = PROJECT_ROOT / 'data' / 'raw' / 'artist_raw_data_merged.csv'
        if merged_path.exists():
            df = pd.read_csv(merged_path)
            stats['merged_data'] = len(df)
    except:
        pass
    
    return stats


def main_collection_loop(max_iterations=None):
    """
    Main collection loop that runs all scripts in sequence.
    
    Args:
        max_iterations: Maximum number of full cycles (None = infinite)
    """
    log_message("=" * 80)
    log_message("Starting Automated Data Collection")
    log_message("=" * 80)
    
    checkpoint = load_checkpoint()
    iteration = checkpoint.get('iteration', 0)
    
    if max_iterations:
        log_message(f"Will run for {max_iterations} iterations")
    else:
        log_message("Running indefinitely until stopped (Ctrl+C)")
    
    try:
        while True:
            if max_iterations and iteration >= max_iterations:
                log_message(f"Reached maximum iterations ({max_iterations}). Stopping.")
                break
            
            iteration += 1
            log_message(f"\n{'=' * 80}")
            log_message(f"Starting iteration {iteration}")
            log_message(f"{'=' * 80}\n")
            
            # Get current stats
            stats_before = get_current_stats()
            log_message(f"Current stats: {stats_before}")
            
            # Step 1: Collect Last.fm artists
            log_message("\n--- Step 1: Collecting Last.fm Artists ---")
            if run_script('collect_lastfm_artists.py', max_retries=2):
                stats_after = get_current_stats()
                new_artists = stats_after['lastfm_artists'] - stats_before['lastfm_artists']
                if new_artists > 0:
                    log_message(f"Added {new_artists} new Last.fm artists")
            else:
                log_message("Last.fm collection had issues, but continuing...")
            
            time.sleep(10)  # Brief pause between scripts
            
            # Step 2: Collect Instagram data
            log_message("\n--- Step 2: Collecting Instagram Data ---")
            
            # Check if we need to wait for rate limit
            if check_instagram_rate_limit():
                log_message(f"Waiting {INSTAGRAM_RATE_LIMIT_WAIT} seconds for Instagram rate limit...")
                time.sleep(INSTAGRAM_RATE_LIMIT_WAIT)
            
            if run_script('collect_instagram_data.py', max_retries=5, retry_delay=INSTAGRAM_RETRY_DELAY):
                stats_after = get_current_stats()
                new_instagram = stats_after['instagram_data'] - stats_before['instagram_data']
                if new_instagram > 0:
                    log_message(f"Added Instagram data for {new_instagram} artists")
            else:
                log_message("Instagram collection had issues, but continuing...")
            
            time.sleep(10)
            
            # Step 3: Collect YouTube data
            log_message("\n--- Step 3: Collecting YouTube Data ---")
            if run_script('collect_youtube_data.py', max_retries=3):
                stats_after = get_current_stats()
                new_youtube = stats_after['youtube_data'] - stats_before['youtube_data']
                if new_youtube > 0:
                    log_message(f"Added YouTube data for {new_youtube} artists")
            else:
                log_message("YouTube collection had issues, but continuing...")
            
            time.sleep(10)
            
            # Step 4: Merge and upload to BigQuery
            log_message("\n--- Step 4: Merging Data and Uploading to BigQuery ---")
            if run_script('merge_and_upload_bigquery.py', max_retries=2):
                stats_after = get_current_stats()
                log_message(f"Merged data uploaded. Total artists in merged dataset: {stats_after['merged_data']}")
            else:
                log_message("Merge/upload had issues, but continuing...")
            
            # Save checkpoint
            checkpoint = {
                'iteration': iteration,
                'last_run': datetime.now().isoformat(),
                'stats': get_current_stats()
            }
            save_checkpoint(checkpoint)
            
            # Summary
            stats_final = get_current_stats()
            log_message(f"\n--- Iteration {iteration} Complete ---")
            log_message(f"Final stats: {stats_final}")
            log_message(f"Total Last.fm artists: {stats_final['lastfm_artists']}")
            log_message(f"Artists with Instagram data: {stats_final['instagram_data']}")
            log_message(f"Artists with YouTube data: {stats_final['youtube_data']}")
            log_message(f"Total merged artists: {stats_final['merged_data']}")
            
            # Wait before next iteration (if running continuously)
            if not max_iterations:
                wait_time = 1800  # Wait 30 minutes before next full cycle (reduced for faster collection)
                log_message(f"\nCompleted iteration {iteration}. Waiting {wait_time} seconds (30 minutes) before next iteration...")
                log_message("Press Ctrl+C to stop")
                time.sleep(wait_time)
            else:
                break
        
        log_message("\n" + "=" * 80)
        log_message("Automated Data Collection Complete")
        log_message("=" * 80)
        
    except KeyboardInterrupt:
        log_message("\n\nCollection interrupted by user (Ctrl+C)")
        log_message("Saving checkpoint...")
        checkpoint = {
            'iteration': iteration,
            'last_run': datetime.now().isoformat(),
            'stats': get_current_stats(),
            'interrupted': True
        }
        save_checkpoint(checkpoint)
        log_message("Checkpoint saved. You can resume later.")
        log_message("Final stats:", get_current_stats())
    except Exception as e:
        log_message(f"\n\nFatal error: {str(e)}", "ERROR")
        import traceback
        log_message(traceback.format_exc(), "ERROR")
        raise


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Automated data collection with rate limit handling')
    parser.add_argument('--iterations', type=int, default=None,
                       help='Number of full cycles to run (default: infinite)')
    parser.add_argument('--once', action='store_true',
                       help='Run once and exit (equivalent to --iterations 1)')
    
    args = parser.parse_args()
    
    if args.once:
        main_collection_loop(max_iterations=1)
    else:
        main_collection_loop(max_iterations=args.iterations)
