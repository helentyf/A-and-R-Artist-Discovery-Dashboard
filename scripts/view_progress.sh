#!/bin/bash

# Quick progress viewer

cd "$(dirname "$0")/.."

echo "=== CURRENT SCRAPING PROGRESS ==="
echo ""

# Check if script is running
if pgrep -f "automated_data_collection.py" > /dev/null 2>&1; then
    echo "✓ Script IS RUNNING"
else
    echo "✗ Script is NOT running"
fi

echo ""
echo "=== Current Data Counts ==="
python3.11 << 'PYTHON'
import pandas as pd
import os

lf_path = 'data/raw/lastfm_artists_raw.csv'
inst_path = 'data/raw/instagram_data_raw.csv'
yt_path = 'data/raw/youtube_data_raw.csv'

lf_count = len(pd.read_csv(lf_path)) if os.path.exists(lf_path) else 0
inst_count = len(pd.read_csv(inst_path)) if os.path.exists(inst_path) else 0
yt_count = len(pd.read_csv(yt_path)) if os.path.exists(yt_path) else 0

print(f"Last.fm artists: {lf_count}")
print(f"Instagram profiles: {inst_count}")
print(f"YouTube channels: {yt_count}")
PYTHON

echo ""
echo "=== Latest Log Activity ==="
if [ -f "data/logs/automated_collection.log" ]; then
    tail -10 data/logs/automated_collection.log | grep -E "(Step|Running|Successfully|ERROR)" | tail -5
else
    echo "No log file found"
fi

echo ""
echo "=== To watch live updates ==="
echo "  tail -f data/logs/automated_collection.log"
