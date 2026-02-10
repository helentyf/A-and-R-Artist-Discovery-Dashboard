#!/bin/bash

# Quick status check script for automated data collection

cd "$(dirname "$0")/.."

echo "=== Automated Data Collection Status ==="
echo ""

# Check if process is running
if pgrep -f "automated_data_collection.py" > /dev/null; then
    echo "✓ Collection script is RUNNING"
    echo ""
    echo "Process details:"
    ps aux | grep "automated_data_collection.py" | grep -v grep
else
    echo "✗ Collection script is NOT running"
    echo ""
    echo "To start it:"
    echo "  cd /Users/helenfung/Documents/UniversalMusic_DataAnalyst_project"
    echo "  nohup python3.11 scripts/automated_data_collection.py > data/logs/automated_collection_output.log 2>&1 &"
fi

echo ""
echo "=== Recent Log Activity ==="
if [ -f "data/logs/automated_collection.log" ]; then
    echo "Last 10 log entries:"
    tail -10 data/logs/automated_collection.log
else
    echo "No log file found yet"
fi

echo ""
echo "=== Checkpoint Status ==="
if [ -f "data/logs/checkpoint.json" ]; then
    cat data/logs/checkpoint.json | python3.11 -m json.tool 2>/dev/null || cat data/logs/checkpoint.json
else
    echo "No checkpoint file found yet"
fi

echo ""
echo "=== Current Data Stats ==="
if [ -f "data/raw/lastfm_artists_raw.csv" ]; then
    LASTFM_COUNT=$(wc -l < data/raw/lastfm_artists_raw.csv | tr -d ' ')
    echo "Last.fm artists: $((LASTFM_COUNT - 1))"  # Subtract header
else
    echo "Last.fm artists: 0 (file not found)"
fi

if [ -f "data/raw/instagram_data_raw.csv" ]; then
    INSTAGRAM_COUNT=$(wc -l < data/raw/instagram_data_raw.csv | tr -d ' ')
    echo "Instagram data: $((INSTAGRAM_COUNT - 1))"  # Subtract header
else
    echo "Instagram data: 0 (file not found)"
fi

if [ -f "data/raw/youtube_data_raw.csv" ]; then
    YOUTUBE_COUNT=$(wc -l < data/raw/youtube_data_raw.csv | tr -d ' ')
    echo "YouTube data: $((YOUTUBE_COUNT - 1))"  # Subtract header
else
    echo "YouTube data: 0 (file not found)"
fi

if [ -f "data/raw/artist_raw_data_merged.csv" ]; then
    MERGED_COUNT=$(wc -l < data/raw/artist_raw_data_merged.csv | tr -d ' ')
    echo "Merged artists: $((MERGED_COUNT - 1))"  # Subtract header
else
    echo "Merged artists: 0 (file not found)"
fi

echo ""
echo "=== To monitor in real-time ==="
echo "  tail -f data/logs/automated_collection.log"
echo ""
echo "=== To stop the collection ==="
echo "  pkill -f automated_data_collection.py"
