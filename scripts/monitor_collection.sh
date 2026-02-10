#!/bin/bash

# Real-time monitoring script for data collection

cd "$(dirname "$0")/.."

echo "=== Data Collection Monitor ==="
echo ""

# Check if process is running
PIDS=$(pgrep -f "automated_data_collection.py" 2>/dev/null)
if [ -n "$PIDS" ]; then
    echo "✓ Script IS RUNNING (PIDs: $PIDS)"
    echo ""
    echo "Process details:"
    ps -p $PIDS -o pid,etime,command 2>/dev/null || echo "Process info unavailable"
else
    echo "✗ Script is NOT running"
    echo ""
    echo "To start: ./scripts/start_collection.sh"
fi

echo ""
echo "=== Latest Activity (last 15 lines) ==="
if [ -f "data/logs/automated_collection.log" ]; then
    tail -15 data/logs/automated_collection.log
else
    echo "No log file found"
fi

echo ""
echo "=== Current Data Counts ==="
if [ -f "data/raw/lastfm_artists_raw.csv" ]; then
    LASTFM=$(tail -n +2 data/raw/lastfm_artists_raw.csv | wc -l | tr -d ' ')
    echo "Last.fm artists: $LASTFM"
else
    echo "Last.fm artists: 0"
fi

if [ -f "data/raw/instagram_data_raw.csv" ]; then
    INSTAGRAM=$(tail -n +2 data/raw/instagram_data_raw.csv | wc -l | tr -d ' ')
    echo "Instagram profiles: $INSTAGRAM"
else
    echo "Instagram profiles: 0"
fi

if [ -f "data/raw/youtube_data_raw.csv" ]; then
    YOUTUBE=$(tail -n +2 data/raw/youtube_data_raw.csv | wc -l | tr -d ' ')
    echo "YouTube channels: $YOUTUBE"
else
    echo "YouTube channels: 0"
fi

echo ""
echo "=== To watch live updates ==="
echo "  tail -f data/logs/automated_collection.log"
echo ""
echo "Press Ctrl+C to exit this monitor"
