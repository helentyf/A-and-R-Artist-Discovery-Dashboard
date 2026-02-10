#!/bin/bash

cd /Users/helenfung/Documents/UniversalMusic_DataAnalyst_project

echo "=========================================="
echo "PROOF THAT SCRIPT IS RUNNING"
echo "=========================================="
echo ""

# Check if process exists
echo "1. PROCESS CHECK:"
if pgrep -f "automated_data_collection.py" > /dev/null 2>&1; then
    PID=$(pgrep -f "automated_data_collection.py" | head -1)
    echo "   ✓ Process IS RUNNING (PID: $PID)"
else
    echo "   ✗ Process NOT found"
fi

echo ""
echo "2. LOG FILE ACTIVITY:"
if [ -f "data/logs/automated_collection.log" ]; then
    LOG_SIZE=$(wc -l < data/logs/automated_collection.log | tr -d ' ')
    LOG_TIME=$(stat -f "%Sm" -t "%H:%M:%S" data/logs/automated_collection.log 2>/dev/null || stat -c "%y" data/logs/automated_collection.log | cut -d' ' -f2 | cut -d'.' -f1)
    echo "   ✓ Log file exists"
    echo "   - Total log lines: $LOG_SIZE"
    echo "   - Last modified: $LOG_TIME"
    echo ""
    echo "   Latest 5 log entries:"
    tail -5 data/logs/automated_collection.log | sed 's/^/   /'
else
    echo "   ✗ Log file not found"
fi

echo ""
echo "3. CURRENT DATA COUNTS:"
python3.11 << 'PYTHON'
import pandas as pd
import os
from datetime import datetime

try:
    lf = pd.read_csv('data/raw/lastfm_artists_raw.csv')
    lf_time = datetime.fromtimestamp(os.path.getmtime('data/raw/lastfm_artists_raw.csv'))
    print(f"   Last.fm: {len(lf)} artists (updated: {lf_time.strftime('%H:%M:%S')})")
except:
    print("   Last.fm: File not found")

try:
    inst = pd.read_csv('data/raw/instagram_data_raw.csv')
    inst_time = datetime.fromtimestamp(os.path.getmtime('data/raw/instagram_data_raw.csv'))
    print(f"   Instagram: {len(inst)} profiles (updated: {inst_time.strftime('%H:%M:%S')})")
except:
    print("   Instagram: File not found or no data yet")

try:
    yt = pd.read_csv('data/raw/youtube_data_raw.csv')
    yt_time = datetime.fromtimestamp(os.path.getmtime('data/raw/youtube_data_raw.csv'))
    print(f"   YouTube: {len(yt)} channels (updated: {yt_time.strftime('%H:%M:%S')})")
except:
    print("   YouTube: File not found or no data yet")
PYTHON

echo ""
echo "4. SCRIPT STATUS:"
if pgrep -f "automated_data_collection.py" > /dev/null 2>&1; then
    echo "   ✓ Script is ACTIVE and running"
    echo "   ✓ Will continue for 8+ hours"
    echo "   ✓ Collecting: Last.fm → Instagram → YouTube → Merge"
else
    echo "   ✗ Script is NOT running - restart with: ./scripts/restart_collection.sh"
fi

echo ""
echo "=========================================="
echo "To watch live updates:"
echo "  tail -f data/logs/automated_collection.log"
echo "=========================================="
