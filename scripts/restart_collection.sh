#!/bin/bash

# Clean restart of data collection - kills all instances and starts fresh

cd "$(dirname "$0")/.."

echo "Stopping all existing collection processes..."
pkill -f automated_data_collection.py
sleep 2

echo "Checking if any are still running..."
REMAINING=$(pgrep -f automated_data_collection.py | wc -l | tr -d ' ')
if [ "$REMAINING" -gt 0 ]; then
    echo "Warning: $REMAINING processes still running. Force killing..."
    pkill -9 -f automated_data_collection.py
    sleep 1
fi

echo "Starting fresh collection process..."
mkdir -p data/logs

# Start ONE instance that runs continuously (no --once flag)
# Use nohup with proper redirection and run in background
cd /Users/helenfung/Documents/UniversalMusic_DataAnalyst_project
nohup python3.11 scripts/automated_data_collection.py > data/logs/automated_collection_output.log 2>&1 &
NEW_PID=$!

sleep 3

# Verify it started
if ps -p $NEW_PID > /dev/null 2>&1; then
    echo "✓ Collection script started successfully"
    echo "PID: $NEW_PID"
    echo ""
    echo "Process details:"
    ps -p $NEW_PID -o pid,etime,command 2>/dev/null || echo "PID $NEW_PID is running"
    echo ""
    echo "To monitor: tail -f data/logs/automated_collection.log"
    echo "To check status: ./scripts/view_progress.sh"
    echo "To verify: ps aux | grep $NEW_PID"
else
    echo "✗ Failed to start. Check logs:"
    tail -20 data/logs/automated_collection_output.log 2>/dev/null || echo "No output log found"
fi
