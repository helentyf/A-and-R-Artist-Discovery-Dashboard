#!/bin/bash

# Single command to run all data collection in a loop with rate limit handling
# Usage: ./scripts/run_all_collection.sh

cd /Users/helenfung/Documents/UniversalMusic_DataAnalyst_project

# Kill any existing processes
pkill -f automated_data_collection.py 2>/dev/null
sleep 2

# Create logs directory
mkdir -p data/logs

echo "=========================================="
echo "Starting Complete Data Collection Loop"
echo "=========================================="
echo ""
echo "This will run continuously:"
echo "  1. Last.fm → 2. Instagram → 3. YouTube → 4. Instagram Verification → Repeat"
echo ""
echo "Rate limits handled automatically (waits when needed)"
echo "Press Ctrl+C to stop"
echo ""
echo "Logs: data/logs/automated_collection.log"
echo "=========================================="
echo ""

# Start the automated collection script (runs indefinitely)
# Use absolute path and ensure we're in the right directory
cd /Users/helenfung/Documents/UniversalMusic_DataAnalyst_project
python3.11 scripts/automated_data_collection.py > data/logs/automated_collection_output.log 2>&1 &
NEW_PID=$!

sleep 5

# Check if process is still running
if ps -p $NEW_PID > /dev/null 2>&1 || pgrep -f "automated_data_collection.py" > /dev/null 2>&1; then
    ACTUAL_PID=$(pgrep -f "automated_data_collection.py" | head -1)
    echo "✓ Started successfully (PID: $ACTUAL_PID)"
    echo ""
    echo "To monitor: tail -f data/logs/automated_collection.log"
    echo "To check status: ./scripts/show_proof.sh"
    echo "To stop: pkill -f automated_data_collection.py"
else
    echo "✗ Process may have exited. Checking logs..."
    tail -20 data/logs/automated_collection_output.log 2>/dev/null || echo "No output log yet"
    echo ""
    echo "Trying to start in background with nohup..."
    nohup python3.11 scripts/automated_data_collection.py > data/logs/automated_collection_output.log 2>&1 &
    sleep 3
    if pgrep -f "automated_data_collection.py" > /dev/null 2>&1; then
        echo "✓ Started with nohup (PID: $(pgrep -f 'automated_data_collection.py' | head -1))"
    else
        echo "✗ Still failed. Check logs manually."
    fi
fi
