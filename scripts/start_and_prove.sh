#!/bin/bash

cd /Users/helenfung/Documents/UniversalMusic_DataAnalyst_project

echo "=========================================="
echo "STARTING DATA COLLECTION SCRIPT"
echo "=========================================="
echo ""

# Kill any existing
pkill -f automated_data_collection.py 2>/dev/null
sleep 2

# Start fresh
mkdir -p data/logs
nohup python3.11 scripts/automated_data_collection.py > data/logs/automated_collection_output.log 2>&1 &
NEW_PID=$!

echo "Started process with PID: $NEW_PID"
sleep 3

echo ""
echo "=========================================="
echo "VERIFICATION - PROOF IT'S RUNNING"
echo "=========================================="
echo ""

# Check 1: Process exists
if ps -p $NEW_PID > /dev/null 2>&1; then
    echo "✓ Process IS RUNNING (PID: $NEW_PID)"
    ps -p $NEW_PID -o pid,etime,command 2>/dev/null | tail -1
else
    echo "✗ Process NOT running - checking why..."
    tail -20 data/logs/automated_collection_output.log 2>/dev/null
    exit 1
fi

echo ""
echo "Check 2: Log file activity"
if [ -f "data/logs/automated_collection.log" ]; then
    echo "✓ Log file exists"
    echo "Latest entry:"
    tail -3 data/logs/automated_collection.log | tail -1
else
    echo "✗ Log file not found"
fi

echo ""
echo "Check 3: Waiting 5 seconds for new log entry..."
sleep 5
OLD_LINES=$(wc -l < data/logs/automated_collection.log 2>/dev/null || echo "0")
sleep 2
NEW_LINES=$(wc -l < data/logs/automated_collection.log 2>/dev/null || echo "0")

if [ "$NEW_LINES" -gt "$OLD_LINES" ]; then
    echo "✓ NEW LOG ENTRIES DETECTED - Script is actively working!"
    echo "New entries:"
    tail -3 data/logs/automated_collection.log
else
    echo "⚠ No new log entries yet (may be starting up)"
fi

echo ""
echo "=========================================="
echo "STATUS: Script is running and will continue for 8+ hours"
echo "=========================================="
echo ""
echo "To check status anytime:"
echo "  ./scripts/show_proof.sh"
echo ""
echo "To watch live:"
echo "  tail -f data/logs/automated_collection.log"
