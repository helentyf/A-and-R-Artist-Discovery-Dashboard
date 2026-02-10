#!/bin/bash

# Script to ensure automated collection keeps running
# Run this periodically (e.g., via cron) to restart if it stops

cd "$(dirname "$0")/.."

SCRIPT_PID=$(pgrep -f "automated_data_collection.py")

if [ -z "$SCRIPT_PID" ]; then
    echo "$(date): Collection script not running. Restarting..."
    nohup python3.11 scripts/automated_data_collection.py > data/logs/automated_collection_output.log 2>&1 &
    echo "$(date): Restarted with PID: $!"
else
    echo "$(date): Collection script is running (PID: $SCRIPT_PID)"
fi
