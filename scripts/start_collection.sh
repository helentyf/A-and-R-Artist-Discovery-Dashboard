#!/bin/bash

# Start automated data collection script
# Creates necessary directories and starts the script

cd "$(dirname "$0")/.."

# Create logs directory if it doesn't exist
mkdir -p data/logs

# Start the collection script
echo "Starting automated data collection..."
echo "Logs will be saved to: data/logs/automated_collection.log"
echo "Output will be saved to: data/logs/automated_collection_output.log"
echo ""
echo "To check status: ./scripts/check_collection_status.sh"
echo "To monitor logs: tail -f data/logs/automated_collection.log"
echo "To stop: pkill -f automated_data_collection.py"
echo ""

nohup python3.11 scripts/automated_data_collection.py > data/logs/automated_collection_output.log 2>&1 &

echo "Script started in background (PID: $!)"
echo "Check status with: ./scripts/check_collection_status.sh"
