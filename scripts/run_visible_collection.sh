#!/bin/bash

# Run collection script with visible output in terminal

cd "$(dirname "$0")/.."

# Create logs directory
mkdir -p data/logs

echo "Starting data collection with visible output..."
echo "Press Ctrl+C to stop"
echo ""

# Kill any existing instances
pkill -f automated_data_collection.py 2>/dev/null
sleep 2

# Run with visible output (not background)
python3.11 scripts/automated_data_collection.py
