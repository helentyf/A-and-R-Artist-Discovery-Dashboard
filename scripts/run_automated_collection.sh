#!/bin/bash

# Automated Data Collection Runner
# This script runs the automated collection script with proper Python version

cd "$(dirname "$0")/.."

# Use Python 3.11 for stability
PYTHON_CMD="python3.11"

# Check if Python 3.11 is available
if ! command -v $PYTHON_CMD &> /dev/null; then
    echo "Python 3.11 not found. Trying python3..."
    PYTHON_CMD="python3"
fi

echo "Starting automated data collection..."
echo "Logs will be saved to: data/logs/automated_collection.log"
echo "Press Ctrl+C to stop"
echo ""

# Run the automated collection script
# Remove --once flag to run continuously
$PYTHON_CMD scripts/automated_data_collection.py
