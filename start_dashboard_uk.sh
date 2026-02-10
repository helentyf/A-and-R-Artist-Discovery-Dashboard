#!/bin/bash
# Safe startup script for UK dashboard with error handling

echo "Starting Underrated Artist Radar - UK Dashboard..."
echo "This may take a few seconds to initialize..."
echo ""

# Add delay before starting
sleep 1

# Change to project directory
cd "$(dirname "$0")"

# Check if Python is available
if ! command -v python3 &> /dev/null; then
    echo "Error: python3 not found. Please install Python 3.10+"
    exit 1
fi

# Check if Streamlit is installed
if ! python3 -c "import streamlit" 2>/dev/null; then
    echo "Error: Streamlit not installed. Run: pip install streamlit"
    exit 1
fi

# Try Python 3.11 first, fallback to python3
if command -v python3.11 &> /dev/null; then
    PYTHON_CMD="python3.11"
    echo "Using Python 3.11 (recommended)"
else
    PYTHON_CMD="python3"
    echo "Using Python 3 (may have compatibility issues)"
fi

# Start dashboard with error handling
echo "Launching dashboard on http://localhost:8501"
echo "Press Ctrl+C to stop"
echo ""

$PYTHON_CMD -u -m streamlit run dashboard/app.py \
    --server.port 8501 \
    --server.headless true \
    --server.runOnSave false \
    --browser.gatherUsageStats false \
    2>&1
