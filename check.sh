#!/bin/bash
cd /Users/helenfung/Documents/UniversalMusic_DataAnalyst_project && ps aux | grep automated_data_collection.py | grep -v grep && echo "---" && tail -3 data/logs/automated_collection.log 2>/dev/null | tail -1 || echo "Not running or no logs"
