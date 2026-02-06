"""
Setup Verification Script

Checks that all required dependencies, API credentials, and configurations
are properly set up before running data collection scripts.
"""

import os
import sys
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()


def check_python_version():
    """Check Python version is 3.10 or higher."""
    version = sys.version_info
    if version.major < 3 or (version.major == 3 and version.minor < 10):
        print("ERROR: Python 3.10 or higher required")
        print(f"Current version: {version.major}.{version.minor}.{version.micro}")
        return False
    print(f"OK: Python {version.major}.{version.minor}.{version.micro}")
    return True


def check_dependencies():
    """Check required packages are installed."""
    required_packages = [
        'requests',
        'google.cloud.bigquery',
        'instaloader',
        'googleapiclient',
        'pandas',
        'dotenv',
        'streamlit',
        'plotly'
    ]
    
    missing = []
    for package in required_packages:
        try:
            if package == 'google.cloud.bigquery':
                import google.cloud.bigquery
            elif package == 'googleapiclient':
                from googleapiclient.discovery import build
            elif package == 'dotenv':
                import dotenv
            else:
                __import__(package)
            print(f"OK: {package}")
        except ImportError:
            print(f"MISSING: {package}")
            missing.append(package)
    
    if missing:
        print(f"\nInstall missing packages: pip install {' '.join(missing)}")
        return False
    return True


def check_env_file():
    """Check .env file exists and has required variables."""
    env_path = Path('.env')
    
    if not env_path.exists():
        print("ERROR: .env file not found")
        print("Copy .env.example to .env and fill in your credentials")
        return False
    
    print("OK: .env file exists")
    
    required_vars = [
        'LASTFM_API_KEY',
        'YOUTUBE_API_KEY',
        'BIGQUERY_PROJECT_ID'
    ]
    
    missing_vars = []
    for var in required_vars:
        value = os.getenv(var)
        if not value or value.startswith('your_'):
            print(f"MISSING or INVALID: {var}")
            missing_vars.append(var)
        else:
            print(f"OK: {var} is set")
    
    if missing_vars:
        print(f"\nPlease set the following in .env: {', '.join(missing_vars)}")
        return False
    
    return True


def check_bigquery_credentials():
    """Check BigQuery credentials file exists."""
    creds_path = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')
    
    if not creds_path:
        print("WARNING: GOOGLE_APPLICATION_CREDENTIALS not set")
        print("Will attempt to use default credentials")
        return True
    
    if not Path(creds_path).exists():
        print(f"ERROR: Credentials file not found: {creds_path}")
        return False
    
    print(f"OK: BigQuery credentials file found: {creds_path}")
    return True


def check_directories():
    """Check required directories exist."""
    required_dirs = [
        'scripts',
        'sql',
        'data',
        'data/raw',
        'dashboard'
    ]
    
    all_exist = True
    for dir_path in required_dirs:
        if not Path(dir_path).exists():
            print(f"MISSING: {dir_path}/")
            Path(dir_path).mkdir(parents=True, exist_ok=True)
            print(f"  Created: {dir_path}/")
            all_exist = False
        else:
            print(f"OK: {dir_path}/")
    
    return all_exist


def check_scripts():
    """Check required scripts exist."""
    required_scripts = [
        'scripts/collect_lastfm_artists.py',
        'scripts/test_lastfm_api.py',
        'scripts/collect_instagram_data.py',
        'scripts/collect_youtube_data.py',
        'scripts/merge_and_upload_bigquery.py',
        'scripts/generate_insights.py',
        'sql/create_artist_scores.sql',
        'dashboard/app.py'
    ]
    
    all_exist = True
    for script in required_scripts:
        if not Path(script).exists():
            print(f"MISSING: {script}")
            all_exist = False
        else:
            print(f"OK: {script}")
    
    return all_exist


def main():
    """Run all setup checks."""
    print("=" * 60)
    print("SETUP VERIFICATION")
    print("=" * 60)
    print()
    
    checks = [
        ("Python Version", check_python_version),
        ("Dependencies", check_dependencies),
        ("Environment File", check_env_file),
        ("BigQuery Credentials", check_bigquery_credentials),
        ("Directories", check_directories),
        ("Scripts", check_scripts)
    ]
    
    results = []
    for name, check_func in checks:
        print(f"\n{name}:")
        print("-" * 40)
        result = check_func()
        results.append((name, result))
    
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    
    all_passed = all(result for _, result in results)
    
    for name, result in results:
        status = "PASS" if result else "FAIL"
        print(f"{name}: {status}")
    
    if all_passed:
        print("\nAll checks passed! You're ready to run the data collection scripts.")
        return 0
    else:
        print("\nSome checks failed. Please fix the issues above before proceeding.")
        return 1


if __name__ == "__main__":
    sys.exit(main())
