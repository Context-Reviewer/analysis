"""
Facebook Group Scraper - Two-Phase Architecture
Usage:
  python step5b_scrape_post_comments.py --mode discovery
  python step5b_scrape_post_comments.py --mode extraction
"""
import argparse
import asyncio
import sys
import os

# Add current directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from step5b_phase1_discover import run_discovery
from step5b_phase2_extract import run_extraction

def main():
    parser = argparse.ArgumentParser(description="Facebook Scraper: Two-Phase Architecture")
    parser.add_argument("--mode", choices=["discovery", "extraction"], required=True,
                        help="Phase to run: 'discovery' or 'extraction'")
    args = parser.parse_args()
    
    if args.mode == "discovery":
        print("Running Phase 1: Discovery")
        asyncio.run(run_discovery())
    elif args.mode == "extraction":
        print("Running Phase 2: Extraction")
        asyncio.run(run_extraction())

if __name__ == "__main__":
    main()
