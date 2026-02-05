#!/usr/bin/env python3
"""Analyze uncategorized items to find patterns for new topic rules."""
import csv
import sys
import re

# Force UTF-8 output on Windows
sys.stdout.reconfigure(encoding='utf-8', errors='replace')

with open('fb_extract_out/sean_topics.csv', encoding='utf-8', newline='') as f:
    rows = [r for r in csv.DictReader(f) if r['topics'] == 'uncategorized']

print(f"Total uncategorized: {len(rows)}")
print()

# Show samples 100-170 for different content
for i, r in enumerate(rows[100:180]):
    preview = r.get('preview', '')[:150].replace('\n', ' ').replace('\r', '')
    # Strip non-ASCII for console safety
    preview = re.sub(r'[^\x20-\x7E]+', ' ', preview)
    print(f"[{i+100:3}] {preview}")



