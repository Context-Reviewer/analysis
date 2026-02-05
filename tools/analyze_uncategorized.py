#!/usr/bin/env python3
"""Analyze uncategorized items to find patterns for new topic rules."""
import csv
import sys
import re
from collections import Counter

# Force UTF-8 output on Windows
sys.stdout.reconfigure(encoding='utf-8', errors='replace')

with open('fb_extract_out/sean_topics.csv', encoding='utf-8', newline='') as f:
    rows = [r for r in csv.DictReader(f) if r['topics'] == 'uncategorized']

print(f"Total uncategorized: {len(rows)}")
print()

# Analyze by body length
lengths = [int(r.get('body_len', 0)) for r in rows]
print("Body length distribution:")
print(f"  0-20 chars:   {sum(1 for l in lengths if l <= 20)}")
print(f"  21-50 chars:  {sum(1 for l in lengths if 21 <= l <= 50)}")
print(f"  51-100 chars: {sum(1 for l in lengths if 51 <= l <= 100)}")
print(f"  101+ chars:   {sum(1 for l in lengths if l > 100)}")
print()

# Show samples from different length ranges
print("=" * 80)
print("SHORT (0-20 chars) - likely fragments:")
print("=" * 80)
for r in [r for r in rows if int(r.get('body_len', 0)) <= 20][:15]:
    preview = re.sub(r'[^\x20-\x7E]+', ' ', r.get('preview', '')[:80])
    print(f"  [{r.get('body_len'):>3}] {preview}")

print()
print("=" * 80)
print("MEDIUM (21-50 chars):")
print("=" * 80)
for r in [r for r in rows if 21 <= int(r.get('body_len', 0)) <= 50][:15]:
    preview = re.sub(r'[^\x20-\x7E]+', ' ', r.get('preview', '')[:100])
    print(f"  [{r.get('body_len'):>3}] {preview}")

print()
print("=" * 80)
print("LONGER (51-100 chars):")
print("=" * 80)
for r in [r for r in rows if 51 <= int(r.get('body_len', 0)) <= 100][:15]:
    preview = re.sub(r'[^\x20-\x7E]+', ' ', r.get('preview', '')[:120])
    print(f"  [{r.get('body_len'):>3}] {preview}")

print()
print("=" * 80)
print("LONG (101+ chars) - substantive content:")
print("=" * 80)
for r in [r for r in rows if int(r.get('body_len', 0)) > 100][:20]:
    preview = re.sub(r'[^\x20-\x7E]+', ' ', r.get('preview', '')[:150])
    print(f"  [{r.get('body_len'):>3}] {preview}")




