#!/usr/bin/env python3
"""
generate_report_json.py
-----------------------
Step 4: Transform pipeline outputs into docs/data/report.json for the dashboard.

Governance:
- Deterministic output: stable ordering, stable rounding, stable ID generation
- No inference: intrusion and self_portrayal are null (not computed)
- Sentiment is metadata only (used for avg_sentiment/hostility_rate, never for filtering/ordering)

Inputs (read-only):
- fb_extract_out/sean_topics.csv (topic assignments + sentiment per item)
- fb_extract_out/sean_timeline.json (for date range extraction)

Output:
- docs/data/report.json

Determinism rules:
- Topics sorted by: (count DESC, topic ASC)
- example_ids: first N items in CSV row order for each topic
- ID = first 8 chars of sha256(permalink|source_index|item_type)
- Floats rounded to 4 decimal places
- hostility_rate = fraction of items with sentiment_compound < -0.6
"""

from __future__ import annotations

import csv
import hashlib
import json
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

# ─────────────────────────────────────────────────────────────────────────────
# Configuration
# ─────────────────────────────────────────────────────────────────────────────

OUT_DIR = Path("fb_extract_out")
SRC_TOPICS_CSV = OUT_DIR / "sean_topics.csv"
SRC_TIMELINE_JSON = OUT_DIR / "sean_timeline.json"

SITE_REPORT_PATH = Path("docs/data/report.json")

# Dashboard config
MAX_EXAMPLE_IDS = 8
HOSTILE_THRESHOLD = -0.6  # sentiment_compound below this = hostile

# ─────────────────────────────────────────────────────────────────────────────
# Deterministic ID generation
# ─────────────────────────────────────────────────────────────────────────────

def generate_item_id(permalink: str, source_index: int, item_type: str) -> str:
    """
    Generate a deterministic 8-char ID from item properties.
    Uses SHA256 hash of 'permalink|source_index|item_type'.
    """
    key = f"{permalink}|{source_index}|{item_type}"
    return hashlib.sha256(key.encode("utf-8")).hexdigest()[:8]


# ─────────────────────────────────────────────────────────────────────────────
# Data loading
# ─────────────────────────────────────────────────────────────────────────────

def load_topics_csv(path: Path) -> List[Dict[str, Any]]:
    """
    Load the topics CSV produced by step3_analyze_reason.py.
    Returns list of dicts with keys matching CSV columns.
    """
    rows: List[Dict[str, Any]] = []
    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append({
                "i": int(row["i"]),
                "item_type": row["item_type"],
                "time": row["time"],
                "permalink": row["permalink"],
                "topics": row["topics"].split("|") if row["topics"] else [],
                "sentiment_compound": float(row["sentiment_compound"]) if row["sentiment_compound"] else 0.0,
            })
    return rows


def load_timeline_json(path: Path) -> List[Dict[str, Any]]:
    """Load timeline JSON for date range extraction."""
    data = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(data, list):
        raise SystemExit(f"Timeline JSON is not a list: {path}")
    return data


def extract_date_range(timeline: List[Dict[str, Any]]) -> tuple[Optional[str], Optional[str]]:
    """
    Extract date_min and date_max from timeline data.
    Uses 'timestamp_parsed' field. Returns (date_min, date_max) as ISO strings or None.
    """
    timestamps: List[str] = []
    for item in timeline:
        ts = item.get("timestamp_parsed")
        if ts and isinstance(ts, str):
            timestamps.append(ts)
    
    if not timestamps:
        return None, None
    
    # Lexicographic sort works for ISO8601 strings
    timestamps.sort()
    return timestamps[0], timestamps[-1]


# ─────────────────────────────────────────────────────────────────────────────
# Report generation
# ─────────────────────────────────────────────────────────────────────────────

def round4(val: float) -> float:
    """Round to 4 decimal places for deterministic output."""
    return round(val, 4)


def build_report(topics_data: List[Dict[str, Any]], date_min: Optional[str], date_max: Optional[str]) -> Dict[str, Any]:
    """
    Build the report.json structure from topics CSV data.
    
    Schema matches docs/assets/site.js expectations:
    - generated_at: ISO timestamp
    - dataset: {total_items, date_min, date_max}
    - topics: [{topic, count, percent_of_total, avg_sentiment, hostility_rate, example_ids}]
    - items: {id: {permalink}} — for linkable example IDs
    - intrusion: null (not computed)
    - self_portrayal: null (not computed)
    """
    total_items = len(topics_data)
    
    # Aggregate by topic
    # Structure: topic -> {count, sentiment_sum, hostile_count, example_ids}
    topic_stats: Dict[str, Dict[str, Any]] = defaultdict(lambda: {
        "count": 0,
        "sentiment_sum": 0.0,
        "hostile_count": 0,
        "example_ids": [],
    })
    
    # Track all items by ID for linkable examples
    all_items: Dict[str, Dict[str, str]] = {}
    
    # Process each row (in CSV order for deterministic example_ids)
    for row in topics_data:
        item_id = generate_item_id(row["permalink"], row["i"], row["item_type"])
        sentiment = row["sentiment_compound"]
        is_hostile = sentiment < HOSTILE_THRESHOLD
        
        # Store item info for potential linking
        all_items[item_id] = {"permalink": row["permalink"]}
        
        for topic in row["topics"]:
            stats = topic_stats[topic]
            stats["count"] += 1
            stats["sentiment_sum"] += sentiment
            if is_hostile:
                stats["hostile_count"] += 1
            # Add to example_ids (first N only, in CSV order)
            if len(stats["example_ids"]) < MAX_EXAMPLE_IDS:
                stats["example_ids"].append(item_id)
    
    # Build topics array with deterministic ordering: (count DESC, topic ASC)
    topics_list: List[Dict[str, Any]] = []
    used_item_ids: set = set()
    
    for topic_name in sorted(topic_stats.keys(), key=lambda t: (-topic_stats[t]["count"], t)):
        stats = topic_stats[topic_name]
        count = stats["count"]
        
        topics_list.append({
            "topic": topic_name,
            "count": count,
            "percent_of_total": round4(count / total_items) if total_items > 0 else 0.0,
            "avg_sentiment": round4(stats["sentiment_sum"] / count) if count > 0 else 0.0,
            "pct_below_neg_threshold": round4(stats["hostile_count"] / count) if count > 0 else 0.0,
            "hostility_rate": round4(stats["hostile_count"] / count) if count > 0 else 0.0,
            "example_ids": stats["example_ids"],
        })
        # Track which items are actually used
        used_item_ids.update(stats["example_ids"])
    
    # Only include items that are referenced in example_ids (space efficiency)
    items_mapping: Dict[str, Dict[str, str]] = {
        item_id: all_items[item_id]
        for item_id in sorted(used_item_ids)
        if item_id in all_items
    }
    
    # Build final report
    report: Dict[str, Any] = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "dataset": {
            "total_items": total_items,
            "date_min": date_min,
            "date_max": date_max,
        },
        "topics": topics_list,
        "items": items_mapping,
        "intrusion": None,
        "self_portrayal": None,
    }
    
    return report



# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

def main() -> None:
    # Validate inputs exist
    if not SRC_TOPICS_CSV.exists():
        raise SystemExit(f"Missing: {SRC_TOPICS_CSV.resolve()}\nRun steps 1-3 first.")
    
    # Load topics CSV
    print(f"Loading: {SRC_TOPICS_CSV}")
    topics_data = load_topics_csv(SRC_TOPICS_CSV)
    print(f"  Loaded {len(topics_data)} items")
    
    # Load timeline for date range (optional)
    date_min, date_max = None, None
    if SRC_TIMELINE_JSON.exists():
        print(f"Loading: {SRC_TIMELINE_JSON}")
        timeline = load_timeline_json(SRC_TIMELINE_JSON)
        date_min, date_max = extract_date_range(timeline)
        print(f"  Date range: {date_min} -> {date_max}")
    else:
        print(f"Note: {SRC_TIMELINE_JSON} not found, date range will be null")
    
    # Build report
    report = build_report(topics_data, date_min, date_max)
    
    # Write output
    SITE_REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)
    SITE_REPORT_PATH.write_text(
        json.dumps(report, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8"
    )
    
    print(f"\nWrote: {SITE_REPORT_PATH.resolve()}")
    print(f"  Total items: {report['dataset']['total_items']}")
    print(f"  Topics: {len(report['topics'])}")
    print(f"  Intrusion: null (not computed)")
    print(f"  Self-portrayal: null (not computed)")


if __name__ == "__main__":
    main()
