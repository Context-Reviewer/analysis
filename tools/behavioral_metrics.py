#!/usr/bin/env python3
"""
tools/behavioral_metrics.py
---------------------------
Compute deterministic behavioral metrics from enriched timeline data.

Governance:
- Deterministic sorting and output.
- No prose, no AI interpretation.
- Fixed rounding (4 decimals).
- Fail-loud on missing data or structure violations.

Input: fb_extract_out/sean_context_enriched.v2.jsonl
Output: docs/data/behavioral_metrics.json
"""

import hashlib
import json
import statistics
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

IN_FILE = Path("fb_extract_out/sean_context_enriched.v2.jsonl")
OUT_FILE = Path("docs/data/behavioral_metrics.json")

NEGATIVE_THRESHOLD = -0.05

def fail(msg: str) -> None:
    print(f"[FAIL] {msg}")
    exit(1)

def get_file_sha256(path: Path) -> str:
    if not path.exists():
        fail(f"Input file not found: {path}")
    sha256 = hashlib.sha256()
    with path.open("rb") as f:
        while True:
            chunk = f.read(65536)
            if not chunk:
                break
            sha256.update(chunk)
    return f"sha256:{sha256.hexdigest()}"

def safe_mean(values: List[float]) -> Optional[float]:
    if not values:
        return None
    return float(round(statistics.mean(values), 4))

def safe_median(values: List[float]) -> Optional[float]:
    if not values:
        return None
    return float(round(statistics.median(values), 4))

def calc_negative_rate(values: List[float]) -> float:
    if not values:
        return 0.0
    count = sum(1 for v in values if v <= NEGATIVE_THRESHOLD)
    return float(round(count / len(values), 4))

def canonical_ts(r: Dict[str, Any]) -> Optional[str]:
    return r.get("timestamp_parsed") or r.get("captured_at") or None

def sort_key_exemplar(r: Dict[str, Any]) -> tuple:
    # (sentiment_compound asc, timestamp asc, thread_permalink asc, source_index asc)
    sc = r.get("sentiment_compound")
    if sc is None: sc = 999.0 
    ts = canonical_ts(r) or "9999-12-31"
    tp = r.get("thread_permalink") or ""
    si = r.get("source_index") or -1
    return (sc, ts, tp, si)

EXEMPLAR_KEYS = (
    "source_index",
    "thread_permalink",
    "parent_context",
    "parent_author_type",
    "sentiment_compound",
    "topics",
    "timestamp_parsed",
    "captured_at",
    "thread_primary_topic",
)

def minimize_exemplar(r: Dict[str, Any]) -> Dict[str, Any]:
    out = {k: r.get(k) for k in EXEMPLAR_KEYS}
    out["timestamp"] = canonical_ts(r)
    out.pop("timestamp_parsed", None)
    out.pop("captured_at", None)
    return out

def main():
    if not IN_FILE.exists():
        fail(f"Missing input: {IN_FILE}")

    # Metadata
    dataset_fingerprint = get_file_sha256(IN_FILE)
    
    records = []
    with IN_FILE.open("r", encoding="utf-8") as f:
        for line in f:
            if line.strip():
                try:
                    records.append(json.loads(line))
                except json.JSONDecodeError:
                    fail(f"Invalid JSONL in {IN_FILE}")

    if not records:
        fail("Input file is empty")

    # Dates
    valid_timestamps = [canonical_ts(r) for r in records if canonical_ts(r)]
    valid_timestamps.sort()
    
    date_min = valid_timestamps[0] if valid_timestamps else None
    date_max = valid_timestamps[-1] if valid_timestamps else None
    timestamp_nonnull_count = len(valid_timestamps)

    # Counts
    items_total = len(records)
    comments_total = sum(1 for r in records if r.get("joined_item_type") == "comment")
    posts_total = sum(1 for r in records if r.get("joined_item_type") == "post")
    
    reply_to_self_items = [r for r in records if r.get("parent_author_type") == "self"]
    reply_to_other_items = [r for r in records if r.get("parent_author_type") == "other"]
    
    reply_to_self_count = len(reply_to_self_items)
    reply_to_other_count = len(reply_to_other_items)

    # Sentiment Collection
    all_sentiments = []
    self_sentiments = []
    other_sentiments = []

    # Topics Aggregation
    topic_data: Dict[str, Dict] = {}

    for r in records:
        s = r.get("sentiment_compound")
        if s is not None:
            all_sentiments.append(s)
            pat = r.get("parent_author_type")
            if pat == "self":
                self_sentiments.append(s)
            elif pat == "other":
                other_sentiments.append(s)

        # Topics
        topics = r.get("topics", [])
        for t in topics:
            if t not in topic_data:
                topic_data[t] = {"count": 0, "sentiments": [], "self": 0, "other": 0, "exemplar_candidates": []}
            
            td = topic_data[t]
            td["count"] += 1
            if s is not None:
                td["sentiments"].append(s)
            
            pat = r.get("parent_author_type")
            if pat == "self":
                td["self"] += 1
            elif pat == "other":
                td["other"] += 1
            
            td["exemplar_candidates"].append(r)

    # Sort topics alphabetically
    sorted_topics = sorted(topic_data.keys())

    # Build Topics structures
    topic_counts = {t: topic_data[t]["count"] for t in sorted_topics}
    topic_share = {t: round(topic_data[t]["count"] / items_total, 4) if items_total > 0 else 0.0 for t in sorted_topics}
    
    by_topic = {}
    for t in sorted_topics:
        td = topic_data[t]
        s_vals = td["sentiments"]
        by_topic[t] = {
            "count": td["count"],
            "mean_compound": safe_mean(s_vals),
            "negative_rate": calc_negative_rate(s_vals),
            "reply_to_self_count": td["self"],
            "reply_to_other_count": td["other"]
        }

    # Exemplars
    valid_sentiment_records = [r for r in records if r.get("sentiment_compound") is not None]
    valid_sentiment_records.sort(key=sort_key_exemplar)
    
    most_negative_overall = valid_sentiment_records[:5]
    
    valid_other = [r for r in valid_sentiment_records if r.get("parent_author_type") == "other"]
    valid_other.sort(key=sort_key_exemplar)
    most_negative_reply_to_other = valid_other[:5]

    # Most negative by topic (top 3 by count)
    topics_by_count = sorted(sorted_topics, key=lambda x: (-topic_data[x]["count"], x))
    top_3_topics = topics_by_count[:3]
    
    most_negative_by_topic = {}
    for t in top_3_topics:
        cands = [r for r in topic_data[t]["exemplar_candidates"] if r.get("sentiment_compound") is not None]
        cands.sort(key=sort_key_exemplar)
        most_negative_by_topic[t] = cands[:5]

    # Minimization
    most_negative_overall = [minimize_exemplar(r) for r in most_negative_overall]
    most_negative_reply_to_other = [minimize_exemplar(r) for r in most_negative_reply_to_other]
    for t in list(most_negative_by_topic.keys()):
        most_negative_by_topic[t] = [minimize_exemplar(r) for r in most_negative_by_topic[t]]

    output = {
        "schema_version": "behavioral_metrics-0.1",
        "dataset": {
            "input_path": str(IN_FILE).replace("\\", "/"),
            "record_count": items_total,
            "date_min": date_min,
            "date_max": date_max,
            "timestamp_source": "timestamp_parsed|captured_at",
            "timestamp_nonnull_count": timestamp_nonnull_count,
            "dataset_fingerprint": dataset_fingerprint
        },
        "definitions": {
            "negative_compound_threshold": NEGATIVE_THRESHOLD,
            "negative_rate": "share of items with sentiment_compound <= negative_compound_threshold",
            "reply_to_self": "parent_author_type == 'self'",
            "reply_to_other": "parent_author_type == 'other'",
            "topic_presence": "topic appears in item.topics list"
        },
        "counts": {
            "items_total": items_total,
            "comments_total": comments_total,
            "posts_total": posts_total,
            "reply_to_self": reply_to_self_count,
            "reply_to_other": reply_to_other_count
        },
        "sentiment": {
            "overall": {
                "mean_compound": safe_mean(all_sentiments),
                "median_compound": safe_median(all_sentiments),
                "negative_rate": calc_negative_rate(all_sentiments)
            },
            "by_parent_author_type": {
                "self": {
                    "count": len(self_sentiments),
                    "mean_compound": safe_mean(self_sentiments),
                    "negative_rate": calc_negative_rate(self_sentiments)
                },
                "other": {
                    "count": len(other_sentiments),
                    "mean_compound": safe_mean(other_sentiments),
                    "negative_rate": calc_negative_rate(other_sentiments)
                }
            }
        },
        "topics": {
            "topic_counts": topic_counts,
            "topic_share": topic_share,
            "by_topic": by_topic
        },
        "exemplars": {
            "most_negative_overall": most_negative_overall,
            "most_negative_reply_to_other": most_negative_reply_to_other,
            "most_negative_by_topic": most_negative_by_topic
        }
    }

    OUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    with OUT_FILE.open("w", encoding="utf-8") as f:
        json.dump(output, f, indent=2, ensure_ascii=False)
        f.write("\n")

    print(f"Generated metrics: {OUT_FILE}")
    print(f"  Records: {items_total}")
    print(f"  Fingerprint: {dataset_fingerprint}")

if __name__ == "__main__":
    main()


