#!/usr/bin/env python3
"""
tools/context_enrich.py
----------------------
Build a context-enriched layer for downstream behavioral metrics.

Governance:
- Deterministic output ordering.
- Do NOT trust sean_topics.csv item_type (it may be noisy). Determine type by join target.
- Join rules are explicit and fail loud if ambiguous.
- sean_topics.csv 'i' is strictly 1-based row numbering. 'source_index' in timeline is 0-based.
- Mapping: source_index = i - 1

Inputs:
- fb_extract_out/sean_timeline.json              (merged posts+comments; posts have permalink, comments have source_index)
- fb_extract_out/sean_topics.csv                 (topic+sentiment rows; contains i + permalink)
- fb_extract_out/posts_normalized_sean.jsonl      (Sean-authored root posts; used to classify thread ownership)

Output:
- fb_extract_out/sean_context_enriched.jsonl
"""

from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

OUT_DIR = Path("fb_extract_out")

TIMELINE_JSON = OUT_DIR / "sean_timeline.json"
TOPICS_CSV = OUT_DIR / "sean_topics.csv"
POSTS_SEAN_JSONL = OUT_DIR / "posts_normalized_sean.jsonl"

OUT_JSONL = OUT_DIR / "sean_context_enriched.jsonl"


def fail(msg: str, code: int = 2) -> None:
    raise SystemExit(f"[FAIL] {msg}")


def load_json(path: Path) -> Any:
    if not path.exists():
        fail(f"Missing required file: {path}")
    return json.loads(path.read_text(encoding="utf-8"))


def load_jsonl(path: Path) -> List[Dict[str, Any]]:
    if not path.exists():
        fail(f"Missing required file: {path}")
    rows: List[Dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
        line = line.strip()
        if not line:
            continue
        obj = json.loads(line)
        if isinstance(obj, dict):
            rows.append(obj)
    return rows


def parse_topics_field(s: str) -> List[str]:
    s = (s or "").strip()
    if not s:
        return []
    parts = [p.strip() for p in s.split("|") if p.strip()]
    return sorted(set(parts))


def coerce_float(v: Optional[str]) -> Optional[float]:
    if v is None:
        return None
    v = str(v).strip()
    if v == "":
        return None
    try:
        return float(v)
    except Exception:
        return None


def sort_key(rec: Dict[str, Any]) -> Tuple[str, str, int, str]:
    # Deterministic: timestamp_parsed, then captured_at, then sentinel
    t = rec.get("timestamp_parsed") or rec.get("captured_at") or "9999-12-31T23:59:59Z"
    thread = rec.get("thread_permalink") or rec.get("permalink") or ""
    si = int(rec.get("source_index", -1))
    kind = rec.get("joined_item_type") or ""
    return (t, thread, si, kind)


def main() -> None:
    ap = argparse.ArgumentParser(description="Context enrichment join for Context-Reviewer.")
    args = ap.parse_args()

    # --- Load timeline ---
    timeline = load_json(TIMELINE_JSON)
    if not isinstance(timeline, list):
        fail(f"Expected {TIMELINE_JSON} to be a JSON list, got: {type(timeline)}")

    comments_by_source_index: Dict[int, Dict[str, Any]] = {}
    posts_by_permalink: Dict[str, Dict[str, Any]] = {}

    for item in timeline:
        if not isinstance(item, dict):
            continue

        si = item.get("source_index")
        pl = item.get("permalink")

        # Comment index (unique)
        if item.get("item_type") == "comment" or (si is not None and item.get("item_type") != "post"):
            if si is None:
                continue
            k = int(si)
            if k in comments_by_source_index and comments_by_source_index[k] is not item:
                fail(f"Ambiguous timeline comment source_index={k}")
            comments_by_source_index[k] = item
            continue

        # Post index (unique)
        if pl:
            k = str(pl)
            if k in posts_by_permalink and posts_by_permalink[k] is not item:
                fail(f"Ambiguous timeline post permalink={k}")
            posts_by_permalink[k] = item

    # --- Load Sean-authored root posts (for thread ownership classification) ---
    sean_posts = load_jsonl(POSTS_SEAN_JSONL)
    sean_post_permalinks = set()
    for r in sean_posts:
        pl = r.get("permalink")
        if pl:
            sean_post_permalinks.add(str(pl))

    # --- Load topics CSV ---
    if not TOPICS_CSV.exists():
        fail(f"Missing required file: {TOPICS_CSV}")

    topic_rows: List[Dict[str, Any]] = []
    with TOPICS_CSV.open("r", encoding="utf-8", newline="") as f:
        rdr = csv.DictReader(f)
        required = {"i", "permalink", "topics"}
        missing_cols = required - set(rdr.fieldnames or [])
        if missing_cols:
            fail(f"{TOPICS_CSV} missing required columns: {sorted(missing_cols)}")
        for row in rdr:
            topic_rows.append(row)

    # Validate i is 1..N unique (Strict Governance)
    i_vals = []
    try:
        i_vals = [int(r["i"]) for r in topic_rows]
    except ValueError as e:
        fail(f"topics.csv contains non-integer i values: {e}")
        
    n = len(i_vals)
    if n > 0:
        min_i, max_i = min(i_vals), max(i_vals)
        if not (min_i == 1 and max_i == n and len(set(i_vals)) == n):
            fail(f"topics.csv i indexing not 1..N unique: rows={n} min={min_i} max={max_i} unique={len(set(i_vals))}")

    # Aggregate deterministic topic set per thread permalink (optional context label)
    thread_topics: Dict[str, List[str]] = {}
    for row in topic_rows:
        pl = (row.get("permalink") or "").strip()
        if not pl:
            continue
        topics = parse_topics_field(row.get("topics") or "")
        if not topics:
            continue
        existing = thread_topics.get(pl, [])
        thread_topics[pl] = sorted(set(existing).union(topics))

    enriched: List[Dict[str, Any]] = []
    join_mode_counts = {"comment_i_to_source_index": 0, "post_permalink": 0}
    missing: List[str] = []

    for row in topic_rows:
        # Required fields handled by loading logic, but check permalink
        pl = (row.get("permalink") or "").strip()
        if not pl:
            missing.append(f"i={row.get('i')} (missing permalink)")
            continue

        i_val = int(row["i"]) # Validated above
        topics_list = parse_topics_field(row.get("topics") or "")
        thread_primary_topic = (thread_topics.get(pl) or [None])[0]

        # Join Logic:
        # 1. Try comment join by source_index (i - 1)
        si_key = i_val - 1
        tl_item = comments_by_source_index.get(si_key)
        joined_item_type = None

        if tl_item:
            joined_item_type = "comment"
            join_mode_counts["comment_i_to_source_index"] += 1
        else:
            # 2. Try post join by permalink
            tl_item = posts_by_permalink.get(pl)
            if tl_item:
                joined_item_type = "post"
                join_mode_counts["post_permalink"] += 1
        
        if not tl_item:
            missing.append(f"i={i_val} permalink={pl}")
            continue

        # Use existing source_index from timeline if present
        source_index_out = tl_item.get("source_index")

        # Thread ownership classification:
        # thread_permalink is always the root post permalink we are in.
        thread_permalink = pl
        parent_author_type = "self" if thread_permalink in sean_post_permalinks else "other"

        rec: Dict[str, Any] = {
            "joined_item_type": joined_item_type,   # derived, trusted
            "source_index": source_index_out,       # only for comment joins
            "thread_permalink": thread_permalink,
            "parent_context": tl_item.get("parent_context") if joined_item_type == "comment" else None,

            # timeline fields
            "author": tl_item.get("author"),
            "body": tl_item.get("body"),
            "body_len": tl_item.get("body_len"),
            "timestamp_parsed": tl_item.get("timestamp_parsed"),
            "timestamp_raw": tl_item.get("timestamp_raw"),
            "captured_at": tl_item.get("captured_at"),

            # topics + sentiment
            "topics": topics_list,
            "topics_raw": (row.get("topics") or ""),
            "sentiment_compound": coerce_float(row.get("sentiment_compound")),
            "sentiment_pos": coerce_float(row.get("sentiment_pos")),
            "sentiment_neg": coerce_float(row.get("sentiment_neg")),
            "sentiment_neu": coerce_float(row.get("sentiment_neu")),

            # context summary
            "parent_author_type": parent_author_type,
            "thread_primary_topic": thread_primary_topic,
        }

        enriched.append(rec)

    if missing:
        preview = missing[:20]
        fail(
            f"Join failure: {len(missing)} topics rows could not join to timeline.\nFirst missing: {preview}"
        )

    enriched.sort(key=sort_key)

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    with OUT_JSONL.open("w", encoding="utf-8", newline="\n") as f:
        for rec in enriched:
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")

    # Stats
    total = len(enriched)
    joined_comments = sum(1 for r in enriched if r.get("joined_item_type") == "comment")
    joined_posts = sum(1 for r in enriched if r.get("joined_item_type") == "post")
    replies_to_self = sum(1 for r in enriched if r.get("joined_item_type") == "comment" and r.get("parent_author_type") == "self")
    replies_to_others = sum(1 for r in enriched if r.get("joined_item_type") == "comment" and r.get("parent_author_type") == "other")
    with_parent_context = sum(1 for r in enriched if r.get("joined_item_type") == "comment" and r.get("parent_context"))
    with_thread_topic = sum(1 for r in enriched if r.get("thread_primary_topic") is not None)

    print(f"Loading timeline: {TIMELINE_JSON}")
    print(f"  Timeline comments indexed: {len(comments_by_source_index)}")
    print(f"  Timeline posts indexed:    {len(posts_by_permalink)}")
    print(f"Loading topics: {TOPICS_CSV}")
    print(f"  Topics rows: {len(topic_rows)}")
    print(f"Writing {len(enriched)} enriched items to {OUT_JSONL}")
    print("")
    print("Context Enrichment Summary:")
    print(f"  Total enriched rows: {total}")
    print(f"  Joined via comment i->source_index: {join_mode_counts['comment_i_to_source_index']}")
    print(f"  Joined via post permalink:          {join_mode_counts['post_permalink']}")
    print(f"  Comments: {joined_comments} | Posts: {joined_posts}")
    print(f"  Comment replies to Self (thread is Sean post):  {replies_to_self}")
    print(f"  Comment replies to Others (thread not Sean post): {replies_to_others}")
    print(f"  Comment rows with parent_context: {with_parent_context} ({(with_parent_context/joined_comments*100.0) if joined_comments else 0.0:.1f}%)")
    print(f"  Rows with thread_primary_topic: {with_thread_topic} ({(with_thread_topic/total*100.0) if total else 0.0:.1f}%)")


if __name__ == "__main__":
    main()
