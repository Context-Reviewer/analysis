# step2_build_timeline.py
# Purpose (Step 2):
# - Read fb_extract_out/posts_normalized_sean.jsonl (posts)
# - Read fb_extract_out/comments_normalized_sean.jsonl (comments)
# - Merge and build a chronological timeline dataset
# - Export JSON + CSV + stats
#
# Governance:
# - Deterministic merge using locked sort key tuple
# - No filtering by content, no scoring, no inference
# - Sort key: (timestamp_parsed, item_type, permalink, source_index)

from __future__ import annotations

import csv
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


OUT_DIR = Path("fb_extract_out")
SRC_POSTS = OUT_DIR / "posts_normalized_sean.jsonl"
SRC_COMMENTS = OUT_DIR / "comments_normalized_sean.jsonl"

OUT_JSON = OUT_DIR / "sean_timeline.json"
OUT_CSV = OUT_DIR / "sean_timeline.csv"
OUT_STATS = OUT_DIR / "sean_stats.json"

BODY_PREVIEW_CHARS = 240

# Sentinel for missing timestamps (deterministic sort)
MISSING_TIMESTAMP_SENTINEL = "9999-12-31T23:59:59Z"


def parse_iso(dt: Optional[str]) -> Optional[datetime]:
    if not dt or not isinstance(dt, str):
        return None

    s = dt.strip()
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"

    try:
        d = datetime.fromisoformat(s)
    except Exception:
        return None

    # Normalize: always offset-aware UTC
    if d.tzinfo is None:
        d = d.replace(tzinfo=timezone.utc)
    else:
        d = d.astimezone(timezone.utc)

    return d

def load_jsonl(path: Path) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            obj = json.loads(line)
        except Exception:
            continue
        if isinstance(obj, dict):
            rows.append(obj)
    return rows


def best_time_key(rec: Dict[str, Any]) -> Tuple[int, Optional[datetime], str]:
    """
    Returns a sortable key:
    - primary: priority (0 best -> 2 worst)
    - dt: datetime (may be None)
    - source: label for debugging
    """
    dt_parsed = parse_iso(rec.get("timestamp_parsed"))
    if dt_parsed:
        return (0, dt_parsed, "timestamp_parsed")

    dt_cap = parse_iso(rec.get("captured_at"))
    if dt_cap:
        return (1, dt_cap, "captured_at")

    return (2, None, "none")


def safe_preview(body: str, n: int = BODY_PREVIEW_CHARS) -> str:
    body = (body or "").strip()
    if len(body) <= n:
        return body
    return body[:n].rstrip() + "…"


def main() -> None:
    # --- Load posts ---
    posts_rows: List[Dict[str, Any]] = []
    if SRC_POSTS.exists():
        posts_rows = load_jsonl(SRC_POSTS)
        # Assign item_type and source_index if missing
        for idx, r in enumerate(posts_rows):
            # Treat None as missing (adapter may emit nulls)
            if r.get("item_type") is None:
                r["item_type"] = "post"
            if r.get("source_index") is None:
                r["source_index"] = idx
    
    # --- Load comments ---
    comments_rows: List[Dict[str, Any]] = []
    if SRC_COMMENTS.exists():
        comments_rows = load_jsonl(SRC_COMMENTS)
        # Backfill item_type and source_index deterministically in case adapter
        # emitted nulls or omitted fields.
        for idx, r in enumerate(comments_rows):
            if r.get("item_type") is None:
                r["item_type"] = "comment"
            if r.get("source_index") is None:
                r["source_index"] = idx
    
    # --- Merge ---
    all_rows = posts_rows + comments_rows
    
    if not all_rows:
        raise SystemExit(f"No records found in posts or comments.")
    
    # Filter: keep only non-empty bodies
    kept: List[Dict[str, Any]] = []
    dropped_empty = 0
    for r in all_rows:
        body = (r.get("body") or "").strip()
        if len(body) < 5:
            dropped_empty += 1
            continue
        kept.append(r)

    # Sort using best time available
    enriched: List[Dict[str, Any]] = []
    missing_time = 0

    for r in kept:
        prio, dt, source = best_time_key(r)
        if dt is None:
            missing_time += 1

        enriched.append({
            "permalink": r.get("permalink") or "",
            "author": r.get("author") or "",
            "item_type": r.get("item_type") or "",
            "source_index": r.get("source_index"),
            "parent_context": r.get("parent_context") or "",
            "timestamp_raw": r.get("timestamp_raw") or "",
            "timestamp_parsed": r.get("timestamp_parsed") or "",
            "timestamp_kind": r.get("timestamp_kind") or "",
            "captured_at": r.get("captured_at") or "",
            "time_source": source,
            "time_priority": prio,
            "body": (r.get("body") or "").strip(),
            "body_preview": safe_preview((r.get("body") or "").strip()),
            "body_len": int(r.get("body_len") or 0),
            "body_confidence": int(r.get("body_confidence") or 0),
            "body_method": r.get("body_method") or "",
            "body_notes": r.get("body_notes") or [],
        })

    # Sort: those with real dt first, missing dt last but stable
    # Locked sort key tuple per governance:
    # (timestamp_parsed, item_type, permalink, source_index)
    def sort_key(x: Dict[str, Any]) -> Tuple[str, int, str, int]:
        ts = x.get("timestamp_parsed") or x.get("captured_at") or MISSING_TIMESTAMP_SENTINEL
        # item_type: "post" < "comment" (0 vs 1)
        item_type_order = 0 if x.get("item_type") == "post" else 1
        permalink = x.get("permalink") or ""
        source_idx = int(x.get("source_index", 0))
        return (ts, item_type_order, permalink, source_idx)

    enriched.sort(key=sort_key)

    # Stats
    dt_first = None
    dt_last = None
    for x in enriched:
        dt = parse_iso(x.get("timestamp_parsed")) or parse_iso(x.get("captured_at"))
        if dt is None:
            continue
        if dt_first is None or dt < dt_first:
            dt_first = dt
        if dt_last is None or dt > dt_last:
            dt_last = dt

    stats = {
        "input_posts": len(posts_rows),
        "input_comments": len(comments_rows),
        "total_input": len(all_rows),
        "kept_nonempty": len(kept),
        "dropped_empty_body": dropped_empty,
        "missing_any_time": missing_time,
        "time_coverage_first": dt_first.isoformat(timespec="seconds") if dt_first else "",
        "time_coverage_last": dt_last.isoformat(timespec="seconds") if dt_last else "",
    }

    OUT_DIR.mkdir(parents=True, exist_ok=True)

    OUT_JSON.write_text(json.dumps(enriched, ensure_ascii=False, indent=2), encoding="utf-8")
    OUT_STATS.write_text(json.dumps(stats, ensure_ascii=False, indent=2), encoding="utf-8")

    # CSV export (Excel-friendly)
    with OUT_CSV.open("w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow([
            "i",
            "time_source",
            "timestamp_parsed",
            "captured_at",
            "timestamp_raw",
            "permalink",
            "body_len",
            "body_confidence",
            "body_preview",
        ])
        for i, x in enumerate(enriched, start=1):
            w.writerow([
                i,
                x.get("time_source", ""),
                x.get("timestamp_parsed", ""),
                x.get("captured_at", ""),
                x.get("timestamp_raw", ""),
                x.get("permalink", ""),
                x.get("body_len", 0),
                x.get("body_confidence", 0),
                x.get("body_preview", ""),
            ])

    print("Step 2 complete.")
    print(f"Read posts:    {SRC_POSTS.resolve()} ({len(posts_rows)} records)")
    print(f"Read comments: {SRC_COMMENTS.resolve()} ({len(comments_rows)} records)")
    print(f"Wrote timeline JSON: {OUT_JSON.resolve()}")
    print(f"Wrote timeline CSV:  {OUT_CSV.resolve()}")
    print(f"Wrote stats JSON:    {OUT_STATS.resolve()}")
    print("")
    print("Stats:")
    for k, v in stats.items():
        print(f"  {k}: {v}")


if __name__ == "__main__":
    main()




