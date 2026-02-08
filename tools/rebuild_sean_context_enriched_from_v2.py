#!/usr/bin/env python3
"""
Rebuild Sean context-enriched dataset from GraphQL v2 comments.

Input:
  fb_extract_out/comments_graphql_v2.jsonl

Output:
  fb_extract_out/sean_context_enriched.v2.jsonl

Notes:
- This replaces v1 comment inputs entirely.
- Timestamps are authoritative from GraphQL.
- Schema matches downstream contract expectations.
"""

from __future__ import annotations

import json
from pathlib import Path
from datetime import datetime, timezone

# --- Config ---
IN_V2 = Path("fb_extract_out/comments_graphql_v2.jsonl")
OUT_V2 = Path("fb_extract_out/sean_context_enriched.v2.jsonl")
SEAN_NAME = "Sean Roy"
GROUP_PREFIX = "https://www.facebook.com/groups/3970539883001618/posts/"

# --- Sentiment (VADER if available) ---
try:
    from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
    _analyzer = SentimentIntensityAnalyzer()
    def sentiment(text: str):
        s = _analyzer.polarity_scores(text or "")
        return s["compound"], s["pos"], s["neu"], s["neg"]
except Exception:
    def sentiment(text: str):
        return 0.0, 0.0, 1.0, 0.0


def main() -> int:
    if not IN_V2.exists():
        raise SystemExit(f"[ERR] missing input: {IN_V2}")

    rows = []
    with IN_V2.open("r", encoding="utf-8") as f:
        for line in f:
            if not line.strip():
                continue
            r = json.loads(line)
            if r.get("author_name") != SEAN_NAME:
                continue
            rows.append(r)

    # Deterministic ordering: timestamp, post, legacy_fbid
    def sort_key(r):
        return (
            r.get("timestamp_parsed") or "",
            r.get("post_id") or "",
            r.get("legacy_fbid") or "",
        )

    rows.sort(key=sort_key)

    OUT_V2.parent.mkdir(parents=True, exist_ok=True)

    out_rows = []
    for idx, r in enumerate(rows):
        body = r.get("body_text") or ""
        compound, pos, neu, neg = sentiment(body)

        out = {
            "joined_item_type": "comment",
            "author": SEAN_NAME,
            "body": body,
            "body_len": len(body),
            "timestamp_parsed": r.get("timestamp_parsed"),
            "timestamp_raw": r.get("created_time"),
            "captured_at": r.get("timestamp_parsed"),
            "thread_permalink": f"{GROUP_PREFIX}{r.get('post_id')}/",
            "parent_author_type": "self",
            "parent_context": r.get("post_id"),
            "thread_primary_topic": None,
            "topics": [],
            "topics_raw": [],
            "sentiment_compound": compound,
            "sentiment_pos": pos,
            "sentiment_neu": neu,
            "sentiment_neg": neg,
            "source_index": idx,
        }
        out_rows.append(out)

    with OUT_V2.open("w", encoding="utf-8") as f:
        for r in out_rows:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")

    print(f"[OK] wrote: {OUT_V2}")
    print("[SUMMARY]")
    print(f"  sean_comments: {len(out_rows)}")
    print(f"  first_ts: {out_rows[0]['timestamp_parsed'] if out_rows else None}")
    print(f"  last_ts:  {out_rows[-1]['timestamp_parsed'] if out_rows else None}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
