#!/usr/bin/env python3
import json
import csv
from pathlib import Path

INP = Path("fb_extract_out/sean_context_enriched.jsonl")
OUT_JSON = Path("fb_extract_out/sean_timeline.json")
OUT_CSV  = Path("fb_extract_out/sean_timeline.csv")

def main():
    if not INP.exists():
        raise SystemExit(f"[ERR] missing input: {INP}")

    rows=[]
    with INP.open("r", encoding="utf-8") as f:
        for line in f:
            if line.strip():
                rows.append(json.loads(line))

    rows.sort(key=lambda r: (r.get("timestamp_parsed") or "", int(r.get("source_index") or 0)))

    timeline = []
    for r in rows:
        timeline.append({
            # Contract-required / commonly required
            "item_type": "comment",
            "joined_item_type": r.get("joined_item_type") or "comment",
            "timestamp_parsed": r.get("timestamp_parsed"),
            "timestamp_raw": r.get("timestamp_raw"),
            "captured_at": r.get("captured_at"),
            "parent_context": r.get("parent_context"),  # REQUIRED by contracts_timeline_semantics.py

            # Useful fields for UI/analysis
            "thread_permalink": r.get("thread_permalink"),
            "author": r.get("author"),
            "body": r.get("body"),
            "body_len": r.get("body_len"),
            "sentiment_compound": r.get("sentiment_compound"),
            "sentiment_pos": r.get("sentiment_pos"),
            "sentiment_neu": r.get("sentiment_neu"),
            "sentiment_neg": r.get("sentiment_neg"),
            "thread_primary_topic": r.get("thread_primary_topic"),
            "topics": r.get("topics"),
            "topics_raw": r.get("topics_raw"),
            "source_index": r.get("source_index"),
        })

    OUT_JSON.write_text(json.dumps(timeline, ensure_ascii=False, indent=2), encoding="utf-8")

    fieldnames = [
        "item_type",
        "joined_item_type",
        "timestamp_parsed",
        "timestamp_raw",
        "captured_at",
        "parent_context",
        "thread_permalink",
        "author",
        "body",
        "body_len",
        "sentiment_compound",
        "sentiment_pos",
        "sentiment_neu",
        "sentiment_neg",
        "thread_primary_topic",
        "source_index",
    ]

    with OUT_CSV.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for t in timeline:
            w.writerow({k: t.get(k) for k in fieldnames})

    print(f"[OK] wrote: {OUT_JSON}")
    print(f"[OK] wrote: {OUT_CSV}")
    print("[SUMMARY]")
    print(f"  rows: {len(timeline)}")
    if timeline:
        print(f"  first_ts: {timeline[0].get('timestamp_parsed')}")
        print(f"  last_ts:  {timeline[-1].get('timestamp_parsed')}")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
