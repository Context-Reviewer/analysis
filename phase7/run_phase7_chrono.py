#!/usr/bin/env python3
"""
Phase 7 Chrono Timeline (anchored-relative)

Goal:
- Provide an approximate chronological ordering for target-only records
  by anchoring relative UI phrases (e.g., "2 weeks ago") to capture
  timestamps from frozen extractor observations.

Inputs:
- fb_extract_out/phase5_target_only.jsonl   (analysis repo, target-only)
- /mnt/c/dev/repos/fb_extractor/fb_extract_out/observations.jsonl (frozen extractor output)

Output:
- phase7/out/<run_id>/phase7_chrono_timeline.json

Determinism:
- No network calls
- Stable parsing
- Stable URL normalization
- Stable ordering (approx_ts_utc, then input_ordinal, then corpus_id)
"""

from __future__ import annotations

import argparse
import json
import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlsplit, urlunsplit


RX_AGO = re.compile(
    r"\b(\d+)\s+(minute|minutes|hour|hours|day|days|week|weeks|month|months|year|years)\s+ago\b",
    re.IGNORECASE,
)


def load_jsonl(path: Path) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            s = line.strip()
            if not s:
                continue
            rows.append(json.loads(s))
    return rows


def norm_url(u: str) -> str:
    """
    Deterministic URL normalization:
    - drop fragment
    - keep scheme/netloc/path
    - keep query but strip noisy __cft__ tail if present
    """
    sp = urlsplit(u)
    q = sp.query or ""
    if "__cft__" in q:
        q = q.split("&__cft__")[0]
    return urlunsplit((sp.scheme, sp.netloc, sp.path, q, ""))


def parse_ts_utc(s: str) -> datetime:
    # observations ts_utc looks like: 2026-02-09T19:11:02.842067+00:00
    dt = datetime.fromisoformat(s)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def delta_from_ago_phrase(text: str) -> Optional[timedelta]:
    m = RX_AGO.search(text or "")
    if not m:
        return None
    n = int(m.group(1))
    unit = m.group(2).lower()
    if unit.startswith("minute"):
        return timedelta(minutes=n)
    if unit.startswith("hour"):
        return timedelta(hours=n)
    if unit.startswith("day"):
        return timedelta(days=n)
    if unit.startswith("week"):
        return timedelta(weeks=n)
    if unit.startswith("month"):
        # deterministic approximation: 30 days per month
        return timedelta(days=30 * n)
    if unit.startswith("year"):
        # deterministic approximation: 365 days per year
        return timedelta(days=365 * n)
    return None


def build_anchor_map(observations_path: Path) -> Dict[str, datetime]:
    """
    Build mapping: norm_url -> anchor_ts_utc

    If multiple ts_utc exist for the same URL, we choose the *latest* capture
    timestamp deterministically (max).
    """
    anchors: Dict[str, datetime] = {}
    with observations_path.open("r", encoding="utf-8", errors="replace") as f:
        for line in f:
            s = line.strip()
            if not s:
                continue
            o = json.loads(s)
            if "ts_utc" not in o:
                continue
            u = o.get("final_url") or o.get("url")
            if not isinstance(u, str) or not u:
                continue
            key = norm_url(u)
            ts = parse_ts_utc(o["ts_utc"])
            prev = anchors.get(key)
            if prev is None or ts > prev:
                anchors[key] = ts
    return anchors


def build_phase7_chrono_timeline(
    p5_rows: List[Dict[str, Any]],
    anchors: Dict[str, datetime],
) -> Dict[str, Any]:
    """
    Output schema:
    {
      "schema": "phase7_chrono_timeline-1.0",
      "summary": {...},
      "items": [ ... sorted ... ]
    }
    """
    items: List[Dict[str, Any]] = []

    for idx, r in enumerate(p5_rows):
        prov = r.get("provenance", {})
        thread_url = prov.get("thread_url") if isinstance(prov, dict) else None
        aria = prov.get("aria_label") if isinstance(prov, dict) else None

        norm_thread_url = norm_url(thread_url) if isinstance(thread_url, str) else None
        anchor = anchors.get(norm_thread_url) if norm_thread_url else None

        delta = delta_from_ago_phrase(aria or "")
        if anchor and delta:
            approx = anchor - delta
            kind = "anchored_relative"
        elif anchor:
            approx = anchor
            kind = "capture_only"
        else:
            approx = None
            kind = "unknown"

        items.append(
            {
                "corpus_id": r.get("corpus_id"),
                "thread_id": r.get("thread_id"),
                "author": r.get("author"),
                "is_reply": r.get("is_reply"),
                "text": r.get("text"),
                "input_ordinal": idx,  # Phase5 is already ordered deterministically
                "thread_url_norm": norm_thread_url,
                "anchor_ts_utc": anchor.isoformat() if anchor else None,
                "relative_text": aria,
                "approx_ts_utc": approx.isoformat() if approx else None,
                "timestamp_kind": kind,
            }
        )

    # Deterministic sort: approx_ts_utc (None last), then input_ordinal, then corpus_id
    def sort_key(it: Dict[str, Any]) -> Tuple[int, str, int, str]:
        ats = it.get("approx_ts_utc")
        if ats is None:
            return (1, "9999-12-31T23:59:59+00:00", int(it["input_ordinal"]), str(it.get("corpus_id") or ""))
        return (0, ats, int(it["input_ordinal"]), str(it.get("corpus_id") or ""))

    items_sorted = sorted(items, key=sort_key)

    # Summary
    total = len(items_sorted)
    anchored = sum(1 for x in items_sorted if x["timestamp_kind"] == "anchored_relative")
    capture_only = sum(1 for x in items_sorted if x["timestamp_kind"] == "capture_only")
    unknown = sum(1 for x in items_sorted if x["timestamp_kind"] == "unknown")

    return {
        "schema": "phase7_chrono_timeline-1.0",
        "summary": {
            "records_total": total,
            "anchored_relative": anchored,
            "capture_only": capture_only,
            "unknown": unknown,
        },
        "items": items_sorted,
    }


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--phase5",
        default="fb_extract_out/phase5_target_only.jsonl",
        help="Path to phase5_target_only.jsonl (analysis repo)",
    )
    ap.add_argument(
        "--observations",
        default="/mnt/c/dev/repos/fb_extractor/fb_extract_out/observations.jsonl",
        help="Path to frozen extractor observations.jsonl",
    )
    ap.add_argument(
        "--out",
        default="phase7/out/phase7_chrono",
        help="Output directory (will be created)",
    )
    args = ap.parse_args()

    phase5_path = Path(args.phase5)
    obs_path = Path(args.observations)
    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)

    p5_rows = load_jsonl(phase5_path)
    anchors = build_anchor_map(obs_path)
    out = build_phase7_chrono_timeline(p5_rows, anchors)

    out_path = out_dir / "phase7_chrono_timeline.json"
    out_path.write_text(json.dumps(out, ensure_ascii=False, sort_keys=True, indent=2) + "\n", encoding="utf-8")
    print(f"[phase7] OK: wrote={out_path} records={len(p5_rows)} anchors={len(anchors)}")


if __name__ == "__main__":
    main()
