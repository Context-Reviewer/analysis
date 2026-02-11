#!/usr/bin/env python3
"""
Phase 7C: Behavioral Profile (signals, not diagnosis)

Inputs:
- Phase 6 outputs directory containing:
  - phase6_normalized.jsonl
  - phase6_topics.json
  - phase6_tone.json
- Phase 7 chrono timeline:
  - phase7_chrono_timeline.json

Output:
- phase7/out/phase7_behavior/phase7_behavior_profile.json

Determinism:
- No wall clock
- Stable ordering
- JSON output uses sort_keys=True
"""

from __future__ import annotations

import argparse
import json
import re
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple


RX_REPLY_TO = re.compile(r"\bto\s+(.+?)'s\s+comment\b", re.IGNORECASE)


def load_json(path: Path) -> Dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def load_jsonl(path: Path) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            s = line.strip()
            if not s:
                continue
            out.append(json.loads(s))
    return out


def parse_iso(ts: Optional[str]) -> Optional[datetime]:
    if not ts:
        return None
    try:
        return datetime.fromisoformat(ts.replace("Z", "+00:00"))
    except Exception:
        return None


def extract_items_container(obj: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Phase 6 topic/tone outputs may store per-record rows under different keys.
    We accept common patterns deterministically.
    """
    for k in ("items", "records", "rows", "data"):
        v = obj.get(k)
        if isinstance(v, list) and all(isinstance(x, dict) for x in v):
            return v  # type: ignore[return-value]
    # fallback: if the object itself looks like an item list wrapped weirdly
    if isinstance(obj.get("output"), list) and all(isinstance(x, dict) for x in obj["output"]):
        return obj["output"]  # type: ignore[return-value]
    return []


def build_map_by_id(items: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    m: Dict[str, Dict[str, Any]] = {}
    for it in items:
        # Accept common id keys
        cid = it.get("id") or it.get("corpus_id") or it.get("item_id")
        if isinstance(cid, str) and cid:
            m[cid] = it
    return m


def safe_get_str(d: Dict[str, Any], *keys: str) -> Optional[str]:
    for k in keys:
        v = d.get(k)
        if isinstance(v, str):
            return v
    return None


def safe_get_list(d: Dict[str, Any], *keys: str) -> List[Any]:
    for k in keys:
        v = d.get(k)
        if isinstance(v, list):
            return v
    return []


def summarize_distribution(counter: Counter, top_n: int = 10) -> List[Dict[str, Any]]:
    total = sum(counter.values())
    out = []
    for k, v in counter.most_common(top_n):
        out.append({"key": k, "count": v, "share": (v / total) if total else 0.0})
    return out


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--phase6-out", required=True, help="Phase 6 output directory (e.g., phase6/out/phase6_71ada6b9)")
    ap.add_argument("--chrono", required=True, help="Path to phase7_chrono_timeline.json")
    ap.add_argument("--out", default="phase7/out/phase7_behavior", help="Output directory")
    args = ap.parse_args()

    p6 = Path(args.phase6_out)
    chrono_path = Path(args.chrono)
    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)

    norm_path = p6 / "phase6_normalized.jsonl"
    topics_path = p6 / "phase6_topics.json"
    tone_path = p6 / "phase6_tone.json"

    records = load_jsonl(norm_path)
    chrono = load_json(chrono_path)

    topics_obj = load_json(topics_path)
    tone_obj = load_json(tone_path)
    topics_items = extract_items_container(topics_obj)
    tone_items = extract_items_container(tone_obj)

    topics_by_id = build_map_by_id(topics_items)
    tone_by_id = build_map_by_id(tone_items)

    # Build record map by corpus_id, and stable ordinal index
    rec_by_id: Dict[str, Dict[str, Any]] = {}
    for i, r in enumerate(records):
        cid = r.get("corpus_id")
        if isinstance(cid, str):
            rec_by_id[cid] = r

    # We’ll use chrono ordering as the “sequence”
    chrono_items = chrono.get("items")
    if not isinstance(chrono_items, list):
        raise SystemExit("chrono.items missing or not a list")

    # Counters
    topic_primary = Counter()
    topic_tags = Counter()
    tone_polarity = Counter()
    tone_intensity = Counter()
    tone_posture = Counter()
    reply_vs_top = Counter()
    thread_counts = Counter()
    reply_targets = Counter()

    # Sequence metrics (volatility-ish)
    polarity_seq: List[str] = []
    intensity_seq: List[str] = []

    # Per-thread aggregation (simple)
    per_thread = defaultdict(lambda: {"count": 0, "replies": 0, "polarity": Counter(), "intensity": Counter(), "topics": Counter()})

    missing_topics = 0
    missing_tone = 0

    for it in chrono_items:
        if not isinstance(it, dict):
            continue
        cid = it.get("corpus_id")
        if not isinstance(cid, str):
            continue

        r = rec_by_id.get(cid, {})
        tid = r.get("thread_id") if isinstance(r, dict) else None
        if not isinstance(tid, str):
            tid = it.get("thread_id") if isinstance(it.get("thread_id"), str) else "unknown"

        is_reply = bool(r.get("is_reply")) if isinstance(r, dict) else bool(it.get("is_reply"))
        reply_vs_top["reply" if is_reply else "top_level"] += 1
        thread_counts[tid] += 1
        per_thread[tid]["count"] += 1
        if is_reply:
            per_thread[tid]["replies"] += 1

        # reply-to target name from aria label (from chrono item relative_text)
        rel = it.get("relative_text")
        if isinstance(rel, str):
            m = RX_REPLY_TO.search(rel)
            if m:
                reply_targets[m.group(1).strip()] += 1

        trow = topics_by_id.get(cid)
        if not trow:
            missing_topics += 1
            primary = None
            tags = []
        else:
            primary = safe_get_str(trow, "topic", "primary_topic", "label")
            tags = safe_get_list(trow, "topic_tags", "tags", "labels")

        if primary:
            topic_primary[primary] += 1
            per_thread[tid]["topics"][primary] += 1
        for tag in tags:
            if isinstance(tag, str) and tag:
                topic_tags[tag] += 1

        brow = tone_by_id.get(cid)
        if not brow:
            missing_tone += 1
            pol = None
            inten = None
            posture = []
        else:
            pol = safe_get_str(brow, "tone_polarity", "polarity")
            inten = safe_get_str(brow, "tone_intensity", "intensity")
            posture = safe_get_list(brow, "tone_posture", "posture")

        if pol:
            tone_polarity[pol] += 1
            per_thread[tid]["polarity"][pol] += 1
            polarity_seq.append(pol)
        if inten:
            tone_intensity[inten] += 1
            per_thread[tid]["intensity"][inten] += 1
            intensity_seq.append(inten)
        for p in posture:
            if isinstance(p, str) and p:
                tone_posture[p] += 1

    # volatility: count transitions in sequences
    def transitions(seq: List[str]) -> int:
        if len(seq) < 2:
            return 0
        return sum(1 for a, b in zip(seq, seq[1:]) if a != b)

    out = {
        "schema": "phase7_behavior_profile-1.0",
        "inputs": {
            "phase6_out": str(p6),
            "chrono": str(chrono_path),
        },
        "summary": {
            "records_total": len(chrono_items),
            "threads_total": len(thread_counts),
            "missing_topics_rows": missing_topics,
            "missing_tone_rows": missing_tone,
        },
        "distributions": {
            "reply_vs_top_level": summarize_distribution(reply_vs_top, top_n=10),
            "topics_primary_top": summarize_distribution(topic_primary, top_n=15),
            "topics_tags_top": summarize_distribution(topic_tags, top_n=25),
            "tone_polarity": summarize_distribution(tone_polarity, top_n=10),
            "tone_intensity": summarize_distribution(tone_intensity, top_n=10),
            "tone_posture_top": summarize_distribution(tone_posture, top_n=20),
            "reply_targets_top": summarize_distribution(reply_targets, top_n=15),
            "threads_top": summarize_distribution(thread_counts, top_n=15),
        },
        "sequence_signals": {
            "polarity_transitions": transitions(polarity_seq),
            "intensity_transitions": transitions(intensity_seq),
            "notes": [
                "These are non-clinical, text-only signals.",
                "Transitions count changes in label between adjacent chrono-ordered items.",
                "With small sample sizes, treat signals as descriptive only.",
            ],
        },
        "per_thread": [
            {
                "thread_id": tid,
                "count": v["count"],
                "replies": v["replies"],
                "reply_share": (v["replies"] / v["count"]) if v["count"] else 0.0,
                "polarity_top": summarize_distribution(v["polarity"], top_n=5),
                "intensity_top": summarize_distribution(v["intensity"], top_n=5),
                "topics_top": summarize_distribution(v["topics"], top_n=5),
            }
            for tid, v in sorted(per_thread.items(), key=lambda kv: (-kv[1]["count"], kv[0]))
        ],
        "interpretation_guardrails": {
            "allowed": [
                "frequency distributions",
                "patterns consistent with X",
                "changes in labeled posture/tone across chrono order",
            ],
            "disallowed": [
                "clinical diagnosis",
                "definitive motive/intent claims",
                "claims about internal mental state as fact",
            ],
        },
    }

    out_path = out_dir / "phase7_behavior_profile.json"
    out_path.write_text(json.dumps(out, ensure_ascii=False, sort_keys=True, indent=2) + "\n", encoding="utf-8")
    print(f"[phase7C] OK: wrote={out_path} records={len(chrono_items)} threads={len(thread_counts)}")


if __name__ == "__main__":
    main()
