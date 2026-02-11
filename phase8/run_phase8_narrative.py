#!/usr/bin/env python3
"""
Phase 8A (v1): Narrative Synthesis (evidence-first, governed)

Builds a deterministic narrative from Phase 7 artifacts:
- Claims (7B)
- Behavior profile (7C)
- Behavior windows (7E)
- Claims↔behavior links (7D)
- Chrono ordering (7A)

No diagnosis. No motive claims. Relative chronology only.
"""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
from typing import Any, Dict, List


# ----------------------------
# Helpers
# ----------------------------

def dumps(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=False, sort_keys=True, indent=2)


def sha256_file(p: Path) -> str:
    return hashlib.sha256(p.read_bytes()).hexdigest()


def load_json(p: Path) -> Any:
    return json.loads(p.read_text(encoding="utf-8"))


# ----------------------------
# Main
# ----------------------------

def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--max-quotes", type=int, default=3)
    ap.add_argument("--out-dir", default="phase8/out")
    args = ap.parse_args()

    root = Path(".").resolve()

    f_chrono = root / "phase7/out/phase7_chrono/phase7_chrono_timeline.json"
    f_claims = root / "phase7/out/phase7_claims/phase7_claims.json"
    f_behavior = root / "phase7/out/phase7_behavior/phase7_behavior_profile.json"
    f_windows = root / "phase7/out/phase7_behavior_windows/phase7_behavior_windows.json"
    f_links = root / "phase7/out/phase7_claims_behavior/phase7_claims_behavior.json"

    chrono = load_json(f_chrono)
    claims = load_json(f_claims)
    behavior = load_json(f_behavior)
    windows = load_json(f_windows)
    links = load_json(f_links)

    # Build ordinal -> chrono item map
    chrono_items = chrono.get("items", [])
    ordinal_map = {}
    for it in chrono_items:
        o = it.get("input_ordinal")
        cid = it.get("corpus_id")
        if isinstance(o, int) and cid:
            ordinal_map[o] = it

    topics_out: List[Dict[str, Any]] = []
    md_lines: List[str] = []

    md_lines.append("# Narrative Summary")
    md_lines.append("")
    md_lines.append("This narrative summarizes repeated assertions and their behavioral context.")
    md_lines.append("All ordering is relative; no calendar claims are made.")
    md_lines.append("")

    for topic in links.get("topics", []):
        topic_id = topic.get("topic_id")
        claims_out = []

        md_lines.append(f"## Topic: {topic_id}")
        md_lines.append("")

        for cl in topic.get("claims", []):
            evid_ord = cl.get("activity_window", {}).get("evidence_ordinals", [])
            quotes: List[str] = []

            for o in evid_ord[: args.max_quotes]:
                it = ordinal_map.get(o)
                if it and it.get("text"):
                    quotes.append(it["text"])

            claims_out.append({
                "claim_id": cl.get("claim_id"),
                "normalized_text": cl.get("normalized_text"),
                "occurrence_count": cl.get("claim_stats", {}).get("occurrence_count"),
                "activity_window": cl.get("activity_window"),
                "behavior_links": cl.get("behavior_links", []),
                "evidence_quotes": quotes,
            })

            md_lines.append(f"### Claim")
            md_lines.append(cl.get("normalized_text", ""))
            md_lines.append("")
            md_lines.append(f"- Occurrences: {cl.get('claim_stats', {}).get('occurrence_count')}")
            md_lines.append(f"- Activity window (ordinal): {cl.get('activity_window', {}).get('start_ordinal')} → {cl.get('activity_window', {}).get('end_ordinal')}")
            md_lines.append("")

            if cl.get("behavior_links"):
                md_lines.append("Linked behavioral context:")
                for bl in cl["behavior_links"]:
                    md_lines.append(f"- {bl.get('behavior_key')} overlaps claim window")
                md_lines.append("")

            if quotes:
                md_lines.append("Evidence excerpts:")
                for q in quotes:
                    md_lines.append(f"> {q}")
                md_lines.append("")

        topics_out.append({
            "topic_id": topic_id,
            "claims": claims_out,
        })

    out_dir = (root / args.out_dir).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    narrative = {
        "schema_version": "phase8_narrative-1.0",
        "build": {
            "deterministic": True,
            "max_quotes_per_claim": args.max_quotes,
        },
        "overview": {
            "topics_count": len(topics_out),
            "records_total": behavior.get("summary", {}).get("records_total"),
            "threads_total": behavior.get("summary", {}).get("threads_total"),
            "notes": [
                "No clinical or motive claims are made.",
                "Behavioral links indicate co-occurrence only.",
            ],
        },
        "topics": topics_out,
    }

    f_json = out_dir / "phase8_narrative.json"
    f_md = out_dir / "phase8_narrative.md"
    f_manifest = out_dir / "phase8_manifest.json"

    f_json.write_text(dumps(narrative) + "\n", encoding="utf-8")
    f_md.write_text("\n".join(md_lines) + "\n", encoding="utf-8")

    manifest = {
        "schema_version": "phase8_manifest-1.0",
        "inputs": {
            "chrono": sha256_file(f_chrono),
            "claims": sha256_file(f_claims),
            "behavior": sha256_file(f_behavior),
            "behavior_windows": sha256_file(f_windows),
            "claims_behavior": sha256_file(f_links),
        },
        "outputs": {
            "narrative_json": sha256_file(f_json),
            "narrative_md": sha256_file(f_md),
        },
        "notes": {
            "deterministic": True,
            "no_wall_clock": True,
        },
    }

    f_manifest.write_text(dumps(manifest) + "\n", encoding="utf-8")

    print("[OK] Phase 8A narrative complete")
    print(f"[OK] wrote: {f_json.relative_to(root)}")
    print(f"[OK] wrote: {f_md.relative_to(root)}")
    print(f"[OK] wrote: {f_manifest.relative_to(root)}")


if __name__ == "__main__":
    main()

