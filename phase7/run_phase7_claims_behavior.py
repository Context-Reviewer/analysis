#!/usr/bin/env python3
"""
Phase 7D (v1): Claims ↔ Behavior Cross-Reference
Option B: ordinal-window linking with automatic fallback to global attach.

Inputs:
- phase7/out/phase7_claims/phase7_claims.json
- phase7/out/phase7_behavior/phase7_behavior_profile.json

Outputs:
- phase7/out/phase7_claims_behavior/phase7_claims_behavior.json
- phase7/out/phase7_claims_behavior/phase7_claims_behavior_manifest.json

Constraints:
- Deterministic output
- No wall-clock timestamps
- No diagnosis, no motive claims
- Descriptive linking only
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from pathlib import Path
from typing import Any, Dict, List, Tuple


# ----------------------------
# Helpers
# ----------------------------

def dumps(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=False, sort_keys=True, indent=2)


def sha256_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def sha256_file(p: Path) -> str:
    return sha256_bytes(p.read_bytes())


def fatal(msg: str) -> None:
    print(f"[FATAL] {msg}", file=sys.stderr)
    raise SystemExit(2)


def load_json(p: Path) -> Any:
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception as e:
        fatal(f"Failed to load JSON {p}: {e}")
        raise


# ----------------------------
# Behavior ordinal extraction
# ----------------------------

def extract_behavior_windows(behavior_obj: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Attempts to extract ordinal-indexed behavior segments.

    Supported patterns (best-effort, deterministic):
    - items / segments with start_ordinal & end_ordinal
    - buckets with explicit ordinals list

    Returns a list of:
      {
        "behavior_key": str,
        "start_ordinal": int,
        "end_ordinal": int
      }
    """
    windows: List[Dict[str, Any]] = []

    def add_window(key: str, start: int, end: int):
        if start <= end:
            windows.append({
                "behavior_key": key,
                "start_ordinal": start,
                "end_ordinal": end,
            })

    # Common places to look
    candidates = []
    if isinstance(behavior_obj, dict):
        candidates.append(behavior_obj)
        for v in behavior_obj.values():
            if isinstance(v, (list, dict)):
                candidates.append(v)

    def walk(obj: Any, prefix: str = ""):
        if isinstance(obj, dict):
            key = obj.get("key") or obj.get("id") or prefix
            if "start_ordinal" in obj and "end_ordinal" in obj:
                try:
                    add_window(str(key), int(obj["start_ordinal"]), int(obj["end_ordinal"]))
                except Exception:
                    pass
            if "ordinals" in obj and isinstance(obj["ordinals"], list) and obj["ordinals"]:
                try:
                    os = sorted(int(x) for x in obj["ordinals"])
                    add_window(str(key), os[0], os[-1])
                except Exception:
                    pass
            for k, v in obj.items():
                walk(v, f"{prefix}.{k}" if prefix else str(k))
        elif isinstance(obj, list):
            for i, v in enumerate(obj):
                walk(v, f"{prefix}[{i}]")

    for c in candidates:
        walk(c)

    # Deduplicate deterministically
    uniq: Dict[Tuple[str, int, int], Dict[str, Any]] = {}
    for w in windows:
        k = (w["behavior_key"], w["start_ordinal"], w["end_ordinal"])
        uniq[k] = w

    return sorted(
        uniq.values(),
        key=lambda x: (x["start_ordinal"], x["end_ordinal"], x["behavior_key"])
    )


def windows_overlap(a_start: int, a_end: int, b_start: int, b_end: int) -> bool:
    return not (a_end < b_start or b_end < a_start)


# ----------------------------
# Main linking logic
# ----------------------------

def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--claims", default="phase7/out/phase7_claims/phase7_claims.json")
    ap.add_argument("--behavior", default="phase7/out/phase7_behavior/phase7_behavior_profile.json")
    ap.add_argument("--out-dir", default="phase7/out/phase7_claims_behavior")
    args = ap.parse_args()

    root = Path(".").resolve()
    f_claims = (root / args.claims).resolve()
    f_behavior = (root / args.behavior).resolve()
    out_dir = (root / args.out_dir).resolve()

    if not f_claims.exists():
        fatal(f"Missing claims input: {f_claims}")
    if not f_behavior.exists():
        fatal(f"Missing behavior input: {f_behavior}")

    claims_obj = load_json(f_claims)
    behavior_obj = load_json(f_behavior)

    behavior_windows = extract_behavior_windows(behavior_obj)
    linking_mode = "ordinal_window" if behavior_windows else "fallback_global"

    topics_out: List[Dict[str, Any]] = []

    for topic in claims_obj.get("topics", []):
        topic_id = topic.get("topic_id")
        claims_out: List[Dict[str, Any]] = []

        for cl in topic.get("claims", []):
            evidence_ordinals = sorted(
                {e["ordinal"] for e in cl.get("evidence", []) if isinstance(e.get("ordinal"), int)}
            )

            if evidence_ordinals:
                start_o = evidence_ordinals[0]
                end_o = evidence_ordinals[-1]
            else:
                start_o = None
                end_o = None

            behavior_links: List[Dict[str, Any]] = []

            if linking_mode == "ordinal_window" and start_o is not None and end_o is not None:
                for bw in behavior_windows:
                    if windows_overlap(start_o, end_o, bw["start_ordinal"], bw["end_ordinal"]):
                        behavior_links.append({
                            "behavior_key": bw["behavior_key"],
                            "relationship": "co_occurs_within_window",
                            "overlap": {
                                "claim_window": [start_o, end_o],
                                "behavior_window": [bw["start_ordinal"], bw["end_ordinal"]],
                            }
                        })

            claims_out.append({
                "claim_id": cl.get("claim_id"),
                "normalized_text": cl.get("normalized_text"),
                "claim_stats": {
                    "occurrence_count": cl.get("occurrence_count"),
                    "first_ordinal": cl.get("first_ordinal"),
                    "last_ordinal": cl.get("last_ordinal"),
                },
                "activity_window": {
                    "start_ordinal": start_o,
                    "end_ordinal": end_o,
                    "evidence_ordinals": evidence_ordinals,
                },
                "behavior_links": behavior_links,
            })

        claims_out.sort(key=lambda x: (x["activity_window"]["start_ordinal"] or -1, x["claim_id"]))
        topics_out.append({
            "topic_id": topic_id,
            "claims": claims_out,
        })

    out_dir.mkdir(parents=True, exist_ok=True)

    out_obj = {
        "schema_version": "phase7_claims_behavior-1.0",
        "build": {
            "deterministic": True,
            "ordering_policy": "input_ordinal",
            "linking_mode": linking_mode,
            "source": {
                "claims_schema": claims_obj.get("schema_version"),
                "behavior_schema": behavior_obj.get("schema_version"),
            },
        },
        "behavior_profile_ref": {
            "schema_version": behavior_obj.get("schema_version"),
            "summary": behavior_obj.get("summary", {}),
        },
        "topics": topics_out,
    }

    f_out = out_dir / "phase7_claims_behavior.json"
    f_out.write_text(dumps(out_obj) + "\n", encoding="utf-8")

    manifest = {
        "schema_version": "phase7_claims_behavior_manifest-1.0",
        "inputs": {
            str(f_claims.relative_to(root)): sha256_file(f_claims),
            str(f_behavior.relative_to(root)): sha256_file(f_behavior),
        },
        "outputs": {
            str(f_out.relative_to(root)): sha256_file(f_out),
        },
        "notes": {
            "deterministic": True,
            "no_wall_clock": True,
        },
    }

    f_manifest = out_dir / "phase7_claims_behavior_manifest.json"
    f_manifest.write_text(dumps(manifest) + "\n", encoding="utf-8")

    print("[OK] Phase 7D claims↔behavior complete")
    print(f"[OK] linking_mode = {linking_mode}")
    print(f"[OK] wrote: {f_out.relative_to(root)}")
    print(f"[OK] wrote: {f_manifest.relative_to(root)}")


if __name__ == "__main__":
    main()