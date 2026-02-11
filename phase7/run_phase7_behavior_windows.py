#!/usr/bin/env python3
"""
Phase 7E (v1): Behavior Windows (ordinal-indexed) derived from:
- Phase 7A chrono order (relative ordering only)
- Phase 6 tone labels (polarity, intensity)
- No new interpretation; strictly locates already-defined transition signals.

Outputs:
- phase7/out/phase7_behavior_windows/phase7_behavior_windows.json
- phase7/out/phase7_behavior_windows/phase7_behavior_windows_manifest.json

Constraints:
- Deterministic output
- No wall-clock time
- No diagnosis / motive claims
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


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


def load_jsonl(p: Path) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for i, line in enumerate(p.read_text(encoding="utf-8").splitlines(), start=1):
        if not line.strip():
            continue
        try:
            rows.append(json.loads(line))
        except Exception as e:
            fatal(f"Failed to parse JSONL line {i} in {p}: {e}")
    return rows


def as_int(x: Any) -> Optional[int]:
    try:
        return int(x)
    except Exception:
        return None


def pick_items_from_chrono(chrono_obj: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Accepts common shapes for chrono output. We only need a deterministic list
    of items in chrono order, with record id and an ordinal-like index.

    We try keys in this order:
    - chrono_obj["items"]
    - chrono_obj["timeline"]
    - chrono_obj["rows"]
    """
    for k in ("items", "timeline", "rows"):
        v = chrono_obj.get(k)
        if isinstance(v, list):
            return v
    fatal("Unrecognized chrono shape: expected one of keys items/timeline/rows to be a list.")
    return []  # unreachable


def build_id_to_tone(tone_obj: Dict[str, Any]) -> Dict[str, Dict[str, str]]:
    """
    Phase 6 tone output can vary; we support:
    - {"items":[{"id":..., "polarity":..., "intensity":...}, ...]}
    - {"rows":[...]} similarly

    Returns mapping: id -> {"polarity": str, "intensity": str}
    """
    items = None
    if isinstance(tone_obj, dict):
        if isinstance(tone_obj.get("items"), list):
            items = tone_obj["items"]
        elif isinstance(tone_obj.get("rows"), list):
            items = tone_obj["rows"]

    if not isinstance(items, list):
        fatal("Unrecognized tone shape: expected key items or rows to be a list.")

    out: Dict[str, Dict[str, str]] = {}
    for it in items:
        if not isinstance(it, dict):
            continue
        rid = it.get("id")
        if rid is None:
            continue
        pol = it.get("polarity") or it.get("tone_polarity") or it.get("polarity_label")
        inten = it.get("intensity") or it.get("tone_intensity") or it.get("intensity_label")
        if pol is None and inten is None:
            continue
        out[str(rid)] = {
            "polarity": str(pol) if pol is not None else "",
            "intensity": str(inten) if inten is not None else "",
        }
    if not out:
        fatal("Tone file parsed but no id->tone mappings found.")
    return out


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--chrono", default="phase7/out/phase7_chrono/phase7_chrono_timeline.json")
    ap.add_argument("--phase6-dir", default="phase6/out/phase6_71ada6b9")
    ap.add_argument("--out-dir", default="phase7/out/phase7_behavior_windows")
    args = ap.parse_args()

    root = Path(".").resolve()
    f_chrono = (root / args.chrono).resolve()
    phase6_dir = (root / args.phase6_dir).resolve()
    out_dir = (root / args.out_dir).resolve()

    f_tone = (phase6_dir / "phase6_tone.json").resolve()
    f_norm = (phase6_dir / "phase6_normalized.jsonl").resolve()

    if not f_chrono.exists():
        fatal(f"Missing chrono input: {f_chrono}")
    if not f_tone.exists():
        fatal(f"Missing Phase 6 tone input: {f_tone}")
    if not f_norm.exists():
        fatal(f"Missing Phase 6 normalized input: {f_norm}")

    chrono_obj = load_json(f_chrono)
    tone_obj = load_json(f_tone)
    norm_rows = load_jsonl(f_norm)

    # Build id -> thread_id (from normalized rows)
    id_to_thread: Dict[str, str] = {}
    for r in norm_rows:
        rid = r.get("id")
        if rid is None:
            continue
        tid = r.get("thread_id") or r.get("parent_context", {}).get("thread_id") or ""
        id_to_thread[str(rid)] = str(tid)

    id_to_tone = build_id_to_tone(tone_obj)

    chrono_items = pick_items_from_chrono(chrono_obj)

    # Build an ordered list of record ids in chrono order.
    # We attempt to read id from each chrono item via keys: id, record_id
    ordered_ids: List[str] = []
    for it in chrono_items:
        if not isinstance(it, dict):
            continue
        rid = it.get("corpus_id") or it.get("id") or it.get("record_id")
        if rid is None:
            continue
        ordered_ids.append(str(rid))

    if not ordered_ids:
        fatal("Chrono items contained no usable id/record_id fields.")

    # Determine ordinals: use explicit ordinal if present in chrono items; else fallback to index
    # We store ordinals per id based on first occurrence.
    id_to_ordinal: Dict[str, int] = {}
    for idx, it in enumerate(chrono_items):
        if not isinstance(it, dict):
            continue
        rid = it.get("corpus_id") or it.get("id") or it.get("record_id")
        if rid is None:
            continue
        rid_s = str(rid)
        if rid_s in id_to_ordinal:
            continue
        ordv = it.get("ordinal") or it.get("input_ordinal") or it.get("index")
        oi = as_int(ordv)
        id_to_ordinal[rid_s] = oi if oi is not None else idx

    # Create windows
    windows: List[Dict[str, Any]] = []

    def add_transition_window(kind: str, a_id: str, b_id: str, a_ord: int, b_ord: int, thread_id: str):
        start_o = min(a_ord, b_ord)
        end_o = max(a_ord, b_ord)
        windows.append({
            "behavior_key": f"{kind}_transition",
            "start_ordinal": start_o,
            "end_ordinal": end_o,
            "thread_id": thread_id,
            "supporting_ids": [a_id, b_id],
        })

    # Transitions across adjacent chrono items
    for i in range(len(ordered_ids) - 1):
        a_id = ordered_ids[i]
        b_id = ordered_ids[i + 1]
        if a_id not in id_to_tone or b_id not in id_to_tone:
            continue

        a_ord = id_to_ordinal.get(a_id, i)
        b_ord = id_to_ordinal.get(b_id, i + 1)
        thread_id = id_to_thread.get(a_id) or id_to_thread.get(b_id) or ""

        a_pol = id_to_tone[a_id].get("polarity", "")
        b_pol = id_to_tone[b_id].get("polarity", "")
        if a_pol and b_pol and a_pol != b_pol:
            add_transition_window("polarity", a_id, b_id, a_ord, b_ord, thread_id)

        a_int = id_to_tone[a_id].get("intensity", "")
        b_int = id_to_tone[b_id].get("intensity", "")
        if a_int and b_int and a_int != b_int:
            add_transition_window("intensity", a_id, b_id, a_ord, b_ord, thread_id)

    # Thread activity window(s): min/max ordinal per thread_id
    thread_to_ordinals: Dict[str, List[int]] = {}
    for rid in ordered_ids:
        tid = id_to_thread.get(rid, "")
        if not tid:
            continue
        thread_to_ordinals.setdefault(tid, []).append(id_to_ordinal.get(rid, 0))

    for tid in sorted(thread_to_ordinals.keys()):
        ords = sorted(thread_to_ordinals[tid])
        if not ords:
            continue
        windows.append({
            "behavior_key": "thread_activity",
            "start_ordinal": ords[0],
            "end_ordinal": ords[-1],
            "thread_id": tid,
            "supporting_ids": [],
        })

    # Deterministic ordering + dedupe
    uniq: Dict[Tuple[str, int, int, str, Tuple[str, ...]], Dict[str, Any]] = {}
    for w in windows:
        key = (
            str(w["behavior_key"]),
            int(w["start_ordinal"]),
            int(w["end_ordinal"]),
            str(w.get("thread_id", "")),
            tuple(w.get("supporting_ids", [])),
        )
        uniq[key] = w

    windows_out = sorted(
        uniq.values(),
        key=lambda w: (
            w["start_ordinal"],
            w["end_ordinal"],
            w["behavior_key"],
            w.get("thread_id", ""),
            ",".join(w.get("supporting_ids", [])),
        )
    )

    out_dir.mkdir(parents=True, exist_ok=True)

    out_obj = {
        "schema_version": "phase7_behavior_windows-1.0",
        "build": {
            "deterministic": True,
            "ordering_policy": "chrono_order_then_ordinal",
            "source": {
                "chrono_schema": chrono_obj.get("schema") or chrono_obj.get("schema_version"),
                "tone_schema": tone_obj.get("schema") or tone_obj.get("schema_version"),
            },
        },
        "behavior_windows": windows_out,
        "notes": {
            "no_new_interpretation": True,
            "computed_from": [
                "phase7_chrono_order",
                "phase6_tone_labels",
            ],
        }
    }

    f_out = out_dir / "phase7_behavior_windows.json"
    f_out.write_text(dumps(out_obj) + "\n", encoding="utf-8")

    manifest = {
        "schema_version": "phase7_behavior_windows_manifest-1.0",
        "inputs": {
            str(f_chrono.relative_to(root)): sha256_file(f_chrono),
            str(f_tone.relative_to(root)): sha256_file(f_tone),
            str(f_norm.relative_to(root)): sha256_file(f_norm),
        },
        "outputs": {
            str(f_out.relative_to(root)): sha256_file(f_out),
        },
        "notes": {
            "deterministic": True,
            "no_wall_clock": True,
        },
    }

    f_manifest = out_dir / "phase7_behavior_windows_manifest.json"
    f_manifest.write_text(dumps(manifest) + "\n", encoding="utf-8")

    print("[OK] Phase 7E behavior windows complete")
    print(f"[OK] windows = {len(windows_out)}")
    print(f"[OK] wrote: {f_out.relative_to(root)}")
    print(f"[OK] wrote: {f_manifest.relative_to(root)}")


if __name__ == "__main__":
    main()
