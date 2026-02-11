#!/usr/bin/env python3
"""
Phase 7B (v1 topic-aware): Claims & Intent (governed, deterministic)

Inputs (Phase 6):
- <phase6-dir>/phase6_normalized.jsonl
- <phase6-dir>/phase6_topics.json   (topics-1.0 per-record assignment items)
- <phase6-dir>/phase6_manifest.json (optional; used for provenance if present)

Outputs:
- phase7/out/phase7_claims/phase7_claims.json
- phase7/out/phase7_claims/phase7_claims_manifest.json

Constraints:
- Deterministic output (stable ordering, stable hashing)
- No wall-clock timestamps
- No diagnosis, no motive claims, no internal mental state asserted as fact
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Tuple


# ----------------------------
# Deterministic JSON helpers
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


# ----------------------------
# Text normalization / parsing
# ----------------------------

_RE_WS = re.compile(r"\s+")
_RE_STRIP_PUNCT = re.compile(r"[^\w\s']+", re.UNICODE)  # keep apostrophes
_RE_SENT_SPLIT = re.compile(r"(?<=[.!])\s+|\n+")
_RE_HAS_ALPHA = re.compile(r"[A-Za-z]")

CERTAINTY_MARKERS = [
    "always", "never", "everyone", "no one", "nobody", "everybody",
    "fact", "facts", "obviously", "clearly", "certainly", "definitely",
    "proven", "proof", "undeniable", "without a doubt",
]
INTENSIFIERS = [
    "very", "extremely", "literally", "absolutely", "totally",
    "completely", "insanely", "unbelievably", "100%", "fully",
]
HEDGES = [
    "i think", "i believe", "maybe", "probably", "possibly", "seems", "seem",
    "might", "could", "i guess", "in my opinion", "imo", "apparently",
]


def collapse_ws(s: str) -> str:
    return _RE_WS.sub(" ", s).strip()


def normalize_claim_text(s: str) -> str:
    s = s.strip().lower()
    s = collapse_ws(s)
    s = _RE_STRIP_PUNCT.sub("", s)
    s = collapse_ws(s)
    return s


def is_question_like(s: str) -> bool:
    return "?" in s


def extract_candidate_sentences(text: str) -> List[str]:
    """
    Deterministic, conservative claim candidates:
    - Split on sentence boundaries / newlines
    - Drop questions
    - Drop very short fragments
    - Keep only pieces with at least one alphabetic character
    """
    text = (text or "").replace("\r\n", "\n").replace("\r", "\n")
    parts = _RE_SENT_SPLIT.split(text)
    out: List[str] = []
    for part in parts:
        s = collapse_ws(part)
        if not s:
            continue
        if is_question_like(s):
            continue
        if len(s) < 18:
            continue
        if not _RE_HAS_ALPHA.search(s):
            continue
        out.append(s)
    return out


def marker_examples(text_lc: str, markers: List[str]) -> List[str]:
    ex: List[str] = []
    for m in markers:
        if m in text_lc:
            ex.append(m)
    return sorted(set(ex))


def posture_bucket(text_lc: str) -> str:
    for h in HEDGES:
        if h in text_lc:
            return "hedged"
    return "assertive"


# ----------------------------
# Loaders
# ----------------------------

def load_json(path: Path) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception as e:
        fatal(f"Failed to parse JSON: {path} ({e})")
        raise


def load_jsonl(path: Path) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    if not path.exists():
        fatal(f"Missing required file: {path}")
    for i, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
        if not line.strip():
            continue
        try:
            rows.append(json.loads(line))
        except Exception as e:
            fatal(f"Failed to parse JSONL line {i} in {path}: {e}")
    return rows


def build_id_to_topics(topics_obj: Any) -> Dict[str, List[str]]:
    """
    Build mapping: record_id -> list of topic_ids (primary + secondary).

    Observed Phase 6 topics-1.0 items shape:
      {"items": [
         {"id": <record_id>, "primary_topic": <tid>, "secondary_tags": [<tid>...], ...},
         ...
      ], ...}

    Returns deterministic ordering of topic ids per record.
    """
    if not isinstance(topics_obj, dict):
        fatal("phase6_topics.json must be a JSON object (dict).")

    items = topics_obj.get("items")
    if not isinstance(items, list):
        fatal("phase6_topics.json missing expected key: items (list)")

    m: Dict[str, List[str]] = {}
    for it in items:
        if not isinstance(it, dict):
            continue
        rid = it.get("id")
        if rid is None:
            continue
        rid_s = str(rid)

        tids: List[str] = []
        pt = it.get("primary_topic")
        if pt is not None and str(pt).strip():
            tids.append(str(pt))

        st = it.get("secondary_tags")
        if isinstance(st, list):
            for x in st:
                if x is None:
                    continue
                xs = str(x).strip()
                if xs:
                    tids.append(xs)

        tids = sorted(set(tids))
        if tids:
            m[rid_s] = tids

    if not m:
        fatal("phase6_topics.json has items but no usable id->topics mappings were found.")
    return m


# ----------------------------
# Claim aggregation
# ----------------------------

@dataclass(frozen=True)
class Evidence:
    quote: str
    record_id: str
    thread_id: str
    ordinal: int


def claim_id_for(topic_id: str, normalized_text: str) -> str:
    raw = (topic_id + "\n" + normalized_text).encode("utf-8")
    return sha256_bytes(raw)


def safe_int(x: Any, default: int = -1) -> int:
    try:
        return int(x)
    except Exception:
        return default


def pick_text(record: Dict[str, Any]) -> str:
    for k in ("text", "body", "content"):
        v = record.get(k)
        if isinstance(v, str) and v.strip():
            return v
    return ""


def pick_ordinal(record: Dict[str, Any], idx_fallback: int) -> int:
    for k in ("input_ordinal", "ordinal", "source_index"):
        if k in record:
            n = safe_int(record.get(k), default=-1)
            if n >= 0:
                return n
    return idx_fallback


def build_claims(
    records: List[Dict[str, Any]],
    id_to_topics: Dict[str, List[str]],
    evidence_cap_per_claim: int = 25,
) -> Dict[str, Any]:
    # We do not have an authoritative topic dictionary here, so labels are stable = ids.
    topic_labels: Dict[str, str] = {}
    acc: Dict[Tuple[str, str], Dict[str, Any]] = {}

    # Deterministic record order: by input_ordinal (fallback file order)
    enriched: List[Tuple[int, Dict[str, Any]]] = []
    for i, r in enumerate(records):
        enriched.append((pick_ordinal(r, i), r))
    enriched.sort(key=lambda x: (x[0], str(x[1].get("id", ""))))

    for ordv, r in enriched:
        record_id = str(r.get("id") or "")
        if not record_id:
            continue

        thread_id = str(r.get("thread_id") or r.get("parent_context", {}).get("thread_id") or "")
        topics = id_to_topics.get(record_id, [])
        if not topics:
            continue

        text = pick_text(r)
        if not text:
            continue

        candidates = extract_candidate_sentences(text)
        if not candidates:
            continue

        for tid in topics:
            topic_labels.setdefault(tid, tid)

            for sent in candidates:
                norm = normalize_claim_text(sent)
                if not norm:
                    continue

                cid = claim_id_for(tid, norm)
                key = (tid, cid)

                ev = Evidence(
                    quote=sent.strip(),
                    record_id=record_id,
                    thread_id=thread_id,
                    ordinal=ordv,
                )

                if key not in acc:
                    acc[key] = {
                        "claim_id": cid,
                        "normalized_text": norm,
                        "first_ordinal": ordv,
                        "last_ordinal": ordv,
                        "occurrence_count": 0,
                        "evidence": [],
                        "_postures": [],
                    }

                a = acc[key]
                a["occurrence_count"] += 1
                a["first_ordinal"] = min(a["first_ordinal"], ordv)
                a["last_ordinal"] = max(a["last_ordinal"], ordv)
                a["evidence"].append(ev)
                a["_postures"].append(posture_bucket(sent.lower()))

    # Group by topic
    topics_out: Dict[str, Dict[str, Any]] = {}
    for (tid, _), a in acc.items():
        topics_out.setdefault(tid, {"topic_id": tid, "topic_label": topic_labels.get(tid, tid), "claims": []})
        topics_out[tid]["claims"].append(a)

    def ev_sort_key(e: Evidence) -> Tuple[int, str, str]:
        return (e.ordinal, e.thread_id, e.record_id)

    finalized_topics: List[Dict[str, Any]] = []
    for tid in sorted(topics_out.keys()):
        tblock = topics_out[tid]
        claims_in = tblock["claims"]

        finalized_claims: List[Dict[str, Any]] = []
        for a in claims_in:
            evs: List[Evidence] = sorted(a["evidence"], key=ev_sort_key)[:evidence_cap_per_claim]

            first_quote_lc = (evs[0].quote.lower() if evs else a["normalized_text"])
            cert_ex = marker_examples(first_quote_lc, CERTAINTY_MARKERS)
            int_ex = marker_examples(first_quote_lc, INTENSIFIERS)

            postures = a["_postures"]
            from_bucket = postures[0] if postures else "unknown"
            to_bucket = postures[-1] if postures else "unknown"
            detected = (from_bucket == "hedged" and to_bucket == "assertive")

            finalized_claims.append({
                "claim_id": a["claim_id"],
                "normalized_text": a["normalized_text"],
                "first_ordinal": a["first_ordinal"],
                "last_ordinal": a["last_ordinal"],
                "occurrence_count": a["occurrence_count"],
                "certainty_markers": {"present": bool(cert_ex), "examples": cert_ex},
                "intensifiers": {"present": bool(int_ex), "examples": int_ex},
                "modal_shift": {"detected": detected, "from": from_bucket, "to": to_bucket},
                "evidence": [
                    {"quote": e.quote, "id": e.record_id, "thread_id": e.thread_id, "ordinal": e.ordinal}
                    for e in evs
                ],
            })

        finalized_claims.sort(key=lambda c: (c["first_ordinal"], c["claim_id"]))
        finalized_topics.append({
            "topic_id": tblock["topic_id"],
            "topic_label": tblock["topic_label"],
            "claims": finalized_claims
        })

    return {
        "schema_version": "phase7_claims-1.0",
        "build": {
            "source_phase": "v0.6.5-phase6-timeline-stable",
            "ordering_policy": "input_ordinal",
            "deterministic": True,
            "mode": "topic-aware",
        },
        "topics": finalized_topics,
    }


# ----------------------------
# Main
# ----------------------------

def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--phase6-dir", default="phase6", help="Phase 6 directory (default: phase6)")
    ap.add_argument("--out-dir", default="phase7/out/phase7_claims", help="Output directory")
    args = ap.parse_args()

    root = Path(".").resolve()
    phase6_dir = (root / args.phase6_dir).resolve()
    out_dir = (root / args.out_dir).resolve()

    f_norm = phase6_dir / "phase6_normalized.jsonl"
    f_topics = phase6_dir / "phase6_topics.json"
    f_manifest = phase6_dir / "phase6_manifest.json"

    if not f_norm.exists():
        fatal(f"Missing Phase 6 normalized input: {f_norm}")
    if not f_topics.exists():
        fatal(f"Missing Phase 6 topics input: {f_topics}")

    records = load_jsonl(f_norm)
    topics_obj = load_json(f_topics)
    id_to_topics = build_id_to_topics(topics_obj)

    out_dir.mkdir(parents=True, exist_ok=True)

    claims_out = build_claims(records, id_to_topics)
    f_out = out_dir / "phase7_claims.json"
    f_out.write_text(dumps(claims_out) + "\n", encoding="utf-8")

    manifest: Dict[str, Any] = {
        "schema_version": "phase7_claims_manifest-1.0",
        "inputs": {
            str(f_norm.relative_to(root)): sha256_file(f_norm),
            str(f_topics.relative_to(root)): sha256_file(f_topics),
        },
        "outputs": {
            str(f_out.relative_to(root)): sha256_file(f_out),
        },
        "notes": {
            "deterministic": True,
            "no_wall_clock": True,
        },
    }
    if f_manifest.exists():
        manifest["inputs"][str(f_manifest.relative_to(root))] = sha256_file(f_manifest)

    f_m = out_dir / "phase7_claims_manifest.json"
    f_m.write_text(dumps(manifest) + "\n", encoding="utf-8")

    print("[OK] Phase 7B claims complete")
    print(f"[OK] wrote: {f_out.relative_to(root)}")
    print(f"[OK] wrote: {f_m.relative_to(root)}")


if __name__ == "__main__":
    main()