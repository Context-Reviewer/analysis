import json
import hashlib
import re
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


# -----------------------------
# Utilities
# -----------------------------

def load_json(path: Path) -> dict:
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)

def load_jsonl(path: Path) -> List[dict]:
    items: List[dict] = []
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            items.append(json.loads(line))
    return items

def fp_obj(obj: Any) -> str:
    blob = json.dumps(obj, sort_keys=True, ensure_ascii=False).encode("utf-8")
    return hashlib.sha256(blob).hexdigest()[:16]

def iso_now_utc() -> str:
    return datetime.now(timezone.utc).isoformat()

def month_key(ts: str) -> str:
    """
    Convert ISO-ish timestamp string to YYYY-MM.
    We keep this tolerant: parse only the prefix if needed.
    """
    # Common: "2026-02-06T18:30:12+00:00" or "2026-02-06T18:30:12Z"
    try:
        # Normalize Z
        if ts.endswith("Z"):
            ts2 = ts[:-1] + "+00:00"
        else:
            ts2 = ts
        dt = datetime.fromisoformat(ts2)
        return f"{dt.year:04d}-{dt.month:02d}"
    except Exception:
        # Fallback: take first 7 chars if looks like YYYY-MM
        if len(ts) >= 7 and ts[4] == "-" and ts[7-1].isdigit():
            return ts[:7]
        return "unknown"

def get_time_for_binning(item: dict) -> str:
    # Deterministic fallback order:
    # 1) timestamp_parsed (preferred, content-derived)
    # 2) captured_at (collection time, still useful)
    ts = item.get("timestamp_parsed")
    if isinstance(ts, str) and ts.strip():
        return ts.strip()
    cap = item.get("captured_at")
    if isinstance(cap, str) and cap.strip():
        return cap.strip()
    return ""

# -----------------------------
# Condition / Rule evaluation
# -----------------------------

@dataclass(frozen=True)
class Condition:
    field: str
    op: str
    value: Any

def get_field(item: dict, field: str) -> Any:
    # support shallow fields only (by design for v1)
    return item.get(field)

def op_equals(v: Any, target: Any) -> bool:
    return v == target

def op_in(v: Any, target_list: Any) -> bool:
    if not isinstance(target_list, list):
        return False
    return v in target_list

def op_gte(v: Any, target: Any) -> bool:
    try:
        return float(v) >= float(target)
    except Exception:
        return False

def op_lte(v: Any, target: Any) -> bool:
    try:
        return float(v) <= float(target)
    except Exception:
        return False

def op_regex_any(v: Any, patterns: Any) -> bool:
    if not isinstance(v, str):
        return False
    if isinstance(patterns, str):
        patterns = [patterns]
    if not isinstance(patterns, list):
        return False
    for p in patterns:
        try:
            if re.search(p, v, flags=re.IGNORECASE):
                return True
        except re.error:
            # invalid regex pattern -> fail closed
            return False
    return False

def op_contains_any(v: Any, needles: Any) -> bool:
    if not isinstance(v, str):
        return False
    if isinstance(needles, str):
        needles = [needles]
    if not isinstance(needles, list):
        return False
    low = v.lower()
    return any(str(n).lower() in low for n in needles)

OPS = {
    "equals": op_equals,
    "in": op_in,
    "gte": op_gte,
    "lte": op_lte,
    "regex_any": op_regex_any,
    "contains_any": op_contains_any,
}

def eval_condition(item: dict, cond: dict) -> bool:
    field = cond["field"]
    op = cond["op"]
    value = cond["value"]
    fn = OPS.get(op)
    if fn is None:
        raise SystemExit(f"Unsupported op: {op}")
    v = get_field(item, field)
    return fn(v, value)

def eval_rule(item: dict, rule: dict) -> Tuple[bool, List[str]]:
    """
    Returns (hit, [rule_id]) so we can accumulate hits.
    """
    logic = rule["logic"]
    conds = rule["conditions"]
    results = [eval_condition(item, c) for c in conds]
    if logic == "all":
        hit = all(results)
    elif logic == "any":
        hit = any(results)
    else:
        raise SystemExit(f"Unsupported rule.logic: {logic}")
    return hit, [rule["rule_id"]] if hit else []


# -----------------------------
# Scoring (v1: simple)
# -----------------------------

def score_item(signal: dict, item: dict, rule_hits: List[str]) -> int:
    """
    v1 numeric scoring: number of inclusion rules hit (0..N), capped by spec range max if present.
    Conservative and auditable.
    """
    score = len(rule_hits)
    try:
        rng = signal["outputs"]["score_policy"]["range"]
        mx = float(rng["max"])
        if score > mx:
            score = int(mx)
    except Exception:
        pass
    return int(score)


# -----------------------------
# Main runner
# -----------------------------

def validate_required_fields(signal: dict, item: dict) -> None:
    required = signal["definition"]["required_input_fields"]
    missing = [f for f in required if f not in item]
    if missing:
        # Fail loud: determinism requires presence of declared fields.
        raise SystemExit(f"Input item missing required fields: {missing}")

def run_signal(signal: dict, items: List[dict]) -> dict:
    # Ensure required fields exist (fail loud) — check first item only, assumes uniform schema
    if not items:
        raise SystemExit("Input JSONL is empty.")
    validate_required_fields(signal, items[0])

    inclusions = signal["definition"]["inclusion_rules"]
    exclusions = signal["definition"]["exclusion_rules"]

    hits: List[dict] = []

    for it in items:
        # Inclusion must hit at least one inclusion rule (we allow multiple)
        inc_hits: List[str] = []
        for rule in inclusions:
            ok, rh = eval_rule(it, rule)
            if ok:
                inc_hits.extend(rh)

        if not inc_hits:
            continue

        # Exclusion: if any exclusion rule hits, drop
        excluded = False
        ex_hits: List[str] = []
        for rule in exclusions:
            ok, rh = eval_rule(it, rule)
            if ok:
                excluded = True
                ex_hits.extend(rh)
        if excluded:
            continue

        score = score_item(signal, it, inc_hits)

        hits.append({
            "source_index": it.get("source_index"),
            "timestamp_parsed": it.get("timestamp_parsed"),
            "captured_at": it.get("captured_at"),
            "joined_item_type": it.get("joined_item_type"),
            "body": it.get("body", ""),
            "thread_permalink": it.get("thread_permalink"),
            "thread_primary_topic": it.get("thread_primary_topic", ""),
            "sentiment_compound": it.get("sentiment_compound", 0),
            "rule_hits": sorted(set(inc_hits)),
            "score": score,
        })

    # Metrics
    count = len(hits)
    total = len(items)
    rate_per_100 = (count / total * 100.0) if total else 0.0

    dist: Dict[str, int] = {}
    for h in hits:
        mk = month_key(get_time_for_binning(h))
        dist[mk] = dist.get(mk, 0) + 1
    # Deterministic ordering
    dist_sorted = dict(sorted(dist.items(), key=lambda kv: kv[0]))

    # Example selection
    sel = signal["outputs"]["example_selection"]
    max_examples = int(sel["max_examples"])
    ordering = sel["ordering"]

    def sort_key(h: dict):
        ts = str(h.get("timestamp_parsed") or "")
        if ordering == "time_desc":
            return (ts, h.get("source_index", 0))
        if ordering == "score_desc_then_time":
            return (h.get("score", 0), ts, h.get("source_index", 0))
        # "source_index_asc"
        return (h.get("source_index", 0), ts)

    reverse = ordering in ("time_desc", "score_desc_then_time")
    hits_sorted = sorted(hits, key=sort_key, reverse=reverse)

    examples = hits_sorted[:max_examples]

    # Dataset scope
    times = [get_time_for_binning(it) for it in items]
    times = [t for t in times if t]
    time_min = min(times) if times else ""
    time_max = max(times) if times else ""

    metric_ids = [m["metric_id"] for m in signal["outputs"]["metrics"]]

    # We support the common trio: count, rate, over_time distribution.
    # Map them deterministically based on known suffixes.
    metrics_out = {}
    for mid in metric_ids:
        if mid.endswith("_count"):
                    metrics_out[mid] = count
        elif mid.endswith("_rate") or mid.endswith("_rate_per_100"):
                    metrics_out[mid] = round(rate_per_100, 4)
        elif mid.endswith("_over_time"):
                    metrics_out[mid] = dist_sorted
        else:
                    # Unknown metric ID: fail loud so we don't silently emit junk
                    raise SystemExit(f"Runner does not know how to produce metric_id: {mid}")

    out = {
        "run_id": "dev-run",
        "signal_id": signal["signal_id"],
        "signal_version": signal["version"],
        "tier": signal["tier"],
        "dataset_scope": {
            "items_analyzed": total,
            "time_min": time_min,
            "time_max": time_max,
            "surfaces": ["fb_extract_out/sean_context_enriched.jsonl"]
        },
        "metrics": metrics_out,
        "examples": examples,
        "fingerprints": {
            "input_fingerprint": fp_obj(items),
            "spec_fingerprint": fp_obj(signal),
            "generated_utc": iso_now_utc()
        }
    }
    return out

def main(signal_path: Path, input_jsonl: Path, out_dir: Path):
    signal = load_json(signal_path)
    items = load_jsonl(input_jsonl)

    out = run_signal(signal, items)

    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"{signal['signal_id']}.json"
    with out_path.open("w", encoding="utf-8") as f:
        json.dump(out, f, indent=2, ensure_ascii=False)

    print(f"Wrote: {out_path}")

if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: python tools/run_signal.py <signal.json> <input.jsonl> <out_dir>")
        raise SystemExit(2)

    main(Path(sys.argv[1]), Path(sys.argv[2]), Path(sys.argv[3]))
