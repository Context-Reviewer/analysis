import json
import hashlib
import statistics
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple


IN_PATH = Path("fb_extract_out/sean_context_enriched.jsonl")
OUT_PATH = Path("docs/data/data_quality_metrics.json")
SCHEMA_VERSION = "data_quality_metrics-0.1"

ROUND_N = 4

FIELD_LIST = [
    "source_index",
    "thread_permalink",
    "parent_context",
    "parent_author_type",
    "topics",
    "thread_primary_topic",
    "sentiment_compound",
    "timestamp_parsed",
    "captured_at",
]


def fail(msg: str) -> None:
    raise SystemExit(f"[FAIL] {msg}")


def sha256_file_bytes(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return "sha256:" + h.hexdigest()


def read_jsonl(path: Path) -> List[Dict[str, Any]]:
    if not path.exists():
        fail(f"Missing input: {path}")
    out: List[Dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as f:
        for lineno, line in enumerate(f, start=1):
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except Exception as e:
                fail(f"Invalid JSONL at line {lineno}: {e}")
            if not isinstance(obj, dict):
                fail(f"Non-object JSON at line {lineno}")
            out.append(obj)
    if not out:
        fail("Input JSONL is empty")
    return out


def canonical_ts(r: Dict[str, Any]) -> Optional[str]:
    return r.get("timestamp_parsed") or r.get("captured_at") or None


def r4(x: Optional[float]) -> Optional[float]:
    if x is None:
        return None
    return round(float(x), ROUND_N)


def safe_mean(xs: List[float]) -> Optional[float]:
    return r4(sum(xs) / len(xs)) if xs else None


def safe_median(xs: List[float]) -> Optional[float]:
    if not xs:
        return None
    xs2 = sorted(xs)
    return r4(float(statistics.median(xs2)))


def nonnull_rate(nonnull: int, total: int) -> float:
    if total <= 0:
        return 0.0
    return r4(nonnull / total)  # type: ignore[return-value]


def main() -> None:
    records = read_jsonl(IN_PATH)
    total = len(records)
    fp = sha256_file_bytes(IN_PATH)

    # Basic counts
    item_type_counts = Counter()
    parent_author_type_counts = Counter()

    # Field completeness
    nonnull_counts = {k: 0 for k in FIELD_LIST}

    # Topics stats
    topics_vocab = Counter()
    topics_per_item: List[int] = []
    topics_nonempty = 0

    # Timestamp stats
    ts_vals: List[str] = []

    # Join integrity checks
    source_index_vals: List[int] = []
    pc_eq_tp_count = 0
    pc_tp_eligible = 0

    # Sentiment quality
    sent_vals: List[float] = []
    sent_out_of_range = 0

    # thread_primary_topic coverage
    thread_primary_topic_nonnull = 0

    for r in records:
        item_type = r.get("joined_item_type") or r.get("item_type") or "unknown"
        item_type_counts[str(item_type)] += 1

        pat = r.get("parent_author_type") or "unknown"
        parent_author_type_counts[str(pat)] += 1

        for k in FIELD_LIST:
            if r.get(k) is not None:
                nonnull_counts[k] += 1

        tpt = r.get("thread_primary_topic")
        if tpt is not None:
            thread_primary_topic_nonnull += 1

        # Topics
        topics = r.get("topics")
        if isinstance(topics, list):
            topics_per_item.append(len(topics))
            if len(topics) > 0:
                topics_nonempty += 1
            for t in topics:
                if t is None:
                    continue
                topics_vocab[str(t)] += 1
        else:
            topics_per_item.append(0)

        # Timestamp
        ts = canonical_ts(r)
        if ts:
            ts_vals.append(str(ts))

        # Join integrity
        si = r.get("source_index")
        if isinstance(si, int):
            source_index_vals.append(si)
        elif isinstance(si, str) and si.isdigit():
            source_index_vals.append(int(si))

        pc = r.get("parent_context")
        tp = r.get("thread_permalink")
        if r.get("joined_item_type") == "comment" and pc and tp:
            pc_tp_eligible += 1
            if pc == tp:
                pc_eq_tp_count += 1

        # Sentiment
        sc = r.get("sentiment_compound")
        if isinstance(sc, (int, float)):
            sent_vals.append(float(sc))
            if sc < -1.0 or sc > 1.0:
                sent_out_of_range += 1

    ts_vals_sorted = sorted(ts_vals)
    date_min = ts_vals_sorted[0] if ts_vals_sorted else None
    date_max = ts_vals_sorted[-1] if ts_vals_sorted else None

    # topic count stats
    topics_min = min(topics_per_item) if topics_per_item else 0
    topics_max = max(topics_per_item) if topics_per_item else 0
    topics_mean = safe_mean([float(x) for x in topics_per_item])
    topics_median = safe_median([float(x) for x in topics_per_item])

    # uniqueness stats
    si_nonnull = len(source_index_vals)
    si_unique = len(set(source_index_vals))
    source_index_unique_rate = nonnull_rate(si_unique, si_nonnull) if si_nonnull else 0.0

    out: Dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "dataset": {
            "input_path": str(IN_PATH).replace("\\", "/"),
            "record_count": total,
            "date_min": date_min,
            "date_max": date_max,
            "dataset_fingerprint": fp,
            "timestamp_source": "timestamp_parsed|captured_at",
            "timestamp_nonnull_count": len(ts_vals),
        },
        "definitions": {
            "canonical_timestamp": "timestamp_parsed if present else captured_at if present else null",
            "field_nonnull_rate": "nonnull_count / record_count",
            "source_index_unique_rate": "unique(source_index) / nonnull(source_index)",
            "parent_context_equals_thread_permalink_rate": "for comments with both fields, share where parent_context == thread_permalink",
            "sentiment_compound_range": "expected in [-1, 1]",
        },
        "counts": {
            "item_type_counts": dict(sorted(item_type_counts.items())),
            "parent_author_type_counts": dict(sorted(parent_author_type_counts.items())),
            "thread_primary_topic_nonnull_count": thread_primary_topic_nonnull,
        },
        "completeness": {
            "fields": {
                k: {
                    "nonnull_count": nonnull_counts[k],
                    "nonnull_rate": nonnull_rate(nonnull_counts[k], total),
                }
                for k in FIELD_LIST
            }
        },
        "topics": {
            "topics_nonempty_count": topics_nonempty,
            "topics_nonempty_rate": nonnull_rate(topics_nonempty, total),
            "topics_unique_vocab_size": len(topics_vocab),
            "topic_counts_top10": [
                {"topic": t, "count": c}
                for t, c in topics_vocab.most_common(10)
            ],
            "topics_per_item": {
                "min": topics_min,
                "max": topics_max,
                "mean": topics_mean,
                "median": topics_median,
            },
        },
        "integrity": {
            "source_index": {
                "nonnull_count": si_nonnull,
                "unique_count": si_unique,
                "unique_rate": source_index_unique_rate,
            },
            "parent_context_equals_thread_permalink": {
                "eligible_count": pc_tp_eligible,
                "equal_count": pc_eq_tp_count,
                "equal_rate": nonnull_rate(pc_eq_tp_count, pc_tp_eligible) if pc_tp_eligible else 0.0,
            },
        },
        "sentiment": {
            "sentiment_compound_nonnull_count": len(sent_vals),
            "sentiment_compound_nonnull_rate": nonnull_rate(len(sent_vals), total),
            "sentiment_compound_out_of_range_count": sent_out_of_range,
            "mean_compound": safe_mean(sent_vals),
            "median_compound": safe_median(sent_vals),
        },
    }

    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with OUT_PATH.open("w", encoding="utf-8", newline="\n") as f:
        json.dump(out, f, indent=2, sort_keys=True)
        f.write("\n")

    print(f"Wrote: {OUT_PATH}")


if __name__ == "__main__":
    main()
