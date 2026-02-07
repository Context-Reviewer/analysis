import json
import hashlib
import statistics
from collections import Counter, defaultdict
from itertools import combinations
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


IN_PATH = Path("fb_extract_out/sean_context_enriched.jsonl")
OUT_PATH = Path("docs/data/behavioral_metrics_v0_2.json")
SCHEMA_VERSION = "behavioral_metrics-0.2"
ROUND_N = 4


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
                fail(f"Invalid JSON at line {lineno}: {e}")
            if not isinstance(obj, dict):
                fail(f"Non-object JSON at line {lineno}")
            out.append(obj)
    if not out:
        fail("Input JSONL is empty")
    return out


def r4(x: Optional[float]) -> Optional[float]:
    if x is None:
        return None
    return round(float(x), ROUND_N)


def safe_mean(xs: List[float]) -> Optional[float]:
    return r4(sum(xs) / len(xs)) if xs else None


def safe_median(xs: List[float]) -> Optional[float]:
    return r4(float(statistics.median(xs))) if xs else None


def topic_group(topic: str) -> str:
    if "_" in topic:
        return topic.split("_", 1)[0]
    return "other"


def main() -> None:
    records = read_jsonl(IN_PATH)
    total = len(records)
    fp = sha256_file_bytes(IN_PATH)

    # Topic density
    topic_counts: List[int] = []
    single_topic = 0
    multi_topic = 0

    # Co-occurrence
    co_occurrence = Counter()

    # Divergence
    divergent_total = 0
    divergent_by_parent = Counter()
    divergent_by_primary = Counter()
    primary_counts = Counter()

    for r in records:
        topics = r.get("topics") or []
        if not isinstance(topics, list):
            topics = []

        tcount = len(topics)
        topic_counts.append(tcount)

        if tcount == 1:
            single_topic += 1
        if tcount >= 2:
            multi_topic += 1

        # Co-occurrence
        for a, b in combinations(sorted(set(topics)), 2):
            co_occurrence[(a, b)] += 1

        # Divergence
        groups = {topic_group(t) for t in topics if isinstance(t, str)}
        is_divergent = len(groups) >= 2

        primary = r.get("thread_primary_topic") or "unknown"
        parent = r.get("parent_author_type") or "unknown"

        primary_counts[str(primary)] += 1

        if is_divergent:
            divergent_total += 1
            divergent_by_parent[str(parent)] += 1
            divergent_by_primary[str(primary)] += 1

    # Topic density stats
    density = {
        "min": min(topic_counts) if topic_counts else 0,
        "max": max(topic_counts) if topic_counts else 0,
        "mean": safe_mean([float(x) for x in topic_counts]),
        "median": safe_median([float(x) for x in topic_counts]),
        "single_topic_rate": r4(single_topic / total),
        "multi_topic_rate": r4(multi_topic / total),
    }

    # Co-occurrence top 25
    co_top25 = [
        {"topic_a": a, "topic_b": b, "count": c}
        for (a, b), c in co_occurrence.most_common(25)
    ]

    # Divergence rates
    divergence = {
        "overall_rate": r4(divergent_total / total),
        "by_parent_author_type": {
            k: r4(v / sum(1 for r in records if (r.get("parent_author_type") or "unknown") == k))
            for k, v in divergent_by_parent.items()
        },
        "by_thread_primary_topic": {
            k: r4(divergent_by_primary[k] / primary_counts[k])
            for k in sorted(primary_counts, key=lambda x: (-primary_counts[x], x))[:10]
            if primary_counts[k] > 0
        },
    }

    out = {
        "schema_version": SCHEMA_VERSION,
        "dataset": {
            "input_path": str(IN_PATH).replace("\\", "/"),
            "record_count": total,
            "dataset_fingerprint": fp,
        },
        "definitions": {
            "topic_group": "prefix before '_' (e.g. politics_x → politics)",
            "divergent_comment": "contains topics from >=2 distinct topic groups",
            "co_occurrence": "unordered topic pairs within the same comment",
        },
        "topic_density": density,
        "topic_co_occurrence_top25": co_top25,
        "divergence": divergence,
    }

    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with OUT_PATH.open("w", encoding="utf-8", newline="\n") as f:
        json.dump(out, f, indent=2, sort_keys=True)
        f.write("\n")

    print(f"Wrote: {OUT_PATH}")


if __name__ == "__main__":
    main()
