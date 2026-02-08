import json
import hashlib
import statistics
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


IN_PATH = Path("fb_extract_out/sean_context_enriched.v2.jsonl")
OUT_PATH = Path("docs/data/behavioral_metrics_v0_3.json")
SCHEMA_VERSION = "behavioral_metrics-0.3"
ROUND_N = 4
NEG_THRESH = -0.05


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


def neg_rate(compounds: List[float]) -> Optional[float]:
    if not compounds:
        return None
    return r4(sum(1 for c in compounds if c <= NEG_THRESH) / len(compounds))


def main() -> None:
    records = read_jsonl(IN_PATH)
    total = len(records)
    fp = sha256_file_bytes(IN_PATH)

    # Overall topic counts (presence-based)
    topic_counts = Counter()

    # Per-topic compounds split by parent_author_type
    topic_compounds_self = defaultdict(list)
    topic_compounds_other = defaultdict(list)
    topic_compounds_all = defaultdict(list)

    # Negativity concentration
    negative_total = 0
    negative_topic_counts = Counter()  # topic -> count of negative comments containing topic

    for r in records:
        topics = r.get("topics") or []
        if not isinstance(topics, list):
            topics = []
        topics_set = sorted(set(str(t) for t in topics if t is not None))

        parent = str(r.get("parent_author_type") or "unknown")

        sc = r.get("sentiment_compound")
        if not isinstance(sc, (int, float)):
            fail("Missing or non-numeric sentiment_compound in input record")
        scf = float(sc)

        for t in topics_set:
            topic_counts[t] += 1
            topic_compounds_all[t].append(scf)
            if parent == "self":
                topic_compounds_self[t].append(scf)
            elif parent == "other":
                topic_compounds_other[t].append(scf)

        if scf <= NEG_THRESH:
            negative_total += 1
            for t in topics_set:
                negative_topic_counts[t] += 1

    # Top topics by overall presence
    top_topics = sorted(topic_counts.items(), key=lambda kv: (-kv[1], kv[0]))[:10]
    top_topic_names = [t for t, _ in top_topics]

    # Per-topic deltas
    per_topic = {}
    for t in top_topic_names:
        all_comp = topic_compounds_all.get(t, [])
        self_comp = topic_compounds_self.get(t, [])
        other_comp = topic_compounds_other.get(t, [])

        mean_self = safe_mean(self_comp)
        mean_other = safe_mean(other_comp)
        nr_self = neg_rate(self_comp)
        nr_other = neg_rate(other_comp)

        delta_mean = None
        if mean_self is not None and mean_other is not None:
            delta_mean = r4(mean_self - mean_other)

        delta_nr = None
        if nr_self is not None and nr_other is not None:
            delta_nr = r4(nr_self - nr_other)

        per_topic[t] = {
            "count_total": len(all_comp),
            "count_self": len(self_comp),
            "count_other": len(other_comp),
            "mean_compound_self": mean_self,
            "mean_compound_other": mean_other,
            "negative_rate_self": nr_self,
            "negative_rate_other": nr_other,
            "delta_mean_compound": delta_mean,
            "delta_negative_rate": delta_nr,
        }

    # Negativity concentration: top topics by negative counts
    neg_rank = sorted(negative_topic_counts.items(), key=lambda kv: (-kv[1], kv[0]))
    top5_neg = [t for t, _ in neg_rank[:5]]
    top10_neg = [t for t, _ in neg_rank[:10]]

    # Because comments can have multiple topics, concentration uses "any of set" overlap counts
    def negative_any_of(topic_set: List[str]) -> int:
        # Count negative comments that include any topic in topic_set (avoid double count)
        n = 0
        for r in records:
            topics = r.get("topics") or []
            if not isinstance(topics, list):
                continue
            ts = set(str(t) for t in topics if t is not None)
            sc = r.get("sentiment_compound")
            scf = float(sc)  # already validated
            if scf <= NEG_THRESH and any(t in ts for t in topic_set):
                n += 1
        return n

    neg_any_top5 = negative_any_of(top5_neg) if negative_total else 0
    neg_any_top10 = negative_any_of(top10_neg) if negative_total else 0

    # Rankings by abs deltas
    def abs_key_mean(t: str) -> Tuple[float, str]:
        d = per_topic[t]["delta_mean_compound"]
        return (abs(d) if d is not None else -1.0, t)

    def abs_key_nr(t: str) -> Tuple[float, str]:
        d = per_topic[t]["delta_negative_rate"]
        return (abs(d) if d is not None else -1.0, t)

    ranked_abs_delta_mean = sorted(top_topic_names, key=lambda t: (-abs_key_mean(t)[0], abs_key_mean(t)[1]))
    ranked_abs_delta_nr = sorted(top_topic_names, key=lambda t: (-abs_key_nr(t)[0], abs_key_nr(t)[1]))

    out = {
        "schema_version": SCHEMA_VERSION,
        "dataset": {
            "input_path": str(IN_PATH).replace("\\", "/"),
            "record_count": total,
            "dataset_fingerprint": fp,
        },
        "definitions": {
            "negative_compound_threshold": NEG_THRESH,
            "topic_presence": "topic appears in item.topics list",
            "negative_comment": f"sentiment_compound <= {NEG_THRESH}",
            "delta_mean_compound": "mean_compound_self - mean_compound_other (if both defined)",
            "delta_negative_rate": "negative_rate_self - negative_rate_other (if both defined)",
        },
        "negativity_concentration": {
            "negative_total": negative_total,
            "top_topics_by_negative_count": [{"topic": t, "negative_count": c} for t, c in neg_rank[:10]],
            "negative_any_top5_count": neg_any_top5,
            "negative_any_top5_rate": r4(neg_any_top5 / negative_total) if negative_total else 0.0,
            "negative_any_top10_count": neg_any_top10,
            "negative_any_top10_rate": r4(neg_any_top10 / negative_total) if negative_total else 0.0,
        },
        "topic_self_other_deltas_top10": per_topic,
        "rankings": {
            "topics_ranked_by_abs_delta_mean_compound": ranked_abs_delta_mean,
            "topics_ranked_by_abs_delta_negative_rate": ranked_abs_delta_nr,
        },
    }

    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with OUT_PATH.open("w", encoding="utf-8", newline="\n") as f:
        json.dump(out, f, indent=2, sort_keys=True)
        f.write("\n")

    print(f"Wrote: {OUT_PATH}")


if __name__ == "__main__":
    main()


