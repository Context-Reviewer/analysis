#!/usr/bin/env python3
"""
Phase 6 runner (deterministic, offline).

Inputs:
- fb_extract_out/phase5_target_only.jsonl (immutable)

Outputs (under phase6/out/<run_id>/):
- phase6_normalized.jsonl
- phase6_topics.json
- phase6_tone.json
- phase6_summary.json
- phase6_manifest.json

Determinism:
- run_id derived from sha256 of input bytes
- ordering policy A: input order only (input_ordinal is line index)
- rule evaluation order is stable (rules file order)
"""

from __future__ import annotations

import argparse
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple


def fatal(msg: str) -> None:
    print(f"[phase6] FATAL: {msg}")
    raise SystemExit(2)


def sha256_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def sha256_file(path: Path) -> str:
    return sha256_bytes(path.read_bytes())


def dumps(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def read_jsonl(path: Path) -> Iterable[Tuple[int, str]]:
    with path.open("r", encoding="utf-8") as f:
        for i, line in enumerate(f, start=1):
            raw = line.rstrip("\n")
            if raw.strip() == "":
                fatal(f"Blank line at {i} (strict JSONL)")
            yield i, raw


def norm_text(s: str) -> str:
    return " ".join(s.lower().split()).strip()


def tok_est(s: str) -> int:
    s = s.strip()
    return 0 if not s else len(s.split())


def stable_id(parts: List[str], n: int = 16) -> str:
    return hashlib.sha256("|".join(parts).encode("utf-8")).hexdigest()[:n]


def load_config(path: Path) -> Dict[str, Any]:
    cfg = json.loads(path.read_text(encoding="utf-8"))
    if cfg.get("schema") != "phase6_config-1.0":
        fatal("Config schema must be 'phase6_config-1.0'")
    if not isinstance(cfg.get("input_path"), str) or not cfg["input_path"].strip():
        fatal("Config missing/invalid 'input_path'")
    if not isinstance(cfg.get("output_root"), str) or not cfg["output_root"].strip():
        fatal("Config missing/invalid 'output_root'")
    if not isinstance(cfg.get("write_markdown"), bool):
        fatal("Config missing/invalid 'write_markdown'")
    return cfg


# -------------------------
# Topics (topics-1.0)
# -------------------------
def load_topics_spec(path: Path) -> Dict[str, Any]:
    spec = json.loads(path.read_text(encoding="utf-8"))
    if spec.get("schema") != "topics-1.0":
        fatal("topics spec schema must be 'topics-1.0'")
    if not isinstance(spec.get("rules"), list):
        fatal("topics spec missing 'rules'")
    if not isinstance(spec.get("tie_break_precedence"), list):
        fatal("topics spec missing 'tie_break_precedence'")
    return spec


def topic_confidence(score: int, bands: List[Dict[str, Any]]) -> str:
    best = "n_a"
    best_min = -1
    for b in bands:
        if isinstance(b, dict):
            m = b.get("min_score")
            bid = b.get("id")
            if isinstance(m, int) and isinstance(bid, str) and score >= m and m > best_min:
                best = bid
                best_min = m
    return best


def apply_topics(records: List[Dict[str, Any]], spec: Dict[str, Any]) -> Dict[str, Any]:
    rules = spec["rules"]
    precedence = spec["tie_break_precedence"]
    bands = spec.get("confidence_bands", [])
    prec_rank = {t: i for i, t in enumerate(precedence)}

    items = []
    by_topic: Dict[str, int] = {}

    for r in records:
        txt = r["derived"]["text_normalized"]

        score_by_topic: Dict[str, int] = {}
        fired: List[str] = []
        tags: List[str] = []

        for rule in rules:
            if not isinstance(rule, dict):
                continue
            if rule.get("type") != "contains_any":
                continue

            terms = rule.get("terms", [])
            if not isinstance(terms, list):
                continue

            matched = False
            for term in terms:
                if isinstance(term, str) and term and (term.lower() in txt):
                    matched = True
                    break

            if matched:
                topic = rule.get("topic")
                rid = rule.get("rule_id")
                if isinstance(topic, str) and isinstance(rid, str):
                    score_by_topic[topic] = score_by_topic.get(topic, 0) + int(rule.get("score", 0))
                    fired.append(rid)
                    rtags = rule.get("tags", [])
                    if isinstance(rtags, list):
                        for tg in rtags:
                            if isinstance(tg, str) and tg:
                                tags.append(tg)

        if not score_by_topic:
            primary = "uncategorized"
            total = 0
            conf = "n_a"
            fired = []
            tags = []
        else:
            primary = sorted(
                score_by_topic.items(),
                key=lambda kv: (-kv[1], prec_rank.get(kv[0], 10**9)),
            )[0][0]
            total = score_by_topic[primary]
            conf = topic_confidence(total, bands)
            fired = sorted(set(fired))
            tags = sorted(set(tags))

        by_topic[primary] = by_topic.get(primary, 0) + 1

        items.append(
            {
                "id": r["id"],
                "input_ordinal": r["input_ordinal"],
                "primary_topic": primary,
                "secondary_tags": tags,
                "confidence": conf,
                "rules_fired": fired,
                "score_total": total,
            }
        )

    items.sort(key=lambda x: x["input_ordinal"])

    return {
        "schema": "phase6_topics-1.0",
        "topic_set_version": "topics-1.0",
        "items": items,
        "summary": {
            "by_topic": dict(sorted(by_topic.items(), key=lambda kv: (-kv[1], kv[0]))),
            "uncategorized": by_topic.get("uncategorized", 0),
        },
    }


# -------------------------
# Tone (tone-1.0)
# -------------------------
def load_tone_spec(path: Path) -> Dict[str, Any]:
    spec = json.loads(path.read_text(encoding="utf-8"))
    if spec.get("schema") != "tone-1.0":
        fatal("tone spec schema must be 'tone-1.0'")
    return spec


def apply_tone(records: List[Dict[str, Any]], spec: Dict[str, Any]) -> Dict[str, Any]:
    pol_rules = spec.get("polarity_rules", [])
    int_rules = spec.get("intensity_rules", [])
    pos_rules = spec.get("posture_rules", [])
    decision = spec.get("decision", {})

    def match_contains_any(txt: str, terms: Any) -> bool:
        if not isinstance(terms, list):
            return False
        for t in terms:
            if isinstance(t, str) and t and (t.lower() in txt):
                return True
        return False

    def decide_polarity(score: int) -> str:
        m = decision.get("polarity_from_score", {})
        neg_max = m.get("negative", {}).get("max")
        neu_min = m.get("neutral", {}).get("min")
        neu_max = m.get("neutral", {}).get("max")
        pos_min = m.get("positive", {}).get("min")

        if isinstance(neg_max, int) and score <= neg_max:
            return "negative"
        if isinstance(pos_min, int) and score >= pos_min:
            return "positive"
        if isinstance(neu_min, int) and isinstance(neu_max, int) and neu_min <= score <= neu_max:
            return "neutral"
        return "unknown"

    def decide_intensity(score: int) -> str:
        m = decision.get("intensity_from_score", {})
        low_max = m.get("low", {}).get("max")
        med_min = m.get("medium", {}).get("min")
        med_max = m.get("medium", {}).get("max")
        hi_min = m.get("high", {}).get("min")

        if isinstance(low_max, int) and score <= low_max:
            return "low"
        if isinstance(hi_min, int) and score >= hi_min:
            return "high"
        if isinstance(med_min, int) and isinstance(med_max, int) and med_min <= score <= med_max:
            return "medium"
        return "unknown"

    items = []
    summary_pol: Dict[str, int] = {}
    summary_int: Dict[str, int] = {}
    summary_post: Dict[str, int] = {}

    for r in records:
        txt = r["derived"]["text_normalized"]

        pol_score = 0
        int_score = 0
        post_hits: List[str] = []
        fired: List[str] = []

        for rule in pol_rules:
            if not isinstance(rule, dict):
                continue
            if rule.get("type") != "contains_any":
                continue
            if match_contains_any(txt, rule.get("terms")):
                rid = rule.get("rule_id")
                if isinstance(rid, str):
                    fired.append(rid)
                pol_score += int(rule.get("polarity_delta", 0))

        for rule in int_rules:
            if not isinstance(rule, dict):
                continue
            if rule.get("type") != "contains_any":
                continue
            if match_contains_any(txt, rule.get("terms")):
                rid = rule.get("rule_id")
                if isinstance(rid, str):
                    fired.append(rid)
                int_score += int(rule.get("intensity_delta", 0))

        for rule in pos_rules:
            if not isinstance(rule, dict):
                continue
            if rule.get("type") != "contains_any":
                continue
            if match_contains_any(txt, rule.get("terms")):
                rid = rule.get("rule_id")
                if isinstance(rid, str):
                    fired.append(rid)
                p = rule.get("posture_add")
                if isinstance(p, str) and p:
                    post_hits.append(p)

        if fired:
            polarity = decide_polarity(pol_score)
            intensity = decide_intensity(int_score)
        else:
            polarity = "unknown"
            intensity = "unknown"

        posture = sorted(set(post_hits))
        fired_sorted = sorted(set(fired))

        summary_pol[polarity] = summary_pol.get(polarity, 0) + 1
        summary_int[intensity] = summary_int.get(intensity, 0) + 1
        for p in posture:
            summary_post[p] = summary_post.get(p, 0) + 1

        items.append(
            {
                "id": r["id"],
                "input_ordinal": r["input_ordinal"],
                "polarity": polarity,
                "intensity": intensity,
                "posture": posture,
                "rules_fired": fired_sorted,
                "score_total": {
                    "polarity": pol_score,
                    "intensity": int_score,
                    "posture": {p: 1 for p in posture},
                },
            }
        )

    items.sort(key=lambda x: x["input_ordinal"])

    return {
        "schema": "phase6_tone-1.0",
        "tone_set_version": "tone-1.0",
        "items": items,
        "summary": {
            "polarity_counts": dict(sorted(summary_pol.items(), key=lambda kv: (-kv[1], kv[0]))),
            "intensity_counts": dict(sorted(summary_int.items(), key=lambda kv: (-kv[1], kv[0]))),
            "posture_counts": dict(sorted(summary_post.items(), key=lambda kv: (-kv[1], kv[0]))),
        },
    }


# -------------------------
# QA Summary (summary-1.0)
# -------------------------
def build_summary(
    records: List[Dict[str, Any]],
    topics_out: Dict[str, Any],
    tone_out: Dict[str, Any],
    examples_per_bucket: int = 5,
) -> Dict[str, Any]:
    # Build id -> minimal fields for examples
    by_id = {
        r["id"]: {
            "id": r["id"],
            "input_ordinal": r["input_ordinal"],
            "thread_id": r.get("thread_id"),
            "is_reply": r.get("is_reply"),
            "text": r.get("text", ""),
        }
        for r in records
    }

    # topic examples
    topic_examples: Dict[str, List[Dict[str, Any]]] = {}
    for it in topics_out.get("items", []):
        if not isinstance(it, dict):
            continue
        tid = it.get("id")
        topic = it.get("primary_topic", "uncategorized")
        if not isinstance(tid, str) or tid not in by_id:
            continue
        ex = dict(by_id[tid])
        ex.update(
            {
                "primary_topic": topic,
                "secondary_tags": it.get("secondary_tags", []),
                "topic_rules_fired": it.get("rules_fired", []),
            }
        )
        topic_examples.setdefault(topic, []).append(ex)

    for t in topic_examples:
        topic_examples[t].sort(key=lambda x: x["input_ordinal"])
        topic_examples[t] = topic_examples[t][:examples_per_bucket]

    # tone examples (by polarity + intensity)
    pol_examples: Dict[str, List[Dict[str, Any]]] = {}
    int_examples: Dict[str, List[Dict[str, Any]]] = {}
    for it in tone_out.get("items", []):
        if not isinstance(it, dict):
            continue
        tid = it.get("id")
        if not isinstance(tid, str) or tid not in by_id:
            continue
        pol = it.get("polarity", "unknown")
        inten = it.get("intensity", "unknown")
        ex = dict(by_id[tid])
        ex.update(
            {
                "polarity": pol,
                "intensity": inten,
                "posture": it.get("posture", []),
                "tone_rules_fired": it.get("rules_fired", []),
            }
        )
        pol_examples.setdefault(pol, []).append(ex)
        int_examples.setdefault(inten, []).append(ex)

    for k in pol_examples:
        pol_examples[k].sort(key=lambda x: x["input_ordinal"])
        pol_examples[k] = pol_examples[k][:examples_per_bucket]

    for k in int_examples:
        int_examples[k].sort(key=lambda x: x["input_ordinal"])
        int_examples[k] = int_examples[k][:examples_per_bucket]

    return {
        "schema": "phase6_summary-1.0",
        "records": len(records),
        "topics": topics_out.get("summary", {}),
        "tone": tone_out.get("summary", {}),
        "examples": {
            "by_topic": topic_examples,
            "by_polarity": pol_examples,
            "by_intensity": int_examples,
        },
    }


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", required=True)
    args = ap.parse_args()

    root = Path.cwd()
    cfg_path = (root / args.config).resolve()
    if not cfg_path.exists():
        fatal(f"Config not found: {cfg_path}")
    cfg = load_config(cfg_path)

    input_path = (root / cfg["input_path"]).resolve()
    if not input_path.exists():
        fatal(f"Input not found: {input_path}")

    input_bytes = input_path.read_bytes()
    input_sha = sha256_bytes(input_bytes)
    run_id = "phase6_" + input_sha[:8]

    out_dir = (root / cfg["output_root"] / run_id).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    records: List[Dict[str, Any]] = []
    seen = set()

    for line_no, raw in read_jsonl(input_path):
        rec = json.loads(raw)
        if not isinstance(rec, dict):
            fatal(f"Record must be object at line {line_no}")

        for k in ("author", "text", "thread_id"):
            if k not in rec:
                fatal(f"Missing '{k}' at line {line_no}")

        rid = rec.get("corpus_id")
        if rid is not None and not isinstance(rid, str):
            fatal(f"Field 'corpus_id' wrong type at line {line_no}")
        rid = (rid or "").strip()
        if not rid:
            rid = stable_id(["phase5", rec["thread_id"], norm_text(rec["text"])])

        if rid in seen:
            fatal(f"Duplicate id {rid} at line {line_no}")
        seen.add(rid)

        text = rec["text"]
        records.append(
            {
                "schema": "phase6_normalized_record-1.0",
                "id": rid,
                "input_ordinal": line_no - 1,
                "thread_id": rec["thread_id"],
                "is_reply": bool(rec.get("is_reply", False)),
                "author": rec["author"],
                "text": text,
                "derived": {
                    "text_normalized": norm_text(text),
                    "char_count": len(text),
                    "token_count_est": tok_est(text),
                    "has_question_mark": "?" in text,
                    "has_exclamation_mark": "!" in text,
                },
                "provenance": rec.get("provenance", {}),
            }
        )

    records.sort(key=lambda x: x["input_ordinal"])

    # Write normalized
    f_norm = out_dir / "phase6_normalized.jsonl"
    f_norm.write_text("\n".join(dumps(x) for x in records) + "\n", encoding="utf-8")

    # Topics
    topics_spec_path = (root / "phase6/topics/topics-1.0.json").resolve()
    if not topics_spec_path.exists():
        fatal(f"Missing topics spec: {topics_spec_path}")
    topics_spec = load_topics_spec(topics_spec_path)
    topics_out = apply_topics(records, topics_spec)
    f_topics = out_dir / "phase6_topics.json"
    f_topics.write_text(dumps(topics_out) + "\n", encoding="utf-8")

    # Tone
    tone_spec_path = (root / "phase6/tone/tone-1.0.json").resolve()
    if not tone_spec_path.exists():
        fatal(f"Missing tone spec: {tone_spec_path}")
    tone_spec = load_tone_spec(tone_spec_path)
    tone_out = apply_tone(records, tone_spec)
    f_tone = out_dir / "phase6_tone.json"
    f_tone.write_text(dumps(tone_out) + "\n", encoding="utf-8")

    # Summary (QA)
    summary_out = build_summary(records, topics_out, tone_out, examples_per_bucket=5)
    f_summary = out_dir / "phase6_summary.json"
    f_summary.write_text(dumps(summary_out) + "\n", encoding="utf-8")

    # Manifest with hashes
    manifest = {
        "schema": "phase6_manifest-1.0",
        "run_id": run_id,
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "input": {"path": cfg["input_path"], "sha256": input_sha, "records": len(records)},
        "outputs": [
            {"name": "phase6_normalized.jsonl", "sha256": sha256_file(f_norm)},
            {"name": "phase6_topics.json", "sha256": sha256_file(f_topics)},
            {"name": "phase6_tone.json", "sha256": sha256_file(f_tone)},
            {"name": "phase6_summary.json", "sha256": sha256_file(f_summary)},
        ],
    }
    (out_dir / "phase6_manifest.json").write_text(dumps(manifest) + "\n", encoding="utf-8")

    print(f"[phase6] OK: run_id={run_id} records={len(records)} out={out_dir}")


if __name__ == "__main__":
    main()

