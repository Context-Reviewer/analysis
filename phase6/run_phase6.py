#!/usr/bin/env python3
"""
Phase 6 skeleton runner (deterministic, offline).
Ordering policy A: input order only.

Supports:
- phase5_target_only.jsonl
"""

import argparse, hashlib, json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Tuple, Iterable


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


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", required=True)
    args = ap.parse_args()

    root = Path.cwd()
    cfg = json.loads((root / args.config).read_text(encoding="utf-8"))
    input_path = (root / cfg["input_path"]).resolve()

    input_bytes = input_path.read_bytes()
    input_sha = sha256_bytes(input_bytes)
    run_id = "phase6_" + input_sha[:8]

    out_dir = (root / cfg["output_root"] / run_id)
    out_dir.mkdir(parents=True, exist_ok=True)

    items = []
    seen = set()

    for line_no, raw in read_jsonl(input_path):
        rec = json.loads(raw)

        for k in ("author", "text", "thread_id"):
            if k not in rec:
                fatal(f"Missing '{k}' at line {line_no}")

        rid = rec.get("corpus_id") or stable_id(
            ["phase5", rec["thread_id"], norm_text(rec["text"])]
        )

        if rid in seen:
            fatal(f"Duplicate id {rid}")
        seen.add(rid)

        items.append({
            "schema": "phase6_normalized_record-1.0",
            "id": rid,
            "input_ordinal": line_no - 1,
            "thread_id": rec["thread_id"],
            "is_reply": bool(rec.get("is_reply", False)),
            "author": rec["author"],
            "text": rec["text"],
            "derived": {
                "text_normalized": norm_text(rec["text"]),
                "char_count": len(rec["text"]),
                "token_count_est": tok_est(rec["text"]),
                "has_question_mark": "?" in rec["text"],
                "has_exclamation_mark": "!" in rec["text"],
            },
            "provenance": rec.get("provenance", {}),
        })

    (out_dir / "phase6_normalized.jsonl").write_text(
        "\n".join(dumps(x) for x in items) + "\n",
        encoding="utf-8",
    )

    (out_dir / "phase6_manifest.json").write_text(
        dumps({
            "schema": "phase6_manifest-1.0",
            "run_id": run_id,
            "records": len(items),
            "input_sha256": input_sha,
            "created_at_utc": datetime.now(timezone.utc).isoformat(),
        }) + "\n",
        encoding="utf-8",
    )

    print(f"[phase6] OK: run_id={run_id} records={len(items)} out={out_dir}")


if __name__ == "__main__":
    main()
