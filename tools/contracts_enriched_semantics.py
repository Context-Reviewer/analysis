from __future__ import annotations
import json
from pathlib import Path

ENRICHED = Path("fb_extract_out/sean_context_enriched.v2.jsonl")

def fail(msg: str) -> None:
    raise SystemExit(f"[FAIL] {msg}")

def main() -> None:
    if not ENRICHED.exists():
        fail(f"Missing enriched file: {ENRICHED}")

    rows = [json.loads(l) for l in ENRICHED.read_text(encoding="utf-8").splitlines() if l.strip()]
    if not rows:
        fail("Enriched JSONL is empty")

    required = {"joined_item_type", "thread_permalink", "thread_primary_topic", "topics", "source_index"}
    for i, r in enumerate(rows, start=1):
        if not isinstance(r, dict):
            fail(f"Row {i} is not an object")
        missing = [k for k in required if k not in r]
        if missing:
            fail(f"Row {i} missing required keys: {missing}")

    kinds = {r.get("joined_item_type") for r in rows}
    if not kinds.issubset({"post", "comment"}):
        fail(f"Invalid joined_item_type values found: {sorted(kinds)}")

    print("[OK] enriched semantics contract")
    print(f"  rows: {len(rows)}")
    print(f"  joined_item_type: {sorted(kinds)}")

if __name__ == "__main__":
    main()


