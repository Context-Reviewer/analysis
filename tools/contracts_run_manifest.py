from __future__ import annotations

import json
from pathlib import Path


def fail(msg: str) -> None:
    raise SystemExit("[FAIL] " + msg)


def main() -> None:
    p = Path("fb_extract_out/run_manifest.json")
    if not p.exists():
        fail("missing fb_extract_out/run_manifest.json (run tools/run_pipeline.py first)")

    m = json.loads(p.read_text(encoding="utf-8"))

    for k in ["run_id", "subject_label", "generated_at", "inputs", "counts", "artifacts"]:
        if k not in m:
            fail(f"missing key: {k}")

    art = m["artifacts"]
    for k in ["timeline_json", "topics_csv", "context_enriched_jsonl", "signals_dir", "docs_dir_snapshot"]:
        if k not in art:
            fail(f"missing artifacts key: {k}")

    for k, v in art.items():
        if not isinstance(v, str):
            continue
        pp = Path(v)
        if not pp.exists():
            fail(f"artifact path does not exist: {k} -> {v}")

    print("[OK] run manifest contract: passed")


if __name__ == "__main__":
    main()
