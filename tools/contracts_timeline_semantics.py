#!/usr/bin/env python3
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
FB_OUT = ROOT / "fb_extract_out"

def load_json(path: Path):
    return json.loads(path.read_text(encoding="utf-8"))

def load_jsonl(path: Path):
    rows = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if line.strip():
            rows.append(json.loads(line))
    return rows

def parent_context_to_str(pc) -> str:
    if pc is None:
        return ""
    if isinstance(pc, str):
        return pc.strip()
    if isinstance(pc, dict):
        for k in ("thread_url", "permalink", "parent_url", "url", "thread_permalink"):
            v = pc.get(k)
            if isinstance(v, str) and v.strip():
                return v.strip()
        return json.dumps(pc, ensure_ascii=False, sort_keys=True)
    return str(pc).strip()

def extract_time(it: dict) -> str:
    # Prefer parsed time; accept captured_at as fallback (common in smoke/fixture runs)
    ts = (
        (it.get("timestamp_parsed") or "").strip()
        or (it.get("created_time") or "").strip()
        or (it.get("timestamp") or "").strip()
        or (it.get("captured_at") or "").strip()
        or (it.get("ts") or "").strip()
        or (it.get("time") or "").strip()
    )
    return ts

def main() -> int:
    enriched = FB_OUT / "sean_context_enriched.v2.jsonl"
    timeline = FB_OUT / "sean_timeline.json"

    if enriched.exists():
        items = load_jsonl(enriched)
        src = enriched
    else:
        if not timeline.exists():
            raise AssertionError("Missing sean_timeline.json (no timeline to validate)")
        obj = load_json(timeline)
        items = obj.get("items") or obj.get("timeline") or obj.get("rows") or []
        src = timeline

    if not isinstance(items, list) or not items:
        raise AssertionError(f"No timeline/enriched items found in {src}")

    missing_body = 0
    missing_time = 0

    for it in items:
        _ = parent_context_to_str(it.get("parent_context"))
        body = (it.get("body") or it.get("text") or "").strip()
        if not body:
            missing_body += 1

        ts = extract_time(it)
        if not ts:
            missing_time += 1

    if missing_body:
        raise AssertionError(f"Missing body/text rows: {missing_body}")
    if missing_time:
        raise AssertionError(f"Missing timestamp rows: {missing_time}")

    print(f"[OK] timeline semantics contract: passed ({len(items)} items) src={src}")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())