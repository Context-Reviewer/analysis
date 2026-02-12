#!/usr/bin/env bash
set -euo pipefail

SUBJECT="${1:-Sean Roy}"
source .venv/bin/activate

mkdir -p fb_extract_out

python - <<'PY'
import json
from pathlib import Path
from datetime import datetime, timezone

src = Path.home() / "repos" / "fb_extractor" / "fb_extract_out" / "phase4_corpus.jsonl"
assert src.exists(), f"Missing {src}"
rows = [json.loads(l) for l in src.read_text(encoding="utf-8").splitlines() if l.strip()]
assert rows, "phase4_corpus.jsonl empty"

now = datetime.now(timezone.utc).isoformat()
thread_url = rows[0].get("thread_url") or rows[0].get("thread_permalink") or rows[0].get("thread") or ""
if not isinstance(thread_url, str) or not thread_url.startswith("http"):
    thread_url = "unknown_thread_url"

posts = [{
    "schema_version": "post-1.0",
    "item_type": "post",
    "permalink": thread_url,
    "captured_at": now,
    "timestamp_parsed": now,
    "timestamp_raw": now,
    "author": "unknown",
    "body": "(smoke) root thread post"
}]

comments = []
for r in rows:
    comments.append({
        "schema_version": "comment-1.0",
        "item_type": "comment",
        "permalink": thread_url,
        "captured_at": now,
        "timestamp_parsed": r.get("timestamp_parsed") or "",
        "timestamp_raw": r.get("timestamp_raw") or "",
        "author": r.get("author") or "",
        "body": r.get("text") or r.get("body") or "",
        "parent_context": thread_url,
        "source_index": r.get("source_index"),
    })

Path("fb_extract_out/posts_normalized.jsonl").write_text("\n".join(json.dumps(x, ensure_ascii=False) for x in posts) + "\n", encoding="utf-8")
Path("fb_extract_out/comments_normalized.jsonl").write_text("\n".join(json.dumps(x, ensure_ascii=False) for x in comments) + "\n", encoding="utf-8")
print("[OK] wrote analysis inputs posts=1 comments=", len(comments))
PY

RUN_ID="smoke__$(date -u +%Y-%m-%dT%H%M%SZ)"

python tools/run_pipeline.py \
  --run-id "$RUN_ID" \
  --subject-label "$SUBJECT" \
  --posts fb_extract_out/posts_normalized.jsonl \
  --comments fb_extract_out/comments_normalized.jsonl

python tools/contracts_all.py
echo "[OK] smoke complete: $RUN_ID"

chmod +x tools/smoke_from_extractor.sh
