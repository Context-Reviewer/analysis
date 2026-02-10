# Phase 6 (Skeleton) — Governed Analysis Pipeline

This is the **Phase 6 skeleton** for `Context-Reviewer/analysis`.

## Contract
- **No scraping / no network / no HTML assumptions**
- Deterministic outputs
- Input ordering is authoritative (timeline is ordinal-only)

## Input
Default expects: `fb_extract_out/comments_normalized.jsonl`

Each line must be JSON with at least:
- schema_version (str)
- item_type (str: "comment" or "reply" or other non-empty)
- body (str, non-empty after trim)
- author (str, non-empty)
- permalink (str, non-empty)
- source_index (int)

Optional (carried to provenance):
- timestamp_parsed, timestamp_raw, captured_at, parent_context

## Run
```bash
python phase6/run_phase6.py --config phase6/config/phase6_config-1.0.json
```

Outputs go to: `phase6/out/<RUN_ID>/`

`RUN_ID = "phase6_" + first8(sha256(input_bytes))`
