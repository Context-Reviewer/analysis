# Running the Analysis Pipeline

This document describes how to execute the analysis pipeline locally.

---

## Overview

The pipeline consists of four deterministic steps executed in strict order:

1. **Normalize posts** — Clean and standardize raw post data.
2. **Adapt comments** — Transform extracted comments to the normalized schema.
3. **Build timeline** — Merge posts and comments into a chronological dataset.
4. **Analyze** — Apply rule-based topic tagging and compute sentiment metadata.

**Governance principles:**

- Each step is deterministic given identical inputs.
- Steps are separated by phase; no step performs work belonging to another.
- No inference, ranking, or filtering by content occurs during normalization.
- Generated artifacts are not committed to the repository.

---

## Prerequisites

| Requirement | Details |
|-------------|---------|
| Python | 3.11+ recommended |
| OS | Windows (PowerShell commands documented) |
| Working directory | Repository root |
| Scraper output | Comment extraction file must exist at the configured path |

---

## Configuration

### Input Paths

Inputs are configured at the top of each script. The only external dependency is the scraper output path.

**`pipeline/step1b_adapt_comments.py`:**

```python
SRC_COMMENTS = Path(r"<path-to-scraper-output>/sean_roy_comments.jsonl")
```

Update this path to match your local environment before running.

### Output Directory

All outputs are written to `fb_extract_out/` (relative to repo root). This directory is created automatically if it does not exist.

---

## Execution

Run all commands from the repository root.

### Step 1: Normalize Posts

```powershell
python .\pipeline\step1_normalize_posts.py
```

**Reads:**
- `fb_extract_out/posts_all.jsonl` — Raw post captures (JSON array or JSONL)

**Writes:**
- `fb_extract_out/posts_normalized_sean.jsonl` — Normalized posts for target author

**Behavior:**
- Extracts clean body text from mbasic plaintext captures.
- Parses relative timestamps to ISO8601 where possible.
- Filters to target author (`Sean Roy`).

---

### Step 1b: Adapt Comments

```powershell
python .\pipeline\step1b_adapt_comments.py
```

**Reads:**
- External scraper output (path configured in `SRC_COMMENTS`)

**Writes:**
- `fb_extract_out/comments_normalized_sean.jsonl`

**Behavior:**
- Schema normalization only (no filtering, no inference).
- Validates required fields; fails loudly on malformed data.
- Preserves input order (deterministic).
- Schema version: `normalized_item_v1`

---

### Step 2: Build Timeline

```powershell
python .\pipeline\step2_build_timeline.py
```

**Reads:**
- `fb_extract_out/posts_normalized_sean.jsonl`
- `fb_extract_out/comments_normalized_sean.jsonl`

**Writes:**
- `fb_extract_out/sean_timeline.json` — Full timeline (JSON)
- `fb_extract_out/sean_timeline.csv` — Timeline for spreadsheet review
- `fb_extract_out/sean_stats.json` — Summary statistics

**Behavior:**
- Merges posts and comments into a single dataset.
- Sorts by locked key tuple in the following order:
  1. `timestamp_parsed` (nulls last)
  2. `item_type` (`post` before `comment`)
  3. `permalink`
  4. `source_index`
- Drops records with empty/trivial bodies (< 5 characters).

---

### Step 3: Analyze

```powershell
python .\pipeline\step3_analyze_reason.py
```

**Reads:**
- `fb_extract_out/sean_timeline.json`

**Writes:**
- `fb_extract_out/sean_topics.csv` — Topic assignments per record
- `fb_extract_out/sean_report.md` — Summary report with representative posts

**Behavior:**
- Rule-based topic tagging using regex keyword lists.
- VADER sentiment analysis (metadata only, not used for filtering/sorting).
- Topic rules are editable in the script (`TOPIC_RULES` dict).

---

## Verification

### View Statistics

```powershell
Get-Content .\fb_extract_out\sean_stats.json
```

### Preview Normalized Comments

```powershell
Get-Content .\fb_extract_out\comments_normalized_sean.jsonl -Head 3
```

### Check Timeline Record Count

```powershell
(Get-Content .\fb_extract_out\sean_timeline.json | ConvertFrom-Json).Count
```

---

## Troubleshooting

| Symptom | Cause | Resolution |
|---------|-------|------------|
| `Missing input: ...` | Required input file does not exist | Run prior steps or verify scraper output path |
| `FAIL: malformed JSONL` | Upstream file contains invalid JSON lines | Fix source extraction artifact |
| `FAIL: non-canonical URL` | Comment URL missing `/posts/` or `/permalink/` | Fix upstream scraper output |
| `No records found` | Input file is empty | Verify upstream pipeline produced data |

---

## Notes

- This repository is code-only. Generated artifacts under `fb_extract_out/` are ignored by git.
- Absolute paths in scripts are user-configurable; update them for your environment.
- After relocating the repository, update any absolute paths in scripts as needed.
