# Analysis Pipeline

A deterministic pipeline for normalizing and analyzing Facebook posts and comments.

This repository is **code-only**. Generated artifacts (JSON, CSV, reports) are written locally and ignored by git.

---

## Quick Start

### Prerequisites

- **Python 3.11+** (recommended)
- **Windows + PowerShell** (documented commands assume PowerShell)
- **Working directory**: All commands must be run from the repository root.
- **Scraper output**: The comment extraction file must exist at the path configured in `pipeline/step1b_adapt_comments.py` (see `SRC_COMMENTS` variable).
- **Repository location**: This repository is intended to live under a development root such as `C:\dev\repos\analysis`; absolute paths in scripts may need adjustment if relocated.

### Execution Order

Run these scripts **in order** from the repository root:

```powershell
python .\pipeline\step1_normalize_posts.py
python .\pipeline\step1b_adapt_comments.py
python .\pipeline\step2_build_timeline.py
python .\pipeline\step3_analyze_reason.py
```

### Summary

| Step | Script | Reads | Writes |
|------|--------|-------|--------|
| 1 | `step1_normalize_posts.py` | `fb_extract_out/posts_all.jsonl` | `fb_extract_out/posts_normalized_sean.jsonl` |
| 1b | `step1b_adapt_comments.py` | External scraper output (configurable) | `fb_extract_out/comments_normalized_sean.jsonl` |
| 2 | `step2_build_timeline.py` | Normalized posts + comments | `fb_extract_out/sean_timeline.json`, `.csv`, `sean_stats.json` |
| 3 | `step3_analyze_reason.py` | Timeline JSON | `fb_extract_out/sean_topics.csv`, `sean_report.md` |

### Verification

```powershell
Get-Content .\fb_extract_out\sean_stats.json
```

---

For detailed documentation, see [docs/RUNNING.md](docs/RUNNING.md).
