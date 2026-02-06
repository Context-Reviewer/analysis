# Orchestrator Acceptance Specification v1

**Status:** Locked contract. Implementation must match exactly.

---

## Entrypoint

```
python tools/orchestrate.py --config config\subjects\sean_roy.json
```

---

## Acceptance Checklist

| # | Requirement |
|---|-------------|
| 1 | Validate `schema_version == "subject_config_v1"` or exit 1 |
| 2 | Validate `subject.id` matches `^[a-z0-9_]+$` or exit 1 |
| 3 | All input paths must exist. `intermediate_dir` must exist and be writable. Exit 1 if not. |
| 4 | Execute: step1 → step1b (if `comments_jsonl` not null) → step2 → step3 → generate_report_json |
| 5 | Zero inference — no logic based on file contents, sizes, or patterns |
| 6 | Zero schema changes — outputs match existing step outputs exactly |
| 7 | Byte-identical `docs/data/report.json` to manual run on same inputs |
| 8 | Only `step1b` may be skipped, only when `comments_jsonl == null` |
| 9 | Any step exit code ≠ 0 → exit immediately with that code |
| 10 | Orchestrator must not delete or overwrite intermediates |

---

## Exit Codes

| Code | Meaning |
|------|---------|
| 0 | Success |
| 1 | Orchestrator validation/path failure |
| N | Propagate failing step's exact exit code |

---

## Verification (PowerShell)

```powershell
# Manual run
python pipeline/step1_normalize_posts.py
python pipeline/step1b_adapt_comments.py  # if applicable
python pipeline/step2_build_timeline.py
python pipeline/step3_analyze_reason.py
python tools/generate_report_json.py
(Get-FileHash docs\data\report.json -Algorithm SHA256).Hash | Out-File manual.sha256 -Encoding ascii

# Orchestrated run (clean intermediates manually first if needed)
Remove-Item -Recurse -Force fb_extract_out\* -ErrorAction SilentlyContinue
python tools/orchestrate.py --config config\subjects\sean_roy.json
(Get-FileHash docs\data\report.json -Algorithm SHA256).Hash | Out-File orchestrated.sha256 -Encoding ascii

# Compare — must show no differences
Compare-Object (Get-Content manual.sha256) (Get-Content orchestrated.sha256)
```

---

## Logging (Minimal)

```
[orchestrate] Validating config...
[orchestrate] Running step1_normalize_posts.py
[orchestrate] step1 complete (exit 0)
[orchestrate] Skipping step1b (comments_jsonl is null)
[orchestrate] Running step2_build_timeline.py
...
```

---

## Clean Run Policy (v1)

- Orchestrator does **NOT** wipe intermediates
- No `--clean` flag in v1
- User wipes manually if clean run needed

---

## Non-Goals (v1)

| Non-Goal | Reason |
|----------|--------|
| Run isolation / unique dirs | Not needed for single-subject |
| Parallel execution | Steps are sequential by design |
| Retry logic | Fail loud, user diagnoses |
| Incremental runs | All steps run every time |
| Config templating | No `{subject.id}` expansion |
| Dashboard deployment | Out of scope |
| Intermediate checksums | Future consideration |
| `--clean` flag | Future consideration |
