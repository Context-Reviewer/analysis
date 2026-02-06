# Governance

## Phase Separation

This pipeline is structured into discrete phases: **Adapter** (schema normalization), **Merge** (timeline construction), and **Analysis** (topic tagging and metadata). Each phase reads from the output of the previous phase and writes to a well-defined artifact. Phases must be executed in order. No phase may modify artifacts produced by earlier phases.

## Generated Artifacts

Generated artifacts (`fb_extract_out/`, `docs/data/*.json`, CSVs, timelines, reports) are ignored by git by default. They are not committed to the repository unless explicitly intended as a snapshot. This keeps the repository code-only and avoids accidental data commits.

## Snapshots

To commit a data snapshot, stage the files explicitly and tag the commit with a version identifier (e.g., `v1.0-snapshot-2026-02-05`).

## Packs (Immutable Snapshots)

A **pack** is the canonical, immutable snapshot format for this project. Packs live **outside** the repository working tree and contain a complete copy of the artifacts required to reproduce analysis results and publish docs.

### Pack Location

Packs are stored at:

`<pack_root>\<subject>\<run_id>\`

Default pack root (local):
`C:\dev\repos\context-reviewer-packs\`

Example:
`C:\dev\repos\context-reviewer-packs\sean_roy\2026-02-06T1954Z\`

### Pack Contents

Each pack contains:

- `manifest.json` (required)
- `files\...` (copied artifacts, preserving repository-relative paths)

`files\` MUST include the generated artifacts required for analysis/publishing, including:
- `fb_extract_out\*.jsonl`
- `fb_extract_out\*.json`
- `fb_extract_out\*.csv`
- `fb_extract_out\*.md`
- `docs\**`
- `README.md` (optional but allowed)

### Manifest Contract (v1.0)

`manifest.json` is the single source of truth for pack contents and integrity. It MUST include:
- `pack_version` (currently `1.0`)
- `subject`
- `run_id`
- `created_utc`
- `source_repo_root`
- `included_globs`
- `file_count`
- `total_bytes`
- `files[]` entries with:
  - `relpath` (repo-relative, forward-slash normalized)
  - `bytes`
  - `sha256`

The `files[]` list MUST be sorted deterministically by `relpath`.

### Immutability

Once created, a pack is immutable:
- Do not edit, delete, or overwrite files inside an existing pack directory.
- To regenerate, create a new pack with a new `run_id`.

### Creation

Packs are created using:

`py .\tools\pack\freeze.py --subject <subject> --run-id <run_id>`

### Repository Policy

Generated artifacts remain ignored by git by default. If a reproducible snapshot is needed:
- Prefer creating a pack (external snapshot) over committing generated artifacts.
- Commits should record code + configuration changes; packs record data outputs.
