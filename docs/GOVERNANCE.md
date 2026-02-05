# Governance

## Phase Separation

This pipeline is structured into discrete phases: **Adapter** (schema normalization), **Merge** (timeline construction), and **Analysis** (topic tagging and metadata). Each phase reads from the output of the previous phase and writes to a well-defined artifact. Phases must be executed in order. No phase may modify artifacts produced by earlier phases.

## Generated Artifacts

Generated artifacts (`fb_extract_out/`, `docs/data/*.json`, CSVs, timelines, reports) are ignored by git by default. They are not committed to the repository unless explicitly intended as a snapshot. This keeps the repository code-only and avoids accidental data commits.

## Snapshots

To commit a data snapshot, stage the files explicitly and tag the commit with a version identifier (e.g., `v1.0-snapshot-2026-02-05`).
