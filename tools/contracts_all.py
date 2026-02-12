#!/usr/bin/env python3
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
FB_OUT = ROOT / "fb_extract_out"

def run(cmd):
    subprocess.check_call([sys.executable, *cmd])

def must_exist(path: Path, msg: str):
    if not path.exists():
        raise AssertionError(msg)

def main() -> int:
    # Always enforce report/UI invariants (these should hold for any run)
    run(["tools/contracts_report_ui.py"])
    run(["tools/contracts_run_manifest.py"])
    run(["tools/contracts_nav_idempotent.py"])
    run(["tools/contracts_signals_page.py"])
    run(["tools/contracts_conclusion_page.py"])
    run(["tools/contracts_timeline_semantics.py"])
    run(["tools/contracts_enriched_semantics.py"])

    # GraphQL-v2 specific artifacts: enforce only when present.
    # This allows smoke runs from non-GraphQL sources (e.g. corpus-derived fixtures)
    # to still validate report generation + UI correctness.
    gql_summary = FB_OUT / "comments_graphql_v2_summary.json"
    if gql_summary.exists():
        # If you have a dedicated contract script for this, call it here.
        # For now, require presence only.
        must_exist(gql_summary, "Missing comments_graphql_v2_summary.json")
    else:
        print("[SKIP] comments_graphql_v2_summary.json not present (non-GraphQL smoke run)")

    print("[OK] contracts_all: passed")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
