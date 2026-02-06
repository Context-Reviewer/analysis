#!/usr/bin/env python3
"""
Orchestrator for analysis pipeline.

Implements ORCHESTRATOR_SPEC.md exactly. No deviations.

Usage:
    python tools/orchestrate.py --config config/subjects/sean_roy.json
"""
import argparse
import json
import os
import re
import subprocess
import sys
from pathlib import Path


def log(msg: str) -> None:
    """Minimal logging per spec."""
    print(f"[orchestrate] {msg}")


def fail(msg: str, code: int = 1) -> None:
    """Log error and exit with specified code."""
    log(f"ERROR: {msg}")
    sys.exit(code)


def validate_config(config: dict) -> None:
    """
    Validate config per ORCHESTRATOR_SPEC.md acceptance checklist items 1-3.
    Fails loud on any validation error.
    """
    # Requirement 1: schema_version must equal "subject_config_v1"
    if config.get("schema_version") != "subject_config_v1":
        fail(f"schema_version must be 'subject_config_v1', got: {config.get('schema_version')}")
    
    # Requirement 2: subject.id must match ^[a-z0-9_]+$
    subject = config.get("subject", {})
    subject_id = subject.get("id", "")
    if not re.match(r"^[a-z0-9_]+$", subject_id):
        fail(f"subject.id must match ^[a-z0-9_]+$, got: {subject_id!r}")
    
    display_name = subject.get("display_name", "")
    if not display_name:
        fail("subject.display_name must be non-empty")
    
    # Requirement 3: Input paths must exist
    inputs = config.get("inputs", {})
    posts_jsonl = inputs.get("posts_jsonl")
    comments_jsonl = inputs.get("comments_jsonl")
    
    if posts_jsonl is None:
        fail("inputs.posts_jsonl is required (cannot be null in v1)")
    
    if not Path(posts_jsonl).exists():
        fail(f"inputs.posts_jsonl does not exist: {posts_jsonl}")
    
    if comments_jsonl is not None and not Path(comments_jsonl).exists():
        fail(f"inputs.comments_jsonl does not exist: {comments_jsonl}")
    
    # Requirement 3: intermediate_dir must exist and be writable
    paths = config.get("paths", {})
    intermediate_dir = paths.get("intermediate_dir")
    
    if not intermediate_dir:
        fail("paths.intermediate_dir is required")
    
    if not Path(intermediate_dir).is_dir():
        fail(f"paths.intermediate_dir does not exist: {intermediate_dir}")
    
    # Check writable by attempting to create a temp file
    test_file = Path(intermediate_dir) / ".orchestrate_write_test"
    try:
        test_file.touch()
        test_file.unlink()
    except Exception as e:
        fail(f"paths.intermediate_dir is not writable: {e}")
    
    report_json = paths.get("report_json")
    if not report_json:
        fail("paths.report_json is required")
    
    # Ensure parent directory exists for report_json
    report_parent = Path(report_json).parent
    if not report_parent.is_dir():
        fail(f"Parent directory for paths.report_json does not exist: {report_parent}")
    
    log("Config validation passed")


def run_step(name: str, script_path: str) -> None:
    """
    Run a pipeline step. Fail loud if exit code != 0.
    Per spec: propagate exact exit code on failure.
    """
    log(f"Running {script_path}")
    
    result = subprocess.run(
        [sys.executable, script_path],
        cwd=Path(__file__).parent.parent  # Run from repo root
    )
    
    if result.returncode != 0:
        log(f"{name} failed (exit {result.returncode})")
        sys.exit(result.returncode)
    
    log(f"{name} complete (exit 0)")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Orchestrate analysis pipeline per ORCHESTRATOR_SPEC.md"
    )
    parser.add_argument(
        "--config",
        required=True,
        help="Path to subject config JSON (e.g., config/subjects/sean_roy.json)"
    )
    args = parser.parse_args()
    
    # Load config
    config_path = Path(args.config)
    if not config_path.exists():
        fail(f"Config file not found: {config_path}")
    
    log("Validating config...")
    try:
        with open(config_path, encoding="utf-8") as f:
            config = json.load(f)
    except json.JSONDecodeError as e:
        fail(f"Invalid JSON in config: {e}")
    
    # Validate config (requirements 1-3)
    validate_config(config)
    
    # Determine if step1b should run (requirement 8)
    comments_jsonl = config.get("inputs", {}).get("comments_jsonl")
    run_step1b = comments_jsonl is not None
    
    # Execute steps in exact order (requirement 4)
    # Step 1
    run_step("step1", "pipeline/step1_normalize_posts.py")
    
    # Step 1b (only if comments_jsonl is not null)
    if run_step1b:
        run_step("step1b", "pipeline/step1b_adapt_comments.py")
    else:
        log("Skipping step1b (comments_jsonl is null)")
    
    # Step 2
    run_step("step2", "pipeline/step2_build_timeline.py")
    
    # Step 3
    run_step("step3", "pipeline/step3_analyze_reason.py")
    
    # Step 4 (generate_report_json)
    run_step("generate_report_json", "tools/generate_report_json.py")
    
    log("Pipeline complete")


if __name__ == "__main__":
    main()
