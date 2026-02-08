from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path


FB_OUT = Path("fb_extract_out")
RUNS = FB_OUT / "runs"

# Canonical inputs (your run passes these)
CANON_POSTS = FB_OUT / "posts_normalized.jsonl"
CANON_COMMENTS = FB_OUT / "comments_normalized.jsonl"

# Legacy aliases expected by the existing pipeline steps
LEGACY_POSTS = FB_OUT / "posts_normalized_sean.jsonl"
LEGACY_COMMENTS = FB_OUT / "comments_normalized_sean.jsonl"


def fail(msg: str) -> None:
    raise SystemExit("[FAIL] " + msg)


def run(cmd: list[str]) -> None:
    # show a stable command line in logs
    print("[RUN] " + " ".join(str(x) for x in cmd))
    subprocess.check_call(cmd)


def _sha256_file(p: Path) -> str:
    h = hashlib.sha256()
    with p.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _write_json(p: Path, obj: dict) -> None:
    p.parent.mkdir(parents=True, exist_ok=True)
    data = json.dumps(obj, ensure_ascii=False, sort_keys=True, indent=2)
    p.write_text(data + "\n", encoding="utf-8")


def copy_tree(src: Path, dst: Path) -> None:
    if dst.exists():
        shutil.rmtree(dst)
    shutil.copytree(src, dst)


def backup_if_exists(p: Path, backup_dir: Path) -> Path | None:
    if not p.exists():
        return None
    backup_dir.mkdir(parents=True, exist_ok=True)
    out = backup_dir / p.name
    shutil.copy2(p, out)
    return out


def restore_backup(backup: Path | None, target: Path) -> None:
    if backup is None:
        return
    if backup.exists():
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(backup, target)


def _emit_run_manifest(run_id: str, subject_label: str, posts_src: Path, comments_src: Path, run_dir: Path) -> Path:
    """
    Emit fb_extract_out/run_manifest.json that conforms EXACTLY to schemas/run_manifest-1.0.schema.json.
    additionalProperties=false => do not add extra keys.
    """
    schema_path = Path("schemas") / "run_manifest-1.0.schema.json"
    if not schema_path.exists():
        fail(f"missing schema: {schema_path}")

    # Phase 1 canonical output names
    timeline_json = FB_OUT / "sean_timeline.json"
    topics_csv = FB_OUT / "sean_topics.csv"
    enriched_jsonl = FB_OUT / "sean_context_enriched.v2.jsonl"
    signals_dir = FB_OUT / "signals"
    docs_dir = Path("docs")
    stats_path = FB_OUT / "sean_stats.json"

    required_paths = [posts_src, comments_src, timeline_json, topics_csv, enriched_jsonl, signals_dir, docs_dir, stats_path]
    for p in required_paths:
        if not p.exists():
            raise RuntimeError(f"run_manifest: missing required path: {p}")

    stats = json.loads(stats_path.read_text(encoding="utf-8"))
    counts = {
        "posts": int(stats.get("input_posts", 0)),
        "comments": int(stats.get("input_comments", 0)),
        "timeline_rows": int(stats.get("kept_nonempty", 0)),
    }

    def norm(p: Path) -> str:
        return str(p).replace("\\", "/")

    manifest = {
        "run_id": run_id,
        "subject_label": subject_label,
        "generated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "inputs": {
            "posts_path": norm(posts_src),
            "comments_path": norm(comments_src),
            "posts_sha256": _sha256_file(posts_src),
            "comments_sha256": _sha256_file(comments_src),
        },
        "counts": counts,
        "fingerprints": {},
        "artifacts": {
            "run_dir": norm(run_dir),
            "timeline_json": norm(timeline_json),
            "topics_csv": norm(topics_csv),
            "context_enriched_jsonl": norm(enriched_jsonl),
            "signals_dir": norm(signals_dir),
            "docs_dir_snapshot": norm(docs_dir),
        },
    }

    out_path = FB_OUT / "run_manifest.json"
    _write_json(out_path, manifest)
    print(f"[OK] wrote run_manifest: {out_path}")
    return out_path


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--run-id", required=True)
    ap.add_argument("--subject-label", required=True)
    ap.add_argument("--posts", required=True)
    ap.add_argument("--comments", required=True)
    args = ap.parse_args()

    FB_OUT.mkdir(parents=True, exist_ok=True)
    RUNS.mkdir(parents=True, exist_ok=True)

    posts_src = Path(args.posts)
    comments_src = Path(args.comments)
    if not posts_src.exists():
        fail(f"posts not found: {posts_src}")
    if not comments_src.exists():
        fail(f"comments not found: {comments_src}")

    run_dir = RUNS / args.run_id
    run_dir.mkdir(parents=True, exist_ok=True)

    posts_abs = posts_src.resolve()
    comments_abs = comments_src.resolve()
    canon_posts_abs = CANON_POSTS.resolve() if CANON_POSTS.exists() else None
    canon_comments_abs = CANON_COMMENTS.resolve() if CANON_COMMENTS.exists() else None

    posts_is_canon = (canon_posts_abs is not None and posts_abs == canon_posts_abs)
    comments_is_canon = (canon_comments_abs is not None and comments_abs == canon_comments_abs)

    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    backup_dir = run_dir / "_canon_backup" / stamp

    # Backup only when we will remap into canonical
    b_posts = None if posts_is_canon else backup_if_exists(CANON_POSTS, backup_dir)
    b_comments = None if comments_is_canon else backup_if_exists(CANON_COMMENTS, backup_dir)

    created_posts = False
    created_comments = False
    legacy_posts_created = False
    legacy_comments_created = False

    try:
        # Map to canonical files if needed
        if not posts_is_canon:
            if not CANON_POSTS.exists():
                created_posts = True
            shutil.copy2(posts_src, CANON_POSTS)
            print(f"[OK] mapped posts -> {CANON_POSTS}")
        else:
            print(f"[OK] posts already canonical: {CANON_POSTS}")

        if not comments_is_canon:
            if not CANON_COMMENTS.exists():
                created_comments = True
            shutil.copy2(comments_src, CANON_COMMENTS)
            print(f"[OK] mapped comments -> {CANON_COMMENTS}")
        else:
            print(f"[OK] comments already canonical: {CANON_COMMENTS}")

        # Legacy aliases for existing pipeline steps
        if not LEGACY_POSTS.exists():
            shutil.copy2(CANON_POSTS, LEGACY_POSTS)
            legacy_posts_created = True
            print(f"[OK] created legacy alias: {LEGACY_POSTS} -> {CANON_POSTS}")

        if not LEGACY_COMMENTS.exists():
            shutil.copy2(CANON_COMMENTS, LEGACY_COMMENTS)
            legacy_comments_created = True
            print(f"[OK] created legacy alias: {LEGACY_COMMENTS} -> {CANON_COMMENTS}")

        # Pipeline steps
        run([sys.executable, "pipeline/step2_build_timeline.py"])
        run([sys.executable, "pipeline/step3_analyze_reason.py"])
        run([sys.executable, "tools/context_enrich.py"])
        run([sys.executable, "tools/data_quality_metrics.py"])
        run([sys.executable, "tools/behavioral_metrics_v0_3.py"])

        # Signals: run all specs deterministically
        specs_dir = Path("signals")
        if not specs_dir.exists():
            fail("missing signals/ directory")

        signal_input = FB_OUT / "sean_context_enriched.v2.jsonl"
        if not signal_input.exists():
            fail(f"missing expected signal input: {signal_input}")

        out_dir = FB_OUT / "signals"
        out_dir.mkdir(parents=True, exist_ok=True)

        specs = sorted(specs_dir.glob("*.json"))
        if not specs:
            fail("no signal specs found in signals/*.json")

        for spec in specs:
            run([sys.executable, "tools/run_signal.py", str(spec), str(signal_input), str(out_dir)])

        run([sys.executable, "tools/build_signals_index.py"])
        run([sys.executable, "tools/generate_signals_page.py"])
        run([sys.executable, "tools/generate_topic_category_pages.py"])
        run([sys.executable, "tools/generate_conclusion_page.py"])
        run([sys.executable, "tools/patch_global_nav.py"])
        run([sys.executable, "tools/patch_topic_nav.py"])

        # Emit schema-locked run manifest BEFORE contracts/cleanup
        _emit_run_manifest(
            run_id=args.run_id,
            subject_label=args.subject_label,
            posts_src=posts_src,
            comments_src=comments_src,
            run_dir=run_dir,
        )

        # Contracts
        run([sys.executable, "tools/contracts_all.py"])

        # Snapshot artifacts into run_dir (published_docs + artifacts)
        (run_dir / "artifacts").mkdir(parents=True, exist_ok=True)
        shutil.copy2(FB_OUT / "sean_timeline.json", run_dir / "artifacts" / "sean_timeline.json")
        shutil.copy2(FB_OUT / "sean_topics.csv", run_dir / "artifacts" / "sean_topics.csv")
        shutil.copy2(FB_OUT / "sean_context_enriched.v2.jsonl", run_dir / "artifacts" / "sean_context_enriched.v2.jsonl")

        copy_tree(FB_OUT / "signals", run_dir / "artifacts" / "signals")
        copy_tree(Path("docs"), run_dir / "published_docs" / "docs")

        print(f"[OK] run complete: {args.run_id}")

    finally:
        # Restore canonical backups if we overwrote them
        restore_backup(b_posts, CANON_POSTS)
        restore_backup(b_comments, CANON_COMMENTS)

        # Remove legacy aliases we created
        try:
            if legacy_posts_created and LEGACY_POSTS.exists():
                LEGACY_POSTS.unlink()
                print(f"[OK] removed legacy alias: {LEGACY_POSTS}")
            if legacy_comments_created and LEGACY_COMMENTS.exists():
                LEGACY_COMMENTS.unlink()
                print(f"[OK] removed legacy alias: {LEGACY_COMMENTS}")
        except Exception:
            pass

        # Remove only temporary canonical files we created (don’t delete real canonical)
        try:
            if created_posts and CANON_POSTS.exists() and not posts_is_canon:
                CANON_POSTS.unlink()
                print(f"[OK] removed temporary {CANON_POSTS}")
            if created_comments and CANON_COMMENTS.exists() and not comments_is_canon:
                CANON_COMMENTS.unlink()
                print(f"[OK] removed temporary {CANON_COMMENTS}")
        except Exception:
            pass


if __name__ == "__main__":
    main()


