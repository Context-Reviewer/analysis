from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import subprocess
import sys

# === RUN MANIFEST HELPERS ===
import json
import hashlib
from datetime import datetime, timezone
from pathlib import Path

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

def _safe_artifact_entry(p: Path) -> dict | None:
    if not p.exists():
        return None
    return {"path": str(p).replace("\\\\", "/"), "sha256": _sha256_file(p), "bytes": p.stat().st_size}

def _emit_run_manifest(run_id, args.subject_label, posts_abs, comments_abs, run_dir) -> Path:
    """
    Emit run_manifest.json that conforms EXACTLY to schemas/run_manifest-1.0.schema.json.
    additionalProperties=false => do not add extra keys.
    """
    from datetime import datetime, timezone

    # Helper: normalize path separators for JSON stability
    def norm(p: Path) -> str:
        return str(p).replace("\\\\", "/")

    # Required artifacts (Phase 1 uses legacy Sean filenames; Phase 2 will generalize)
    timeline_json = Path("fb_extract_out") / "sean_timeline.json"
    topics_csv = Path("fb_extract_out") / "sean_topics.csv"
    enriched_jsonl = Path("fb_extract_out") / "sean_context_enriched.jsonl"
    signals_dir = Path("fb_extract_out") / "signals"
    docs_dir = Path("docs")

    # Fail-loud: required artifacts must exist at emit-time
    required_paths = [timeline_json, topics_csv, enriched_jsonl, signals_dir, docs_dir, posts_src, comments_src]
    for p in required_paths:
        if not p.exists():
            raise RuntimeError(f"run_manifest: missing required path: {p}")

    # Counts: prefer step2 stats json for determinism
    stats_path = Path("fb_extract_out") / "sean_stats.json"
    if not stats_path.exists():
        raise RuntimeError(f"run_manifest: missing required stats file: {stats_path}")
    stats = json.loads(stats_path.read_text(encoding="utf-8"))

    counts = {
        "posts": int(stats.get("input_posts", 0)),
        "comments": int(stats.get("input_comments", 0)),
        "timeline_rows": int(stats.get("kept_nonempty", 0)),
    }

    # Fingerprints: keep flexible per schema (additionalProperties true)
    # Minimal, stable set for Phase 1
    fingerprints = {
        "posts_bytes": posts_src.stat().st_size,
        "comments_bytes": comments_src.stat().st_size,
    }

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
        "fingerprints": fingerprints,
        "artifacts": {
            "run_dir": norm(run_dir),
            "timeline_json": norm(timeline_json),
            "topics_csv": norm(topics_csv),
            "context_enriched_jsonl": norm(enriched_jsonl),
            "signals_dir": norm(signals_dir),
            "docs_dir_snapshot": norm(docs_dir),
        },
    }

    out_path = Path("fb_extract_out") / "run_manifest.json"
    _write_json(out_path, manifest)
    print(f"[OK] wrote run_manifest: {out_path}")
    return out_path

def fail(msg: str) -> None:
    raise SystemExit("[FAIL] " + msg)


def sha256_file(p: Path) -> str:
    h = hashlib.sha256()
    with p.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def run(cmd: list[str]) -> None:
    print("[RUN]", " ".join(cmd))
    r = subprocess.run(cmd)
    if r.returncode != 0:
        raise SystemExit(r.returncode)


def backup_if_exists(p: Path, backup_dir: Path) -> Path | None:
    if not p.exists():
        return None
    backup_dir.mkdir(parents=True, exist_ok=True)
    dst = backup_dir / p.name
    shutil.move(str(p), str(dst))
    print(f"[OK] backed up {p} -> {dst}")
    return dst


def restore_backup(backup_path: Path | None, original_path: Path) -> None:
    if backup_path is None:
        return
    if original_path.exists():
        original_path.unlink()
    shutil.move(str(backup_path), str(original_path))
    print(f"[OK] restored {original_path} from backup")


def copy_tree(src: Path, dst: Path) -> None:
    if dst.exists():
        shutil.rmtree(dst)
    shutil.copytree(src, dst)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--run-id", required=True)
    ap.add_argument("--subject-label", required=True)
    ap.add_argument("--posts", required=True, help="Path to normalized posts JSONL for this run")
    ap.add_argument("--comments", required=True, help="Path to normalized comments JSONL for this run")
    args = ap.parse_args()

    run_id = args.run_id.strip()
    if not run_id or any(c in run_id for c in r'\/:*?"<>| '):
        fail("run-id must be a safe slug (no spaces or Windows path characters)")

    posts_src = Path(args.posts)
    comments_src = Path(args.comments)
    if not posts_src.exists():
        fail(f"posts not found: {posts_src}")
    if not comments_src.exists():
        fail(f"comments not found: {comments_src}")

    # Self-map detection (avoid backup/remap of canonical files)
    canon_posts_abs = CANON_POSTS.resolve()
    canon_comments_abs = CANON_COMMENTS.resolve()
    posts_abs = posts_src.resolve()
    comments_abs = comments_src.resolve()
    posts_is_canon = (posts_abs == canon_posts_abs)
    comments_is_canon = (comments_abs == canon_comments_abs)

    RUNS.mkdir(parents=True, exist_ok=True)
    run_dir = RUNS / run_id
    (run_dir / "artifacts").mkdir(parents=True, exist_ok=True)
    (run_dir / "published_docs").mkdir(parents=True, exist_ok=True)

    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    backup_dir = run_dir / "_canon_backup" / stamp

    # Backup only if we're going to remap
    b_posts = None if posts_is_canon else backup_if_exists(CANON_POSTS, backup_dir)
    b_comments = None if comments_is_canon else backup_if_exists(CANON_COMMENTS, backup_dir)

    # Track whether we created canonical files (so we can remove only our own temp files)
    created_posts = False
    created_comments = False

    try:
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


        # Phase 1 compatibility: provide legacy filenames for existing scripts.
        legacy_posts_created = False
        legacy_comments_created = False

        if not LEGACY_POSTS.exists():
            shutil.copy2(CANON_POSTS, LEGACY_POSTS)
            legacy_posts_created = True
            print(f"[OK] created legacy alias: {LEGACY_POSTS} -> {CANON_POSTS}")

        if not LEGACY_COMMENTS.exists():
            shutil.copy2(CANON_COMMENTS, LEGACY_COMMENTS)
            legacy_comments_created = True
            print(f"[OK] created legacy alias: {LEGACY_COMMENTS} -> {CANON_COMMENTS}")

        # Run the existing pipeline steps (edit only here)
        run([sys.executable, "pipeline/step2_build_timeline.py"])
        run([sys.executable, "pipeline/step3_analyze_reason.py"])
        run([sys.executable, "tools/context_enrich.py"])
        run([sys.executable, "tools/data_quality_metrics.py"])
        run([sys.executable, "tools/behavioral_metrics_v0_3.py"])

        # Signals: run each spec deterministically
        signals_specs_dir = Path("signals")
        if not signals_specs_dir.exists():
            fail("missing signals/ directory")

        signal_input = FB_OUT / "sean_context_enriched.jsonl"
        if not signal_input.exists():
            fail(f"missing expected signal input: {signal_input}")

        out_dir = FB_OUT / "signals"
        out_dir.mkdir(parents=True, exist_ok=True)

        specs = sorted(signals_specs_dir.glob("*.json"))
        if not specs:
            fail("no signal specs found in signals/*.json")

        for spec in specs:
            run([sys.executable, "tools/run_signal.py", str(spec), str(signal_input), str(out_dir)])

        run([sys.executable, "tools/build_signals_index.py"])
        run([sys.executable, "tools/generate_signals_page.py"])

        # Pages + nav patchers
        run([sys.executable, "tools/generate_topic_category_pages.py"])
        run([sys.executable, "tools/generate_conclusion_page.py"])
        run([sys.executable, "tools/patch_global_nav.py"])
        run([sys.executable, "tools/patch_topic_nav.py"])
        # Emit required run manifest AFTER pipeline steps, BEFORE contracts/cleanup (Phase 1)
        run_dir = Path('fb_extract_out') / 'runs' / args.run_id
        _emit_run_manifest(
            args.run_id,
            args.subject_label,
            Path(args.posts),
            Path(args.comments),
            run_dir=run_dir,
        )
        print('[OK] wrote run_manifest: fb_extract_out\\run_manifest.json')


        # Contracts
        run([sys.executable, "tools/contracts_all.py"])

        # Snapshot key artifacts into run_dir
        timeline = FB_OUT / "sean_timeline.json"
        topics = FB_OUT / "sean_topics.csv"
        enriched = FB_OUT / "sean_context_enriched.jsonl"
        signals_out_dir = FB_OUT / "signals"

        for p in [timeline, topics, enriched]:
            if not p.exists():
                fail(f"missing expected artifact: {p}")

        shutil.copy2(timeline, run_dir / "artifacts" / timeline.name)
        shutil.copy2(topics, run_dir / "artifacts" / topics.name)
        shutil.copy2(enriched, run_dir / "artifacts" / enriched.name)

        if not signals_out_dir.exists():
            fail("missing fb_extract_out/signals")
        copy_tree(signals_out_dir, run_dir / "artifacts" / "signals")

        docs_dir = Path("docs")
        if not docs_dir.exists():
            fail("missing docs/")
        copy_tree(docs_dir, run_dir / "published_docs" / "docs")

        # Build run manifest
        manifest = {
            "run_id": run_id,
            "subject_label": args.subject_label,
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "inputs": {
                "posts_path": str(posts_src).replace("\\", "/"),
                "comments_path": str(comments_src).replace("\\", "/"),
                "posts_sha256": sha256_file(posts_src),
                "comments_sha256": sha256_file(comments_src),
            },
            "counts": {"posts": 0, "comments": 0, "timeline_rows": 0},
            "fingerprints": {},
            "artifacts": {
                "run_dir": str(run_dir).replace("\\", "/"),
                "timeline_json": str((run_dir / "artifacts" / timeline.name)).replace("\\", "/"),
                "topics_csv": str((run_dir / "artifacts" / topics.name)).replace("\\", "/"),
                "context_enriched_jsonl": str((run_dir / "artifacts" / enriched.name)).replace("\\", "/"),
                "signals_dir": str((run_dir / "artifacts" / "signals")).replace("\\", "/"),
                "docs_dir_snapshot": str((run_dir / "published_docs" / "docs")).replace("\\", "/"),
            },
        }

        # counts from outputs
        try:
            data = json.loads(timeline.read_text(encoding="utf-8"))
            if isinstance(data, list):
                manifest["counts"]["timeline_rows"] = len(data)
        except Exception:
            pass

        try:
            posts_n = 0
            comments_n = 0
            for line in enriched.read_text(encoding="utf-8").splitlines():
                if not line.strip():
                    continue
                obj = json.loads(line)
                t = obj.get("joined_item_type") or obj.get("item_type")
                if t == "post":
                    posts_n += 1
                elif t == "comment":
                    comments_n += 1
            manifest["counts"]["posts"] = posts_n
            manifest["counts"]["comments"] = comments_n
        except Exception:
            pass

        (run_dir / "run_manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
        (FB_OUT / "run_manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")

        print(f"[OK] run complete: {run_id}")
        print(f"[OK] wrote: {run_dir / 'run_manifest.json'}")
        print(f"[OK] wrote: {FB_OUT / 'run_manifest.json'}")

    finally:
        # Restore backups if they existed
        restore_backup(b_posts, CANON_POSTS)
        restore_backup(b_comments, CANON_COMMENTS)


        # Remove legacy aliases created during this run
        try:
            if "legacy_posts_created" in locals() and legacy_posts_created and LEGACY_POSTS.exists():
                LEGACY_POSTS.unlink()
                print(f"[OK] removed legacy alias: {LEGACY_POSTS}")
            if "legacy_comments_created" in locals() and legacy_comments_created and LEGACY_COMMENTS.exists():
                LEGACY_COMMENTS.unlink()
                print(f"[OK] removed legacy alias: {LEGACY_COMMENTS}")
        except Exception:
            pass

        # Remove only temporary canon files we created ourselves (and only if we weren't self-mapped)
        if (not posts_is_canon) and created_posts and CANON_POSTS.exists():
            CANON_POSTS.unlink()
            print(f"[OK] removed temporary {CANON_POSTS}")

        if (not comments_is_canon) and created_comments and CANON_COMMENTS.exists():
            CANON_COMMENTS.unlink()
            print(f"[OK] removed temporary {CANON_COMMENTS}")


if __name__ == "__main__":
    main()
