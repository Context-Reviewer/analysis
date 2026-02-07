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

# Current canonical filenames your pipeline expects today.
CANON_POSTS = FB_OUT / "posts_normalized_sean.jsonl"
CANON_COMMENTS = FB_OUT / "comments_normalized_sean.jsonl"


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

    # If the provided inputs are already the canonical files, do NOT back them up or remap.
    # This avoids a self-copy edge case on Windows and preserves deterministic behavior.
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

    b_posts = None if posts_is_canon else backup_if_exists(CANON_POSTS, backup_dir)
    b_comments = None if comments_is_canon else backup_if_exists(CANON_COMMENTS, backup_dir)

    try:
        if not posts_is_canon:
            shutil.copy2(posts_src, CANON_POSTS)
            print(f"[OK] mapped posts -> {CANON_POSTS}")
        else:
            print(f"[OK] posts already canonical: {CANON_POSTS}")

        if not comments_is_canon:
            shutil.copy2(comments_src, CANON_COMMENTS)
            print(f"[OK] mapped comments -> {CANON_COMMENTS}")
        else:
            print(f"[OK] comments already canonical: {CANON_COMMENTS}")

        run([sys.executable, "pipeline/step2_build_timeline.py"])
        run([sys.executable, "pipeline/step3_analyze_reason.py"])
        run([sys.executable, "tools/context_enrich.py"])
        run([sys.executable, "tools/data_quality_metrics.py"])
        run([sys.executable, "tools/behavioral_metrics_v0_3.py"])
        run([sys.executable, "tools/run_signal.py"])
        run([sys.executable, "tools/build_signals_index.py"])
        run([sys.executable, "tools/generate_signals_page.py"])
        run([sys.executable, "tools/generate_topic_category_pages.py"])
        run([sys.executable, "tools/generate_conclusion_page.py"])
        run([sys.executable, "tools/patch_global_nav.py"])
        run([sys.executable, "tools/patch_topic_nav.py"])
        run([sys.executable, "tools/contracts_all.py"])

        timeline = FB_OUT / "sean_timeline.json"
        topics = FB_OUT / "sean_topics.csv"
        enriched = FB_OUT / "sean_context_enriched.jsonl"
        signals_dir = FB_OUT / "signals"

        for p in [timeline, topics, enriched]:
            if not p.exists():
                fail(f"missing expected artifact: {p}")

        shutil.copy2(timeline, run_dir / "artifacts" / timeline.name)
        shutil.copy2(topics, run_dir / "artifacts" / topics.name)
        shutil.copy2(enriched, run_dir / "artifacts" / enriched.name)

        if not signals_dir.exists():
            fail("missing fb_extract_out/signals")
        copy_tree(signals_dir, run_dir / "artifacts" / "signals")

        docs_dir = Path("docs")
        if not docs_dir.exists():
            fail("missing docs/")
        copy_tree(docs_dir, run_dir / "published_docs" / "docs")

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
        if b_posts is None and CANON_POSTS.exists():
            CANON_POSTS.unlink()
            print(f"[OK] removed mapped {CANON_POSTS}")
        else:
            restore_backup(b_posts, CANON_POSTS)

        if b_comments is None and CANON_COMMENTS.exists():
            CANON_COMMENTS.unlink()
            print(f"[OK] removed mapped {CANON_COMMENTS}")
        else:
            restore_backup(b_comments, CANON_COMMENTS)


if __name__ == "__main__":
    main()
