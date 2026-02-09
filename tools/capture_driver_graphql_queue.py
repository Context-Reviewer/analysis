import argparse
import json
import pathlib
import subprocess
from typing import Dict, Any

ROOT = pathlib.Path(__file__).resolve().parents[1]
QUEUE = ROOT / "fb_extract_out" / "missing_capture_targets_unique_urls.jsonl"
OUT = ROOT / "fb_extract_out" / "netlog_queue_urls"


def run(cmd):
    print("[run]", " ".join(cmd))
    subprocess.run(cmd, check=True)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--headless", type=int, default=1)
    ap.add_argument("--force", type=int, default=0)
    ap.add_argument("--max-rounds", type=int, default=60)
    ap.add_argument("--stable-rounds", type=int, default=3)
    ap.add_argument("--sleep-ms", type=int, default=250)
    ap.add_argument("--scroll-steps", type=int, default=3)
    ap.add_argument("--max-clicks", type=int, default=80)

    # Optional: rebuild a specific target after canonical rebuild
    ap.add_argument("--target-name", default="")
    ap.add_argument("--target-key", default="")
    ap.add_argument("--target-author-id", default="")
    args = ap.parse_args()

    if not QUEUE.exists():
        raise SystemExit(f"Missing queue file: {QUEUE}")

    OUT.mkdir(parents=True, exist_ok=True)

    lines = [ln for ln in QUEUE.read_text(encoding="utf-8").splitlines() if ln.strip()]
    targets = [json.loads(ln) for ln in lines]

    total = len(targets)
    done = 0
    skipped = 0

    for i, t in enumerate(targets, start=1):
        url = t.get("url")
        post_id = t.get("post_id")

        if not url:
            continue
        if not post_id:
            # Fallback: deterministic id from url hash if post_id missing
            post_id = "url_" + str(abs(hash(url)))  # stable enough for local dir naming

        run_dir = OUT / f"post_{post_id}"
        run_dir.mkdir(parents=True, exist_ok=True)

        run_json = run_dir / "run.json"
        if run_json.exists() and not args.force:
            skipped += 1
            continue

        print(f"[capture] ({i}/{total}) post_id={post_id} url={url}")

        cmd = [
            "py", "tools/capture_fb_graphql.py",
            "--url", url,
            "--out", str(run_dir),
            "--headless", str(args.headless),
            "--max-rounds", str(args.max_rounds),
            "--stable-rounds", str(args.stable_rounds),
            "--sleep-ms", str(args.sleep_ms),
            "--scroll-steps", str(args.scroll_steps),
            "--max-clicks", str(args.max_clicks),
        ]
        run(cmd)
        done += 1

    # Canonical rebuild + contracts gate
    run(["py", "tools/build_comments_graphql_v2.py"])
    run(["py", "tools/contracts_all.py"])

    # Optional: rebuild a target slice (person-agnostic)
    if args.target_name.strip() and args.target_key.strip():
        if args.target_author_id.strip():
            run([
                "py", "tools/build_target_context_from_v2.py",
                "--target-name", args.target_name,
                "--target-key", args.target_key,
                "--target-author-id", args.target_author_id,
            ])
        else:
            run([
                "py", "tools/build_target_context_from_v2.py",
                "--target-name", args.target_name,
                "--target-key", args.target_key,
            ])
        run(["py", "tools/build_target_timeline.py", "--target-key", args.target_key])

    print(f"[queue] complete: done={done} skipped={skipped} total={total}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
