import argparse
import json
import pathlib
import subprocess
import re
from typing import Any, Dict, Optional, Tuple

ROOT = pathlib.Path(__file__).resolve().parents[1]
QUEUE = ROOT / "fb_extract_out" / "missing_capture_targets_unique_urls.jsonl"
OUT = ROOT / "fb_extract_out" / "netlog_queue_urls"


def run(cmd):
    print("[run]", " ".join(cmd), flush=True)
    subprocess.run(cmd, check=True)


def infer_post_id_from_url(url: str) -> Optional[str]:
    m = re.search(r"/posts/(\d+)", url)
    if m:
        return m.group(1)
    m = re.search(r"/permalink/(\d+)", url)
    if m:
        return m.group(1)
    return None


def pick_url_and_post_id(t: Dict[str, Any]) -> Tuple[Optional[str], Optional[str]]:
    # Wide net: queue generators drift; accept multiple shapes.
    url = (
        t.get("url")
        or t.get("post_url")
        or t.get("link")
        or t.get("permalink")
        or t.get("target_url")
        or t.get("suggested_open_url")
        or (t.get("context") or {}).get("url")
        or (t.get("target") or {}).get("url")
        or (t.get("post") or {}).get("url")
    )

    post_id = (
        t.get("post_id")
        or t.get("legacy_fbid")
        or t.get("id")
        or (t.get("context") or {}).get("post_id")
        or (t.get("target") or {}).get("post_id")
        or (t.get("post") or {}).get("id")
    )

    if isinstance(url, str):
        url = url.strip() or None
    else:
        url = None

    if isinstance(post_id, (int, str)):
        post_id = str(post_id).strip() or None
    else:
        post_id = None

    if url and not post_id:
        post_id = infer_post_id_from_url(url)

    return url, post_id


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--headless", type=int, default=1)
    ap.add_argument("--force", type=int, default=0)
    ap.add_argument("--profile", default="", help="Persistent Playwright profile dir (Windows path recommended)")
    ap.add_argument("--target-name", default="")
    ap.add_argument("--target-key", default="")
    ap.add_argument("--max-rounds", type=int, default=30)
    ap.add_argument("--stable-rounds", type=int, default=3)
    ap.add_argument("--sleep-ms", type=int, default=250)
    ap.add_argument("--scroll-steps", type=int, default=3)
    ap.add_argument("--max-clicks", type=int, default=80)
    args = ap.parse_args()

    if not QUEUE.exists():
        print(f"[queue] missing {QUEUE}")
        return 0

    OUT.mkdir(parents=True, exist_ok=True)

    raw_lines = [ln for ln in QUEUE.read_text(encoding="utf-8").splitlines() if ln.strip()]
    items = [json.loads(l) for l in raw_lines]

    done = 0
    skipped_existing = 0
    skipped_invalid = 0
    total = len(items)

    debug = {
        "queue_file": str(QUEUE),
        "raw_lines": len(raw_lines),
        "parsed_targets": total,
        "skip_reasons": {},
        "skipped_examples": [],
    }

    def note_skip(reason: str, item: Dict[str, Any]):
        debug["skip_reasons"][reason] = debug["skip_reasons"].get(reason, 0) + 1
        if len(debug["skipped_examples"]) < 8:
            debug["skipped_examples"].append({"reason": reason, "keys": sorted(list(item.keys()))})

    for i, t in enumerate(items, 1):
        url, post_id = pick_url_and_post_id(t)

        if not url:
            skipped_invalid += 1
            note_skip("missing_url", t)
            continue

        if not post_id:
            skipped_invalid += 1
            note_skip("missing_post_id", t)
            continue

        run_dir = OUT / f"post_{post_id}"
        if run_dir.exists() and not args.force:
            skipped_existing += 1
            note_skip("already_captured", t)
            continue

        print(f"[capture] ({i}/{total}) post_id={post_id} url={url}", flush=True)

        cmd = [
            "py", "tools/capture_fb_graphql.py",
            "--url", url,
            "--out", str(run_dir),
            "--headless", str(args.headless),
            "--max-rounds", str(args.max_rounds),
            "--stable-rounds", str(args.stable_rounds),
            "--min-rounds", "2",
            "--max-seconds", "120",
            "--sleep-ms", str(args.sleep_ms),
            "--scroll-steps", str(args.scroll_steps),
            "--max-clicks", str(args.max_clicks),
        ]

        if args.profile:
            cmd += ["--profile", args.profile]

        run(cmd)
        done += 1

    dbg_path = ROOT / "fb_extract_out" / "queue_debug.json"
    dbg_path.write_text(json.dumps(debug, indent=2), encoding="utf-8")
    print(f"[queue] debug: {dbg_path}", flush=True)

    # Rebuild canonical pipeline after capture
    run(["py", "tools/build_comments_graphql_v2.py"])
    run(["py", "tools/contracts_all.py"])

    if args.target_name and args.target_key:
        run([
            "py", "tools/build_target_context_from_v2.py",
            "--target-name", args.target_name,
            "--target-key", args.target_key,
        ])
        run([
            "py", "tools/build_target_timeline.py",
            "--target-key", args.target_key,
        ])

    print(
        f"[queue] complete: done={done} "
        f"skipped_existing={skipped_existing} "
        f"skipped_invalid={skipped_invalid} "
        f"total={total}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
