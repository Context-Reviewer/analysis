import json
import pathlib
import subprocess

ROOT = pathlib.Path(__file__).resolve().parents[1]
QUEUE = ROOT / "fb_extract_out" / "missing_capture_targets_unique_urls.jsonl"
OUT = ROOT / "fb_extract_out" / "netlog_queue_urls"

for line in QUEUE.read_text().splitlines():
    target = json.loads(line)
    url = target["url"]
    post_id = target["post_id"]

    run_dir = OUT / f"post_{post_id}"
    run_dir.mkdir(parents=True, exist_ok=True)

    run_json = run_dir / "run.json"
    if run_json.exists():
        continue

    subprocess.run([
        "py", "tools/capture_single_post_graphql.py",
        "--url", url,
        "--out", str(run_dir)
    ], check=True)

    run_json.write_text(json.dumps({
        "post_id": post_id,
        "url": url,
        "source": "graphql",
    }, indent=2))

subprocess.run(["py", "tools/build_comments_graphql_v2.py"], check=True)
subprocess.run(["py", "tools/rebuild_sean_context_enriched_from_v2.py"], check=True)
subprocess.run(["py", "tools/build_sean_timeline_from_enriched.py"], check=True)
subprocess.run(["py", "tools/contracts_all.py"], check=True)

print("[capture] batch complete")
