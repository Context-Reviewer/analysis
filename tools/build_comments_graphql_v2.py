import json
import pathlib
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, Optional

ROOT = pathlib.Path(__file__).resolve().parents[1]
NETLOG_ROOT = ROOT / "fb_extract_out" / "netlog_queue_urls"
OUT_DIR = ROOT / "fb_extract_out"

OUT_JSONL = OUT_DIR / "comments_graphql_v2.jsonl"
OUT_SUMMARY = OUT_DIR / "comments_graphql_v2_summary.json"

def load_json_any_encoding(p: pathlib.Path) -> Optional[Dict[str, Any]]:
    b = p.read_bytes()
    for enc in ("utf-8", "utf-8-sig", "latin-1"):
        try:
            s = b.decode(enc)
            return json.loads(s)
        except Exception:
            continue
    return None

def walk(obj: Any) -> Iterable[Dict[str, Any]]:
    if isinstance(obj, dict):
        yield obj
        for v in obj.values():
            yield from walk(v)
    elif isinstance(obj, list):
        for v in obj:
            yield from walk(v)

def get_body_text(node: Dict[str, Any]) -> Optional[str]:
    body = node.get("body")
    if isinstance(body, dict):
        t = body.get("text")
        if isinstance(t, str) and t.strip():
            return t
    if isinstance(body, str) and body.strip():
        return body
    return None

def get_author_name(node: Dict[str, Any]) -> Optional[str]:
    a = node.get("author")
    if isinstance(a, dict):
        n = a.get("name")
        if isinstance(n, str) and n.strip():
            return n
    return None

def iso_from_created_time(ts: Any) -> Optional[str]:
    if isinstance(ts, int) and ts > 0:
        return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()
    if isinstance(ts, str):
        # Sometimes already ISO
        try:
            datetime.fromisoformat(ts.replace("Z", "+00:00"))
            return ts
        except Exception:
            return None
    return None

rows = []
seen_legacy = set()

capture_root = ROOT / "fb_extract_out" / "netlog_queue_urls"
capture_dirs = sorted([
    p for p in capture_root.iterdir()
    if p.is_dir() and (p / "gql").exists()
])
payload_files_seen = 0
comment_nodes_seen = 0
comment_nodes_with_time = 0
comment_nodes_with_body = 0
posts_covered = set()
urls_covered = set()

for capdir in sorted(capture_dirs):
    run_json = capdir / "run.json"
    post_id = None
    url = None

    if run_json.exists():
        try:
            run_data = json.loads(run_json.read_text(encoding="utf-8"))
            post_id = run_data.get("post_id")
            url = run_data.get("url")
        except Exception:
            pass

    if post_id:
        posts_covered.add(str(post_id))
    if url:
        urls_covered.add(str(url))

    for jf in sorted(capdir.glob("*.json")):
        if jf.name == "run.json":
            continue

        payload = load_json_any_encoding(jf)
        if payload is None:
            continue

        payload_files_seen += 1

        for d in walk(payload):
            # Comment-like node detection
            if "legacy_fbid" not in d:
                continue

            legacy = d.get("legacy_fbid")
            if not isinstance(legacy, str) or not legacy.strip():
                continue

            comment_nodes_seen += 1

            created_time = d.get("created_time")
            created_time_iso = iso_from_created_time(created_time)
            if created_time_iso:
                comment_nodes_with_time += 1

            body_text = get_body_text(d)
            if body_text:
                comment_nodes_with_body += 1

            # Canonical emit rule: require time + body
            if not (created_time_iso and body_text):
                continue

            # Dedup by legacy_fbid (authoritative)
            if legacy in seen_legacy:
                continue
            seen_legacy.add(legacy)

            rows.append({
                "legacy_fbid": legacy,
                "post_id": str(post_id) if post_id else None,
                "author": get_author_name(d),
                "body": {"text": body_text},
                "created_time": created_time,
                "created_time_iso": created_time_iso,
                "source": "graphql_v2",
            })

OUT_DIR.mkdir(parents=True, exist_ok=True)

with OUT_JSONL.open("w", encoding="utf-8") as f:
    for r in rows:
        f.write(json.dumps(r, ensure_ascii=False) + "\n")

summary = {
    "capture_dirs": len(capture_dirs),
    "payload_files_seen": payload_files_seen,
    "comment_nodes_seen": comment_nodes_seen,
    "comment_nodes_with_time": comment_nodes_with_time,
    "comment_nodes_with_body": comment_nodes_with_body,
    "comment_rows_emitted": len(rows),
    "unique_by_legacy": len(rows),
    "unique_by_gql": 0,
    "posts_covered": len(posts_covered),
    "post_ids": sorted(posts_covered),
    "urls_covered": sorted(urls_covered),
}

OUT_SUMMARY.write_text(json.dumps(summary, indent=2), encoding="utf-8")
print("[v2] build complete:", len(rows), "rows")

