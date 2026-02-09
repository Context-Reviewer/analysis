import json
import pathlib
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Tuple


ROOT = pathlib.Path(__file__).resolve().parents[1]
NETLOG_ROOT = ROOT / "fb_extract_out" / "netlog_queue_urls"
OUT_JSONL = ROOT / "fb_extract_out" / "comments_graphql_v2.jsonl"
OUT_SUMMARY = ROOT / "fb_extract_out" / "comments_graphql_v2_summary.json"


def _iso_from_unix(ts: Any) -> Optional[str]:
    try:
        t = int(ts)
        return datetime.fromtimestamp(t, tz=timezone.utc).isoformat()
    except Exception:
        return None


def _iter_capture_dirs() -> List[pathlib.Path]:
    if not NETLOG_ROOT.exists():
        return []
    # Accept both historical and new capture layouts.
    # A capture dir is any directory under NETLOG_ROOT that either:
    #   - contains gql/*.json (new layout)
    #   - contains *.json at root (old layout)
    dirs: List[pathlib.Path] = []
    for p in sorted(NETLOG_ROOT.iterdir()):
        if not p.is_dir():
            continue
        has_new = (p / "gql").exists()
        has_old = any(p.glob("*.json"))
        if has_new or has_old:
            dirs.append(p)
    return dirs


def _iter_payload_files(capdir: pathlib.Path) -> Iterable[pathlib.Path]:
    gql_dir = capdir / "gql"
    if gql_dir.exists():
        yield from sorted(gql_dir.glob("*.json"))
    else:
        yield from sorted(capdir.glob("*.json"))


def _load_json(path: pathlib.Path) -> Optional[Dict[str, Any]]:
    try:
        # Always utf-8; FB payloads are utf-8 JSON.
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def _walk(obj: Any) -> Iterable[Any]:
    # Generic JSON walker
    if isinstance(obj, dict):
        yield obj
        for v in obj.values():
            yield from _walk(v)
    elif isinstance(obj, list):
        for it in obj:
            yield from _walk(it)


def _extract_comment_nodes(payload: Dict[str, Any]) -> Iterable[Dict[str, Any]]:
    # Facebook GraphQL shapes vary; we treat any dict with legacy_fbid + created_time/body
    # as a comment-like node, then normalize.
    for node in _walk(payload):
        if not isinstance(node, dict):
            continue
        if "legacy_fbid" not in node:
            continue
        yield node


def _get_body_text(node: Dict[str, Any]) -> Optional[str]:
    body = node.get("body")
    if isinstance(body, dict):
        txt = body.get("text")
        if isinstance(txt, str) and txt.strip():
            return txt
    # Sometimes "message" fields exist
    msg = node.get("message")
    if isinstance(msg, dict):
        txt = msg.get("text")
        if isinstance(txt, str) and txt.strip():
            return txt
    return None


def _get_author_name(node: Dict[str, Any]) -> Optional[str]:
    author = node.get("author")
    if isinstance(author, dict):
        nm = author.get("name")
        if isinstance(nm, str) and nm.strip():
            return nm.strip()
    # Some shapes embed "commenter"
    commenter = node.get("commenter")
    if isinstance(commenter, dict):
        nm = commenter.get("name")
        if isinstance(nm, str) and nm.strip():
            return nm.strip()
    return None


def _infer_post_id(payload: Dict[str, Any], fallback: Optional[str]) -> Optional[str]:
    # Prefer explicit post_id in run.json, else try to find a plausible id field
    if isinstance(fallback, str) and fallback.strip():
        return fallback.strip()
    for node in _walk(payload):
        if not isinstance(node, dict):
            continue
        # Some payloads include "post_id" explicitly
        pid = node.get("post_id")
        if isinstance(pid, (int, str)):
            s = str(pid).strip()
            if s.isdigit():
                return s
    return None


def main() -> int:
    capture_dirs = _iter_capture_dirs()

    payload_files_seen = 0
    comment_nodes_seen = 0
    comment_nodes_with_time = 0
    comment_nodes_with_body = 0

    rows: List[Dict[str, Any]] = []
    seen_legacy: set[str] = set()
    seen_gql: set[str] = set()

    post_ids: List[str] = []
    urls_covered: List[str] = []

    for capdir in capture_dirs:
        # load run.json if present (helps post_id + url)
        run_json = capdir / "run.json"
        run_meta: Dict[str, Any] = {}
        if run_json.exists():
            try:
                run_meta = json.loads(run_json.read_text(encoding="utf-8"))
            except Exception:
                run_meta = {}

        fallback_post_id = None
        if isinstance(run_meta.get("post_id"), (int, str)):
            fallback_post_id = str(run_meta.get("post_id"))
        fallback_url = run_meta.get("url") if isinstance(run_meta.get("url"), str) else None

        cap_post_id = None

        for jf in _iter_payload_files(capdir):
            payload_files_seen += 1
            payload = _load_json(jf)
            if not payload:
                continue

            if cap_post_id is None:
                cap_post_id = _infer_post_id(payload, fallback_post_id)

            for node in _extract_comment_nodes(payload):
                comment_nodes_seen += 1

                legacy = node.get("legacy_fbid")
                if not isinstance(legacy, (int, str)):
                    continue
                legacy_s = str(legacy).strip()
                if not legacy_s.isdigit():
                    continue

                created_iso = _iso_from_unix(node.get("created_time"))
                if created_iso:
                    comment_nodes_with_time += 1

                body_text = _get_body_text(node)
                if body_text:
                    comment_nodes_with_body += 1

                # Canonical row requires both time and body
                if not created_iso or not body_text:
                    continue

                if legacy_s in seen_legacy:
                    continue
                seen_legacy.add(legacy_s)

                gql_id = node.get("id")
                gql_s = str(gql_id).strip() if isinstance(gql_id, (int, str)) else ""
                if gql_s:
                    seen_gql.add(gql_s)

                author_name = _get_author_name(node) or ""

                row = {
                    "source": "graphql_v2",
                    "legacy_fbid": legacy_s,
                    "id": gql_s,
                    "post_id": cap_post_id or fallback_post_id or "",
                    "author": {"name": author_name},
                    "body": {"text": body_text},
                    "created_time": created_iso,
                }
                rows.append(row)

        # Accounting for covered posts/urls
        if cap_post_id:
            post_ids.append(cap_post_id)
        elif fallback_post_id:
            post_ids.append(str(fallback_post_id))
        if fallback_url:
            urls_covered.append(fallback_url)

    # Write JSONL deterministically by legacy_fbid
    rows.sort(key=lambda r: int(r["legacy_fbid"]))

    OUT_JSONL.parent.mkdir(parents=True, exist_ok=True)
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
        "unique_by_legacy": len(seen_legacy),
        "unique_by_gql": len(seen_gql),
        "posts_covered": len(sorted(set([p for p in post_ids if p]))),
        "post_ids": sorted(set([p for p in post_ids if p])),
        "urls_covered": sorted(set([u for u in urls_covered if u])),
    }

    OUT_SUMMARY.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print(f"[v2] build complete: {len(rows)} rows")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
