#!/usr/bin/env python3
"""
Build an authoritative comment dataset from GraphQL netlog capture dirs.

Reads:
  fb_extract_out/netlog_queue_urls/**/run.json
  fb_extract_out/netlog_queue_urls/**/*.json  (payloads)

Writes:
  fb_extract_out/comments_graphql_v2.jsonl              (all comment objects)
  fb_extract_out/comments_graphql_v2_sean_roy.jsonl     (filtered author name match)
  fb_extract_out/comments_graphql_v2_summary.json       (stats)
  fb_extract_out/comments_graphql_v2_dupe_report.jsonl  (only if collisions)

Notes:
- This is SOURCE-OF-TRUTH for timestamps & IDs.
- Deterministic ordering (post_id, legacy_fbid/gql_id, timestamp).
"""

from __future__ import annotations

import argparse
import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple


POST_ID_RE = re.compile(r"/(posts|permalink)/(\d{10,})", re.IGNORECASE)


def iso_utc_from_epoch_seconds(sec: int) -> str:
    dt = datetime.fromtimestamp(int(sec), tz=timezone.utc)
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def parse_json(p: Path) -> Optional[Any]:
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return None


def walk(obj: Any) -> Iterable[Any]:
    stack = [obj]
    while stack:
        cur = stack.pop()
        yield cur
        if isinstance(cur, dict):
            for v in cur.values():
                if isinstance(v, (dict, list)):
                    stack.append(v)
        elif isinstance(cur, list):
            for v in cur:
                if isinstance(v, (dict, list)):
                    stack.append(v)


def extract_post_id(url: str) -> Optional[str]:
    m = POST_ID_RE.search(url or "")
    return m.group(2) if m else None


def get_comment_body(obj: dict) -> Optional[str]:
    # prefer body.text
    b = obj.get("body")
    if isinstance(b, dict):
        t = b.get("text")
        if isinstance(t, str) and t.strip():
            return t
    # fallback: message.text
    m = obj.get("message")
    if isinstance(m, dict):
        t = m.get("text")
        if isinstance(t, str) and t.strip():
            return t
    # fallback: text
    t2 = obj.get("text")
    if isinstance(t2, str) and t2.strip():
        return t2
    return None


def get_author(obj: dict) -> Tuple[Optional[str], Optional[str]]:
    a = obj.get("author")
    if isinstance(a, dict):
        name = a.get("name")
        aid = a.get("id")
        if isinstance(name, str) and name.strip():
            return name.strip(), (str(aid) if aid is not None else None)
    return None, None


def stable_key(post_id: str, legacy_fbid: Optional[str], gql_id: Optional[str], created_time: Optional[int], author: str, body: str) -> Tuple:
    # deterministic sort and dedupe preference:
    # legacy_fbid first, else gql_id
    return (
        post_id or "",
        str(legacy_fbid or ""),
        str(gql_id or ""),
        int(created_time or 0),
        author.lower(),
        body[:64].lower(),
    )


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", default="fb_extract_out/netlog_queue_urls")
    ap.add_argument("--out", default="fb_extract_out/comments_graphql_v2.jsonl")
    ap.add_argument("--out-sean", default="fb_extract_out/comments_graphql_v2_sean_roy.jsonl")
    ap.add_argument("--summary", default="fb_extract_out/comments_graphql_v2_summary.json")
    ap.add_argument("--dupes", default="fb_extract_out/comments_graphql_v2_dupe_report.jsonl")
    ap.add_argument("--sean-name", default="Sean Roy")
    args = ap.parse_args()

    root = Path(args.root)
    if not root.exists():
        print(f"[ERR] missing root dir: {root}")
        return 2

    cap_dirs = sorted([p for p in root.glob("**/*") if p.is_dir() and (p / "run.json").exists()])

    rows: List[dict] = []
    seen: Dict[Tuple[str, str], dict] = {}  # (post_id, legacy_fbid) -> row
    seen_gql: Dict[Tuple[str, str], dict] = {}  # (post_id, gql_id) -> row
    dupes_out: List[dict] = []

    stats = {
        "capture_dirs": len(cap_dirs),
        "runjson_loaded": 0,
        "runjson_failed": 0,
        "payload_files_seen": 0,
        "payload_json_loaded": 0,
        "payload_json_failed": 0,
        "comment_nodes_seen": 0,
        "comment_nodes_with_time": 0,
        "comment_nodes_with_body": 0,
        "comment_rows_emitted": 0,
        "unique_by_legacy": 0,
        "unique_by_gql": 0,
        "posts_covered": 0,
        "sean_rows": 0,
    }

    post_ids_seen = set()

    for d in cap_dirs:
        runp = d / "run.json"
        runj = parse_json(runp)
        if not isinstance(runj, dict):
            stats["runjson_failed"] += 1
            continue
        stats["runjson_loaded"] += 1

        url = runj.get("url")
        if not isinstance(url, str):
            continue
        post_id = extract_post_id(url)
        if not post_id:
            continue
        post_ids_seen.add(post_id)

        payload_files = sorted([p for p in d.glob("*.json") if p.name not in ("run.json", "metrics.json")])
        stats["payload_files_seen"] += len(payload_files)

        for jp in payload_files:
            data = parse_json(jp)
            if data is None:
                stats["payload_json_failed"] += 1
                continue
            stats["payload_json_loaded"] += 1

            for obj in walk(data):
                if not isinstance(obj, dict):
                    continue
                if obj.get("__typename") != "Comment":
                    continue

                stats["comment_nodes_seen"] += 1

                ct = obj.get("created_time")
                try:
                    ct_int = int(ct) if ct is not None else None
                except Exception:
                    ct_int = None

                if ct_int is not None:
                    stats["comment_nodes_with_time"] += 1

                author_name, author_id = get_author(obj)
                body = get_comment_body(obj)

                if not body:
                    continue
                stats["comment_nodes_with_body"] += 1

                legacy_fbid = obj.get("legacy_fbid")
                gql_id = obj.get("id")
                legacy_fbid_s = str(legacy_fbid) if legacy_fbid is not None else None
                gql_id_s = str(gql_id) if gql_id is not None else None

                row = {
                    "item_type": "comment_v2",
                    "post_id": post_id,
                    "legacy_fbid": legacy_fbid_s,
                    "gql_id": gql_id_s,
                    "created_time": ct_int,
                    "timestamp_parsed": (iso_utc_from_epoch_seconds(ct_int) if ct_int is not None else None),
                    "author_name": author_name,
                    "author_id": author_id,
                    "body_text": body,
                    "capture_dir": str(d).replace("\\", "/"),
                    "source_file": jp.name,
                }

                # Deduplicate deterministically:
                # Prefer legacy_fbid key, else gql_id. If collision with differing timestamps/body, record dupes.
                if legacy_fbid_s:
                    k = (post_id, legacy_fbid_s)
                    if k in seen:
                        prev = seen[k]
                        if (prev.get("timestamp_parsed") != row.get("timestamp_parsed")) or (prev.get("body_text") != row.get("body_text")):
                            dupes_out.append({"key": {"post_id": post_id, "legacy_fbid": legacy_fbid_s}, "a": prev, "b": row})
                        # keep first (stable: first seen via sorted dirs/files)
                        continue
                    seen[k] = row
                    stats["unique_by_legacy"] += 1
                elif gql_id_s:
                    k = (post_id, gql_id_s)
                    if k in seen_gql:
                        prev = seen_gql[k]
                        if (prev.get("timestamp_parsed") != row.get("timestamp_parsed")) or (prev.get("body_text") != row.get("body_text")):
                            dupes_out.append({"key": {"post_id": post_id, "gql_id": gql_id_s}, "a": prev, "b": row})
                        continue
                    seen_gql[k] = row
                    stats["unique_by_gql"] += 1
                else:
                    # no IDs at all: keep it, but it’s rare; allow duplicates by body
                    pass

                rows.append(row)

    stats["posts_covered"] = len(post_ids_seen)
    stats["comment_rows_emitted"] = len(rows)

    # stable sort for determinism
    def sort_key(r: dict) -> Tuple:
        return stable_key(
            r.get("post_id") or "",
            r.get("legacy_fbid"),
            r.get("gql_id"),
            r.get("created_time"),
            r.get("author_name") or "",
            r.get("body_text") or "",
        )

    rows.sort(key=sort_key)

    outp = Path(args.out)
    outp.parent.mkdir(parents=True, exist_ok=True)
    with outp.open("w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")

    # Sean-only
    sean = (args.sean_name or "").strip().lower()
    out_sean = Path(args.out_sean)
    with out_sean.open("w", encoding="utf-8") as f:
        for r in rows:
            if isinstance(r.get("author_name"), str) and r["author_name"].strip().lower() == sean:
                stats["sean_rows"] += 1
                f.write(json.dumps(r, ensure_ascii=False) + "\n")

    # dupes report
    dupes_path = Path(args.dupes)
    if dupes_out:
        with dupes_path.open("w", encoding="utf-8") as f:
            for d in dupes_out:
                f.write(json.dumps(d, ensure_ascii=False) + "\n")
    else:
        # remove stale dupes file if exists
        if dupes_path.exists():
            dupes_path.unlink()

    # summary json
    sump = Path(args.summary)
    sump.write_text(json.dumps(stats, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"[OK] wrote: {outp}")
    print(f"[OK] wrote: {out_sean}")
    print(f"[OK] wrote: {sump}")
    if dupes_out:
        print(f"[WARN] dupes written: {dupes_path} count={len(dupes_out)}")
    print("[V2 SUMMARY]")
    for k in sorted(stats.keys()):
        print(f"  {k}: {stats[k]}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
