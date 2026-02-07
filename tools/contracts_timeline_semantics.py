from __future__ import annotations

import json
from pathlib import Path

TIMELINE = Path("fb_extract_out/sean_timeline.json")

def fail(msg: str) -> None:
    raise SystemExit(f"[FAIL] {msg}")

def main() -> None:
    if not TIMELINE.exists():
        fail(f"Missing timeline file: {TIMELINE}")

    data = json.loads(TIMELINE.read_text(encoding="utf-8"))
    if not isinstance(data, list):
        fail(f"Timeline must be a JSON list, got: {type(data)}")

    allowed = {"post", "comment"}
    bad_type = 0
    missing_type = 0
    missing_source_index = 0
    missing_parent_context_for_comment = 0

    posts = 0
    comments = 0

    for i, it in enumerate(data, start=1):
        if not isinstance(it, dict):
            fail(f"Timeline row {i} is not an object")

        item_type = it.get("item_type")
        if item_type is None:
            missing_type += 1
        elif item_type not in allowed:
            bad_type += 1
        else:
            if item_type == "post":
                posts += 1
            else:
                comments += 1
                pc = (it.get("parent_context") or "").strip()
                if pc == "":
                    missing_parent_context_for_comment += 1

        if it.get("source_index") is None:
            missing_source_index += 1

    if missing_type:
        fail(f"Missing item_type on {missing_type} row(s)")
    if bad_type:
        fail(f"Invalid item_type on {bad_type} row(s) (allowed: {sorted(allowed)})")
    if missing_source_index:
        fail(f"Missing source_index on {missing_source_index} row(s)")
    if missing_parent_context_for_comment:
        fail(f"Missing parent_context on {missing_parent_context_for_comment} comment row(s)")

    print("[OK] timeline semantics")
    print(f"  rows: {len(data)}")
    print(f"  posts: {posts}")
    print(f"  comments: {comments}")

if __name__ == "__main__":
    main()
