# step1b_adapt_comments.py
# Purpose (Step 1b):
# - Read extracted comments from sean_roy_comments.jsonl
# - Transform to normalized_item_v1 schema
# - Output comments_normalized_sean.jsonl
#
# Governance:
# - Schema normalization ONLY
# - No filtering, no ranking, no inference
# - Deterministic: input order preserved
# - Fail-loud on malformed data

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any, Dict, List, Tuple

# --- Paths ---
# Source: Comment extraction output
SRC_COMMENTS = Path(r"C:\Users\lwpar\Desktop\Jules\Projects\Scraper\fb_extract_out\sean_roy_comments.jsonl")

# Output: Normalized comments for pipeline
OUT_DIR = Path("fb_extract_out")
OUT_NORMALIZED = OUT_DIR / "comments_normalized_sean.jsonl"

# Schema version (locked)
SCHEMA_VERSION = "normalized_item_v1"

# Regex for ISO8601-like timestamp (YYYY-MM-DD or YYYY-MM-DDTHH:MM:SS...)
ISO8601_PATTERN = re.compile(r"^\d{4}-\d{2}-\d{2}(T\d{2}:\d{2}:\d{2})?$")


def load_jsonl(path: Path) -> Tuple[List[Dict[str, Any]], int]:
    """
    Load JSONL file, preserving order.
    Returns (rows, bad_line_count).
    Does NOT silently drop lines - caller must handle bad_line_count.
    """
    rows: List[Dict[str, Any]] = []
    bad_lines = 0
    for line_num, line in enumerate(path.read_text(encoding="utf-8", errors="replace").splitlines(), start=1):
        line = line.strip()
        if not line:
            continue
        try:
            obj = json.loads(line)
        except json.JSONDecodeError as e:
            print(f"ERROR: Malformed JSON at line {line_num}: {e}")
            bad_lines += 1
            continue
        if not isinstance(obj, dict):
            print(f"ERROR: Line {line_num} is not a JSON object")
            bad_lines += 1
            continue
        rows.append(obj)
    return rows, bad_lines


def validate_permalink(url: str, source_index: int) -> None:
    """
    Validate permalink per schema contract:
    - Must contain /posts/ or /permalink/
    - Must NOT contain # (fragment identifier)
    - Must be non-empty
    Raises SystemExit on failure.
    """
    if not url:
        raise SystemExit(f"FAIL: source_index={source_index} has empty permalink")
    
    if "#" in url:
        raise SystemExit(f"FAIL: source_index={source_index} has fragment in URL: {url[:100]}")
    
    if "/posts/" not in url and "/permalink/" not in url:
        raise SystemExit(f"FAIL: source_index={source_index} has non-canonical URL: {url[:100]}")


def validate_required_fields(author: str, body: str, source_index: int) -> None:
    """
    Validate required fields per schema contract:
    - author must be non-empty after strip
    - body must be non-empty after strip
    Raises SystemExit on failure.
    """
    if not author:
        raise SystemExit(f"FAIL: source_index={source_index} has empty author")
    
    if not body:
        raise SystemExit(f"FAIL: source_index={source_index} has empty body")


def parse_timestamp(raw_timestamp: Any) -> Tuple[str | None, str | None]:
    """
    Correctly assign timestamp_parsed vs timestamp_raw.
    - timestamp_parsed: ONLY if ISO8601-like format
    - timestamp_raw: original value if not ISO8601
    Returns (timestamp_parsed, timestamp_raw).
    """
    if raw_timestamp is None:
        return None, None
    
    ts_str = str(raw_timestamp).strip()
    if not ts_str:
        return None, None
    
    # Check if ISO8601-like
    if ISO8601_PATTERN.match(ts_str):
        return ts_str, None  # parsed, no raw needed
    else:
        return None, ts_str  # not parsed, keep as raw


def adapt_comment(record: Dict[str, Any], source_index: int) -> Dict[str, Any]:
    """
    Transform a raw comment record to normalized_item_v1 schema.
    
    Source fields:
        - post_url: str
        - author: str
        - comment_text: str
        - timestamp: str | null
        - debug_evidence: dict (ignored)
    
    Target schema (normalized_item_v1):
        - schema_version: "normalized_item_v1" (REQUIRED)
        - item_type: "comment" (REQUIRED)
        - body: str (REQUIRED, non-null, non-empty)
        - author: str (REQUIRED, non-null, non-empty)
        - permalink: str (REQUIRED, canonical post-level URL)
        - timestamp_parsed: str | null (ISO8601 only)
        - timestamp_raw: str | null (original if not ISO8601)
        - captured_at: str | null
        - parent_context: str | null (thread/post-level URL, NOT comment-parent)
        - source_index: int (REQUIRED, monotonic)
    """
    # Extract and clean source fields
    post_url = (record.get("post_url") or "").strip()
    author = (record.get("author") or "").strip()
    body = (record.get("comment_text") or "").strip()
    raw_timestamp = record.get("timestamp")
    
    # Validate required fields (fail-loud)
    validate_permalink(post_url, source_index)
    validate_required_fields(author, body, source_index)
    
    # Parse timestamp correctly
    timestamp_parsed, timestamp_raw = parse_timestamp(raw_timestamp)
    
    # Build normalized record
    return {
        "schema_version": SCHEMA_VERSION,
        "item_type": "comment",
        "body": body,
        "author": author,
        "permalink": post_url,  # Canonical post-level URL (validated)
        "timestamp_parsed": timestamp_parsed,
        "timestamp_raw": timestamp_raw,
        "captured_at": None,  # Not available in source
        # parent_context: thread/post-level URL where this comment appears
        # NOTE: This is the parent POST, not a parent comment. No reply-chain inference.
        "parent_context": post_url if post_url else None,
        "source_index": source_index,
    }


def main() -> None:
    if not SRC_COMMENTS.exists():
        raise SystemExit(f"Missing input: {SRC_COMMENTS.resolve()}")
    
    # Load source comments (order preserved)
    raw_records, bad_lines = load_jsonl(SRC_COMMENTS)
    
    # Fail-loud if any malformed lines
    if bad_lines > 0:
        raise SystemExit(f"FAIL: {bad_lines} malformed JSONL line(s) in {SRC_COMMENTS.name}. Fix source data.")
    
    if not raw_records:
        raise SystemExit(f"No records found in {SRC_COMMENTS.resolve()}")
    
    # Adapt each record with monotonic source_index
    normalized: List[Dict[str, Any]] = []
    for idx, record in enumerate(raw_records):
        normalized.append(adapt_comment(record, source_index=idx))
    
    # Write output (order preserved = deterministic)
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    
    with OUT_NORMALIZED.open("w", encoding="utf-8") as f:
        for item in normalized:
            f.write(json.dumps(item, ensure_ascii=False) + "\n")
    
    print("Step 1b complete.")
    print(f"Read:  {SRC_COMMENTS.resolve()}")
    print(f"Wrote: {OUT_NORMALIZED.resolve()}")
    print(f"Records: {len(normalized)}")
    print(f"Schema version: {SCHEMA_VERSION}")


if __name__ == "__main__":
    main()
