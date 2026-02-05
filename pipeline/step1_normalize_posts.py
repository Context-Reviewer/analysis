# step1_normalize_posts.py
# Purpose (Step 1):
# - Read fb_extract_out/posts_all.jsonl (supports JSON array OR JSONL)
# - Normalize each record and extract a clean "body" from messy mbasic/plaintext captures
# - Output JSONL:
#     fb_extract_out/posts_normalized.jsonl
#     fb_extract_out/posts_normalized_sean.jsonl
#
# Windows / no extra deps.

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple


OUT_DIR = Path("fb_extract_out")
SRC_DEFAULT = OUT_DIR / "posts_all.jsonl"

TARGET_AUTHOR = "Sean Roy"   # used only for the _sean output file
MIN_BODY_CHARS = 5           # discard bodies shorter than this as "empty"


# -----------------------------
# Utilities
# -----------------------------

def now_iso() -> str:
    return datetime.now().isoformat(timespec="seconds")

def safe_load_json(text: str) -> Any:
    return json.loads(text)

def iter_records_from_file(path: Path) -> Iterable[Dict[str, Any]]:
    """
    Supports:
    - JSON array file (your posts_all.jsonl currently looks like a JSON array)
    - JSONL file (one object per line)
    """
    raw = path.read_text(encoding="utf-8", errors="replace").strip()
    if not raw:
        return []

    # If it starts like a JSON array, parse as array.
    if raw.lstrip().startswith("["):
        data = safe_load_json(raw)
        if isinstance(data, list):
            for obj in data:
                if isinstance(obj, dict):
                    yield obj
        return

    # Otherwise treat as JSONL
    for line in raw.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            obj = safe_load_json(line)
        except Exception:
            continue
        if isinstance(obj, dict):
            yield obj


def normalize_ws(s: str) -> str:
    # Convert newlines/tabs to spaces, collapse repeats
    s = s.replace("\u00a0", " ")
    s = re.sub(r"[\r\n\t]+", " ", s)
    s = re.sub(r"\s{2,}", " ", s).strip()
    return s


def strip_weird_spaced_letters(s: str) -> str:
    """
    mbasic often inserts 'd p n t r o e o s ...' garbage from UI elements.
    Strategy: find first real word (3+ consecutive letters) and keep from there.
    This removes the garbage prefix while preserving actual content.
    """
    # Find first real word (3+ letters in a row)
    m = re.search(r'[A-Za-z]{3,}', s)
    if m:
        s = s[m.start():]
    # Also remove remaining short single-char token runs (6+ in a row)
    s = re.sub(r'(?:\b[A-Za-z0-9]\b\s+){6,}', ' ', s)
    return re.sub(r'\s{2,}', ' ', s).strip()


def parse_relative_time(timestamp_raw: str, captured_at_iso: str) -> Tuple[Optional[str], Optional[str]]:
    """
    Converts simple relative timestamps like:
      '47m', '1h', '4d', '2w'
    into an ISO datetime based on captured_at.
    Returns (parsed_iso, kind)
    """
    ts = (timestamp_raw or "").strip()
    if not ts:
        return None, None

    # Examples seen: '47m', '1h', '4d'
    m = re.fullmatch(r"(\d+)\s*([mhdw])", ts, flags=re.IGNORECASE)
    if not m:
        return None, None

    n = int(m.group(1))
    unit = m.group(2).lower()

    try:
        cap = datetime.fromisoformat(captured_at_iso)
    except Exception:
        return None, None

    delta = None
    if unit == "m":
        delta = timedelta(minutes=n)
    elif unit == "h":
        delta = timedelta(hours=n)
    elif unit == "d":
        delta = timedelta(days=n)
    elif unit == "w":
        delta = timedelta(weeks=n)

    if delta is None:
        return None, None

    dt = cap - delta
    return dt.isoformat(timespec="seconds"), f"relative_{unit}"


# -----------------------------
# Body extraction heuristics
# -----------------------------

@dataclass
class BodyExtractResult:
    body: str
    confidence: int
    method: str
    notes: List[str]


def extract_body_from_mbasic_text(full_text: str, author: str, group_name_hint: Optional[str] = None) -> BodyExtractResult:
    """
    Extract the post body from your mbasic_html plaintext dump.

    Typical pattern (from your sample):
      ... "Sean Roy's Post" ... <tons> ... "<Group Name> Sean Roy · All-star contributor · <junk time> · <BODY> All reactions: ..."
    We'll:
      1) normalize whitespace
      2) prefer the LAST occurrence of the author header in the text
      3) from there, skip 1-3 '·' segments and capture the remainder
      4) cut off at "All reactions:" or "Like Comment" etc
      5) remove obvious FB chrome chunks
    """
    notes: List[str] = []
    if not full_text:
        return BodyExtractResult("", 0, "empty_text", ["empty input"])

    s = normalize_ws(full_text)
    s = strip_weird_spaced_letters(s)

    # Hard stop: kill the giant "Facebook Menu ... Contacts Meta AI ..." lead-in if present.
    # We'll try to anchor near the post header instead.
    # Prefer: last occurrence of "<Group Name> <Author> ·"
    author_pat = re.escape(author)

    # Candidate anchors ordered strongest -> weaker:
    anchors: List[Tuple[str, str, int]] = []

    if group_name_hint:
        g = re.escape(group_name_hint)
        anchors.append((rf"{g}\s+{author_pat}\s*·", "group+author", 300))

    anchors.append((rf"{author_pat}\s*·", "author_dot", 200))
    anchors.append((rf"{author_pat}'s Post", "author_possessive", 120))

    anchor_match = None
    anchor_kind = None
    anchor_score = 0

    for pat, kind, score in anchors:
        matches = list(re.finditer(pat, s, flags=re.IGNORECASE))
        if matches:
            anchor_match = matches[-1]  # last occurrence is usually the actual post header
            anchor_kind = kind
            anchor_score = score
            notes.append(f"anchor={kind}")
            break

    if not anchor_match:
        # fallback: try a generic "Send Most relevant" etc marker? not great.
        return BodyExtractResult("", 0, "no_anchor", ["no author anchor found"])

    tail = s[anchor_match.end():].strip()

    # The body is usually after 1-3 '·' separators (badge/time garbage).
    # Example: "All-star contributor · <junk> · BODY ..."
    # We'll split on the '·' char and walk segments.
    segments = [seg.strip() for seg in tail.split("·")]

    # remove empty segments
    segments = [seg for seg in segments if seg]

    # Choose a start index:
    # - skip badge-like segments ("All-star contributor", "Follow", etc)
    # - skip segments that are mostly short or look like timestamp tokens
    def looks_like_badge(seg: str) -> bool:
        seg_l = seg.lower()
        if "all-star contributor" in seg_l:
            return True
        if seg_l in {"follow", "edited"}:
            return True
        if "group chats" in seg_l:
            return True
        return False

    def looks_like_time_or_garbage(seg: str) -> bool:
        seg = seg.strip()
        if re.fullmatch(r"\d+\s*[mhdw]", seg, flags=re.IGNORECASE):
            return True
        # Very short segments are not body
        if len(seg) <= 2:
            return True
        # Segments that are mostly punctuation/numbers
        if len(re.sub(r"[A-Za-z]", "", seg)) / max(1, len(seg)) > 0.85:
            return True
        return False

    start_idx = 0
    skipped = 0
    for i, seg in enumerate(segments[:8]):  # don’t scan forever
        if looks_like_badge(seg) or looks_like_time_or_garbage(seg):
            skipped += 1
            continue
        # first decent segment becomes the start
        start_idx = i
        break

    notes.append(f"segments={len(segments)} skipped_prefix={skipped}")

    candidate = " · ".join(segments[start_idx:]).strip()

    # Cut off at common UI markers
    cut_markers = [
        "All reactions:",
        "Like Comment Share",
        "Like Comment Send",
        "Most relevant",
        "Comment as ",
        "Answer as ",
        "View all",
        "Facebook Facebook",  # lots of dumps contain repeated "Facebook"
    ]
    cut_at = None
    for m in cut_markers:
        pos = candidate.find(m)
        if pos != -1:
            if cut_at is None or pos < cut_at:
                cut_at = pos

    if cut_at is not None:
        candidate = candidate[:cut_at].strip()
        notes.append(f"cut_marker_at={cut_at}")

    # Final cleanup
    candidate = normalize_ws(candidate)
    candidate = candidate.strip(" ·-—|")

    # Remove some remaining chrome phrases that sneak in
    chrome_phrases = [
        "Facebook Menu",
        "Home Create a post",
        "What's on your mind",
        "Stories Create story",
        "Sponsored",
        "Contacts Meta AI",
        "Group chats Create group chat",
        "your shortcuts",
    ]
    for ph in chrome_phrases:
        candidate = candidate.replace(ph, "").strip()

    candidate = normalize_ws(candidate)

    confidence = anchor_score
    if len(candidate) >= 30:
        confidence += 200
    elif len(candidate) >= 10:
        confidence += 100
    else:
        confidence -= 150
        notes.append("short_body")

    # If it STILL contains lots of repeated "Facebook", confidence down
    if candidate.lower().count("facebook") >= 3:
        confidence -= 150
        notes.append("facebook_repetition")

    return BodyExtractResult(candidate, confidence, f"mbasic_v1({anchor_kind})", notes)


# -----------------------------
# Main normalization
# -----------------------------

def detect_group_name_hint(text: str) -> Optional[str]:
    # Your group name appears a lot; we can try to detect it once.
    if not text:
        return None
    s = normalize_ws(text)
    # crude: look for "... | Facebook" title or the group name phrase
    m = re.search(r"(E4 Mafia's International House of Sausage \(RAW, AND UNCUT\))", s, flags=re.IGNORECASE)
    if m:
        return m.group(1)
    return None


def pick_author(rec: Dict[str, Any], text: str) -> str:
    # Prefer explicit field if it exists
    for key in ("author", "chosen_author"):
        v = rec.get(key)
        if isinstance(v, str) and v.strip():
            return v.strip()

    # Otherwise, try to infer from the text around "...'s Post"
    s = normalize_ws(text or "")
    m = re.search(r"([A-Z][A-Za-z'\-]+\s+[A-Z][A-Za-z'\-]+)'s Post", s)
    if m:
        return m.group(1).strip()

    # Fallback: empty
    return ""


def normalize_record(rec: Dict[str, Any], group_hint: Optional[str]) -> Dict[str, Any]:
    permalink = (rec.get("permalink") or rec.get("url") or "").strip()
    captured_at = (rec.get("captured_at") or rec.get("sort_time") or "").strip()
    timestamp_raw = (rec.get("timestamp_raw") or rec.get("timestamp") or "").strip()
    text = rec.get("text") or ""

    if not isinstance(text, str):
        text = str(text)

    author = pick_author(rec, text)

    body_res = extract_body_from_mbasic_text(text, author=author or TARGET_AUTHOR, group_name_hint=group_hint)
    body = body_res.body

    parsed_time_iso, time_kind = parse_relative_time(timestamp_raw, captured_at) if captured_at else (None, None)

    out = {
        "permalink": permalink,
        "author": author,
        "captured_at": captured_at,
        "timestamp_raw": timestamp_raw,
        "timestamp_parsed": parsed_time_iso,
        "timestamp_kind": time_kind,
        "extraction_source": rec.get("extraction_source") or rec.get("source") or "",
        "body": body,
        "body_len": len(body),
        "body_method": body_res.method,
        "body_confidence": body_res.confidence,
        "body_notes": body_res.notes,
        # Keep original index if present (useful for tracing)
        "idx": rec.get("idx"),
    }
    return out


def write_jsonl(path: Path, rows: List[Dict[str, Any]]) -> None:
    with path.open("w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")


def main() -> None:
    src = SRC_DEFAULT
    if not src.exists():
        raise SystemExit(f"Missing input: {src.resolve()}")

    records = list(iter_records_from_file(src))
    if not records:
        raise SystemExit(f"No records parsed from {src.resolve()} (empty or invalid JSON)")

    # Try to detect group name from first few records
    group_hint = None
    for r in records[:5]:
        t = r.get("text") if isinstance(r, dict) else None
        if isinstance(t, str):
            group_hint = detect_group_name_hint(t)
            if group_hint:
                break

    normalized: List[Dict[str, Any]] = []
    failures: List[Dict[str, Any]] = []

    for r in records:
        if not isinstance(r, dict):
            continue
        n = normalize_record(r, group_hint=group_hint)
        normalized.append(n)
        if n["body_len"] < MIN_BODY_CHARS:
            failures.append(n)

    # Output
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    out_all = OUT_DIR / "posts_normalized.jsonl"
    out_sean = OUT_DIR / "posts_normalized_sean.jsonl"

    write_jsonl(out_all, normalized)

    norm_sean = [x for x in normalized if (x.get("author") or "").strip().lower() == TARGET_AUTHOR.lower()]
    write_jsonl(out_sean, norm_sean)

    # Summary stats
    total = len(normalized)
    empty = sum(1 for x in normalized if x["body_len"] < MIN_BODY_CHARS)

    authors: Dict[str, int] = {}
    for x in normalized:
        a = (x.get("author") or "").strip() or "(unknown)"
        authors[a] = authors.get(a, 0) + 1

    top_authors = sorted(authors.items(), key=lambda kv: kv[1], reverse=True)[:10]

    print(f"[{now_iso()}] Step1 normalize complete")
    print(f"Input records: {len(records)}")
    print(f"Output: {out_all.resolve()}")
    print(f"Output (Sean only): {out_sean.resolve()}")
    print(f"Total normalized: {total}")
    print(f"Empty/too-short bodies (<{MIN_BODY_CHARS} chars): {empty}")
    print("Top authors:")
    for a, c in top_authors:
        print(f"  {c:>4}  {a}")

    if failures:
        print("\n--- Sample failures (first 5) ---")
        for x in failures[:5]:
            print(f"- idx={x.get('idx')} author='{x.get('author')}' body_len={x.get('body_len')} url={x.get('permalink')}")
            print(f"  method={x.get('body_method')} confidence={x.get('body_confidence')} notes={x.get('body_notes')}")
        print("If these are actually valid posts, we’ll tune the body cut markers / anchor logic in Step 1.1.")


if __name__ == "__main__":
    main()
