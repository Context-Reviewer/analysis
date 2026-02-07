from __future__ import annotations

from pathlib import Path
import re
import sys

PATH = Path("tools/context_enrich.py")

text = PATH.read_text(encoding="utf-8")

def require(substr: str) -> None:
    if substr not in text:
        raise SystemExit(f"[PATCH FAIL] Expected anchor not found: {substr!r}")

# --- anchors we expect (fail-loud if file changed unexpectedly) ---
require("posts_by_permalink")
require("Ambiguous timeline post permalink")
require("sean_posts = load_jsonl(POSTS_SEAN_JSONL)")
require("sean_post_permalinks = set()")
require("tl_item = posts_by_permalink.get(pl)")
require("Timeline posts indexed")

# 1) Remove timeline-based posts index declaration
text2 = re.sub(
    r"^\s*posts_by_permalink:\s*Dict\[str,\s*Dict\[str,\s*Any\]\]\s*=\s*\{\}\s*\r?\n",
    "",
    text,
    flags=re.M,
)
if text2 == text:
    raise SystemExit("[PATCH FAIL] Could not remove posts_by_permalink declaration (pattern mismatch).")
text = text2

# 2) Remove pl = item.get("permalink") within timeline loop
text2 = re.sub(
    r"^\s*pl\s*=\s*item\.get\(\"permalink\"\)\s*\r?\n",
    "",
    text,
    flags=re.M,
)
if text2 == text:
    raise SystemExit("[PATCH FAIL] Could not remove pl = item.get('permalink') line (pattern mismatch).")
text = text2

# 3) Remove the entire 'Post index (unique)' block that fails on repeated permalinks
#    This block begins with a comment and ends with posts_by_permalink[k] = item
post_block_re = re.compile(
    r"\r?\n\s*# Post index \(unique\)\s*\r?\n"
    r"\s*if pl:\s*\r?\n"
    r"\s*k = str\(pl\)\s*\r?\n"
    r"\s*if k in posts_by_permalink.*?\r?\n"
    r"\s*fail\(f\"Ambiguous timeline post permalink=\{k\}\"\)\s*\r?\n"
    r"\s*posts_by_permalink\[k\] = item\s*\r?\n",
    flags=re.S,
)
text2, n = post_block_re.subn("\n", text, count=1)
if n != 1:
    raise SystemExit(f"[PATCH FAIL] Could not remove Post index block cleanly (matches={n}).")
text = text2

# 4) Replace sean_post_permalinks initialization with root_posts_by_permalink index
text2, n = re.subn(
    r"(sean_posts\s*=\s*load_jsonl\(POSTS_SEAN_JSONL\)\s*\r?\n)"
    r"(\s*)sean_post_permalinks\s*=\s*set\(\)\s*\r?\n",
    r"\1\2root_posts_by_permalink: Dict[str, Dict[str, Any]] = {}\n\2sean_post_permalinks = set()\n",
    text,
    count=1,
)
if n != 1:
    raise SystemExit("[PATCH FAIL] Could not inject root_posts_by_permalink (pattern mismatch).")
text = text2

# 5) Replace permalink add line to also populate root_posts_by_permalink with fail-loud on duplicates
#    Original:
#        sean_post_permalinks.add(str(pl))
replacement_block = (
    "            k = str(pl)\n"
    "            if k in root_posts_by_permalink and root_posts_by_permalink[k] is not r:\n"
    "                fail(f\"Ambiguous root post permalink in posts_normalized: permalink={k}\")\n"
    "            root_posts_by_permalink[k] = r\n"
    "            sean_post_permalinks.add(k)\n"
)
text2, n = re.subn(
    r"^\s*sean_post_permalinks\.add\(str\(pl\)\)\s*\r?\n",
    replacement_block,
    text,
    flags=re.M,
    count=1,
)
if n != 1:
    raise SystemExit("[PATCH FAIL] Could not rewrite sean_post_permalinks.add(str(pl)) (pattern mismatch).")
text = text2

# 6) Switch post join to use root_posts_by_permalink
text2, n = re.subn(
    r"tl_item\s*=\s*posts_by_permalink\.get\(pl\)",
    "tl_item = root_posts_by_permalink.get(pl)",
    text,
    count=1,
)
if n != 1:
    raise SystemExit("[PATCH FAIL] Could not rewrite posts_by_permalink.get(pl) join (pattern mismatch).")
text = text2

# 7) Fix print line (remove Timeline posts indexed; replace with Root posts indexed)
text2, n = re.subn(
    r"^\s*print\(f\"  Timeline posts indexed:\s*\{len\(posts_by_permalink\)\}\"\)\s*\r?\n",
    "    print(f\"Loading root posts: {POSTS_SEAN_JSONL}\")\n"
    "    print(f\"  Root posts indexed:        {len(root_posts_by_permalink)}\")\n",
    text,
    flags=re.M,
    count=1,
)
if n != 1:
    raise SystemExit("[PATCH FAIL] Could not rewrite Timeline posts indexed print (pattern mismatch).")
text = text2

# Final sanity: the bad invariant should be gone
if "Ambiguous timeline post permalink" in text:
    raise SystemExit("[PATCH FAIL] Ambiguous timeline post permalink is still present after patch.")

PATH.write_text(text, encoding="utf-8")
print("[PATCH OK] tools/context_enrich.py updated.")
