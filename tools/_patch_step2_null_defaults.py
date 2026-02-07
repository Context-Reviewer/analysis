from pathlib import Path
import re, sys

p = Path("pipeline/step2_build_timeline.py")
t = p.read_text(encoding="utf-8")

# Patch posts default fill block
old_posts = (
    '            if "item_type" not in r:\n'
    '                r["item_type"] = "post"\n'
    '            if "source_index" not in r:\n'
    '                r["source_index"] = idx\n'
)
if old_posts not in t:
    raise SystemExit("[PATCH FAIL] Expected posts default-fill block not found verbatim.")

new_posts = (
    '            # Treat None as missing (adapter may emit nulls)\n'
    '            if r.get("item_type") is None:\n'
    '                r["item_type"] = "post"\n'
    '            if r.get("source_index") is None:\n'
    '                r["source_index"] = idx\n'
)
t = t.replace(old_posts, new_posts, 1)

# Patch the comments section note by inserting a backfill right after it
marker = "        # Comments already have item_type and source_index from adapter"
if marker not in t:
    raise SystemExit("[PATCH FAIL] Comments marker not found.")

inject = (
    marker + "\n"
    "        # Treat None as missing (adapter may emit nulls)\n"
    "        for idx, r in enumerate(comments):\n"
    "            if r.get(\"item_type\") is None:\n"
    "                r[\"item_type\"] = \"comment\"\n"
    "            if r.get(\"source_index\") is None:\n"
    "                r[\"source_index\"] = idx\n"
)
t = t.replace(marker, inject, 1)

p.write_text(t, encoding="utf-8")
print("[PATCH OK] step2 now backfills item_type/source_index when null.")
