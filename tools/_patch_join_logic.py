from pathlib import Path
import re, sys

path = Path("tools/context_enrich.py")
text = path.read_text(encoding="utf-8")

anchor = "# Join Logic:"
if anchor not in text:
    raise SystemExit("[PATCH FAIL] Join Logic anchor not found")

pattern = re.compile(
    r"# Join Logic:\s*"
    r"# 1\. Try comment join by source_index \(i - 1\).*?"
    r"if not tl_item:\s*",
    re.S
)

replacement = (
    "# Join Logic:\n"
    "# Deterministic join: topics.csv i is a 1-based index into the timeline\n"
    "if not (1 <= i_val <= len(timeline)):\n"
    "    fail(f\"topics.csv i out of range: i={i_val} timeline_len={len(timeline)}\")\n"
    "\n"
    "tl_item = timeline[i_val - 1]\n"
    "tl_pl = (tl_item.get(\"permalink\") or \"\").strip()\n"
    "if tl_pl != pl:\n"
    "    fail(f\"topics.csv row i does not match timeline permalink: i={i_val} topics_permalink={pl} timeline_permalink={tl_pl}\")\n"
    "\n"
    "joined_item_type = \"comment\" if tl_item.get(\"source_index\") is not None else \"post\"\n"
    "join_mode_counts[\"timeline_i\"] = join_mode_counts.get(\"timeline_i\", 0) + 1\n"
    "\n"
    "if not tl_item:\n"
)

new_text, n = pattern.subn(replacement, text, count=1)
if n != 1:
    raise SystemExit("[PATCH FAIL] Join block replacement failed")

path.write_text(new_text, encoding="utf-8")
print("[PATCH OK] Join logic updated to timeline index semantics.")
