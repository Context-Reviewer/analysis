from __future__ import annotations
from pathlib import Path
import re, sys

path = Path("tools/context_enrich.py")
text = path.read_text(encoding="utf-8")

start_anchor = "        # Join Logic:"
end_anchor = "        # Use existing source_index from timeline if present"

if start_anchor not in text:
    raise SystemExit("[PATCH FAIL] start anchor not found (# Join Logic:)")
if end_anchor not in text:
    raise SystemExit("[PATCH FAIL] end anchor not found (# Use existing source_index...)")

pattern = re.compile(
    re.escape(start_anchor) + r".*?" + re.escape(end_anchor),
    flags=re.S
)

replacement = (
    "        # Join Logic:\n"
    "        # Deterministic join: topics.csv i is a 1-based index into the timeline.\n"
    "        if not (1 <= i_val <= len(timeline)):\n"
    "            fail(f\"topics.csv i out of range: i={i_val} timeline_len={len(timeline)}\")\n"
    "\n"
    "        tl_item = timeline[i_val - 1]\n"
    "        tl_pl = (tl_item.get(\"permalink\") or \"\").strip()\n"
    "        if tl_pl != pl:\n"
    "            fail(\n"
    "                f\"topics.csv row i does not match timeline permalink: \"\n"
    "                f\"i={i_val} topics_permalink={pl} timeline_permalink={tl_pl}\"\n"
    "            )\n"
    "\n"
    "        joined_item_type = \"comment\" if tl_item.get(\"source_index\") is not None else \"post\"\n"
    "        join_mode_counts[\"timeline_i\"] = join_mode_counts.get(\"timeline_i\", 0) + 1\n"
    "\n"
    "        # Use existing source_index from timeline if present"
)

new_text, n = pattern.subn(replacement, text, count=1)
if n != 1:
    raise SystemExit(f"[PATCH FAIL] Join block replace did not apply cleanly (matches={n}).")

path.write_text(new_text, encoding="utf-8")
print("[PATCH OK] Rewrote join logic block with correct indentation.")
