from __future__ import annotations
from pathlib import Path
import re, sys

p = Path("tools/context_enrich.py")
t = p.read_text(encoding="utf-8")

# Replace join_mode_counts initialization
t2, n = re.subn(
    r"join_mode_counts\s*=\s*\{[^\}]*\}",
    "join_mode_counts = {\"timeline_i\": 0}",
    t,
    count=1
)
if n != 1:
    raise SystemExit(f"[PATCH FAIL] Could not replace join_mode_counts init (matches={n}).")
t = t2

# Replace summary print lines that reference removed keys
t = re.sub(
    r"print\(f\"  Joined via comment i->source_index: \{join_mode_counts\['comment_i_to_source_index'\]\}\"\)\s*\r?\n",
    "    print(f\"  Joined via timeline i:              {join_mode_counts['timeline_i']}\")\n",
    t
)
t = re.sub(
    r"print\(f\"  Joined via post permalink:\s*\{join_mode_counts\['post_permalink'\]\}\"\)\s*\r?\n",
    "",
    t
)

p.write_text(t, encoding="utf-8")
print("[PATCH OK] Normalized join_mode_counts + summary prints.")
