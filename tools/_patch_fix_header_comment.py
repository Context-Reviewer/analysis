from pathlib import Path
import sys

p = Path("tools/context_enrich.py")
t = p.read_text(encoding="utf-8")

old = "- Mapping: source_index = i - 1"
if old not in t:
    print("[PATCH SKIP] mapping comment not found (maybe already edited).")
    sys.exit(0)

new = "- Join key: timeline_i (1-based index into sean_timeline.json at generation time)."
t = t.replace(old, new, 1)

p.write_text(t, encoding="utf-8")
print("[PATCH OK] Updated stale mapping comment.")
