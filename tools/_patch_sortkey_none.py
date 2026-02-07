from pathlib import Path
import sys

p = Path("tools/context_enrich.py")
t = p.read_text(encoding="utf-8")

old = 'si = int(rec.get("source_index", -1))'
if old not in t:
    print("[PATCH SKIP] sort_key line not found (maybe already fixed).")
    sys.exit(0)

new = 'raw_si = rec.get("source_index")\n    si = int(raw_si) if raw_si is not None else -1'
p.write_text(t.replace(old, new, 1), encoding="utf-8")
print("[PATCH OK] sort_key now handles source_index=None.")
