from pathlib import Path
import sys

p = Path("pipeline/step2_build_timeline.py")
t = p.read_text(encoding="utf-8")

needle = '        enriched.append({\n'
if needle not in t:
    raise SystemExit("[PATCH FAIL] Could not find enriched.append({ block start.")

# Insert right after permalink/author (stable location)
insert_after = '            "author": r.get("author") or "",\n'
if insert_after not in t:
    raise SystemExit("[PATCH FAIL] Could not find author line inside enriched dict.")

insertion = (
    '            "author": r.get("author") or "",\n'
    '            "item_type": r.get("item_type") or "",\n'
    '            "source_index": r.get("source_index"),\n'
    '            "parent_context": r.get("parent_context") or "",\n'
)

t = t.replace(insert_after, insertion, 1)

p.write_text(t, encoding="utf-8")
print("[PATCH OK] step2 timeline now preserves item_type/source_index/parent_context.")
