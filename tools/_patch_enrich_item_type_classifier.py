from pathlib import Path
import sys

p = Path("tools/context_enrich.py")
t = p.read_text(encoding="utf-8")

old = 'joined_item_type = "comment" if tl_item.get("source_index") is not None else "post"'
if old not in t:
    raise SystemExit("[PATCH FAIL] Expected joined_item_type classification line not found.")

new = (
    'joined_item_type = (tl_item.get("item_type") or "").strip()\n'
    '        if joined_item_type not in ("post", "comment"):\n'
    '            fail(f"Timeline item missing/invalid item_type at timeline_i={timeline_i}: item_type={joined_item_type!r}")\n'
)

t = t.replace(old, new, 1)

p.write_text(t, encoding="utf-8")
print("[PATCH OK] context_enrich now classifies via tl_item['item_type'] (fail-loud if missing).")
