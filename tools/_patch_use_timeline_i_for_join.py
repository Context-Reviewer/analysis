from pathlib import Path
import sys

p = Path("tools/context_enrich.py")
t = p.read_text(encoding="utf-8")

# 1) Insert timeline_i parsing right after i_val line
needle = 'i_val = int(row["i"]) # Validated above'
if needle not in t:
    raise SystemExit("[PATCH FAIL] Could not find i_val assignment line.")

insert = (
    'i_val = int(row["i"]) # Validated above\n'
    '        timeline_i = int(row["timeline_i"]) # Deterministic join key\n'
)
t = t.replace(needle, insert, 1)

# 2) Replace timeline index usage to use timeline_i
old = 'tl_item = timeline[i_val - 1]'
new = 'tl_item = timeline[timeline_i - 1]'
if old not in t:
    raise SystemExit("[PATCH FAIL] Could not find tl_item = timeline[i_val - 1] line.")
t = t.replace(old, new, 1)

# 3) Update range check to use timeline_i (if present)
# If your code currently checks i_val bounds, swap it.
t = t.replace('if not (1 <= i_val <= len(timeline)):', 'if not (1 <= timeline_i <= len(timeline)):', 1)

p.write_text(t, encoding="utf-8")
print("[PATCH OK] context_enrich now joins timeline using row['timeline_i'].")
