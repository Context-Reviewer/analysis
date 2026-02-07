from pathlib import Path
import re, sys

p = Path("tools/context_enrich.py")
t = p.read_text(encoding="utf-8")

# Replace required columns set
old_pat = re.compile(r"required\s*=\s*\{\s*\"i\"\s*,\s*\"permalink\"\s*,\s*\"topics\"\s*\}")
if not old_pat.search(t):
    raise SystemExit("[PATCH FAIL] required columns set not found in expected form.")
t = old_pat.sub('required = {"i", "timeline_i", "permalink", "topics"}', t, count=1)

p.write_text(t, encoding="utf-8")
print("[PATCH OK] required now includes timeline_i.")
