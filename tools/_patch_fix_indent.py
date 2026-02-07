from pathlib import Path
import re, sys

path = Path("tools/context_enrich.py")
text = path.read_text(encoding="utf-8")

# We expect a dangling:
#     if not tl_item:
#         missing.append(...)
pattern = re.compile(
    r"\n\s*if not tl_item:\s*\n\s*missing\.append\(f\"i=\{i_val\} permalink=\{pl\}\"\)\s*\n",
    re.M
)

new_text, n = pattern.subn("\n", text, count=1)

if n != 1:
    raise SystemExit(f"[PATCH FAIL] Expected dangling if-not-tl_item block not found (matches={n})")

path.write_text(new_text, encoding="utf-8")
print("[PATCH OK] Removed unreachable if-not-tl_item block.")
