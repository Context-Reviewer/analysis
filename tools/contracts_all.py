import sys
import subprocess
from pathlib import Path

CONTRACTS = [
    "tools/contracts_timeline_semantics.py",
    "tools/contracts_enriched_semantics.py",
    "tools/contracts_report_ui.py",
    "tools/contracts_conclusion_page.py",
    "tools/contracts_signals_page.py",
]

def run_one(path: str) -> None:
    p = Path(path)
    if not p.exists():
        raise SystemExit(f"[FAIL] Contract missing: {path}")
    print(f"[RUN] {path}")
    r = subprocess.run([sys.executable, str(p)])
    if r.returncode != 0:
        raise SystemExit(r.returncode)

def main() -> None:
    for c in CONTRACTS:
        run_one(c)
    print("[OK] All contracts passed")

if __name__ == "__main__":
    main()

print("[RUN] tools/contracts_nav_idempotent.py")
r = subprocess.run([sys.executable, "tools/contracts_nav_idempotent.py"])
if r.returncode != 0:
    raise SystemExit(r.returncode)

print("[RUN] tools/contracts_run_manifest.py")
r = subprocess.run([sys.executable, "tools/contracts_run_manifest.py"])
if r.returncode != 0:
    raise SystemExit(r.returncode)
