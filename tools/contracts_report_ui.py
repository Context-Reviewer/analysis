from __future__ import annotations
from pathlib import Path

def fail(msg: str) -> None:
    raise SystemExit(f"[FAIL] {msg}")

def main() -> None:
    p = Path("docs/report.html")
    if not p.exists():
        fail("Missing docs/report.html")

    s = p.read_text(encoding="utf-8")
    if 'id="topic-dropdown"' in s:
        fail('report.html must not contain the legacy topic dropdown select (id="topic-dropdown").')

    print("[OK] report UI contract (no legacy topic dropdown)")

if __name__ == "__main__":
    main()
