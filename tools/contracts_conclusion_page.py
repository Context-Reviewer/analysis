from pathlib import Path

def fail(msg: str) -> None:
    raise SystemExit(f"[FAIL] {msg}")

def main() -> None:
    p = Path("docs/conclusion.html")
    j = Path("docs/data/conclusion.json")
    if not p.exists():
        fail("Missing docs/conclusion.html")
    if not j.exists():
        fail("Missing docs/data/conclusion.json")
    s = p.read_text(encoding="utf-8")
    if "<h1>Conclusion</h1>" not in s:
        fail("Conclusion page missing H1")
    if "Scope and caveats" not in s:
        fail("Conclusion page missing caveats section")
    print("[OK] conclusion page contract")

if __name__ == "__main__":
    main()
