import json
import subprocess
import sys
from pathlib import Path

SCHEMA_OUT = Path("schemas/signal_output-1.0.schema.json")

SIGNALS_DIR = Path("fb_extract_out/signals")
DOCS_SIGNALS_HTML = Path("docs/signals.html")
DOCS_DATA = Path("docs/data")
DOCS_INDEX = DOCS_DATA / "signals_index.json"

def die(msg: str, code: int = 2) -> None:
    print(f"[FAIL] {msg}")
    raise SystemExit(code)

def run_check_jsonschema(schema: Path, target: Path) -> None:
    cmd = [sys.executable, "-m", "check_jsonschema", "--schemafile", str(schema), str(target)]
    r = subprocess.run(cmd, capture_output=True, text=True)
    if r.returncode != 0:
        print(r.stdout.strip())
        print(r.stderr.strip())
        die(f"Schema validation failed: {target}")

def load_json(p: Path) -> dict:
    return json.loads(p.read_text(encoding="utf-8"))

def main() -> None:
    # 1) Required artifacts exist
    if not SCHEMA_OUT.exists():
        die(f"Missing schema: {SCHEMA_OUT}")
    if not DOCS_SIGNALS_HTML.exists():
        die(f"Missing page: {DOCS_SIGNALS_HTML}")
    if not DOCS_INDEX.exists():
        die(f"Missing docs index: {DOCS_INDEX}")
    if not SIGNALS_DIR.exists():
        die(f"Missing signals dir: {SIGNALS_DIR}")

    # 2) Validate every signal output in fb_extract_out/signals
    outputs = sorted([p for p in SIGNALS_DIR.glob("*.json") if p.name != "index.json"])
    if not outputs:
        die("No signal outputs found in fb_extract_out/signals/*.json")

    for p in outputs:
        run_check_jsonschema(SCHEMA_OUT, p)

    # 3) Validate docs index references
    idx = load_json(DOCS_INDEX)
    signals = idx.get("signals", [])
    if not isinstance(signals, list):
        die("docs/data/signals_index.json: 'signals' is not a list")

    missing = []
    for s in signals:
        sid = (s or {}).get("signal_id")
        if not sid:
            die("docs index contains a signal without signal_id")
        expected = DOCS_DATA / f"{sid}.json"
        if not expected.exists():
            missing.append(str(expected))

    if missing:
        die("Missing published signal JSON(s):\n  " + "\n  ".join(missing))

    print("[OK] contracts_signals_page: all checks passed")

if __name__ == "__main__":
    main()
