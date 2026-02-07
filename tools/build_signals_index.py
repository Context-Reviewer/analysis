import json
from pathlib import Path

SIGNALS_DIR = Path("fb_extract_out/signals")
OUT_INDEX = SIGNALS_DIR / "index.json"

def load_json(p: Path) -> dict:
    return json.loads(p.read_text(encoding="utf-8"))

def main() -> None:
    if not SIGNALS_DIR.exists():
        raise SystemExit(f"Missing: {SIGNALS_DIR}")

    rows = []
    for p in sorted(SIGNALS_DIR.glob("*.json")):
        if p.name == "index.json":
            continue
        d = load_json(p)

        rows.append({
            "signal_id": d.get("signal_id"),
            "signal_version": d.get("signal_version"),
            "tier": d.get("tier"),
            "dataset_scope": d.get("dataset_scope", {}),
            "metrics": d.get("metrics", {}),
            "examples_count": len(d.get("examples", [])),
            "fingerprints": d.get("fingerprints", {}),
            "path": str(p.as_posix()),
        })

    # Deterministic sort: tier then signal_id
    rows.sort(key=lambda r: (str(r.get("tier")), str(r.get("signal_id"))))

    out = {
        "signals_dir": str(SIGNALS_DIR.as_posix()),
        "count": len(rows),
        "signals": rows,
    }
    OUT_INDEX.write_text(json.dumps(out, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"Wrote: {OUT_INDEX}")

if __name__ == "__main__":
    main()
