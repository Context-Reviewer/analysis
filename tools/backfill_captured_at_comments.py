import json
from pathlib import Path
from datetime import datetime, timezone


IN_PATH = Path("fb_extract_out/comments_normalized_sean.jsonl")
BAK_PATH = Path("fb_extract_out/comments_normalized_sean.jsonl.bak")
RUN_TS_UTC = datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def fail(msg: str) -> None:
    raise SystemExit(f"[FAIL] {msg}")


def main() -> None:
    if not IN_PATH.exists():
        fail(f"Missing input: {IN_PATH}")

    lines = IN_PATH.read_text(encoding="utf-8").splitlines()
    if not lines:
        fail("Input file is empty")

    # Backup (fail-loud if already exists to avoid accidental overwrite)
    if BAK_PATH.exists():
        fail(f"Backup already exists: {BAK_PATH} (remove it manually if you intend to re-run)")
    BAK_PATH.write_text("\n".join(lines) + "\n", encoding="utf-8", newline="\n")

    out_lines = []
    total = 0
    filled = 0
    kept = 0

    for lineno, line in enumerate(lines, start=1):
        if not line.strip():
            continue
        try:
            r = json.loads(line)
        except Exception as e:
            fail(f"Invalid JSON at line {lineno}: {e}")
        if not isinstance(r, dict):
            fail(f"Non-object JSON at line {lineno}")

        total += 1
        if r.get("captured_at") is None:
            r["captured_at"] = RUN_TS_UTC
            r["captured_at_source"] = "backfill_run_utc"
            filled += 1
        else:
            kept += 1

        out_lines.append(json.dumps(r, ensure_ascii=False, separators=(",", ":")))

    if total == 0:
        fail("No records found")

    # Write normalized JSONL (deterministic line order preserved)
    IN_PATH.write_text("\n".join(out_lines) + "\n", encoding="utf-8", newline="\n")

    print(f"Backfill complete: {IN_PATH}")
    print(f"  run_ts_utc: {RUN_TS_UTC}")
    print(f"  total: {total}")
    print(f"  filled: {filled}")
    print(f"  kept: {kept}")
    print(f"  backup: {BAK_PATH}")


if __name__ == "__main__":
    main()
