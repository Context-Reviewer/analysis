#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
from typing import Dict


def sha256_file(path: Path, chunk_size: int = 1024 * 1024) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        while True:
            chunk = f.read(chunk_size)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Verify repo docs/ against docs/pack_ref.json")
    p.add_argument("--ref", default="docs/pack_ref.json", help="Path to pack ref JSON in repo.")
    p.add_argument("--docs-root", default="docs", help="Docs root directory in repo.")
    p.add_argument("--strict", action="store_true", help="Fail if extra docs files exist not listed in ref.")
    return p.parse_args()


def main() -> int:
    args = parse_args()
    ref_path = Path(args.ref).resolve()
    docs_root = Path(args.docs_root).resolve()

    if not ref_path.exists():
        print(f"[FAIL] Missing ref file: {ref_path}")
        return 1
    if not docs_root.exists():
        print(f"[FAIL] Missing docs root: {docs_root}")
        return 1

    ref = json.loads(ref_path.read_text(encoding="utf-8"))
    files = ref.get("files", [])
    if not isinstance(files, list) or not files:
        print("[FAIL] Ref JSON has no files[] entries.")
        return 1

    expected: Dict[Path, dict] = {}
    for e in files:
        rel = Path(e["relpath"])
        expected[rel] = e

    errors = 0

    # verify expected files
    for rel, meta in expected.items():
        path = docs_root / rel
        if not path.exists():
            print(f"[FAIL] Missing docs file: {rel}")
            errors += 1
            continue

        size = path.stat().st_size
        if size != meta["bytes"]:
            print(f"[FAIL] Size mismatch: {rel} (expected {meta['bytes']}, got {size})")
            errors += 1

        digest = sha256_file(path)
        if digest != meta["sha256"]:
            print(f"[FAIL] Hash mismatch: {rel}")
            errors += 1

    # strict mode: fail if extra files in docs not listed
    if args.strict:
        actual = {
            p.relative_to(docs_root)
            for p in docs_root.rglob("*")
            if p.is_file()
        }
        extra = actual - set(expected.keys())
        for rel in sorted(extra):
            # allow the ref file itself even if not listed (some people prefer listing it; either way is fine)
            if rel.as_posix() == "pack_ref.json":
                continue
            print(f"[FAIL] Extra docs file not in ref: {rel}")
            errors += 1

    if errors:
        print(f"[i] Verification failed: {errors} error(s)")
        return 1

    print("[OK] docs/ matches docs/pack_ref.json")
    print(f"[OK] files verified: {len(expected)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
