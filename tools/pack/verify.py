#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
from typing import Dict


def _sha256_file(path: Path, chunk_size: int = 1024 * 1024) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        while True:
            chunk = f.read(chunk_size)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Verify integrity of a frozen pack.")
    p.add_argument("--pack-dir", required=True, help="Path to pack directory (contains manifest.json)")
    p.add_argument(
        "--strict",
        action="store_true",
        help="Fail if extra files exist in files/ that are not listed in the manifest.",
    )
    return p.parse_args()


def main() -> int:
    args = parse_args()
    pack_dir = Path(args.pack_dir).resolve()
    manifest_path = pack_dir / "manifest.json"
    files_root = pack_dir / "files"

    if not manifest_path.exists():
        print(f"[!] Missing manifest.json: {manifest_path}")
        return 1

    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))

    entries = manifest.get("files", [])
    expected: Dict[Path, dict] = {}

    for e in entries:
        rel = Path(e["relpath"])
        expected[rel] = e

    errors = 0

    # Verify expected files
    for rel, meta in expected.items():
        path = files_root / rel
        if not path.exists():
            print(f"[FAIL] Missing file: {rel}")
            errors += 1
            continue

        size = path.stat().st_size
        if size != meta["bytes"]:
            print(f"[FAIL] Size mismatch: {rel} (expected {meta['bytes']}, got {size})")
            errors += 1

        digest = _sha256_file(path)
        if digest != meta["sha256"]:
            print(f"[FAIL] Hash mismatch: {rel}")
            errors += 1

    # Strict mode: detect extras
    if args.strict:
        actual_files = {
            p.relative_to(files_root)
            for p in files_root.rglob("*")
            if p.is_file()
        }
        extra = actual_files - set(expected.keys())
        for rel in sorted(extra):
            print(f"[FAIL] Extra file not in manifest: {rel}")
            errors += 1

    if errors:
        print(f"[i] Verification failed: {errors} error(s)")
        return 1

    print(f"[OK] Pack verified: {pack_dir}")
    print(f"[OK] Files: {len(expected)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
