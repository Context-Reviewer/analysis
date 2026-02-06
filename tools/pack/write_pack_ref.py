#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path


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
    p = argparse.ArgumentParser(description="Write docs/pack_ref.json for the currently published docs/ tree.")
    p.add_argument("--subject", required=True)
    p.add_argument("--run-id", required=True)
    p.add_argument("--docs-root", default="docs")
    p.add_argument("--out", default="docs/pack_ref.json")
    return p.parse_args()


def main() -> int:
    args = parse_args()
    docs_root = Path(args.docs_root).resolve()
    out_path = Path(args.out).resolve()

    if not docs_root.exists():
        raise SystemExit(f"[!] docs root missing: {docs_root}")

    entries = []
    for p in sorted([p for p in docs_root.rglob("*") if p.is_file()], key=lambda x: x.relative_to(docs_root).as_posix()):
        rel = p.relative_to(docs_root).as_posix()
        # Include the ref itself? No: we generate it after enumeration.
        if rel == "pack_ref.json":
            continue
        entries.append(
            {
                "relpath": rel,
                "bytes": p.stat().st_size,
                "sha256": sha256_file(p),
            }
        )

    ref = {
        "ref_version": "1.0",
        "subject": args.subject,
        "run_id": args.run_id,
        "docs_root": "docs",
        "file_count": len(entries),
        "files": entries,
    }

    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(ref, indent=2) + "\n", encoding="utf-8")
    print(f"[i] Wrote: {out_path} ({len(entries)} files)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
