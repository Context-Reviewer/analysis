#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, List, Tuple


PACK_VERSION = "1.0"


DEFAULT_GLOBS = [
    "fb_extract_out/*.jsonl",
    "fb_extract_out/*.json",
    "fb_extract_out/*.csv",
    "fb_extract_out/*.md",
    "docs/**",
    "README.md",
]


@dataclass(frozen=True)
class FileEntry:
    relpath: str
    bytes: int
    sha256: str


def _sha256_file(path: Path, chunk_size: int = 1024 * 1024) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        while True:
            chunk = f.read(chunk_size)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()


def _iter_globbed_files(repo_root: Path, patterns: List[str]) -> Iterable[Path]:
    seen: set[Path] = set()
    for pat in patterns:
        # Use pathlib glob; "**" works when recursive=True via glob on full pattern
        for p in repo_root.glob(pat):
            if p.is_file():
                rp = p.resolve()
                if rp not in seen:
                    seen.add(rp)
                    yield rp


def _to_relpath(repo_root: Path, abs_path: Path) -> str:
    return abs_path.resolve().relative_to(repo_root.resolve()).as_posix()


def _copy_file(src: Path, dst: Path) -> None:
    dst.parent.mkdir(parents=True, exist_ok=True)
    # copy2 preserves mtime; that’s fine inside the pack; determinism comes from hashing + sorted manifest
    shutil.copy2(src, dst)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Freeze current outputs into an immutable pack directory.")
    p.add_argument("--pack-root", default=r"C:\dev\repos\context-reviewer-packs", help="External packs root directory.")
    p.add_argument("--subject", required=True, help="Subject folder name, e.g. sean_roy")
    p.add_argument("--run-id", required=True, help="Run identifier, e.g. 2026-02-06T1954Z")
    p.add_argument("--repo-root", default=str(Path.cwd()), help="Repository root (default: current working directory).")
    p.add_argument("--globs", nargs="*", default=DEFAULT_GLOBS, help="Glob patterns to include (relative to repo root).")
    p.add_argument("--dry-run", action="store_true", help="Compute manifest but do not copy files.")
    return p.parse_args()


def main() -> int:
    args = parse_args()
    repo_root = Path(args.repo_root).resolve()

    pack_root = Path(args.pack_root).resolve()
    pack_dir = pack_root / args.subject / args.run_id
    files_dir = pack_dir / "files"
    manifest_path = pack_dir / "manifest.json"

    if pack_dir.exists():
        raise SystemExit(f"[!] Pack directory already exists: {pack_dir}")

    # Collect files deterministically
    abs_files = list(_iter_globbed_files(repo_root, list(args.globs)))
    rel_files: List[Tuple[str, Path]] = [(_to_relpath(repo_root, p), p) for p in abs_files]
    rel_files.sort(key=lambda t: t[0])

    entries: List[FileEntry] = []
    total_bytes = 0

    for rel, p in rel_files:
        size = p.stat().st_size
        digest = _sha256_file(p)
        entries.append(FileEntry(relpath=rel, bytes=size, sha256=digest))
        total_bytes += size

    created_utc = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")

    manifest = {
        "pack_version": PACK_VERSION,
        "subject": args.subject,
        "run_id": args.run_id,
        "created_utc": created_utc,
        "source_repo_root": str(repo_root),
        "included_globs": list(args.globs),
        "file_count": len(entries),
        "total_bytes": total_bytes,
        "files": [e.__dict__ for e in entries],
    }

    # Create dirs + write manifest + copy
    pack_dir.mkdir(parents=True, exist_ok=False)

    # Write manifest first (even in dry run); it’s useful as an inventory
    manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=False) + "\n", encoding="utf-8")

    if not args.dry_run:
        for e in entries:
            src = repo_root / Path(e.relpath)
            dst = files_dir / Path(e.relpath)
            _copy_file(src, dst)

    print(f"[i] Pack created: {pack_dir}")
    print(f"[i] Manifest:     {manifest_path}")
    print(f"[i] Files:        {len(entries)} ({total_bytes} bytes){' [dry-run]' if args.dry_run else ''}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
