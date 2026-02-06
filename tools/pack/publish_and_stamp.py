#!/usr/bin/env python3
from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path


def run(cmd: list[str]) -> None:
    print("[cmd] " + " ".join(cmd))
    r = subprocess.run(cmd)
    if r.returncode != 0:
        raise SystemExit(r.returncode)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Publish docs from pack, write pack_ref, and verify.")
    p.add_argument("--subject", required=True)
    p.add_argument("--run-id", required=True)
    p.add_argument("--pack-root", default=r"C:\dev\repos\context-reviewer-packs")
    p.add_argument("--repo-root", default=str(Path.cwd()))
    p.add_argument("--clean", action="store_true", help="Clean destination docs folder before copy.")
    p.add_argument("--force-all", action="store_true", help="Pass through to publish.py to overwrite all docs.")
    p.add_argument("--strict", action="store_true", help="Verify in strict mode (recommended).")
    return p.parse_args()


def main() -> int:
    args = parse_args()
    repo_root = Path(args.repo_root).resolve()

    py = sys.executable
    publish = str(repo_root / "tools" / "pack" / "publish.py")
    stamp = str(repo_root / "tools" / "pack" / "write_pack_ref.py")
    verify = str(repo_root / "tools" / "pack" / "verify_docs_ref.py")

    publish_cmd = [
        py, publish,
        "--pack-root", args.pack_root,
        "--subject", args.subject,
        "--run-id", args.run_id,
        "--mode", "publish",
    ]
    if args.clean:
        publish_cmd.append("--clean")
    if args.force_all:
        publish_cmd.append("--force-all")

    stamp_cmd = [
        py, stamp,
        "--subject", args.subject,
        "--run-id", args.run_id,
        "--docs-root", str(repo_root / "docs"),
        "--out", str(repo_root / "docs" / "pack_ref.json"),
    ]

    verify_cmd = [
        py, verify,
        "--ref", str(repo_root / "docs" / "pack_ref.json"),
        "--docs-root", str(repo_root / "docs"),
    ]
    if args.strict:
        verify_cmd.append("--strict")

    run(publish_cmd)
    run(stamp_cmd)
    run(verify_cmd)

    print("[OK] publish + stamp + verify complete.")
    print("[i] Next: commit docs/ + docs/pack_ref.json together.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
