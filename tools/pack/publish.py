#!/usr/bin/env python3
from __future__ import annotations

import argparse
import shutil
from pathlib import Path


DEFAULT_EXCLUDE_NAMES = {
    "GOVERNANCE.md",
    "RUNNING.md",
    "NEXT_STEP.md",
    "ORCHESTRATOR_SPEC.md",
    "STEP5_SPEC.md",
    "CLAUDE_SESSION_HANDOFF.md",
}

DEFAULT_EXCLUDE_SUFFIXES = (
    ".code-workspace",
)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Publish docs from a frozen pack into the repo docs/ directory.")
    p.add_argument("--pack-root", default=r"C:\dev\repos\context-reviewer-packs", help="External packs root directory.")
    p.add_argument("--subject", required=True, help="Subject folder name, e.g. sean_roy")
    p.add_argument("--run-id", required=True, help="Run identifier, e.g. 2026-02-06T1954Z")
    p.add_argument("--repo-root", default=str(Path.cwd()), help="Repository root (default: current working directory).")
    p.add_argument(
        "--mode",
        choices=["publish", "preview"],
        default="publish",
        help="publish=overwrite repo docs/. preview=write to <repo>/fb_extract_out/preview_docs/<run-id>/",
    )
    p.add_argument("--clean", action="store_true", help="Clean destination docs folder before copy.")
    p.add_argument(
        "--force-all",
        action="store_true",
        help="Copy all docs from pack, including governance/process files.",
    )
    return p.parse_args()


def _should_exclude(path: Path, force_all: bool) -> bool:
    if force_all:
        return False
    name = path.name
    if name in DEFAULT_EXCLUDE_NAMES:
        return True
    for suf in DEFAULT_EXCLUDE_SUFFIXES:
        if name.endswith(suf):
            return True
    return False


def _copy_tree(src: Path, dst: Path, *, force_all: bool) -> None:
    # shutil.copytree requires dst not exist; we want merge semantics.
    for p in src.rglob("*"):
        if _should_exclude(p, force_all):
            continue
        rel = p.relative_to(src)
        out = dst / rel
        if p.is_dir():
            out.mkdir(parents=True, exist_ok=True)
        else:
            out.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(p, out)


def main() -> int:
    args = parse_args()
    repo_root = Path(args.repo_root).resolve()

    pack_dir = Path(args.pack_root).resolve() / args.subject / args.run_id
    pack_docs = pack_dir / "files" / "docs"

    if not pack_docs.exists() or not pack_docs.is_dir():
        raise SystemExit(f"[!] Pack docs not found: {pack_docs}")

    if args.mode == "publish":
        dst_docs = repo_root / "docs"
    else:
        dst_docs = repo_root / "fb_extract_out" / "preview_docs" / args.run_id

    if args.clean and dst_docs.exists():
        shutil.rmtree(dst_docs)

    dst_docs.mkdir(parents=True, exist_ok=True)
    _copy_tree(pack_docs, dst_docs, force_all=args.force_all)

    print(f"[i] Source: {pack_docs}")
    print(f"[i] Dest:   {dst_docs}")
    print(f"[i] Mode:   {args.mode}")
    print(f"[i] Force:  {args.force_all}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
