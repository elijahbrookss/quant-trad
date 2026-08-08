#!/usr/bin/env python3
"""Hash the exact backend source material copied into the runtime image."""

from __future__ import annotations

import argparse
import hashlib
import subprocess
from pathlib import Path, PurePosixPath


ROOTS = ("config", "src", "portal", "scripts/provenance")
ROOT_FILES = ("requirements.txt",)
IGNORED_PARTS = {"__pycache__", "node_modules", "dist", ".vite", ".npm-cache"}
IGNORED_SUFFIXES = {".pyc", ".pyo"}


def _included(path: str) -> bool:
    candidate = PurePosixPath(path)
    return not any(part in IGNORED_PARTS for part in candidate.parts) and (
        candidate.suffix not in IGNORED_SUFFIXES
    )


def _digest(files: list[tuple[str, bytes]]) -> str:
    digest = hashlib.sha256()
    for relative, content in sorted(files):
        encoded = relative.encode("utf-8")
        digest.update(len(encoded).to_bytes(8, "big"))
        digest.update(encoded)
        digest.update(len(content).to_bytes(8, "big"))
        digest.update(content)
    return digest.hexdigest()


def working_tree_hash(root: Path) -> str:
    files: list[tuple[str, bytes]] = []
    for name in ROOT_FILES:
        path = root / name
        if path.is_file() and _included(name):
            files.append((name, path.read_bytes()))
    for name in ROOTS:
        directory = root / name
        if not directory.exists():
            continue
        for path in directory.rglob("*"):
            if path.is_file():
                relative = path.relative_to(root).as_posix()
                if _included(relative):
                    files.append((relative, path.read_bytes()))
    return _digest(files)


def git_revision_hash(root: Path, revision: str) -> str:
    names = subprocess.run(
        [
            "git",
            "ls-tree",
            "-r",
            "--name-only",
            revision,
            "--",
            *ROOTS,
            *ROOT_FILES,
        ],
        cwd=root,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.splitlines()
    files = []
    for name in names:
        if not _included(name):
            continue
        content = subprocess.run(
            ["git", "show", f"{revision}:{name}"],
            cwd=root,
            check=True,
            capture_output=True,
        ).stdout
        files.append((name, content))
    return _digest(files)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=Path, default=Path.cwd())
    parser.add_argument("--git-revision")
    args = parser.parse_args()
    root = args.root.resolve()
    value = (
        git_revision_hash(root, args.git_revision)
        if args.git_revision
        else working_tree_hash(root)
    )
    print(value)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
