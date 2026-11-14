#!/usr/bin/env python3
"""Check the archive half of a recovered QT pgBackRest/Restic pair offline."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import re
import stat


def _sha256(value: object) -> bool:
    return isinstance(value, str) and re.fullmatch(r"[0-9a-f]{64}", value) is not None


def _path(root: Path, value: object, *, absolute: bool) -> Path:
    if not isinstance(value, str) or "\\" in value or "\x00" in value or (os.name == "nt" and ":" in value):
        raise ValueError("invalid recovery path")
    path = PurePosixPath(value)
    if path.is_absolute() != absolute or str(path) != value or ".." in path.parts:
        raise ValueError(f"invalid recovery path: {value}")
    parts = path.parts[1:] if absolute else path.parts
    if not parts or len(parts) > 64:
        raise ValueError(f"invalid recovery path depth: {value}")
    candidate = root.joinpath(*parts)
    if candidate.resolve(strict=True) != candidate or candidate.stat().st_dev != root.stat().st_dev:
        raise ValueError(f"restored path crosses an alias or filesystem boundary: {value}")
    return candidate


def _sha_file(path: Path) -> tuple[str, int]:
    digest = hashlib.sha256()
    before_open = path.lstat()
    if not stat.S_ISREG(before_open.st_mode):
        raise ValueError(f"restored object is not a regular file: {path}")
    with os.fdopen(os.open(path, os.O_RDONLY | getattr(os, "O_BINARY", 0) | getattr(os, "O_NOFOLLOW", 0)), "rb") as stream:
        before = os.fstat(stream.fileno())
        if not stat.S_ISREG(before.st_mode):
            raise ValueError(f"restored object is not a regular file: {path}")
        while chunk := stream.read(1024 * 1024):
            digest.update(chunk)
        after = os.fstat(stream.fileno())
    if len({(s.st_dev, s.st_ino, s.st_size, s.st_mtime_ns, s.st_ctime_ns)
            for s in (before_open, before, after, path.lstat())}) != 1:
        raise ValueError(f"restored object changed while reading: {path}")
    return digest.hexdigest(), before.st_size


def verify_archive(receipt_path: Path, restore_target: Path) -> dict:
    """Bind the restored inventory and all listed files to the completion receipt."""
    with receipt_path.open("rb") as stream:
        encoded_receipt = stream.read(65537)
    if len(encoded_receipt) > 65536:
        raise ValueError("paired recovery receipt is too large")
    receipt = json.loads(encoded_receipt)
    if receipt.get("schema_version") != "qt.encrypted_recovery_pair.v1":
        raise ValueError("unrecognized QT encrypted recovery pair")
    if receipt.get("restore_endpoint") != "immediate" or not receipt.get("database_label"):
        raise ValueError("paired database restore identity is missing")
    if not _sha256(receipt.get("archive_snapshot")) or not _sha256(receipt.get("inventory_sha256")):
        raise ValueError("paired archive identity or inventory hash is invalid")
    expected_count, expected_bytes = receipt.get("archive_objects"), receipt.get("archive_bytes")
    if type(expected_count) is not int or type(expected_bytes) is not int or min(expected_count, expected_bytes) < 0:
        raise ValueError("paired archive count or byte total is invalid")
    root = restore_target.resolve(strict=True)
    if root.parent == root:
        raise ValueError("an isolated restore directory is required")
    inventory = _path(root, receipt["inventory_snapshot_path"], absolute=True)
    if not inventory.is_file():
        raise ValueError("the restored inventory is absent")
    archive = root
    if expected_count:
        archive = _path(root, receipt["archive_source_root"], absolute=True)
        if not archive.is_dir():
            raise ValueError("the restored archive root is not a directory")
    digest = hashlib.sha256()
    seen = set()
    total = 0
    with inventory.open("rb") as stream:
        while line := stream.readline(65537):
            if len(line) > 65536 or len(seen) >= expected_count or not line.endswith(b"\n"):
                raise ValueError("unexpected inventory row count or line length")
            digest.update(line)
            item = json.loads(line)
            key, size, sha256 = item["object_key"], item["bytes"], item["sha256"]
            if not isinstance(key, str) or type(size) is not int or size <= 0 or not _sha256(sha256):
                raise ValueError("invalid restored archive inventory object")
            if key in seen:
                raise ValueError(f"duplicate restored archive object: {key}")
            object_path = _path(archive, key, absolute=False)
            actual_digest, actual_size = _sha_file(object_path)
            if actual_size != size or actual_digest != sha256:
                raise ValueError(f"restored archive object failed size or hash check: {key}")
            seen.add(key)
            total += size
    if len(seen) != expected_count or total != expected_bytes or digest.hexdigest() != receipt["inventory_sha256"]:
        raise ValueError("archive inventory digest, object count, or byte total does not match the pair")
    return {
        "database_label": receipt["database_label"],
        "archive_snapshot": receipt["archive_snapshot"],
        "inventory_sha256": digest.hexdigest(),
        "verified_archive_objects": len(seen),
        "verified_archive_bytes": total,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--receipt", type=Path, required=True, help="independently copied complete.json")
    parser.add_argument("--restore-target", type=Path, required=True, help="isolated Restic restore root")
    args = parser.parse_args()
    try:
        result = verify_archive(args.receipt, args.restore_target)
    except (OSError, ValueError, AttributeError, KeyError, TypeError) as error:
        parser.exit(1, f"archive verification failed: {error}\n")
    print(json.dumps(result, sort_keys=True))


if __name__ == "__main__":
    main()
