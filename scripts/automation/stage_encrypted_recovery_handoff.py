#!/usr/bin/env python3
"""Copy QT's immutable encrypted repositories to an already authorized receiving mount."""

from __future__ import annotations

import argparse
from contextlib import contextmanager
from dataclasses import dataclass
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import stat
from time import monotonic

from portal.backend.service.storage.incremental_recovery import _root_label
from portal.backend.service.storage.maintenance_runtime import _unique_fields
from portal.backend.service.storage.recovery_copies import _NAME, _json_write, _sync
from scripts.automation.verify_encrypted_recovery_handoff import _bound_json, _directory, _sha256


_SCHEMA = "qt.encrypted_recovery_transfer.v1"
_WAL_FILE = re.compile(r"database/archive/qt/15-[1-9][0-9]*/[0-9A-F]{16}/"
                       r"[0-9A-F]{24}-[0-9a-f]{40}(?:\.(?:zst|gz|bz2|lz4))?")
_WAL_DIRECTORY = re.compile(r"database/archive/qt/15-[1-9][0-9]*/[0-9A-F]{16}")
_MANIFEST = "encrypted-copy.jsonl"
_PUBLISHED = "encrypted-copy.json"
_CHUNK = 1024**2


@dataclass(frozen=True)
class TransferLimits:
    seconds: int
    bytes: int
    files: int
    reserve_bytes: int = 0

    def __post_init__(self):
        if any(type(value) is not int or not low <= value <= high for value, low, high in (
            (self.seconds, 1, 86400), (self.bytes, 1, 2**63 - 1),
            (self.files, 1, 2_000_000), (self.reserve_bytes, 0, 2**63 - 1),
        )):
            raise ValueError("recovery_transfer_invalid_limits")


def _identity(info):
    return info.st_dev, info.st_ino


def _stamp(info):
    return (*_identity(info), info.st_size, info.st_mtime_ns, info.st_ctime_ns, info.st_nlink)


def _private_root(path):
    path = _directory(path)
    info = path.stat()
    if info.st_uid != os.getuid() or info.st_mode & 0o077:
        raise ValueError("recovery_transfer_private_directory_required")
    return path, _identity(info)


@contextmanager
def _source_hold(root):
    name = root / "writer.lock"
    with os.fdopen(os.open(name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK), "rb") as descriptor:
        info = os.fstat(descriptor.fileno())
        if (not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid() or info.st_mode & 0o077
                or info.st_nlink != 1 or info.st_dev != root.stat().st_dev):
            raise ValueError("recovery_transfer_source_lock_invalid")
        try:
            fcntl.flock(descriptor, fcntl.LOCK_SH | fcntl.LOCK_NB)
        except BlockingIOError:
            raise RuntimeError("recovery_transfer_source_writer_busy") from None
        if _identity(name.lstat()) != _identity(info):
            raise RuntimeError("recovery_transfer_source_lock_changed")
        yield
        if _identity(name.lstat()) != _identity(info):
            raise RuntimeError("recovery_transfer_source_lock_changed")


def _safe_member(path, device):
    info = path.lstat()
    if info.st_dev != device or info.st_uid != os.getuid():
        raise ValueError("recovery_transfer_member_wrong_device_or_owner")
    return info


def _inventory(root, device, limits, check, *, source=False):
    directories, files = set(), {}
    total = 0
    pending = [root / "archives", root / "database"]
    while pending:
        check()
        directory = pending.pop()
        relative = directory.relative_to(root).as_posix()
        if not stat.S_ISDIR(_safe_member(directory, device).st_mode):
            raise ValueError("recovery_transfer_member_not_native_directory")
        directories.add(relative)
        with os.scandir(directory) as entries:
            for entry in entries:
                check()
                path = Path(entry.path)
                key = path.relative_to(root).as_posix()
                if (len(key.encode()) > 2048 or len(path.relative_to(root).parts) > 32
                        or any(ord(char) < 32 or char == "\\" for char in key)):
                    raise ValueError("recovery_transfer_member_path_invalid")
                if source and (relative == "archives/locks" or
                               (key.endswith(".pgbackrest.tmp") and _WAL_FILE.fullmatch(key.removesuffix(".pgbackrest.tmp")))):
                    continue
                info = _safe_member(path, device)
                if source and key == "database/backup/qt/latest" and stat.S_ISLNK(info.st_mode):
                    _root_label(os.readlink(path))
                    continue
                if stat.S_ISDIR(info.st_mode):
                    pending.append(path)
                    if len(pending) + len(directories) > limits.files + 20:
                        raise ValueError("recovery_transfer_member_budget_exceeded")
                    continue
                if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
                    raise ValueError("recovery_transfer_member_not_regular_file")
                files[key] = _stamp(info)
                total += info.st_size
                if len(files) > limits.files or total > limits.bytes:
                    raise ValueError("recovery_transfer_member_budget_exceeded")
    return directories, files, total


def _file(path, expected, check, copy_to=None):
    digest = hashlib.sha256()
    with os.fdopen(os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK), "rb") as source:
        if _stamp(os.fstat(source.fileno())) != expected:
            raise RuntimeError("recovery_transfer_file_changed")
        size = 0
        while data := source.read(_CHUNK):
            check()
            size += len(data)
            if size > expected[2]:
                raise RuntimeError("recovery_transfer_file_changed")
            digest.update(data)
            if copy_to is not None:
                copy_to.write(data)
        if size != expected[2] or _stamp(os.fstat(source.fileno())) != expected or _stamp(path.lstat()) != expected:
            raise RuntimeError("recovery_transfer_file_changed")
    return {"bytes": size, "sha256": digest.hexdigest()}


def _encode(evidence):
    return (json.dumps(evidence, sort_keys=True, separators=(",", ":")) + "\n").encode()


def _append_only_wal(before, after):
    previous_directories, previous_files, _ = before
    directories, files, _ = after
    new = files.keys() - previous_files.keys()
    if (not previous_directories <= directories or any(not _WAL_DIRECTORY.fullmatch(path) for path in directories - previous_directories)
            or any(files.get(key) != value for key, value in previous_files.items())
            or any(not _WAL_FILE.fullmatch(key) for key in new)):
        raise RuntimeError("recovery_transfer_source_inventory_changed")


def _manifest_entry(key, evidence):
    return _encode({"path": key, **evidence})


def stage_handoff(*, source, receiving, generation, receipt_sha256, limits):
    """Publish a ciphertext manifest last. An interrupted receiving directory remains unpublished."""
    started = monotonic()
    source, source_id = _private_root(source)
    receiving, receiving_id = _private_root(receiving)
    if (source_id[0] == receiving_id[0] or source.is_relative_to(receiving) or receiving.is_relative_to(source)
            or any(receiving.iterdir()) or not isinstance(generation, str) or not _NAME.fullmatch(generation)):
        raise ValueError("recovery_transfer_distinct_empty_receiving_mount_required")

    def check():
        if monotonic() - started >= limits.seconds:
            raise TimeoutError("recovery_transfer_time_budget_exceeded")
        if _identity(source.lstat()) != source_id or _identity(receiving.lstat()) != receiving_id:
            raise RuntimeError("recovery_transfer_mount_changed")
        if shutil.disk_usage(receiving).free < limits.reserve_bytes:
            raise RuntimeError("recovery_transfer_receiving_reserve_exhausted")

    with _source_hold(source):
        directory, generation_id = _private_root(source / generation)
        if generation_id[0] != source_id[0]:
            raise ValueError("recovery_transfer_source_generation_wrong_device")
        receipt = _bound_json(directory / "complete.json", receipt_sha256)
        if not isinstance(receipt, dict):
            raise ValueError("recovery_transfer_source_receipt_invalid")
        owned = {"schema_version": "qt.encrypted_recovery_pair.v1", "name": generation,
                 "database_identity": receipt["database_identity"], "filesystem_uuid": receipt["filesystem_uuid"]}
        owner_path = directory / "owner.json"
        if not stat.S_ISREG(_safe_member(owner_path, source_id[0]).st_mode):
            raise ValueError("recovery_transfer_source_generation_owner_invalid")
        with os.fdopen(os.open(owner_path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK), "rb") as marker:
            owner = json.loads(marker.read(4097), object_pairs_hook=_unique_fields)
        if owner != owned or any(receipt.get(key) != value for key, value in owned.items()):
            raise ValueError("recovery_transfer_source_generation_owner_invalid")
        before = _inventory(source, source_id[0], limits, check, source=True)
        directories, files, total = before
        manifest_ceiling = sum(len(_manifest_entry(key, {"bytes": expected[2], "sha256": "0" * 64}))
                               for key, expected in files.items())
        if not files or shutil.disk_usage(receiving).free < total + manifest_ceiling + limits.reserve_bytes:
            raise RuntimeError("recovery_transfer_receiving_space_insufficient")
        for key in sorted(directories):
            (receiving / key).mkdir(mode=0o700)
        manifest_digest = hashlib.sha256()
        with (receiving / _MANIFEST).open("xb") as manifest:
            os.fchmod(manifest.fileno(), 0o600)
            for key, expected in sorted(files.items()):
                check()
                with (receiving / key).open("xb") as copy:
                    os.fchmod(copy.fileno(), 0o600)
                    evidence = _file(source / key, expected, check, copy)
                    copy.flush()
                    os.fsync(copy.fileno())
                line = _manifest_entry(key, evidence)
                manifest.write(line)
                manifest_digest.update(line)
            manifest.flush()
            os.fsync(manifest.fileno())
        _append_only_wal(before, _inventory(source, source_id[0], limits, check, source=True))
        pair = directory / "complete.json"
        pair_info = _safe_member(pair, source_id[0])
        if not stat.S_ISREG(pair_info.st_mode) or not 1 <= pair_info.st_size <= 65536:
            raise ValueError("recovery_transfer_source_receipt_invalid")
        pair_stamp = _stamp(pair_info)
        with (receiving / "complete.json").open("xb") as copy:
            os.fchmod(copy.fileno(), 0o600)
            if _file(pair, pair_stamp, check, copy)["sha256"] != receipt_sha256:
                raise RuntimeError("recovery_transfer_source_receipt_changed")
            copy.flush()
            os.fsync(copy.fileno())
        for key in sorted(directories, reverse=True):
            _sync(receiving / key)
        check()
    report = {"schema": _SCHEMA, "pair_receipt_sha256": receipt_sha256, "ciphertext_manifest_sha256": manifest_digest.hexdigest(),
              "encrypted_files": len(files), "encrypted_bytes": total}
    pending = receiving / ("." + _PUBLISHED)
    _json_write(pending, report)
    check()
    _sync(receiving)
    pending.rename(receiving / _PUBLISHED)
    _sync(receiving)
    return report | {"transfer_receipt_sha256": hashlib.sha256(_encode(report)).hexdigest()}


def verify_staged_handoff(*, receiving, transfer_receipt_sha256, limits):
    """Rehash every shipped encrypted file from the receiving mount."""
    started = monotonic()
    receiving, receiver_id = _private_root(receiving)

    def check():
        if monotonic() - started >= limits.seconds:
            raise TimeoutError("recovery_transfer_time_budget_exceeded")
        if _identity(receiving.lstat()) != receiver_id:
            raise RuntimeError("recovery_transfer_mount_changed")

    report = _bound_json(receiving / _PUBLISHED, transfer_receipt_sha256)
    if (not isinstance(report, dict) or report.get("schema") != _SCHEMA
            or not _sha256(report.get("ciphertext_manifest_sha256"))):
        raise ValueError("recovery_transfer_invalid_publication")
    _bound_json(receiving / "complete.json", report["pair_receipt_sha256"])
    before = _inventory(receiving, receiver_id[0], limits, check)
    _, files, total = before
    if (report.get("encrypted_files"), report.get("encrypted_bytes")) != (len(files), total):
        raise RuntimeError("recovery_transfer_receiving_inventory_changed")
    digest = hashlib.sha256()
    manifest_path = receiving / _MANIFEST
    if not stat.S_ISREG(_safe_member(manifest_path, receiver_id[0]).st_mode):
        raise ValueError("recovery_transfer_ciphertext_manifest_invalid")
    with os.fdopen(os.open(manifest_path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK), "rb") as manifest:
        for key, stamp in sorted(files.items()):
            check()
            line = manifest.readline(8193)
            if len(line) > 8192 or not line.endswith(b"\n"):
                raise ValueError("recovery_transfer_ciphertext_manifest_invalid")
            digest.update(line)
            if line != _manifest_entry(key, _file(receiving / key, stamp, check)):
                raise RuntimeError("recovery_transfer_receiving_ciphertext_changed")
        if manifest.read(1) or digest.hexdigest() != report["ciphertext_manifest_sha256"]:
            raise RuntimeError("recovery_transfer_receiving_manifest_changed")
    if _inventory(receiving, receiver_id[0], limits, check) != before:
        raise RuntimeError("recovery_transfer_receiving_inventory_changed")
    return report | {"transfer_receipt_sha256": transfer_receipt_sha256}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    operation = parser.add_subparsers(dest="operation", required=True)
    stage = operation.add_parser("stage")
    stage.add_argument("--source", required=True, type=Path)
    stage.add_argument("--generation", required=True)
    stage.add_argument("--receipt-sha256", required=True)
    verify = operation.add_parser("verify")
    verify.add_argument("--transfer-receipt-sha256", required=True)
    for command in (stage, verify):
        command.add_argument("--receiving", required=True, type=Path)
        command.add_argument("--max-bytes", required=True, type=int)
        command.add_argument("--max-files", required=True, type=int)
        command.add_argument("--timeout-seconds", required=True, type=int)
    stage.add_argument("--reserve-bytes", type=int, default=0)
    args = vars(parser.parse_args())
    limits = TransferLimits(args.pop("timeout_seconds"), args.pop("max_bytes"), args.pop("max_files"), args.pop("reserve_bytes", 0))
    command = stage_handoff if args.pop("operation") == "stage" else verify_staged_handoff
    print(json.dumps(command(**args, limits=limits), sort_keys=True))


if __name__ == "__main__":
    main()
