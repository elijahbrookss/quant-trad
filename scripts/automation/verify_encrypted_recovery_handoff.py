#!/usr/bin/env python3
"""Verify a copied QT recovery pair on the receiving host without touching source."""

from __future__ import annotations

import argparse
from functools import partial
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
from tempfile import TemporaryDirectory
from time import monotonic
from types import SimpleNamespace

import ijson

from market_data.fact_archive import archive_evidence_hash
from portal.backend.service.storage.incremental_recovery import _binary, _keys, _root_label, _run_native
from portal.backend.service.storage.maintenance_runtime import _unique_fields
from portal.backend.service.storage.recovery_copies import MAX_RECOVERY_OBJECTS, _NAME
from scripts.automation.verify_canonical_recovery_archive import _sha256, _verify_encrypted_inventory


_VERSION = "qt.encrypted_recovery_repository_verification.v1"
_WAL = re.compile(r"[0-9A-F]{24}")
_VERIFY_BACKUP = re.compile(r"  backup: ([^,]+), status: valid, total files checked: ([0-9]+), total valid files: ([0-9]+)")


def _bound_json(path, expected_hash):
    path = Path(path)
    if not _sha256(expected_hash) or path.resolve(strict=True) != path or not path.is_file():
        raise ValueError("recovery_handoff_evidence_path_or_hash_invalid")
    with path.open("rb") as stream:
        encoded = stream.read(65537)
    if len(encoded) > 65536 or hashlib.sha256(encoded).hexdigest() != expected_hash:
        raise ValueError("recovery_handoff_evidence_hash_mismatch")
    return json.loads(encoded, object_pairs_hook=_unique_fields)


def _directory(path):
    path = Path(path)
    if path.parent == path or path.resolve(strict=True) != path or not path.is_dir():
        raise ValueError("recovery_handoff_directory_invalid")
    return path


def _selected_backup(output, receipt):
    def require(valid):
        if not valid:
            raise RuntimeError("recovery_handoff_pgbackrest_backup_mismatch")

    rows = json.loads(output, object_pairs_hook=_unique_fields)
    require(isinstance(rows, list) and len(rows) == 1)
    stanza = rows[0]
    require(stanza["name"] == "qt" and stanza["cipher"] == "aes-256-cbc"
            and stanza["status"]["code"] == 0 and len(stanza["db"]) == 1 and len(stanza["repo"]) == 1)
    database = stanza["db"][0]
    require(str(database["system-id"]) == receipt["database_identity"].split("/")[0]
            and str(database["version"]) == "15" and type(database["id"]) is int and database["id"] > 0)
    backups = {row["label"]: row for row in stanza["backup"]}
    label = receipt["database_label"]
    selected = backups.get(label, {})
    require(len(backups) == len(stanza["backup"]) and selected.get("type") == receipt["database_type"]
            and selected.get("annotation", {}).get("qt-generation") == receipt["name"])
    chain = {label, _root_label(label), *(selected.get("reference") or ())}
    require(all(_root_label(item) == _root_label(label) and item in backups
                and backups[item].get("error") is False and backups[item]["database"]["id"] == database["id"]
                for item in chain))
    return selected, database["id"], sorted(chain)


def _verified_backup(output, label, database_id, expected_wal):
    lines = output.decode("ascii").splitlines()
    rows = [match.groups() for line in lines if (match := _VERIFY_BACKUP.fullmatch(line))]
    selected = [(total, valid) for name, total, valid in rows if name == label]
    archive = f"  archiveId: 15-{database_id}, total WAL checked: {expected_wal}, total valid WAL: {expected_wal}"
    if (lines[:2] != ["stanza: qt", "status: ok"] or len(selected) != 1
            or lines.count(archive) != 1 or selected[0][0] != selected[0][1] or int(selected[0][0]) <= 0):
        raise RuntimeError("recovery_handoff_pgbackrest_verify_failed")
    return int(selected[0][0])


def _wal_range(first, last, segment_bytes, limit):
    per_log = (1 << 32) // segment_bytes
    if not all(isinstance(name, str) and _WAL.fullmatch(name) for name in (first, last)):
        raise ValueError("recovery_handoff_wal_name_invalid")
    timeline = first[:8]
    if (last[:8] != timeline or int(timeline, 16) == 0
            or any(int(name[16:], 16) >= per_log for name in (first, last))):
        raise ValueError("recovery_handoff_wal_timeline_or_segment_invalid")
    start, stop = (int(name[8:16], 16) * per_log + int(name[16:], 16) for name in (first, last))
    if not 1 <= stop - start + 1 <= limit:
        raise ValueError("recovery_handoff_wal_range_outside_budget")
    return [f"{timeline}{index // per_log:08X}{index % per_log:08X}" for index in range(start, stop + 1)]


def _archive_filenames(database_repo, archive_id, segments, check):
    current_parent = None
    for segment in segments:
        check()
        parent = database_repo / "archive" / "qt" / archive_id / segment[:16]
        if parent != current_parent:
            _directory(parent)
            current_parent = parent
            files = {}
            with os.scandir(parent) as entries:
                for index, entry in enumerate(entries):
                    check()
                    if index >= 4096:
                        raise ValueError("recovery_handoff_wal_archive_directory_outside_budget")
                    match = re.fullmatch(r"([0-9A-F]{24})-([0-9a-f]{40})(?:\.(?:zst|gz|bz2|lz4))?", entry.name)
                    if match:
                        files.setdefault(match[1], []).append((match[2], entry.is_file(follow_symlinks=False)))
        found = files.get(segment, [])
        if len(found) != 1 or not found[0][1]:
            raise RuntimeError(f"recovery_handoff_wal_segment_missing_or_ambiguous:{segment}")
        yield segment, found[0][0]


def _anchor_wal(path, expected_hash, receipt, stop):
    anchor = _bound_json(path, expected_hash)
    database = anchor["database"]
    segment = anchor["wal_segment"]
    if (f"{database['system_identifier']}/{database['database_oid']}" != receipt["database_identity"]
            or anchor["storage_layout"] != receipt["storage_layout"] or database.get("allow_connections") is not False
            or type(anchor["source_commit_seq"]) is not int or anchor["source_commit_seq"] < 0
            or not re.fullmatch(r"qt_original_[0-9a-f]{32}", anchor["restore_name"])
            or not _WAL.fullmatch(segment) or type(anchor["timeline"]) is not int
            or anchor["timeline"] != int(segment[:8], 16) or segment < stop):
        raise ValueError("recovery_handoff_named_anchor_mismatch")
    return segment


def _snapshot_owner(run, command, receipt):
    owner = {"hostname": "", "tags": []}
    events = ijson.sendable_list()
    parser = ijson.parse_coro(events)
    pending_bytes = 0

    def consume(chunk):
        nonlocal pending_bytes
        parser.send(chunk)
        pending_bytes = 0 if events else pending_bytes + len(chunk)
        if pending_bytes > 65536:
            raise ValueError("recovery_handoff_restic_snapshot_token_limit")
        for prefix, event, value in events:
            if prefix == "hostname" and event == "string":
                owner["hostname"] = value
            if prefix == "tags.item" and event == "string":
                owner["tags"].append(value)
                if len(owner["tags"]) > 1:
                    raise ValueError("recovery_handoff_restic_snapshot_owner_mismatch")
        events.clear()

    try:
        run([*command, "cat", "snapshot", receipt["archive_snapshot"]], stdout_consumer=consume)
        parser.close()
    except ijson.JSONError:
        raise ValueError("recovery_handoff_restic_snapshot_invalid") from None
    expected_host = "qt-" + hashlib.sha256(receipt["database_identity"].encode()).hexdigest()[:32]
    if owner != {"hostname": expected_host, "tags": [receipt["name"]]}:
        raise RuntimeError("recovery_handoff_restic_snapshot_owner_mismatch")


def verify_handoff(*, repository, receipt_sha256, archive_restore, pgbackrest, restic,
                   database_key, archive_key, timeout_seconds, max_archive_bytes, max_archive_objects,
                   wal_segment_bytes, max_wal_segments, reserve_bytes=0, anchor=None,
                   anchor_sha256=None, isolated_pgdata=None):
    """Read an immutable receiving copy; restore only archives into the empty supplied directory."""
    if (type(timeout_seconds) is not int or not 1 <= timeout_seconds <= 86400
            or type(max_archive_objects) is not int or not 1 <= max_archive_objects <= MAX_RECOVERY_OBJECTS
            or type(max_archive_bytes) is not int or not 0 <= max_archive_bytes <= 2**63 - 1
            or type(reserve_bytes) is not int or not 0 <= reserve_bytes <= 2**63 - 1
            or type(wal_segment_bytes) is not int or not 2**20 <= wal_segment_bytes <= 2**30
            or wal_segment_bytes & (wal_segment_bytes - 1)
            or type(max_wal_segments) is not int or not 1 <= max_wal_segments <= 65536
            or len({anchor is None, anchor_sha256 is None, isolated_pgdata is None}) != 1):
        raise ValueError("recovery_handoff_limits_or_anchor_invalid")
    repository, archive_restore = map(_directory, (repository, archive_restore))
    database_repo, archive_repo = map(_directory, (repository / "database", repository / "archives"))
    if (repository.is_relative_to(archive_restore) or archive_restore.is_relative_to(repository)
            or archive_restore.stat().st_uid != os.getuid() or archive_restore.stat().st_mode & 0o077
            or any(archive_restore.iterdir())):
        raise ValueError("recovery_handoff_requires_private_empty_archive_restore")
    pair = repository / "complete.json"
    receipt = _bound_json(pair, receipt_sha256)
    if (receipt.get("schema_version") != "qt.encrypted_recovery_pair.v1" or receipt.get("restore_endpoint") != "immediate"
            or not re.fullmatch(r"[1-9][0-9]*/[1-9][0-9]*", receipt.get("database_identity", ""))
            or not _NAME.fullmatch(receipt.get("name", "")) or not _sha256(receipt.get("archive_snapshot"))
            or not _sha256(receipt.get("inventory_sha256"))
            or type(receipt.get("archive_objects")) is not int or not 0 <= receipt["archive_objects"] <= max_archive_objects
            or type(receipt.get("archive_bytes")) is not int or not 0 <= receipt["archive_bytes"] <= max_archive_bytes):
        raise ValueError("recovery_handoff_pair_outside_scope")
    _root_label(receipt["database_label"])
    free = shutil.disk_usage(archive_restore).free
    if free < reserve_bytes + receipt["archive_bytes"]:
        raise RuntimeError("recovery_handoff_archive_restore_space_insufficient")
    pgbackrest, restic = map(_binary, (pgbackrest, restic))
    pg_secret, restic_secret = _keys(SimpleNamespace(database_key_path=Path(database_key), archive_key_path=Path(archive_key)), repository)
    root_before = repository.stat()
    deadline = monotonic() + timeout_seconds

    def check():
        if monotonic() >= deadline:
            raise TimeoutError("recovery_handoff_time_budget_exceeded")
        current = repository.stat()
        if (current.st_dev, current.st_ino) != (root_before.st_dev, root_before.st_ino):
            raise RuntimeError("recovery_handoff_repository_changed")
        remaining = shutil.disk_usage(archive_restore).free
        if remaining < reserve_bytes:
            raise RuntimeError("recovery_handoff_archive_restore_space_exceeded")
        if max(0, free - remaining) > max_archive_bytes:
            raise RuntimeError("recovery_handoff_archive_restore_byte_budget_exceeded")

    with TemporaryDirectory(prefix="qt-recovery-verification-") as work:
        env = {"PATH": os.defpath, "LC_ALL": "C", "HOME": work, "TMPDIR": work,
               "PGBACKREST_REPO1_CIPHER_PASS": pg_secret, "RESTIC_PASSWORD": restic_secret}
        run = partial(_run_native, cwd=work, env=env, deadline=deadline, check=check)
        tools = {name: run([str(binary), "version"]).decode("ascii").strip()
                 for name, binary in (("pgbackrest", pgbackrest), ("restic", restic))}
        if (tools != receipt["tools"] or tools["pgbackrest"] != "pgBackRest 2.59.1"
                or not tools["restic"].startswith("restic 0.19.1 ")):
            raise ValueError("recovery_handoff_tools_changed")
        br = [str(pgbackrest), "--no-config", "--stanza=qt", "--repo1-cipher-type=aes-256-cbc",
              f"--repo1-path={database_repo}", "--log-level-file=off", "--log-level-console=off"]
        rs = [str(restic), "--repo", str(archive_repo), "--no-lock", "--no-cache", "--json", "--quiet"]
        selected, database_id, chain = _selected_backup(run([*br, f"--lock-path={work}", "--output=json", "info"]), receipt)
        first, stop = (selected["archive"][key] for key in ("start", "stop"))
        last = _anchor_wal(anchor, anchor_sha256, receipt, stop) if anchor is not None else stop
        segments = _wal_range(first, last, wal_segment_bytes, max_wal_segments)
        backup_segments = _wal_range(first, stop, wal_segment_bytes, max_wal_segments)
        entries = list(_archive_filenames(database_repo, f"15-{database_id}", segments, check))
        verified_files = _verified_backup(run([*br, "--output=text", "--verbose",
                                                "--set=" + receipt["database_label"], "verify"]),
                                          receipt["database_label"], database_id, len(backup_segments))
        if isolated_pgdata is not None:
            pgdata = _directory(isolated_pgdata)
            if (pgdata.is_relative_to(repository) or repository.is_relative_to(pgdata)
                    or (pgdata / "postmaster.pid").exists() or (pgdata / "postmaster.pid").is_symlink()
                    or not (pgdata / "backup_label").is_file() or (pgdata / "backup_label").is_symlink()):
                raise ValueError("recovery_handoff_offline_restored_pgdata_required")
            if shutil.disk_usage(work).free < wal_segment_bytes:
                raise RuntimeError("recovery_handoff_wal_work_space_insufficient")
            for segment, checksum in entries[len(backup_segments) - 1:]:
                destination = Path(work) / segment
                run([*br, f"--pg1-path={pgdata}", f"--lock-path={work}", "--archive-async=n",
                     "archive-get", segment, str(destination)])
                with destination.open("rb") as stream:
                    digest = hashlib.file_digest(stream, "sha1").hexdigest()
                if destination.stat().st_size != wal_segment_bytes or digest != checksum:
                    raise RuntimeError(f"recovery_handoff_wal_segment_checksum_or_size_mismatch:{segment}")
                destination.unlink()
        _snapshot_owner(run, rs, receipt)
        size = json.loads(run([*rs, "stats", receipt["archive_snapshot"], "--mode=restore-size"]),
                          object_pairs_hook=_unique_fields)["total_size"]
        if type(size) is not int or not receipt["archive_bytes"] <= size <= max_archive_bytes:
            raise ValueError("recovery_handoff_restic_restore_size_outside_budget")
        if shutil.disk_usage(archive_restore).free < reserve_bytes + size:
            raise RuntimeError("recovery_handoff_archive_restore_space_insufficient")
        run([*rs, "restore", receipt["archive_snapshot"], "--target", str(archive_restore), "--verify"])
        archives = _verify_encrypted_inventory(receipt, archive_restore, check=check)
        if _bound_json(pair, receipt_sha256) != receipt:
            raise RuntimeError("recovery_handoff_pair_changed")
        if anchor is not None and _anchor_wal(anchor, anchor_sha256, receipt, stop) != last:
            raise RuntimeError("recovery_handoff_anchor_changed")
    report = {"schema": _VERSION, "pair_receipt_sha256": receipt_sha256, "database_identity": receipt["database_identity"],
              "storage_layout": receipt["storage_layout"], "database_label": receipt["database_label"], "tools": tools,
              "archive_snapshot": receipt["archive_snapshot"], "database_chain": chain, "verified_backup_files": verified_files,
              "wal_start": first, "wal_backup_stop": stop, "wal_verified_through": last, "wal_segments": len(entries),
              "wal_inventory_sha256": archive_evidence_hash(entries), "anchor_sha256": anchor_sha256, **archives}
    return report | {"verification_sha256": archive_evidence_hash(report)}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("repository", "archive-restore", "pgbackrest", "restic", "database-key", "archive-key"):
        parser.add_argument("--" + name, required=True, type=Path)
    parser.add_argument("--receipt-sha256", required=True)
    for name in ("timeout-seconds", "max-archive-bytes", "max-archive-objects", "wal-segment-bytes", "max-wal-segments"):
        parser.add_argument("--" + name, required=True, type=int)
    parser.add_argument("--reserve-bytes", type=int, default=0)
    parser.add_argument("--anchor", type=Path)
    parser.add_argument("--anchor-sha256")
    parser.add_argument("--isolated-pgdata", type=Path)
    print(json.dumps(verify_handoff(**vars(parser.parse_args())), sort_keys=True))


if __name__ == "__main__":
    main()
