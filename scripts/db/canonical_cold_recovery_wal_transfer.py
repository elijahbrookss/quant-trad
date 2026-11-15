"""Append bounded encrypted WAL to the mounted receiving copy under a live SQL fence."""

from __future__ import annotations

from contextlib import contextmanager
import fcntl
import hashlib
import os
from pathlib import Path
import re
import shutil
import stat
from tempfile import NamedTemporaryFile
from time import monotonic, sleep

from sqlalchemy import text

from market_data.fact_archive import archive_evidence_hash
from portal.backend.service.storage.recovery_copies import _snapshot_layout, _sync
from portal.backend.service.storage.repos.market_data import PostgresMarketDataRepository
from scripts.automation.stage_encrypted_recovery_handoff import (
    _PUBLISHED, _SCHEMA, _encode, _file, _identity, _private_root, _safe_member, _source_hold, _stamp,
)
from scripts.automation.verify_encrypted_recovery_handoff import _anchor_wal, _archive_filenames, _bound_json, _wal_range
from scripts.db.canonical_cold_database_fence import DatabaseFence, _DATABASE, _database
from scripts.db.fact_header_v2_capture import _bounded_step


_VERSION = "qt.encrypted_recovery_wal_transfer.v1"
_RECEIPT = "encrypted-wal-transfer.json"
_MAX_SEGMENTS = 128


@contextmanager
def _receiving_lock(root):
    flags = os.O_CREAT | os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK
    with os.fdopen(os.open(root / ".wal-transfer.lock", flags, 0o600), "rb") as lock:
        info = os.fstat(lock.fileno())
        if not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid() or info.st_mode & 0o077 or info.st_nlink != 1:
            raise ValueError("canonical_recovery_wal_receiving_lock_invalid")
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise RuntimeError("canonical_recovery_wal_receiving_busy") from None
        yield


def _baseline(receiving, transfer_sha256, baseline, expected_baseline):
    base = _bound_json(receiving / _PUBLISHED, transfer_sha256)
    if not isinstance(base, dict) or base.get("schema") != _SCHEMA:
        raise ValueError("canonical_recovery_wal_base_transfer_invalid")
    pair = _bound_json(receiving / "complete.json", base["pair_receipt_sha256"])
    if not isinstance(baseline, dict):
        raise ValueError("canonical_recovery_wal_baseline_mismatch")
    digest = {key: value for key, value in baseline.items() if key != "verification_sha256"}
    if (baseline.get("schema") != "qt.encrypted_recovery_repository_verification.v1"
            or baseline.get("verification_sha256") != expected_baseline or archive_evidence_hash(digest) != expected_baseline
            or baseline.get("pair_receipt_sha256") != base["pair_receipt_sha256"]
            or any(baseline.get(key) != pair[key] for key in ("database_identity", "database_label", "archive_snapshot", "storage_layout"))
            or baseline.get("anchor_sha256") is not None or baseline.get("wal_verified_through") != baseline.get("wal_backup_stop")
            or not re.fullmatch(r"15-[1-9][0-9]*", baseline.get("database_archive_id", ""))):
        raise ValueError("canonical_recovery_wal_baseline_mismatch")
    return pair


def _suffix(stop, last, segment_bytes, max_segments):
    if (type(segment_bytes) is not int or not 2**20 <= segment_bytes <= 2**30
            or segment_bytes & (segment_bytes - 1)):
        raise ValueError("canonical_recovery_wal_segment_size_invalid")
    return _wal_range(stop, last, segment_bytes, max_segments + 1)[1:]


def check_source_wal_capacity(control, *, expected_database, backup_stop, segment_bytes, max_segments, timeout_seconds):
    """Reject a span that already exceeds the named receiver's WAL file cap."""
    if type(max_segments) is not int or not 1 <= max_segments <= _MAX_SEGMENTS:
        raise ValueError("canonical_recovery_wal_segment_budget_invalid")
    timeout_ms = min(int(timeout_seconds * 1000), 5_000)
    if timeout_ms < 1:
        raise TimeoutError("canonical_recovery_wal_preflight_deadline")
    with control.begin():
        control.exec_driver_sql("SET TRANSACTION READ ONLY")
        control.execute(text("""
            SELECT set_config('statement_timeout',
                LEAST(:timeout, CASE WHEN setting::bigint=0 THEN :timeout
                                    ELSE setting::bigint END)::text, true)
            FROM pg_settings WHERE name='statement_timeout'
        """), {"timeout": timeout_ms}).close()
        row = control.execute(text(f"""
            SELECT target.*, current_database() AS control_database, current_setting('archive_mode') AS archive_mode,
                   pg_size_bytes(current_setting('wal_segment_size')) AS segment_bytes,
                   CASE WHEN NOT pg_is_in_recovery()
                        THEN pg_walfile_name(pg_current_wal_insert_lsn()) END AS segment
            FROM ({_DATABASE}) AS target
        """), {"name": expected_database["database"]}).mappings().one_or_none()
    if (row is None or any(row[key] != value for key, value in expected_database.items())
            or (row["control_database"], row["archive_mode"], row["segment_bytes"]) != ("postgres", "on", segment_bytes)
            or not row["allow_connections"] or row["segment"] is None):
        raise RuntimeError("canonical_recovery_wal_preflight_source_changed")
    # The current WAL is a lower bound. The exact fenced anchor is checked after the stop.
    return len(_suffix(backup_stop, row["segment"], segment_bytes, max_segments))


def _check_wal_archiver(control, fence, last, floor, timeout, check):
    while True:
        check()
        with control.begin(), _bounded_step(control, timeout):
            if _database(control, fence.database["database"]) != fence.database:
                raise RuntimeError("canonical_recovery_wal_control_database_changed")
            row = control.execute(text("""
                SELECT current_database(), current_setting('archive_mode'), pg_size_bytes(current_setting('wal_segment_size')),
                       last_archived_wal FROM pg_stat_archiver
            """)).one()
        if row[:3] != ("postgres", "on", floor):
            raise RuntimeError("canonical_recovery_wal_source_archiver_changed")
        archived = row[3]
        if archived is not None and re.fullmatch(r"[0-9A-F]{24}", archived):
            if archived[:8] != last[:8]:
                raise RuntimeError("canonical_recovery_wal_source_timeline_changed")
            if archived >= last:
                return
        sleep(0.1)


def _copy_one(source_path, expected, receiving, relative, receiving_device, check):
    directory = receiving / Path(relative).parent
    parent = directory.parent
    if parent.resolve(strict=True) != parent or not stat.S_ISDIR(_safe_member(parent, receiving_device).st_mode):
        raise ValueError("canonical_recovery_wal_receiving_directory_invalid")
    directory.mkdir(mode=0o700, exist_ok=True)
    if directory.resolve(strict=True) != directory or not stat.S_ISDIR(_safe_member(directory, receiving_device).st_mode):
        raise ValueError("canonical_recovery_wal_receiving_directory_invalid")
    destination = receiving / relative
    if destination.exists() or destination.is_symlink():
        found = _safe_member(destination, receiving_device)
        if (not stat.S_ISREG(found.st_mode) or found.st_nlink != 1
                or _file(destination, _stamp(found), check) != (evidence := _file(source_path, expected, check))):
            raise RuntimeError("canonical_recovery_wal_existing_ciphertext_changed")
        return evidence | {"path": relative}
    with NamedTemporaryFile(mode="wb", prefix=".qt-wal-", dir=directory, delete=False) as output:
        temporary = Path(output.name)
        try:
            evidence = _file(source_path, expected, check, output)
            output.flush()
            os.fsync(output.fileno())
            check()
            os.link(temporary, destination)
        finally:
            temporary.unlink()
    _sync(directory)
    _sync(parent)
    return evidence | {"path": relative}


def stage_final_wal(*, fence, control, source, receiving, base_transfer_sha256, baseline, anchor_path,
                    baseline_verification_sha256, anchor_sha256, limits):
    """Read the archiver and write only selected encrypted files on the receiving mount."""
    started = monotonic()
    source, source_id = _private_root(source)
    receiving, receiver_id = _private_root(receiving)
    if (not isinstance(fence, DatabaseFence) or control is fence.connection or control.in_transaction()
            or source_id[0] == receiver_id[0] or source.is_relative_to(receiving) or receiving.is_relative_to(source)
            or limits.files > _MAX_SEGMENTS):
        raise ValueError("canonical_recovery_wal_invalid_boundary")
    conn = fence.connection

    def check():
        fence.check_clock()
        if monotonic() - started >= limits.seconds:
            raise TimeoutError("canonical_recovery_wal_transfer_time_exceeded")
        if _identity(source.lstat()) != source_id or _identity(receiving.lstat()) != receiver_id:
            raise RuntimeError("canonical_recovery_wal_transfer_mount_changed")
        if shutil.disk_usage(receiving).free < limits.reserve_bytes:
            raise RuntimeError("canonical_recovery_wal_transfer_reserve_exhausted")

    if not conn.in_transaction():
        raise ValueError("canonical_recovery_wal_requires_retained_transaction")
    timeout = max(1, min(5, limits.seconds))
    with _source_hold(source), _receiving_lock(receiving), _bounded_step(conn, min(limits.seconds, 600)) as bound:
        bound(fence.deadline - monotonic())
        fence.assert_quiet(conn)
        pair = _baseline(receiving, base_transfer_sha256, baseline, baseline_verification_sha256)
        source_pair = _bound_json(source / pair["name"] / "complete.json", baseline["pair_receipt_sha256"])
        stop = baseline["wal_backup_stop"]
        anchor = _bound_json(anchor_path, anchor_sha256)
        last = _anchor_wal(anchor_path, anchor_sha256, pair, stop)
        if (source_pair != pair or anchor["database"] != fence.database or _snapshot_layout(conn) != anchor["storage_layout"]
                or PostgresMarketDataRepository._current_commit_seq_with_session(conn) != anchor["source_commit_seq"]):
            raise RuntimeError("canonical_recovery_wal_live_anchor_changed")
        segment_bytes = baseline["wal_segment_bytes"]
        suffix = _suffix(stop, last, segment_bytes, limits.files)
        _check_wal_archiver(control, fence, last, segment_bytes, timeout, check)
        observed = []
        for segment, checksum, path in _archive_filenames(source / "database", baseline["database_archive_id"], suffix, check):
            info = _safe_member(path, source_id[0])
            if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1 or info.st_size <= 0:
                raise ValueError("canonical_recovery_wal_source_file_invalid")
            observed.append((segment, checksum, path, _stamp(info)))
        total = sum(expected[2] for _, _, _, expected in observed)
        if total > limits.bytes:
            raise ValueError("canonical_recovery_wal_ciphertext_budget_exceeded")
        if shutil.disk_usage(receiving).free < limits.reserve_bytes + total:
            raise RuntimeError("canonical_recovery_wal_transfer_space_insufficient")
        files = []
        for segment, checksum, path, expected in observed:
            entry = _copy_one(path, expected, receiving, path.relative_to(source).as_posix(), receiver_id[0], check)
            files.append({"segment": segment, "wal_checksum": checksum, **entry})
        if total != sum(entry["bytes"] for entry in files):
            raise RuntimeError("canonical_recovery_wal_ciphertext_size_changed")
        fence.assert_quiet(conn)
        report = {"schema": _VERSION, "base_transfer_sha256": base_transfer_sha256,
                  "baseline_verification_sha256": baseline["verification_sha256"], "anchor_sha256": anchor_sha256,
                  "wal_backup_stop": stop, "wal_transferred_through": last, "wal_segment_bytes": segment_bytes,
                  "database_archive_id": baseline["database_archive_id"], "encrypted_bytes": total, "files": files}
        published = receiving / _RECEIPT
        digest = hashlib.sha256(_encode(report)).hexdigest()
        if published.exists() or published.is_symlink():
            if _bound_json(published, digest) != report:
                raise RuntimeError("canonical_recovery_wal_receipt_changed")
        else:
            with NamedTemporaryFile(mode="wb", prefix=".qt-wal-receipt-", dir=receiving, delete=False) as output:
                pending = Path(output.name)
                try:
                    output.write(_encode(report))
                    output.flush()
                    os.fsync(output.fileno())
                    check()
                    os.link(pending, published)
                finally:
                    pending.unlink()
            _sync(receiving)
        return report | {"wal_transfer_sha256": digest}


def verify_final_wal(*, receiving, expected_hash, baseline, baseline_verification_sha256,
                     base_transfer_sha256, anchor_path, anchor_sha256, limits):
    """Check installed ciphertext binding before native archive-get and named restore."""
    started = monotonic()
    receiving, device = _private_root(receiving)

    def check():
        if monotonic() - started >= limits.seconds:
            raise TimeoutError("canonical_recovery_wal_transfer_time_exceeded")
        if _identity(receiving.lstat()) != device:
            raise RuntimeError("canonical_recovery_wal_transfer_mount_changed")

    if limits.files > _MAX_SEGMENTS:
        raise ValueError("canonical_recovery_wal_segment_budget_invalid")
    pair = _baseline(receiving, base_transfer_sha256, baseline, baseline_verification_sha256)
    last = _anchor_wal(anchor_path, anchor_sha256, pair, baseline["wal_backup_stop"])
    report = _bound_json(receiving / _RECEIPT, expected_hash)
    expected = {"schema": _VERSION, "base_transfer_sha256": base_transfer_sha256,
                "baseline_verification_sha256": baseline["verification_sha256"], "anchor_sha256": anchor_sha256,
                "wal_backup_stop": baseline["wal_backup_stop"], "wal_transferred_through": last,
                "wal_segment_bytes": baseline["wal_segment_bytes"], "database_archive_id": baseline["database_archive_id"]}
    if any(report.get(key) != value for key, value in expected.items()):
        raise ValueError("canonical_recovery_wal_receipt_binding_changed")
    suffix = _suffix(report["wal_backup_stop"], last, report["wal_segment_bytes"], limits.files)
    entries = list(_archive_filenames(receiving / "database", report["database_archive_id"], suffix, check))
    if (len(report["files"]) != len(entries) or type(report["encrypted_bytes"]) is not int
            or not 0 <= report["encrypted_bytes"] <= limits.bytes):
        raise ValueError("canonical_recovery_wal_receipt_files_changed")
    total = 0
    for recorded, (segment, checksum, path) in zip(report["files"], entries, strict=True):
        observed = _safe_member(path, device[0])
        total += observed.st_size
        if (not stat.S_ISREG(observed.st_mode) or observed.st_nlink != 1 or total > limits.bytes
                or recorded != {"segment": segment, "wal_checksum": checksum,
                                "path": path.relative_to(receiving).as_posix(), **_file(path, _stamp(observed), check)}):
            raise RuntimeError("canonical_recovery_wal_receiving_ciphertext_changed")
    if total != report["encrypted_bytes"]:
        raise RuntimeError("canonical_recovery_wal_receiving_size_changed")
    return report | {"wal_transfer_sha256": expected_hash}
