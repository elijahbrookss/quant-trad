"""Copy bounded encrypted WAL prefixes and the final fenced suffix."""

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
from scripts.automation.canonical_cold_receiver_ready import verify_replayed_checkpoint_baseline
from scripts.automation.stage_encrypted_recovery_handoff import (
    TransferLimits, _PUBLISHED, _SCHEMA, _encode, _file, _identity, _private_root, _safe_member, _source_generation,
    _source_hold, _stamp,
)
from scripts.automation.verify_encrypted_recovery_handoff import _WAL, _anchor_wal, _archive_filenames, _bound_json, _wal_range
from scripts.db.canonical_cold_database_fence import DatabaseFence, _DATABASE, _database, native_transaction_mode
from scripts.db.canonical_cold_replayed_checkpoint import _lsn, _segment_number
from scripts.db.fact_header_v2_capture import _bounded_step


_VERSION = "qt.encrypted_recovery_wal_transfer.v1"
_CHECKPOINT_VERSION = "qt.encrypted_recovery_wal_transfer.v2"
_RECEIPT = "encrypted-wal-transfer.json"
_PREFIX_VERSION = "qt.encrypted_recovery_wal_prefix.v1"
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


def _segments_per_log(segment_bytes):
    if (type(segment_bytes) is not int or not 2**20 <= segment_bytes <= 2**30
            or segment_bytes & (segment_bytes - 1)):
        raise ValueError("canonical_recovery_wal_segment_size_invalid")
    return (1 << 32) // segment_bytes


def _suffix(stop, last, segment_bytes, max_segments):
    _segments_per_log(segment_bytes)
    return _wal_range(stop, last, segment_bytes, max_segments + 1)[1:]


def _count_restart_segments(redo, terminal, segment_bytes, max_restart_segments):
    if type(max_restart_segments) is not int or max_restart_segments < 1:
        raise ValueError("canonical_recovery_wal_restart_segment_budget_invalid")
    if not isinstance(terminal, str) or not _WAL.fullmatch(terminal) or int(terminal[:8], 16) == 0:
        raise ValueError("canonical_recovery_wal_restart_range_invalid")
    timeline = int(terminal[:8], 16)
    per_log = _segments_per_log(segment_bytes)
    first = _segment_number(redo, timeline, per_log)
    last = _segment_number(terminal, timeline, per_log)
    count = last - first + 1
    if not 1 <= count <= max_restart_segments:
        raise ValueError("canonical_recovery_wal_restart_range_outside_budget")
    return count


def final_wal_origin(baseline, *, checkpoint=None, checkpoint_sha256=None):
    """Return the transport floor from the original pinned backup or trusted restart evidence."""
    if checkpoint is None and checkpoint_sha256 is None:
        return baseline["wal_backup_stop"]
    return verify_replayed_checkpoint_baseline(
        checkpoint, baseline_verification=baseline, expected_checkpoint_sha256=checkpoint_sha256,
    )


def _final_scope(baseline, *, base_transfer_sha256, anchor_sha256, anchor, last, checkpoint, checkpoint_sha256,
                 max_restart_segments=None):
    origin = final_wal_origin(baseline, checkpoint=checkpoint, checkpoint_sha256=checkpoint_sha256)
    segment_bytes = baseline["wal_segment_bytes"]
    scope = {"schema": _VERSION, "base_transfer_sha256": base_transfer_sha256,
             "baseline_verification_sha256": baseline["verification_sha256"], "anchor_sha256": anchor_sha256,
             "wal_backup_stop": baseline["wal_backup_stop"], "wal_transferred_through": last,
             "wal_segment_bytes": segment_bytes, "database_archive_id": baseline["database_archive_id"]}
    if checkpoint is not None:
        end = _lsn(anchor.get("anchor_lsn"))
        if (last[:8] != origin[:8] or last <= origin or anchor["timeline"] != checkpoint["timeline"]
                or end <= _lsn(checkpoint["replay_lsn"])
                or _segment_number(last, checkpoint["timeline"], _segments_per_log(segment_bytes)) != (end - 1) // segment_bytes
                or anchor["source_commit_seq"] < checkpoint["accepted_commit_seq"]):
            raise ValueError("canonical_recovery_wal_anchor_not_after_checkpoint")
        scope.update({"schema": _CHECKPOINT_VERSION, "wal_after": origin, "checkpoint_sha256": checkpoint_sha256,
                      "wal_verification_sha256": checkpoint["wal_verification_sha256"]})
    if max_restart_segments is not None:
        if checkpoint is None:
            raise ValueError("canonical_recovery_wal_restart_checkpoint_required")
        scope.update({"max_restart_segments": max_restart_segments,
                      "wal_restart_segments": _count_restart_segments(
                          checkpoint["redo_wal_segment"], last, segment_bytes, max_restart_segments,
                      )})
    return origin, scope


def check_source_wal_capacity(control, *, expected_database, backup_stop, segment_bytes, max_segments, timeout_seconds,
                              restart_from=None, max_restart_segments=None):
    """Reject a span that already exceeds the named receiver's WAL file cap."""
    if type(max_segments) is not int or not 1 <= max_segments <= _MAX_SEGMENTS:
        raise ValueError("canonical_recovery_wal_segment_budget_invalid")
    if (restart_from is None) != (max_restart_segments is None):
        raise ValueError("canonical_recovery_wal_restart_segment_budget_invalid")
    row = _source_wal_snapshot(control, expected_database, segment_bytes, timeout_seconds)
    # The current WAL is a lower bound. The exact fenced anchor is checked after the stop.
    count = len(_suffix(backup_stop, row["segment"], segment_bytes, max_segments))
    if restart_from is not None:
        _count_restart_segments(restart_from, row["segment"], segment_bytes, max_restart_segments)
    return count


def _source_wal_snapshot(control, expected_database, segment_bytes, timeout_seconds):
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
                        THEN pg_walfile_name(pg_current_wal_insert_lsn()) END AS segment,
                   last_archived_wal
            FROM ({_DATABASE}) AS target
            CROSS JOIN pg_stat_archiver
        """), {"name": expected_database["database"]}).mappings().one_or_none()
    if (row is None or any(row[key] != value for key, value in expected_database.items())
            or (row["control_database"], row["archive_mode"], row["segment_bytes"]) != ("postgres", "on", segment_bytes)
            or not row["allow_connections"] or row["segment"] is None):
        raise RuntimeError("canonical_recovery_wal_preflight_source_changed")
    return row


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


def _publish_wal_receipt(path, report, check):
    encoded = _encode(report)
    digest = hashlib.sha256(encoded).hexdigest()
    check()
    if path.exists() or path.is_symlink():
        if _bound_json(path, digest) != report:
            raise RuntimeError("canonical_recovery_wal_receipt_changed")
    else:
        with NamedTemporaryFile(mode="wb", prefix=".qt-wal-receipt-", dir=path.parent, delete=False) as output:
            pending = Path(output.name)
            try:
                output.write(encoded)
                output.flush()
                os.fsync(output.fileno())
                check()
                os.link(pending, path)
            finally:
                pending.unlink()
        _sync(path.parent)
    return digest


def _observe_wal(source, device, archive_id, segments, check):
    observed = []
    for segment, checksum, path in _archive_filenames(source / "database", archive_id, segments, check):
        info = _safe_member(path, device)
        if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1 or info.st_size <= 0:
            raise ValueError("canonical_recovery_wal_source_file_invalid")
        observed.append((segment, checksum, path, _stamp(info)))
    return observed


def _copy_wal(observed, *, source, receiving, device, total, check):
    files = []
    for segment, checksum, path, expected in observed:
        entry = _copy_one(path, expected, receiving, path.relative_to(source).as_posix(), device, check)
        files.append({"segment": segment, "wal_checksum": checksum, **entry})
    if total != sum(entry["bytes"] for entry in files):
        raise RuntimeError("canonical_recovery_wal_ciphertext_size_changed")
    return files


def _prefix_path(receiving, through):
    return receiving / f"encrypted-wal-prefix-through-{through}.json"


def _prefix_window(baseline, after, through, max_segments, previous_prefix_sha256):
    stop = baseline["wal_backup_stop"]
    segments = _suffix(after, through, baseline["wal_segment_bytes"], max_segments)
    if not segments or after[:8] != stop[:8] or after < stop:
        raise ValueError("canonical_recovery_wal_prefix_outside_baseline")
    if after == stop:
        if previous_prefix_sha256 is not None:
            raise ValueError("canonical_recovery_wal_prefix_unexpected_previous")
    elif not isinstance(previous_prefix_sha256, str) or not re.fullmatch(r"[a-f0-9]{64}", previous_prefix_sha256):
        raise ValueError("canonical_recovery_wal_prefix_previous_required")
    return segments


def _prefix_binding(base_transfer_sha256, baseline_verification_sha256):
    return {"schema": _PREFIX_VERSION, "base_transfer_sha256": base_transfer_sha256,
            "baseline_verification_sha256": baseline_verification_sha256}


def _assert_previous_prefix(receiving, binding, baseline, after, previous_prefix_sha256):
    if previous_prefix_sha256 is None:
        return
    prior = _bound_json(_prefix_path(receiving, after), previous_prefix_sha256)
    if (not isinstance(prior, dict) or any(prior.get(key) != value for key, value in binding.items())
            or prior.get("wal_transferred_through") != after):
        raise ValueError("canonical_recovery_wal_prefix_previous_changed")
    segments = _prefix_window(baseline, prior.get("wal_after"), after, _MAX_SEGMENTS, prior.get("previous_prefix_sha256"))
    if not isinstance(prior.get("files"), list) or [entry["segment"] for entry in prior["files"]] != segments:
        raise ValueError("canonical_recovery_wal_prefix_previous_changed")


def stage_wal_prefix(*, control, expected_database, source, receiving, base_transfer_sha256, baseline,
                     baseline_verification_sha256, after, through, limits, previous_prefix_sha256=None):
    """Copy a closed WAL interval while the source serves. The caller pins the verified previous receipt."""
    started = monotonic()
    source, source_id = _private_root(source)
    receiving, receiver_id = _private_root(receiving)
    if (not native_transaction_mode(control) or control.in_transaction()
            or not {"database", "database_oid", "system_identifier", "postmaster_started_at", "allow_connections"}
            <= expected_database.keys() or not isinstance(limits, TransferLimits) or limits.files > _MAX_SEGMENTS
            or source_id[0] == receiver_id[0] or source.is_relative_to(receiving) or receiving.is_relative_to(source)):
        raise ValueError("canonical_recovery_wal_prefix_invalid_source")

    def check():
        remaining = limits.seconds - (monotonic() - started)
        if remaining <= 0:
            raise TimeoutError("canonical_recovery_wal_transfer_time_exceeded")
        if _identity(source.lstat()) != source_id or _identity(receiving.lstat()) != receiver_id:
            raise RuntimeError("canonical_recovery_wal_transfer_mount_changed")
        if shutil.disk_usage(receiving).free < limits.reserve_bytes:
            raise RuntimeError("canonical_recovery_wal_transfer_reserve_exhausted")
        return remaining

    with _source_hold(source), _receiving_lock(receiving):
        pair = _baseline(receiving, base_transfer_sha256, baseline, baseline_verification_sha256)
        _, _, source_pair = _source_generation(source, source_id[0], pair["name"], baseline["pair_receipt_sha256"])
        if (source_pair != pair or f"{expected_database['system_identifier']}/{expected_database['database_oid']}"
                != pair["database_identity"]):
            raise RuntimeError("canonical_recovery_wal_prefix_source_changed")
        segments = _prefix_window(baseline, after, through, limits.files, previous_prefix_sha256)
        binding = _prefix_binding(base_transfer_sha256, baseline_verification_sha256)
        _assert_previous_prefix(receiving, binding, baseline, after, previous_prefix_sha256)
        source_wal = _source_wal_snapshot(control, expected_database, baseline["wal_segment_bytes"], check())
        archived = source_wal["last_archived_wal"]
        if (not isinstance(archived, str) or not _WAL.fullmatch(archived)
                or archived[:8] != through[:8] or source_wal["segment"][:8] != through[:8]
                or not through <= min(archived, source_wal["segment"])):
            raise RuntimeError("canonical_recovery_wal_prefix_not_archived")
        observed = _observe_wal(source, source_id[0], baseline["database_archive_id"], segments, check)
        total = sum(stamp[2] for _, _, _, stamp in observed)
        if total > limits.bytes:
            raise ValueError("canonical_recovery_wal_ciphertext_budget_exceeded")
        new_bytes = sum(stamp[2] for _, _, path, stamp in observed
                        if not os.path.lexists(receiving / path.relative_to(source)))
        if shutil.disk_usage(receiving).free < limits.reserve_bytes + new_bytes:
            raise RuntimeError("canonical_recovery_wal_transfer_space_insufficient")
        files = _copy_wal(observed, source=source, receiving=receiving, device=receiver_id[0], total=total, check=check)
        installed = [(segment, checksum, path.relative_to(receiving).as_posix()) for segment, checksum, path
                     in _archive_filenames(receiving / "database", baseline["database_archive_id"], segments, check)]
        if installed != [(row["segment"], row["wal_checksum"], row["path"]) for row in files]:
            raise RuntimeError("canonical_recovery_wal_receiving_ciphertext_changed")
        report = binding | {"wal_after": after, "wal_transferred_through": through, "previous_prefix_sha256": previous_prefix_sha256,
                            "encrypted_bytes": total, "files": files}
        digest = _publish_wal_receipt(_prefix_path(receiving, through), report, check)
        return report | {"wal_prefix_sha256": digest}


def verify_wal_prefix(*, receiving, base_transfer_sha256, baseline, baseline_verification_sha256, after, through,
                      expected_hash, limits, previous_prefix_sha256=None):
    """Rehash the exact ciphertext interval on the receiver without making a named restore claim."""
    started = monotonic()
    receiving, device = _private_root(receiving)
    if not isinstance(limits, TransferLimits) or limits.files > _MAX_SEGMENTS:
        raise ValueError("canonical_recovery_wal_segment_budget_invalid")

    def check():
        if monotonic() - started >= limits.seconds:
            raise TimeoutError("canonical_recovery_wal_transfer_time_exceeded")
        if _identity(receiving.lstat()) != device:
            raise RuntimeError("canonical_recovery_wal_transfer_mount_changed")

    with _receiving_lock(receiving):
        _baseline(receiving, base_transfer_sha256, baseline, baseline_verification_sha256)
        segments = _prefix_window(baseline, after, through, limits.files, previous_prefix_sha256)
        binding = _prefix_binding(base_transfer_sha256, baseline_verification_sha256)
        _assert_previous_prefix(receiving, binding, baseline, after, previous_prefix_sha256)
        report = _bound_json(_prefix_path(receiving, through), expected_hash)
        expected = binding | {"wal_after": after, "wal_transferred_through": through, "previous_prefix_sha256": previous_prefix_sha256}
        if not isinstance(report, dict) or any(report.get(key) != value for key, value in expected.items()):
            raise ValueError("canonical_recovery_wal_prefix_receipt_changed")
        _verify_ciphertext(receiving, device, baseline["database_archive_id"], segments, report, limits, check)
        return report | {"wal_prefix_sha256": expected_hash}


def stage_final_wal(*, fence, control, source, receiving, base_transfer_sha256, baseline, anchor_path,
                    baseline_verification_sha256, anchor_sha256, limits, checkpoint=None, checkpoint_sha256=None,
                    max_restart_segments=None):
    """Transfer the exact encrypted tail while the trusted earlier restart WAL remains in custody."""
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
        origin, scope = _final_scope(baseline, base_transfer_sha256=base_transfer_sha256, anchor_sha256=anchor_sha256,
                                     anchor=anchor, last=last, checkpoint=checkpoint, checkpoint_sha256=checkpoint_sha256,
                                     max_restart_segments=max_restart_segments)
        segment_bytes = baseline["wal_segment_bytes"]
        suffix = _suffix(origin, last, segment_bytes, limits.files)
        _check_wal_archiver(control, fence, last, segment_bytes, timeout, check)
        observed = _observe_wal(source, source_id[0], baseline["database_archive_id"], suffix, check)
        total = sum(expected[2] for _, _, _, expected in observed)
        if total > limits.bytes:
            raise ValueError("canonical_recovery_wal_ciphertext_budget_exceeded")
        if shutil.disk_usage(receiving).free < limits.reserve_bytes + total:
            raise RuntimeError("canonical_recovery_wal_transfer_space_insufficient")
        files = _copy_wal(observed, source=source, receiving=receiving, device=receiver_id[0], total=total, check=check)
        fence.assert_quiet(conn)
        report = scope | {"encrypted_bytes": total, "files": files}
        digest = _publish_wal_receipt(receiving / _RECEIPT, report, check)
        return report | {"wal_transfer_sha256": digest}


def _verify_ciphertext(receiving, device, archive_id, segments, report, limits, check):
    entries = list(_archive_filenames(receiving / "database", archive_id, segments, check))
    if (not isinstance(report.get("files"), list) or len(report["files"]) != len(entries)
            or type(report.get("encrypted_bytes")) is not int or not 0 <= report["encrypted_bytes"] <= limits.bytes):
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


def verify_final_wal(*, receiving, expected_hash, baseline, baseline_verification_sha256,
                     base_transfer_sha256, anchor_path, anchor_sha256, limits, checkpoint=None, checkpoint_sha256=None,
                     max_restart_segments=None):
    """Recheck final ciphertext against the named anchor and independently pinned restart evidence."""
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
    anchor = _bound_json(anchor_path, anchor_sha256) if checkpoint is not None else None
    origin, expected = _final_scope(baseline, base_transfer_sha256=base_transfer_sha256, anchor_sha256=anchor_sha256,
                                    anchor=anchor, last=last, checkpoint=checkpoint, checkpoint_sha256=checkpoint_sha256,
                                    max_restart_segments=max_restart_segments)
    report = _bound_json(receiving / _RECEIPT, expected_hash)
    if (not isinstance(report, dict) or any(report.get(key) != value for key, value in expected.items())
            or (checkpoint is not None and report.keys() != expected.keys() | {"encrypted_bytes", "files"})):
        raise ValueError("canonical_recovery_wal_receipt_binding_changed")
    suffix = _suffix(origin, last, report["wal_segment_bytes"], limits.files)
    _verify_ciphertext(receiving, device, report["database_archive_id"], suffix, report, limits, check)
    return report | {"wal_transfer_sha256": expected_hash}
