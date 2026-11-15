"""Stage contiguous encrypted WAL while the original database continues serving."""

from contextlib import ExitStack, contextmanager
from dataclasses import replace
import fcntl
import os
from pathlib import Path
import stat
from time import monotonic

from market_data.fact_archive import archive_evidence_hash
from scripts.automation.stage_encrypted_recovery_handoff import TransferLimits, _identity, _private_root, _source_generation, _stamp
from scripts.automation.storage_host_boundary import load_receipt, save_receipt
from scripts.automation.verify_encrypted_recovery_handoff import _WAL, _bound_json, _sha256
from scripts.db.canonical_cold_database_fence import native_transaction_mode
from scripts.db.canonical_cold_recovery_wal_transfer import (
    _MAX_SEGMENTS, _baseline, _prefix_binding, _prefix_path, _prefix_window, _receiving_lock, _segments_per_log,
    _source_wal_snapshot, stage_wal_prefix,
)
from scripts.db.canonical_cold_replayed_checkpoint import _segment_number
from scripts.db.canonical_cold_retained_wal import capture_wal_members


_SCHEMA = "qt.canonical_cold_source_wal_prefix_progress.v1"
_PREFIX_NAME = "encrypted-wal-prefix-through-"
_MAX_PROGRESS_BYTES = 8 * 1024**2
_MAX_BATCHES = 20_000
_DATABASE_FIELDS = {"database", "database_oid", "system_identifier", "allow_connections", "postmaster_started_at"}
_CURSOR_FIELDS = {"after", "through", "previous_prefix_sha256"}
_PREFIX_FIELDS = _CURSOR_FIELDS | {"prefix_sha256"}


@contextmanager
def _directory_lock(path, identity):
    fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        if _identity(os.fstat(fd)) != identity:
            raise RuntimeError("canonical_source_prefix_mount_changed")
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise RuntimeError("canonical_source_prefix_progress_busy") from None
        yield
    finally:
        os.close(fd)


def _prefix_members(receiving, allowed, expected, check):
    found = set()
    with os.scandir(receiving) as entries:
        for entry in entries:
            check()
            if not entry.name.startswith(_PREFIX_NAME):
                continue
            through = entry.name.removeprefix(_PREFIX_NAME).removesuffix(".json")
            if (entry.name != _prefix_path(receiving, through).name or _WAL.fullmatch(through) is None
                    or not entry.is_file(follow_symlinks=False) or through not in allowed):
                raise RuntimeError("canonical_source_prefix_unexpected_exchange_branch")
            found.add(through)
    if not expected <= found:
        raise RuntimeError("canonical_source_prefix_exchange_receipt_missing")


def _save(path, body):
    save_receipt(path, body | {"progress_sha256": archive_evidence_hash(body)}, initial=False)


def _load(path, scope, max_batches):
    if not os.path.lexists(path):
        return {"schema": _SCHEMA, "scope": scope, "prefixes": [], "pending": None}
    before = path.lstat()
    if not stat.S_ISREG(before.st_mode) or before.st_nlink != 1:
        raise RuntimeError("canonical_source_prefix_progress_changed")
    record = load_receipt(path, max_bytes=_MAX_PROGRESS_BYTES)
    if (not isinstance(record, dict) or set(record) != {"schema", "scope", "prefixes", "pending", "progress_sha256"}
            or record["schema"] != _SCHEMA or record["scope"] != scope
            or not isinstance(record["prefixes"], list) or len(record["prefixes"]) > max_batches
            or _stamp(path.lstat()) != _stamp(before)):
        raise RuntimeError("canonical_source_prefix_progress_changed")
    digest = record.pop("progress_sha256")
    if not _sha256(digest) or archive_evidence_hash(record) != digest:
        raise RuntimeError("canonical_source_prefix_progress_changed")
    return record


def _cursor(baseline, prefix, after, previous, target, *, fields, max_segments):
    if (not isinstance(prefix, dict) or set(prefix) != fields
            or prefix["after"] != after or prefix["previous_prefix_sha256"] != previous
            or not isinstance(prefix["through"], str) or prefix["through"] > target):
        raise RuntimeError("canonical_source_prefix_progress_gap_or_branch")
    return _prefix_window(baseline, after, prefix["through"], max_segments, previous)


def _receipt(receiving, binding, prefix, names, archive_id, check):
    digest = prefix["prefix_sha256"]
    report = _bound_json(_prefix_path(receiving, prefix["through"]), digest)
    expected = binding | {"wal_after": prefix["after"], "wal_transferred_through": prefix["through"],
                          "previous_prefix_sha256": prefix["previous_prefix_sha256"]}
    if (not isinstance(report, dict) or any(report.get(key) != value for key, value in expected.items())
            or set(report) != expected.keys() | {"files", "encrypted_bytes"}):
        raise RuntimeError("canonical_source_prefix_exchange_receipt_changed")
    rows = report["files"]
    if (not isinstance(rows, list) or len(rows) != len(names)
            or any(not isinstance(row, dict) or set(row) != {"segment", "path", "bytes", "sha256", "wal_checksum"}
                   or row["segment"] != name or type(row["bytes"]) is not int or row["bytes"] <= 0
                   or not _sha256(row["sha256"]) for row, name in zip(rows, names))
            or type(report["encrypted_bytes"]) is not int or report["encrypted_bytes"] != sum(row["bytes"] for row in rows)):
        raise RuntimeError("canonical_source_prefix_exchange_receipt_changed")
    custody = capture_wal_members(receiving=receiving, archive_id=archive_id, rows=rows, check=check)
    return report, custody.sha256


def _resume(state, *, receiving, baseline, digest, target, limits, check):
    after, previous = baseline["wal_backup_stop"], None
    terminal = None
    binding = _prefix_binding(state["scope"]["base_transfer_sha256"], digest)
    archive_id = baseline["database_archive_id"]
    completed = set()
    for prefix in state["prefixes"]:
        check()
        names = _cursor(baseline, prefix, after, previous, target, fields=_PREFIX_FIELDS | {"custody_sha256"},
                        max_segments=_MAX_SEGMENTS)
        report, custody = _receipt(receiving, binding, prefix, names, archive_id, check)
        if prefix["custody_sha256"] != custody:
            raise RuntimeError("canonical_source_prefix_exchange_custody_changed")
        after, previous = prefix["through"], prefix["prefix_sha256"]
        completed.add(after)
        terminal = report["files"][-1]
    pending = state["pending"]
    allowed = completed.copy()
    if pending is not None:
        _cursor(baseline, pending, after, previous, target, fields=_CURSOR_FIELDS, max_segments=limits.files)
        allowed.add(pending["through"])
    _prefix_members(receiving, allowed, completed, check)
    return after, previous, terminal, completed


def _assert_archived(control, expected_database, segment_bytes, target_through, check):
    observed = _source_wal_snapshot(control, expected_database, segment_bytes, check())
    archived, current = observed["last_archived_wal"], observed["segment"]
    if (not all(isinstance(value, str) and _WAL.fullmatch(value) and value[:8] == target_through[:8]
                for value in (archived, current)) or not target_through <= min(archived, current)):
        raise RuntimeError("canonical_source_prefix_target_not_archived")


def stage_source_wal_prefixes(*, control, expected_database, source, receiving, base_transfer_sha256, baseline,
                              target_through, limits, progress_path, max_batches, timeout_seconds):
    """Resume at the private checkpoint and stage through one exact archived segment.

    A pending interval is written durably before copying it. Only that bounded
    interval may be rechecked by the encrypted stager after interruption. Earlier
    checkpoints compare pinned receipt digests and installed filesystem metadata.
    The returned prefix descriptors still require native receiver authentication.
    """
    if (not isinstance(expected_database, dict) or set(expected_database) != _DATABASE_FIELDS
            or expected_database["allow_connections"] is not True
            or not isinstance(baseline, dict) or not _sha256(base_transfer_sha256)
            or not _sha256(baseline.get("verification_sha256")) or not isinstance(limits, TransferLimits)
            or limits.files > _MAX_SEGMENTS or type(max_batches) is not int or not 1 <= max_batches <= _MAX_BATCHES
            or type(timeout_seconds) is not int or timeout_seconds < 1
            or not isinstance(target_through, str) or _WAL.fullmatch(target_through) is None
            or not native_transaction_mode(control) or control.in_transaction()):
        raise ValueError("canonical_source_prefix_invalid_scope")
    source, source_identity = _private_root(source)
    receiving, exchange_identity = _private_root(receiving)
    progress = Path(progress_path)
    parent, progress_identity = _private_root(progress.parent)
    if (source_identity[0] == exchange_identity[0] or progress != parent / progress.name
            or any(left.is_relative_to(right) or right.is_relative_to(left)
                   for left, right in ((source, receiving), (progress, source), (progress, receiving),
                                       (parent, source), (parent, receiving)))):
        raise ValueError("canonical_source_prefix_output_scope_invalid")

    started = monotonic()

    def check():
        left = timeout_seconds - (monotonic() - started)
        if left <= 0:
            raise TimeoutError("canonical_source_prefix_deadline")
        if any(_identity(path.lstat()) != identity for path, identity in (
            (source, source_identity), (receiving, exchange_identity), (parent, progress_identity),
        )):
            raise RuntimeError("canonical_source_prefix_mount_changed")
        return left

    digest = baseline["verification_sha256"]
    pair = _baseline(receiving, base_transfer_sha256, baseline, digest)
    _, _, original = _source_generation(source, source_identity[0], pair["name"], baseline["pair_receipt_sha256"])
    if (original != pair or f"{expected_database['system_identifier']}/{expected_database['database_oid']}" != pair["database_identity"]):
        raise RuntimeError("canonical_source_prefix_original_pair_changed")
    stop = baseline["wal_backup_stop"]
    timeline = int(target_through[:8], 16)
    segment_bytes = baseline["wal_segment_bytes"]
    per_log = _segments_per_log(segment_bytes)
    stop_number = _segment_number(stop, timeline, per_log)
    target = _segment_number(target_through, timeline, per_log)
    if timeline == 0 or target <= stop_number:
        raise ValueError("canonical_source_prefix_target_outside_baseline")
    scope = {"database": {key: value for key, value in expected_database.items() if key != "postmaster_started_at"},
             "base_transfer_sha256": base_transfer_sha256,
             "baseline_verification_sha256": digest, "pair_receipt_sha256": baseline["pair_receipt_sha256"],
             "source_root": str(source), "source_identity": list(source_identity),
             "receiving_root": str(receiving), "receiving_identity": list(exchange_identity),
             "progress_directory_identity": list(progress_identity)}

    with ExitStack() as stack:
        stack.enter_context(_directory_lock(parent, progress_identity))
        stack.enter_context(_directory_lock(receiving, exchange_identity))
        state = _load(progress, scope, max_batches)
        _assert_archived(control, expected_database, segment_bytes, target_through, check)
        with _receiving_lock(receiving):
            after, previous, terminal, completed = _resume(state, receiving=receiving, baseline=baseline,
                digest=digest, target=target_through, limits=limits, check=check)
        cursor = _segment_number(after, timeline, per_log)
        pending = state["pending"]
        planned_until = cursor if pending is None else _segment_number(pending["through"], timeline, per_log)
        count = len(completed) + (pending is not None) + (target - planned_until + limits.files - 1) // limits.files
        if count > max_batches:
            raise ValueError("canonical_source_prefix_batch_budget_exceeded")

        extending = cursor < target
        binding = _prefix_binding(base_transfer_sha256, digest)
        while cursor < target:
            check()
            if state["pending"] is None:
                end = min(target, cursor + limits.files)
                through = f"{timeline:08X}{end // per_log:08X}{end % per_log:08X}"
                state["pending"] = {"after": after, "through": through, "previous_prefix_sha256": previous}
                _save(progress, state)
            pending = state["pending"]
            seconds = int(check())
            if seconds < 1:
                raise TimeoutError("canonical_source_prefix_deadline")
            staged = stage_wal_prefix(control=control, expected_database=expected_database, source=source,
                receiving=receiving, base_transfer_sha256=base_transfer_sha256, baseline=baseline,
                baseline_verification_sha256=digest, limits=replace(limits, seconds=min(limits.seconds, seconds)), **pending)
            prefix = pending | {"prefix_sha256": staged["wal_prefix_sha256"]}
            names = _prefix_window(baseline, after, prefix["through"], limits.files, previous)
            with _receiving_lock(receiving):
                report, custody = _receipt(receiving, binding, prefix, names, baseline["database_archive_id"], check)
                if staged != report | {"wal_prefix_sha256": prefix["prefix_sha256"]}:
                    raise RuntimeError("canonical_source_prefix_staged_receipt_changed")
                after, previous = prefix["through"], prefix["prefix_sha256"]
                completed.add(after)
                _prefix_members(receiving, completed, completed, check)
                state["prefixes"].append(prefix | {"custody_sha256": custody})
                state["pending"] = None
                _save(progress, state)
            cursor = _segment_number(after, timeline, per_log)

        if extending:
            _assert_archived(control, expected_database, segment_bytes, target_through, check)
            with _receiving_lock(receiving):
                _, _, terminal, _ = _resume(state, receiving=receiving, baseline=baseline, digest=digest,
                                            target=target_through, limits=limits, check=check)
        check()
        return {"target_through": target_through,
                "base_transfer_sha256": base_transfer_sha256, "baseline_verification_sha256": digest,
                "pair_receipt_sha256": baseline["pair_receipt_sha256"], "database_identity": pair["database_identity"],
                "prefixes": [{key: prefix[key] for key in _PREFIX_FIELDS} for prefix in state["prefixes"]],
                "terminal": terminal, "progress_sha256": archive_evidence_hash(state)}
