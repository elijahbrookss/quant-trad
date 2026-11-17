"""Preserve exact pending sealed input in private, independently restored custody."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import UTC, datetime
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import stat
from tempfile import TemporaryDirectory
from time import monotonic

from core.storage_mounts import configured_working_root, require_configured_working_mount
from market_data.archive import read_sealed_spool_bytes, _safe_component
from market_data.fact_archive import archive_evidence_hash
from market_data.structure import build_spool_segment_id
from portal.backend.service.storage.maintenance_runtime import _unique_fields
from portal.backend.service.storage.recovery_copies import _json_write, _sync
from scripts.automation.stage_encrypted_recovery_handoff import _identity, _private_root, _stamp
from scripts.automation.verify_canonical_recovery_archive import _path, _sha256
from scripts.automation.verify_encrypted_recovery_handoff import _bound_json
from scripts.db.canonical_cold_recovery_archive_transport import _encrypted_inventory, _restic_run
from scripts.db.canonical_cold_recovery_wal_transfer import _receiving_lock


_VERSION = "qt.pending_raw_spool_transport.v1"
_VERIFIED = "qt.pending_raw_spool_verified.v1"
_SOURCE = "qt.pending_raw_spool_source.v1"
_PRE_ANCHOR = "qt.pending_raw_spool_source_before_anchor.v1"
_BUNDLE = "pending-spool"
_FILE = re.compile(r"spool_[0-9a-f]{64}\.sealed")
_SOURCE_BOUNDARY_FIELDS = {"database_identity", "source_commit_seq"}


@dataclass(frozen=True)
class PendingSpoolScope:
    definition_id: str
    session_id: str
    connection_epoch: int
    provider: str
    venue: str
    provider_product_id: str
    requested_channel: str
    sealed_files: int
    sealed_bytes: int

    def __post_init__(self):
        tokens = (self.definition_id, self.session_id, self.provider, self.venue, self.provider_product_id,
                  self.requested_channel)
        if (any(not isinstance(token, str) or not token or len(token) > 160 or token != token.strip() for token in tokens)
                or type(self.connection_epoch) is not int or not 0 <= self.connection_epoch <= 2**31 - 1
                or type(self.sealed_files) is not int or not 1 <= self.sealed_files <= 32
                or type(self.sealed_bytes) is not int or not 1 <= self.sealed_bytes <= 64 * 1024**2):
            raise ValueError("canonical_recovery_pending_spool_invalid_scope")
        _safe_component(self.definition_id)
        _safe_component(self.session_id)

    def relative_epoch(self):
        return Path(_safe_component(self.definition_id), _safe_component(self.session_id), f"epoch={self.connection_epoch}")


def _boundary(boundary, *, anchored=True):
    fields = _SOURCE_BOUNDARY_FIELDS | {"anchor_sha256"} if anchored else _SOURCE_BOUNDARY_FIELDS
    if (not isinstance(boundary, dict) or boundary.keys() != fields
            or not re.fullmatch(r"[1-9][0-9]*/[1-9][0-9]*", str(boundary["database_identity"]))
            or type(boundary["source_commit_seq"]) is not int or boundary["source_commit_seq"] < 0
            or (anchored and not _sha256(boundary["anchor_sha256"]))):
        raise ValueError("canonical_recovery_pending_spool_invalid_boundary")
    return dict(boundary)


def _pending(root, scope, max_entries, check, *, copied):
    if root.resolve(strict=True) != root or not root.is_dir():
        raise ValueError("canonical_recovery_pending_spool_alias_or_mount_changed")
    paths, pending = {}, [root]
    count = total = 0
    device = root.stat().st_dev
    epoch = root / scope.relative_epoch()
    while pending:
        with os.scandir(pending.pop()) as children:
            for child in children:
                check()
                count += 1
                if count > max_entries:
                    raise RuntimeError("canonical_recovery_pending_spool_entry_budget_exceeded")
                info = child.stat(follow_symlinks=False)
                path = Path(child.path)
                if info.st_dev != device or not (stat.S_ISDIR(info.st_mode) or stat.S_ISREG(info.st_mode)):
                    raise ValueError("canonical_recovery_pending_spool_alias_or_mount_changed")
                if stat.S_ISDIR(info.st_mode):
                    pending.append(path)
                elif re.fullmatch(r"spool_[0-9a-f]{64}\.ack\.json", path.name) and not copied:
                    if path.parent == epoch and (epoch / (path.name.removesuffix(".ack.json") + ".sealed")).exists():
                        raise RuntimeError("canonical_recovery_pending_spool_unexpected_ack")
                elif (_FILE.fullmatch(path.name) and path.parent == epoch and info.st_nlink == 1):
                    paths[path.relative_to(root).as_posix()] = _stamp(info)
                    total += info.st_size
                    if len(paths) > scope.sealed_files or total > scope.sealed_bytes:
                        raise RuntimeError("canonical_recovery_pending_spool_file_scope_changed")
                else:
                    raise RuntimeError("canonical_recovery_pending_spool_unexpected_file")
    if len(paths) != scope.sealed_files or total != scope.sealed_bytes:
        raise RuntimeError("canonical_recovery_pending_spool_file_scope_changed")
    return paths


def _read(path, expected, check):
    with os.fdopen(os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK), "rb") as stream:
        if _stamp(os.fstat(stream.fileno())) != expected:
            raise RuntimeError("canonical_recovery_pending_spool_source_changed")
        chunks = []
        remaining = expected[2]
        while remaining:
            check()
            chunk = stream.read(min(remaining, 1024**2))
            if not chunk:
                raise RuntimeError("canonical_recovery_pending_spool_source_changed")
            chunks.append(chunk)
            remaining -= len(chunk)
        if stream.read(1) or _stamp(os.fstat(stream.fileno())) != expected or _stamp(path.lstat()) != expected:
            raise RuntimeError("canonical_recovery_pending_spool_source_changed")
    return b"".join(chunks)


def pending_spool_record_witness(*, receive_ordinal, received_at, raw_record_id, raw_frame_sha256, observed_channel):
    """Serialize the same raw identity and receive clocks for sealed and archived frames."""
    material = [receive_ordinal, received_at.isoformat(), raw_record_id, raw_frame_sha256, observed_channel]
    return (json.dumps(material, separators=(",", ":")) + "\n").encode()


def _descriptor(relative, raw, scope, check):
    check()
    header, records = read_sealed_spool_bytes(raw)
    check()
    ordinal = header["segment_ordinal"]
    source = scope.definition_id, scope.session_id, scope.connection_epoch
    if (type(ordinal) is not int or not 0 <= ordinal <= 2**31 - 1 or type(header.get("connection_epoch")) is not int
            or (header["definition_id"], header["session_id"], header["connection_epoch"]) != source):
        raise RuntimeError("canonical_recovery_pending_spool_session_mismatch")
    segment_id = build_spool_segment_id(definition_id=scope.definition_id, session_id=scope.session_id,
                                        connection_epoch=scope.connection_epoch, segment_ordinal=ordinal)
    if header["spool_segment_id"] != segment_id or Path(relative).name != segment_id + ".sealed":
        raise RuntimeError("canonical_recovery_pending_spool_segment_id_mismatch")
    created = datetime.fromisoformat(header["created_at"])
    if created.tzinfo is None:
        raise RuntimeError("canonical_recovery_pending_spool_created_clock_invalid")
    stream = scope.provider, scope.venue, scope.provider_product_id, scope.requested_channel
    rows = hashlib.sha256()
    for record in records:
        check()
        if ((record.definition_id, record.session_id, record.connection_epoch) != source
                or (record.provider, record.venue, record.provider_product_id, record.requested_channel) != stream):
            raise RuntimeError("canonical_recovery_pending_spool_record_scope_mismatch")
        rows.update(pending_spool_record_witness(receive_ordinal=record.receive_ordinal, received_at=record.received_at,
            raw_record_id=record.raw_record_id, raw_frame_sha256=record.raw_frame_sha256,
            observed_channel=record.observed_channel))
    return {"path": relative, "segment_id": segment_id, "segment_ordinal": ordinal, "bytes": len(raw),
            "sha256": hashlib.sha256(raw).hexdigest(), "created_at": created.astimezone(UTC).isoformat(),
            "records": len(records), "records_sha256": rows.hexdigest(),
            "first_receive_ordinal": records[0].receive_ordinal, "last_receive_ordinal": records[-1].receive_ordinal,
            "first_received_at": records[0].received_at.isoformat(), "last_received_at": records[-1].received_at.isoformat()}


def _measure(root, scope, max_entries, check, *, copied=False, write_to=None):
    found = _pending(root, scope, max_entries, check, copied=copied)
    descriptors = []
    for relative, expected in sorted(found.items()):
        raw = _read(root / relative, expected, check)
        descriptors.append(_descriptor(relative, raw, scope, check))
        if write_to is not None:
            path = write_to / relative
            path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
            with os.fdopen(os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600), "wb") as output:
                output.write(raw)
                output.flush()
                os.fsync(output.fileno())
    if _pending(root, scope, max_entries, check, copied=copied) != found:
        raise RuntimeError("canonical_recovery_pending_spool_source_changed")
    descriptors.sort(key=lambda entry: entry["segment_ordinal"])
    if any(left["last_receive_ordinal"] >= right["first_receive_ordinal"] for left, right in zip(descriptors, descriptors[1:])):
        raise RuntimeError("canonical_recovery_pending_spool_overlapping_positions")
    return descriptors


def _roots_check(deadline, *roots):
    if monotonic() >= deadline:
        raise TimeoutError("canonical_recovery_pending_spool_timeout")
    if any(_identity(path.lstat()) != expected for path, expected in roots):
        raise RuntimeError("canonical_recovery_pending_spool_mount_changed")


def measure_pending_spool(*, source, scope, boundary, timeout_seconds, max_entries, check_held):
    """Measure the exact sealed set from a held source without a copy or filesystem mutation."""
    return _measure_source(source=source, scope=scope, boundary=_boundary(boundary), schema=_SOURCE,
                           timeout_seconds=timeout_seconds, max_entries=max_entries, check_held=check_held)


def measure_pending_spool_before_anchor(*, source, scope, database_identity, source_commit_seq,
                                       timeout_seconds, max_entries, check_held):
    """Measure held native spool bytes at a database and commit before a named anchor exists."""
    boundary = _boundary({"database_identity": database_identity, "source_commit_seq": source_commit_seq}, anchored=False)
    return _measure_source(source=source, scope=scope, boundary=boundary, schema=_PRE_ANCHOR,
                           timeout_seconds=timeout_seconds, max_entries=max_entries, check_held=check_held)


def _measure_source(*, source, scope, boundary, schema, timeout_seconds, max_entries, check_held):
    if (not isinstance(scope, PendingSpoolScope) or not callable(check_held) or type(max_entries) is not int
            or not 1 <= max_entries <= 1_000_000 or type(timeout_seconds) is not int or not 1 <= timeout_seconds <= 600):
        raise ValueError("canonical_recovery_pending_spool_invalid_limits")
    source = Path(source)
    if source.resolve(strict=True) != source or not source.is_dir():
        raise ValueError("canonical_recovery_pending_spool_invalid_source")
    require_configured_working_mount(source, require_writable=False)
    root = source, _identity(source.lstat())
    deadline = monotonic() + timeout_seconds

    def check():
        _roots_check(deadline, root)
        check_held()

    check()
    measured = {"schema": schema, "scope": asdict(scope), "boundary": boundary,
                "segments": _measure(source, scope, max_entries, check)}
    check()
    return measured | {"source_sha256": archive_evidence_hash(measured)}


def _report(path):
    with path.open("rb") as stream:
        contents = stream.read(65537)
    if len(contents) > 65536:
        raise ValueError("canonical_recovery_pending_spool_receipt_size_exceeded")
    digest = hashlib.sha256(contents).hexdigest()
    return _bound_json(path, digest), digest


def _pre_anchor_binding(pre_anchor, expected_hash, scope, boundary):
    if pre_anchor is None and expected_hash is None:
        return {}
    if (not isinstance(pre_anchor, dict) or not _sha256(expected_hash)
            or pre_anchor.keys() != {"schema", "scope", "boundary", "segments", "source_sha256"}
            or pre_anchor["schema"] != _PRE_ANCHOR or pre_anchor["source_sha256"] != expected_hash
            or archive_evidence_hash({key: value for key, value in pre_anchor.items() if key != "source_sha256"}) != expected_hash
            or pre_anchor["scope"] != asdict(scope)
            or pre_anchor["boundary"] != {key: boundary[key] for key in _SOURCE_BOUNDARY_FIELDS}):
        raise ValueError("canonical_recovery_pending_spool_pre_anchor_binding_changed")
    return {"pre_anchor_sha256": expected_hash}


def stage_pending_spool(*, source, receiving, private_scratch, scope, boundary, restic, encryption_key,
                        limits, max_entries, check_held, pre_anchor=None, expected_pre_anchor_sha256=None):
    """Preserve already sealed and unacknowledged source files under the caller's publisher hold."""
    if not isinstance(scope, PendingSpoolScope) or not callable(check_held) or type(max_entries) is not int or not 1 <= max_entries <= 1_000_000:
        raise ValueError("canonical_recovery_pending_spool_invalid_limits")
    boundary = _boundary(boundary)
    binding = _pre_anchor_binding(pre_anchor, expected_pre_anchor_sha256, scope, boundary)
    source = Path(source)
    scratch, scratch_id = _private_root(private_scratch)
    receiving, receiver_id = _private_root(receiving)
    if source.resolve(strict=True) != source or not source.is_dir():
        raise ValueError("canonical_recovery_pending_spool_invalid_source")
    require_configured_working_mount(source, require_writable=False)
    source_id = _identity(source.lstat())
    if (source_id[0] == receiver_id[0] or scratch_id[0] == receiver_id[0]
            or source.is_relative_to(receiving) or receiving.is_relative_to(source)
            or scratch.is_relative_to(source) or source.is_relative_to(scratch) or Path(encryption_key).is_relative_to(receiving)):
        raise ValueError("canonical_recovery_pending_spool_distinct_mount_required")
    deadline = monotonic() + limits.seconds
    free = shutil.disk_usage(receiving).free

    def check():
        _roots_check(deadline, (source, source_id), (scratch, scratch_id), (receiving, receiver_id))
        check_held()
        available = shutil.disk_usage(receiving).free
        if available < limits.reserve_bytes or free - available > limits.bytes:
            raise RuntimeError("canonical_recovery_pending_spool_receiving_capacity_exceeded")

    def bind_segments(segments):
        if binding and segments != pre_anchor["segments"]:
            raise RuntimeError("canonical_recovery_pending_spool_pre_anchor_bytes_changed")
        return {"schema": _VERSION, "scope": asdict(scope), "boundary": boundary, "segments": segments, **binding}

    with _receiving_lock(receiving):
        check()
        destination = receiving / _BUNDLE
        if destination.exists() or destination.is_symlink():
            published, identity = _private_root(destination)
            observed, digest = _report(published / "complete.json")
            segments = _measure(source, scope, max_entries, check)
            fields = bind_segments(segments)
            if (identity[0] != receiver_id[0] or any(observed.get(key) != value for key, value in fields.items())
                    or observed.get("pre_anchor_sha256") != expected_pre_anchor_sha256
                    or observed.get("ciphertext") != _encrypted_inventory(published / "restic", receiver_id[0], limits, check)):
                raise RuntimeError("canonical_recovery_pending_spool_existing_bundle_changed")
            return observed | {"transfer_sha256": digest}
        if shutil.disk_usage(scratch).free < scope.sealed_bytes:
            raise RuntimeError("canonical_recovery_pending_spool_scratch_capacity_exceeded")
        with (TemporaryDirectory(prefix="qt-spool-seal-", dir=scratch) as work,
              TemporaryDirectory(prefix=".qt-pending-spool-", dir=receiving) as unpublished):
            payload = Path(work) / "spool"
            payload.mkdir(mode=0o700)
            segments = _measure(source, scope, max_entries, check, write_to=payload)
            fields = bind_segments(segments)
            pending = Path(unpublished) / "bundle"
            pending.mkdir(mode=0o700)
            repository = pending / "restic"
            repository.mkdir(mode=0o700)
            listing = Path(work) / "files.list"
            listing.write_bytes(b"".join(os.fsencode(payload / entry["path"]) + b"\x00" for entry in segments))
            run = _restic_run(restic, encryption_key, repository, work, deadline, check, read_only=False)
            run("init")
            owner = "qt-" + hashlib.sha256(boundary["database_identity"].encode()).hexdigest()[:32]
            output = run("backup", "--host", owner, "--tag", boundary["anchor_sha256"], "--no-scan",
                         "--read-concurrency", "1", "--files-from-raw", str(listing))
            summaries = [row for line in output.splitlines() if (row := json.loads(line)).get("message_type") == "summary"]
            if len(summaries) != 1 or not _sha256(summaries[0].get("snapshot_id")):
                raise RuntimeError("canonical_recovery_pending_spool_snapshot_invalid")
            if segments != _measure(source, scope, max_entries, check):
                raise RuntimeError("canonical_recovery_pending_spool_source_changed")
            report = {**fields,
                      "snapshot": summaries[0]["snapshot_id"], "snapshot_spool_root": str(payload),
                      "ciphertext": _encrypted_inventory(repository, receiver_id[0], limits, check)}
            _json_write(pending / "complete.json", report)
            _, digest = _report(pending / "complete.json")
            check()
            _sync(repository)
            _sync(pending)
            pending.rename(destination)
        _sync(receiving)
    return report | {"transfer_sha256": digest}


def verify_pending_spool(*, receiving, expected_transfer_sha256, scope, boundary, inactive_restore,
                         restic, decryption_key, limits, max_entries, expected_pre_anchor_sha256=None):
    """Restore into a private inactive path and verify exact sealed bytes with no writes to a running spool."""
    if not isinstance(scope, PendingSpoolScope) or type(max_entries) is not int or not 1 <= max_entries <= 1_000_000:
        raise ValueError("canonical_recovery_pending_spool_invalid_limits")
    boundary = _boundary(boundary)
    receiving, receiver_id = _private_root(receiving)
    inactive_restore, restore_id = _private_root(inactive_restore)
    working_root = configured_working_root().resolve()
    if (receiving.is_relative_to(inactive_restore) or inactive_restore.is_relative_to(receiving)
            or inactive_restore.is_relative_to(working_root) or working_root.is_relative_to(inactive_restore)
            or Path(decryption_key).is_relative_to(receiving)):
        raise ValueError("canonical_recovery_pending_spool_inactive_path_required")
    deadline = monotonic() + limits.seconds
    bundle, bundle_id = _private_root(receiving / _BUNDLE)
    repository, repo_id = _private_root(bundle / "restic")
    receipt = _bound_json(bundle / "complete.json", expected_transfer_sha256)
    pre_anchor_sha256 = receipt.get("pre_anchor_sha256")
    if (bundle_id[0] != receiver_id[0] or repo_id[0] != receiver_id[0] or receipt.get("schema") != _VERSION
            or receipt.get("scope") != asdict(scope) or receipt.get("boundary") != _boundary(boundary)
            or (pre_anchor_sha256 is not None and not _sha256(pre_anchor_sha256))
            or (expected_pre_anchor_sha256 is not None and pre_anchor_sha256 != expected_pre_anchor_sha256)
            or not _sha256(receipt.get("snapshot"))):
        raise RuntimeError("canonical_recovery_pending_spool_receipt_scope_changed")

    def check():
        _roots_check(deadline, (receiving, receiver_id), (inactive_restore, restore_id))
        if shutil.disk_usage(inactive_restore).free < limits.reserve_bytes:
            raise RuntimeError("canonical_recovery_pending_spool_restore_reserve_exhausted")

    ciphertext = _encrypted_inventory(repository, receiver_id[0], limits, check)
    if receipt["ciphertext"] != ciphertext:
        raise RuntimeError("canonical_recovery_pending_spool_ciphertext_changed")
    result = {"schema": _VERIFIED, "boundary": receipt["boundary"], "scope": receipt["scope"],
              "transfer_sha256": expected_transfer_sha256, "snapshot": receipt["snapshot"],
              "segments": receipt["segments"]}
    if pre_anchor_sha256 is not None:
        result["pre_anchor_sha256"] = pre_anchor_sha256
    result["verification_sha256"] = archive_evidence_hash(result)
    marker = inactive_restore / "pending-spool-verification.json"
    destination = inactive_restore / "spool"
    if marker.exists() or marker.is_symlink():
        if (set(inactive_restore.iterdir()) != {marker, destination} or marker.resolve(strict=True) != marker
                or _report(marker)[0] != result
                or _measure(destination, scope, max_entries, check, copied=True) != receipt["segments"]):
            raise RuntimeError("canonical_recovery_pending_spool_existing_restore_changed")
        return result
    if any(inactive_restore.iterdir()) or shutil.disk_usage(inactive_restore).free < limits.reserve_bytes + scope.sealed_bytes:
        raise ValueError("canonical_recovery_pending_spool_inactive_path_required")
    with TemporaryDirectory(prefix=".qt-spool-restore-", dir=inactive_restore) as work:
        run = _restic_run(restic, decryption_key, repository, work, deadline, check, read_only=True)
        snapshot = receipt["snapshot"]
        metadata = json.loads(run("cat", "snapshot", snapshot), object_pairs_hook=_unique_fields)
        owner = "qt-" + hashlib.sha256(boundary["database_identity"].encode()).hexdigest()[:32]
        if metadata.get("hostname") != owner or metadata.get("tags") != [boundary["anchor_sha256"]]:
            raise RuntimeError("canonical_recovery_pending_spool_snapshot_scope_changed")
        size = json.loads(run("stats", snapshot, "--mode=restore-size"), object_pairs_hook=_unique_fields)["total_size"]
        if type(size) is not int or size != scope.sealed_bytes:
            raise RuntimeError("canonical_recovery_pending_spool_snapshot_size_changed")
        restore = Path(work) / "restored"
        run("restore", snapshot, "--target", str(restore), "--verify")
        payload = _path(restore, receipt["snapshot_spool_root"], absolute=True)
        if _measure(payload, scope, max_entries, check, copied=True) != receipt["segments"]:
            raise RuntimeError("canonical_recovery_pending_spool_restored_bytes_changed")
        for path in payload.rglob("*.sealed"):
            _sync(path)
        for directory, _, _ in os.walk(payload, topdown=False):
            _sync(Path(directory))
        _json_write(Path(work) / marker.name, result)
        check()
        payload.rename(destination)
        try:
            os.link(Path(work) / marker.name, marker)
        except OSError:
            destination.rename(payload)
            raise
    _sync(inactive_restore)
    return result
