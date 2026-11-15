"""Hold the bounded archive WAL interval needed to restart a replayed receiver."""

from dataclasses import dataclass
import os
from pathlib import Path
import stat

from market_data.fact_archive import archive_evidence_hash
from scripts.automation.canonical_cold_receiver_ready import verify_replayed_checkpoint_baseline
from scripts.automation.stage_encrypted_recovery_handoff import _file, _identity, _private_root, _safe_member, _stamp
from scripts.automation.verify_encrypted_recovery_handoff import _archive_filenames, _bound_json, _sha256, _wal_range
from scripts.db.canonical_cold_recovery_wal_transfer import (
    _baseline, _MAX_SEGMENTS, _prefix_binding, _prefix_path, _prefix_window,
)


_PREFIX_VERIFICATION = "qt.named_recovery_wal_prefix_verification.v1"
_SCHEMA = "qt.named_recovery_retained_wal.v1"


@dataclass(frozen=True)
class RetainedWALFile:
    segment: str
    wal_checksum: str
    path: str
    stamp: tuple[int, ...]


@dataclass(frozen=True)
class RetainedWAL:
    receiving: Path
    archive_id: str
    root_identity: tuple[int, int]
    directories: tuple
    files: tuple[RetainedWALFile, ...]
    sha256: str

    def assert_held(self, check):
        """Recheck archive names and metadata without reading any file contents."""
        segments = tuple(row.segment for row in self.files)

        def assert_namespace():
            check()
            if (_private_root(self.receiving)[1] != self.root_identity
                    or _directories(self.receiving, self.archive_id, segments, self.root_identity[0]) != self.directories):
                raise RuntimeError("canonical_retained_wal_archive_changed")

        try:
            assert_namespace()
            if _observe(self.receiving, self.archive_id, segments, self.root_identity[0], check) != self.files:
                raise RuntimeError("canonical_retained_wal_archive_changed")
            assert_namespace()
        except (OSError, ValueError) as error:
            raise RuntimeError("canonical_retained_wal_archive_changed") from error


def _directories(receiving, archive_id, segments, device):
    root = receiving / "database" / "archive" / "qt" / archive_id
    paths = (receiving / "database", receiving / "database" / "archive", root.parent, root,
             *(root / name for name in sorted({segment[:16] for segment in segments})))
    identities = []
    for path in paths:
        info = _safe_member(path, device)
        if not stat.S_ISDIR(info.st_mode) or path.resolve(strict=True) != path:
            raise ValueError("canonical_retained_wal_invalid_archive_directory")
        identities.append((path.relative_to(receiving).as_posix(), _identity(info)))
    return tuple(identities)


def _observe(receiving, archive_id, segments, device, check):
    rows = []
    for segment, checksum, path in _archive_filenames(receiving / "database", archive_id, segments, check):
        info = _safe_member(path, device)
        if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1 or info.st_size < 1:
            raise ValueError("canonical_retained_wal_invalid_file")
        stamp = (*_stamp(info), info.st_mode, info.st_uid, info.st_gid)
        rows.append(RetainedWALFile(segment, checksum, path.relative_to(receiving).as_posix(), stamp))
    check()
    return tuple(rows)


def _prefixes(baseline, checkpoint, verification, envelopes, check):
    """Match envelopes already authenticated by verify_wal_prefix_chain to its exact digest."""
    if not isinstance(verification, dict) or not isinstance(envelopes, (list, tuple)):
        raise ValueError("canonical_retained_wal_prefix_chain_required")
    expected = {"pair_receipt_sha256": baseline["pair_receipt_sha256"],
                "baseline_verification_sha256": baseline["verification_sha256"],
                **{key: checkpoint[key] for key in ("wal_first", "wal_verified_through", "wal_segment_bytes")}}
    if (any(verification.get(key) != value for key, value in expected.items())
            or not _sha256(verification.get("prepared_receiver_sha256"))
            or verification.get("wal_verification_sha256") != checkpoint["wal_verification_sha256"]
            or type(verification.get("prefix_batches")) is not int
            or not 1 <= verification["prefix_batches"] == len(envelopes)
            or type(verification.get("prefix_segments")) is not int
            or verification["prefix_segments"] < len(envelopes)):
        raise ValueError("canonical_retained_wal_prefix_chain_changed")

    bodies, hashes = [], []
    after = baseline["wal_backup_stop"]
    previous = None
    prefix_segments = 0
    for envelope in envelopes:
        check()
        body = envelope.get("body") if isinstance(envelope, dict) else None
        if (not isinstance(body, dict) or body.get("schema") != _PREFIX_VERIFICATION
                or body.get("prepared_receiver_sha256") != verification["prepared_receiver_sha256"]
                or body.get("pair_receipt_sha256") != baseline["pair_receipt_sha256"]
                or body.get("baseline_verification_sha256") != baseline["verification_sha256"]
                or body.get("after") != after or body.get("previous_prefix_sha256") != previous
                or not _sha256(body.get("wal_prefix_sha256")) or not _sha256(body.get("verified_wal_sha256"))):
            raise ValueError("canonical_retained_wal_prefix_chain_changed")
        names = _prefix_window(baseline, after, body.get("through"), _MAX_SEGMENTS, previous)
        if type(body.get("segments")) is not int or body["segments"] != len(names):
            raise ValueError("canonical_retained_wal_prefix_chain_changed")
        if names[-1] >= checkpoint["redo_wal_segment"]:
            bodies.append((body, names))
        hashes.append(archive_evidence_hash(envelope))
        prefix_segments += len(names)
        after, previous = names[-1], body["wal_prefix_sha256"]

    verification_body = {key: value for key, value in verification.items() if key != "wal_verification_sha256"}
    if (after != checkpoint["wal_verified_through"] or prefix_segments != verification["prefix_segments"]
            or archive_evidence_hash(verification_body | {"receipts": hashes}) != checkpoint["wal_verification_sha256"]):
        raise ValueError("canonical_retained_wal_prefix_chain_changed")
    return bodies


def _base_checksums(receiving, baseline, required, max_segments, check):
    if required[0] > baseline["wal_backup_stop"]:
        return {}
    base = _wal_range(baseline["wal_start"], baseline["wal_backup_stop"], baseline["wal_segment_bytes"], max_segments)
    entries = [[name, digest] for name, digest, _ in
               _archive_filenames(receiving / "database", baseline["database_archive_id"], base, check)]
    if (type(baseline.get("wal_segments")) is not int or len(entries) != baseline["wal_segments"]
            or archive_evidence_hash(entries) != baseline.get("wal_inventory_sha256")):
        raise ValueError("canonical_retained_wal_base_inventory_changed")
    return {name: digest for name, digest in entries if name >= required[0]}


def _prefix_files(receiving, baseline, base_transfer_sha256, overlapping, required, check):
    files = {}
    binding = _prefix_binding(base_transfer_sha256, baseline["verification_sha256"])
    for body, names in overlapping:
        check()
        receipt = _bound_json(_prefix_path(receiving, body["through"]), body["wal_prefix_sha256"])
        expected = binding | {"wal_after": body["after"], "wal_transferred_through": body["through"],
                              "previous_prefix_sha256": body["previous_prefix_sha256"]}
        if not isinstance(receipt, dict) or any(receipt.get(key) != value for key, value in expected.items()):
            raise ValueError("canonical_retained_wal_prefix_receipt_changed")
        rows = receipt.get("files")
        if (not isinstance(rows, list) or len(rows) != len(names)
                or any(not isinstance(row, dict) or row.get("segment") != name
                       or not isinstance(row.get("wal_checksum"), str) or len(row["wal_checksum"]) != 40
                       for row, name in zip(rows, names))
                or archive_evidence_hash([[row["segment"], row["wal_checksum"]] for row in rows]) != body["verified_wal_sha256"]):
            raise ValueError("canonical_retained_wal_prefix_native_inventory_changed")
        files.update((row["segment"], row) for row in rows if row["segment"] >= required[0])
    return files


def capture_retained_wal(*, receiving, baseline, base_transfer_sha256, checkpoint, checkpoint_sha256,
                         wal_verification, prefix_receipts, max_segments, native_verify, check):
    """Seal restart WAL before readiness and return a metadata-only custody check.

    Authenticate ``prefix_receipts`` with the receiver's key via
    ``verify_wal_prefix_chain`` first and pass its result as ``wal_verification``.
    ``native_verify`` runs pgBackRest archive-get in the same receiving repository
    for only the listed files and returns ordered [segment, SHA1] rows after
    checking plaintext segment sizes. The caller keeps custody of the receiver
    and its private restore configuration. The cap applies to the restart
    interval and to metadata verification of the original base if it overlaps.
    """
    if type(max_segments) is not int or max_segments < 1 or not all(map(callable, (native_verify, check))):
        raise ValueError("canonical_retained_wal_invalid_budget")
    check()
    receiving, root_identity = _private_root(receiving)
    through = verify_replayed_checkpoint_baseline(checkpoint, baseline_verification=baseline,
                                                   expected_checkpoint_sha256=checkpoint_sha256)
    _baseline(receiving, base_transfer_sha256, baseline, baseline["verification_sha256"])
    required = _wal_range(checkpoint["redo_wal_segment"], through, baseline["wal_segment_bytes"], max_segments)
    overlapping = _prefixes(baseline, checkpoint, wal_verification, prefix_receipts, check)
    base = _base_checksums(receiving, baseline, required, max_segments, check)
    prefix = _prefix_files(receiving, baseline, base_transfer_sha256, overlapping, required, check)
    archive_id = baseline["database_archive_id"]
    directories = _directories(receiving, archive_id, required, root_identity[0])
    files = _observe(receiving, archive_id, required, root_identity[0], check)
    for row in files:
        check()
        if row.segment in base:
            if row.wal_checksum != base[row.segment]:
                raise ValueError("canonical_retained_wal_base_file_changed")
            continue
        receipt = prefix.get(row.segment)
        if (receipt is None or any(receipt.get(key) != getattr(row, key) for key in ("path", "wal_checksum"))
                or type(receipt.get("bytes")) is not int or receipt["bytes"] != row.stamp[2]
                or not _sha256(receipt.get("sha256"))
                or _file(receiving / row.path, row.stamp[:6], check) != {key: receipt[key] for key in ("bytes", "sha256")}):
            raise ValueError("canonical_retained_wal_prefix_file_changed")

    wal = {"wal_segment_bytes": baseline["wal_segment_bytes"],
           "files": [{"segment": row.segment, "path": row.path, "wal_checksum": row.wal_checksum} for row in files]}
    if native_verify(wal) != [[row.segment, row.wal_checksum] for row in files]:
        raise ValueError("canonical_retained_wal_native_verification_changed")
    evidence = {"schema": _SCHEMA, "base_transfer_sha256": base_transfer_sha256, "checkpoint_sha256": checkpoint_sha256,
                "wal_verification_sha256": checkpoint["wal_verification_sha256"], "root_identity": root_identity,
                "archive_id": archive_id, "directories": directories,
                "files": [[row.segment, row.path, row.wal_checksum, row.stamp] for row in files]}
    held = RetainedWAL(receiving, archive_id, root_identity, directories, files, archive_evidence_hash(evidence))
    held.assert_held(check)
    return held
