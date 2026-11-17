"""Bind a stopped PostgreSQL receiver to a previously verified replay endpoint."""

from __future__ import annotations

from collections.abc import Mapping
import hashlib
import json
import re


_SCHEMA = "qt.replayed_recovery_checkpoint.v1"
_ARCHIVE_SCHEMA = "qt.paused_archive_suffix.v1"
_CONTROL_FIELDS = (
    "pg_control version number",
    "Database cluster state",
    "Database system identifier",
    "Latest checkpoint location",
    "Latest checkpoint's REDO location",
    "Latest checkpoint's REDO WAL file",
    "Latest checkpoint's TimeLineID",
    "Minimum recovery ending location",
    "Min recovery ending loc's timeline",
    "Bytes per WAL segment",
)


def _hash(receipt):
    try:
        encoded = json.dumps(receipt, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False)
    except (TypeError, ValueError) as exc:
        raise ValueError("replayed_checkpoint_receipt_not_json") from exc
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def _matches(value, pattern):
    return isinstance(value, str) and re.fullmatch(pattern, value) is not None


def _digest(value):
    return _matches(value, r"[0-9a-f]{64}")


def _integer(value, *, minimum, maximum):
    return type(value) is int and minimum <= value <= maximum


def _lsn(value):
    if not _matches(value, r"[0-9A-Fa-f]{1,8}/[0-9A-Fa-f]{1,8}"):
        raise ValueError("replayed_checkpoint_lsn_invalid")
    high, low = value.split("/")
    return (int(high, 16) << 32) | int(low, 16)


def _lsn_text(value):
    return f"{value >> 32:X}/{value & 0xffffffff:X}"


def _segment_number(name, timeline, segments_per_log):
    if (not _matches(name, r"[0-9A-F]{24}") or int(name[:8], 16) != timeline
            or int(name[16:], 16) >= segments_per_log):
        raise ValueError("replayed_checkpoint_wal_segment_invalid")
    return int(name[8:16], 16) * segments_per_log + int(name[16:], 16)


def _control_fields(output):
    if not isinstance(output, str) or not 0 < len(output) <= 65536 or not output.isascii():
        raise ValueError("replayed_checkpoint_native_control_invalid")
    fields = {}
    for line in output.splitlines():
        name, separator, value = line.partition(":")
        # PostgreSQL 15 reports a bad control CRC on stdout, then prints fields.
        if name.strip() == "WARNING":
            raise ValueError("replayed_checkpoint_native_control_warning")
        if not separator or name not in _CONTROL_FIELDS:
            continue
        if name in fields:
            raise ValueError("replayed_checkpoint_native_control_duplicate_field")
        fields[name] = value.strip()
    if any(not fields.get(name) for name in _CONTROL_FIELDS):
        raise ValueError("replayed_checkpoint_native_control_missing_field")
    return fields


def _endpoint_scope(receipt):
    return {
        "database_identity": receipt.get("database_identity"),
        "timeline": receipt.get("timeline"),
        "target_lsn": _lsn(receipt.get("target_lsn")),
        "replay_lsn": _lsn(receipt.get("replay_lsn")),
        "accepted_commit_seq": receipt.get("accepted_commit_seq"),
        "storage_layout": receipt.get("storage_layout"),
    }


def build_replayed_checkpoint(*, pair_receipt_sha256, database_identity, timeline,
                              wal_first, wal_verified_through, wal_segment_bytes, wal_verification_sha256,
                              paused_receipt, archive_receipt, stopped_control, stopped_control_sha256):
    """Check the inputs to restarting the receiver from verified WAL.

    The caller already holds the exact receiver and retained WAL under exclusive
    custody, and has verified contiguous native contents through both named range
    bounds inclusive. ``paused_receipt`` gives the observed endpoint scope and
    ``archive_receipt`` is the matching ``qt.paused_archive_suffix.v1`` JSON
    receipt. The caller obtained LC_ALL=C pg_controldata stdout and the binary
    ``global/pg_control`` SHA256 from the same unchanged, successfully stopped
    receiver, checking pg_controldata exit status and stderr. This function
    matches those proofs and does no I/O or authorization.
    """
    if (not all(_digest(value) for value in (pair_receipt_sha256, wal_verification_sha256, stopped_control_sha256))
            or not _matches(database_identity, r"[1-9][0-9]{0,19}/[1-9][0-9]{0,9}")
            or not _integer(timeline, minimum=1, maximum=2**32 - 1)
            or not _integer(wal_segment_bytes, minimum=2**20, maximum=2**30)
            or wal_segment_bytes & (wal_segment_bytes - 1)):
        raise ValueError("replayed_checkpoint_scope_invalid")
    system_identifier, database_oid = database_identity.split("/")
    if int(system_identifier) >= 2**64 or int(database_oid) >= 2**32:
        raise ValueError("replayed_checkpoint_database_identity_invalid")

    if not isinstance(paused_receipt, Mapping) or not isinstance(archive_receipt, Mapping):
        raise ValueError("replayed_checkpoint_endpoint_receipt_invalid")
    endpoint = _endpoint_scope(paused_receipt)
    archive_endpoint = _endpoint_scope(archive_receipt)
    layout = endpoint["storage_layout"]
    if (endpoint != archive_endpoint or endpoint["database_identity"] != database_identity
            or endpoint["timeline"] != timeline or type(endpoint["timeline"]) is not int
            or type(archive_endpoint["timeline"]) is not int
            or not _integer(endpoint["accepted_commit_seq"], minimum=0, maximum=2**63 - 1)
            or type(archive_endpoint["accepted_commit_seq"]) is not int
            or not isinstance(layout, dict) or set(layout) != {"layout_version", "certificate_sha256"}
            or not isinstance(layout["layout_version"], str) or not layout["layout_version"]
            or not _digest(layout["certificate_sha256"])):
        raise ValueError("replayed_checkpoint_endpoint_mismatch")
    target, replay = endpoint["target_lsn"], endpoint["replay_lsn"]
    if not 0 < target < replay:
        raise ValueError("replayed_checkpoint_endpoint_lsn_order_invalid")

    archive_body = dict(archive_receipt)
    archive_digest = archive_body.pop("verification_sha256", None)
    if (archive_body.get("schema") != _ARCHIVE_SCHEMA or archive_body.get("pair_receipt_sha256") != pair_receipt_sha256
            or not _digest(archive_digest) or not _digest(archive_body.get("catalog_inventory_sha256"))
            or not all(_integer(archive_body.get(key), minimum=0, maximum=2**63 - 1)
                       for key in ("catalog_objects", "catalog_bytes"))
            or archive_digest != _hash(archive_body)):
        raise ValueError("replayed_checkpoint_archive_receipt_invalid")

    control = _control_fields(stopped_control)
    if (control["pg_control version number"] != "1300"
            or control["Database cluster state"] != "shut down in recovery"
            or control["Database system identifier"] != system_identifier
            or control["Latest checkpoint's TimeLineID"] != str(timeline)
            or control["Min recovery ending loc's timeline"] != str(timeline)
            or control["Bytes per WAL segment"] != str(wal_segment_bytes)):
        raise ValueError("replayed_checkpoint_native_control_scope_changed")
    redo = _lsn(control["Latest checkpoint's REDO location"])
    checkpoint = _lsn(control["Latest checkpoint location"])
    minimum = _lsn(control["Minimum recovery ending location"])
    if not 0 < redo <= checkpoint < minimum <= replay:
        raise ValueError("replayed_checkpoint_native_lsn_order_invalid")

    segments_per_log = 2**32 // wal_segment_bytes
    first = _segment_number(wal_first, timeline, segments_per_log)
    through = _segment_number(wal_verified_through, timeline, segments_per_log)
    redo_name = control["Latest checkpoint's REDO WAL file"]
    redo_segment = redo // wal_segment_bytes
    if _segment_number(redo_name, timeline, segments_per_log) != redo_segment:
        raise ValueError("replayed_checkpoint_native_redo_segment_changed")
    # REDO, checkpoint and target are positions. Replay and minRecoveryPoint are
    # end pointers, so an aligned end uses the previous segment's final byte.
    first_required = min(redo_segment, target // wal_segment_bytes)
    last_required = (replay - 1) // wal_segment_bytes
    if not first <= first_required <= last_required <= through:
        raise ValueError("replayed_checkpoint_wal_coverage_incomplete")

    receipt = endpoint | {
        "schema": _SCHEMA,
        "target_lsn": _lsn_text(target),
        "replay_lsn": _lsn_text(replay),
        "storage_layout": layout.copy(),
        "pair_receipt_sha256": pair_receipt_sha256,
        "archive_verification_sha256": archive_digest,
        "catalog_inventory_sha256": archive_body["catalog_inventory_sha256"],
        "catalog_objects": archive_body["catalog_objects"],
        "catalog_bytes": archive_body["catalog_bytes"],
        "wal_first": wal_first,
        "wal_verified_through": wal_verified_through,
        "wal_segment_bytes": wal_segment_bytes,
        "wal_verification_sha256": wal_verification_sha256,
        "stopped_control_sha256": stopped_control_sha256,
        "pg_controldata_sha256": hashlib.sha256(stopped_control.encode("ascii")).hexdigest(),
        "checkpoint_lsn": _lsn_text(checkpoint),
        "redo_lsn": _lsn_text(redo),
        "redo_wal_segment": redo_name,
        "minimum_recovery_lsn": _lsn_text(minimum),
    }
    return receipt | {"verification_sha256": _hash(receipt)}
