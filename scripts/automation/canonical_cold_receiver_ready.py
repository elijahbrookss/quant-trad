"""Bind one prepared receiver attempt to the approved recovery site."""

import re
from time import time
from uuid import uuid4

from market_data.fact_archive import archive_evidence_hash
from scripts.automation.verify_canonical_recovery_archive import _sha256
from scripts.db.canonical_cold_replayed_checkpoint import (
    _SCHEMA as _CHECKPOINT_VERSION, _integer, _lsn, _lsn_text, _segment_number,
)


PREPARED_READY_FILENAME = "named-recovery-prepared.json"
PREPARED_READY_VERSION = "qt.named_recovery_prepared.v1"
PREPARED_READY_REPLAYED_VERSION = "qt.named_recovery_prepared.v2"
_BASE_PINS = ("base_transfer_sha256", "baseline_verification_sha256", "pair_receipt_sha256")
_NATIVE_BASELINE_VERSION = "qt.encrypted_recovery_repository_verification.v1"
_CHECKPOINT_DIGESTS = {
    "archive_verification_sha256", "catalog_inventory_sha256", "pair_receipt_sha256", "pg_controldata_sha256",
    "stopped_control_sha256", "verification_sha256", "wal_verification_sha256",
}
_CHECKPOINT_LSNS = {"checkpoint_lsn", "minimum_recovery_lsn", "redo_lsn", "replay_lsn", "target_lsn"}
_CHECKPOINT_FIELDS = _CHECKPOINT_DIGESTS | _CHECKPOINT_LSNS | {
    "schema", "database_identity", "storage_layout", "accepted_commit_seq", "catalog_objects", "catalog_bytes",
    "timeline", "wal_first", "wal_verified_through", "wal_segment_bytes", "redo_wal_segment",
}


def prepared_receiver_scope(*, approved_package_sha256, receiver_preflight_sha256, site_policy):
    scope = {"approved_package_sha256": approved_package_sha256,
             "receiver_preflight_sha256": receiver_preflight_sha256,
             **{field: site_policy[field] for field in ("database_identity", *_BASE_PINS)}}
    if (not all(map(_sha256, (approved_package_sha256, receiver_preflight_sha256, *(scope[field] for field in _BASE_PINS))))
            or re.fullmatch(r"[1-9][0-9]*/[1-9][0-9]*", str(scope["database_identity"])) is None):
        raise ValueError("canonical_named_recovery_invalid_prepared_scope")
    return scope


def _verify_original_baseline(baseline):
    if (not isinstance(baseline, dict) or baseline.get("schema") != _NATIVE_BASELINE_VERSION
            or "anchor_sha256" not in baseline or baseline["anchor_sha256"] is not None
            or not isinstance(baseline.get("wal_backup_stop"), str)
            or baseline.get("wal_verified_through") != baseline["wal_backup_stop"]
            or not all(_sha256(baseline.get(field)) for field in ("pair_receipt_sha256", "verification_sha256"))
            or archive_evidence_hash({key: value for key, value in baseline.items() if key != "verification_sha256"})
            != baseline["verification_sha256"]):
        raise ValueError("canonical_named_recovery_prepared_original_baseline_changed")
    stop, segment_bytes = baseline["wal_backup_stop"], baseline.get("wal_segment_bytes")
    if (re.fullmatch(r"[0-9A-F]{24}", stop) is None
            or not _integer(segment_bytes, minimum=2**20, maximum=2**30)
            or segment_bytes & (segment_bytes - 1)):
        raise ValueError("canonical_named_recovery_prepared_original_baseline_changed")
    timeline = int(stop[:8], 16)
    per_log = 2**32 // segment_bytes
    first = _segment_number(baseline.get("wal_start"), timeline, per_log)
    last = _segment_number(stop, timeline, per_log)
    if timeline == 0 or first > last:
        raise ValueError("canonical_named_recovery_prepared_original_baseline_changed")
    return first, last, timeline


def verify_replayed_checkpoint_baseline(checkpoint, *, baseline_verification, expected_checkpoint_sha256):
    """Validate an exact witness against the pinned native base, returning the transfer endpoint."""
    baseline = baseline_verification
    first, backup_stop, baseline_timeline = _verify_original_baseline(baseline)
    if (not isinstance(checkpoint, dict) or set(checkpoint) != _CHECKPOINT_FIELDS
            or checkpoint["schema"] != _CHECKPOINT_VERSION
            or not all(_sha256(checkpoint[field]) for field in _CHECKPOINT_DIGESTS)
            or checkpoint["verification_sha256"] != expected_checkpoint_sha256
            or archive_evidence_hash({key: value for key, value in checkpoint.items() if key != "verification_sha256"})
            != expected_checkpoint_sha256):
        raise ValueError("canonical_named_recovery_prepared_checkpoint_digest_changed")
    if (any(checkpoint[field] != baseline.get(field) for field in ("database_identity", "pair_receipt_sha256", "storage_layout"))
            or not isinstance(checkpoint["database_identity"], str)
            or re.fullmatch(r"[1-9][0-9]{0,19}/[1-9][0-9]{0,9}", checkpoint["database_identity"]) is None
            or checkpoint["wal_first"] != baseline.get("wal_start")
            or checkpoint["wal_segment_bytes"] != baseline.get("wal_segment_bytes")
            or not all(_integer(checkpoint[field], minimum=0, maximum=2**63 - 1)
                       for field in ("accepted_commit_seq", "catalog_objects", "catalog_bytes"))):
        raise ValueError("canonical_named_recovery_prepared_checkpoint_scope_changed")
    layout = checkpoint["storage_layout"]
    segment_bytes = checkpoint["wal_segment_bytes"]
    timeline = checkpoint["timeline"]
    if (not isinstance(layout, dict) or set(layout) != {"layout_version", "certificate_sha256"}
            or not isinstance(layout["layout_version"], str) or not layout["layout_version"]
            or not _sha256(layout["certificate_sha256"])
            or type(timeline) is not int or timeline != baseline_timeline
            or type(segment_bytes) is not int):
        raise ValueError("canonical_named_recovery_prepared_checkpoint_scope_invalid")

    positions = {field: _lsn(checkpoint[field]) for field in _CHECKPOINT_LSNS}
    target, replay = positions["target_lsn"], positions["replay_lsn"]
    redo, minimum = positions["redo_lsn"], positions["minimum_recovery_lsn"]
    if (any(checkpoint[field] != _lsn_text(position) for field, position in positions.items())
            or not 0 < target < replay or not 0 < redo <= positions["checkpoint_lsn"] < minimum <= replay):
        raise ValueError("canonical_named_recovery_prepared_checkpoint_lsn_changed")

    per_log = 2**32 // segment_bytes
    through = _segment_number(checkpoint["wal_verified_through"], timeline, per_log)
    redo_segment = _segment_number(checkpoint["redo_wal_segment"], timeline, per_log)
    first_required = min(redo, target) // segment_bytes
    last_replayed = (replay - 1) // segment_bytes
    if (backup_stop >= through or not first <= first_required <= last_replayed
            or redo_segment != redo // segment_bytes or last_replayed != through):
        raise ValueError("canonical_named_recovery_prepared_checkpoint_wal_coverage_changed")
    return checkpoint["wal_verified_through"]


def make_prepared_receiver_ready(*, scope, materialized_sha256, expires_at, replayed_checkpoint=None,
                                 baseline_verification=None):
    schema = PREPARED_READY_VERSION if replayed_checkpoint is None else PREPARED_READY_REPLAYED_VERSION
    body = {"schema": schema, **scope, "materialized_sha256": materialized_sha256,
            "attempt_id": uuid4().hex, "expires_at": expires_at}
    if replayed_checkpoint is not None:
        body["replayed_checkpoint"] = replayed_checkpoint
    verify_prepared_receiver_ready(body, scope=scope, baseline_verification=baseline_verification)
    return body


def verify_prepared_receiver_ready(body, *, scope, baseline_verification=None):
    """Accept an exact, current body after verification of its receiver signature."""
    schema = body.get("schema") if isinstance(body, dict) else None
    replayed = schema == PREPARED_READY_REPLAYED_VERSION
    fields = set(scope) | {"schema", "materialized_sha256", "attempt_id", "expires_at"}
    if replayed:
        fields.add("replayed_checkpoint")
    if (schema not in (PREPARED_READY_VERSION, PREPARED_READY_REPLAYED_VERSION) or set(body) != fields
            or any(body[key] != value for key, value in scope.items())
            or not _sha256(body["materialized_sha256"])
            or not isinstance(body["attempt_id"], str) or re.fullmatch(r"[0-9a-f]{32}", body["attempt_id"]) is None
            or type(body["expires_at"]) is not int):
        raise ValueError("canonical_named_recovery_prepared_scope_changed")
    if body["expires_at"] <= time():
        raise TimeoutError("canonical_named_recovery_prepared_receiver_expired")
    if replayed:
        _match_scope_to_baseline(scope, baseline_verification)
        checkpoint = body["replayed_checkpoint"]
        expected = checkpoint.get("verification_sha256") if isinstance(checkpoint, dict) else None
        verify_replayed_checkpoint_baseline(checkpoint, baseline_verification=baseline_verification,
                                            expected_checkpoint_sha256=expected)
    return archive_evidence_hash(body)


def _match_scope_to_baseline(scope, baseline):
    if (not isinstance(baseline, dict) or baseline.get("verification_sha256") != scope["baseline_verification_sha256"]
            or any(baseline.get(field) != scope[field] for field in ("database_identity", "pair_receipt_sha256"))):
        raise ValueError("canonical_named_recovery_prepared_original_baseline_changed")
