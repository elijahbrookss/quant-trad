"""Bind the independently verified restore to the held source commit."""

from collections.abc import Callable
from dataclasses import dataclass
import json
import os
from pathlib import Path
import re

from market_data.fact_archive import archive_evidence_hash
from portal.backend.db.fact_storage_schema import FACT_STORAGE_LAYOUT_VERSION
from scripts.automation import storage_host_boundary as host
from scripts.automation.canonical_cold_collector_checks import RECEIPT_VERSION as COLLECTOR_RECEIPT_VERSION
from scripts.automation.canonical_cold_named_recovery import _RESPONSE, verify_receiver_preflight, verify_signed_recovery
from scripts.automation.canonical_cold_package import _hash_file, _pin, _unique_fields
from scripts.automation.canonical_cold_receiver_ready import prepared_receiver_scope, verify_prepared_receiver_ready
from scripts.db.canonical_cold_recovery_archive_transport import _CLOSURE
from scripts.db.canonical_cold_recovery_wal_transfer import check_source_wal_capacity


PLAN_VERSION = "qt.canonical_cold_independent_plan.v2"
RECEIPT_VERSION = "qt.canonical_cold_independent_execution.v2"
_BASE_PINS = {"base_transfer_sha256", "baseline_verification_sha256", "pair_receipt_sha256"}


@dataclass(frozen=True)
class _ApprovedRecoveryChecks:
    assert_receiver_ready: Callable[[], str]
    verify: Callable
    assert_source_wal_capacity: Callable

    def __call__(self, fence, control, source_commit, provider_boundary, remaining, check_archive):
        return self.verify(fence, control, source_commit, provider_boundary, remaining, check_archive)


def _verify_digest(record, *, field, schema):
    if (not isinstance(record, dict) or record.get("schema") != schema or not _pin(record.get(field))
            or archive_evidence_hash({key: value for key, value in record.items() if key != field}) != record[field]):
        raise RuntimeError(f"canonical_cold_independent_recovery_receipt_changed: {schema}")


def _directory(path):
    if not path.is_absolute() or path.resolve(strict=True) != path or not path.is_dir():
        raise ValueError("canonical_cold_independent_recovery_invalid_receipt_directory")
    info = path.stat()
    if info.st_uid != os.getuid() or info.st_mode & 0o077:
        raise ValueError("canonical_cold_independent_recovery_requires_private_receipts")
    return info.st_dev, info.st_ino


def bind_approved_recovery_checks(*, package_path, package, approved_package_hash, receipt_root, verify_site_restore,
                                  expected_pending_scope_sha256):
    """Accept the receiver's native receipts for the approved prestaged backup."""
    package_path = Path(package_path)
    package_hash, package_content = _hash_file(package_path, json_limit=65_536)
    if package_hash != approved_package_hash or json.loads(package_content, object_pairs_hook=_unique_fields) != package:
        raise RuntimeError("canonical_cold_independent_recovery_package_changed")
    binding = package["artifacts"]["independent_recovery"]
    plan_hash, content = _hash_file(package_path.parent / binding["filename"], json_limit=65_536)
    if plan_hash != binding["sha256"]:
        raise RuntimeError("canonical_cold_independent_recovery_plan_digest_changed")
    plan = json.loads(content, object_pairs_hook=_unique_fields)
    if (not isinstance(plan, dict) or set(plan) != _BASE_PINS | {"schema", "pending_spool_scope_sha256", "receiver_public_key", "receiver_preflight"}
            or plan["schema"] != PLAN_VERSION or not all(_pin(plan[field]) for field in _BASE_PINS)
            or not _pin(plan["receiver_public_key"])
            or (plan["pending_spool_scope_sha256"] is not None and not _pin(plan["pending_spool_scope_sha256"]))):
        raise ValueError("canonical_cold_independent_recovery_invalid_plan")
    if plan["pending_spool_scope_sha256"] != expected_pending_scope_sha256:
        raise ValueError("canonical_cold_independent_recovery_collector_scope_changed")
    target = package["scope"]["target_database"]
    identity = f"{target['system_identifier']}/{target['database_oid']}"
    measurement = verify_receiver_preflight(plan["receiver_preflight"], plan["receiver_public_key"])
    site = measurement["site_policy"]
    if (any(site.get(field) != plan[field] for field in _BASE_PINS) or site.get("database_identity") != identity
            or measurement["pending_expected"] != (expected_pending_scope_sha256 is not None)):
        raise RuntimeError("canonical_cold_independent_recovery_preflight_scope_changed")
    root = Path(receipt_root)
    root_identity = _directory(root)
    readiness_scope = prepared_receiver_scope(
        approved_package_sha256=package_hash, receiver_preflight_sha256=measurement["measurement_sha256"], site_policy=site,
    )
    prepared = None

    def assert_receiver_ready():
        nonlocal prepared
        if _directory(root) != root_identity:
            raise RuntimeError("canonical_cold_independent_recovery_receipt_directory_changed")
        check_ready = getattr(verify_site_restore, "assert_receiver_ready", None)
        if not callable(check_ready):
            raise RuntimeError("canonical_cold_independent_recovery_prepared_receiver_required")
        envelope = check_ready()
        ready_sha256 = verify_prepared_receiver_ready(
            verify_signed_recovery(envelope, plan["receiver_public_key"]), scope=readiness_scope,
        )
        if prepared is not None and prepared[0] != ready_sha256:
            raise RuntimeError("canonical_cold_independent_recovery_prepared_attempt_changed")
        prepared = ready_sha256, envelope
        return ready_sha256

    def assert_source_wal_capacity(control, *, expected_database, timeout_seconds):
        baseline = getattr(verify_site_restore, "wal_baseline", None)
        _verify_digest(baseline, field="verification_sha256", schema="qt.encrypted_recovery_repository_verification.v1")
        if (prepared is None or baseline["verification_sha256"] != plan["baseline_verification_sha256"]
                or baseline.get("pair_receipt_sha256") != plan["pair_receipt_sha256"]
                or baseline.get("database_identity") != identity
                or baseline.get("anchor_sha256") is not None
                or baseline.get("wal_verified_through") != baseline.get("wal_backup_stop")
                or any(expected_database[key] != target[key] for key in ("database", "system_identifier", "database_oid"))):
            raise RuntimeError("canonical_cold_independent_recovery_wal_baseline_changed")
        return check_source_wal_capacity(
            control, expected_database=expected_database, backup_stop=baseline["wal_backup_stop"],
            segment_bytes=baseline["wal_segment_bytes"], max_segments=site["wal"]["files"], timeout_seconds=timeout_seconds,
        )

    def verify(fence, control, source_commit, provider_boundary, remaining, check_archive):
        if prepared is None:
            raise RuntimeError("canonical_cold_independent_recovery_receiver_not_prepared_before_stop")
        check_archive()
        if (type(source_commit) is not int or source_commit < package["source_commit_floor"]
                or not _pin(provider_boundary) or _directory(root) != root_identity):
            raise RuntimeError("canonical_cold_independent_recovery_source_boundary_changed")
        provider = host.load_receipt(root / f"collector-drained-{provider_boundary}.json", max_bytes=128 * 1024**2)
        _verify_digest(provider, field="sha256", schema=COLLECTOR_RECEIPT_VERSION)
        provider_evidence = provider.get("evidence")
        if (provider["sha256"] != provider_boundary or provider.get("phase") != "drained"
                or not isinstance(provider_evidence, dict) or provider_evidence.get("source_commit_seq") != source_commit):
            raise RuntimeError("canonical_cold_independent_recovery_collector_boundary_changed")
        pending_before_anchor = provider_evidence.get("pending_spool")
        if expected_pending_scope_sha256 is None:
            if pending_before_anchor is not None:
                raise RuntimeError("canonical_cold_independent_recovery_unapproved_pending_spool")
        else:
            _verify_digest(pending_before_anchor, field="source_sha256", schema="qt.pending_raw_spool_source_before_anchor.v1")
            if (pending_before_anchor.get("boundary") != {"database_identity": identity, "source_commit_seq": source_commit}
                    or archive_evidence_hash(pending_before_anchor.get("scope")) != expected_pending_scope_sha256):
                raise RuntimeError("canonical_cold_independent_recovery_collector_pending_scope_changed")
        report = verify_site_restore(fence, control, source_commit, provider_boundary, remaining, check_archive)
        if not isinstance(report, dict) or set(report) != {"archive_closure", "pending_spool_source", "pending_spool_verified", "receiver_response"}:
            raise ValueError("canonical_cold_independent_recovery_requires_native_receipts")
        reply = verify_signed_recovery(report["receiver_response"], plan["receiver_public_key"])
        if (set(reply) != {"schema", "request_sha256", "archive_closure", "pending_spool_verified", "runtime_sha256",
                           "approved_package_sha256", "provider_boundary_sha256", "receiver_preflight_sha256",
                           "prepared_receiver_sha256"}
                or reply["schema"] != _RESPONSE or not _pin(reply["request_sha256"])
                or reply["approved_package_sha256"] != package_hash or reply["provider_boundary_sha256"] != provider_boundary
                or reply["receiver_preflight_sha256"] != measurement["measurement_sha256"]
                or reply["prepared_receiver_sha256"] != prepared[0]
                or any(reply[key] != report[key] for key in ("archive_closure", "pending_spool_verified"))):
            raise RuntimeError("canonical_cold_independent_recovery_signed_boundary_changed")
        closure = report["archive_closure"]
        _verify_digest(closure, field="closure_sha256", schema=_CLOSURE)
        coverage = closure.get("coverage")
        layout = closure.get("storage_layout")
        if (any(closure.get(field) != plan[field] for field in _BASE_PINS)
                or not _pin(reply.get("runtime_sha256")) or closure.get("receiver_runtime_sha256") != reply["runtime_sha256"]
                or closure.get("database_identity") != identity or closure.get("source_commit_seq") != source_commit
                or not isinstance(layout, dict) or layout.get("layout_version") != FACT_STORAGE_LAYOUT_VERSION
                or not _pin(layout.get("certificate_sha256"))
                or re.fullmatch(r"qt_original_[a-f0-9]{32}", str(closure.get("restore_name"))) is None
                or re.fullmatch(r"[0-9A-F]{24}", str(closure.get("wal_segment"))) is None
                or re.fullmatch(r"[0-9A-F]+/[0-9A-F]+", str(closure.get("replay_lsn"))) is None
                or not all(_pin(closure.get(field)) for field in (
                    "anchor_sha256", "archive_transfer_sha256", "native_named_sha256", "wal_transfer_sha256",
                )) or not isinstance(coverage, dict)
                or not all(_pin(coverage.get(field)) for field in ("archive_snapshot", "final_inventory_sha256"))
                or any(type(coverage.get(field)) is not int or coverage[field] < 0 for field in (
                    "final_objects", "final_bytes", "baseline_objects_used", "verified_suffix_objects",
                )) or coverage["final_objects"] != coverage["baseline_objects_used"] + coverage["verified_suffix_objects"]):
            raise RuntimeError("canonical_cold_independent_recovery_named_boundary_changed")
        source, received = report["pending_spool_source"], report["pending_spool_verified"]
        if plan["pending_spool_scope_sha256"] is None:
            if source is not None or received is not None:
                raise RuntimeError("canonical_cold_independent_recovery_unapproved_pending_spool")
        else:
            _verify_digest(source, field="source_sha256", schema="qt.pending_raw_spool_source.v1")
            _verify_digest(received, field="verification_sha256", schema="qt.pending_raw_spool_verified.v1")
            expected = {"database_identity": identity, "source_commit_seq": source_commit, "anchor_sha256": closure["anchor_sha256"]}
            if (source.get("boundary") != expected or received.get("boundary") != expected
                    or archive_evidence_hash(source.get("scope")) != plan["pending_spool_scope_sha256"]
                    or source.get("segments") != pending_before_anchor.get("segments")
                    or received.get("pre_anchor_sha256") != pending_before_anchor["source_sha256"]
                    or received.get("scope") != source.get("scope") or not source.get("segments")
                    or received.get("segments") != source["segments"]
                    or not _pin(received.get("transfer_sha256")) or not _pin(received.get("snapshot"))):
                raise RuntimeError("canonical_cold_independent_recovery_pending_spool_boundary_changed")
        remaining()
        check_archive()
        if _directory(root) != root_identity:
            raise RuntimeError("canonical_cold_independent_recovery_receipt_directory_changed")
        evidence = {"schema": RECEIPT_VERSION, "plan_sha256": plan_hash, "database_identity": identity,
                    "source_commit_seq": source_commit, "provider_boundary_sha256": provider_boundary,
                    "receiver_preflight_sha256": measurement["measurement_sha256"],
                    "prepared_receiver_sha256": prepared[0], "prepared_receiver": prepared[1],
                    "pending_spool_before_anchor_sha256": None if pending_before_anchor is None else pending_before_anchor["source_sha256"],
                    **report}
        digest = archive_evidence_hash(evidence)
        artifact = {**evidence, "sha256": digest}
        path = root / f"independent-recovery-{digest}.json"
        try:
            host.save_receipt(path, artifact, initial=True)
        except FileExistsError:
            if host.load_receipt(path, max_bytes=128 * 1024**2) != artifact:
                raise RuntimeError("canonical_cold_independent_recovery_execution_receipt_changed")
        remaining()
        check_archive()
        return digest

    return _ApprovedRecoveryChecks(assert_receiver_ready, verify, assert_source_wal_capacity)
