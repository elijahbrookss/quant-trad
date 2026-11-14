"""Bind the independently verified restore to the held source commit."""

import json
import os
from pathlib import Path
import re

from market_data.fact_archive import archive_evidence_hash
from portal.backend.db.fact_storage_schema import FACT_STORAGE_LAYOUT_VERSION
from scripts.automation import storage_host_boundary as host
from scripts.automation.canonical_cold_package import _hash_file, _pin, _unique_fields
from scripts.db.canonical_cold_recovery_archive_transport import _CLOSURE


PLAN_VERSION = "qt.canonical_cold_independent_plan.v1"
RECEIPT_VERSION = "qt.canonical_cold_independent_execution.v1"
_BASE_PINS = {"base_transfer_sha256", "baseline_verification_sha256", "pair_receipt_sha256"}


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


def bind_approved_recovery_checks(*, package_path, package, receipt_root, verify_site_restore):
    """Accept the receiver's native receipts for the approved prestaged backup."""
    binding = package["artifacts"]["independent_recovery"]
    plan_hash, content = _hash_file(Path(package_path).parent / binding["filename"], json_limit=65_536)
    if plan_hash != binding["sha256"]:
        raise RuntimeError("canonical_cold_independent_recovery_plan_digest_changed")
    plan = json.loads(content, object_pairs_hook=_unique_fields)
    if (not isinstance(plan, dict) or set(plan) != _BASE_PINS | {"schema", "pending_spool_scope_sha256"}
            or plan["schema"] != PLAN_VERSION or not all(_pin(plan[field]) for field in _BASE_PINS)
            or (plan["pending_spool_scope_sha256"] is not None and not _pin(plan["pending_spool_scope_sha256"]))):
        raise ValueError("canonical_cold_independent_recovery_invalid_plan")
    target = package["scope"]["target_database"]
    identity = f"{target['system_identifier']}/{target['database_oid']}"
    root = Path(receipt_root)
    root_identity = _directory(root)

    def verify(fence, control, source_commit, provider_boundary, remaining, check_archive):
        check_archive()
        if (type(source_commit) is not int or source_commit < package["source_commit_floor"]
                or not _pin(provider_boundary) or _directory(root) != root_identity):
            raise RuntimeError("canonical_cold_independent_recovery_source_boundary_changed")
        report = verify_site_restore(fence, control, source_commit, provider_boundary, remaining, check_archive)
        if not isinstance(report, dict) or set(report) != {"archive_closure", "pending_spool_source", "pending_spool_verified"}:
            raise ValueError("canonical_cold_independent_recovery_requires_native_receipts")
        closure = report["archive_closure"]
        _verify_digest(closure, field="closure_sha256", schema=_CLOSURE)
        coverage = closure.get("coverage")
        layout = closure.get("storage_layout")
        if (any(closure.get(field) != plan[field] for field in _BASE_PINS)
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
                    or received.get("scope") != source.get("scope") or not source.get("segments")
                    or received.get("segments") != source["segments"]
                    or not _pin(received.get("transfer_sha256")) or not _pin(received.get("snapshot"))):
                raise RuntimeError("canonical_cold_independent_recovery_pending_spool_boundary_changed")
        remaining()
        check_archive()
        if _directory(root) != root_identity:
            raise RuntimeError("canonical_cold_independent_recovery_receipt_directory_changed")
        evidence = {"schema": RECEIPT_VERSION, "plan_sha256": plan_hash, "database_identity": identity,
                    "source_commit_seq": source_commit, "provider_boundary_sha256": provider_boundary, **report}
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

    return verify
