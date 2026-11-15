"""Publish an exact approval package for the standalone receiver fixture."""

from dataclasses import asdict
import hashlib
import json
import os
from pathlib import Path
from tempfile import NamedTemporaryFile

from sqlalchemy import text

from market_data.archive import RawArchiveReadLimits
from market_data.archive_verification import ArchiveVerificationLimits
from market_data.fact_archive import archive_evidence_hash
from portal.backend.db.cold_history_models import COLD_CUSTODY_TABLE_NAMES
from portal.backend.service.storage.recovery_copies import _sync
from scripts.automation import canonical_cold_package as package
from scripts.automation import storage_host_boundary as host
from scripts.automation.canonical_cold_capacity_checks import VERSION as CAPACITY_VERSION
from scripts.automation.canonical_cold_collector_checks import PLAN_VERSION as COLLECTOR_VERSION
from scripts.automation.canonical_cold_named_recovery import verify_receiver_preflight
from scripts.automation.canonical_cold_receiver_operator import APPROVAL_VERSION, _fields
from scripts.automation.canonical_cold_receiver_runtime import validate_runtime_witness
from scripts.automation.canonical_cold_recovery_checks import REPLAYED_PLAN_VERSION
from scripts.automation.stage_encrypted_recovery_handoff import _encode, _private_root
from scripts.db.canonical_cold_activate import _PHYSICAL_GATES
from scripts.db.canonical_cold_collector_drain import InactiveLevel2
from scripts.db.canonical_cold_witness import measure_witnesses


def _publish(path, content):
    with NamedTemporaryFile(mode="wb", dir=path.parent, prefix=".fixture-approval-", delete=False) as stream:
        staging = Path(stream.name)
        try:
            stream.write(content)
            stream.flush()
            os.fsync(stream.fileno())
            os.link(staging, path)
        finally:
            staging.unlink()
    _sync(path.parent)
    return {"filename": path.name, "sha256": hashlib.sha256(content).hexdigest()}


def _reviewed_file(path, digest, *, limit):
    observed, content = package._hash_file(Path(path), json_limit=limit)
    if observed != digest:
        raise ValueError("fixture_approval_reviewed_artifact_changed")
    return bytes(content), json.loads(content, object_pairs_hook=package._unique_fields)


def measure_local_candidate_witness(conn, *, store, fact_version_id, dataset_id, dataset_hash, series_id):
    """Record real exact, frozen and as-of fixture results from the same SQL snapshot."""
    if not conn.in_transaction() or conn.exec_driver_sql("SHOW transaction_read_only").scalar_one() != "on":
        raise ValueError("fixture_candidate_witness_requires_readonly_transaction")
    start, end, cutoff = conn.execute(text("""
        SELECT range_start, range_end, max_commit_seq FROM market.dataset_series
        WHERE dataset_id=:dataset AND series_id=:series
    """), {"dataset": dataset_id, "series": series_id}).one()
    history = {"series_id": series_id, "start": start.isoformat(), "end": end.isoformat(),
               "known_at_lte": None, "max_rows": 32}
    return measure_witnesses(conn, [
        {"name": "exact", "mode": "exact", "ids": [fact_version_id], "max_rows": 1},
        {**history, "name": "frozen", "mode": "frozen_revisions", "dataset_id": dataset_id, "dataset_hash": dataset_hash},
        {**history, "name": "as-of", "mode": "as_of", "as_of_commit_seq": cutoff},
    ], store=store, timeout_seconds=15, max_archive_bytes=16 * 1024**2)


def _receiver_only_scope(target, tablespace_oid, checkpoint_sha256):
    # The receiver CLI never authorizes source activation using this fixture scope.
    return {
        "source_database": target, "target_database": target,
        "audit_set_hash": archive_evidence_hash({"fixture_scope": "audit", "checkpoint_sha256": checkpoint_sha256}),
        "loaded_inventory_hash": archive_evidence_hash({"fixture_scope": "inventory", "checkpoint_sha256": checkpoint_sha256}),
        "payload_children": {}, "custody_target_id": "fixture-history", "custody_tablespace_oid": tablespace_oid,
        "custody_relation_oids": {name: oid for oid, name in enumerate(COLD_CUSTODY_TABLE_NAMES, 201)},
        "raw_mapping_index": {"target_id": "fixture-history", "tablespace_oid": tablespace_oid, "table_oid": 51,
                              "index_oid": 52, "max_copy_bytes": 1024**2, "timeout_seconds": 60},
        "identity_registry": {"old_oid": 53, "new_oid": 54},
    }


def _provider_plan(scope, spool_root, pending_pin):
    inactive = None
    definition_ids = []
    if (scope is None) != (pending_pin is None):
        raise ValueError("fixture_approval_pending_scope_changed")
    if scope is not None:
        if archive_evidence_hash(asdict(scope)) != pending_pin:
            raise ValueError("fixture_approval_pending_scope_changed")
        definition_ids = [scope.definition_id]
        inactive = asdict(InactiveLevel2(
            scope, "fixture-failed", archive_evidence_hash({"fixture_event": "failed"}),
            "fixture-published", archive_evidence_hash({"fixture_event": "published"}),
        ))
    return {"schema": COLLECTOR_VERSION, "stream_ids": definition_ids, "poll_ids": [], "spool_root": str(spool_root),
            "rehearsal_sha256": archive_evidence_hash({"fixture_scope": "receiver-pending-spool"}), "inactive_level2": inactive,
            "limits": {"archive": asdict(ArchiveVerificationLimits()), "raw": asdict(RawArchiveReadLimits()),
                       "max_events": 128, "max_raw_rows": 128, "max_spool_entries": 128}}


def publish_local_receiver_operator_package(*, inbox, target_database, source_commit_floor, tablespace_oid,
                                            checkpoint_sha256, independent_recovery_path, independent_recovery_sha256,
                                            replay_equivalence_path, replay_equivalence_sha256, candidate_witness,
                                            host_inventory=None, execution_scope=None, provider_scope=None, spool_root=None):
    """Validate the full v4 package and atomically publish approval to the private receiver inbox.

    The recovery plan and runtime witness preserve the reviewed bytes verbatim.
    Source activation-only gate files are fixture artifacts. This package is
    solely for the disposable standalone receive and reopen test.
    """
    inbox, _ = _private_root(inbox)
    recovery_bytes, recovery = _reviewed_file(independent_recovery_path, independent_recovery_sha256, limit=65_536)
    witness_bytes, witness = _reviewed_file(replay_equivalence_path, replay_equivalence_sha256, limit=1024**2)
    _fields(recovery, "schema base_transfer_sha256 baseline_verification_sha256 pair_receipt_sha256 "
            "pending_spool_scope_sha256 receiver_public_key receiver_preflight replayed_checkpoint_sha256 max_restart_segments",
            "fixture_approval_recovery_plan_changed")
    validate_runtime_witness(witness)
    identity = f"{target_database['system_identifier']}/{target_database['database_oid']}"
    if (recovery["schema"] != REPLAYED_PLAN_VERSION or recovery["replayed_checkpoint_sha256"] != checkpoint_sha256
            or not package._pin(checkpoint_sha256) or witness["database_identity"] != identity
            or type(recovery["max_restart_segments"]) is not int or not 1 <= recovery["max_restart_segments"] <= 2**44):
        raise ValueError("fixture_approval_replay_evidence_changed")
    preflight = verify_receiver_preflight(recovery["receiver_preflight"], recovery["receiver_public_key"])
    site = preflight["site_policy"]
    if (site["database_identity"] != identity
            or any(site[key] != recovery[key] for key in (
                "base_transfer_sha256", "baseline_verification_sha256", "pair_receipt_sha256", "receiver_public_key",
            ))
            or preflight["pending_expected"] != (recovery["pending_spool_scope_sha256"] is not None)):
        raise ValueError("fixture_approval_recovery_plan_changed")
    scope = execution_scope if execution_scope is not None else _receiver_only_scope(
        dict(target_database), tablespace_oid, checkpoint_sha256,
    )
    if scope["target_database"] != target_database:
        raise ValueError("fixture_approval_target_scope_changed")
    inventory = host_inventory if host_inventory is not None else {"project": "local-receiver-operator-fixture", "services": {}}
    runtime = {"schema": "qt.local_receiver_operator_fixture_runtime.v1", "database_identity": identity,
               "replay_equivalence_sha256": replay_equivalence_sha256}
    fixture_gate = {"schema": "qt.local_receiver_operator_fixture_gate.v1", "database_identity": identity,
                    "checkpoint_sha256": checkpoint_sha256}
    documents = {name: _encode(fixture_gate | {"gate": name}) for name in _PHYSICAL_GATES | {"command_sheet"}}
    physical_capacity = {"schema": CAPACITY_VERSION, "rehearsal_sha256": archive_evidence_hash(fixture_gate),
                         "ssd": {"path": str(inbox.parent / "fixture-ssd"), "filesystem_uuid": "fixture-ssd",
                                 "reserve_bytes": 1, "temporary_bytes": 1},
                         "hdd": {"path": str(inbox.parent / "fixture-hdd"), "filesystem_uuid": "fixture-hdd",
                                 "reserve_bytes": 1, "temporary_bytes": scope["raw_mapping_index"]["max_copy_bytes"]
                                 + scope.get("custody_move", {}).get("max_copy_bytes", 0)}}
    documents.update(host_inventory=_encode(inventory), runtime_model=_encode(runtime), independent_recovery=recovery_bytes,
                     replay_equivalence=witness_bytes, candidate_transaction_witness=_encode(candidate_witness),
                     physical_capacity=_encode(physical_capacity),
                     provider_boundary=_encode(_provider_plan(provider_scope, spool_root or inbox,
                                                              recovery["pending_spool_scope_sha256"])))
    artifacts = {name: _publish(inbox / f"{name.replace('_', '-')}.json", content)
                 for name, content in sorted(documents.items())}
    manifest = {"schema": package.VERSION, "approved_inventory_hash": host.digest(inventory), "scope": scope,
                "source_commit_floor": source_commit_floor, "max_outage_seconds": 1200,
                "runtime_model_hash": host.digest(runtime), "artifacts": artifacts}
    path = inbox / "approved-package.json"
    digest = _publish(path, _encode(manifest))["sha256"]
    accepted = package.load_approved_package(path, approved_package_hash=digest, inventory=inventory)
    assert package.read_approved_runtime_model(path, accepted) == runtime
    _publish(inbox / "approval.json", _encode({
        "schema": APPROVAL_VERSION, "checkpoint_sha256": checkpoint_sha256, "package_filename": path.name,
        "package_sha256": digest, "inventory_filename": artifacts["host_inventory"]["filename"],
    }))
    return path, accepted, digest
