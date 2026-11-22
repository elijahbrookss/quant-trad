from contextlib import nullcontext
from datetime import UTC, datetime
from hashlib import sha256
import json
import os
from pathlib import Path
from types import SimpleNamespace
from time import monotonic, time

import pytest
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

from portal.backend.db.cold_history_models import COLD_CUSTODY_TABLE_NAMES
from portal.backend.db.fact_storage_schema import FACT_STORAGE_LAYOUT_VERSION
from scripts.automation import canonical_cold_collector_checks as collectors
from scripts.automation import canonical_cold_named_recovery as named
from scripts.automation import canonical_cold_package as package
from scripts.automation import canonical_cold_receiver_ready as ready
from scripts.automation import canonical_cold_recovery_checks as recovery
from scripts.automation.storage_host_boundary import digest, load_receipt
from scripts.db.canonical_cold_witness import VERSION as WITNESS_VERSION
from tests.test_canonical_cold_receiver_ready import replayed_receiver_evidence


def _write(path, content):
    path.write_bytes(content)
    path.chmod(0o600)
    return sha256(content).hexdigest()


@pytest.mark.skipif(os.name != "posix", reason="requires POSIX private recovery directories")
@pytest.mark.parametrize(("version", "accepted"), [
    (b"pgBackRest 2.59.1", True), (b"pgBackRest 2.59.3", True),
    (b"pgBackRest 2.59.2", False), (b"pgBackRest 2.59.4", False),
])
def test_named_restore_checks_qualified_reader_before_data_writes(tmp_path, monkeypatch, version, accepted):
    workspace, pgdata = tmp_path / "workspace", tmp_path / "pgdata"
    workspace.mkdir(mode=0o700)
    pgdata.mkdir(mode=0o700)
    key = tmp_path / "database-key"
    _write(key, b"1" * 64)
    monkeypatch.setattr(named, "_binary", Path)

    def native(command, **kwargs):
        assert command == ["/fixture/pgbackrest", "version"]
        kwargs["check"]()
        return version

    def manifest(*args):
        raise RuntimeError("fixture_native_manifest_reached")

    monkeypatch.setattr(named, "_run_native", native)
    monkeypatch.setattr(named, "_selected_backup_manifest", manifest)
    reason = "fixture_native_manifest_reached" if accepted else "canonical_named_recovery_pgbackrest_unqualified"
    with pytest.raises(RuntimeError, match=reason):
        named._native_restore(
            root=tmp_path / "repository", workspace=workspace, pgdata=pgdata, tablespaces={},
            pgbackrest="/fixture/pgbackrest", database_key=key, pair={}, unarmed="fixture_unarmed",
            budget=SimpleNamespace(seconds=30, reserve_bytes=0, bytes=2**30),
            deadline=monotonic() + 30, check=lambda: None,
        )
    assert not any(pgdata.iterdir())
    assert not (workspace / "pgbackrest.conf").exists()


def test_approved_collector_plan_persists_both_boundary_receipts(tmp_path, monkeypatch):
    spool = tmp_path / "spool"
    receipts = tmp_path / "private-receipts"
    spool.mkdir()
    receipts.mkdir(mode=0o700)
    plan = {
        "schema": collectors.PLAN_VERSION, "stream_ids": ["fixture-feed"], "poll_ids": ["fixture-poll"],
        "spool_root": str(spool), "limits": {
            "archive": {"max_objects": 3, "max_bytes": 4096}, "raw": {"max_rows": 5}, "max_events": 5,
            "max_raw_rows": 15, "max_spool_entries": 100,
        }, "rehearsal_sha256": "f" * 64, "inactive_level2": None,
    }
    plan_path = tmp_path / "provider.json"
    plan_hash = _write(plan_path, json.dumps(plan).encode())
    approved = {"scope": {"target_database": {"system_identifier": "123", "database_oid": 42}},
                "artifacts": {"provider_boundary": {"filename": plan_path.name, "sha256": plan_hash}}}
    monkeypatch.setattr(collectors, "require_configured_working_mount", lambda path: None)

    class Connection:
        def begin(self):
            return nullcontext()

        def exec_driver_sql(self, command):
            assert command == "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY"

        def execute(self, statement, params):
            assert str(statement) == "SELECT set_config('statement_timeout', :ms, true)" and params == {"ms": "15000"}

    connection = Connection()
    store = object()

    def capture(target, *, stream_ids, poll_ids, check, inactive_level2):
        assert target is connection and stream_ids == ("fixture-feed",) and poll_ids == ("fixture-poll",)
        assert inactive_level2 is None
        check()
        return {"phase": "pre_stop", "source_commit_seq": 41}

    def verify(target, *, before, source_commit_seq, spool_root, store, limits, check, inactive_level2, database_identity):
        assert target is connection and before["source_commit_seq"] == 41 and source_commit_seq == 45
        assert spool_root == spool and limits.max_raw_rows == 15
        assert inactive_level2 is None and database_identity == "123/42"
        check()
        return {"phase": "drained", "source_commit_seq": source_commit_seq}

    monkeypatch.setattr(collectors, "capture_collector_tail", capture)
    monkeypatch.setattr(collectors, "verify_collector_drain", verify)
    starts = []

    def native_restart(target, *, drained, started_at, store, limits, check, spool_root):
        assert target is connection and drained["source_commit_seq"] == 45 and started_at.tzinfo is UTC
        assert spool_root == spool
        starts.append(started_at)
        if len(starts) == 1:
            raise collectors.CollectorRestartPending("scripted qualification")
        return {"phase": "restarted", "source_commit_seq": 46}

    monkeypatch.setattr(collectors, "verify_collector_restart", native_restart)
    monkeypatch.setattr(collectors, "sleep", lambda _: None)
    capture_check, verify_check, restarted_check, pending_scope = collectors.bind_approved_collector_checks(
        connection, package_path=tmp_path / "package.json", package=approved, store=store, receipt_root=receipts,
    )
    assert pending_scope is None
    before = capture_check(lambda: 20)
    with pytest.raises(RuntimeError, match="execution_tail_changed"):
        verify_check(connection, 45, {**before, "evidence": {"source_commit_seq": 42}})
    drained_hash = verify_check(connection, 45, before)
    assert verify_check(connection, 45, before) == drained_hash
    clear = load_receipt(receipts / f"collector-drained-{drained_hash}.json")
    assert clear["plan_sha256"] == before["plan_sha256"] == plan_hash
    assert clear["evidence"]["execution_pre_stop_sha256"] == before["sha256"]
    start = datetime.now(UTC)
    runtime = {"market-data-collector": {"running": True, "started_at": start.isoformat()}}
    restarted_hash = restarted_check(runtime)
    final = load_receipt(receipts / f"collector-restarted-{restarted_hash}.json")
    assert starts == [start, start] and final["evidence"]["execution_drained_sha256"] == drained_hash
    spool.rename(tmp_path / "moved-spool")
    spool.mkdir()
    with pytest.raises(RuntimeError, match="execution_directory_changed"):
        verify_check(connection, 45, before)
    _write(plan_path, b"{}")
    with pytest.raises(RuntimeError, match="plan_digest_changed"):
        collectors.bind_approved_collector_checks(
            connection, package_path=tmp_path / "package.json", package=approved, store=store, receipt_root=receipts,
        )


@pytest.mark.parametrize("pending", (False, True))
@pytest.mark.parametrize("replayed", (False, True))
def test_independent_report_binds_exact_approved_base_anchor_and_pending_bytes(tmp_path, monkeypatch, pending, replayed):
    root = tmp_path / "private-receipts"
    root.mkdir(mode=0o700)
    plan = {"schema": recovery.PLAN_VERSION, "base_transfer_sha256": "a" * 64,
            "baseline_verification_sha256": "b" * 64, "pair_receipt_sha256": "c" * 64}
    checkpoint = baseline = None
    if replayed:
        _, baseline, checkpoint = replayed_receiver_evidence()
        plan.update(schema=recovery.REPLAYED_PLAN_VERSION, baseline_verification_sha256=baseline["verification_sha256"],
                    pair_receipt_sha256=baseline["pair_receipt_sha256"],
                    replayed_checkpoint_sha256=checkpoint["verification_sha256"], max_restart_segments=3)
    scope = {"definition_id": "fixture-level2", "session_id": "old", "sealed_files": 2}
    plan["pending_spool_scope_sha256"] = digest(scope) if pending else None
    receiver = Ed25519PrivateKey.generate()
    public = receiver.public_key().public_bytes_raw().hex()
    roles = ("exchange", "workspace", "pgdata")
    capacity = {"roles": list(roles), "required_bytes": 18, "required_entries": 3, "reserve_bytes": 4,
                "available_bytes": 100, "available_entries": 10}
    measurement = {"schema": named.PREFLIGHT_VERSION, "receiver_host_sha256": "1" * 64,
        "source_host_sha256": "2" * 64, "pending_expected": pending,
        "receiver_mounts": {role: {"device_id": "1:2", "block_size": 4} for role in roles},
        "requirements": {role: {"checkpoint_bytes": 2, "entries": 1, "reserve_bytes": 4} for role in roles},
        "capacity_by_device": {"1:2": capacity},
        "site_policy": {"database_identity": "123/42", "receiver_public_key": public, "wal": {"files": 1},
                        **{key: plan[key] for key in recovery._BASE_PINS}}}
    measured = digest(measurement)
    plan.update(receiver_public_key=public, receiver_preflight=named._sign(measurement | {"measurement_sha256": measured}, receiver))
    impossible = {**measurement, "capacity_by_device": {"1:2": {**capacity, "available_entries": 2}}}
    with pytest.raises(ValueError, match="invalid_preflight_capacity"):
        named.verify_receiver_preflight(named._sign(impossible | {"measurement_sha256": digest(impossible)}, receiver), public)
    path = tmp_path / "independent.json"
    target_database = {"database": "fixture", "system_identifier": "123", "database_oid": 42}
    accepted = {"source_commit_floor": 40, "scope": {"target_database": target_database},
                "artifacts": {"independent_recovery": {"filename": path.name, "sha256": _write(path, json.dumps(plan).encode())}}}
    package_path = tmp_path / "package.json"
    package_hash = _write(package_path, json.dumps(accepted).encode())
    readiness_scope = ready.prepared_receiver_scope(
        approved_package_sha256=package_hash, receiver_preflight_sha256=measured, site_policy=measurement["site_policy"],
    )
    prepared = ready.make_prepared_receiver_ready(
        scope=readiness_scope, materialized_sha256="b" * 64, expires_at=int(time()) + 3600,
        replayed_checkpoint=checkpoint, baseline_verification=baseline,
    )
    signed_prepared = named._sign(prepared, receiver)
    active_preparation = signed_prepared
    boundary = {"database_identity": "123/42", "source_commit_seq": 45, "anchor_sha256": "d" * 64}
    segments = [{"id": "first", "records_sha256": "1" * 64}, {"id": "second", "records_sha256": "2" * 64}]
    archive = {
        "schema": recovery._CLOSURE, **boundary, **{key: plan[key] for key in recovery._BASE_PINS},
        "storage_layout": {"layout_version": FACT_STORAGE_LAYOUT_VERSION, "certificate_sha256": "6" * 64},
        "restore_name": "qt_original_" + "d" * 32,
        "wal_segment": "00000001000000000000000F", "replay_lsn": "0/F441",
        "archive_transfer_sha256": "e" * 64, "native_named_sha256": "f" * 64, "wal_transfer_sha256": "1" * 64,
        "receiver_runtime_sha256": "8" * 64,
        "coverage": {"archive_snapshot": "2" * 64, "final_inventory_sha256": "3" * 64, "final_objects": 20,
                     "final_bytes": 30000, "baseline_objects_used": 19, "verified_suffix_objects": 1},
    }
    if replayed:
        archive["checkpoint_sha256"] = plan["replayed_checkpoint_sha256"]
    observed = {"schema": "qt.pending_raw_spool_source.v1", "scope": scope, "boundary": boundary, "segments": segments}
    before_anchor = {"schema": "qt.pending_raw_spool_source_before_anchor.v1", "scope": scope,
                     "boundary": {key: value for key, value in boundary.items() if key != "anchor_sha256"}, "segments": segments}
    received = {"schema": "qt.pending_raw_spool_verified.v1", "scope": scope, "boundary": boundary, "segments": segments,
                "transfer_sha256": "4" * 64, "snapshot": "5" * 64, "pre_anchor_sha256": digest(before_anchor)}
    report = {"archive_closure": {**archive, "closure_sha256": digest(archive)},
              "pending_spool_source": {**observed, "source_sha256": digest(observed)} if pending else None,
              "pending_spool_verified": {**received, "verification_sha256": digest(received)} if pending else None}
    provider = {"schema": collectors.RECEIPT_VERSION, "phase": "drained", "plan_sha256": "7" * 64,
                "evidence": {"source_commit_seq": 45, "pending_spool":
                             {**before_anchor, "source_sha256": digest(before_anchor)} if pending else None}}
    provider_digest = digest(provider)
    _write(root / f"collector-drained-{provider_digest}.json", json.dumps({**provider, "sha256": provider_digest}).encode())

    def acknowledge(**overrides):
        body = {"schema": named._RESPONSE, "request_sha256": "a" * 64, "runtime_sha256": archive["receiver_runtime_sha256"],
                "approved_package_sha256": package_hash, "provider_boundary_sha256": provider_digest,
                "receiver_preflight_sha256": measured, "prepared_receiver_sha256": digest(prepared),
                **{key: report[key] for key in ("archive_closure", "pending_spool_verified")}}
        report["receiver_response"] = named._sign(body | overrides, receiver)

    acknowledge()
    def restore(*args):
        return report

    restore.assert_receiver_ready = lambda: active_preparation
    restore.wal_baseline = baseline
    restore.restart_budget = plan.get("max_restart_segments")
    options = dict(package_path=package_path, package=accepted, approved_package_hash=package_hash,
                   receipt_root=root, verify_site_restore=restore,
                   expected_pending_scope_sha256=plan["pending_spool_scope_sha256"])
    checker = recovery.bind_approved_recovery_checks(**options)
    arguments = (None, None, 45, provider_digest, lambda: 30, lambda: None)
    with pytest.raises(RuntimeError, match="receiver_not_prepared_before_stop"):
        checker(*arguments)
    if replayed:
        original = ready.make_prepared_receiver_ready(scope=readiness_scope, materialized_sha256="b" * 64,
                                                       expires_at=int(time()) + 3600)
        active_preparation = named._sign(original, receiver)
        with pytest.raises(ValueError, match="unapproved_checkpoint"):
            checker.assert_receiver_ready()
    active_preparation = named._sign({**prepared, "expires_at": int(time()) - 1}, receiver)
    with pytest.raises(TimeoutError, match="prepared_receiver_expired"):
        checker.assert_receiver_ready()
    active_preparation = signed_prepared
    assert checker.assert_receiver_ready() == digest(prepared)
    if replayed:
        def capacity(control, *, expected_database, backup_stop, restart_from, max_restart_segments,
                     segment_bytes, max_segments, timeout_seconds):
            assert expected_database == target_database and timeout_seconds == 5
            assert (backup_stop, restart_from) == (checkpoint["wal_verified_through"], checkpoint["redo_wal_segment"])
            assert max_restart_segments == 3 and segment_bytes == 2**24 and max_segments == 1
            return 1

        monkeypatch.setattr(recovery, "check_source_wal_capacity", capacity)
        assert checker.assert_source_wal_capacity(None, expected_database=target_database, timeout_seconds=5) == 1
    active_preparation = named._sign({**prepared, "attempt_id": "f" * 32}, receiver)
    with pytest.raises(RuntimeError, match="prepared_attempt_changed"):
        checker.assert_receiver_ready()
    active_preparation = signed_prepared
    certificate = checker(*arguments)
    assert checker(*arguments) == certificate
    proof = load_receipt(root / f"independent-recovery-{certificate}.json")
    assert proof["source_commit_seq"] == 45 and proof["provider_boundary_sha256"] == provider_digest
    assert proof["archive_closure"] == report["archive_closure"]
    assert proof["receiver_preflight_sha256"] == measured
    assert proof["prepared_receiver"] == signed_prepared and proof["prepared_receiver_sha256"] == digest(prepared)
    for field in ("approved_package_sha256", "provider_boundary_sha256", "receiver_preflight_sha256", "prepared_receiver_sha256"):
        acknowledge(**{field: "9" * 64})
        with pytest.raises(RuntimeError, match="signed_boundary_changed"):
            checker(*arguments)
    acknowledge()
    report["receiver_response"] = named._sign(report["receiver_response"]["body"], Ed25519PrivateKey.generate())
    with pytest.raises(ValueError, match="signature_mismatch"):
        checker(*arguments)
    changed = {**archive, "source_commit_seq": 46}
    report["archive_closure"] = {**changed, "closure_sha256": digest(changed)}
    acknowledge()
    with pytest.raises(RuntimeError, match="named_boundary_changed"):
        checker(*arguments)
    if replayed:
        omitted = {key: value for key, value in archive.items() if key != "checkpoint_sha256"}
        report["archive_closure"] = {**omitted, "closure_sha256": digest(omitted)}
        acknowledge()
        with pytest.raises(RuntimeError, match="named_boundary_changed"):
            checker(*arguments)
    report["archive_closure"] = {**archive, "closure_sha256": digest(archive)}
    changed = {**received, "segments": segments[:1]}
    report["pending_spool_verified"] = {**changed, "verification_sha256": digest(changed)}
    acknowledge()
    with pytest.raises(RuntimeError, match="pending_spool_boundary_changed|unapproved_pending_spool"):
        checker(*arguments)
    with pytest.raises(ValueError, match="collector_scope_changed"):
        recovery.bind_approved_recovery_checks(**(options | {"expected_pending_scope_sha256": "9" * 64}))
    _write(package_path, json.dumps(accepted, indent=2).encode())
    with pytest.raises(RuntimeError, match="package_changed"):
        recovery.bind_approved_recovery_checks(**options)


@pytest.fixture
def artifacts(tmp_path):
    inventory = {"project": "fixture", "services": {}}
    identity = {"database": "qt_fixture", "database_oid": 22, "system_identifier": "13", "header_oid": 33}
    manifest = {
        "schema": package.VERSION, "source_commit_floor": 100, "max_outage_seconds": 1200,
        "approved_inventory_hash": digest(inventory),
        "runtime_model_hash": digest({"exact_fixture_evidence_for": "runtime_model"}),
        "scope": {
            "source_database": identity, "target_database": identity,
            "audit_set_hash": "a" * 64, "loaded_inventory_hash": "b" * 64, "payload_children": {},
            "custody_target_id": "history", "custody_tablespace_oid": 50,
            "custody_relation_oids": {name: index for index, name in enumerate(COLD_CUSTODY_TABLE_NAMES, 80)},
            "raw_mapping_index": {"target_id": "history", "tablespace_oid": 50, "table_oid": 90, "index_oid": 91,
                                  "max_copy_bytes": 1024**3, "timeout_seconds": 120},
            "identity_registry": {"old_oid": 92, "new_oid": 93},
        },
        "artifacts": {},
    }
    common = {"expected_rows": 1, "expected_hash": "1" * 64, "max_rows": 2}
    history = {**common, "series_id": 1, "known_at_lte": None,
               "start": "2026-01-01T00:00:00Z", "end": "2026-01-02T00:00:00Z"}
    witness = {"schema": WITNESS_VERSION, "max_archive_bytes": 1024**2, "cases": [
        {**common, "name": "identity", "mode": "exact", "ids": ["fact-id"]},
        {**history, "name": "frozen", "mode": "frozen_revisions", "dataset_id": "dataset", "dataset_hash": "2" * 64},
        {**history, "name": "as-of", "mode": "as_of", "as_of_commit_seq": 1},
    ]}
    content = {"host_inventory": inventory, "candidate_transaction_witness": witness}
    for name in package._PHYSICAL_GATES | {"command_sheet", "host_inventory", "runtime_model"}:
        filename = f"{name}.json"
        data = json.dumps(content.get(name, {"exact_fixture_evidence_for": name})).encode()
        manifest["artifacts"][name] = {"filename": filename, "sha256": _write(tmp_path / filename, data)}
    path = tmp_path / "approval.json"
    approved = _write(path, json.dumps(manifest).encode())
    return path, approved, inventory


def test_only_current_boundary_fields_can_change_in_approved_scope(artifacts):
    path, approved, inventory = artifacts
    reviewed = package.load_approved_package(path, approved_package_hash=approved, inventory=inventory)
    assert package.read_approved_runtime_model(path, reviewed) == {"exact_fixture_evidence_for": "runtime_model"}
    execution, execution_hash = package.bind_execution(
        reviewed, approved_package_hash=approved, source_commit_seq=105,
        provider_boundary="c" * 64, independent_recovery="d" * 64,
    )
    assert execution["approved_package_hash"] == approved
    assert execution["source_commit_seq"] == 105
    assert execution["evidence"]["provider_boundary"] == "c" * 64
    assert execution["evidence"]["independent_recovery"] == "d" * 64
    witness = package.read_approved_candidate_witness(path, reviewed)
    assert sha256(witness).hexdigest() == execution["evidence"]["candidate_transaction_witness"]
    assert execution["audit_set_hash"] == reviewed["scope"]["audit_set_hash"]
    assert execution["raw_mapping_index"] == reviewed["scope"]["raw_mapping_index"]
    assert execution["identity_registry"] == reviewed["scope"]["identity_registry"]
    assert len(execution_hash) == 64
    with pytest.raises(ValueError, match="final_boundary_missing"):
        package.bind_execution(reviewed, approved_package_hash=approved, source_commit_seq=99,
                               provider_boundary="c" * 64, independent_recovery="d" * 64)

    for placement in ({}, {**reviewed["scope"]["raw_mapping_index"], "tablespace_oid": 1663},
                      {**reviewed["scope"]["raw_mapping_index"], "max_copy_bytes": 0}):
        unbound = {**reviewed, "scope": {**reviewed["scope"], "raw_mapping_index": placement}}
        with pytest.raises(ValueError, match="raw_index_scope_invalid"):
            package.bind_execution(unbound, approved_package_hash=approved, source_commit_seq=105,
                                   provider_boundary="c" * 64, independent_recovery="d" * 64)
    unbound = {**reviewed, "scope": {**reviewed["scope"], "identity_registry": {"old_oid": 92, "new_oid": 92}}}
    with pytest.raises(ValueError, match="identity_registry_scope_invalid"):
        package.bind_execution(unbound, approved_package_hash=approved, source_commit_seq=105,
                               provider_boundary="c" * 64, independent_recovery="d" * 64)


def test_reviewed_custody_move_requires_exact_source_and_shared_target(artifacts):
    path, approved, inventory = artifacts
    reviewed = package.load_approved_package(path, approved_package_hash=approved, inventory=inventory)
    raw = reviewed["scope"]["raw_mapping_index"]
    movement = {"source_placement_hash": "f" * 64, "max_copy_bytes": 2**30, "timeout_seconds": 60}
    scope = {**reviewed["scope"], "custody_move": movement,
             "custody_tablespace_oid": raw["tablespace_oid"], "custody_target_id": raw["target_id"]}
    approved = _write(path, json.dumps({**reviewed, "scope": scope}).encode())
    reviewed = package.load_approved_package(path, approved_package_hash=approved, inventory=inventory)
    boundary = {"approved_package_hash": approved, "source_commit_seq": 105,
                "provider_boundary": "c" * 64, "independent_recovery": "d" * 64}
    assert package.bind_execution(reviewed, **boundary)[0]["custody_move"] == movement
    for change in ({"custody_tablespace_oid": raw["tablespace_oid"] + 1},
                   {"custody_move": {**movement, "source_placement_hash": "invalid"}},
                   {"custody_move": None}):
        with pytest.raises(ValueError, match="custody_move_scope_invalid"):
            package.bind_execution({**reviewed, "scope": {**scope, **change}}, **boundary)


def test_changed_evidence_or_inventory_refuses_the_package(artifacts):
    path, approved, inventory = artifacts
    reviewed = package.load_approved_package(path, approved_package_hash=approved, inventory=inventory)
    original_witness = package.read_approved_candidate_witness(path, reviewed)
    _write(path.with_name("candidate_transaction_witness.json"), b"changed witness")
    with pytest.raises(RuntimeError, match="candidate_witness_changed"):
        package.read_approved_candidate_witness(path, reviewed)
    _write(path.with_name("candidate_transaction_witness.json"), original_witness)
    with pytest.raises(ValueError, match="approval_mismatch"):
        package.load_approved_package(path, approved_package_hash="e" * 64, inventory=inventory)
    with pytest.raises(ValueError, match="host_inventory_changed"):
        package.load_approved_package(path, approved_package_hash=approved, inventory={"unexpected": True})
    _write(path.with_name("independent_recovery.json"), b"changed evidence")
    with pytest.raises(RuntimeError, match="artifact_digest_changed: independent_recovery"):
        package.load_approved_package(path, approved_package_hash=approved, inventory=inventory)
