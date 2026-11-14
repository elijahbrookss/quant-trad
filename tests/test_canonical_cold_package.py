from hashlib import sha256
import json

import pytest

from portal.backend.db.cold_history_models import COLD_CUSTODY_TABLE_NAMES
from scripts.automation import canonical_cold_package as package
from scripts.automation.storage_host_boundary import digest
from scripts.db.canonical_cold_witness import VERSION as WITNESS_VERSION


def _write(path, content):
    path.write_bytes(content)
    path.chmod(0o600)
    return sha256(content).hexdigest()


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
