from hashlib import sha256
import json

import pytest

from portal.backend.db.cold_history_models import COLD_CUSTODY_TABLE_NAMES
from scripts.automation import canonical_cold_package as package
from scripts.automation.storage_host_boundary import digest


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
        "scope": {
            "source_database": identity, "target_database": identity,
            "audit_set_hash": "a" * 64, "loaded_inventory_hash": "b" * 64, "payload_children": {},
            "custody_target_id": "history", "custody_tablespace_oid": 50,
            "custody_relation_oids": {name: index for index, name in enumerate(COLD_CUSTODY_TABLE_NAMES, 80)},
        },
        "artifacts": {},
    }
    for name in package._PHYSICAL_GATES | {"command_sheet", "host_inventory"}:
        filename = f"{name}.json"
        data = json.dumps(inventory if name == "host_inventory" else {"exact_fixture_evidence_for": name}).encode()
        manifest["artifacts"][name] = {"filename": filename, "sha256": _write(tmp_path / filename, data)}
    path = tmp_path / "approval.json"
    approved = _write(path, json.dumps(manifest).encode())
    return path, approved, inventory


def test_only_current_boundary_fields_can_change_in_approved_scope(artifacts):
    path, approved, inventory = artifacts
    reviewed = package.load_approved_package(path, approved_package_hash=approved, inventory=inventory)
    execution, execution_hash = package.bind_execution(
        reviewed, approved_package_hash=approved, source_commit_seq=105,
        provider_boundary="c" * 64, independent_recovery="d" * 64,
    )
    assert execution["approved_package_hash"] == approved
    assert execution["source_commit_seq"] == 105
    assert execution["evidence"]["provider_boundary"] == "c" * 64
    assert execution["evidence"]["independent_recovery"] == "d" * 64
    assert execution["audit_set_hash"] == reviewed["scope"]["audit_set_hash"]
    assert len(execution_hash) == 64
    with pytest.raises(ValueError, match="final_boundary_missing"):
        package.bind_execution(reviewed, approved_package_hash=approved, source_commit_seq=99,
                               provider_boundary="c" * 64, independent_recovery="d" * 64)


def test_changed_evidence_or_inventory_refuses_the_package(artifacts):
    path, approved, inventory = artifacts
    with pytest.raises(ValueError, match="approval_mismatch"):
        package.load_approved_package(path, approved_package_hash="e" * 64, inventory=inventory)
    with pytest.raises(ValueError, match="host_inventory_changed"):
        package.load_approved_package(path, approved_package_hash=approved, inventory={"unexpected": True})
    _write(path.with_name("independent_recovery.json"), b"changed evidence")
    with pytest.raises(RuntimeError, match="artifact_digest_changed: independent_recovery"):
        package.load_approved_package(path, approved_package_hash=approved, inventory=inventory)
