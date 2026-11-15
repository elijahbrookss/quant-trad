"""Qualify original-name donor admission on a private restarted hot standby."""
from hashlib import sha256
from io import BytesIO
import json
import os
from pathlib import Path

import pytest
from sqlalchemy.exc import DBAPIError

from market_data.fact_archive import archive_evidence_hash
from scripts.db.canonical_cold_paused_donor import open_paused_donor
from scripts.db.canonical_cold_rollback_cohort import _require_snapshot


pytestmark = pytest.mark.db


def _receipt(writable, served, name, body):
    receipt = body | {"verification_sha256": archive_evidence_hash(body)}
    payload = (json.dumps(receipt, sort_keys=True) + "\n").encode()
    (writable / name).write_bytes(payload)
    return served / name, sha256(payload).hexdigest()


@pytest.fixture
def original_donor():
    if os.environ.get("QT_PRIVATE_PAUSED_DONOR_FIXTURE") != "1":
        pytest.skip("requires the isolated original-layout hot standby fixture")
    writable = Path("/fixture-receipts-write")
    served = Path("/fixture-receipts")
    native = json.loads((served / "native.json").read_bytes())
    pgdata, history, archive = Path("/donor/pgdata"), Path("/donor/history"), Path("/late-archive")
    device = pgdata.stat().st_dev
    device_id = f"{os.major(device)}:{os.minor(device)}"
    certificate = {"layout_version": "market.fact_storage_tiers.v2",
                   "completed_at": "2026-10-19T00:00:00+00:00", "evidence": {"fixture": "donor"}}
    digest = sha256(json.dumps(certificate, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()).hexdigest()
    boundary = {"database_identity": f"{native['system_identifier']}/{native['database_oid']}",
                "device_id": device_id, "timeline": 1, "target_lsn": native["target_lsn"],
                "replay_lsn": native["replay_lsn"], "accepted_commit_seq": 7, "pair_receipt_sha256": "1" * 64,
                "storage_layout": {"layout_version": certificate["layout_version"], "certificate_sha256": digest}}
    scope = {"inventory_sha256": "2" * 64, "objects": 1, "bytes": 1}
    binding = "3" * 64
    copy, copy_sha = _receipt(writable, served, "copy.json", {
        "schema": "qt.stopped_rehearsal_database_copy.v1", "binding_sha256": binding,
        "system_identifier": native["system_identifier"], "timeline": 1, "device_id": device_id,
        "tablespace_oid": native["tablespace_oid"], "source": {"pgdata": str(pgdata), "history": str(history),
            "pgdata_inode": pgdata.stat().st_ino, "history_inode": history.stat().st_ino,
            "control": {"sha256": native["stopped_control_sha256"]}, "metadata_sha256": "4" * 64},
    })
    suffix, suffix_sha = _receipt(writable, served, "suffix.json", boundary | {
        "schema": "qt.paused_archive_suffix.v1", "materialize_baseline": True, "suffix_inode": archive.stat().st_ino,
        "catalog_inventory_sha256": scope["inventory_sha256"], "catalog_objects": scope["objects"],
        "catalog_bytes": scope["bytes"],
        "materialized_inventory_sha256": scope["inventory_sha256"], "materialized_objects": scope["objects"],
        "materialized_bytes": scope["bytes"], "pgdata": str(pgdata), "pgdata_inode": pgdata.stat().st_ino,
    })
    proof, proof_sha = _receipt(writable, served, "clone.json", boundary | {
        "schema": "qt.benchmark_clone_boundary.v1", "database": "original_layout_fixture", "role": "qt_test",
        "role_oid": native["role_oid"], "tablespace_oid": native["tablespace_oid"],
        "archive_inode": archive.stat().st_ino, "archive_objects_inode": (archive / "objects").stat().st_ino,
        "pgdata_inode": native["different_pgdata_inode"], "tablespace_inode": native["different_history_inode"],
        "archive": scope,
        "copy_receipt_sha256": copy_sha, "suffix_receipt_sha256": suffix_sha, "copy_binding_sha256": binding,
    })
    return dict(clone_proof=proof, clone_proof_sha256=proof_sha, copy_receipt=copy, copy_receipt_sha256=copy_sha,
                suffix_receipt=suffix, suffix_receipt_sha256=suffix_sha, archive_root=archive, tablespace_name="donor_history",
                port=55483, timeout_seconds=30)


def test_admitted_original_name_supports_read_only_donor_copy(original_donor):
    with open_paused_donor(**original_donor) as donor:
        conn = donor.connection
        assert donor.identity["database"] == "original_layout_fixture"
        with pytest.raises(ValueError, match="requires_isolated_clone"):
            _require_snapshot(conn, donor.identity)
        _require_snapshot(conn, donor.identity, paused_donor=donor)
        assert donor.accepted_commit_seq == conn.exec_driver_sql("SELECT market_commit_seq FROM market.fact_versions").scalar_one()
        with conn.begin_nested(), conn.connection.driver_connection.cursor() as cursor:
            data = BytesIO()
            cursor.copy_expert("COPY (SELECT market_commit_seq FROM market.fact_versions) TO STDOUT", data)
            assert data.getvalue() == b"7\n"
        with pytest.raises(DBAPIError) as rejected, conn.begin_nested():
            conn.exec_driver_sql("DELETE FROM market.fact_versions")
        assert rejected.value.orig.pgcode == "25006"
    assert conn.closed


def test_original_name_cannot_borrow_wrong_receipt_or_archive(original_donor):
    with pytest.raises(ValueError, match="hash_mismatch"):
        with open_paused_donor(**(original_donor | {"clone_proof_sha256": "0" * 64})):
            pytest.fail("accepted an unpinned donor proof")
    with pytest.raises(ValueError, match="archive_overlaps_workspace"):
        with open_paused_donor(**(original_donor | {"archive_root": Path("/donor/archive")})):
            pytest.fail("accepted an aliased donor archive")


def test_native_pause_refuses_another_reviewed_target(original_donor):
    def retarget(path):
        body = json.loads(path.read_bytes())
        body.pop("verification_sha256")
        return body | {"target_lsn": "0/1000"}

    writable, served = Path("/fixture-receipts-write"), Path("/fixture-receipts")
    suffix, suffix_sha = _receipt(writable, served, "wrong-suffix.json", retarget(original_donor["suffix_receipt"]))
    proof, proof_sha = _receipt(writable, served, "wrong-clone.json", retarget(original_donor["clone_proof"]) | {
        "suffix_receipt_sha256": suffix_sha,
    })
    changed = {"suffix_receipt": suffix, "suffix_receipt_sha256": suffix_sha,
               "clone_proof": proof, "clone_proof_sha256": proof_sha}
    with pytest.raises(RuntimeError, match="paused_recovery_native_state_changed: recovery_lsn"):
        with open_paused_donor(**(original_donor | changed)):
            pytest.fail("admitted evidence from a different recovery target")


def test_paused_donor_readonly_alias_keeps_native_server_paths(original_donor):
    options = original_donor | {"readonly_mount_alias": Path("/donor-alias")}
    with open_paused_donor(**options) as donor:
        conn = donor.connection
        assert conn.engine.url.query["host"] == "/donor-alias/control/socket"
        assert conn.exec_driver_sql("SHOW data_directory").scalar_one() == "/donor/pgdata"
        _require_snapshot(conn, donor.identity, paused_donor=donor)
    with pytest.raises(ValueError, match="invalid_mount_alias"):
        with open_paused_donor(**(options | {"readonly_mount_alias": Path("/donor/control")})):
            pytest.fail("admitted a bind inside the native donor path")
