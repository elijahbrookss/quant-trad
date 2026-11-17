"""Exercise the one-day wrapper on disposable PostgreSQL."""

from datetime import timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest
from sqlalchemy import text
from sqlalchemy.engine import make_url

from core.storage_targets import StoragePolicy
from portal.backend.db.fact_identity_schema import fact_header_partition_name
from portal.backend.db.fact_storage_schema import fact_partition_name
from portal.backend.db.storage_target_models import StoragePolicyRecord
from scripts.db import canonical_cold_private_seal as private
from tests.test_market_data.test_fact_seal_timeout_db import state
from tests.test_market_data.test_fact_storage_tiers_db import _ingest, _placement, storage


pytestmark = pytest.mark.db


def test_private_seal_requires_reviewed_children_and_open_marker(storage, monkeypatch, tmp_path):
    day = storage.today - timedelta(days=15)
    _placement(monkeypatch, day)
    _ingest(storage)
    with storage.database.session() as session:
        header, payload = session.execute(text("""
            SELECT to_regclass(:header)::oid::bigint, to_regclass(:payload)::oid::bigint
        """), {"header": f"market.{fact_header_partition_name(day)}",
                "payload": f"market.{fact_partition_name(day)}"}).one()

    # Existing shared database fixture is over loopback, with no promoted
    # donor or read-only mount. Keep these external checks out of this test.
    checks = []
    monkeypatch.setattr(private, "assert_promoted_clone", lambda *_: checks.append("clone"))
    monkeypatch.setattr(private, "_matches_inode", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(private, "os", SimpleNamespace(
        ST_RDONLY=1, statvfs=lambda path: SimpleNamespace(f_flag=1),
    ))
    clone = private.PrivateClone(make_url(storage.dsn), {"database_identity": "fixture/1", "accepted_commit_seq": 1}, None)
    options = {"proof_sha256": "a" * 64, "day": day, "expected_state": "open", "payload_oid": payload,
               "archive_object_root": tmp_path, "max_seconds": 15, "count_statement_timeout_ms": 5000}

    with pytest.raises(RuntimeError, match="private_seal_partition_identity_changed"):
        private.seal_one(clone, header_oid=payload, **options)
    with pytest.raises(RuntimeError, match="canonical_retention_hot_window_not_elapsed"):
        private.seal_one(clone, header_oid=header, **options)
    assert state(storage, day) == {"state": "open", "expected_rows": None, "sealed_at": None}

    policy = StoragePolicy(recent=("fixture-recent",), history=("fixture-history",),
                           archives=("fixture-history",), backups=("fixture-backup",), recent_days=14)
    with storage.database.session() as session:
        session.merge(StoragePolicyRecord(id=1, revision=7, policy=policy.to_dict()))
    result = private.seal_one(clone, header_oid=header, **options)
    assert (result["state"], result["expected_rows"], result["header_oid"], result["payload_oid"]) == (
        "sealed", 1, header, payload,
    )
    assert result["policy"] == {"policy_revision": 7, "policy_hash": policy.fingerprint}
    assert state(storage, day)["sealed_at"].isoformat() == result["sealed_at"]
    with pytest.raises(RuntimeError, match="private_seal_partition_state_changed"):
        private.seal_one(clone, header_oid=header, **options)
    assert checks == ["clone", "clone", "clone", "clone"]


def test_native_connection_must_match_promoted_clone(storage):
    original = {"database_identity": "1234/5", "role": "qt_fixture", "role_oid": 10,
                "postmaster_epoch": "1.000000", "replay_lsn": "0/1", "tablespace_oid": 1663}
    clone = private.PrivateClone(make_url(storage.dsn), original,
                                 Path("/example/control/socket/.s.PGSQL.5432"))
    with storage.database.session() as session:
        with pytest.raises(RuntimeError, match="private_seal_promoted_clone_changed"):
            private.assert_promoted_clone(session, clone)
