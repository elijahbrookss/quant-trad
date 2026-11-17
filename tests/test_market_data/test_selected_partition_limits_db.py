from dataclasses import replace
from datetime import timedelta

import pytest
from sqlalchemy import text

from core.market_storage_lifecycle import CanonicalFactRetentionPolicy
from portal.backend.service.market.canonical_retention import CanonicalFactRetentionExecutor
from portal.backend.service.storage.repos.fact_reclamation import PostgresCanonicalFactReclamationRepository
from portal.backend.service.storage.repos.fact_retention import PostgresCanonicalFactRetentionRepository
from tests.test_market_data.test_fact_reclamation_db import _physical
from tests.test_market_data.test_fact_storage_tiers_db import BASE, _ingest, _placement, storage  # noqa: F401


pytestmark = pytest.mark.db


def test_selected_day_overrides_are_bounded_and_rechecked_before_retirement(storage, tmp_path, monkeypatch):
    day = storage.today - timedelta(days=31)
    _placement(monkeypatch, day)
    _ingest(storage)
    _ingest(storage, replace(storage.fact, payload={**storage.fact.payload, "rate": "0.345", "raw_rate": "0.345"},
                             accepted_at=BASE + timedelta(seconds=1), known_at=BASE + timedelta(seconds=1)))
    repository = PostgresCanonicalFactRetentionRepository(database=storage.database)
    policy = CanonicalFactRetentionPolicy(execution_enabled=True, max_steps_per_run=5, max_page_rows=1,
                                         max_verification_pages=1, archive_min_free_bytes=0)
    executor = CanonicalFactRetentionExecutor(repository=repository)
    setup = executor.run(policy=policy, storage_root=tmp_path, execute=True)
    assert [item["status"] for item in setup["outcomes"]] == [
        "partition_sealed", "page_acknowledged", "page_acknowledged", "page_verified", "page_verified",
    ]
    verify = {"policy": policy, "storage_root": tmp_path, "storage_day": day,
              "expected_action": "verify_partition", "execute": True}
    base = executor.run_selected(**verify)
    assert base["status"] == "failed"
    assert "verify_page_scope_invalid" in base["outcomes"][0]["error"]
    with pytest.raises(RuntimeError, match="selected_page_scope_changed"):
        executor.run_selected(**verify, verification_pages=1)
    for key, refusal in (("verification_bytes", "byte_budget_exceeded"),
                         ("verification_objects", "object_budget_exceeded")):
        denied = executor.run_selected(**verify, verification_pages=2, **{key: 1})
        assert denied["status"] == "failed" and refusal in denied["outcomes"][0]["error"]
    with storage.database.session() as session:
        assert session.scalar(text("SELECT state FROM market.fact_retention_partitions WHERE storage_day=:day"),
                              {"day": day}) == "sealed"
        object_bytes = session.scalar(text("SELECT sum(byte_count)::bigint FROM market.fact_archive_manifests WHERE storage_day=:day"),
                                    {"day": day})
    limits = {"verification_pages": 2, "verification_bytes": object_bytes, "verification_objects": 2}
    admitted = executor.run_selected(**verify, **limits)
    assert admitted["status"] == "completed"
    assert admitted["selected_verification_limits"] == {"max_" + key: value for key, value in limits.items()}
    proof, = admitted["outcomes"]
    assert proof["verified_objects"] == 2 and proof["verified_bytes"] == object_bytes

    reclaim = verify | {"expected_action": "reclaim_partition"}
    with pytest.raises(RuntimeError, match="selected_manifest_set_changed"):
        executor.run_selected(**reclaim, **limits, manifest_set_hash="f" * 64)
    unmodified = executor.run_selected(**reclaim)
    assert unmodified["status"] == "failed"
    assert "partition_page_budget_exceeded" in unmodified["outcomes"][0]["error"]
    low_bytes = executor.run_selected(**reclaim, **(limits | {"verification_bytes": object_bytes - 1}),
                                      manifest_set_hash=proof["manifest_set_hash"])
    assert low_bytes["status"] == "failed" and "byte_budget_exceeded" in low_bytes["outcomes"][0]["error"]
    assert _physical(storage, day)["hot_rows"] == 2

    scope_checks = []
    original = PostgresCanonicalFactReclamationRepository._check_selected_scope

    def changed_on_handoff(session, partition, *scope):
        scope_checks.append(scope)
        if len(scope_checks) == 2:
            partition = dict(partition) | {"manifest_set_hash": "a" * 64}
        original(session, partition, *scope)

    with monkeypatch.context() as patch:
        patch.setattr(PostgresCanonicalFactReclamationRepository, "_check_selected_scope",
                      staticmethod(changed_on_handoff))
        stale = executor.run_selected(**reclaim, **limits, manifest_set_hash=proof["manifest_set_hash"])
    assert stale["status"] == "failed"
    assert "reclaim_selected_manifest_set_changed" in stale["outcomes"][0]["error"]
    assert len(scope_checks) == 2 and _physical(storage, day)["hot_rows"] == 2

    retired = executor.run_selected(**reclaim, **limits, manifest_set_hash=proof["manifest_set_hash"])
    assert retired["status"] == "completed" and retired["outcomes"][0]["status"] == "partition_reclaimed"
    assert _physical(storage, day)["relation"] is None
    assert policy.max_verification_pages == 1
