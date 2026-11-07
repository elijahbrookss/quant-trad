import pytest
from sqlalchemy import text

from portal.backend.service.storage.repos.market_lifecycle import _LIFECYCLE_LOCK_NAME
from portal.backend.service.storage.repos.query_diagnostic import (
    QueryDiagnosticLimits,
    run_live_query_diagnostic,
)
from tests.test_market_data.test_fact_reclamation_db import _prepare
from tests.test_market_data.test_fact_storage_tiers_db import storage  # noqa: F401


pytestmark = pytest.mark.db


def _counts(storage, day):
    with storage.database.session() as session:
        return dict(session.execute(text("""
            SELECT
              (SELECT state FROM market.fact_retention_partitions WHERE storage_day=:day) AS state,
              (SELECT count(*) FROM market.fact_archive_manifests WHERE storage_day=:day) AS manifests,
              (SELECT count(*) FROM market.fact_archive_verifications AS proof
                 JOIN market.fact_archive_manifests AS pages ON pages.id=proof.manifest_id
                WHERE pages.storage_day=:day) AS receipts,
              (SELECT count(*) FROM market.storage_lifecycle_events) AS lifecycle_events
        """), {"day": day}).mappings().one())


def test_live_diagnostic_runs_closed_read_only_inventory_without_mutation(
        storage, tmp_path, monkeypatch):
    day, _archive, _reclaimer = _prepare(storage, tmp_path, monkeypatch)
    before = _counts(storage, day)
    with monkeypatch.context() as boundary:
        boundary.setattr(
            storage.database, "ensure_schema",
            lambda: (_ for _ in ()).throw(
                AssertionError("diagnostic must not enter schema bootstrap")
            ),
        )
        receipt = run_live_query_diagnostic(
            database=storage.database, storage_day=day,
            limits=QueryDiagnosticLimits(total_seconds=60),
            source_revision="a" * 40, source_tree_hash="b" * 64,
        )
    after = _counts(storage, day)
    assert after == before
    assert receipt["status"] == "passed"
    assert receipt["read_only"] is True and receipt["maintenance_authority"] is False
    by_id = {item["case_id"]: item for item in receipt["cases"]}
    assert by_id["reclamation.pinned_ranges"]["outcome"] == "passed"
    assert by_id["publication.alias_write"]["outcome"] == "untested"
    assert by_id["publication.canonical_dependency_write"]["outcome"] == "untested"
    assert by_id["reclamation.exclusive_handoff_and_drop"]["outcome"] == "unsupported"
    assert all(
        item["outcome"] == "passed"
        for item in receipt["cases"]
        if item["execution"] in {"plan", "analyze"}
    )


def test_live_diagnostic_refuses_active_lifecycle_owner(storage, tmp_path, monkeypatch):
    day, _archive, _reclaimer = _prepare(storage, tmp_path, monkeypatch)
    with storage.database._engine.connect() as owner:
        owner.execute(text("SELECT pg_advisory_lock(hashtextextended(:name,0))"),
                      {"name": _LIFECYCLE_LOCK_NAME})
        try:
            with pytest.raises(RuntimeError, match="lifecycle_owner_active"):
                run_live_query_diagnostic(
                    database=storage.database, storage_day=day,
                    source_revision="a" * 40, source_tree_hash="b" * 64,
                )
        finally:
            owner.execute(text("SELECT pg_advisory_unlock(hashtextextended(:name,0))"),
                          {"name": _LIFECYCLE_LOCK_NAME})


def test_live_diagnostic_refuses_nonverified_partition(storage):
    day = storage.today
    with pytest.raises(RuntimeError, match="partition_not_verified"):
        run_live_query_diagnostic(
            database=storage.database, storage_day=day,
            source_revision="a" * 40, source_tree_hash="b" * 64,
        )
