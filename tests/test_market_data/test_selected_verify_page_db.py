from dataclasses import replace
from datetime import timedelta

import pytest
from sqlalchemy import text

from core.market_storage_lifecycle import CanonicalFactRetentionPolicy
from portal.backend.service.market.canonical_retention import CanonicalFactRetentionExecutor
from portal.backend.service.storage.repos.fact_archival import SelectedVerificationPage
from portal.backend.service.storage.repos.fact_retention import PostgresCanonicalFactRetentionRepository
from tests.test_market_data.test_fact_reclamation_db import _physical
from tests.test_market_data.test_fact_storage_tiers_db import BASE, _ingest, _placement, storage  # noqa: F401


pytestmark = pytest.mark.db


def test_selected_verification_pins_current_generation_and_resume_page_without_retirement(storage, tmp_path, monkeypatch):
    day = storage.today - timedelta(days=31)
    _placement(monkeypatch, day)
    _ingest(storage)
    _ingest(storage, replace(storage.fact, payload={**storage.fact.payload, "rate": "0.345", "raw_rate": "0.345"},
                             accepted_at=BASE + timedelta(seconds=1), known_at=BASE + timedelta(seconds=1)))
    repository = PostgresCanonicalFactRetentionRepository(database=storage.database)
    policy = CanonicalFactRetentionPolicy(execution_enabled=True, max_steps_per_run=3,
                                         max_page_rows=1, archive_min_free_bytes=0)
    executor = CanonicalFactRetentionExecutor(repository=repository)
    setup = executor.run(policy=policy, storage_root=tmp_path, execute=True)
    assert [item["status"] for item in setup["outcomes"]] == [
        "partition_sealed", "page_acknowledged", "page_acknowledged",
    ]
    with storage.database.session() as session:
        pages = [SelectedVerificationPage(*row) for row in session.execute(text("""
            SELECT m.page_ordinal,m.id,m.manifest_hash,g.generation,g.preparation_id,g.catalog_commitment
            FROM market.fact_archive_manifests m JOIN market.fact_archive_active_generations g ON g.manifest_id=m.id
            WHERE m.storage_day=:day ORDER BY m.page_ordinal
        """), {"day": day})]
    assert len(pages) == 2

    options = {"policy": policy, "storage_root": tmp_path, "storage_day": day,
               "expected_action": "verify_page"}
    wrong_generation = replace(pages[0], generation=pages[0].generation + 1)
    with pytest.raises(RuntimeError, match="selected_page_changed"):
        executor.run_selected(**options, verification_page=wrong_generation)
    dry = executor.run_selected(**options, verification_page=pages[0])
    assert dry["status"] == "dry_run" and dry["expected_verification_page"]["manifest_id"] == pages[0].manifest_id
    for stale in (wrong_generation, replace(pages[0], catalog_commitment="f" * 64), pages[1]):
        result = executor.run_selected(**options, verification_page=stale, execute=True)
        assert result["status"] == "failed" and "selected_page_changed" in result["outcomes"][0]["error"]
    with storage.database.session() as session:
        assert session.scalar(text("SELECT count(*) FROM market.fact_archive_verifications")) == 0

    result = executor.run_selected(**options, verification_page=pages[0], execute=True)
    assert result["status"] == "completed"
    assert [(item["status"], item["manifest_id"]) for item in result["outcomes"]] == [
        ("page_verified", pages[0].manifest_id),
    ]
    stale = executor.run_selected(**options, verification_page=pages[0], execute=True)
    assert stale["status"] == "failed" and "selected_page_changed" in stale["outcomes"][0]["error"]
    assert executor.run_selected(**options, verification_page=pages[1])["status"] == "dry_run"
    assert _physical(storage, day)["hot_rows"] == 2
    assert _physical(storage, day)["header_rows"] == 2
    with storage.database.session() as session:
        proof = session.execute(text("""
            SELECT v.manifest_id,g.preparation_id,g.catalog_commitment FROM market.fact_archive_verifications v
            JOIN market.fact_archive_verification_generations g USING(manifest_id,verifier_version)
        """)).all()
        assert proof == [(pages[0].manifest_id, pages[0].preparation_id, pages[0].catalog_commitment)]
        assert session.execute(text("""
            SELECT state,reclaimed_at FROM market.fact_retention_partitions WHERE storage_day=:day
        """), {"day": day}).one() == ("sealed", None)
