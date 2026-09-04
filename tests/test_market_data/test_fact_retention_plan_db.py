from dataclasses import replace
from datetime import timedelta

import pytest
from sqlalchemy import text

from core.market_storage_lifecycle import CanonicalFactRetentionPolicy
from market_data.archive import FilesystemRawArchiveObjectStore
from portal.backend.service.storage.repos.fact_archival import PostgresCanonicalFactArchiveRepository
from portal.backend.service.storage.repos.fact_retention import PostgresCanonicalFactRetentionRepository
from tests.test_market_data.test_fact_storage_tiers_db import storage, _placement, _ingest

pytestmark = pytest.mark.db


def _state(storage):
    with storage.database.session() as session:
        return list(session.execute(text("SELECT * FROM market.fact_retention_partitions ORDER BY storage_day")).mappings())


def test_real_retention_inventory_is_read_only_bounded_and_resumes_after_cursor(storage, tmp_path, monkeypatch):
    days = [storage.today - timedelta(days=age) for age in (33, 32, 0)]
    for ordinal, day in enumerate(days):
        _placement(monkeypatch, day)
        # All observations remain old. A newly accepted revision must still
        # receive its full hot lifetime from physical placement, not event age.
        _ingest(storage, replace(storage.fact, observation_key=f"retention-plan-{ordinal}"))
    repository = PostgresCanonicalFactRetentionRepository(database=storage.database)
    policy = CanonicalFactRetentionPolicy(max_candidate_partitions=1, archive_min_free_bytes=0)
    before = _state(storage)
    files = sorted(str(path) for path in tmp_path.rglob("*"))
    first = repository.plan(policy=policy, storage_root=tmp_path)
    assert first["database_day"] == storage.today.isoformat()
    assert first["inventory"]["hot_partition_count"] == len(before) == 4
    assert first["inventory"]["database_bytes"] > first["inventory"]["hot_payload_bytes"] > 0
    assert first["inventory"]["canonical_header_bytes"] > 0
    assert first["inventory"]["raw_mapping_bytes"] > 0
    assert first["actions"][0]["storage_day"] == days[0].isoformat()
    assert first["actions"][0]["eligible"] is True
    assert first["candidate_scan_complete"] is False
    second = repository.plan(policy=policy, storage_root=tmp_path, after_storage_day=days[0])
    assert second["actions"][0]["storage_day"] == days[1].isoformat()
    current = repository.plan(policy=policy, storage_root=tmp_path, after_storage_day=days[1])
    assert current["actions"][0]["blockers"] == ["active_or_future_storage_day"]
    # Clean bootstrap also pre-creates tomorrow's empty partition.
    last = repository.plan(policy=policy, storage_root=tmp_path, after_storage_day=storage.today)
    assert last["actions"][0]["blockers"] == ["active_or_future_storage_day"]
    assert last["candidate_scan_complete"] is True and last["next_after_storage_day"] is None
    assert first["inventory"]["hot_payload_bytes"] == last["inventory"]["hot_payload_bytes"]
    with pytest.raises(RuntimeError, match="inventory_budget_exceeded"):
        repository.plan(policy=replace(policy, max_inventory_partitions=2), storage_root=tmp_path)
    assert _state(storage) == before
    assert sorted(str(path) for path in tmp_path.rglob("*")) == files

    # Persisted page/receipt progress, not filesystem presence, selects the
    # next operation. Actual staging is deliberately outside plan().
    archive = PostgresCanonicalFactArchiveRepository(
        database=storage.database, object_store=FilesystemRawArchiveObjectStore(tmp_path / "objects"),
        temporary_directory=tmp_path / "staging")
    archive.seal_partition(days[0])
    assert repository.plan(policy=policy, storage_root=tmp_path)["actions"][0]["action"] == "stage_page"
    archive.stage_next_page(days[0])
    assert repository.plan(policy=policy, storage_root=tmp_path)["actions"][0]["action"] == "verify_page"
    archive.verify_next_page(days[0])
    assert repository.plan(policy=policy, storage_root=tmp_path)["actions"][0]["action"] == "verify_partition"
    archive.verify_partition(days[0])
    final = repository.plan(policy=policy, storage_root=tmp_path)
    assert final["actions"][0]["action"] == "reclaim_partition"
    assert final["metadata_eligible_reclaim_bytes"] > 0
    assert final["execution_available"] is False
    assert _state(storage)[0]["state"] == "verified"  # Not reclaimed by planning.
