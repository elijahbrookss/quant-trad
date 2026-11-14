from dataclasses import replace
from datetime import timedelta

import pytest
from sqlalchemy import text

from market_data.archive import FilesystemRawArchiveObjectStore
from market_data.canonical_storage import record_from_storage_row, record_to_storage_row
from market_data.cold_history import ColdHistoryReader, publish_cold_page
from portal.backend.service.storage.repos import collector_operations
from portal.backend.service.storage.repos.cold_history import PostgresColdHistory
from tests.test_market_data.test_canonical_fact_archive import _row
from tests.test_market_data.test_postgres_cold_history import database


pytestmark = pytest.mark.db


def test_collectors_fetch_cold_latest_and_keep_active_revision_telemetry(database, tmp_path, monkeypatch):
    database.execute(text("""
        UPDATE market.series SET instrument_id='fixture-' || id;
        CREATE TABLE market.fact_identities (
            id text PRIMARY KEY, series_id bigint NOT NULL, observation_key text NOT NULL
        )
    """))
    store = FilesystemRawArchiveObjectStore(tmp_path / "objects")
    cold = PostgresColdHistory(ColdHistoryReader(store))
    monkeypatch.setattr(collector_operations.canonical_fact_storage_repository, "cold_history",
                        lambda _session: cold)
    active = _row(1)
    invalidated = _row(2, state="invalidated")
    other = _row(1, series_id=2, commit=3)
    for row in (active, invalidated, other):
        cold.admit(database, publish_cold_page(
            [row], object_store=store, temporary_directory=tmp_path,
        ))
    recent = collector_operations._merge_cold_recent(database, [], [1, 2], 2)
    assert [row["id"] for row in recent] == [other["id"]]
    assert recent[0]["instrument_id"] == "fixture-2"
    assert recent[0]["provider"] == "COINBASE"
    assert recent[0]["payload"] == other["payload"]

    telemetry = {row["series_id"]: row for row in
                 collector_operations._merge_cold_telemetry(database, [], [1, 2])}
    assert telemetry[1]["last_accepted_at"] == active["accepted_at"]
    assert telemetry[1]["last_observation_time"] == active["observation_time"]
    assert telemetry[2]["last_accepted_at"] == other["accepted_at"]

    database.execute(text("""
        INSERT INTO market.fact_identities VALUES ('newer-hot', :series_id, :key)
    """), {"series_id": other["series_id"], "key": other["observation_key"]})
    assert not collector_operations._merge_cold_recent(database, [], [1, 2], 2)

    monkeypatch.setattr(collector_operations, "_COLD_COLLECTOR_MAX_PAGES", 1)
    with pytest.raises(RuntimeError, match="collector_cold_fact_budget_exceeded"):
        collector_operations._merge_cold_recent(database, [], [1], 2)


def test_overlap_counts_one_recent_acceptance(database, tmp_path, monkeypatch):
    database.execute(text("""
        CREATE TABLE market.fact_identities (
            id text PRIMARY KEY, series_id bigint NOT NULL, observation_key text NOT NULL
        )
    """))
    original = record_from_storage_row(_row())
    accepted = database.execute(text("SELECT transaction_timestamp()")).scalar_one() - timedelta(seconds=10)
    new_fact = replace(original.fact, accepted_at=accepted, known_at=accepted)
    new_row = record_to_storage_row(
        replace(original, fact=new_fact, row_hash=None, fact_version_id=None),
        series_dimensions={"currency": "USD"},
    )
    store = FilesystemRawArchiveObjectStore(tmp_path / "objects")
    cold = PostgresColdHistory(ColdHistoryReader(store))
    monkeypatch.setattr(collector_operations.canonical_fact_storage_repository, "cold_history",
                        lambda _session: cold)
    cold.admit(database, publish_cold_page([new_row], object_store=store, temporary_directory=tmp_path))
    hot_row = {
        "series_id": 1, "last_observation_time": original.fact.observation_time,
        "last_accepted_at": accepted, "accepted_last_minute": 1, "accepted_last_five_minutes": 1,
    }
    database.execute(text("""
        INSERT INTO market.fact_identities VALUES (:id, :series_id, :observation_key)
    """), {name: new_row[name] for name in ("id", "series_id", "observation_key")})
    assert list(collector_operations._merge_cold_telemetry(database, [hot_row], [1])) == [hot_row]
    database.execute(text("DELETE FROM market.fact_identities"))
    assert list(collector_operations._merge_cold_telemetry(database, [], [1])) == [hot_row]
