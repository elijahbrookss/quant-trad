"""Timed disposable fixture for the recurring cold identity prune batch."""
from dataclasses import replace
from datetime import timedelta
from time import perf_counter_ns
import json
import os

import pytest
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError

from market_data.archive import FilesystemRawArchiveObjectStore
from market_data.cold_history import ColdHistoryReader
from portal.backend.db.cold_history_models import COLD_HISTORY_LAYOUT_VERSION
from portal.backend.db.cold_retirement_schema import prepare_cold_dependency_bindings
from portal.backend.service.storage.repos.cold_history import PostgresColdHistory
from portal.backend.service.storage.repos.cold_retirement import prune_oldest_retired_identities
from portal.backend.service.storage.repos.fact_archival import PostgresCanonicalFactArchiveRepository
from portal.backend.service.storage.repos.fact_reclamation import PostgresCanonicalFactReclamationRepository
from tests.test_market_data.test_canonical_cold_activate_db import _configure_fixture_catalog
from tests.test_market_data.test_fact_derived_retention_db import _archive_day
from tests.test_market_data.test_fact_storage_tiers_db import _placement, storage  # noqa: F401


pytestmark = [
    pytest.mark.db,
    pytest.mark.skipif(os.getenv("QT_COLD_PRUNE_SCALE_FIXTURE") != "1", reason="requires the opt-in 10k prune fixture"),
]


def test_native_prune_timeout_and_held_writer(storage, tmp_path, monkeypatch):
    rows = 10_001
    day = storage.today - timedelta(days=2)
    db = storage.database
    store = FilesystemRawArchiveObjectStore(tmp_path / "objects")
    archive = PostgresCanonicalFactArchiveRepository(
        database=db, object_store=store, temporary_directory=tmp_path / "staging")
    reclaimer = PostgresCanonicalFactReclamationRepository(archive_repository=archive, enabled=True)
    _placement(monkeypatch, day)
    outcome = storage.repo.ingest_facts(series_id=storage.series_id, source_id=storage.source_id,
        facts=(replace(storage.fact, observation_key=f"prune-{index:06d}") for index in range(rows)))
    assert outcome.inserted_count == rows
    _archive_day(archive, day)
    with db.session() as session:
        identity = session.scalar(text("""
            SELECT control.system_identifier::text || '/' || db.oid::text
            FROM pg_control_system() AS control CROSS JOIN pg_database AS db
            WHERE db.datname=current_database()
        """))
        _configure_fixture_catalog(session.connection(), identity, store.root)
        prepare_cold_dependency_bindings(session.connection())
        session.execute(text("CREATE INDEX idx_identity_prune_fixture_day ON market.fact_identities(storage_day,id)"))
        session.execute(text("UPDATE market.fact_storage_state SET layout_version=:layout"),
                        {"layout": COLD_HISTORY_LAYOUT_VERSION})
    assert reclaimer.reclaim_partition(day, eligible_before=storage.today, execute=True)["status"] == "cold_page_admitted"
    assert reclaimer.reclaim_partition(day, eligible_before=storage.today, execute=True)["status"] == "partition_reclaimed"

    def state():
        with db.session() as session:
            return tuple(session.execute(text("""
                SELECT (SELECT count(*) FROM market.fact_identities WHERE storage_day=:day),
                       (SELECT count(*) FROM market.cold_fact_locators l JOIN market.cold_fact_pages p ON p.id=l.page_id
                           WHERE p.staged_storage_day=:day),
                       EXISTS (SELECT FROM market.cold_fact_pruned_days WHERE storage_day=:day)
            """), {"day": day}).one())

    assert state() == (rows, rows, False)
    with db._engine.connect() as writer, writer.begin():
        # This is the relation lock held by a competing identity INSERT.
        writer.exec_driver_sql("LOCK TABLE market.fact_identities IN ROW EXCLUSIVE MODE")
        started = perf_counter_ns()
        with pytest.raises(DBAPIError) as detail:
            prune_oldest_retired_identities(db, deadline=lambda: None)
        refused_ns = perf_counter_ns() - started
        assert detail.value.orig.pgcode == "55P03"
        assert state() == (rows, rows, False)

    def measure():
        started = perf_counter_ns()
        result = prune_oldest_retired_identities(db, deadline=lambda: None)
        return perf_counter_ns() - started, result

    first_ns, first = measure()
    assert first["rows_removed"] == 10_000 and not first["day_complete"]
    assert state() == (1, rows, False)
    tail_ns, tail = measure()
    assert tail["rows_removed"] == 1 and tail["day_complete"]
    assert state() == (0, rows, True)
    assert prune_oldest_retired_identities(db, deadline=lambda: None) is None
    repo = PostgresColdHistory(ColdHistoryReader(store))
    with db.session() as session:
        selected = session.scalars(text("SELECT id FROM market.cold_fact_locators ORDER BY id LIMIT 2")).all()
        assert set(repo.read_by_ids(session, selected)) == set(selected)
        postgres, timescale = session.execute(text("""
            SELECT version(), (SELECT extversion FROM pg_extension WHERE extname='timescaledb')
        """)).one()
    print(json.dumps({"postgres": postgres, "timescaledb": timescale, "day": day.isoformat(),
                      "held_writer": {"wall_ns": refused_ns, "sqlstate": "55P03", "rows_changed": 0},
                      "no_contender": {"wall_ns": first_ns, "result": first},
                      "tail": {"wall_ns": tail_ns, "result": tail}, "cold_rows_retained": rows}))
