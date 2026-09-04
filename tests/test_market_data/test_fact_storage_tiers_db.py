from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError

from market_data.canonical import CanonicalFact
from market_data.contracts import SourceIdentity
from portal.backend.db import InstrumentRecord
from portal.backend.db.fact_storage_schema import (
    FACT_STORAGE_LAYOUT_VERSION, FACT_ROWS_VIEW_SELECT, assert_fact_storage_contract,
    ensure_fact_payload_partition, fact_partition_name,
)
from portal.backend.db.session import Database
from portal.backend.service.storage.repos import market_data as repository_module
from tests.test_market_data.migration_test_support import fresh_migration_database

pytestmark = pytest.mark.db
BASE = datetime(2026, 8, 9, 12, 0, 0, 123456, tzinfo=UTC)


@pytest.fixture
def storage(monkeypatch):
    with fresh_migration_database("fact_storage") as dsn:
        database = Database(dsn)
        try:
            assert database.ensure_schema(), str(database.last_error)
            monkeypatch.setattr(repository_module, "db", database)
            repo = repository_module.PostgresMarketDataRepository()
            with database.session() as session:
                today = session.execute(text("SELECT (clock_timestamp() AT TIME ZONE 'UTC')::date")).scalar_one()
                session.add(InstrumentRecord(
                    id="storage-fixture", datasource="TEST", exchange="ISOLATED", symbol="BTC-TEST",
                    instrument_type="spot", can_short=False, short_requires_borrow=False,
                    has_funding=False, extra_metadata={},
                ))
            source = SourceIdentity(provider="TEST", venue="ISOLATED", source_kind="fixture",
                                    adapter_version="storage.fixture.v1")
            source_id = repo.register_source(source, lineage={"fixture": "storage"})
            series_id = repo.register_series(
                instrument_id="storage-fixture", fact_type="derivatives.funding_rate",
                timeframe_seconds=None, contract_version="derivatives.funding_rate.v2",
            )
            fact = CanonicalFact(
                fact_type="derivatives.funding_rate", payload_schema_id="derivatives.funding_rate.v2",
                observation_key="funding-fixture", observation_time=BASE,
                observation_time_method="collector_schedule", accepted_at=BASE, known_at=BASE,
                known_at_method="platform_acceptance", received_at=BASE, source_published_at=BASE,
                source=source, transformation_id="fixture.v1",
                payload={"rate": "0.1234567890123456789", "raw_rate": "+0.1234567890123456789",
                         "funding_time": BASE - timedelta(hours=1), "interval_seconds": 3600, "unit": "fraction"},
                provenance={"source_material": "a" * 64}, quality={"valid": True},
            )
            yield SimpleNamespace(database=database, repo=repo, source_id=source_id,
                                  series_id=series_id, fact=fact, today=today, dsn=dsn)
        finally:
            database._reset_engine()


def _ingest(storage, fact=None):
    return storage.repo.ingest_facts(
        series_id=storage.series_id, source_id=storage.source_id, facts=[fact or storage.fact],
    )


def _read(storage, **kwargs):
    return storage.repo.read_facts(series_id=storage.series_id, start=BASE - timedelta(days=1),
                                   end=BASE + timedelta(days=1), **kwargs)


def _placement(monkeypatch, day):
    def choose(session):
        ensure_fact_payload_partition(session.connection(), day)
        return day
    monkeypatch.setattr(repository_module, "current_fact_storage_day", choose)


def test_clean_layout_is_admitted_after_restart(storage):
    restarted = Database(storage.dsn)
    try:
        assert restarted.ensure_schema(), str(restarted.last_error)
        with restarted.session() as session:
            assert_fact_storage_contract(session.connection())
            count = session.execute(text(
                "SELECT count(*) FROM pg_inherits WHERE inhparent = 'market.fact_hot_payloads'::regclass"
            )).scalar_one()
            assert count == 2
            state = session.execute(text(
                "SELECT state FROM market.fact_storage_state WHERE layout_version=:version"
            ), {"version": FACT_STORAGE_LAYOUT_VERSION}).scalar_one()
            assert state == "ready"
    finally:
        restarted._reset_engine()

    for unsafe_view in (
        FACT_ROWS_VIEW_SELECT.replace("LEFT JOIN", "JOIN"),
        FACT_ROWS_VIEW_SELECT.replace("market.cold_fact_read_required(versions.id, 'payload')", "NULL::jsonb"),
    ):
        with storage.database.session() as session:
            session.execute(text("CREATE OR REPLACE VIEW market.fact_rows AS " + unsafe_view))
        rejected = Database(storage.dsn)
        try:
            assert rejected.ensure_schema() is False
            assert "hot-row projection differs" in str(rejected.last_error)
        finally:
            rejected._reset_engine()
        with storage.database.session() as session:
            session.execute(text("CREATE OR REPLACE VIEW market.fact_rows AS " + FACT_ROWS_VIEW_SELECT))


def test_hot_history_preserves_identity_clocks_and_corrections_across_days(storage, monkeypatch):
    _placement(monkeypatch, storage.today - timedelta(days=2))
    first = _ingest(storage)
    original = _read(storage)[0]
    _placement(monkeypatch, storage.today)
    assert _ingest(storage).noop_count == 1
    corrected = replace(storage.fact, payload={**storage.fact.payload, "rate": "0.2", "raw_rate": "0.2"},
                        accepted_at=BASE + timedelta(seconds=1), known_at=BASE + timedelta(seconds=1))
    assert _ingest(storage, corrected).corrected_count == 1
    assert _read(storage)[0].revision == 2
    assert _read(storage, as_of_commit_seq=first.max_commit_seq)[0] == original
    assert _read(storage, known_at_lte=BASE)[0] == original
    with storage.database.session() as session:
        rows = session.execute(text(
            "SELECT storage_day, revision FROM market.fact_versions ORDER BY revision"
        )).all()
        assert rows == [(storage.today - timedelta(days=2), 1), (storage.today, 2)]
        assert session.execute(text("SELECT count(*) FROM market.fact_hot_payloads")).scalar_one() == 2
    def reject_placement(_session):
        raise AssertionError("no-op ingestion must not provision a payload partition")
    monkeypatch.setattr(repository_module, "current_fact_storage_day", reject_placement)
    assert _ingest(storage, corrected).noop_count == 1

def _verified_cold_fixture(storage, tmp_path, monkeypatch):
    """Install known-verified bytes/catalog to test readers, not executor admission."""
    from market_data.archive import FilesystemRawArchiveObjectStore
    from market_data.fact_archive import publish_canonical_fact_archive, verify_canonical_fact_archive_rows
    from portal.backend.db import MarketFactArchiveManifestRecord, MarketFactArchiveMaterialAliasRecord
    from market_data.canonical_storage import legacy_material_alias
    from portal.backend.service.storage.repos.fact_storage import (
        CANONICAL_ROW_COLUMNS, CANONICAL_ROW_FROM, PostgresCanonicalFactStorageRepository,
    )
    with storage.database.session() as session:
        rows = session.execute(text(
            "SELECT " + CANONICAL_ROW_COLUMNS + CANONICAL_ROW_FROM +
            " WHERE versions.storage_day=:day ORDER BY versions.market_commit_seq,versions.id"
        ), {"day": storage.today}).mappings().all()
    archive_rows = [{key: value for key, value in row.items() if key != "storage_day"} for row in rows]
    store = FilesystemRawArchiveObjectStore(tmp_path / "objects")
    manifest = publish_canonical_fact_archive(archive_rows, object_store=store, temporary_directory=tmp_path / "staging")
    verify_canonical_fact_archive_rows(archive_rows, expected=manifest)
    with storage.database.session() as session:
        session.add(MarketFactArchiveManifestRecord(
            id=manifest.manifest_id, storage_day=storage.today, page_ordinal=0,
            object_key=manifest.object_key, object_sha256=manifest.object_sha256,
            manifest_hash=manifest.manifest_hash, row_count=manifest.row_count, byte_count=manifest.byte_count,
            first_commit_seq=manifest.first_cursor[0], first_id=manifest.first_cursor[1],
            last_commit_seq=manifest.last_cursor[0], last_id=manifest.last_cursor[1],
            descriptor=manifest.to_dict(),
        ))
        session.flush()
        for archived in archive_rows:
            alias = legacy_material_alias(archived)
            if alias is not None:
                session.add(MarketFactArchiveMaterialAliasRecord(manifest_id=manifest.manifest_id, **alias))
        session.flush()
        session.execute(text(
            "UPDATE market.fact_retention_partitions SET state='verified', sealed_at=now(), "
            "expected_rows=:count, verified_at=now(), manifest_set_hash=:hash WHERE storage_day=:day"
        ), {"count": len(rows), "hash": manifest.manifest_hash, "day": storage.today})
        session.execute(text("DROP TABLE market." + fact_partition_name(storage.today)))
    reader = FilesystemRawArchiveObjectStore(store.root, writable=False)
    monkeypatch.setattr(repository_module, "canonical_fact_storage_repository",
                        PostgresCanonicalFactStorageRepository(object_store_factory=lambda: reader))
    return reader.local_path(manifest.object_key)


def test_cold_and_mixed_reads_preserve_frozen_all_revision_history(storage, tmp_path, monkeypatch):
    from market_data.contracts import DatasetSeriesRequest
    _ingest(storage)
    corrected = replace(storage.fact, payload={**storage.fact.payload, "rate": "0.2", "raw_rate": "0.2"},
                        accepted_at=BASE + timedelta(seconds=1), known_at=BASE + timedelta(seconds=1))
    _ingest(storage, corrected)
    request = DatasetSeriesRequest(storage.series_id, BASE - timedelta(days=1), BASE + timedelta(days=1))
    frozen = storage.repo.freeze_dataset([request])
    assert frozen.series[0]["row_count"] == 2
    before = storage.repo.read_dataset_fact_revisions(dataset_id=frozen.dataset_id, series_id=storage.series_id)
    assert [record.revision for record in before] == [1, 2]
    first_visible = _read(storage, known_at_lte=BASE)
    archive_path = _verified_cold_fixture(storage, tmp_path, monkeypatch)
    assert _read(storage, known_at_lte=BASE) == first_visible
    assert _read(storage)[0] == before[-1]
    assert storage.repo.read_dataset_fact_revisions(dataset_id=frozen.dataset_id, series_id=storage.series_id) == before
    assert storage.repo.freeze_dataset([request]).dataset_hash == frozen.dataset_hash
    # A later hot correction cannot rewrite an already frozen cold history.
    _placement(monkeypatch, storage.today + timedelta(days=1))
    newest = replace(corrected, payload={**corrected.payload, "rate": "0.3", "raw_rate": "0.3"},
                     accepted_at=BASE + timedelta(seconds=2), known_at=BASE + timedelta(seconds=2))
    assert _ingest(storage, newest).corrected_count == 1
    assert _read(storage)[0].revision == 3
    assert storage.repo.read_dataset_fact_revisions(dataset_id=frozen.dataset_id, series_id=storage.series_id) == before
    assert storage.repo.freeze_dataset([request]).series[0]["row_count"] == 3
    original_bytes = archive_path.read_bytes()
    archive_path.write_bytes(b"corrupt")
    with pytest.raises(RuntimeError, match="canonical_archive_checksum_mismatch"):
        storage.repo.read_dataset_fact_revisions(dataset_id=frozen.dataset_id, series_id=storage.series_id)
    archive_path.write_bytes(original_bytes)
    archive_path.unlink()
    with pytest.raises(FileNotFoundError):
        storage.repo.read_dataset_fact_revisions(dataset_id=frozen.dataset_id, series_id=storage.series_id)


def test_snapshot_waits_before_taking_visibility_and_always_releases_its_fence(storage):
    from concurrent.futures import ThreadPoolExecutor
    from threading import Event
    import time
    lock_name = "quant-trad:test-snapshot-fence:v1"
    params = {"name": lock_name}
    def read_after_fence():
        with storage.database.locked_snapshot_session(shared_lock_name=lock_name) as reader:
            return reader.execute(text(
                "SELECT evidence FROM market.fact_storage_state WHERE layout_version=:version"
            ), {"version": FACT_STORAGE_LAYOUT_VERSION}).scalar_one()

    with ThreadPoolExecutor(max_workers=1) as executor:
        # Keep the physical connection checked out across commits. A Session
        # may return it to the pool while its session-level lock is still held;
        # the reader could then borrow that same connection and not wait.
        with storage.database._engine.connect() as lifecycle:
            lifecycle.execute(text("SELECT pg_advisory_lock(hashtextextended(:name,0))"), params)
            lifecycle.commit()
            try:
                future = executor.submit(read_after_fence)
                deadline = time.monotonic() + 30
                while True:
                    waiting = lifecycle.execute(text(
                        "SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() "
                        "AND pid<>pg_backend_pid() AND wait_event='advisory' "
                        "AND query LIKE '%pg_advisory_lock_shared%'"
                    )).scalar_one()
                    lifecycle.commit()  # Refresh PostgreSQL's activity snapshot; the session lock remains held.
                    if waiting:
                        break
                    if future.done():
                        raise AssertionError(f"snapshot reader bypassed its fence: {future.result()}")
                    if time.monotonic() >= deadline:
                        raise AssertionError("snapshot reader did not reach its fence")
                    Event().wait(0.02)
                lifecycle.execute(text(
                    "UPDATE market.fact_storage_state SET evidence=CAST(:evidence AS jsonb) "
                    "WHERE layout_version=:version"
                ), {"version": FACT_STORAGE_LAYOUT_VERSION, "evidence": '{"fence_committed":true}'})
                lifecycle.commit()
            finally:
                lifecycle.execute(text("SELECT pg_advisory_unlock(hashtextextended(:name,0))"), params)
                lifecycle.commit()
            assert future.result(timeout=30)["fence_committed"] is True

    with pytest.raises(RuntimeError, match="reader interrupted"):
        with storage.database.locked_snapshot_session(shared_lock_name=lock_name):
            raise RuntimeError("reader interrupted")
    with storage.database.session() as session:
        assert session.execute(text("SELECT pg_try_advisory_lock(hashtextextended(:name,0))"), params).scalar_one()
        assert session.execute(text("SELECT pg_advisory_unlock(hashtextextended(:name,0))"), params).scalar_one()


def test_candle_paging_summaries_and_causal_selection_survive_cooling(storage, tmp_path, monkeypatch):
    from market_data.contracts import CANDLE_FACT_TYPE, CANDLE_FACT_VERSION, CandleFact, DatasetSeriesRequest
    from portal.backend.service.storage.repos import candles as candle_repo

    monkeypatch.setattr(candle_repo, "db", storage.database)
    series_id = storage.repo.register_series(
        instrument_id="storage-fixture", fact_type=CANDLE_FACT_TYPE,
        timeframe_seconds=60, contract_version=CANDLE_FACT_VERSION,
    )
    start = BASE.replace(second=0, microsecond=0)
    end = start + timedelta(minutes=4)
    facts = [CandleFact(
        open_time=start + timedelta(minutes=minute), close_time=start + timedelta(minutes=minute + 1),
        open=100, high=110, low=90, close=101 + minute, volume=None,
        known_at=start + timedelta(minutes=minute + 1), known_at_method="provider_close_time",
        accepted_at=start + timedelta(minutes=minute + 1),
    ) for minute in (0, 2, 3)]
    storage.repo.ingest_candles(series_id=series_id, source_id=storage.source_id, facts=facts)
    corrected = replace(facts[0], close=109, known_at=end + timedelta(minutes=1), accepted_at=end + timedelta(minutes=1))
    storage.repo.ingest_candles(series_id=series_id, source_id=storage.source_id, facts=[corrected])
    frozen = storage.repo.freeze_dataset([DatasetSeriesRequest(series_id, start, end)])
    window = dict(instrument_id="storage-fixture", timeframe="1m", start=start, end=end)
    frozen_window = dict(dataset_id=frozen.dataset_id, series_id=series_id, start=start, end=end, limit=2)
    windows = [{"window_id": "a", "start": start, "end": end, "limit": 2},
               {"window_id": "overlap", "start": start, "end": end, "limit": 1}]

    def snapshot():
        return {
            "first": candle_repo.list_candles_for_series(**window, limit=2),
            "last": candle_repo.list_candles_for_series(**window, limit=2, prefer_latest=True),
            "summary": candle_repo.get_candle_storage_summary(**window),
            "windows": candle_repo.list_candles_for_series_windows(instrument_id="storage-fixture", timeframe="1m", windows=windows),
            "frozen": candle_repo.read_frozen_dataset_candles(**frozen_window),
            "frozen_last": candle_repo.read_frozen_dataset_candles(**frozen_window, prefer_latest=True),
            "causal": storage.repo.read_dataset_series(dataset_id=frozen.dataset_id, series_id=series_id, causal_at_interval_close=True),
        }

    before = snapshot()
    assert before["summary"]["candle_count"] == 3
    assert before["summary"]["gap_count"] == before["summary"]["missing_count"] == 1
    assert before["first"][0]["close"] == 109
    assert before["causal"][0].fact.close == 101  # Late revision must not hide the close-time fact.
    archive_path = _verified_cold_fixture(storage, tmp_path, monkeypatch)
    monkeypatch.setattr(candle_repo, "canonical_fact_storage_repository", repository_module.canonical_fact_storage_repository)
    assert snapshot() == before
    _placement(monkeypatch, storage.today + timedelta(days=1))
    storage.repo.ingest_candles(series_id=series_id, source_id=storage.source_id,
                               facts=[replace(corrected, close=108, accepted_at=end + timedelta(minutes=2), known_at=end + timedelta(minutes=2))])
    assert candle_repo.list_candles_for_series(**window, limit=2)[0]["revision"] == 3
    assert candle_repo.read_frozen_dataset_candles(**frozen_window) == before["frozen"]
    archive_path.unlink()
    with pytest.raises(FileNotFoundError):
        candle_repo.read_frozen_dataset_candles(**frozen_window)


def test_a_header_cannot_commit_without_its_payload(storage, monkeypatch):
    original_execute = None
    with pytest.raises(DBAPIError, match="fact_hot_payload_missing"):
        with storage.database.session() as session:
            original_execute = session.execute
            def omit_payload(statement, *args, **kwargs):
                if "INSERT INTO market.fact_hot_payloads" in str(statement):
                    return None
                return original_execute(statement, *args, **kwargs)
            monkeypatch.setattr(session, "execute", omit_payload)
            storage.repo.ingest_facts_in_session(
                session, series_id=storage.series_id, source_id=storage.source_id, facts=[storage.fact],
            )
    with storage.database.session() as session:
        assert session.execute(text("SELECT count(*) FROM market.fact_versions")).scalar_one() == 0


def test_partition_identity_and_payload_immutability_are_enforced(storage):
    _ingest(storage)
    tomorrow = storage.today + timedelta(days=1)
    with pytest.raises(DBAPIError, match="fact_hot_payload_identity_mismatch"):
        with storage.database.session() as session:
            session.execute(text(
                "INSERT INTO market.fact_hot_payloads "
                "SELECT :day, id, series_id, payload_schema_id, observation_time, payload, provenance, quality "
                "FROM market.fact_hot_payloads"
            ), {"day": tomorrow})
    for statement in (
        "UPDATE market.fact_versions SET state='invalidated'",
        "UPDATE market.fact_hot_payloads SET quality='{}'",
        "DELETE FROM market.fact_hot_payloads",
    ):
        with pytest.raises(DBAPIError, match="immutable"):
            with storage.database.session() as session:
                session.execute(text(statement))
    with pytest.raises(DBAPIError, match="ck_market_fact_archive_descriptor_binding"):
        with storage.database.session() as session:
            session.execute(text(
                "INSERT INTO market.fact_archive_manifests "
                "(id,storage_day,page_ordinal,object_key,object_sha256,manifest_hash,row_count,byte_count,"
                "first_commit_seq,first_id,last_commit_seq,last_id,descriptor) "
                "VALUES ('bad-descriptor',:day,0,'bad-object',:hash,:hash,1,1,1,'fixture',1,'fixture','{}')"
            ), {"day": storage.today, "hash": "1" * 64})


@pytest.mark.parametrize("table,trigger", [
    ("fact_versions", "trg_require_fact_hot_payload"),
    ("fact_hot_payloads", "trg_assert_fact_hot_payload_valid"),
])
def test_disabled_enforcement_is_rejected_without_runtime_repair(storage, table, trigger):
    with storage.database.session() as session:
        session.execute(text(f"ALTER TABLE market.{table} DISABLE TRIGGER {trigger}"))
    restarted = Database(storage.dsn)
    try:
        assert restarted.ensure_schema() is False
        assert "storage enforcement differs" in str(restarted.last_error)
    finally:
        restarted._reset_engine()
    with storage.database.session() as session:
        assert session.execute(text(
            "SELECT tgenabled FROM pg_trigger WHERE tgrelid=to_regclass(:relation) AND tgname=:trigger"
        ), {"relation": "market." + table, "trigger": trigger}).scalar_one() == "D"


def test_seal_waits_for_inflight_collection_and_rejects_late_writes(storage):
    with storage.database.session() as writer:
        storage.repo.ingest_facts_in_session(
            writer, series_id=storage.series_id, source_id=storage.source_id, facts=[storage.fact],
        )
        # Payload insertion holds a SHARE row lock until commit. A bounded
        # second transaction must not seal and count this day ahead of it.
        with pytest.raises(DBAPIError, match="lock timeout"):
            with storage.database.session() as sealer:
                sealer.execute(text("SET LOCAL lock_timeout='100ms'"))
                sealer.execute(text(
                    "UPDATE market.fact_retention_partitions SET state='sealed', sealed_at=now(), expected_rows=0 "
                    "WHERE storage_day=:day"
                ), {"day": storage.today})
    with storage.database.session() as sealer:
        sealer.execute(text(
            "UPDATE market.fact_retention_partitions SET state='sealed', sealed_at=now(), "
            "expected_rows=(SELECT count(*) FROM market.fact_versions WHERE storage_day=:day) "
            "WHERE storage_day=:day"
        ), {"day": storage.today})
    with pytest.raises(RuntimeError, match="fact_hot_partition_not_open"):
        _ingest(storage, replace(storage.fact, observation_key="late-fact"))
    assert len(_read(storage)) == 1


def test_reclaimed_projection_fails_loud_instead_of_silently_omitting_rows(storage):
    _ingest(storage)
    original = _read(storage)[0]
    # Disposable structural proof only; the real executor must prove archive
    # completeness and dependencies before it is allowed to issue this DDL.
    with storage.database.session() as session:
        relation = "market." + fact_partition_name(storage.today)
        before = session.execute(text("SELECT pg_total_relation_size(to_regclass(:name))"),
                                 {"name": relation}).scalar_one()
        assert before > 0
        session.execute(text(f"DROP TABLE {relation}"))
        assert session.execute(text("SELECT to_regclass(:name)"), {"name": relation}).scalar_one() is None
        assert session.execute(text("SELECT count(*) FROM market.fact_versions")).scalar_one() == 1
    with pytest.raises(DBAPIError, match="canonical_fact_cold_read_required"):
        with storage.database.session() as session:
            session.execute(text("SELECT payload FROM market.fact_rows")).all()
    with pytest.raises(RuntimeError, match="canonical_archive_coverage_invalid"):
        _read(storage)
    # Dedupe still uses permanent identity, not the removed JSON.
    assert _ingest(storage).noop_count == 1
    assert original.fact.row_hash == storage.fact.row_hash

def _rewind_disposable_storage_to_old_layout(storage):
    """Construct the prior full-row shape only inside this test's private DB."""
    from portal.backend.db import Base
    from portal.backend.db.fact_storage_schema import FACT_STORAGE_TABLES
    with storage.database.session() as session:
        conn = session.connection()
        conn.execute(text("CREATE TEMP TABLE fact_copy_fixture ON COMMIT DROP AS SELECT * FROM market.fact_rows"))
        conn.execute(text("DROP VIEW market.fact_rows"))
        conn.execute(text("DROP TRIGGER trg_require_fact_hot_payload ON market.fact_versions"))
        for table in reversed(Base.metadata.sorted_tables):
            if table.schema == "market" and table.name in FACT_STORAGE_TABLES:
                table.drop(conn)
        conn.execute(text("ALTER TABLE market.fact_versions DISABLE TRIGGER trg_reject_mutation_fact_versions"))
        conn.execute(text("ALTER TABLE market.fact_versions ADD COLUMN payload jsonb, ADD COLUMN provenance jsonb, ADD COLUMN quality jsonb"))
        conn.execute(text(
            "UPDATE market.fact_versions target SET payload=source.payload, provenance=source.provenance, quality=source.quality "
            "FROM fact_copy_fixture source WHERE source.id=target.id"
        ))
        conn.execute(text("ALTER TABLE market.fact_versions ENABLE TRIGGER trg_reject_mutation_fact_versions"))
        conn.execute(text("ALTER TABLE market.fact_versions DROP COLUMN storage_day"))
    return storage.database._engine


def test_prefix_metadata_cutover_is_explicit_atomic_and_preserves_ready_facts(storage):
    from sqlalchemy import event
    from portal.backend.db.fact_storage_schema import FACT_BOOK_PREFIX_TABLES
    from scripts.db.manual_migration_fact_storage_tiers_v1 import run_cutover
    _ingest(storage)
    original = _read(storage)
    engine = storage.database._engine
    # This fixture owns a disposable DB. Simulate the earlier ready layout by
    # removing exactly the two still-empty metadata tables, never Fact data.
    with engine.begin() as conn:
        for name in reversed(FACT_BOOK_PREFIX_TABLES):
            assert conn.execute(text(f"SELECT count(*) FROM market.{name}")).scalar_one() == 0
            conn.execute(text(f"DROP TABLE market.{name}"))
    inspected = run_cutover(engine)
    assert inspected["status"] == "book_prefix_metadata_required"
    assert inspected["missing_tables"] == list(FACT_BOOK_PREFIX_TABLES)
    with engine.connect() as conn:
        assert all(conn.execute(text("SELECT to_regclass(:name)"), {"name": "market." + name}).scalar_one() is None
                   for name in FACT_BOOK_PREFIX_TABLES)
    with pytest.raises(ValueError, match="writers_stopped"):
        run_cutover(engine, execute=True)
    interrupted = Database(storage.dsn)
    try:
        assert interrupted.ensure_schema() is False
        assert "fact_book_prefix" in str(interrupted.last_error)
    finally:
        interrupted._reset_engine()
    def interrupt_second_table(conn, cursor, statement, parameters, context, executemany):
        if "CREATE TABLE market.fact_book_prefix_dependencies" in statement:
            raise RuntimeError("injected metadata cutover interruption")
    event.listen(engine, "before_cursor_execute", interrupt_second_table)
    try:
        with pytest.raises(RuntimeError, match="injected metadata cutover"):
            run_cutover(engine, execute=True, writers_stopped=True)
    finally:
        event.remove(engine, "before_cursor_execute", interrupt_second_table)
    assert run_cutover(engine)["missing_tables"] == list(FACT_BOOK_PREFIX_TABLES)
    final = run_cutover(engine, execute=True, writers_stopped=True)
    assert final["status"] == "ready"
    assert run_cutover(engine, execute=True, writers_stopped=True) == final
    with engine.connect() as conn:
        for name in FACT_BOOK_PREFIX_TABLES:
            assert conn.execute(text(f"SELECT count(*) FROM market.{name}")).scalar_one() == 0
            assert conn.execute(text("SELECT tgtype FROM pg_trigger WHERE tgrelid=to_regclass(:name) AND tgname=:trigger"),
                {"name": "market." + name, "trigger": "trg_reject_mutation_" + name}).scalar_one() == 27
    assert _read(storage) == original


def test_offline_cutover_dry_run_and_resume_preserve_every_field(storage):
    from scripts.db.manual_migration_fact_storage_tiers_v1 import SOURCE, run_cutover
    _ingest(storage)
    corrected = replace(storage.fact, payload={**storage.fact.payload, "rate": "0.2", "raw_rate": "0.2"},
                        accepted_at=BASE + timedelta(seconds=1), known_at=BASE + timedelta(seconds=1))
    _ingest(storage, corrected)
    original = _read(storage, known_at_lte=BASE)
    engine = _rewind_disposable_storage_to_old_layout(storage)
    inspected = run_cutover(engine)
    assert inspected["status"] == "not_started"
    assert inspected["source_rows"] == 2
    with engine.connect() as conn:
        assert conn.execute(text("SELECT to_regclass('market.fact_storage_state')")).scalar_one() is None
        source_before = conn.execute(text(
            "SELECT to_jsonb(facts) FROM market.fact_versions facts ORDER BY revision"
        )).scalars().all()
    with pytest.raises(ValueError, match="writers_stopped"):
        run_cutover(engine, execute=True)
    first = run_cutover(engine, execute=True, writers_stopped=True, batch_rows=1, max_pages=1)
    assert first["status"] == "copying"
    assert first["evidence"]["copied_rows"] == 1
    interrupted = Database(storage.dsn)
    try:
        assert interrupted.ensure_schema() is False
        assert "cutover is not ready" in str(interrupted.last_error)
    finally:
        interrupted._reset_engine()
    with pytest.raises(DBAPIError, match="immutable"):
        with engine.begin() as conn:
            conn.execute(text(f"INSERT INTO {SOURCE} SELECT * FROM {SOURCE} LIMIT 1"))
    second = run_cutover(engine, execute=True, writers_stopped=True, batch_rows=1, max_pages=1)
    assert second["evidence"]["copied_rows"] == 2
    final = run_cutover(engine, execute=True, writers_stopped=True, batch_rows=1, max_pages=1)
    assert final["status"] == "ready"
    assert final["source_retained"] is True
    assert final["evidence"]["verified_rows"] == 2
    assert run_cutover(engine, execute=True, writers_stopped=True) == final
    with engine.connect() as conn:
        target_after = conn.execute(text(
            "SELECT to_jsonb(facts)-'storage_day' FROM market.fact_rows facts ORDER BY revision"
        )).scalars().all()
        assert target_after == source_before
    restarted = Database(storage.dsn)
    try:
        assert restarted.ensure_schema(), str(restarted.last_error)
    finally:
        restarted._reset_engine()
    assert _read(storage, known_at_lte=BASE) == original
    assert _ingest(storage, corrected).noop_count == 1
