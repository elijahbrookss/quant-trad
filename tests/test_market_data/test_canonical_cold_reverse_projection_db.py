from dataclasses import replace
from datetime import UTC, datetime, time, timedelta
import json
from types import SimpleNamespace
from uuid import uuid4

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.exc import IntegrityError, InternalError

from market_data.archive import FilesystemRawArchiveObjectStore
from market_data.cold_history import ColdHistoryReader, publish_cold_page
from market_data.contracts import CANDLE_FACT_TYPE, CANDLE_FACT_VERSION, CandleFact, SourceIdentity
from portal.backend.db.cold_history_models import COLD_HISTORY_LAYOUT_VERSION
from portal.backend.db.cold_retirement_schema import prepare_cold_dependency_bindings
from portal.backend.db.fact_storage_schema import FACT_STORAGE_LAYOUT_VERSION, ensure_fact_payload_partition
from portal.backend.db.market_data_models import MarketDataIngestionRunRecord
from portal.backend.db.session import Database
from portal.backend.service.storage import history_policy
from portal.backend.service.storage.repos import cold_history, market_data
from portal.backend.service.storage.repos.fact_storage import CANONICAL_ROW_COLUMNS, CANONICAL_ROW_FROM
from scripts.db.canonical_cold_database_fence import close_database_fence
from scripts.db.canonical_cold_migration import PostgresMigrationSource
from scripts.db.canonical_cold_reverse_allocator import capture_allocator, synchronize_allocator
from scripts.db.canonical_cold_reverse_projection import ReversePage, rematerialize_page, restore_candidate_only_pages
from tests.test_market_data.test_canonical_cold_activate_db import _configure_fixture_catalog, _fence_connections
from tests.test_market_data.test_fact_storage_tiers_db import _placement, storage  # noqa: F401


pytestmark = pytest.mark.db


def test_reverse_projection_rehydrates_verified_old_and_candidate_only_rows_under_original_guards(storage, tmp_path, monkeypatch):
    source_db = storage.database
    engine = source_db._engine
    old_name = f"qt_bench_reverse_{uuid4().hex[:12]}"
    old_url = engine.url.set(database=old_name)
    admin = create_engine(engine.url.set(database="postgres"), isolation_level="AUTOCOMMIT")
    storage_root = tmp_path / "archive"
    store = FilesystemRawArchiveObjectStore(storage_root / "objects")
    monkeypatch.setenv("MARKET_STRUCTURE_STORAGE_ROOT", str(storage_root))
    monkeypatch.setattr(cold_history, "require_cold_acquisition_headroom", lambda session: None)
    monkeypatch.setattr(history_policy, "_read", lambda session: (SimpleNamespace(movement_enabled=True), None))
    monkeypatch.setattr(history_policy, "_archive_policy", lambda session, saved, defaults, root: defaults)
    # Only historical SSD policy/headroom is modeled by the fixture. The
    # native publisher, verified files, table guards, and read APIs run intact.
    with source_db.session() as session:
        database_identity = session.scalar(text("""SELECT c.system_identifier::text || '/' || d.oid::text
            FROM pg_control_system() AS c CROSS JOIN pg_database AS d WHERE d.datname=current_database()"""))
        _configure_fixture_catalog(session.connection(), database_identity, storage_root)
        session.execute(text("""INSERT INTO public.portal_storage_policy(id,revision,policy,updated_at)
            VALUES (1,1,jsonb_build_object('recent_days',14),clock_timestamp())"""))
        # The restore has passed operational-layout repair before rows are
        # loaded, including provisioned empty header/payload days.
        old_day = storage.today - timedelta(days=1)
        ensure_fact_payload_partition(session.connection(), old_day)

    source_id = storage.repo.register_source(SourceIdentity(provider="COINBASE", venue="COINBASE_DIRECT",
        source_kind="historical_api", adapter_version="reverse.fixture.v1"))
    series = storage.repo.register_series(instrument_id="storage-fixture", fact_type=CANDLE_FACT_TYPE,
                                           timeframe_seconds=60, contract_version=CANDLE_FACT_VERSION)
    start = datetime.combine(storage.today - timedelta(days=90), time(), tzinfo=UTC)
    accepted = datetime.combine(storage.today, time(hour=1), tzinfo=UTC)
    original = CandleFact(open_time=start, close_time=start+timedelta(minutes=1), open=100, high=105, low=95,
        close=103, volume=1, received_at=accepted, accepted_at=accepted, known_at=accepted,
        known_at_method="platform_acceptance")
    old_db = Database(old_url.render_as_string(hide_password=False))
    try:
        _placement(monkeypatch, old_day)
        original_outcome = storage.repo.ingest_candles(
            series_id=series, source_id=source_id, facts=[original], ingestion_run_id="older-reverse")
        # With no active SQL connections or writers, clone one consistent v2
        # checkpoint before candidate activation and any later accepted rows.
        engine.dispose()
        with admin.connect() as conn:
            conn.exec_driver_sql(f"CREATE DATABASE {old_name} TEMPLATE {engine.url.database}")
        assert old_db.ensure_schema(), str(old_db.last_error)
        with engine.begin() as conn:
            record, = conn.execute(text(f"SELECT {CANONICAL_ROW_COLUMNS} {CANONICAL_ROW_FROM} WHERE versions.series_id=:series"),
                                   {"series": series}).mappings().all()
            original_page = publish_cold_page([record], object_store=store, temporary_directory=tmp_path)
            prepare_cold_dependency_bindings(conn)
            conn.execute(text("UPDATE market.fact_storage_state SET layout_version=:layout,evidence=CAST(:evidence AS jsonb)"), {
                "layout": COLD_HISTORY_LAYOUT_VERSION,
                "evidence": json.dumps({"execution_hash": "a" * 64, "source_commit_seq": original_outcome.max_commit_seq}),
            })
        correction = replace(original, close=104, received_at=accepted+timedelta(minutes=1),
                             accepted_at=accepted+timedelta(minutes=1), known_at=accepted+timedelta(minutes=1))
        # A correction with a hot head remains hot. Pick an independent old
        # observation to exercise the native candidate-only cold path.
        correction = replace(correction, open_time=start+timedelta(minutes=1), close_time=start+timedelta(minutes=2))
        outcome = storage.repo.ingest_candles(series_id=series, source_id=source_id, facts=[correction],
                                               ingestion_run_id="candidate-only-reverse")
        assert outcome.inserted_count == 1
        selection = {"series_id": series, "start": start, "end": start+timedelta(minutes=3)}
        expected = storage.repo.read_fact_revisions(**selection)
        assert len(expected) == 2
        with engine.connect() as conn:
            assert conn.scalar(text("SELECT count(*) FROM market.fact_versions WHERE series_id=:series"), {"series": series}) == 1
            descriptor, digest = conn.execute(text("SELECT descriptor,manifest_hash FROM market.cold_fact_pages")).one()
            from market_data.fact_archive import FactArchiveManifest
            candidate_page = FactArchiveManifest.from_dict(descriptor, expected_hash=digest)
            runs = [dict(row) for row in conn.execute(text("SELECT * FROM market.ingestion_runs WHERE id=:id"),
                                                     {"id": "candidate-only-reverse"}).mappings()]
            system_id = conn.scalar(text("SELECT system_identifier::text FROM pg_control_system()"))
        with engine.connect() as conn:
            txn = conn.begin()
            conn.scalar(text("SELECT nextval('market.fact_commit_seq')"))
            burned = conn.scalar(text("SELECT nextval('market.fact_commit_seq')"))
            txn.rollback()  # The source still consumed both. Neither is an accepted fact.
        reader = ColdHistoryReader(FilesystemRawArchiveObjectStore(store.root, writable=False))
        with old_db._engine.begin() as conn:
            identity = PostgresMigrationSource(old_db._engine, expected_system_identifier=system_id).identity(conn)
        with engine.begin() as conn:
            source_identity = PostgresMigrationSource(engine, expected_system_identifier=system_id).identity(conn)

        def retain(conn, control, expected):
            return close_database_fence(conn, control, expected_database=expected, timeout_seconds=30,
                                        record=lambda _phase, _evidence: None)

        with _fence_connections(storage) as (conn, control):
            source_fence = retain(conn, control, source_identity)
            with conn.begin():
                boundary = capture_allocator(conn, fence=source_fence, layout=COLD_HISTORY_LAYOUT_VERSION)
                assert (boundary.visible_commit, boundary.last_value, boundary.is_called) == (outcome.max_commit_seq, burned, True)
                assert boundary.next_value == burned + 1 > boundary.visible_commit + 1
                reverse = {"fence": source_fence, "expected_database": source_identity, "expected_execution_hash": "a" * 64,
                           "expected_source_commit": boundary.visible_commit, "reader": reader, "max_pages": 4,
                           "max_bytes": candidate_page.byte_count}
                with pytest.raises(RuntimeError, match="candidate page budget exceeded"):
                    restore_candidate_only_pages(conn, **{**reverse, "max_bytes": candidate_page.byte_count - 1})
                first = restore_candidate_only_pages(conn, **reverse)
                repeat = restore_candidate_only_pages(conn, **reverse)
                assert (first["page_count"], first["row_count"], first["inserted"], repeat["inserted"]) == (1, 1, 1, 0)
                assert first["receipt_sha256"] == repeat["receipt_sha256"]
                assert capture_allocator(conn, fence=source_fence, layout=COLD_HISTORY_LAYOUT_VERSION) == boundary
        original_plan = ReversePage(original_page.manifest_hash, original_page.object_sha256, outcome.max_commit_seq,
                                    identity, {expected[0].fact_version_id: old_day})
        candidate_plan = ReversePage(candidate_page.manifest_hash, candidate_page.object_sha256, outcome.max_commit_seq, identity)

        def project(conn, plan, page):
            return rematerialize_page(conn, plan=plan, manifest=page, reader=reader, expected_plan_hash=plan.plan_hash)

        with _fence_connections(SimpleNamespace(database=old_db)) as (conn, control):
            old_fence = retain(conn, control, identity)
            with conn.begin():
                baseline = capture_allocator(conn, fence=old_fence, layout=FACT_STORAGE_LAYOUT_VERSION)
                assert baseline.visible_commit == original_outcome.max_commit_seq

            def sync():
                return synchronize_allocator(conn, fence=old_fence, candidate=boundary, baseline=baseline,
                    expected_candidate_hash=boundary.sha256, expected_baseline_hash=baseline.sha256)

            with conn.begin():
                with pytest.raises(RuntimeError, match="accepted watermark differs"):
                    sync()
                with pytest.raises(IntegrityError):
                    project(conn, candidate_plan, candidate_page)  # missing native ingestion run FK
                assert conn.scalar(text("SELECT count(*) FROM market.fact_identities")) == 1
                conn.execute(MarketDataIngestionRunRecord.__table__.insert(), runs)
                assert project(conn, original_plan, original_page)["placement_mode"] == "original_storage_day"
                candidate_result = project(conn, candidate_plan, candidate_page)
                assert candidate_result["inserted"] == candidate_result["verified"] == 1
                assert candidate_result["placement_mode"] == "candidate_acceptance_utc_day"
                repeated = project(conn, candidate_plan, candidate_page)
                assert repeated["inserted"] == 0 and repeated["stored_rows_sha256"] == candidate_result["stored_rows_sha256"]
                assert conn.execute(text("SELECT storage_day,market_commit_seq FROM market.fact_versions ORDER BY market_commit_seq")).all() == [
                    (old_day, expected[0].market_commit_seq), (storage.today, expected[1].market_commit_seq)]
                changed_day = replace(candidate_plan, original_storage_days={expected[1].fact_version_id: old_day})
                with pytest.raises(InternalError, match="canonical_fact_identity_mismatch"):
                    project(conn, changed_day, candidate_page)

            with pytest.raises(RuntimeError, match="fixture interruption"):
                with conn.begin():
                    assert sync()["changed"] is True
                    raise RuntimeError("fixture interruption")
            with conn.begin():
                assert conn.exec_driver_sql("SELECT last_value,is_called FROM market.fact_commit_seq").one() == (
                    baseline.last_value, baseline.is_called)
                drift = conn.begin_nested()
                conn.exec_driver_sql(f"ALTER SEQUENCE market.fact_commit_seq RESTART WITH {boundary.next_value - 1}")
                assert conn.scalar(text("SELECT nextval('market.fact_commit_seq')")) == boundary.next_value - 1
                with pytest.raises(RuntimeError, match="advanced independently"):
                    sync()  # equal next number, but allocation consumed it from a different origin
                drift.rollback()
                assert sync()["changed"] is True
            with conn.begin():
                retry = sync()
                assert retry["changed"] is False and retry["next_value"] == boundary.next_value

        old_db._reset_engine()
        assert old_db.ensure_schema(), str(old_db.last_error)
        monkeypatch.setattr(market_data, "db", old_db)
        repository = market_data.PostgresMarketDataRepository()
        assert repository.read_fact_revisions(**selection) == expected
        assert repository.read_facts(**selection, as_of_commit_seq=expected[0].market_commit_seq) == expected[:1]
        assert repository.read_facts(**selection, known_at_lte=accepted) == expected[:1]
        next_fact = replace(correction, open_time=start+timedelta(minutes=2), close_time=start+timedelta(minutes=3))
        new = repository.ingest_candles(series_id=series, source_id=source_id, facts=[next_fact])
        assert new.max_commit_seq == boundary.next_value
        with _fence_connections(SimpleNamespace(database=old_db)) as (conn, control):
            old_fence = retain(conn, control, identity)
            with conn.begin():
                with pytest.raises(RuntimeError, match="accepted watermark differs"):
                    sync()
    finally:
        old_db._reset_engine()
        with admin.connect() as conn:
            conn.exec_driver_sql(f"DROP DATABASE IF EXISTS {old_name} WITH (FORCE)")
        admin.dispose()
