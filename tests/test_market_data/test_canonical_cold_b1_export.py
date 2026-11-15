from dataclasses import replace
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

from market_data.archive import FilesystemRawArchiveObjectStore
from market_data.contracts import CANDLE_FACT_TYPE, CANDLE_FACT_VERSION, CandleFact, DatasetSeriesRequest
from portal.backend.db.cold_history_models import COLD_HISTORY_LAYOUT_VERSION
from portal.backend.db.cold_retirement_schema import prepare_cold_dependency_bindings
from portal.backend.db.session import Database
from portal.backend.service.storage.repos import market_data
from portal.backend.service.storage.repos.fact_archival import PostgresCanonicalFactArchiveRepository
from portal.backend.service.storage.repos.fact_reclamation import PostgresCanonicalFactReclamationRepository
from scripts.db import canonical_cold_b1_export as export
from tests.test_market_data.test_canonical_cold_activate_db import _configure_fixture_catalog
from tests.test_market_data.test_fact_derived_retention_db import _archive_day
from tests.test_market_data.test_fact_storage_tiers_db import _placement, storage  # noqa: F401


def _record(row):
    return {"series_id": row.series_id, "revision": row.revision, "market_commit_seq": row.market_commit_seq,
            "source_identity_key": row.source_identity_key, "canonical_material_hash": row.canonical_material_hash,
            "fact": row.fact.to_dict()}


def _selection(dataset, series, source, start, end, rows):
    manifest = next(row for row in dataset.series if row["series_id"] == series)
    return export.Selection(dataset.dataset_id, dataset.dataset_hash, dataset.max_commit_seq, series, source,
        start, end, len(rows), export.bench._digest(export._material(manifest)),
        export.bench._digest(export._material([_record(row) for row in rows])))


def _candles(start):
    result = []
    for ordinal in range(4):
        opened = start + timedelta(minutes=15 * ordinal)
        closed = opened + timedelta(minutes=15)
        result.append(CandleFact(open_time=opened, close_time=closed, open=100 + ordinal, high=110,
            low=90, close=100 + ordinal, volume=10, accepted_at=closed, known_at=closed,
            known_at_method="interval_close_inferred"))
    return result


def test_b1_export_rejects_changed_clock_order_and_manifest():
    start = datetime(2024, 1, 1, tzinfo=UTC)
    end = start + timedelta(hours=1)
    dataset = SimpleNamespace(dataset_id="dataset", dataset_hash="a" * 64, max_commit_seq=4,
                              series=[{"series_id": 7, "payload_schemas": [], "range_start": start}])
    rows = [SimpleNamespace(series_id=7, revision=1, source_identity_key="source", market_commit_seq=ordinal,
                            canonical_material_hash=str(ordinal) * 64, fact=fact)
            for ordinal, fact in enumerate(_candles(start), 1)]
    selected = _selection(dataset, 7, "source", start, end, rows)

    class Repo:
        def get_dataset(self, dataset_id):
            assert dataset_id == dataset.dataset_id
            return dataset

        def read_dataset_series(self, **scope):
            assert scope == {"dataset_id": "dataset", "series_id": 7, "start": start, "end": end,
                "known_at_lte": end, "source_identity_keys": ("source",), "causal_at_interval_close": True}
            return rows

    repo = Repo()
    observed = export.capture(repo, selected)
    assert observed["records"] == export._material([_record(row) for row in rows])
    assert (observed["provider_access"], observed["read_only"], observed["new_check"]) == (False, True, False)
    with pytest.raises(ValueError, match="series manifest changed"):
        export.capture(repo, replace(selected, manifest_sha256="b" * 64))
    rows[0].fact = replace(rows[0].fact, accepted_at=rows[0].fact.accepted_at + timedelta(seconds=1))
    with pytest.raises(ValueError, match="ordered candle material or clock changed"):
        export.capture(repo, selected)
    rows.reverse()
    with pytest.raises(ValueError, match="selection changed at ordinal 0"):
        export.capture(repo, selected)


def test_b1_archive_scope_is_bound_to_clone_but_may_use_a_separate_mount():
    proof = {"database_identity": "123/16384", "accepted_commit_seq": 207402044,
             "device_id": "8:0", "archive_inode": 111, "archive_objects_inode": 112}
    clone_digest = "a" * 64
    cutoff = "2026-10-18"
    scope = {"schema": export.ARCHIVE_SCOPE_VERSION, "clone_proof_sha256": clone_digest,
             "database_identity": proof["database_identity"], "accepted_commit_seq": proof["accepted_commit_seq"],
             "cold_before_day": cutoff, "execution_hash": "b" * 64, "loaded_inventory_hash": "c" * 64,
             "device_id": "8:16", "filesystem_uuid": "fixture-hdd", "archive_inode": 211,
             "archive_objects_inode": 212, "verification_sha256": "d" * 64}
    export.validate_archive_scope(scope, proof=proof, proof_sha256=clone_digest, retire_before=cutoff)
    with pytest.raises(ValueError, match="archive scope does not match"):
        export.validate_archive_scope(scope | {"clone_proof_sha256": "0" * 64}, proof=proof,
                                       proof_sha256=clone_digest, retire_before=cutoff)


@pytest.mark.db
def test_b1_candidate_capture_uses_native_cold_page_and_retained_hot_rows(storage, tmp_path, monkeypatch):
    monkeypatch.setenv("MARKET_STRUCTURE_STORAGE_ROOT", str(tmp_path))
    monkeypatch.setenv("QT_HISTORY_READ_CACHE_BYTES", "0")
    start = datetime(2024, 1, 1, tzinfo=UTC)
    end = start + timedelta(hours=1)
    cold_day = storage.today - timedelta(days=2)
    series = storage.repo.register_series(instrument_id="storage-fixture", fact_type=CANDLE_FACT_TYPE,
        timeframe_seconds=900, contract_version=CANDLE_FACT_VERSION)
    _placement(monkeypatch, cold_day)
    candles = _candles(start)
    storage.repo.ingest_candles(series_id=series, source_id=storage.source_id, facts=candles[:3])
    _placement(monkeypatch, storage.today)
    storage.repo.ingest_candles(series_id=series, source_id=storage.source_id, facts=candles[3:])
    dataset = storage.repo.freeze_dataset([DatasetSeriesRequest(series, start, end)])
    source = storage.fact.source.identity_key
    selected_rows = storage.repo.read_dataset_series(dataset_id=dataset.dataset_id, series_id=series,
        start=start, end=end, known_at_lte=end, source_identity_keys=(source,), causal_at_interval_close=True)
    assert len(selected_rows) == 4
    expected = _selection(dataset, series, source, start, end, selected_rows)
    before = export.capture(storage.repo, expected)

    store = FilesystemRawArchiveObjectStore(tmp_path / "objects")
    archive = PostgresCanonicalFactArchiveRepository(database=storage.database, object_store=store,
                                                      temporary_directory=tmp_path / "staging")
    _archive_day(archive, cold_day)
    with storage.database.session() as session:
        owner = session.scalar(text("""
            SELECT system_identifier::text || '/' || (SELECT oid FROM pg_database WHERE datname=current_database())
            FROM pg_control_system()
        """))
        _configure_fixture_catalog(session.connection(), owner, store.root)
        prepare_cold_dependency_bindings(session.connection())
        session.execute(text("UPDATE market.fact_storage_state SET layout_version=:layout, "
                             "evidence=jsonb_build_object('cold_before_day', CAST(:day AS text))"),
                        {"layout": COLD_HISTORY_LAYOUT_VERSION, "day": cold_day + timedelta(days=1)})
    reclaimer = PostgresCanonicalFactReclamationRepository(archive_repository=archive, enabled=True)
    for _ in range(8):
        retired = reclaimer.reclaim_partition(cold_day, eligible_before=storage.today, execute=True)
        if retired["status"] == "partition_reclaimed":
            break
    else:
        pytest.fail("B1 fixture cold retirement did not complete")
    with storage.database.session() as session:
        assert session.scalar(text("SELECT count(*) FROM market.fact_versions WHERE series_id=:series"), {"series": series}) == 1
        assert session.scalar(text("SELECT count(*) FROM market.cold_fact_locators WHERE series_id=:series"), {"series": series}) == 3

    reader = Database()
    reader._engine = create_engine(storage.dsn, connect_args={"options": "-c default_transaction_read_only=on"})
    reader._session_factory = sessionmaker(bind=reader._engine, expire_on_commit=False, autoflush=False)
    reader._available = True
    monkeypatch.setattr(market_data, "db", reader)
    try:
        candidate = export.capture(storage.repo, expected)
        assert {k: v for k, v in candidate.items() if k != "resource_metrics"} == {
            k: v for k, v in before.items() if k != "resource_metrics"
        }
        used = candidate["resource_metrics"]["consumed"]["archive_source_bytes"]
        assert used > 0
        with reader._engine.begin() as conn:
            role, role_oid, epoch = conn.exec_driver_sql("""
                SELECT session_user::text, (SELECT oid FROM pg_roles WHERE rolname=session_user),
                       extract(epoch FROM pg_postmaster_start_time())::text
            """).one()
        with pytest.raises(ValueError, match="promoted clone identity"):
            export.verify_candidate(reader._engine, url=reader._engine.url,
                proof={"database_identity": owner, "role": role, "role_oid": role_oid,
                       "postmaster_epoch": epoch, "replay_lsn": "0/1000000"},
                archive_scope={}, retire_before=(cold_day + timedelta(days=1)).isoformat())
        print(f"QT_B1_EXPORT_FIXTURE synthetic_rows=4 cold_rows=3 hot_rows=1 archive_source_bytes={used} ordered_sha256={expected.records_sha256}")
    finally:
        reader._reset_engine()
