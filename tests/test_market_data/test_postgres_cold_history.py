"""Run against the disposable cold-catalog PostgreSQL fixture only."""

from dataclasses import replace
from contextlib import nullcontext
from datetime import timedelta
import os

import pytest
from sqlalchemy import JSON, bindparam, create_engine, text
from sqlalchemy.engine import make_url
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from market_data.archive import FilesystemRawArchiveObjectStore
from market_data.canonical_storage import record_from_storage_row, record_to_storage_row
from market_data.cold_history import ColdHistoryReader, publish_cold_page
from market_data.contracts import CANDLE_FACT_TYPE, CANDLE_FACT_VERSION
from portal.backend.db.cold_history_models import Base, COLD_TABLE_NAMES
from portal.backend.service.storage.repos.cold_history import PostgresColdHistory, cold_layout_active
from portal.backend.service.storage.repos import fact_dependencies
from portal.backend.service.storage.repos import candles
from portal.backend.service.storage.repos.fact_storage import canonical_fact_storage_repository
from tests.test_market_data.test_canonical_fact_archive import BASE, _row

pytestmark = pytest.mark.db


@pytest.fixture
def database():
    dsn = os.environ.get("PG_DSN")
    if not dsn:
        pytest.skip("requires disposable cold catalog PostgreSQL")
    url = make_url(dsn)
    if url.host != "127.0.0.1" or url.database != "qt_cold_catalog_test":
        pytest.skip("requires dedicated loopback fixture database")
    engine = create_engine(url)
    try:
        with engine.connect() as connection:
            transaction = connection.begin()
            try:
                connection.execute(text("CREATE SCHEMA market"))
                connection.execute(text("""
                    CREATE TABLE market.series (id bigint PRIMARY KEY, instrument_id text, fact_type text,
                        contract_version text, timeframe_seconds integer, dimensions jsonb DEFAULT '{}')
                """))
                connection.execute(text("""
                    INSERT INTO market.series(id,instrument_id,fact_type) VALUES
                        (1, 'scope', 'derivatives.funding_rate'), (2, 'scope', 'market.l2_book')
                """))
                connection.execute(text("""
                    CREATE TABLE market.fact_storage_state (
                        layout_version text PRIMARY KEY, state text NOT NULL, completed_at timestamptz
                    )
                """))
                for table in Base.metadata.sorted_tables:
                    if table.schema == "market" and table.name in COLD_TABLE_NAMES:
                        table.create(connection)
                with Session(connection, join_transaction_mode="create_savepoint") as session:
                    yield session
            finally:
                transaction.rollback()
    finally:
        engine.dispose()


def test_postgres_admission_is_exact_atomic_and_idempotent(database, tmp_path):
    store = FilesystemRawArchiveObjectStore(tmp_path / "objects")
    repository = PostgresColdHistory(ColdHistoryReader(store))
    first = _row()
    second = _row(revision=2, commit=4)
    unrelated = _row(series_id=2, commit=5)
    page = publish_cold_page([first, second], object_store=store, temporary_directory=tmp_path)

    assert not cold_layout_active(database)
    with pytest.raises(RuntimeError, match="cold_fact_exact_identity_missing"):
        repository.read_by_ids(database, [first["id"]])
    repository.admit(database, page)
    repository.admit(database, page)

    scope = {"series_id": 1, "start": BASE, "end": BASE + timedelta(seconds=1)}
    assert list(repository.candidate_pages(database, **scope)) == [page]
    assert list(repository.candidate_pages(database, **scope, as_of_commit_seq=0)) == []
    assert repository.read_by_ids(database, [first["id"], second["id"]]) == {
        first["id"]: first, second["id"]: second,
    }
    assert repository.latest_for_keys(
        database, series_id=1, keys=[first["observation_key"], "absent-key"],
    ) == {second["observation_key"]: second}

    collision = publish_cold_page(
        [first, unrelated], object_store=store, temporary_directory=tmp_path,
    )
    with pytest.raises(IntegrityError):
        repository.admit(database, collision)
    assert list(repository.candidate_pages(database, **scope)) == [page]
    assert repository.read_by_ids(database, [unrelated["id"]], require_all=False) == {}
    with pytest.raises(RuntimeError, match="test interrupted"):
        with database.begin_nested():
            new_page = publish_cold_page([unrelated], object_store=store, temporary_directory=tmp_path)
            repository.admit(database, new_page)
            raise RuntimeError("test interrupted")
    assert repository.read_by_ids(database, [unrelated["id"]], require_all=False) == {}


def test_catalog_witnesses_preserve_exact_material_and_causal_scope(database, tmp_path):
    initial = record_from_storage_row(_row())
    row = record_to_storage_row(replace(
        initial, row_hash=None, fact_version_id=None,
        fact=replace(initial.fact, provenance={
            "pinned": {"legacy_material_hash": 101098},
            "_qt_normalization_evidence": {"spec_id": "nsp_legacy"},
        }),
    ), series_dimensions={})
    store = FilesystemRawArchiveObjectStore(tmp_path / "objects")
    repository = PostgresColdHistory(ColdHistoryReader(store))
    page = publish_cold_page([row], object_store=store, temporary_directory=tmp_path)
    repository.admit(database, page)
    assert repository.material_candidates(
        database, series_ids=[1], material_hashes=[row["material_hash"]], include_legacy=False,
    ) == ((row["id"], row["material_hash"]),)
    assert repository.material_candidates(
        database, series_ids=[1], material_hashes=["101098"], evidence_key="pinned", include_canonical=False,
    ) == ((row["id"], "101098"),)
    assert not repository.material_candidates(
        database, series_ids=[1], material_hashes=["101098"], evidence_key="different", include_canonical=False,
    )
    assert list(repository.normalization_spec_ids(database, "nsp_legacy")) == [row["id"]]
    assert repository.commit_candidate_id(database, series_id=1, commit_seq=1) == row["id"]
    request = {"root_id": "root", "instrument_id": "scope", "fact_type": row["fact_type"],
               "series_id": 1, "source_id": row["source_id"], "root_commit": 1, "known_at": row["known_at"],
               "range_start": BASE, "range_end": BASE, "include_end": True}
    json_scope = {key: value.isoformat() if hasattr(value, "isoformat") else value for key, value in request.items()}
    scope = dict(indexed={"root": request}, requests=[json_scope], cold=repository, max_rows=5, check_budget=None)
    rows, selections, bytes_needed = fact_dependencies._cold_window_revisions(
        database, max_logical_bytes=page.logical_byte_count, **scope,
    )
    assert bytes_needed == page.logical_byte_count and rows == {row["id"]: row}
    assert dict(selections) == {"root": [row["id"]]}
    with pytest.raises(fact_dependencies.CanonicalSourceByteBudgetExceeded):
        fact_dependencies._cold_window_revisions(database, max_logical_bytes=page.logical_byte_count - 1, **scope)


def test_staged_page_appears_only_after_retirement_receipt(database, tmp_path):
    store = FilesystemRawArchiveObjectStore(tmp_path / "objects")
    repository = PostgresColdHistory(ColdHistoryReader(store))
    row, staged = _row(), _row(revision=2, commit=4)
    visible_page = publish_cold_page([row], object_store=store, temporary_directory=tmp_path)
    staged_page = publish_cold_page([staged], object_store=store, temporary_directory=tmp_path)
    day = BASE.date()
    repository.admit(database, visible_page)
    repository.admit(database, staged_page, staged_storage_day=day)
    repository.admit(database, staged_page, staged_storage_day=day)
    with pytest.raises(RuntimeError, match="cold_fact_page_publication_conflict"):
        repository.admit(database, staged_page)

    scope = {"series_id": 1, "start": BASE, "end": BASE + timedelta(seconds=1)}
    key = row["observation_key"]
    assert list(repository.candidate_pages(database, **scope)) == [visible_page]
    assert repository.read_by_ids(database, [staged["id"]], require_all=False) == {}
    assert repository.latest_for_keys(database, series_id=1, keys=[key]) == {key: row}
    assert repository.key_candidate_ids(database, series_id=1, key=key, max_candidates=1) == (row["id"],)
    assert repository.commit_candidate_id(database, series_id=1, commit_seq=4) is None
    assert repository.material_candidates(
        database, series_ids=[1], material_hashes=[staged["material_hash"]], include_legacy=False,
    ) == ()

    retirement = text("""
        INSERT INTO market.cold_fact_retired_days
            (storage_day, manifest_set_hash, row_count, reclaimed_header_bytes, protected_dataset_ranges)
        VALUES (:day, :manifest_hash, 1, 0, 0)
    """)
    receipt = {"day": day, "manifest_hash": staged_page.manifest_hash}
    with pytest.raises(RuntimeError, match="test interrupted"):
        with database.begin_nested():
            database.execute(retirement, receipt)
            assert repository.latest_for_keys(database, series_id=1, keys=[key]) == {key: staged}
            raise RuntimeError("test interrupted")
    assert repository.latest_for_keys(database, series_id=1, keys=[key]) == {key: row}

    database.execute(retirement, receipt)
    assert list(repository.candidate_pages(database, **scope)) == [visible_page, staged_page]
    assert repository.read_by_ids(database, [staged["id"]]) == {staged["id"]: staged}
    assert repository.latest_for_keys(database, series_id=1, keys=[key]) == {key: staged}
    assert repository.commit_candidate_id(database, series_id=1, commit_seq=4) == staged["id"]
    assert repository.material_candidates(
        database, series_ids=[1], material_hashes=[staged["material_hash"]], include_legacy=False,
    ) == ((staged["id"], staged["material_hash"]),)


def test_candle_seek_orders_exact_observations_and_obeys_frozen_watermark(database, tmp_path):
    database.execute(text("""
        CREATE TABLE market.fact_versions (series_id bigint, observation_time timestamptz, market_commit_seq bigint)
    """))
    later = BASE + timedelta(hours=1)
    intervening = BASE + timedelta(minutes=10)
    database.execute(text("INSERT INTO market.fact_versions VALUES (1, :observed, 9)"), {"observed": intervening})
    base = record_from_storage_row(_row())
    first = record_to_storage_row(base, series_dimensions={})
    second = record_to_storage_row(replace(
        base, market_commit_seq=3, row_hash=None, fact_version_id=None,
        fact=replace(base.fact, observation_key="later", observation_time=later),
    ), series_dimensions={})
    store = FilesystemRawArchiveObjectStore(tmp_path / "objects")
    repository = PostgresColdHistory(ColdHistoryReader(store))
    repository.admit(database, publish_cold_page([first, second], object_store=store, temporary_directory=tmp_path))
    end = later + timedelta(microseconds=1)
    scope = dict(series_id=1, start=BASE, end=end)
    assert repository.candle_nearest_observation(
        database, **scope, prefer_latest=False, cursor=BASE, commit=None,
    ) == BASE
    assert repository.candle_nearest_observation(
        database, **scope, prefer_latest=False, cursor=BASE + timedelta(microseconds=1), commit=None,
    ) == intervening
    assert repository.candle_nearest_observation(
        database, **scope, prefer_latest=True, cursor=end, commit=None,
    ) == later
    assert repository.candle_nearest_observation(
        database, **scope, prefer_latest=True, cursor=end, commit=2,
    ) == BASE


def test_candle_entry_points_merge_frozen_and_live_paging(database, tmp_path, monkeypatch):
    def revision(hour, commit, *, version=1):
        base = record_from_storage_row(_row())
        opened = BASE + timedelta(hours=hour)
        closed = opened + timedelta(hours=1)
        payload = {"close_time": closed, "open": 1.1, "high": 3.0, "low": 0.1,
                   "close": 1.2 * version, "volume": None, "trade_count": None}
        fact = replace(base.fact, fact_type=CANDLE_FACT_TYPE, payload_schema_id=CANDLE_FACT_VERSION,
                       observation_key=opened.isoformat(), observation_time=opened,
                       known_at=closed, accepted_at=closed, payload=payload)
        return record_to_storage_row(replace(
            base, fact=fact, revision=version, market_commit_seq=commit, fact_version_id=None, row_hash=None,
        ), series_dimensions={})

    archived, gapped, hot_corrected, hot_current = (
        revision(0, 1), revision(1000, 2), revision(1000, 3, version=2), revision(1001, 4),
    )
    source_fields = {"source_identity_key", "source_provider", "source_venue", "source_kind", "source_adapter_version"}
    document_fields = ("payload", "provenance", "quality")
    fields = sorted(set(archived) - source_fields - set(document_fields) - {"series_dimensions"})
    types = {**dict.fromkeys(("series_id", "source_id", "market_commit_seq", "revision"), "bigint"),
             **dict.fromkeys(("observation_time", "known_at", "accepted_at", "received_at", "source_published_at"), "timestamptz")}
    columns = ", ".join(f"{name} {types.get(name, 'text')}" for name in fields)
    database.execute(text(f"CREATE TABLE market.fact_versions ({columns}, storage_day date DEFAULT current_date)"))
    database.execute(text("""CREATE FUNCTION market.read_fact_headers_in_range(bigint,timestamptz,timestamptz)
        RETURNS SETOF market.fact_versions LANGUAGE sql STABLE AS $$
        SELECT * FROM market.fact_versions WHERE series_id=$1 AND observation_time >= $2 AND observation_time < $3
        $$"""))
    database.execute(text("CREATE TABLE market.fact_hot_payloads (id text, storage_day date DEFAULT current_date, "
                          "payload jsonb, provenance jsonb, quality jsonb)"))
    database.execute(text("""CREATE TABLE market.sources
        (id bigint, identity_key text, provider text, venue text, source_kind text, adapter_version text)"""))
    database.execute(text("""INSERT INTO market.sources VALUES
        (:source_id,:source_identity_key,:source_provider,:source_venue,:source_kind,:source_adapter_version)"""), archived)
    database.execute(text("UPDATE market.series SET fact_type=:family,contract_version=:version,timeframe_seconds=3600 WHERE id=1"),
                     {"family": CANDLE_FACT_TYPE, "version": CANDLE_FACT_VERSION})
    values = ", ".join(f":{name}" for name in fields)
    database.execute(text(f"INSERT INTO market.fact_versions ({', '.join(fields)}) VALUES ({values})"),
                     [hot_corrected, hot_current])
    database.execute(text("INSERT INTO market.fact_hot_payloads(id,payload,provenance,quality) VALUES (:id,:payload,:provenance,:quality)")
                     .bindparams(*(bindparam(name,type_=JSON) for name in document_fields)), [hot_corrected, hot_current])
    database.execute(text("""CREATE TABLE market.dataset_series
        (dataset_id text, series_id bigint, range_start timestamptz, range_end timestamptz, max_commit_seq bigint)"""))
    end = hot_current["observation_time"] + timedelta(hours=1)
    database.execute(text("INSERT INTO market.dataset_series VALUES ('frozen',1,:start,:end,2)"), {"start": BASE, "end": end})
    store = FilesystemRawArchiveObjectStore(tmp_path / "objects")
    repository = PostgresColdHistory(ColdHistoryReader(store))
    repository.admit(database, publish_cold_page([archived], object_store=store, temporary_directory=tmp_path))
    repository.admit(database, publish_cold_page([gapped], object_store=store, temporary_directory=tmp_path))
    database.execute(text("INSERT INTO market.fact_storage_state VALUES ('market.canonical_cold_history.v1','ready',now())"))
    monkeypatch.setattr(candles.db, "session", lambda: nullcontext(database))
    monkeypatch.setattr(canonical_fact_storage_repository, "cold_history", lambda _: repository)
    monkeypatch.setattr("portal.backend.service.storage.repos.fact_storage.ensure_payload_contracts", lambda *_: None)

    scope = dict(instrument_id="scope", timeframe="1h", start=BASE, end=end)
    earliest = candles.list_candles_for_series(**scope, limit=2)
    latest = candles.list_candles_for_series(**scope, limit=2, prefer_latest=True)
    assert [row["market_commit_seq"] for row in earliest] == [1, 3]
    assert [row["market_commit_seq"] for row in latest] == [3, 4]
    summary = candles.get_candle_storage_summary(**scope)
    assert summary["candle_count"] == 3 and summary["missing_count"] == 999
    frozen = candles.read_frozen_dataset_candles(dataset_id="frozen", series_id=1, start=BASE, end=end, limit=2)
    assert [row["market_commit_seq"] for row in frozen["candles"]] == [1, 2]
