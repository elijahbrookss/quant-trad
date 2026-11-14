"""Run against the disposable cold-catalog PostgreSQL fixture only."""

from datetime import timedelta
import os

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.engine import make_url
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from market_data.archive import FilesystemRawArchiveObjectStore
from market_data.cold_history import ColdHistoryReader, publish_cold_page
from portal.backend.db.cold_history_models import Base, COLD_TABLE_NAMES
from portal.backend.service.storage.repos.cold_history import PostgresColdHistory, cold_layout_active
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
                connection.execute(text("CREATE TABLE market.series (id bigint PRIMARY KEY)"))
                connection.execute(text("INSERT INTO market.series VALUES (1), (2)"))
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
