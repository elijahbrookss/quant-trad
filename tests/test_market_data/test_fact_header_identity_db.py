"""Global identities survive dated header placement without weakened uniqueness."""
from datetime import timedelta

import pytest
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError

from portal.backend.db import MarketFactVersionRecord
from portal.backend.db.fact_identity_schema import (
    assert_fact_identity_contract, ensure_fact_header_partition, fact_header_partition_name,
)
from portal.backend.db.fact_storage_schema import ensure_fact_payload_partition
from portal.backend.db.session import Database
from tests.test_market_data.test_fact_storage_tiers_db import storage, _ingest, _read

pytestmark = pytest.mark.db


def _duplicate_header(session, day, *, same_id):
    columns = [column.name for column in MarketFactVersionRecord.__table__.columns]
    expressions = [
        ":day" if name == "storage_day" else
        "repeat('f',64)" if name == "id" and not same_id else name
        for name in columns
    ]
    session.execute(text(
        "INSERT INTO market.fact_versions (" + ",".join(columns) + ") SELECT " +
        ",".join(expressions) + " FROM market.fact_versions LIMIT 1"
    ), {"day": day})


@pytest.mark.parametrize("same_id", [True, False])
def test_global_identity_rejects_duplicate_across_days(storage, same_id):
    _ingest(storage)
    before = _read(storage)
    other_day = storage.today - timedelta(days=100)
    error = "canonical_fact_identity_mismatch" if same_id else "uq_market_fact_identity_revision"
    with pytest.raises(DBAPIError, match=error):
        with storage.database.session() as session:
            ensure_fact_payload_partition(session.connection(), other_day)
            _duplicate_header(session, other_day, same_id=same_id)
    assert _read(storage) == before
    with storage.database.session() as session:
        assert session.execute(text("SELECT count(*) FROM market.fact_identities")).scalar_one() == 1
        assert session.execute(text("SELECT count(*) FROM market.fact_versions")).scalar_one() == 1
        assert session.execute(text("SELECT count(*) FROM market.fact_hot_payloads")).scalar_one() == 1


def test_orphan_identity_and_mutation_are_rejected(storage):
    with pytest.raises(DBAPIError, match="canonical_fact_identity_header_missing"):
        with storage.database.session() as session:
            session.execute(text(
                "INSERT INTO market.fact_identities(id,storage_day,series_id,observation_key,revision) "
                "VALUES (repeat('a',64),:day,:series,'orphan',1)"
            ), {"day": storage.today, "series": storage.series_id})
    _ingest(storage)
    with pytest.raises(DBAPIError, match="immutable canonical identity"):
        with storage.database.session() as session:
            session.execute(text("UPDATE market.fact_identities SET storage_day=storage_day+1"))
    with pytest.raises(DBAPIError, match="immutable canonical identity"):
        with storage.database.session() as session:
            session.execute(text("DELETE FROM market.fact_header_partitions"))


def test_missing_registered_header_partition_is_not_recreated(storage):
    _ingest(storage)
    name = fact_header_partition_name(storage.today)
    with storage.database.session() as session:
        session.execute(text(f"DROP TABLE market.{name}"))
    with pytest.raises(RuntimeError, match="fact_header_partition_missing_or_incompatible"):
        with storage.database.session() as session:
            ensure_fact_header_partition(session.connection(), storage.today)
    with storage.database.session() as session:
        assert session.execute(text("SELECT to_regclass(:name)"),
                               {"name": "market." + name}).scalar_one() is None
    restarted = Database(storage.dsn)
    try:
        assert restarted.ensure_schema() is False
        assert "fact_header_partition_missing_or_incompatible" in str(restarted.last_error)
    finally:
        restarted._reset_engine()


def test_unknown_partition_and_disabled_guard_are_refused(storage):
    day = storage.today - timedelta(days=100)
    name = fact_header_partition_name(day)
    with storage.database.session() as session:
        session.execute(text(f"CREATE TABLE market.{name}(unexpected integer)"))
    with pytest.raises(RuntimeError, match="fact_header_partition_unregistered"):
        with storage.database.session() as session:
            ensure_fact_header_partition(session.connection(), day)
    with storage.database.session() as session:
        session.execute(text("ALTER TABLE market.fact_versions DISABLE TRIGGER trg_register_fact_identity"))
    with pytest.raises(RuntimeError, match="Canonical identity enforcement differs"):
        with storage.database.session() as session:
            assert_fact_identity_contract(session.connection())


def test_missing_identity_table_requires_explicit_cutover(storage):
    # No rows in this disposable database. CASCADE deliberately models a damaged
    # existing layout, which startup must reject without manufacturing identity.
    with storage.database.session() as session:
        session.execute(text("DROP TABLE market.fact_identities CASCADE"))
    restarted = Database(storage.dsn)
    try:
        assert restarted.ensure_schema() is False
        assert "fact_identities" in str(restarted.last_error)
        with storage.database.session() as session:
            assert session.execute(text("SELECT to_regclass('market.fact_identities')")).scalar_one() is None
    finally:
        restarted._reset_engine()


def test_id_read_uses_registered_day_without_scanning_other_headers(storage):
    from sqlalchemy import event
    from portal.backend.service.storage.repos.fact_storage import PostgresCanonicalFactStorageRepository
    _ingest(storage)
    original = _read(storage)[0]
    with storage.database.session() as session:
        for delta in range(2, 7):
            ensure_fact_payload_partition(session.connection(), storage.today - timedelta(days=delta))
        identity = session.execute(text("SELECT id FROM market.fact_identities")).scalar_one()
    captured = []
    def observe(conn, cursor, statement, parameters, context, executemany):
        if "versions.storage_day = ANY(" in statement and "WHERE versions.id = ANY(" in statement:
            captured.append((statement, parameters))
    engine = storage.database._engine
    event.listen(engine, "before_cursor_execute", observe)
    try:
        with storage.database.session() as session:
            rows = PostgresCanonicalFactStorageRepository().read_rows_by_ids(session, [identity])
            assert rows[identity]["row_hash"] == original.fact.row_hash
    finally:
        event.remove(engine, "before_cursor_execute", observe)
    assert len(captured) == 1
    statement, parameters = captured[0]
    with engine.connect() as conn:
        plan = conn.exec_driver_sql("EXPLAIN (ANALYZE, FORMAT JSON) " + statement, parameters).scalar_one()
    def executed_headers(node):
        names = set()
        if node.get("Actual Loops", 0) and node.get("Relation Name", "").startswith("fact_versions_"):
            names.add(node["Relation Name"])
        for child in node.get("Plans", []):
            names.update(executed_headers(child))
        return names
    assert executed_headers(plan[0]["Plan"]) == {fact_header_partition_name(storage.today)}


def test_missing_payload_identity_reference_is_not_admitted(storage):
    from portal.backend.db.fact_storage_schema import assert_fact_storage_contract
    with storage.database.session() as session:
        name = session.execute(text(
            "SELECT conname FROM pg_constraint WHERE conrelid='market.fact_hot_payloads'::regclass "
            "AND contype='f'"
        )).scalar_one()
        assert name.replace("_", "").isalnum()
        session.execute(text(f'ALTER TABLE market.fact_hot_payloads DROP CONSTRAINT "{name}"'))
    with pytest.raises(RuntimeError, match="canonical_identity_reference_invalid"):
        with storage.database.session() as session:
            assert_fact_storage_contract(session.connection())


@pytest.mark.parametrize("index_name,columns", [
    ("ix_market_fact_series_material", "material_hash, series_id"),
    ("ix_market_fact_series_source", "source_id, series_id"),
])
def test_current_header_layout_rejects_wrong_lookup_index_order(storage, index_name, columns):
    with storage.database.session() as session:
        session.execute(text(f"DROP INDEX market.{index_name}"))
        session.execute(text(f"CREATE INDEX {index_name} ON market.fact_versions({columns})"))
    with pytest.raises(RuntimeError, match="invalid canonical Fact lookup index definitions"):
        with storage.database.session() as session:
            storage.database._assert_canonical_fact_migration(session.connection())
