"""Catalog admission uses isolated databases; no live settings or disk probing."""
from dataclasses import asdict

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.exc import DBAPIError

from portal.backend.service.storage.header_catalog import read_header_catalog
from tests.test_market_data.migration_test_support import fresh_migration_database

pytestmark = pytest.mark.db


@pytest.fixture
def catalog_engine():
    with fresh_migration_database("header_catalog", install_extensions=False) as dsn:
        engine = create_engine(dsn, future=True)
        try:
            with engine.begin() as conn:
                conn.execute(text("""
                    CREATE SCHEMA market;
                    CREATE TABLE market.fact_header_partitions(storage_day date PRIMARY KEY);
                    INSERT INTO market.fact_header_partitions VALUES ('2026-09-01'), ('2026-09-02');
                    CREATE TABLE market.fact_versions (
                        id text, storage_day date NOT NULL, payload text
                    ) PARTITION BY RANGE(storage_day);
                    CREATE TABLE market.fact_versions_20260901 PARTITION OF market.fact_versions
                        FOR VALUES FROM ('2026-09-01') TO ('2026-09-02');
                    CREATE TABLE market.fact_versions_20260902 PARTITION OF market.fact_versions
                        FOR VALUES FROM ('2026-09-02') TO ('2026-09-03');
                    CREATE INDEX header_id ON market.fact_versions(id);
                    INSERT INTO market.fact_versions
                        SELECT 'same', '2026-09-01'::date,
                               string_agg(md5(g::text), '')
                        FROM generate_series(1,2000) g;
                    INSERT INTO market.fact_versions VALUES ('same', '2026-09-01', 'other');
                """))
            yield engine
        finally:
            engine.dispose()


def test_inventory_sizes_include_toast_and_all_indexes_without_claiming_drive_binding(catalog_engine):
    with catalog_engine.connect() as conn:
        before = conn.execute(text("SELECT count(*) FROM market.fact_versions")).scalar_one()
        sizes = dict(conn.execute(text("""
            SELECT c.oid::bigint, pg_total_relation_size(c.oid)
            FROM pg_class c JOIN pg_inherits i ON i.inhrelid=c.oid
            WHERE i.inhparent='market.fact_versions'::regclass
        """)).all())
        defaults = conn.execute(text("""
            SELECT oid::bigint,dattablespace::bigint FROM pg_database WHERE datname=current_database()
        """)).one()
    report = read_header_catalog(catalog_engine)
    assert report.snapshot.inventory_complete
    assert len(report.snapshot.partitions) == 2
    assert not report.filesystem_bindings_verified
    for group in report.snapshot.partitions:
        assert group.index_inventory_complete and group.toast_colocated
        assert len(group.indexes) == 1
        assert sum(item.byte_count for item in group.relations) == sizes[group.heap.oid]
        assert all(item.target_id is None for item in group.relations)
    for location in report.physical_locations:
        assert location["tablespace_oid"] == defaults[1] != 0
        assert location["database_oid"] == defaults[0]
        assert location["database_default"]
        assert location["tablespace_location"] == ""
        assert location["relative_path"].startswith(f"base/{defaults[0]}/")
    with catalog_engine.connect() as conn:
        assert conn.execute(text("SELECT count(*) FROM market.fact_versions")).scalar_one() == before
        assert conn.execute(text("SHOW transaction_read_only")).scalar_one() == "off"
    assert asdict(report)["filesystem_bindings_verified"] is False


@pytest.mark.parametrize("alteration", [
    "DROP TABLE market.fact_versions_20260902",
    "ALTER TABLE market.fact_versions DETACH PARTITION market.fact_versions_20260902",
    "INSERT INTO market.fact_header_partitions VALUES ('2026-09-03')",
    "DELETE FROM market.fact_header_partitions WHERE storage_day='2026-09-02'",
])
def test_missing_unregistered_or_detached_partitions_fail_inventory(catalog_engine, alteration):
    with catalog_engine.begin() as conn:
        conn.execute(text(alteration))
    with pytest.raises(RuntimeError, match="registry_attachment_mismatch"):
        read_header_catalog(catalog_engine)


def test_wrong_daily_bound_is_not_trusted_from_name(catalog_engine):
    with catalog_engine.begin() as conn:
        conn.execute(text("""
            DROP TABLE market.fact_versions_20260902;
            CREATE TABLE market.fact_versions_20260902 PARTITION OF market.fact_versions
                FOR VALUES FROM ('2026-09-02') TO ('2026-09-04');
        """))
    with pytest.raises(RuntimeError, match="partition_incompatible"):
        read_header_catalog(catalog_engine)


def test_partition_budget_refuses_partial_inventory(catalog_engine):
    with pytest.raises(RuntimeError, match="partition_budget_exceeded"):
        read_header_catalog(catalog_engine, max_partitions=1)


def test_failed_concurrent_index_is_not_accepted_as_complete_group(catalog_engine):
    with catalog_engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
        with pytest.raises(DBAPIError):
            conn.execute(text("""
                CREATE UNIQUE INDEX CONCURRENTLY failed_unique_header
                ON market.fact_versions_20260901(id)
            """))
    with pytest.raises(RuntimeError, match="index_unproven"):
        read_header_catalog(catalog_engine)


def test_lock_timeout_is_bounded_and_does_not_change_locked_data(catalog_engine):
    with catalog_engine.connect() as blocker:
        transaction = blocker.begin()
        try:
            blocker.execute(text("LOCK TABLE market.fact_versions_20260901 IN ACCESS EXCLUSIVE MODE"))
            with pytest.raises(DBAPIError, match="statement timeout"):
                read_header_catalog(catalog_engine, timeout_seconds=1)
        finally:
            transaction.rollback()
    assert len(read_header_catalog(catalog_engine).snapshot.partitions) == 2


def test_reader_allows_collection_while_holding_its_catalog_locks(catalog_engine):
    from concurrent.futures import ThreadPoolExecutor
    from threading import Event
    from sqlalchemy import event

    locked, release = Event(), Event()

    def pause_after_child_lock(conn, cursor, statement, parameters, context, executemany):
        if statement.startswith('LOCK TABLE ONLY market."fact_versions_'):
            locked.set()
            if not release.wait(10):
                raise RuntimeError("test_reader_release_timeout")

    event.listen(catalog_engine, "after_cursor_execute", pause_after_child_lock)
    try:
        with ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(read_header_catalog, catalog_engine)
            try:
                assert locked.wait(5)
                with catalog_engine.begin() as writer:
                    writer.execute(text("SET LOCAL statement_timeout='1s'"))
                    writer.execute(text("""
                        INSERT INTO market.fact_versions VALUES ('during-read', '2026-09-01', 'payload')
                    """))
            finally:
                release.set()
            assert future.result(timeout=10).snapshot.inventory_complete
    finally:
        event.remove(catalog_engine, "after_cursor_execute", pause_after_child_lock)


def test_index_budget_refuses_an_oversized_group(catalog_engine):
    with catalog_engine.begin() as conn:
        for number in range(64):
            conn.execute(text(f"CREATE INDEX extra_{number} ON market.fact_versions_20260901(id)"))
    with pytest.raises(RuntimeError, match="index_budget_exceeded"):
        read_header_catalog(catalog_engine)


def test_requested_destination_catalog_records_oid_name_privilege_and_catalog_version(catalog_engine):
    report = read_header_catalog(catalog_engine, destination_tablespace_oids=(1663,))
    destination, = report.destination_tablespaces
    assert (destination.oid, destination.name, destination.location, destination.can_create) == (
        1663, "pg_default", "", True)
    assert report.catalog_version > 0
    assert not report.filesystem_bindings_verified


def test_missing_requested_destination_refuses_complete_catalog(catalog_engine):
    with pytest.raises(RuntimeError, match="tablespace_changed"):
        read_header_catalog(catalog_engine, destination_tablespace_oids=(4294967295,))
