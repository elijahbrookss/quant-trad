"""Actual PostgreSQL distinction between retained rollback data and online capture."""
import json

import pytest
from sqlalchemy import create_engine, text

from scripts.automation import storage_online_prepare as online
from tests.test_market_data.migration_test_support import fresh_migration_database

pytestmark = pytest.mark.db


@pytest.fixture
def source_catalog(monkeypatch):
    with fresh_migration_database("initial_catalog", install_extensions=False) as dsn:
        engine = create_engine(dsn)
        monkeypatch.setattr(online.host_boundary, "source_storage", lambda _: None)
        def query(_, statement):
            with engine.connect() as conn:
                return json.dumps(conn.scalar(text(statement)))
        monkeypatch.setattr(online.host_boundary, "database_query", query)
        try:
            yield engine
        finally:
            engine.dispose()


def retained(engine):
    with engine.begin() as conn:
        conn.exec_driver_sql("CREATE SCHEMA qt_fact_storage_cutover_v1")
        conn.exec_driver_sql("CREATE TABLE qt_fact_storage_cutover_v1.fact_versions "
                            "(id text PRIMARY KEY, body text NOT NULL)")
        conn.exec_driver_sql("INSERT INTO qt_fact_storage_cutover_v1.fact_versions "
                            "VALUES ('retained', 'preserve me')")


def test_retained_legacy_source_is_preserved_and_not_an_online_capture(source_catalog):
    online._before_capture("fixture")
    retained(source_catalog)
    online._before_capture("fixture")
    with source_catalog.connect() as conn:
        assert conn.scalar(text("SELECT body FROM qt_fact_storage_cutover_v1.fact_versions")) == "preserve me"


@pytest.mark.parametrize("extra", [
    "CREATE SCHEMA qt_fact_header_cutover_v2",
    "CREATE SCHEMA qt_unknown_cutover_v9",
    "CREATE TABLE qt_fact_storage_cutover_v1.capture (id integer)",
    "CREATE FUNCTION qt_fact_storage_cutover_v1.capture() RETURNS integer "
    "LANGUAGE sql AS 'SELECT 1'",
])
def test_capture_or_unexpected_legacy_objects_still_refuse(source_catalog, extra):
    retained(source_catalog)
    with source_catalog.begin() as conn:
        conn.exec_driver_sql(extra)
    with pytest.raises(RuntimeError, match="uncaptured_key_free_source"):
        online._before_capture("fixture")


def test_empty_legacy_namespace_is_not_a_retained_source(source_catalog):
    with source_catalog.begin() as conn:
        conn.exec_driver_sql("CREATE SCHEMA qt_fact_storage_cutover_v1")
    with pytest.raises(RuntimeError, match="uncaptured_key_free_source"):
        online._before_capture("fixture")


def test_storage_control_preflight_requires_explicit_existing_tables(source_catalog):
    from portal.backend.db import storage_target_models as models
    from scripts.automation.storage_online_worker import inspect_storage_schema
    classes=(models.StorageTargetRecord, models.StoragePolicyRecord, models.StoragePlanRecord,
             models.StorageObjectLocationRecord, models.StorageHeaderTablespaceRecord,
             models.StorageHeaderBatchRecord, models.StorageHeaderMoveRecord)
    tables=[model.__table__ for model in classes]
    with source_catalog.begin() as conn:
        conn.exec_driver_sql("SET TRANSACTION READ ONLY")
        with pytest.raises(RuntimeError,match="storage_schema_missing: tables=portal_storage_targets"):
            inspect_storage_schema(conn)
        assert conn.scalar(text("SELECT to_regclass('public.portal_storage_targets')")) is None
    # Only explicit fixture setup creates the canonical schema, never inspection.
    models.Base.metadata.create_all(source_catalog,tables=tables)
    with source_catalog.begin() as conn:
        conn.exec_driver_sql("SET TRANSACTION READ ONLY")
        inspect_storage_schema(conn)
        for table in tables:
            assert conn.scalar(text('SELECT count(*) FROM public.'+table.name))==0
    with source_catalog.begin() as conn:
        conn.exec_driver_sql("ALTER TABLE public.portal_storage_policy RENAME TO retained_policy")
    with source_catalog.begin() as conn:
        conn.exec_driver_sql("SET TRANSACTION READ ONLY")
        with pytest.raises(RuntimeError,match="storage_schema_missing: tables=portal_storage_policy$"):
            inspect_storage_schema(conn)
