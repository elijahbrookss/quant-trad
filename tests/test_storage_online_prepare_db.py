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
