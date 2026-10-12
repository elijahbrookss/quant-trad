"""Exercise operator inventory against isolated synthetic catalog state."""
import json

import pytest
from sqlalchemy import create_engine, event, text
from sqlalchemy.exc import DBAPIError

from scripts.db.inspect_fact_header_cutover_v2 import inspect_preflight
from tests.test_market_data.migration_test_support import fresh_migration_database

pytestmark = pytest.mark.db


@pytest.fixture
def preflight_source():
    with fresh_migration_database("header_preflight") as dsn:
        engine = create_engine(dsn, future=True)
        try:
            with engine.begin() as conn:
                conn.execute(text("CREATE SCHEMA market"))
                conn.execute(text("""
                    CREATE TABLE market.fact_versions (
                        id text PRIMARY KEY, storage_day date NOT NULL,
                        series_id bigint NOT NULL, observation_key text NOT NULL,
                        revision integer NOT NULL, market_commit_seq bigint NOT NULL,
                        observation_time timestamptz NOT NULL, known_at timestamptz NOT NULL,
                        row_hash text NOT NULL, UNIQUE(series_id,observation_key,revision)
                    )
                """))
                conn.execute(text("""
                    CREATE INDEX ix_market_fact_storage_page
                    ON market.fact_versions(storage_day,market_commit_seq,id)
                """))
                conn.execute(text("""
                    INSERT INTO market.fact_versions VALUES
                    ('one','2026-09-01',1,'observation-one',1,1,now(),now(),'hash-one'),
                    ('two','2026-09-02',1,'observation-two',1,2,now(),now(),'hash-two')
                """))
                conn.execute(text("""
                    CREATE TABLE market.fact_hot_payloads (
                        id text REFERENCES market.fact_versions(id) ON DELETE RESTRICT
                    )
                """))
                conn.execute(text("CREATE VIEW market.fact_rows AS SELECT * FROM market.fact_versions"))
                conn.execute(text("""
                    CREATE TABLE market.fact_storage_state(layout_version text PRIMARY KEY,state text);
                    INSERT INTO market.fact_storage_state VALUES('market.fact_storage_tiers.v1','ready')
                """))
            yield engine
        finally:
            engine.dispose()


def test_preflight_reports_dependencies_and_estimates_without_claiming_readiness(preflight_source):
    result = inspect_preflight(preflight_source)
    repeated = inspect_preflight(preflight_source)
    assert result["status"] == "inspection_only"
    assert result["migration_ready"] is False
    assert result["changes_performed"] is False
    assert result["context"]["transaction_read_only"] == "on"
    assert result["source"]["kind"] == "r"
    assert result["source"]["total_bytes"] > 0
    assert result["catalog_fingerprint"] == repeated["catalog_fingerprint"]
    # A shape-compatible synthetic table lacks application guards. Inventory
    # must report that gap and may never treat this as an admitted migration.
    assert result["source_findings"] == ["source_required_triggers_missing"]
    assert "source_schema_and_guard_contract_must_be_verified" in result["required_gates"]
    assert [(row["relation"], row["expected_consumer"]) for row in result["incoming_foreign_keys"]] == [
        ("market.fact_hot_payloads", True),
    ]
    assert result["dependent_views"] == [{"relation": "market.fact_rows", "kind": "v"}]
    json.dumps(result)
    with preflight_source.connect() as conn:
        assert conn.execute(text("SELECT count(*) FROM market.fact_versions")).scalar_one() == 2
        assert conn.execute(text("SELECT to_regclass('market.fact_identities')")).scalar_one() is None


def test_preflight_identifies_unknown_dependencies_and_missing_copy_index(preflight_source):
    before = inspect_preflight(preflight_source)
    with preflight_source.begin() as conn:
        conn.execute(text("CREATE SCHEMA unrelated"))
        conn.execute(text("CREATE TABLE unrelated.extra_refs(id text REFERENCES market.fact_versions(id))"))
        conn.execute(text("CREATE VIEW unrelated.extra_view AS SELECT id FROM market.fact_versions"))
        conn.execute(text("DROP INDEX market.ix_market_fact_storage_page"))
    after = inspect_preflight(preflight_source)
    assert {
        "unhandled_incoming_foreign_key", "unhandled_dependent_view",
        "bounded_source_copy_index_missing_or_incompatible",
    } <= set(after["source_findings"])
    assert after["catalog_fingerprint"] != before["catalog_fingerprint"]
    assert after["migration_ready"] is False


def test_preflight_transaction_refuses_an_injected_write(preflight_source):
    def inject(conn, cursor, statement, parameters, context, executemany):
        if "clock_timestamp()::text AS observed_at" in statement:
            conn.exec_driver_sql("CREATE TABLE market.must_not_be_created(id integer)")
    event.listen(preflight_source, "before_cursor_execute", inject)
    try:
        with pytest.raises(DBAPIError, match="read-only transaction"):
            inspect_preflight(preflight_source)
    finally:
        event.remove(preflight_source, "before_cursor_execute", inject)
    with preflight_source.connect() as conn:
        assert conn.execute(text("SELECT to_regclass('market.must_not_be_created')")).scalar_one() is None
