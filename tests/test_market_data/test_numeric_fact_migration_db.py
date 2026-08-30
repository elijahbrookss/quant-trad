from __future__ import annotations

from pathlib import Path
import subprocess
from typing import Any

import pytest
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.engine import make_url
from sqlalchemy.exc import DBAPIError

from portal.backend.db import Database
from tests.test_market_data.migration_test_support import (
    fresh_migration_database,
    prepare_numeric_pre_migration_schema,
)


pytestmark = pytest.mark.db

_REPO_ROOT = Path(__file__).resolve().parents[2]
_MIGRATION = _REPO_ROOT / "scripts/db/manual_migration_numeric_fact_store_v1.sql"
_NUMERIC_INDEXES = frozenset(
    {
        "ix_market_numeric_fact_series_time_revision",
        "ix_market_numeric_fact_series_commit",
        "ix_market_numeric_fact_series_known",
        "ix_market_numeric_fact_event_group",
    }
)
_NUMERIC_CONSTRAINTS = frozenset(
    {
        "ck_market_numeric_fact_revision_positive",
        "ck_market_numeric_fact_type",
        "ck_market_numeric_fact_contract",
        "ck_market_numeric_fact_raw_value",
        "ck_market_numeric_fact_unit",
        "ck_market_numeric_fact_dimensions_object",
        "ck_market_numeric_fact_state",
        "ck_market_numeric_fact_known_after_effective",
        "ck_market_numeric_fact_known_after_publication",
        "ck_market_numeric_fact_acceptance_after_receipt",
        "ck_market_numeric_fact_source_material_hash",
        "ck_market_numeric_fact_row_hash",
    }
)
_COVERAGE_CONSTRAINTS = frozenset(
    {
        "ck_market_fact_acquisition_coverage_range",
        "ck_market_fact_acquisition_coverage_status",
        "ck_market_fact_acquisition_confirmation_depth",
        "ck_market_fact_acquisition_evidence_object",
    }
)


def _run_migration(dsn: str) -> None:
    psql_url = make_url(dsn).set(drivername="postgresql")
    completed = subprocess.run(
        [
            "psql",
            psql_url.render_as_string(hide_password=False),
            "-v",
            "ON_ERROR_STOP=1",
            "-f",
            str(_MIGRATION),
        ],
        cwd=_REPO_ROOT,
        check=False,
        capture_output=True,
        text=True,
    )
    assert completed.returncode == 0, (
        "numeric fact migration failed\n"
        f"stdout:\n{completed.stdout}\n"
        f"stderr:\n{completed.stderr}"
    )


def _assert_pre_migration_schema(dsn: str) -> None:
    engine = create_engine(dsn, future=True)
    try:
        with engine.connect() as conn:
            inspector = inspect(conn)
            market_tables = set(inspector.get_table_names(schema="market"))
            public_tables = set(inspector.get_table_names(schema="public"))
            series_columns = {
                str(column["name"])
                for column in inspector.get_columns("series", schema="market")
            }
            assert "portal_instruments" in public_tables
            assert {"sources", "series", "ingestion_runs"} <= market_tables
            assert {
                "numeric_fact_versions",
                "fact_acquisition_coverage",
                "fact_schemas",
                "fact_versions",
            }.isdisjoint(market_tables)
            assert "dimensions" not in series_columns
            assert conn.execute(
                text("SELECT to_regclass('market.fact_commit_seq')")
            ).scalar_one() is not None
            assert conn.execute(
                text(
                    "SELECT EXISTS ("
                    "SELECT 1 FROM pg_extension WHERE extname='timescaledb'"
                    ")"
                )
            ).scalar_one() is True
    finally:
        engine.dispose()


def _schema_snapshot(dsn: str) -> dict[str, Any]:
    engine = create_engine(dsn, future=True)
    try:
        with engine.connect() as conn:
            inspector = inspect(conn)
            numeric_type = conn.execute(
                text(
                    """
                    SELECT format_type(attribute.atttypid, attribute.atttypmod)
                    FROM pg_attribute AS attribute
                    WHERE attribute.attrelid = 'market.numeric_fact_versions'::regclass
                      AND attribute.attname = 'numeric_value'
                      AND NOT attribute.attisdropped
                    """
                )
            ).scalar_one()
            commit_default = conn.execute(
                text(
                    """
                    SELECT pg_get_expr(default_value.adbin, default_value.adrelid)
                    FROM pg_attribute AS attribute
                    JOIN pg_attrdef AS default_value
                      ON default_value.adrelid = attribute.attrelid
                     AND default_value.adnum = attribute.attnum
                    WHERE attribute.attrelid = 'market.numeric_fact_versions'::regclass
                      AND attribute.attname = 'market_commit_seq'
                    """
                )
            ).scalar_one()
            triggers = tuple(
                sorted(
                    str(row[0])
                    for row in conn.execute(
                        text(
                            """
                            SELECT trigger.tgname
                            FROM pg_trigger AS trigger
                            WHERE trigger.tgrelid IN (
                                'market.numeric_fact_versions'::regclass,
                                'market.fact_acquisition_coverage'::regclass
                            )
                              AND NOT trigger.tgisinternal
                            """
                        )
                    ).fetchall()
                )
            )
            return {
                "tables": tuple(
                    sorted(inspector.get_table_names(schema="market"))
                ),
                "series_columns": tuple(
                    sorted(
                        str(column["name"])
                        for column in inspector.get_columns(
                            "series", schema="market"
                        )
                    )
                ),
                "numeric_type": str(numeric_type),
                "commit_default": str(commit_default),
                "primary_key": tuple(
                    inspector.get_pk_constraint(
                        "numeric_fact_versions", schema="market"
                    ).get("constrained_columns")
                    or ()
                ),
                "numeric_indexes": tuple(
                    sorted(
                        str(index.get("name") or "")
                        for index in inspector.get_indexes(
                            "numeric_fact_versions", schema="market"
                        )
                    )
                ),
                "numeric_constraints": tuple(
                    sorted(
                        str(constraint.get("name") or "")
                        for constraint in inspector.get_check_constraints(
                            "numeric_fact_versions", schema="market"
                        )
                    )
                ),
                "coverage_indexes": tuple(
                    sorted(
                        str(index.get("name") or "")
                        for index in inspector.get_indexes(
                            "fact_acquisition_coverage", schema="market"
                        )
                    )
                ),
                "coverage_constraints": tuple(
                    sorted(
                        str(constraint.get("name") or "")
                        for constraint in inspector.get_check_constraints(
                            "fact_acquisition_coverage", schema="market"
                        )
                    )
                ),
                "triggers": triggers,
            }
    finally:
        engine.dispose()


def _insert_immutable_fixtures(conn) -> None:
    conn.execute(
        text(
            """
            INSERT INTO public.portal_instruments (
                id, symbol, can_short, short_requires_borrow, has_funding,
                metadata, created_at, updated_at
            )
            VALUES (
                'numeric-fact-test-instrument', 'NFT-USD', false, false,
                false, '{}'::json, now(), now()
            )
            """
        )
    )
    conn.execute(
        text(
            """
            INSERT INTO market.sources (
                id, identity_key, provider, venue, source_kind, adapter_version
            )
            OVERRIDING SYSTEM VALUE
            VALUES (92001, :identity_key, 'TEST', 'ISOLATED', 'fixture', 'fixture.v1')
            """
        ),
        {"identity_key": "a" * 64},
    )
    conn.execute(
        text(
            """
            INSERT INTO market.series (
                id, identity_key, instrument_id, fact_type,
                timeframe_seconds, contract_version, dimensions
            )
            OVERRIDING SYSTEM VALUE
            VALUES (
                92001, :identity_key, 'numeric-fact-test-instrument',
                'market.reference_price', NULL,
                'market.reference_price.v1', '{}'::jsonb
            )
            """
        ),
        {"identity_key": "b" * 64},
    )
    conn.execute(
        text(
            """
            INSERT INTO market.ingestion_runs (
                id, source_id, status, request, requested_count,
                inserted_count, corrected_count, noop_count
            )
            VALUES (
                'numeric-migration-run', 92001, 'completed', '{}'::jsonb,
                1, 1, 0, 0
            )
            """
        )
    )
    conn.execute(
        text(
            """
            INSERT INTO market.numeric_fact_versions (
                series_id, source_event_key, revision, ingestion_run_id,
                fact_type, contract_version, numeric_value, raw_value, unit,
                dimensions, effective_at, effective_at_method,
                source_published_at, received_at, accepted_at, known_at,
                known_at_method, state, source_event_material_hash,
                provenance, row_hash
            )
            VALUES (
                92001, 'event:1', 1, 'numeric-migration-run',
                'market.reference_price', 'market.reference_price.v1',
                1.25, '1.25', 'USD', '{}'::jsonb,
                '2026-08-09T12:00:00Z', 'provider_effective_time',
                '2026-08-09T12:00:00Z', '2026-08-09T12:00:01Z',
                '2026-08-09T12:00:02Z', '2026-08-09T12:00:02Z',
                'platform_acceptance', 'active', :material_hash,
                '{}'::jsonb, :row_hash
            )
            """
        ),
        {"material_hash": "c" * 64, "row_hash": "d" * 64},
    )
    conn.execute(
        text(
            """
            INSERT INTO market.fact_acquisition_coverage (
                identity_key, series_id, source_id, binding_id, manifest_hash,
                interface_version, confirmation_depth, range_start, range_end,
                source_position_start, source_position_end, status,
                ingestion_run_id, evidence
            )
            VALUES (
                :identity_key, 92001, 92001, 'fixture-binding', :manifest_hash,
                'fixture.v1', 0, '2026-08-09T12:00:00Z',
                '2026-08-09T13:00:00Z', '1', '2', 'complete',
                'numeric-migration-run', '{}'::jsonb
            )
            """
        ),
        {"identity_key": "e" * 64, "manifest_hash": "f" * 64},
    )


def test_numeric_fact_store_migration_is_explicit_idempotent_and_validated() -> None:
    with fresh_migration_database("numeric") as dsn:
        prepare_numeric_pre_migration_schema(dsn)
        _assert_pre_migration_schema(dsn)

        _run_migration(dsn)
        first = _schema_snapshot(dsn)
        _run_migration(dsn)
        second = _schema_snapshot(dsn)

        assert second == first
        assert "numeric_fact_versions" in second["tables"]
        assert "fact_acquisition_coverage" in second["tables"]
        assert "dimensions" in second["series_columns"]
        assert second["numeric_type"] == "numeric"
        assert "market.fact_commit_seq" in second["commit_default"]
        assert second["primary_key"] == (
            "series_id",
            "source_event_key",
            "revision",
        )
        assert _NUMERIC_INDEXES <= set(second["numeric_indexes"])
        assert _NUMERIC_CONSTRAINTS <= set(second["numeric_constraints"])
        assert "ix_market_fact_acquisition_coverage_lookup" in second[
            "coverage_indexes"
        ]
        assert _COVERAGE_CONSTRAINTS <= set(second["coverage_constraints"])
        assert second["triggers"] == (
            "trg_reject_mutation_fact_acquisition_coverage",
            "trg_reject_mutation_numeric_fact_versions",
        )

        engine = create_engine(dsn, future=True)
        try:
            with engine.begin() as conn:
                _insert_immutable_fixtures(conn)
                mutations = (
                    "UPDATE market.numeric_fact_versions "
                    "SET state='invalidated' WHERE series_id=92001",
                    "DELETE FROM market.numeric_fact_versions "
                    "WHERE series_id=92001",
                    "UPDATE market.fact_acquisition_coverage "
                    "SET status='partial' WHERE series_id=92001",
                    "DELETE FROM market.fact_acquisition_coverage "
                    "WHERE series_id=92001",
                )
                for mutation in mutations:
                    with pytest.raises(
                        DBAPIError, match="immutable market-data relation"
                    ):
                        with conn.begin_nested():
                            conn.execute(text(mutation))
                assert conn.execute(
                    text(
                        "SELECT count(*) FROM market.numeric_fact_versions "
                        "WHERE series_id=92001"
                    )
                ).scalar_one() == 1
                assert conn.execute(
                    text(
                        "SELECT count(*) FROM market.fact_acquisition_coverage "
                        "WHERE series_id=92001"
                    )
                ).scalar_one() == 1
        finally:
            engine.dispose()

        compatibility_database = Database(dsn)
        try:
            assert not compatibility_database.ensure_schema()
            assert "Legacy market-data tables remain active" in str(
                compatibility_database.last_error
            )
        finally:
            compatibility_database.reset_connection_state()
