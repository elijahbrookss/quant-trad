from __future__ import annotations

import os
from pathlib import Path
import subprocess
from typing import Any

import pytest
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.engine import make_url

from portal.backend.db import db
from portal.backend.db.models import Base


pytestmark = pytest.mark.db

_REPO_ROOT = Path(__file__).resolve().parents[2]
_MIGRATION = _REPO_ROOT / "scripts/db/manual_migration_numeric_fact_store_v1.sql"
_EXPLICIT_TABLE_KEYS = frozenset(
    {
        ("market", "numeric_fact_versions"),
        ("market", "fact_acquisition_coverage"),
    }
)
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
    }
)


def _isolated_dsn() -> str:
    if os.getenv("QT_DB_TEST_ISOLATED", "").strip() != "1":
        pytest.fail("numeric fact migration test requires QT_DB_TEST_ISOLATED=1")
    dsn = os.getenv("PG_DSN", "").strip()
    if not dsn:
        pytest.fail("numeric fact migration test requires the disposable PG_DSN")
    return dsn


def _prepare_pre_migration_schema(dsn: str) -> None:
    """Build the existing ORM baseline while omitting migration-owned objects."""

    db.reset_connection_state()
    engine = create_engine(dsn, future=True)
    try:
        with engine.begin() as conn:
            schemas = sorted(
                {
                    str(table.schema)
                    for table in Base.metadata.sorted_tables
                    if str(table.schema or "").strip()
                }
            )
            for schema in schemas:
                conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{schema}"'))
            conn.execute(text("CREATE SEQUENCE IF NOT EXISTS market.fact_commit_seq"))

            # This is intentionally test-only baseline construction. Production
            # owns the two excluded relations through the explicit SQL migration.
            for table in Base.metadata.sorted_tables:
                key = (str(table.schema or "").strip() or None, str(table.name))
                if key in _EXPLICIT_TABLE_KEYS:
                    continue
                table.create(bind=conn, checkfirst=True)

            conn.execute(
                text("DROP TABLE IF EXISTS market.fact_acquisition_coverage")
            )
            conn.execute(text("DROP TABLE IF EXISTS market.numeric_fact_versions"))
            conn.execute(
                text(
                    "ALTER TABLE market.series "
                    "DROP CONSTRAINT IF EXISTS ck_market_series_dimensions_object"
                )
            )
            conn.execute(
                text("ALTER TABLE market.series DROP COLUMN IF EXISTS dimensions")
            )
    finally:
        engine.dispose()


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
                "triggers": triggers,
            }
    finally:
        engine.dispose()


def test_numeric_fact_store_migration_is_explicit_idempotent_and_validated() -> None:
    dsn = _isolated_dsn()
    _prepare_pre_migration_schema(dsn)

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
    assert second["triggers"] == (
        "trg_reject_mutation_fact_acquisition_coverage",
        "trg_reject_mutation_numeric_fact_versions",
    )

    db.reset_connection_state()
    db.dsn = dsn
    assert not db.ensure_schema()
    assert "Legacy market-data tables remain active" in str(db.last_error)

    # The numeric store is immutable migration history, not a supported
    # runtime compatibility path. Remove it before returning the shared
    # disposable database to the final canonical state.
    db.reset_connection_state()
    engine = create_engine(dsn, future=True)
    try:
        with engine.begin() as conn:
            conn.execute(text("DROP TABLE market.numeric_fact_versions"))
    finally:
        engine.dispose()

    db.dsn = dsn
    assert db.ensure_schema(), repr(db.last_error)
    db.reset_connection_state()
