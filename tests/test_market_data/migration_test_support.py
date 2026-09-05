from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
import os
import re
from time import monotonic, sleep
from uuid import uuid4

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.engine import make_url

from portal.backend.db import (
    InstrumentRecord,
    MarketDataIngestionRunRecord,
    MarketDataSeriesRecord,
    MarketDataSourceRecord,
    MarketDatasetRecord,
    MarketDatasetSeriesRecord,
)


PRE_CUTOVER_TABLES = (
    "candle_versions",
    "open_interest_versions",
    "funding_rate_versions",
    "market_trade_versions",
    "trade_flow_aggregate_versions",
    "l2_snapshot_versions",
    "l2_snapshot_levels",
    "l2_mutation_batches",
    "l2_mutations",
    "bbo_feature_versions",
    "depth_feature_versions",
    "trade_flow_feature_versions",
    "futures_spot_relationship_versions",
    "derivative_state_versions",
    "market_response_feature_versions",
    "normalized_feature_versions",
)

_DATABASE_NAME = re.compile(r"^[a-z][a-z0-9_]{0,62}$")


def _isolated_parent_dsn() -> str:
    if os.getenv("QT_DB_TEST_ISOLATED", "").strip() != "1":
        pytest.fail("migration tests require QT_DB_TEST_ISOLATED=1")
    if os.getenv("RUN_DB_TESTS", "").strip() != "1":
        pytest.fail("migration tests require RUN_DB_TESTS=1")
    dsn = os.getenv("PG_DSN", "").strip()
    if not dsn:
        pytest.fail("migration tests require the disposable PG_DSN")
    url = make_url(dsn)
    if url.get_backend_name() != "postgresql" or not url.database:
        pytest.fail("migration tests require a PostgreSQL PG_DSN with a database")
    return dsn


@contextmanager
def fresh_migration_database(label: str) -> Iterator[str]:
    """Yield an empty, uniquely named database and always remove it afterward."""

    safe_label = re.sub(r"[^a-z0-9]+", "_", label.lower()).strip("_")[:16]
    if not safe_label:
        raise ValueError("migration database label must contain a letter or digit")
    database_name = f"qt_migration_{safe_label}_{uuid4().hex[:16]}"
    if not _DATABASE_NAME.fullmatch(database_name):
        raise AssertionError(f"unsafe generated database name: {database_name!r}")

    parent_url = make_url(_isolated_parent_dsn())
    admin_url = parent_url.set(database="postgres")
    target_url = parent_url.set(database=database_name)
    quoted_name = f'"{database_name}"'
    admin_engine = create_engine(
        admin_url,
        future=True,
        isolation_level="AUTOCOMMIT",
    )
    target_engine = None
    database_created = False
    try:
        with admin_engine.connect() as conn:
            conn.exec_driver_sql(
                f"CREATE DATABASE {quoted_name} TEMPLATE template0"
            )
        database_created = True

        target_engine = create_engine(target_url, future=True)
        with target_engine.begin() as conn:
            conn.execute(text("CREATE EXTENSION IF NOT EXISTS timescaledb"))
            conn.execute(text("CREATE EXTENSION IF NOT EXISTS pgcrypto"))
        target_engine.dispose()
        target_engine = None

        yield target_url.render_as_string(hide_password=False)
    finally:
        if target_engine is not None:
            target_engine.dispose()
        try:
            if database_created:
                with admin_engine.connect() as conn:
                    conn.execute(
                        text(
                            "SELECT pg_terminate_backend(pid) "
                            "FROM pg_stat_activity "
                            "WHERE datname = :database_name "
                            "AND pid <> pg_backend_pid()"
                        ),
                        {"database_name": database_name},
                    ).all()
                    deadline = monotonic() + 5
                    while True:
                        remaining_pids = conn.execute(
                            text(
                                "SELECT array_agg(pid ORDER BY pid) "
                                "FROM pg_stat_activity "
                                "WHERE datname = :database_name "
                                "AND pid <> pg_backend_pid()"
                            ),
                            {"database_name": database_name},
                        ).scalar_one()
                        if not remaining_pids:
                            break
                        if monotonic() >= deadline:
                            raise RuntimeError(
                                "migration_database_cleanup_timeout: "
                                f"database={database_name} pids={remaining_pids}"
                            )
                        sleep(0.05)
                    conn.exec_driver_sql(
                        f"DROP DATABASE IF EXISTS {quoted_name} WITH (FORCE)"
                    )
        finally:
            admin_engine.dispose()


def _prepare_shared_schema(dsn: str, *, include_datasets: bool) -> None:
    engine = create_engine(dsn, future=True)
    try:
        with engine.begin() as conn:
            conn.execute(text("CREATE SCHEMA market"))
            conn.execute(text("CREATE SEQUENCE market.fact_commit_seq"))

            tables = [
                InstrumentRecord.__table__,
                MarketDataSourceRecord.__table__,
                MarketDataSeriesRecord.__table__,
                MarketDataIngestionRunRecord.__table__,
            ]
            if include_datasets:
                tables.extend(
                    [
                        MarketDatasetRecord.__table__,
                        MarketDatasetSeriesRecord.__table__,
                    ]
                )
            for table in tables:
                table.create(bind=conn, checkfirst=False)

            # Rewind only the additive fields owned by the migrations under test.
            conn.execute(
                text(
                    "ALTER TABLE market.series "
                    "DROP CONSTRAINT ck_market_series_dimensions_object"
                )
            )
            conn.execute(text("ALTER TABLE market.series DROP COLUMN dimensions"))

            if include_datasets:
                conn.execute(
                    text(
                        "ALTER TABLE market.dataset_series DROP CONSTRAINT "
                        "ck_market_dataset_series_payload_schemas_array"
                    )
                )
                conn.execute(
                    text(
                        "ALTER TABLE market.dataset_series "
                        "DROP COLUMN payload_schemas"
                    )
                )
    finally:
        engine.dispose()


def prepare_numeric_pre_migration_schema(dsn: str) -> None:
    """Create the explicit deployed state immediately before numeric Facts."""

    _prepare_shared_schema(dsn, include_datasets=False)


def prepare_canonical_pre_migration_schema(dsn: str) -> None:
    """Create the explicit deployed state before numeric and canonical Facts."""

    _prepare_shared_schema(dsn, include_datasets=True)
    engine = create_engine(dsn, future=True)
    try:
        with engine.begin() as conn:
            # Runtime metadata no longer owns these retired relations. Empty
            # placeholders express the structural boundary exercised here; the
            # separate data-migration program owns historical row equivalence.
            for table_name in PRE_CUTOVER_TABLES:
                if not re.fullmatch(r"[a-z][a-z0-9_]*", table_name):
                    raise AssertionError(
                        f"unsafe pre-cutover table name: {table_name!r}"
                    )
                conn.exec_driver_sql(
                    f'CREATE TABLE market."{table_name}" '
                    "(id varchar PRIMARY KEY)"
                )
    finally:
        engine.dispose()
