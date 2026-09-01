from __future__ import annotations

import os
import subprocess
from pathlib import Path
from typing import Any

import pytest
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.engine import make_url
from sqlalchemy.exc import DBAPIError

from market_data.fact_registry import supported_fact_payload_schemas
from portal.backend.db.session import Database
from tests.test_market_data.migration_test_support import (
    PRE_CUTOVER_TABLES,
    fresh_migration_database,
    prepare_canonical_pre_migration_schema,
)


pytestmark = pytest.mark.db

_REPO_ROOT = Path(__file__).resolve().parents[2]
_MIGRATION = _REPO_ROOT / "scripts/db/manual_migration_canonical_fact_store_v1.sql"
_LOOKUP_INDEX_MIGRATION = (
    _REPO_ROOT / "scripts/db/manual_migration_canonical_fact_lookup_indexes_v1.sql"
)
_HARD_CUTOVER = (
    _REPO_ROOT / "scripts/db/manual_migration_canonical_fact_hard_cutover_v1.sql"
)
_NUMERIC_MIGRATION = _REPO_ROOT / "scripts/db/manual_migration_numeric_fact_store_v1.sql"
_REQUIRED_INDEXES = frozenset(
    {
        "ix_market_fact_series_time_revision",
        "ix_market_fact_series_commit",
        "ix_market_fact_series_material",
        "ix_market_fact_series_source",
        "ix_market_fact_series_known",
        "ix_market_fact_schema_time",
        "ix_market_fact_source_time",
        "ix_market_fact_external_group",
        "ix_market_fact_payload_gin",
        "ix_market_fact_provenance_gin",
        "ix_market_fact_exact_value",
        "ix_market_fact_exact_rate",
        "ix_market_fact_funding_time",
    }
)


def _run_sql_migration(dsn: str, migration: Path, label: str) -> None:
    psql_url = make_url(dsn).set(drivername="postgresql")
    completed = subprocess.run(
        [
            "psql",
            psql_url.render_as_string(hide_password=False),
            "-v",
            "ON_ERROR_STOP=1",
            "-f",
            str(migration),
        ],
        cwd=_REPO_ROOT,
        check=False,
        capture_output=True,
        text=True,
    )
    assert completed.returncode == 0, (
        f"{label} migration failed\n"
        f"stdout:\n{completed.stdout}\n"
        f"stderr:\n{completed.stderr}"
    )


def _run_sql_command(
    dsn: str,
    sql: str,
    *,
    expected_success: bool,
    statement_timeout_ms: int | None = None,
) -> subprocess.CompletedProcess[str]:
    psql_url = make_url(dsn).set(drivername="postgresql")
    environment = dict(os.environ)
    if statement_timeout_ms is not None:
        environment["PGOPTIONS"] = f"-c statement_timeout={statement_timeout_ms}ms"
    completed = subprocess.run(
        [
            "psql",
            psql_url.render_as_string(hide_password=False),
            "-v",
            "ON_ERROR_STOP=1",
            "-c",
            sql,
        ],
        cwd=_REPO_ROOT,
        check=False,
        capture_output=True,
        text=True,
        env=environment,
    )
    assert (completed.returncode == 0) is expected_success, (
        f"unexpected psql result for {sql!r}\n"
        f"stdout:\n{completed.stdout}\n"
        f"stderr:\n{completed.stderr}"
    )
    return completed


def _run_migration(dsn: str) -> None:
    _run_sql_migration(dsn, _MIGRATION, "canonical Fact store")


def _run_hard_cutover(dsn: str) -> None:
    _run_sql_migration(dsn, _HARD_CUTOVER, "canonical Fact hard cutover")


def _run_lookup_index_migration(dsn: str) -> None:
    _run_sql_migration(
        dsn,
        _LOOKUP_INDEX_MIGRATION,
        "canonical Fact lookup indexes",
    )


def _lookup_index_snapshot(dsn: str) -> dict[str, tuple[bool, bool, str]]:
    engine = create_engine(dsn, future=True)
    try:
        with engine.connect() as conn:
            rows = conn.execute(
                text(
                    """
                    SELECT index_class.relname, index_state.indisvalid,
                           index_state.indisready,
                           pg_get_indexdef(index_state.indexrelid)
                    FROM pg_index AS index_state
                    JOIN pg_class AS index_class
                      ON index_class.oid = index_state.indexrelid
                    JOIN pg_class AS table_class
                      ON table_class.oid = index_state.indrelid
                    JOIN pg_namespace AS namespace
                      ON namespace.oid = table_class.relnamespace
                    WHERE namespace.nspname = 'market'
                      AND table_class.relname = 'fact_versions'
                      AND index_class.relname IN (
                          'ix_market_fact_series_material',
                          'ix_market_fact_series_source'
                      )
                    ORDER BY index_class.relname
                    """
                )
            ).all()
        return {
            str(name): (bool(valid), bool(ready), str(definition))
            for name, valid, ready, definition in rows
        }
    finally:
        engine.dispose()


def _lookup_index_oids(dsn: str) -> dict[str, int]:
    engine = create_engine(dsn, future=True)
    try:
        with engine.connect() as conn:
            return {
                str(name): int(oid)
                for name, oid in conn.execute(
                    text(
                        """
                        SELECT index_class.relname, index_class.oid
                        FROM pg_index AS index_state
                        JOIN pg_class AS index_class
                          ON index_class.oid = index_state.indexrelid
                        JOIN pg_class AS table_class
                          ON table_class.oid = index_state.indrelid
                        JOIN pg_namespace AS namespace
                          ON namespace.oid = table_class.relnamespace
                        WHERE namespace.nspname = 'market'
                          AND table_class.relname = 'fact_versions'
                          AND index_class.relname IN (
                              'ix_market_fact_series_material',
                              'ix_market_fact_series_source'
                          )
                        ORDER BY index_class.relname
                        """
                    )
                ).all()
            }
    finally:
        engine.dispose()


def _pre_migration_state(dsn: str) -> dict[str, Any]:
    engine = create_engine(dsn, future=True)
    try:
        with engine.connect() as conn:
            inspector = inspect(conn)
            return {
                "market_tables": frozenset(
                    inspector.get_table_names(schema="market")
                ),
                "public_tables": frozenset(
                    inspector.get_table_names(schema="public")
                ),
                "series_columns": frozenset(
                    str(column["name"])
                    for column in inspector.get_columns("series", schema="market")
                ),
                "dataset_series_columns": frozenset(
                    str(column["name"])
                    for column in inspector.get_columns(
                        "dataset_series", schema="market"
                    )
                ),
                "commit_sequence": conn.execute(
                    text("SELECT to_regclass('market.fact_commit_seq')")
                ).scalar_one(),
                "timescaledb": conn.execute(
                    text(
                        "SELECT EXISTS ("
                        "SELECT 1 FROM pg_extension WHERE extname='timescaledb'"
                        ")"
                    )
                ).scalar_one(),
            }
    finally:
        engine.dispose()


def _assert_pre_numeric_schema(dsn: str) -> None:
    state = _pre_migration_state(dsn)
    assert "portal_instruments" in state["public_tables"]
    assert {
        "sources",
        "series",
        "ingestion_runs",
        "datasets",
        "dataset_series",
        *PRE_CUTOVER_TABLES,
    } <= state["market_tables"]
    assert {
        "numeric_fact_versions",
        "fact_acquisition_coverage",
        "fact_schemas",
        "fact_versions",
    }.isdisjoint(state["market_tables"])
    assert "dimensions" not in state["series_columns"]
    assert "payload_schemas" not in state["dataset_series_columns"]
    assert state["commit_sequence"] is not None
    assert state["timescaledb"] is True


def _assert_pre_canonical_schema(dsn: str) -> None:
    state = _pre_migration_state(dsn)
    assert {
        "sources",
        "series",
        "ingestion_runs",
        "dataset_series",
        "candle_versions",
        "open_interest_versions",
        "funding_rate_versions",
        "numeric_fact_versions",
        "fact_acquisition_coverage",
        *PRE_CUTOVER_TABLES,
    } <= state["market_tables"]
    assert {"fact_schemas", "fact_versions"}.isdisjoint(state["market_tables"])
    assert "dimensions" in state["series_columns"]
    assert "payload_schemas" not in state["dataset_series_columns"]
    assert state["commit_sequence"] is not None
    assert state["timescaledb"] is True


def _schema_snapshot(dsn: str) -> dict[str, Any]:
    engine = create_engine(dsn, future=True)
    try:
        with engine.connect() as conn:
            inspector = inspect(conn)
            registry = tuple(
                conn.execute(
                    text(
                        "SELECT schema_id, fact_type, contract_hash "
                        "FROM market.fact_schemas ORDER BY schema_id"
                    )
                ).tuples()
            )
            triggers = tuple(
                sorted(
                    str(row[0])
                    for row in conn.execute(
                        text(
                            """
                            SELECT trigger.tgname
                            FROM pg_trigger AS trigger
                            WHERE trigger.tgrelid IN (
                                'market.fact_schemas'::regclass,
                                'market.fact_versions'::regclass
                            )
                              AND NOT trigger.tgisinternal
                            """
                        )
                    )
                )
            )
            commit_default = conn.execute(
                text(
                    """
                    SELECT pg_get_expr(default_value.adbin, default_value.adrelid)
                    FROM pg_attribute AS attribute
                    JOIN pg_attrdef AS default_value
                      ON default_value.adrelid = attribute.attrelid
                     AND default_value.adnum = attribute.attnum
                    WHERE attribute.attrelid = 'market.fact_versions'::regclass
                      AND attribute.attname = 'market_commit_seq'
                    """
                )
            ).scalar_one()
            return {
                "tables": tuple(sorted(inspector.get_table_names(schema="market"))),
                "dataset_series_columns": tuple(
                    sorted(
                        str(column["name"])
                        for column in inspector.get_columns(
                            "dataset_series", schema="market"
                        )
                    )
                ),
                "registry": registry,
                "primary_key": tuple(
                    inspector.get_pk_constraint(
                        "fact_versions", schema="market"
                    ).get("constrained_columns")
                    or ()
                ),
                "indexes": tuple(
                    sorted(
                        str(index.get("name") or "")
                        for index in inspector.get_indexes(
                            "fact_versions", schema="market"
                        )
                    )
                ),
                "triggers": triggers,
                "commit_default": str(commit_default),
            }
    finally:
        engine.dispose()


def _insert_fixture_identity(conn) -> None:
    conn.execute(
        text(
            """
            INSERT INTO public.portal_instruments (
                id, symbol, can_short, short_requires_borrow, has_funding,
                metadata, created_at, updated_at
            )
            VALUES (
                'canonical-fact-test-instrument', 'CFT-PERP', false, false,
                true, '{}'::jsonb, now(), now()
            )
            ON CONFLICT (id) DO NOTHING
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
            VALUES (91001, :identity_key, 'TEST', 'ISOLATED', 'fixture', 'fixture.v2')
            ON CONFLICT (id) DO NOTHING
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
                91001, :identity_key, 'canonical-fact-test-instrument',
                'derivatives.funding_rate', NULL,
                'derivatives.funding_rate.v2', '{}'::jsonb
            )
            ON CONFLICT (id) DO NOTHING
            """
        ),
        {"identity_key": "b" * 64},
    )


def _fact_insert_sql(payload_expression: str) -> str:
    return f"""
        INSERT INTO market.fact_versions (
            id, series_id, observation_key, revision, source_id,
            fact_type, payload_schema_id, payload_contract_hash,
            observation_time, observation_time_method, accepted_at,
            known_at, known_at_method, transformation_id, state, payload,
            payload_hash, material_hash, provenance_schema_id, provenance,
            provenance_hash, quality_schema_id, quality, quality_hash, row_hash
        )
        VALUES (
            'mfv_fixture', 91001, 'schedule:1', 1, 91001,
            'derivatives.funding_rate', 'derivatives.funding_rate.v2',
            '075ee70123395504c5e8ab9ffdcfd4121ba89eafd90cd4e9e0ee22bc19558165',
            '2026-08-09T12:00:00Z', 'collector_schedule',
            '2026-08-09T12:00:02Z', '2026-08-09T12:00:02Z',
            'platform_acceptance', 'fixture_to_canonical.v2', 'active',
            {payload_expression},
            :hash, :hash, 'market.fact_provenance.v1', '{{}}'::jsonb,
            :hash, 'market.fact_quality.v1', '{{}}'::jsonb, :hash, :hash
        )
    """


def test_canonical_fact_store_migration_is_explicit_strict_and_idempotent() -> None:
    with fresh_migration_database("canonical") as dsn:
        prepare_canonical_pre_migration_schema(dsn)
        _assert_pre_numeric_schema(dsn)
        _run_sql_migration(dsn, _NUMERIC_MIGRATION, "numeric Fact baseline")
        _assert_pre_canonical_schema(dsn)

        _run_migration(dsn)
        first = _schema_snapshot(dsn)
        _run_migration(dsn)
        second = _schema_snapshot(dsn)

        assert second == first
        assert {"fact_schemas", "fact_versions"} <= set(second["tables"])
        assert second["primary_key"] == ("id",)
        assert "payload_schemas" in second["dataset_series_columns"]
        assert _REQUIRED_INDEXES <= set(second["indexes"])
        assert "market.fact_commit_seq" in second["commit_default"]
        assert second["triggers"] == (
            "trg_assert_fact_version_valid",
            "trg_reject_mutation_fact_schemas",
            "trg_reject_mutation_fact_versions",
        )
        expected_registry = tuple(
            (schema.schema_id, schema.fact_type, schema.contract_hash)
            for schema in supported_fact_payload_schemas()
        )
        assert second["registry"] == expected_registry

        correct_lookup_indexes = _lookup_index_snapshot(dsn)
        engine = create_engine(dsn, future=True)
        database = Database(dsn)
        try:
            with engine.begin() as conn:
                conn.execute(
                    text("DROP INDEX market.ix_market_fact_series_material")
                )
                conn.execute(
                    text(
                        "CREATE INDEX ix_market_fact_series_material "
                        "ON market.fact_versions (material_hash, series_id)"
                    )
                )
            with engine.connect() as conn:
                with pytest.raises(
                    RuntimeError,
                    match="invalid canonical Fact lookup index definitions",
                ):
                    database._assert_canonical_fact_migration(conn)
        finally:
            engine.dispose()
        _run_lookup_index_migration(dsn)
        assert _lookup_index_snapshot(dsn) == correct_lookup_indexes

        engine = create_engine(dsn, future=True)
        try:
            with engine.begin() as conn:
                for mutation in (
                    "UPDATE market.fact_schemas SET created_at=created_at "
                    "WHERE schema_id='asset.reserve_state.v1'",
                    "DELETE FROM market.fact_schemas "
                    "WHERE schema_id='asset.reserve_state.v1'",
                ):
                    with pytest.raises(
                        DBAPIError, match="immutable market-data relation"
                    ):
                        with conn.begin_nested():
                            conn.execute(text(mutation))

                _insert_fixture_identity(conn)
                l2_payload = """{
                    "event_type":"snapshot",
                    "product_definition_version_id":"coinbase.BTC-USD.v1",
                    "validity_interval_id":"validity-1",
                    "reconstruction_version":"l2-absolute.v1",
                    "before_state_hash":null,
                    "after_state_hash":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                    "event_material_hash":"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
                    "entry_count":1,
                    "unknown_zero_delete_count":0,
                    "entries":[{
                        "ordinal":0,
                        "side":"bid",
                        "price":"118000",
                        "quantity":"1.25",
                        "provider_size_unit":"base",
                        "provider_event_time":"2026-08-09T12:00:00.000000Z"
                    }]
                }"""
                assert conn.execute(
                    text(
                        "SELECT market.validate_fact_payload("
                        "'market.l2_book.v1', CAST(:payload AS jsonb))"
                    ),
                    {"payload": l2_payload},
                ).scalar_one() is True
                assert conn.execute(
                    text(
                        "SELECT market.validate_fact_payload("
                        "'market.l2_book.v1', "
                        "jsonb_set(CAST(:payload AS jsonb), "
                        "'{entries,0,provider}', '\"COINBASE\"'::jsonb))"
                    ),
                    {"payload": l2_payload},
                ).scalar_one() is False
                with pytest.raises(DBAPIError, match="payload does not satisfy"):
                    with conn.begin_nested():
                        conn.execute(
                            text(
                                _fact_insert_sql(
                                    """'{
                                        "rate":"0.0001",
                                        "raw_rate":"0.0001",
                                        "funding_time":"2026-08-09T11:00:00Z",
                                        "interval_seconds": 3600,
                                        "unit":"fraction",
                                        "provider":"TEST"
                                    }'::jsonb"""
                                )
                            ),
                            {"hash": "c" * 64},
                        )
                conn.execute(
                    text(
                        _fact_insert_sql(
                            """'{
                                "rate":"0.0001",
                                "raw_rate":"0.0001",
                                "funding_time":"2026-08-09T11:00:00.000000Z",
                                "interval_seconds": 3600,
                                "unit":"fraction"
                            }'::jsonb"""
                        )
                    ),
                    {"hash": "c" * 64},
                )
                for mutation in (
                    "UPDATE market.fact_versions SET state='invalidated' "
                    "WHERE id='mfv_fixture'",
                    "DELETE FROM market.fact_versions WHERE id='mfv_fixture'",
                ):
                    with pytest.raises(
                        DBAPIError, match="immutable market-data relation"
                    ):
                        with conn.begin_nested():
                            conn.execute(text(mutation))
                assert conn.execute(
                    text(
                        "SELECT count(*) FROM market.fact_versions "
                        "WHERE id='mfv_fixture'"
                    )
                ).scalar_one() == 1
        finally:
            engine.dispose()

        _run_hard_cutover(dsn)
        engine = create_engine(dsn, future=True)
        try:
            with engine.begin() as conn:
                conn.execute(
                    text(
                        "DROP INDEX market.ix_market_fact_series_material, "
                        "market.ix_market_fact_series_source"
                    )
                )
        finally:
            engine.dispose()
        _run_lookup_index_migration(dsn)
        first_lookup_snapshot = _schema_snapshot(dsn)
        _run_lookup_index_migration(dsn)
        assert _schema_snapshot(dsn) == first_lookup_snapshot
        assert {
            "ix_market_fact_series_material",
            "ix_market_fact_series_source",
        } <= set(first_lookup_snapshot["indexes"])

        engine = create_engine(dsn, future=True)
        try:
            with engine.connect() as conn:
                remaining = conn.execute(
                    text(
                        "SELECT count(*) FROM pg_tables "
                        "WHERE schemaname='market' "
                        "AND tablename = ANY(CAST(:tables AS text[]))"
                    ),
                    {
                        "tables": list(PRE_CUTOVER_TABLES)
                        + ["numeric_fact_versions"]
                    },
                ).scalar_one()
                assert int(remaining) == 0
                assert conn.execute(
                    text("SELECT count(*) FROM market.fact_versions")
                ).scalar_one() == 1
                assert conn.execute(
                    text("SELECT count(*) FROM market.fact_schemas")
                ).scalar_one() == len(expected_registry)
        finally:
            engine.dispose()


def test_lookup_index_migration_recovers_interrupted_and_wrong_indexes() -> None:
    with fresh_migration_database("canonical_lookup_indexes") as dsn:
        engine = create_engine(dsn, future=True)
        try:
            with engine.begin() as conn:
                conn.execute(text("CREATE SCHEMA market"))
                conn.execute(
                    text(
                        """
                        CREATE TABLE market.fact_versions (
                            series_id bigint NOT NULL,
                            material_hash varchar(64) NOT NULL,
                            source_id bigint NOT NULL
                        )
                        """
                    )
                )
                conn.execute(
                    text(
                        """
                        INSERT INTO market.fact_versions (
                            series_id, material_hash, source_id
                        ) VALUES
                            (1, :first_hash, 17),
                            (1, :second_hash, 17)
                        """
                    ),
                    {"first_hash": "a" * 64, "second_hash": "b" * 64},
                )
        finally:
            engine.dispose()

        for index_name, columns in (
            ("ix_market_fact_series_material", "series_id"),
            ("ix_market_fact_series_source", "source_id"),
        ):
            failed = _run_sql_command(
                dsn,
                f"CREATE UNIQUE INDEX CONCURRENTLY {index_name} "
                f"ON market.fact_versions ({columns})",
                expected_success=False,
            )
            assert "could not create unique index" in failed.stderr

        engine = create_engine(dsn, future=True)
        try:
            with engine.connect() as conn:
                invalid_before = dict(
                    conn.execute(
                        text(
                            """
                            SELECT index_class.relname, index_state.indisvalid
                            FROM pg_index AS index_state
                            JOIN pg_class AS index_class
                              ON index_class.oid = index_state.indexrelid
                            WHERE index_class.relname IN (
                                'ix_market_fact_series_material',
                                'ix_market_fact_series_source'
                            )
                            ORDER BY index_class.relname
                            """
                        )
                    ).all()
                )
            assert invalid_before == {
                "ix_market_fact_series_material": False,
                "ix_market_fact_series_source": False,
            }
        finally:
            engine.dispose()

        psql_url = make_url(dsn).set(drivername="postgresql")
        environment = dict(os.environ)
        environment["PGOPTIONS"] = "-c statement_timeout=1ms"
        completed = subprocess.run(
            [
                "psql",
                psql_url.render_as_string(hide_password=False),
                "-v",
                "ON_ERROR_STOP=1",
                "-f",
                str(_LOOKUP_INDEX_MIGRATION),
            ],
            cwd=_REPO_ROOT,
            check=False,
            capture_output=True,
            text=True,
            env=environment,
        )
        assert completed.returncode == 0, (
            f"lookup migration failed\nstdout:\n{completed.stdout}\n"
            f"stderr:\n{completed.stderr}"
        )

        first = _lookup_index_snapshot(dsn)
        first_oids = _lookup_index_oids(dsn)
        _run_lookup_index_migration(dsn)
        assert _lookup_index_snapshot(dsn) == first
        assert _lookup_index_oids(dsn) == first_oids
        assert first == {
            "ix_market_fact_series_material": (
                True,
                True,
                "CREATE INDEX ix_market_fact_series_material ON "
                "market.fact_versions USING btree (series_id, material_hash)",
            ),
            "ix_market_fact_series_source": (
                True,
                True,
                "CREATE INDEX ix_market_fact_series_source ON "
                "market.fact_versions USING btree (series_id, source_id)",
            ),
        }

        engine = create_engine(dsn, future=True)
        try:
            with engine.begin() as conn:
                conn.execute(
                    text("DROP INDEX market.ix_market_fact_series_material")
                )
                conn.execute(
                    text(
                        "CREATE INDEX ix_market_fact_series_material "
                        "ON market.fact_versions (material_hash, series_id)"
                    )
                )
        finally:
            engine.dispose()
        wrong = _lookup_index_snapshot(dsn)
        assert wrong["ix_market_fact_series_material"] == (
            True,
            True,
            "CREATE INDEX ix_market_fact_series_material ON "
            "market.fact_versions USING btree (material_hash, series_id)",
        )
        _run_lookup_index_migration(dsn)
        assert _lookup_index_snapshot(dsn) == first
