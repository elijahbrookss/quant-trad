from __future__ import annotations

import os
import subprocess
from pathlib import Path

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.engine import make_url

from tests.test_market_data.migration_test_support import fresh_migration_database


pytestmark = pytest.mark.db

_REPO_ROOT = Path(__file__).resolve().parents[2]
_MIGRATION = (
    _REPO_ROOT
    / "scripts/db/manual_migration_book_operational_rollups_v1.sql"
)


def _run_migration(dsn: str, *, expected_success: bool) -> subprocess.CompletedProcess[str]:
    url = make_url(dsn).set(drivername="postgresql")
    environment = dict(os.environ)
    existing_options = environment.get("PGOPTIONS", "").strip()
    environment["PGOPTIONS"] = (
        f"{existing_options} -c statement_timeout=0".strip()
    )
    completed = subprocess.run(
        [
            "psql",
            url.render_as_string(hide_password=False),
            "-X",
            "-v",
            "ON_ERROR_STOP=1",
            "-f",
            str(_MIGRATION),
        ],
        cwd=_REPO_ROOT,
        check=False,
        capture_output=True,
        text=True,
        env=environment,
    )
    assert (completed.returncode == 0) is expected_success, (
        f"unexpected rollup migration result\nstdout:\n{completed.stdout}\n"
        f"stderr:\n{completed.stderr}"
    )
    return completed


def _prepare_pre_migration_schema(dsn: str) -> None:
    engine = create_engine(dsn, future=True)
    try:
        with engine.begin() as conn:
            conn.execute(text("CREATE SCHEMA market"))
            conn.execute(
                text(
                    "CREATE TABLE market.series ("
                    "id bigint PRIMARY KEY, contract_version text NOT NULL)"
                )
            )
            conn.execute(
                text(
                    "CREATE TABLE market.stream_definitions ("
                    "id text PRIMARY KEY, "
                    "series_id bigint NOT NULL REFERENCES market.series(id), "
                    "contract_version text NOT NULL)"
                )
            )
            conn.execute(
                text(
                    "CREATE TABLE market.stream_lease_state ("
                    "expires_at timestamptz NOT NULL)"
                )
            )
            conn.execute(
                text(
                    "CREATE TABLE market.fact_versions ("
                    "series_id bigint NOT NULL REFERENCES market.series(id), "
                    "payload_schema_id text NOT NULL, payload jsonb NOT NULL, "
                    "market_commit_seq bigint NOT NULL)"
                )
            )
            conn.execute(
                text(
                    "CREATE TABLE market.book_checkpoint_manifests ("
                    "id text PRIMARY KEY, series_id bigint NOT NULL "
                    "REFERENCES market.series(id), "
                    "acknowledged_at timestamptz NOT NULL)"
                )
            )
            conn.execute(
                text(
                    "CREATE INDEX ix_market_fact_series_commit "
                    "ON market.fact_versions "
                    "(market_commit_seq, series_id)"
                )
            )
            conn.execute(
                text(
                    "CREATE INDEX "
                    "ix_market_book_checkpoint_series_acknowledged "
                    "ON market.book_checkpoint_manifests "
                    "(series_id, id, acknowledged_at)"
                )
            )
            conn.execute(
                text(
                    """
                    INSERT INTO market.series (id, contract_version) VALUES
                        (1, 'market.l2_book.v1'),
                        (2, 'market.trade.v1'),
                        (3, 'market.l2_book.v1')
                    """
                )
            )
            conn.exec_driver_sql(
                """
                    INSERT INTO market.fact_versions (
                        series_id, payload_schema_id, payload, market_commit_seq
                    ) VALUES
                        (1, 'market.l2_book.v1',
                         '{"event_type":"snapshot","entry_count":2}'::jsonb, 1),
                        (1, 'market.l2_book.v1',
                         '{"event_type":"update","entry_count":3}'::jsonb, 2),
                        (1, 'market.l2_book.v1',
                         '{"event_type":"update","entry_count":4}'::jsonb, 3),
                        (2, 'market.trade.v1',
                         '{"event_type":"update","entry_count":999}'::jsonb, 4)
                """
            )
            conn.execute(
                text(
                    "INSERT INTO market.book_checkpoint_manifests "
                    "(id, series_id, acknowledged_at) VALUES "
                    "('checkpoint-a', 1, '2026-08-22T00:00:00Z'), "
                    "('checkpoint-b', 1, '2026-08-22T00:01:00Z')"
                )
            )
            conn.execute(
                text(
                    "INSERT INTO market.stream_definitions "
                    "(id, series_id, contract_version) VALUES "
                    "('book-a', 1, 'market.l2_book.v1'), "
                    "('book-empty', 3, 'market.l2_book.v1')"
                )
            )
    finally:
        engine.dispose()


def _rollups(dsn: str) -> dict[int, tuple[object, ...]]:
    engine = create_engine(dsn, future=True)
    try:
        with engine.connect() as conn:
            rows = conn.execute(
                text(
                    """
                    SELECT series_id, snapshot_count, batch_count,
                           mutation_count, checkpoint_count,
                           fact_high_water_commit_seq,
                           checkpoint_high_water_id,
                           checkpoint_high_water_acknowledged_at
                    FROM market.book_operational_rollups
                    ORDER BY series_id
                    """
                )
            ).all()
        return {
            int(row[0]): (
                *(int(value) for value in row[1:6]),
                row[6],
                row[7].isoformat() if row[7] is not None else None,
            )
            for row in rows
        }
    finally:
        engine.dispose()


def test_book_operational_rollup_migration_guards_seeds_and_reruns() -> None:
    with fresh_migration_database("book_rollup") as dsn:
        _prepare_pre_migration_schema(dsn)
        engine = create_engine(dsn, future=True)
        try:
            with engine.begin() as conn:
                conn.execute(
                    text(
                        "INSERT INTO market.stream_lease_state (expires_at) "
                        "VALUES (now() + interval '5 minutes')"
                    )
                )
            blocked = _run_migration(dsn, expected_success=False)
            assert "active stream leases" in blocked.stderr
            with engine.connect() as conn:
                assert conn.execute(
                    text("SELECT to_regclass('market.book_operational_rollups')")
                ).scalar_one_or_none() is None

            with engine.begin() as conn:
                conn.execute(text("DELETE FROM market.stream_lease_state"))
            _run_migration(dsn, expected_success=True)
            assert _rollups(dsn) == {
                1: (
                    1,
                    2,
                    7,
                    2,
                    3,
                    "checkpoint-b",
                    "2026-08-22T00:01:00+00:00",
                ),
                3: (0, 0, 0, 0, 0, None, None),
            }
            with engine.connect() as conn:
                assert conn.execute(
                    text(
                        "SELECT pg_get_indexdef("
                        "to_regclass('market.ix_market_fact_series_commit'))"
                    )
                ).scalar_one() == (
                    "CREATE INDEX ix_market_fact_series_commit ON "
                    "market.fact_versions USING btree "
                    "(series_id, market_commit_seq)"
                )
                assert conn.execute(
                    text(
                        "SELECT pg_get_indexdef(to_regclass("
                        "'market.ix_market_book_checkpoint_series_acknowledged'"
                        "))"
                    )
                ).scalar_one() == (
                    "CREATE INDEX "
                    "ix_market_book_checkpoint_series_acknowledged ON "
                    "market.book_checkpoint_manifests USING btree "
                    "(series_id, acknowledged_at, id)"
                )

            with engine.begin() as conn:
                conn.execute(
                    text(
                        "INSERT INTO market.book_checkpoint_manifests "
                        "(id, series_id, acknowledged_at) VALUES "
                        "('checkpoint-backdated', 1, "
                        "'2026-08-21T23:59:00Z')"
                    )
                )
            assert _rollups(dsn)[1] == (
                1,
                2,
                7,
                3,
                3,
                "checkpoint-b",
                "2026-08-22T00:01:00+00:00",
            )

            with engine.begin() as conn:
                conn.exec_driver_sql(
                    """
                        INSERT INTO market.fact_versions (
                            series_id, payload_schema_id, payload,
                            market_commit_seq
                        ) VALUES (
                            1, 'market.l2_book.v1',
                            '{"event_type":"update","entry_count":2}'::jsonb,
                            5
                        )
                    """
                )
                conn.execute(
                    text(
                        "INSERT INTO market.book_checkpoint_manifests "
                        "(id, series_id, acknowledged_at) VALUES "
                        "('checkpoint-c', 1, '2026-09-01T00:00:00Z')"
                    )
                )
            _run_migration(dsn, expected_success=True)
            assert _rollups(dsn) == {
                1: (
                    1,
                    3,
                    9,
                    4,
                    5,
                    "checkpoint-c",
                    "2026-09-01T00:00:00+00:00",
                ),
                3: (0, 0, 0, 0, 0, None, None),
            }
        finally:
            engine.dispose()


def test_book_operational_rollup_migration_rejects_definition_contract_mismatch() -> None:
    with fresh_migration_database("book_rollup_mismatch") as dsn:
        _prepare_pre_migration_schema(dsn)
        engine = create_engine(dsn, future=True)
        try:
            with engine.begin() as conn:
                conn.execute(
                    text(
                        "UPDATE market.stream_definitions "
                        "SET series_id = 2 WHERE id = 'book-a'"
                    )
                )
            blocked = _run_migration(dsn, expected_success=False)
            assert "stream definition contract mismatch" in blocked.stderr
            with engine.connect() as conn:
                assert conn.execute(
                    text("SELECT to_regclass('market.book_operational_rollups')")
                ).scalar_one_or_none() is None
        finally:
            engine.dispose()


def test_book_operational_rollup_migration_rejects_non_l2_checkpoint_series() -> None:
    with fresh_migration_database("book_rollup_checkpoint_mismatch") as dsn:
        _prepare_pre_migration_schema(dsn)
        engine = create_engine(dsn, future=True)
        try:
            with engine.begin() as conn:
                conn.execute(
                    text(
                        "INSERT INTO market.book_checkpoint_manifests "
                        "(id, series_id, acknowledged_at) VALUES "
                        "('checkpoint-wrong-series', 2, now())"
                    )
                )
            blocked = _run_migration(dsn, expected_success=False)
            assert "book checkpoints reference non-L2 series" in blocked.stderr
            with engine.connect() as conn:
                assert conn.execute(
                    text("SELECT to_regclass('market.book_operational_rollups')")
                ).scalar_one_or_none() is None
        finally:
            engine.dispose()
