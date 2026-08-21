from __future__ import annotations

import os
from pathlib import Path

import pytest
from sqlalchemy import create_engine, text

from market_data.fact_registry import supported_static_fact_payload_schemas
from portal.backend.db.session import Database


pytestmark = pytest.mark.db


def _clean_bootstrap_dsn() -> str:
    dsn = str(os.getenv("QT_CLEAN_BOOTSTRAP_TEST_DSN") or "").strip()
    if not dsn:
        pytest.skip("QT_CLEAN_BOOTSTRAP_TEST_DSN is required")
    return dsn


def test_empty_timescale_database_bootstraps_current_schema_atomically(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    dsn = _clean_bootstrap_dsn()
    engine = create_engine(dsn, future=True)
    try:
        with engine.connect() as conn:
            existing = conn.execute(
                text(
                    """
                    SELECT count(*)
                    FROM pg_class AS relation
                    JOIN pg_namespace AS namespace
                      ON namespace.oid = relation.relnamespace
                    WHERE relation.relkind IN ('r', 'p')
                      AND (
                          namespace.nspname IN (
                              'market',
                              'observability_events',
                              'observability_metrics'
                          )
                          OR (
                              namespace.nspname = 'public'
                              AND relation.relname LIKE 'portal_%'
                          )
                      )
                    """
                )
            ).scalar_one()
            assert int(existing) == 0

        first = Database(dsn)
        assert first.ensure_schema() is True, str(first.last_error)

        with engine.connect() as conn:
            tables = {
                str(row[0])
                for row in conn.execute(
                    text(
                        """
                        SELECT tablename
                        FROM pg_tables
                        WHERE schemaname = 'market'
                        """
                    )
                )
            }
            assert {
                "fact_schemas",
                "fact_versions",
                "fact_acquisition_coverage",
                "collection_definitions",
                "collector_operation_events",
                "stream_definitions",
            } <= tables

            registry_count = conn.execute(
                text("SELECT count(*) FROM market.fact_schemas")
            ).scalar_one()
            assert int(registry_count) == len(supported_static_fact_payload_schemas())

            triggers = {
                str(row[0])
                for row in conn.execute(
                    text(
                        """
                        SELECT trigger_name
                        FROM information_schema.triggers
                        WHERE event_object_schema = 'market'
                          AND event_object_table IN (
                              'fact_schemas',
                              'fact_versions',
                              'fact_acquisition_coverage'
                          )
                        """
                    )
                )
            }
            assert {
                "trg_reject_mutation_fact_schemas",
                "trg_reject_mutation_fact_versions",
                "trg_reject_mutation_fact_acquisition_coverage",
                "trg_assert_fact_version_valid",
            } <= triggers

        root = Path(__file__).parents[2]
        monkeypatch.setenv("PG_DSN", dsn)
        monkeypatch.setenv(
            "QT_SINGLE_NODE_INSTRUMENT_MANIFEST",
            str(
                root
                / "config/market_data/coinbase_perpetual_instruments.v1.json"
            ),
        )
        monkeypatch.setenv(
            "QT_SINGLE_NODE_TRADE_MANIFEST",
            str(root / "config/market_data/coinbase_perpetual_trade_fleet.v1.json"),
        )
        monkeypatch.setenv(
            "QT_SINGLE_NODE_L2_MANIFEST",
            str(
                root
                / "config/market_data/coinbase_perpetual_l2_fleet.v1.json"
            ),
        )
        monkeypatch.setenv(
            "QT_SINGLE_NODE_STRUCTURED_FACT_MANIFESTS",
            str(
                root
                / "config/market-data/structured-facts"
                / "chainlink-nxtassets-btc-etp-reserves.json"
            ),
        )
        from portal.backend.workers.single_node_initializer import (
            initialize_single_node_market_data,
        )

        initialized = initialize_single_node_market_data()
        repeated = initialize_single_node_market_data()
        assert len(initialized["instruments"]) == 3
        assert len(initialized["structured_fact_manifests"]) == 1
        assert len(initialized["scheduled_facts"]) == 7
        assert len(initialized["trade_streams"]["definitions"]) == 3
        assert len(initialized["level2_streams"]["definitions"]) == 3
        assert repeated["instrument_manifest_hash"] == (
            initialized["instrument_manifest_hash"]
        )

        with engine.connect() as conn:
            counts = conn.execute(
                text(
                    """
                    SELECT
                      (SELECT count(*) FROM portal_instruments) AS instruments,
                      (SELECT count(*) FROM market.collection_definitions) AS scheduled,
                      (SELECT count(*) FROM market.stream_definitions) AS streams,
                      (
                        SELECT count(*)
                        FROM market.collection_definitions AS definitions
                        JOIN market.sources AS sources
                          ON sources.id = definitions.source_id
                        JOIN market.series AS series
                          ON series.id = definitions.series_id
                        WHERE sources.provider = 'CHAINLINK'
                          AND sources.venue = 'ARBITRUM_MAINNET'
                          AND series.fact_type = 'asset.reserve_state'
                          AND definitions.enabled
                          AND definitions.desired_state = 'running'
                          AND definitions.poll_interval_seconds = 3600
                      ) AS chainlink_scheduled
                    """
                )
            ).mappings().one()
            assert int(counts["instruments"]) == 4
            assert int(counts["scheduled"]) == 7
            assert int(counts["streams"]) == 6
            assert int(counts["chainlink_scheduled"]) == 1

        second = Database(dsn)
        assert second.ensure_schema() is True, str(second.last_error)
    finally:
        engine.dispose()
