"""Qualify range selection over dated headers on disposable data.

This small-row fixture measures partition fan-out and semantic correctness, not
hardware throughput, cold-disk performance, hydration, or two-year capacity.
"""
from datetime import UTC, date, datetime, timedelta
import json

import pytest
from sqlalchemy import create_engine, event, text
from sqlalchemy.orm import Session

from portal.backend.db import MarketFactHeaderSeriesDayRecord
from portal.backend.db.fact_series_day_schema import (
    assert_fact_series_day_contract, install_fact_series_day_functions,
)
from portal.backend.service.storage.repos import market_data as repository
from portal.backend.service.storage.repos.fact_storage import CANONICAL_RANGE_ROW_FROM
from tests.test_market_data.migration_test_support import fresh_migration_database

pytestmark = pytest.mark.db
FIRST = date(2024, 1, 1)


def _executed_partitions(node, prefix):
    names = set()
    if node.get("Actual Loops", 0) and node.get("Relation Name", "").startswith(prefix):
        names.add(node["Relation Name"])
    for child in node.get("Plans", []):
        names.update(_executed_partitions(child, prefix))
    return names


@pytest.mark.parametrize("days", [pytest.param(32, id="smoke"), pytest.param(730, id="two_years")])
def test_range_selection_preserves_late_corrections(monkeypatch, record_property, days):
    with fresh_migration_database("header_horizon", install_extensions=False) as dsn:
        engine = create_engine(dsn, future=True)
        try:
            with engine.begin() as conn:
                conn.execute(text("""
                    CREATE SCHEMA market;
                    CREATE TABLE market.sources (
                        id bigint PRIMARY KEY, identity_key text, provider text, venue text,
                        source_kind text, adapter_version text
                    );
                    INSERT INTO market.sources VALUES(1,'fixture','TEST','ISOLATED','fixture','v1');
                    CREATE TABLE market.series(id bigint PRIMARY KEY,dimensions jsonb);
                    INSERT INTO market.series VALUES(1,'{}');
                    CREATE TABLE market.fact_versions (
                        id text NOT NULL,storage_day date NOT NULL,series_id bigint NOT NULL,
                        source_id bigint NOT NULL,observation_key text NOT NULL,revision integer NOT NULL,
                        market_commit_seq bigint NOT NULL,observation_time timestamptz NOT NULL,
                        known_at timestamptz NOT NULL,state text NOT NULL,
                        PRIMARY KEY(id,storage_day)
                    ) PARTITION BY RANGE(storage_day);
                    CREATE INDEX ix_market_fact_series_time_revision ON market.fact_versions
                        (series_id,observation_time DESC,observation_key,revision DESC);
                    CREATE INDEX ix_market_fact_series_known ON market.fact_versions
                        (series_id,known_at,observation_time);
                    CREATE TABLE market.fact_hot_payloads (
                        id text,storage_day date,payload jsonb,provenance jsonb,quality jsonb,
                        PRIMARY KEY(id,storage_day)
                    ) PARTITION BY RANGE(storage_day);
                """))
                MarketFactHeaderSeriesDayRecord.__table__.create(conn)
                install_fact_series_day_functions(conn)
                for offset in range(days):
                    day = FIRST + timedelta(days=offset)
                    until = day + timedelta(days=1)
                    conn.exec_driver_sql(
                        f"CREATE TABLE market.fact_versions_{day:%Y%m%d} "
                        f"PARTITION OF market.fact_versions FOR VALUES FROM ('{day}') TO ('{until}')"
                    )
                for offset in range(max(0, days - 32), days):
                    day = FIRST + timedelta(days=offset)
                    until = day + timedelta(days=1)
                    conn.exec_driver_sql(
                        f"CREATE TABLE market.fact_hot_payloads_{day:%Y%m%d} "
                        f"PARTITION OF market.fact_hot_payloads FOR VALUES FROM ('{day}') TO ('{until}')"
                    )
                conn.execute(text("""
                    INSERT INTO market.fact_versions
                    SELECT 'base-'||d,CAST(:first AS date)+d,1,1,'observation-'||d,1,d+1,
                           (CAST(:first AS date)+d)::timestamp AT TIME ZONE 'UTC',
                           (CAST(:first AS date)+d)::timestamp AT TIME ZONE 'UTC','active'
                    FROM generate_series(0,:last) d
                """), {"first": FIRST, "last": days - 1})
                # Observation zero is corrected on the last storage day. Storage-day
                # pruning by the requested observation window would lose it.
                conn.execute(text("""
                    INSERT INTO market.fact_versions VALUES (
                        'late-correction',:day,1,1,'observation-0',2,10000,:observation,:known,'active'
                    )
                """), {"day": FIRST + timedelta(days=days - 1),
                       "observation": datetime.combine(FIRST, datetime.min.time(), UTC),
                       "known": datetime.combine(FIRST + timedelta(days=days - 1), datetime.min.time(), UTC)})
                conn.execute(text("""
                    INSERT INTO market.fact_hot_payloads
                    SELECT 'base-'||d,CAST(:first AS date)+d,
                           jsonb_build_object('fixture',d),'{}','{}'
                    FROM generate_series(:hot_first,:last) d
                """), {"first": FIRST, "hot_first": max(0, days - 32), "last": days - 1})
                conn.execute(text("""
                    INSERT INTO market.fact_hot_payloads
                    VALUES ('late-correction',:day,'{"fixture":"correction"}','{}','{}')
                """), {"day": FIRST + timedelta(days=days - 1)})
                conn.execute(text("ANALYZE market.fact_hot_payloads"))
                conn.execute(text("ANALYZE market.fact_versions"))
                conn.execute(text("ANALYZE market.sources"))
                conn.execute(text("ANALYZE market.series"))
                conn.execute(text("ANALYZE market.fact_header_series_days"))
                assert_fact_series_day_contract(conn)
            # Isolate selection from independently covered payload decoding.
            monkeypatch.setattr(repository.canonical_fact_storage_repository, "hydrate_rows",
                                lambda session, rows: [dict(row) for row in rows])
            captured = []
            def observe(conn, cursor, statement, parameters, context, executemany):
                if "WITH visible AS" in statement:
                    captured.append((statement, parameters))
            event.listen(engine, "before_cursor_execute", observe)
            start = datetime.combine(FIRST, datetime.min.time(), UTC)
            try:
                with Session(engine) as session:
                    kwargs = dict(series_id=1,start=start,end=start + timedelta(days=1),
                                  as_of_commit_seq=None,known_at_lte=None)
                    latest = repository.PostgresMarketDataRepository._read_canonical_rows_with_session(session, **kwargs)
                    original = repository.PostgresMarketDataRepository._read_canonical_rows_with_session(
                        session, **{**kwargs,"as_of_commit_seq":days})
                    causal = repository.PostgresMarketDataRepository._read_canonical_rows_with_session(
                        session, **{**kwargs,"known_at_lte":start})
                    assert [row["id"] for row in latest] == ["late-correction"]
                    assert [row["id"] for row in original] == ["base-0"]
                    assert [row["id"] for row in causal] == ["base-0"]
                    assert latest[0]["payload"] == {"fixture": "correction"}
                    assert original[0]["payload"] == (None if days > 32 else {"fixture": 0})
            finally:
                event.remove(engine, "before_cursor_execute", observe)
            statement, parameters = captured[0]
            assert "market.read_fact_headers_in_range" in statement
            with engine.connect() as conn:
                plan = conn.exec_driver_sql("EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) " + statement,
                                           parameters).scalar_one()[0]
            metrics = {
                "fixture_days": days, "fixture_rows": days + 1,
                "executed_header_partitions": None,  # FunctionScan hides its nested plan.
                "planning_ms": plan["Planning Time"], "execution_ms": plan["Execution Time"],
                "hot_fixture_partitions": min(days, 32),
                "executed_hot_partitions": len(_executed_partitions(plan["Plan"], "fact_hot_payloads_")),
                "scope": "runtime_directory_and_partitioned_hot_join_small_rows",
            }
            record_property("header_horizon", json.dumps(metrics, sort_keys=True))
            print("HEADER_HORIZON " + json.dumps(metrics, sort_keys=True))

            # Compare the same production selector and filters with its old
            # unpruned source. The directory above is maintained by real insert
            # triggers; no fixture seed substitutes for capture.
            runtime_source = str(text(CANONICAL_RANGE_ROW_FROM).compile(dialect=engine.dialect))
            runtime_header = runtime_source.split("JOIN market.sources", 1)[0].strip()
            baseline_plan = None
            with engine.connect() as conn:
                for index, (original_sql, original_params) in enumerate(captured):
                    assert runtime_header in original_sql
                    baseline_sql = original_sql.replace(
                        runtime_header, "FROM market.fact_versions AS versions", 1)
                    result = conn.exec_driver_sql(baseline_sql, original_params).mappings().all()
                    assert [row["id"] for row in result] == [
                        "late-correction" if index == 0 else "base-0"
                    ]
                    if index == 0:
                        baseline_plan = conn.exec_driver_sql(
                            "EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) " + baseline_sql,
                            original_params).scalar_one()[0]
            baseline = {
                "candidate": "unpruned_reference",
                "executed_header_partitions": len(_executed_partitions(baseline_plan["Plan"], "fact_versions_")),
                "planning_ms": baseline_plan["Planning Time"],
                "execution_ms": baseline_plan["Execution Time"],
                "scope": "runtime_selector_reference_small_rows",
            }
            assert baseline["executed_header_partitions"] == days
            record_property("unpruned_reference", json.dumps(baseline, sort_keys=True))
            print("HEADER_HORIZON_REFERENCE " + json.dumps(baseline, sort_keys=True))
        finally:
            engine.dispose()
