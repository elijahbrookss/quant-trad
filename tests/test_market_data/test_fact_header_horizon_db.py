"""Qualify range selection over dated headers on disposable data.

This small-row fixture measures partition fan-out and semantic correctness, not
hardware throughput, cold-disk performance, hydration, or two-year capacity.
"""
from datetime import UTC, date, datetime, timedelta
import json

import pytest
from sqlalchemy import create_engine, event, text
from sqlalchemy.orm import Session

from portal.backend.db.fact_series_day_schema import (
    assert_fact_series_day_contract, install_fact_series_day_functions,
)
from portal.backend.service.storage.repos import market_data as repository
from tests.test_market_data.migration_test_support import fresh_migration_database

pytestmark = pytest.mark.db
FIRST = date(2024, 1, 1)


def _executed_headers(node):
    names = set()
    if node.get("Actual Loops", 0) and node.get("Relation Name", "").startswith("fact_versions_"):
        names.add(node["Relation Name"])
    for child in node.get("Plans", []):
        names.update(_executed_headers(child))
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
                    );
                """))
                for offset in range(days):
                    day = FIRST + timedelta(days=offset)
                    until = day + timedelta(days=1)
                    conn.exec_driver_sql(
                        f"CREATE TABLE market.fact_versions_{day:%Y%m%d} "
                        f"PARTITION OF market.fact_versions FOR VALUES FROM ('{day}') TO ('{until}')"
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
                conn.execute(text("ANALYZE market.fact_versions"))
                conn.execute(text("ANALYZE market.sources"))
                conn.execute(text("ANALYZE market.series"))
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
            finally:
                event.remove(engine, "before_cursor_execute", observe)
            statement, parameters = captured[0]
            with engine.connect() as conn:
                plan = conn.exec_driver_sql("EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) " + statement,
                                           parameters).scalar_one()[0]
            metrics = {
                "fixture_days": days, "fixture_rows": days + 1,
                "executed_header_partitions": len(_executed_headers(plan["Plan"])),
                "planning_ms": plan["Planning Time"], "execution_ms": plan["Execution Time"],
                "scope": "local_small_row_selection_only",
            }
            record_property("header_horizon", json.dumps(metrics, sort_keys=True))
            print("HEADER_HORIZON " + json.dumps(metrics, sort_keys=True))

            # Candidate experiment only: a directory seeded from this fixture,
            # not a runtime schema or an accepted transactional maintenance path.
            with engine.begin() as conn:
                conn.execute(text("""
                    CREATE TABLE market.fixture_day_ranges AS
                    SELECT series_id,storage_day,min(observation_time) AS minimum,
                           max(observation_time) AS maximum
                    FROM market.fact_versions GROUP BY series_id,storage_day;
                    CREATE UNIQUE INDEX ON market.fixture_day_ranges(series_id,storage_day);
                    ANALYZE market.fixture_day_ranges;
                    CREATE TABLE market.fact_header_series_days (
                        series_id bigint NOT NULL,storage_day date NOT NULL,
                        min_observation_time timestamptz NOT NULL,max_observation_time timestamptz NOT NULL,
                        PRIMARY KEY(series_id,storage_day),
                        CONSTRAINT ck_market_fact_series_day_bounds
                            CHECK(min_observation_time <= max_observation_time)
                    );
                    INSERT INTO market.fact_header_series_days SELECT * FROM market.fixture_day_ranges;
                    ANALYZE market.fact_header_series_days;
                """))
                install_fact_series_day_functions(conn)
                assert_fact_series_day_contract(conn)
            replacements = {
                "stable_directory": """
                    FROM market.read_fact_headers_in_range(
                        %(series_id)s,%(start)s,%(end)s
                    ) AS versions
                """,
                "array_directory": """FROM market.fact_versions AS versions""",
                "lateral_directory": """
                    FROM market.fixture_day_ranges AS days
                    CROSS JOIN LATERAL (
                        SELECT headers.* FROM market.fact_versions AS headers
                        WHERE headers.storage_day=days.storage_day
                          AND headers.series_id=%(series_id)s
                          AND headers.observation_time >= %(start)s
                          AND headers.observation_time < %(end)s
                        OFFSET 0
                    ) AS versions
                """,
            }
            for label, replacement in replacements.items():
                plans = []
                with engine.connect() as conn:
                    for index, (original_sql, original_params) in enumerate(captured):
                        candidate_sql = original_sql.replace(
                            "FROM market.fact_versions AS versions", replacement)
                        if label == "array_directory":
                            candidate_sql = candidate_sql.replace(
                                "versions.series_id =", """versions.storage_day = ANY(ARRAY(
                                    SELECT storage_day FROM market.fixture_day_ranges
                                    WHERE series_id=%(series_id)s AND minimum < %(end)s
                                      AND maximum >= %(start)s
                                )) AND versions.series_id =""")
                        elif label == "lateral_directory":
                            candidate_sql = candidate_sql.replace(
                                "versions.series_id =", """days.series_id=%(series_id)s
                                AND days.minimum < %(end)s AND days.maximum >= %(start)s
                                AND versions.series_id =""")
                        result = conn.exec_driver_sql(candidate_sql, original_params).mappings().all()
                        assert [row["id"] for row in result] == [
                            "late-correction" if index == 0 else "base-0"
                        ]
                        if index == 0:
                            plans.append(conn.exec_driver_sql(
                                "EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) " + candidate_sql,
                                original_params).scalar_one()[0])
                candidate = {
                    "candidate": label,
                    # EXPLAIN does not expose the STABLE function's nested plan.
                    "executed_header_partitions": (
                        None if label == "stable_directory" else len(_executed_headers(plans[0]["Plan"]))
                    ),
                    "planning_ms": plans[0]["Planning Time"],
                    "execution_ms": plans[0]["Execution Time"],
                    "scope": "fixture_directory_only_not_runtime",
                }
                record_property(label, json.dumps(candidate, sort_keys=True))
                print("HEADER_HORIZON_CANDIDATE " + json.dumps(candidate, sort_keys=True))
        finally:
            engine.dispose()
