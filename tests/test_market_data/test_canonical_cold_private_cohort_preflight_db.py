"""Exercise one-day planning with native partition pruning and cursor ordering."""
from datetime import date

import pytest
from sqlalchemy import create_engine, text

from market_data.canonical import build_fact_version_id
from scripts.db.canonical_cold_private_cohort_preflight import _arguments, inspect_private_cohort
from tests.test_market_data.migration_test_support import fresh_migration_database


pytestmark = pytest.mark.db
DAY = date(2026, 10, 4)
RESUME_ID = build_fact_version_id(series_id=42, observation_key="cohort-tail", revision=1, row_hash="a" * 64)


@pytest.fixture
def source():
    with fresh_migration_database("cohort_preflight", install_extensions=False) as dsn:
        engine = create_engine(dsn)
        try:
            with engine.begin() as conn:
                conn.exec_driver_sql(f"""
                    CREATE SCHEMA market;
                    CREATE TABLE market.fact_retention_partitions (
                        storage_day date PRIMARY KEY, state text, sealed_at timestamptz, expected_rows bigint
                    );
                    CREATE TABLE market.fact_archive_manifests (
                        id text PRIMARY KEY, storage_day date, page_ordinal int, row_count int,
                        first_commit_seq bigint, first_id text, last_commit_seq bigint, last_id text,
                        UNIQUE(storage_day,page_ordinal)
                    );
                    CREATE TABLE market.fact_archive_active_generations (manifest_id text PRIMARY KEY);
                    CREATE TABLE market.fact_versions (storage_day date, market_commit_seq bigint, id text)
                        PARTITION BY RANGE (storage_day);
                    CREATE INDEX ix_market_fact_storage_page
                        ON market.fact_versions(storage_day,market_commit_seq,id);
                    CREATE TABLE market.fact_versions_legacy PARTITION OF market.fact_versions
                        FOR VALUES FROM (MINVALUE) TO ('2026-10-10');
                    CREATE TABLE market.fact_versions_other PARTITION OF market.fact_versions
                        FOR VALUES FROM ('2026-10-10') TO (MAXVALUE);
                    CREATE TABLE market.fact_hot_payloads (storage_day date, id text, payload jsonb NOT NULL,
                        PRIMARY KEY(storage_day,id)) PARTITION BY RANGE (storage_day);
                    CREATE TABLE market.fact_hot_payloads_selected PARTITION OF market.fact_hot_payloads
                        FOR VALUES FROM ('2026-10-04') TO ('2026-10-05');
                    CREATE TABLE market.fact_hot_payloads_other PARTITION OF market.fact_hot_payloads
                        FOR VALUES FROM ('2026-10-10') TO ('2026-10-11');
                    INSERT INTO market.fact_retention_partitions VALUES ('2026-10-04','sealed',now(),6);
                    INSERT INTO market.fact_archive_manifests VALUES
                        ('old','2026-10-04',0,1,1,'a',1,'a'),
                        ('prepared','2026-10-04',1,2,3,'a',3,'b'),
                        ('other','2026-10-10',0,999,1,'a',99,'a');
                    INSERT INTO market.fact_archive_active_generations VALUES ('prepared'),('other');
                    INSERT INTO market.fact_versions VALUES
                        ('2026-10-04',1,'a'), ('2026-10-04',3,'a'), ('2026-10-04',3,'b'),
                        ('2026-10-04',3,'{RESUME_ID}'), ('2026-10-04',8,'b'), ('2026-10-04',9,'a'),
                        ('2026-10-10',4,'a');
                    INSERT INTO market.fact_hot_payloads VALUES
                        ('2026-10-04','{RESUME_ID}','{{}}'), ('2026-10-04','b','{{}}'), ('2026-10-04','a','{{}}'),
                        ('2026-10-10','b','{{}}');
                """)
            yield engine
        finally:
            engine.dispose()


def _inspect(source, **bounds):
    with source.begin() as conn:
        conn.exec_driver_sql("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
        conn.exec_driver_sql("SET LOCAL enable_seqscan=off")
        return inspect_private_cohort(conn, day=DAY, check=lambda: None,
                                      **({"max_pages": 10, "max_tail_rows": 10} | bounds))


def test_one_day_mixed_prefix_and_sql_tail(source):
    report = _inspect(source)
    assert report["state"] == "mixed_hot_tail_present"
    assert report["published_prefix"] == {
        "rows": 3, "pages": 2, "pages_with_active_generation_pointer": 1, "last_cursor": (3, "b"),
    }
    assert report["expected_uncovered_rows"] == 3
    assert report["tail"] == {
        "after_exclusive": (3, "b"), "before_exclusive": None, "last_inclusive": (9, "a"),
        "next_unmeasured_cursor": None, "rows": 3, "missing_sql_payload_rows": 0,
        "first_missing_sql_payload_cursor": None, "window_exhausted": True,
    }


def test_measured_windows_keep_exclusive_bounds(source, monkeypatch):
    from scripts.db import canonical_cold_private_cohort_preflight as preflight

    monkeypatch.setattr(preflight, "TAIL_BATCH_ROWS", 1)
    partial = _inspect(source, max_tail_rows=2)
    assert partial["state"] == "tail_window_partial"
    assert partial["tail"]["last_inclusive"] == (8, "b")
    assert partial["tail"]["next_unmeasured_cursor"] == (9, "a")
    assert not partial["tail"]["window_exhausted"]
    _, cursors = _arguments(["--proof", "/proof", "--proof-sha256", "a" * 64, "--day", DAY.isoformat(),
                            "--max-pages", "10", "--max-tail-rows", "10", "--max-seconds", "5",
                            "--after-commit-seq", "3", "--after-fact-id", RESUME_ID])
    resumed = _inspect(source, after=cursors["after"], before=(9, "a"))
    assert resumed["state"] == "tail_slice_complete"
    assert resumed["requested"]["after_exclusive"] == resumed["tail"]["after_exclusive"] == (3, RESUME_ID)
    assert resumed["requested"]["before_exclusive"] == resumed["tail"]["before_exclusive"] == (9, "a")
    assert resumed["tail"]["rows"] == 1
    assert resumed["tail"]["last_inclusive"] == (8, "b")
    assert _inspect(source, after=(8, "missing"))["state"] == "requested_cursor_missing"
    assert _inspect(source, after=(3, "a"))["state"] == "requested_cursor_outside_tail"


def test_missing_sql_payload_cannot_use_another_day(source):
    with source.begin() as conn:
        conn.exec_driver_sql("DELETE FROM market.fact_hot_payloads WHERE storage_day='2026-10-04' AND id='b'")
    report = _inspect(source)
    assert report["state"] == "hot_payload_missing"
    assert report["tail"]["missing_sql_payload_rows"] == 1
    assert report["tail"]["first_missing_sql_payload_cursor"] == (8, "b")
    with source.begin() as conn:
        conn.exec_driver_sql("UPDATE market.fact_retention_partitions SET state='reclaimed'")
    assert _inspect(source)["state"] == "reclaimed_archive_missing"


def test_page_bound_ordinal_and_cursor_errors_stop_before_tail(source):
    assert _inspect(source, max_pages=1)["state"] == "archive_window_partial"
    with source.begin() as conn:
        conn.exec_driver_sql("UPDATE market.fact_archive_manifests SET page_ordinal=2 WHERE id='prepared'")
    gap = _inspect(source)
    assert gap["state"] == "archive_ordinal_gap"
    assert (gap["missing_ordinal"], gap["next_published_ordinal"], gap["tail"]) == (1, 2, None)
    with source.begin() as conn:
        conn.exec_driver_sql("""
            UPDATE market.fact_archive_manifests SET page_ordinal=1,first_commit_seq=1 WHERE id='prepared'
        """)
    overlap = _inspect(source)
    assert (overlap["state"], overlap["ordinal"], overlap["tail"]) == ("archive_cursor_invalid", 1, None)


def test_retention_missing_and_count_disagreement(source):
    with source.begin() as conn:
        conn.execute(text("UPDATE market.fact_retention_partitions SET expected_rows=7 WHERE storage_day=:day"),
                     {"day": DAY})
    assert _inspect(source)["state"] == "sealed_row_count_mismatch"
    with source.begin() as conn:
        conn.exec_driver_sql("UPDATE market.fact_retention_partitions SET state='open'")
    assert _inspect(source)["state"] == "retention_day_unsealed"
    with source.begin() as conn:
        conn.exec_driver_sql("DELETE FROM market.fact_retention_partitions")
    assert _inspect(source)["state"] == "retention_day_missing"
