from contextlib import contextmanager
from datetime import UTC, date, datetime
from decimal import Decimal
import json

import pytest
from sqlalchemy.exc import SQLAlchemyError

from portal.backend.service.storage.repos import capacity

NOW = datetime(2026, 1, 20, 12, tzinfo=UTC)


def _row(**changes):
    row = {"policy_available": True, "cohorts_truncated": False, "backlog_truncated": False,
        "backlog_count_inconsistent": False, "verified_source_truncated": False,
        "eligible_cohorts": 2, "retirement_pending_cohorts": 1, "unknown_cohorts": 0,
        "known_backlog_rows": Decimal(90), "backlog_source_bytes": Decimal(1000),
        "oldest_eligible_day": date(2026, 1, 1), "published_pages_window": 1,
        "published_rows_window": Decimal(10), "published_bytes_window": Decimal(100),
        "verified_pages_window": 1, "verified_rows_window": Decimal(10), "verified_bytes_window": Decimal(100),
        "retired_cohorts_window": 1, "retired_rows_window": Decimal(20), "retired_relation_bytes_window": Decimal(200),
        "last_publication_unixtime": Decimal("1768900000.25"), "last_verification_unixtime": None,
        "last_retirement_unixtime": None, "wal_resident_bytes": Decimal(16384),
        "archiver_failed_total": 0, "archiver_last_success_unixtime": None}
    row.update(changes)
    return row


def test_snapshot_preserves_distinct_flow_and_allocation_bases_and_json_numbers():
    result = capacity._storage_operating_payload({}, _row(), NOW)
    assert result["backlog_rows"] == 90 and result["state"] == "ready"
    assert result["published_bytes_window"] == 100
    assert result["retired_relation_bytes_window"] == 200
    assert result["oldest_eligible_age_seconds"] == 19*86400
    assert result["last_publication_unixtime"] == 1768900000.25
    json.dumps(result, allow_nan=False)


def test_open_cohort_is_unknown_not_zero_or_complete():
    result = capacity._storage_operating_payload({}, _row(unknown_cohorts=1), NOW)
    assert result["backlog_rows"] is None and result["backlog_source_bytes"] is None
    assert result["known_backlog_rows"] == 90 and result["known_backlog_source_bytes"] == 1000
    assert result["state"] == "partial" and result["partial"]
    assert result["retirement_pending_cohorts"] == 1


@pytest.mark.parametrize("changes", [{"cohorts_truncated": True}, {"backlog_truncated": True},
    {"policy_available": False}, {"backlog_count_inconsistent": True}])
def test_incomplete_catalog_cannot_claim_an_exact_backlog(changes):
    result = capacity._storage_operating_payload({}, _row(**changes), NOW)
    assert result["backlog_rows"] is None and result["known_backlog_rows"] is None
    assert result["state"] != "ready" and result["partial"]
    assert result["retirement_pending_cohorts"] is None


@pytest.mark.parametrize("prefix,key,limit", [("published", "pages", 10000),
    ("verified", "pages", 10000), ("retired", "cohorts", 2048)])
def test_truncated_window_is_not_published_as_a_complete_flow(prefix, key, limit):
    result = capacity._storage_operating_payload({}, _row(**{prefix+"_"+key+"_window": limit+1}), NOW)
    assert result["state"] == "partial"
    assert all(value is None for name, value in result.items() if name.startswith(prefix+"_") and name.endswith("_window"))


def test_verification_source_limit_stays_partial_even_if_many_receipts_are_unqualified():
    result = capacity._storage_operating_payload({}, _row(verified_source_truncated=True), NOW)
    assert result["state"] == "partial" and result["verified_rows_window"] is None


class _Result:
    def __init__(self, rows): self.rows = rows
    def mappings(self): return self
    def one(self): return self.rows[0]
    def __iter__(self): return iter(self.rows)


class _Session:
    def __init__(self, *, fail_operating=False, fail_allocation=False):
        self.statements = []
        self.fail_operating, self.fail_allocation = fail_operating, fail_allocation
    def execute(self, statement, params=None):
        self.statements.append((str(statement), params))
        if statement is capacity._STORAGE_OPERATING_QUERY:
            if self.fail_operating: raise SQLAlchemyError("private SQL detail")
            return _Result([_row()])
        if statement is capacity._RELATION_ALLOCATION_QUERY:
            if self.fail_allocation: raise SQLAlchemyError("private SQL detail")
            return _Result([{"family": "raw_custody_lookup", "tablespace": "pg_default", "allocation_kind": "index",
                "allocated_bytes": 100, "relation_count": 1, "truncated": False}])
        return _Result([])
    @contextmanager
    def begin_nested(self): yield self


class _Database:
    def __init__(self, **kwargs): self.source = _Session(**kwargs)
    @contextmanager
    def session(self): yield self.source


def test_sample_is_read_only_deadline_bounded_and_does_not_open_objects():
    database = _Database()
    result = capacity.sample_storage_operating_metrics(sampled_at=NOW, database=database)
    statements = database.source.statements
    assert statements[0][0] == "SET TRANSACTION READ ONLY"
    assert statements[1][0] == "SET LOCAL lock_timeout='250ms'"
    timeouts = [params["timeout"] for sql, params in statements if "set_config('statement_timeout'" in sql]
    assert len(timeouts) == 3 and all(0 < int(value) <= 5000 for value in timeouts)
    assert result["operating"]["admitted_revisions_state"] == "unavailable"
    allocation = result["allocation"][0]
    assert allocation["tablespace"] == "pg_default" and allocation["allocation_kind"] == "index"
    assert allocation["basis"] == "postgres_main_fork_allocation_not_filesystem_delta"
    assert "fact_versions" not in str(capacity._STORAGE_OPERATING_QUERY)


def test_query_failure_is_unavailable_and_never_leaks_sql_or_creates_zero_counts():
    result = capacity.sample_storage_operating_metrics(sampled_at=NOW, database=_Database(fail_operating=True))
    assert result["operating"]["state"] == "unavailable"
    assert "published_rows_window" not in result["operating"]
    assert "private" not in json.dumps(result)


def test_optional_allocation_failure_preserves_valid_retention_observation():
    result = capacity.sample_storage_operating_metrics(sampled_at=NOW, database=_Database(fail_allocation=True))
    assert result["operating"]["backlog_rows"] == 90
    assert result["operating"]["allocation_state"] == "unavailable"
    assert result["allocation"] == []


def test_running_phase_age_is_bounded_metadata_and_error_text_is_not_exported():
    result = capacity._worker_phase_payload([{"state": "idle", "heartbeat_at": NOW,
        "phase": {"name": "local_recovery", "started_at": "2026-01-20T11:00:00+00:00"},
        "error_prefix": "RuntimeError: private SQL values"}], NOW)
    assert result["worker_phase"] == "local_recovery" and result["worker_phase_age_seconds"] == 3600
    assert result["worker_last_error_code"] == "RuntimeError"
    assert "private" not in json.dumps(result)


def test_ambiguous_or_future_worker_clocks_remain_unknown():
    assert capacity._worker_phase_payload([{}, {}], NOW)["worker_phase_age_seconds"] is None
    assert capacity._worker_phase_payload([{"state": "idle", "heartbeat_at": NOW,
        "phase": {"name": "local_recovery", "started_at": "2026-01-21T12:00:00+00:00"}}], NOW)["worker_phase"] == "unknown"
