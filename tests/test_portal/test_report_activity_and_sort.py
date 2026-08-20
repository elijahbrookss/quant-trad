from __future__ import annotations

from contextlib import contextmanager
from typing import Any, Iterator

from portal.backend.service.reports import contract, report_data
from portal.backend.service.storage.repos import report_materializations as materializations_repo
from portal.backend.service.storage.repos import runs as runs_repo


def _run(**overrides: Any) -> dict[str, Any]:
    data: dict[str, Any] = {
        "run_id": "run-default",
        "bot_id": "bot-1",
        "bot_name": "Bot",
        "strategy_id": "strategy-1",
        "strategy_name": "Strategy",
        "symbols": ["BTCUSD"],
        "timeframe": "1h",
        "backtest_start": None,
        "backtest_end": None,
        "started_at": "2026-01-01T00:00:00Z",
        "ended_at": "2026-01-01T00:00:00Z",
        "status": "completed",
        "summary": {},
    }
    data.update(overrides)
    return data


def _install_list_runs(monkeypatch, runs: list[dict[str, Any]]) -> None:
    monkeypatch.setattr(report_data, "list_report_catalog_candidates", lambda **_kwargs: [dict(r) for r in runs])
    monkeypatch.setattr(
        report_data,
        "list_report_catalog_details",
        lambda run_ids: {str(run_id): {} for run_id in run_ids},
    )
    monkeypatch.setattr(
        report_data,
        "list_report_materialization_statuses",
        lambda run_ids: {str(run_id): {} for run_id in run_ids},
    )


def _run_ids(payload: dict[str, Any]) -> list[str]:
    return [item["run_id"] for item in payload["items"]]


# ---------------------------------------------------------------------------
# sort param on list_report_summaries
# ---------------------------------------------------------------------------


def test_list_report_summaries_default_sort_is_unchanged_most_recent_first(monkeypatch) -> None:
    _install_list_runs(
        monkeypatch,
        [
            _run(run_id="run-a", ended_at="2026-01-03T00:00:00Z"),
            _run(run_id="run-b", ended_at="2026-01-01T00:00:00Z"),
            _run(run_id="run-c", ended_at="2026-01-02T00:00:00Z"),
        ],
    )

    payload = contract.list_report_summaries()

    assert _run_ids(payload) == ["run-a", "run-c", "run-b"]


def test_list_report_summaries_sort_net_pnl_desc_orders_by_value_with_none_last(monkeypatch) -> None:
    _install_list_runs(
        monkeypatch,
        [
            _run(run_id="run-a", ended_at="2026-01-03T00:00:00Z", summary={"net_pnl": 10.0}),
            _run(run_id="run-b", ended_at="2026-01-01T00:00:00Z", summary={"net_pnl": 30.0}),
            _run(run_id="run-c", ended_at="2026-01-02T00:00:00Z", summary={"net_pnl": None}),
            _run(run_id="run-d", ended_at="2026-01-05T00:00:00Z", summary={"net_pnl": 30.0}),
        ],
    )

    payload = contract.list_report_summaries(sort="net_pnl_desc")

    # run-d and run-b tie on net_pnl=30 -> broken by ended_at desc (run-d is later).
    # run-a is next (net_pnl=10). run-c has no net_pnl -> sorts last regardless of ended_at.
    assert _run_ids(payload) == ["run-d", "run-b", "run-a", "run-c"]


def test_list_report_summaries_sort_treats_invalid_or_non_finite_metrics_as_missing(monkeypatch) -> None:
    _install_list_runs(
        monkeypatch,
        [
            _run(run_id="valid", summary={"net_pnl": 5.0}),
            _run(run_id="text", summary={"net_pnl": "not-a-number"}),
            _run(run_id="infinite", summary={"net_pnl": float("inf")}),
        ],
    )

    payload = contract.list_report_summaries(sort="net_pnl_desc")

    assert _run_ids(payload)[0] == "valid"
    assert set(_run_ids(payload)[1:]) == {"text", "infinite"}


def test_report_catalog_readiness_requires_fresh_materialized_proof(monkeypatch) -> None:
    _install_list_runs(
        monkeypatch,
        [_run(run_id="run-ready", summary={"net_pnl": 5.0, "total_trades": 2})],
    )
    monkeypatch.setattr(
        report_data,
        "list_report_catalog_details",
        lambda _run_ids: {
            "run-ready": {
                "dataset_id": "dataset-1",
                "dataset_hash": "dataset-hash-1",
                "execution_mode": "fast",
            }
        },
    )
    monkeypatch.setattr(
        report_data,
        "list_report_materialization_statuses",
        lambda _run_ids: {
            "run-ready": {
                "status": "ready",
                "can_view": True,
                "freshness_verified": False,
                "artifact_readiness": {
                    "dataset_ready": True,
                    "results_ready": True,
                    "safe_to_compare": True,
                    "reason": "ready",
                },
            }
        },
    )

    payload = contract.list_report_summaries()

    assert payload["items"][0]["readiness"] == {
        "dataset_ready": False,
        "results_ready": False,
        "safe_to_compare": False,
        "reason": "report_freshness_unverified",
        "dataset_status": "blocked",
    }


def test_compact_report_status_projection_preserves_exact_artifact_readiness() -> None:
    query = materializations_repo._status_projection_query(["run-ready"])
    selected_keys = {str(column.key) for column in query.selected_columns}
    assert "artifact" not in selected_keys
    row = {
        "run_id": "run-ready",
        "status": "ready",
        "contract_version": materializations_repo.REPORT_CONTRACT_VERSION,
        "report_schema_version": materializations_repo.REPORT_SCHEMA_VERSION,
        "dataset_schema_version": materializations_repo.REPORT_DATASET_SCHEMA_VERSION,
        "builder_source_revision": materializations_repo.source_revision(),
        "storage_schema_version": materializations_repo.REPORT_MATERIALIZATION_STORAGE_SCHEMA_VERSION,
        "artifact_valid": True,
        "artifact_dataset_ready": True,
        "artifact_results_ready": True,
        "artifact_safe_to_compare": True,
        "artifact_readiness_reason": "ready",
        "artifact_dataset_status": "ready",
        "artifact_results_status": "ready",
        "artifact_comparison_status": "ready",
        "input_fingerprint": "fingerprint-1",
    }

    status = materializations_repo._status_from_projection(
        row,
        contract_version=materializations_repo.REPORT_CONTRACT_VERSION,
        input_fingerprint="fingerprint-1",
        freshness_verified=True,
    )

    assert status["status"] == "ready"
    assert status["can_view"] is True
    assert status["freshness_verified"] is True
    assert status["artifact_readiness"]["safe_to_compare"] is True

    stale = materializations_repo._status_from_projection(
        row,
        contract_version=materializations_repo.REPORT_CONTRACT_VERSION,
        input_fingerprint="fingerprint-2",
        freshness_verified=True,
    )
    assert stale["status"] == "stale"
    assert stale["can_view"] is False
    assert stale["stale_reason"] == "input_fingerprint_changed"


def test_list_report_summaries_sort_sharpe_desc(monkeypatch) -> None:
    _install_list_runs(
        monkeypatch,
        [
            _run(run_id="run-low", summary={"sharpe": 0.4}),
            _run(run_id="run-high", summary={"sharpe": 1.8}),
            _run(run_id="run-missing", summary={}),
        ],
    )

    payload = contract.list_report_summaries(sort="sharpe_desc")

    assert _run_ids(payload) == ["run-high", "run-low", "run-missing"]


def test_list_report_summaries_sort_total_return_desc(monkeypatch) -> None:
    _install_list_runs(
        monkeypatch,
        [
            _run(run_id="run-low", summary={"total_return": 0.02}),
            _run(run_id="run-high", summary={"total_return": 0.55}),
        ],
    )

    payload = contract.list_report_summaries(sort="total_return_desc")

    assert _run_ids(payload) == ["run-high", "run-low"]


def test_list_report_summaries_sort_ties_break_on_ended_at_then_run_id(monkeypatch) -> None:
    _install_list_runs(
        monkeypatch,
        [
            _run(run_id="run-z", ended_at="2026-01-01T00:00:00Z", summary={"net_pnl": 5.0}),
            _run(run_id="run-a", ended_at="2026-01-01T00:00:00Z", summary={"net_pnl": 5.0}),
        ],
    )

    payload = contract.list_report_summaries(sort="net_pnl_desc")

    # Equal value and equal ended_at -> tertiary tiebreak is run_id desc.
    assert _run_ids(payload) == ["run-z", "run-a"]


def test_list_report_summaries_unknown_sort_falls_back_to_default(monkeypatch) -> None:
    _install_list_runs(
        monkeypatch,
        [
            _run(run_id="run-a", ended_at="2026-01-03T00:00:00Z"),
            _run(run_id="run-b", ended_at="2026-01-01T00:00:00Z"),
        ],
    )

    payload = contract.list_report_summaries(sort="not_a_real_sort_key")

    assert _run_ids(payload) == ["run-a", "run-b"]


# ---------------------------------------------------------------------------
# get_backtest_activity — zero-fill day range, completed-only totals
# ---------------------------------------------------------------------------


def test_get_backtest_activity_zero_fills_days_with_no_runs(monkeypatch) -> None:
    # Anchor the fake row to "today" so it falls inside the function's own
    # dynamically-computed [since, today] window (today - 2 days .. today).
    today = _today_utc()
    monkeypatch.setattr(
        report_data,
        "count_runs_by_day",
        lambda **_kwargs: [
            {"day": today, "status": "completed", "total": 3},
        ],
    )

    payload = contract.get_backtest_activity(days=3)

    dates = [entry["date"] for entry in payload["days"]]
    totals = {entry["date"]: entry["total"] for entry in payload["days"]}
    assert len(payload["days"]) == 3
    # The two days with no matching rows must still be present, with total 0.
    assert sum(1 for total in totals.values() if total == 0) == 2
    assert 3 in totals.values()
    assert dates == sorted(dates)


def test_get_backtest_activity_total_reflects_completed_only(monkeypatch) -> None:
    day = _today_utc()
    monkeypatch.setattr(
        report_data,
        "count_runs_by_day",
        lambda **_kwargs: [
            {"day": day, "status": "completed", "total": 4},
            {"day": day, "status": "failed", "total": 2},
        ],
    )

    payload = contract.get_backtest_activity(days=1)

    entry = payload["days"][0]
    assert entry["total"] == 4
    assert entry["by_status"] == {"completed": 4, "failed": 2}
    assert payload["qualifying_statuses"] == ["completed"]
    assert payload["timestamp_field"] == "ended_at"
    assert payload["timezone"] == "UTC"
    assert payload["activity_type"] == "backtests_completed"


def test_get_backtest_activity_bounds_days_param(monkeypatch) -> None:
    monkeypatch.setattr(report_data, "count_runs_by_day", lambda **_kwargs: [])

    payload = contract.get_backtest_activity(days=10_000)

    assert len(payload["days"]) == contract._ACTIVITY_MAX_DAYS


def _today_utc():
    from datetime import datetime, timezone

    return datetime.now(timezone.utc)


# ---------------------------------------------------------------------------
# count_bot_runs_by_day — query shape (UTC day_trunc bucketing, filters applied)
# ---------------------------------------------------------------------------


class _FakeResult:
    def all(self):
        return []


class _FakeSession:
    def __init__(self) -> None:
        self.captured_query = None

    def execute(self, query):
        self.captured_query = query
        return _FakeResult()


class _FakeDb:
    available = True

    def __init__(self) -> None:
        self.session_instance = _FakeSession()

    @contextmanager
    def session(self) -> Iterator[_FakeSession]:
        yield self.session_instance


def test_count_bot_runs_by_day_query_buckets_by_ended_at_and_applies_filters(monkeypatch) -> None:
    fake_db = _FakeDb()
    monkeypatch.setattr(runs_repo, "db", fake_db)

    runs_repo.count_bot_runs_by_day(run_type="backtest", status="completed", since=None)

    compiled = str(fake_db.session_instance.captured_query)
    assert "date_trunc" in compiled.lower()
    assert "ended_at" in compiled.lower()
    assert "run_type" in compiled.lower()
    assert "status" in compiled.lower()
    assert "group by" in compiled.lower()


def test_count_bot_runs_by_day_returns_empty_when_db_unavailable(monkeypatch) -> None:
    class _UnavailableDb:
        available = False

    monkeypatch.setattr(runs_repo, "db", _UnavailableDb())

    assert runs_repo.count_bot_runs_by_day(run_type="backtest") == []
