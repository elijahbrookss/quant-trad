from __future__ import annotations

from contextlib import nullcontext
from typing import Any

import pytest

pytest.importorskip("fastapi")
pytest.importorskip("pandas")

import pandas as pd
from fastapi.testclient import TestClient

from portal.backend.controller import research as research_controller
from portal.backend.main import app
from portal.backend.service.research import checks, service
from portal.backend.service.research import async_dispatch as research_async_dispatch
from portal.backend.service.async_jobs import ClaimedJob
from portal.backend.service.async_jobs import EnqueuedJob
from portal.backend.workers import research_worker


def _candles() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "open": [100.0, 101.0, 102.0, 102.5, 104.0, 105.0],
            "high": [101.0, 101.4, 103.0, 103.0, 106.0, 107.0],
            "low": [99.0, 100.8, 101.8, 102.1, 103.5, 104.5],
            "close": [101.0, 101.2, 102.6, 102.8, 105.5, 106.0],
            "volume": [10.0, 9.0, 20.0, 19.0, 30.0, 32.0],
        },
        index=pd.to_datetime(
            [
                "2026-01-01T00:00:00Z",
                "2026-01-01T01:00:00Z",
                "2026-01-01T02:00:00Z",
                "2026-01-01T03:00:00Z",
                "2026-01-01T04:00:00Z",
                "2026-01-01T05:00:00Z",
            ],
            utc=True,
        ),
    )






















def test_research_worker_commits_check_artifacts_under_the_current_claim(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    job = ClaimedJob(
        id="job-1",
        job_type=research_async_dispatch.JOB_TYPE_RESEARCH_CHECK_RUN,
        payload={"request": {"title": "ATR check"}},
        attempts=1,
        max_attempts=2,
        partition_key=None,
        partition_hash=0,
        lock_owner="worker-1",
        claim_token="token-1",
        claim_generation=1,
    )
    observed: dict[str, Any] = {}

    monkeypatch.setattr(
        research_worker,
        "maintain_job_heartbeat",
        lambda claimed, **kwargs: nullcontext(),
    )
    def fake_build(request):
        observed["request"] = request
        return {"schema_version": "research_check_evidence_build.v1"}

    def fake_persist(built, *, session):
        observed["built"] = built
        observed["session"] = session
        return {"schema_version": "research_check_run.v2", "status": "completed"}

    def fake_complete(claimed, effect):
        observed["claim"] = claimed
        return dict(effect("owned-session"))

    monkeypatch.setattr(
        research_worker.research_service,
        "build_research_check_evidence",
        fake_build,
    )
    monkeypatch.setattr(
        research_worker.research_service,
        "persist_built_research_check_evidence",
        fake_persist,
    )
    monkeypatch.setattr(
        research_worker,
        "complete_job_with_owned_effect",
        fake_complete,
    )

    result = research_worker.execute_claimed_research_job(job)

    assert result["status"] == "completed"
    assert observed == {
        "request": {"title": "ATR check"},
        "built": {"schema_version": "research_check_evidence_build.v1"},
        "session": "owned-session",
        "claim": job,
    }










































def test_active_run_research_evidence_does_not_materialize_growing_dataset(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    dataset_calls = 0

    def _dataset(_run_id):
        nonlocal dataset_calls
        dataset_calls += 1
        raise AssertionError("active evidence must not build the research dataset")

    monkeypatch.setattr(
        service.runs_repo,
        "get_bot_run",
        lambda run_id: {
            "run_id": run_id,
            "bot_id": "bot-1",
            "status": "running",
            "config_snapshot": {
                "strategy_id": "strategy-1",
                "symbols": ["BTCUSDT"],
                "timeframe": "5m",
            },
            "summary": {"total_trades": 7, "open_trades": 1},
        },
    )
    monkeypatch.setattr(
        service.lifecycle_repo,
        "get_bot_run_lifecycle",
        lambda _run_id: {"status": "running", "phase": "live"},
    )
    monkeypatch.setattr(service.reports_contract, "get_run_research_dataset", _dataset)

    payload = service.get_run_research_evidence("run-1")

    assert payload["readiness"]["dataset_status"] == "deferred_while_run_active"
    assert payload["readiness"]["safe_to_compare"] is False
    assert payload["counts"]["trades"] == 7
    assert dataset_calls == 0


def test_run_research_evidence_summarizes_checkable_report_fields(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        service.reports_contract,
        "get_run_research_dataset",
        lambda run_id: {
            "metadata": {
                "run_id": run_id,
                "bot_id": "bot-1",
                "strategy_id": "strategy-1",
                "symbols": ["BTCUSDT"],
                "instrument_ids": ["inst-btc"],
                "timeframe": "5m",
                "simulated_window": {"start": "2026-01-01T00:00:00Z", "end": "2026-01-02T00:00:00Z"},
            },
            "readiness": {"dataset_status": "ready", "safe_to_compare": True, "caveats": []},
            "summary": {"accepted_decisions": 1, "rejected_decisions": 1, "closed_trades": 1, "open_trades": 0},
            "signals": [
                {
                    "signal_id": "sig-1",
                    "symbol": "BTCUSDT",
                    "context": {"event_key": "confirmed_balance_breakout_long"},
                    "indicator_context": {
                        "outputs": {
                            "profile-1.confirmed_balance_breakout": {
                                "output_name": "confirmed_balance_breakout",
                                "event_keys": ["confirmed_balance_breakout_long"],
                            }
                        }
                    },
                }
            ],
            "decisions": [
                {"decision_id": "decision-1", "decision_state": "accepted", "reason_code": "ENTRY"},
                {"decision_id": "decision-2", "decision_state": "rejected", "reason_code": "FILTER"},
            ],
            "trades": [{"id": "trade-1", "decision_id": "decision-1"}],
        },
    )

    payload = service.get_run_research_evidence("run-1")

    assert payload["schema_version"] == "run_research_evidence.v1"
    assert payload["counts"]["signals"] == 1
    assert payload["signals"]["output_names"] == {"confirmed_balance_breakout": 1}
    assert payload["signals"]["event_keys"] == {"confirmed_balance_breakout_long": 1}
    assert payload["decisions"]["states"] == {"accepted": 1, "rejected": 1}
    assert payload["supported_checks"] == []


def test_research_trail_collects_related_items_and_runs(monkeypatch: pytest.MonkeyPatch) -> None:
    items = {
        "obs-1": {"id": "obs-1", "kind": "observation", "title": "Observation", "payload": {}},
        "check-1": {
            "id": "check-1",
            "kind": "research_check",
            "title": "Check",
            "payload": {"result": {"check_family": "run_signal_summary", "sample_count": 1}},
        },
    }
    links = [
        {
            "id": "link-1",
            "source_item_id": "check-1",
            "target_type": "research_item",
            "target_id": "obs-1",
            "relation": "tests",
            "metadata": {},
        },
        {
            "id": "link-2",
            "source_item_id": "check-1",
            "target_type": "run",
            "target_id": "run-1",
            "relation": "analyzes",
            "metadata": {},
        },
    ]
    links_by_item = {
        "obs-1": [links[0]],
        "check-1": links,
    }

    monkeypatch.setattr(service.repository, "get_item", lambda item_id: items[item_id])
    monkeypatch.setattr(
        service.repository,
        "list_links",
        lambda item_id, include_inbound=True: links_by_item[item_id],
    )
    monkeypatch.setattr(
        service,
        "_run_evidence_summary",
        lambda run_id: {"schema_version": "run_research_evidence.v1", "run_id": run_id},
    )

    payload = service.get_research_trail("obs-1")

    assert payload["schema_version"] == "research_trail.v1"
    assert payload["item"]["id"] == "obs-1"
    assert payload["checks"][0]["id"] == "check-1"
    assert payload["runs"] == [{"schema_version": "run_research_evidence.v1", "run_id": "run-1"}]
    assert {link["id"] for link in payload["links"]} == {"link-1", "link-2"}
    assert payload["summary"]["check_count"] == 1


def test_research_check_compare_uses_emitted_forward_metrics(monkeypatch: pytest.MonkeyPatch) -> None:
    items = {
        "left": {
            "id": "left",
            "kind": "research_check",
            "title": "Left",
            "payload": {
                "result": {
                    "check_family": "indicator_forward_outcome",
                    "status": "completed",
                    "sample_count": 2,
                    "outcomes": {
                        "summary": {
                            "1": {
                                "median_forward_return_pct": 0.01,
                                "positive_rate": 0.5,
                                "mean_mfe_pct": 0.02,
                                "ignored_label": "left",
                            }
                        }
                    },
                }
            },
        },
        "right": {
            "id": "right",
            "kind": "research_check",
            "title": "Right",
            "payload": {
                "result": {
                    "check_family": "indicator_forward_outcome",
                    "status": "completed",
                    "sample_count": 5,
                    "outcomes": {
                        "summary": {
                            "1": {
                                "median_forward_return_pct": 0.03,
                                "positive_rate": 0.75,
                                "mean_mfe_pct": 0.04,
                                "ignored_label": "right",
                            }
                        }
                    },
                }
            },
        },
    }
    monkeypatch.setattr(service.repository, "get_item", lambda item_id: items[item_id])

    payload = service.compare_research_checks("left", "right")

    assert payload["schema_version"] == "research_check_comparison.v1"
    assert payload["deltas"]["sample_count"] == {"left": 2, "right": 5, "delta": 3.0}
    assert payload["deltas"]["forward_summary"]["1"]["median_forward_return_pct"]["delta"] == 0.019999999999999997
    assert payload["deltas"]["forward_summary"]["1"]["positive_rate"]["delta"] == 0.25
    assert payload["deltas"]["forward_summary"]["1"]["mean_mfe_pct"]["delta"] == 0.02
    assert "ignored_label" not in payload["deltas"]["forward_summary"]["1"]


def test_research_check_compare_includes_decision_denominator(monkeypatch: pytest.MonkeyPatch) -> None:
    items = {
        "left": {
            "id": "left",
            "kind": "research_check",
            "title": "Left",
            "payload": {
                "result": {
                    "check_family": "run_decision_trade_comparison",
                    "status": "completed",
                    "sample_count": 2,
                    "eligible_decisions": 10,
                    "outcomes": {"by_decision_state": {}},
                }
            },
        },
        "right": {
            "id": "right",
            "kind": "research_check",
            "title": "Right",
            "payload": {
                "result": {
                    "check_family": "run_decision_trade_comparison",
                    "status": "completed",
                    "sample_count": 3,
                    "eligible_decisions": 12,
                    "outcomes": {"by_decision_state": {}},
                }
            },
        },
    }
    monkeypatch.setattr(service.repository, "get_item", lambda item_id: items[item_id])

    payload = service.compare_research_checks("left", "right")

    assert payload["left"]["eligible_decisions"] == 10
    assert payload["right"]["eligible_decisions"] == 12
    assert payload["deltas"]["eligible_decisions"] == {"left": 10, "right": 12, "delta": 2.0}


def test_research_check_route_delegates_to_service(monkeypatch: pytest.MonkeyPatch) -> None:
    observed = {}

    def fake_run_research_check(payload):
        observed["payload"] = payload
        return {"schema_version": "research_check_run.v1", "status": "completed"}

    monkeypatch.setattr(research_controller.research_service, "run_research_check", fake_run_research_check)

    response = TestClient(app).post(
        "/api/research/checks/run",
        json={
            "title": "Quick check",
            "scope": {"instrument_id": "inst-1", "timeframe": "1h", "start": "2026-01-01T00:00:00Z", "end": "2026-01-02T00:00:00Z"},
            "detector": {"type": "raw_condition", "field": "close", "operator": "gt", "value_field": "previous_close"},
        },
    )

    assert response.status_code == 201
    assert response.json()["status"] == "completed"
    assert observed["payload"]["title"] == "Quick check"


def test_research_check_preview_and_sweep_routes_delegate_to_service(monkeypatch: pytest.MonkeyPatch) -> None:
    observed = {}

    def fake_evaluate_research_check(payload):
        observed["evaluate"] = payload
        return {"schema_version": "research_check_evaluation.v1", "status": "completed"}

    def fake_sweep_research_checks(payload):
        observed["sweep"] = payload
        return {"schema_version": "research_check_sweep.v1", "evaluation_count": 1}

    monkeypatch.setattr(research_controller.research_service, "evaluate_research_check", fake_evaluate_research_check)
    monkeypatch.setattr(research_controller.research_service, "sweep_research_checks", fake_sweep_research_checks)

    client = TestClient(app)
    evaluate_response = client.post(
        "/api/research/checks/evaluate",
        json={
            "title": "Preview check",
            "scope": {"instrument_id": "inst-1", "timeframe": "1h", "start": "2026-01-01T00:00:00Z", "end": "2026-01-02T00:00:00Z"},
            "detector": {"type": "raw_condition", "field": "close", "operator": "gt", "value_field": "previous_close"},
        },
    )
    sweep_response = client.post(
        "/api/research/checks/sweep",
        json={
            "check_family": "candidate_lifecycle",
            "scope": {"indicator_id": "indicator-1", "instrument_id": "inst-1", "timeframe": "1h", "start": "2026-01-01T00:00:00Z", "end": "2026-01-02T00:00:00Z"},
            "detector": {"type": "candidate_lifecycle"},
            "variants": [{"id": "base", "param_overrides": {}}],
            "ranking": {"rank_by": "sample_count", "direction": "desc"},
        },
    )

    assert evaluate_response.status_code == 200
    assert sweep_response.status_code == 200
    assert observed["evaluate"]["title"] == "Preview check"
    assert observed["sweep"]["check_family"] == "candidate_lifecycle"


def test_research_job_routes_delegate_to_async_dispatch(monkeypatch: pytest.MonkeyPatch) -> None:
    observed = {}

    def fake_dispatch(payload):
        observed["dispatch"] = payload
        return {"schema_version": "research_job_dispatch.v1", "job_id": "job-1", "status": "queued"}

    def fake_status(job_id: str):
        observed["status"] = job_id
        return {"schema_version": "research_job_status.v1", "job_id": job_id, "status": "queued"}

    def fake_result(job_id: str):
        observed["result"] = job_id
        return {
            "schema_version": "research_job_status.v1",
            "job_id": job_id,
            "status": "succeeded",
            "result": {"schema_version": "research_check_sweep.v1", "evaluation_count": 1},
        }

    monkeypatch.setattr(research_controller.research_async_dispatch, "dispatch_research_check_sweep", fake_dispatch)
    monkeypatch.setattr(research_controller.research_async_dispatch, "get_research_job_status", fake_status)
    monkeypatch.setattr(research_controller.research_async_dispatch, "get_research_job_result", fake_result)

    dispatch_response = research_controller.dispatch_research_check_sweep(
        research_controller.ResearchCheckSweepRequest(
            **{
                "check_family": "candidate_lifecycle",
                "scope": {
                    "indicator_id": "indicator-1",
                    "instrument_id": "inst-1",
                    "timeframe": "1h",
                    "start": "2026-01-01T00:00:00Z",
                    "end": "2026-01-02T00:00:00Z",
                },
                "detector": {"type": "candidate_lifecycle"},
                "variants": [{"id": "base", "param_overrides": {}}],
                "ranking": {"rank_by": "sample_count", "direction": "desc"},
            }
        )
    )
    status_response = research_controller.get_research_job_status("job-1")
    result_response = research_controller.get_research_job_result("job-1")

    assert dispatch_response["job_id"] == "job-1"
    assert status_response["status"] == "queued"
    assert result_response["result"]["schema_version"] == "research_check_sweep.v1"
    assert observed["dispatch"]["check_family"] == "candidate_lifecycle"
    assert observed["status"] == "job-1"
    assert observed["result"] == "job-1"


def test_research_pass_gate_route_delegates_to_owned_evaluator(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observed = {}

    def fake_evaluate(payload):
        observed["payload"] = payload
        return {
            "schema_version": "pass_gate_result.v2",
            "status": "PASSED",
            "gates": [],
        }

    monkeypatch.setattr(
        research_controller.research_pass_gates,
        "evaluate_pass_gate_request",
        fake_evaluate,
    )
    response = TestClient(app).post(
        "/api/research/comparisons/pass-gates/evaluate",
        json={
            "plan": {"intent": "exploration", "pass_gates": {"gates": []}},
            "summaries": [
                {
                    "window_id": "w1",
                    "variant_id": "candidate",
                    "summary": {"metrics": {"trade_count": 3}},
                }
            ],
            "comparison_refs": [],
        },
    )

    assert response.status_code == 200
    assert response.json()["status"] == "PASSED"
    assert observed["payload"]["summaries"][0]["variant_id"] == "candidate"


def test_research_read_routes_delegate_to_service_exports(monkeypatch: pytest.MonkeyPatch) -> None:
    observed = {}

    async def fake_run_in_threadpool(function, *args, **kwargs):
        observed["threadpool_function"] = function
        observed["threadpool_args"] = args
        return function(*args, **kwargs)

    def fake_run_research_evidence(run_id: str):
        observed["run_id"] = run_id
        return {"schema_version": "run_research_evidence.v1", "run_id": run_id}

    def fake_research_trail(item_id: str):
        observed["trail_item_id"] = item_id
        return {"schema_version": "research_trail.v1", "item_id": item_id, "items": []}

    def fake_compare(left_check_id: str, right_check_id: str):
        observed["compare"] = (left_check_id, right_check_id)
        return {
            "schema_version": "research_check_comparison.v1",
            "left": {"check_id": left_check_id},
            "right": {"check_id": right_check_id},
            "deltas": {},
        }

    monkeypatch.setattr(research_controller.research_service, "get_run_research_evidence", fake_run_research_evidence)
    monkeypatch.setattr(research_controller, "run_in_threadpool", fake_run_in_threadpool)
    monkeypatch.setattr(research_controller.research_service, "get_research_trail", fake_research_trail)
    monkeypatch.setattr(research_controller.research_service, "compare_research_checks", fake_compare)

    client = TestClient(app)

    run_response = client.get("/api/research/runs/run-1/evidence")
    trail_response = client.get("/api/research/items/obs-1/trail")
    compare_response = client.get(
        "/api/research/checks/compare",
        params={"left_check_id": "check-a", "right_check_id": "check-b"},
    )

    assert run_response.status_code == 200
    assert run_response.json()["run_id"] == "run-1"
    assert trail_response.status_code == 200
    assert trail_response.json()["item_id"] == "obs-1"
    assert compare_response.status_code == 200
    assert compare_response.json()["left"]["check_id"] == "check-a"
    assert observed == {
        "run_id": "run-1",
        "threadpool_function": fake_run_research_evidence,
        "threadpool_args": ("run-1",),
        "trail_item_id": "obs-1",
        "compare": ("check-a", "check-b"),
    }


@pytest.mark.parametrize(
    ("route", "service_name", "item_request"),
    [
        ("/api/research/items", "create_research_item", {"kind": "study", "title": "Episode review"}),
        ("/api/research/checks/check-1/observations", "create_observation_from_check_evidence", {}),
    ],
)
@pytest.mark.parametrize("body", [None, "é" * 8192, "é" * 8193], ids=["null", "at-limit", "over-limit"])
def test_research_body_limit_rejects_before_service_without_truncation(
    monkeypatch: pytest.MonkeyPatch, route: str, service_name: str,
    item_request: dict[str, Any], body: str | None,
) -> None:
    received = []

    def capture(*args):
        received.append(args[-1])
        return {"id": "saved", "body": args[-1].get("body")}

    monkeypatch.setattr(research_controller.research_service, service_name, capture)
    response = TestClient(app).post(route, json={**item_request, "body": body})

    if body is not None and len(body) > 8192:
        assert response.status_code == 422
        assert received == []
        error = response.json()["detail"][0]
        assert error["loc"] == ["body", "body"]
        assert error["ctx"]["max_length"] == 8192
    else:
        assert response.status_code == 201
        assert received[0]["body"] == body
        assert response.json()["body"] == body


def test_research_check_compare_preserves_frozen_descriptive_summary_and_clocks(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    summaries = {
        "left": {
            "schema_version": "eligible_population_descriptive.v1",
            "population": "eligible_events_before_feature_complete_case_filter",
            "population_count": 2,
            "inference": "descriptive_only_no_significance_or_execution_claim",
            "horizons": {"24": {"mean_direction_signed_return": None, "resolved_count": 0, "unresolved_count": 2}},
        },
        "right": {
            "schema_version": "eligible_population_descriptive.v1",
            "population": "eligible_events_before_feature_complete_case_filter",
            "population_count": 1,
            "inference": "descriptive_only_no_significance_or_execution_claim",
            "horizons": {"24": {"mean_direction_signed_return": -0.01, "resolved_count": 1, "unresolved_count": 0}},
        },
    }
    resolutions = {
        "left": {"24": {"horizon": 24, "horizon_kind": "bars", "resolved_count": 0, "unresolved_count": 2}},
        "right": {"24": {"horizon": 24, "horizon_kind": "seconds", "resolved_count": 1, "unresolved_count": 0}},
    }
    items = {
        key: {
            "id": key, "kind": "research_check", "timeframe": "5m",
            "payload": {"result": {"schema_version": "research.check_result.v2", "result": {
                "check_family": "event_fact_analysis", "status": "completed",
                "sample_count": summary["population_count"],
                "descriptive_outcomes": summary, "outcome_resolution": resolutions[key],
            }}},
        }
        for key, summary in summaries.items()
    }
    reads = []

    def get_item(item_id):
        reads.append(item_id)
        return items[item_id]

    monkeypatch.setattr(service.repository, "get_item", get_item)
    result = service.compare_research_checks("left", "right")

    assert reads == ["left", "right"]
    for key in items:
        assert result[key]["descriptive_outcomes"] == summaries[key]
        assert result[key]["outcome_resolution"] == resolutions[key]
        assert result[key]["timeframe"] == "5m"
    # Distinct clocks/populations and unresolved values remain visible; this
    # projection does not manufacture matched statistics or legacy deltas.
    assert result["deltas"]["forward_summary"] == {}
    assert result["left"]["outcomes"] == {}
    assert result["right"]["outcomes"] == {}


def test_retired_sweeps_refuse_before_job_creation(monkeypatch):
    def forbidden(**kwargs): raise AssertionError("must not enqueue retired sweep")
    monkeypatch.setattr(research_async_dispatch,"enqueue_or_reuse_job",forbidden)
    with pytest.raises(ValueError,match="check_definition_retired"):
        research_async_dispatch.dispatch_research_check_sweep({"check_family":"signal_audit"})
    with pytest.raises(ValueError,match="check_definition_retired"):
        service.sweep_research_checks({"check_family":"signal_audit"})
