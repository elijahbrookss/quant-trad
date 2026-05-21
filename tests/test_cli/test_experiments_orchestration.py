from __future__ import annotations

import json
from pathlib import Path
import urllib.parse
import urllib.request

from cli.experiments.pass_gates import evaluate_pass_gates
from cli.main import main


class _Response:
    status = 200

    def __init__(self, body: bytes, headers: dict[str, str] | None = None) -> None:
        self._body = body
        self.headers = headers or {}

    def __enter__(self) -> "_Response":
        return self

    def __exit__(self, *_args: object) -> None:
        return None

    def read(self) -> bytes:
        return self._body


def _plan_text() -> str:
    return """
schema_version: experiment_plan.v1
name: range-contraction-fresh-window-validation
hypothesis: Candidate should keep enough trades while improving drawdown.
run_policy:
  mode: sequential
  stop_on_first_failure: false
  poll_interval_seconds: 0.01
  run_timeout_seconds: 1
windows:
  - id: window_a
    start: "2026-01-01T00:00:00Z"
    end: "2026-01-31T23:59:59Z"
variants:
  - id: baseline
    bot_id: bot-base
    expected_strategy_variant: confirmed-breakout-expanding-only
  - id: candidate
    bot_id: bot-candidate
    expected_strategy_variant: expanding-range-contraction-max-3
comparisons:
  - id: candidate_vs_baseline
    baseline_variant_id: baseline
    candidate_variant_id: candidate
    compare_per_window: true
    aggregate_summary: true
pass_gates:
  max_drawdown_pct: 15.0
  min_trade_count_per_window: 25
  min_trade_count_ratio_vs_baseline: 0.50
  min_windows_with_pf_gt_1: 1
notification_policy:
  enabled: true
  sinks: [file]
"""


def test_experiments_validate_plan_prints_step_preview(tmp_path, capsys):
    plan_path = tmp_path / "plan.yaml"
    plan_path.write_text(_plan_text(), encoding="utf-8")

    exit_code = main(["--log-root", str(tmp_path), "experiments", "validate-plan", str(plan_path), "--skip-data-preflight"])

    out = json.loads(capsys.readouterr().out)
    assert exit_code == 0
    assert out["schema_version"] == "experiment_plan_preview.v1"
    assert out["plan"]["schema_version"] == "experiment_plan.v1"
    assert [step["type"] for step in out["steps"]].count("RUN_BOT") == 2
    assert out["steps"][-1]["type"] == "NOTIFY"


def test_experiments_run_plan_writes_state_events_artifacts_and_pass_gates(tmp_path, monkeypatch):
    plan_path = tmp_path / "plan.yaml"
    plan_path.write_text(_plan_text(), encoding="utf-8")
    calls: list[tuple[str, str]] = []

    def fake_urlopen(request, timeout):
        _ = timeout
        parsed = urllib.parse.urlparse(request.full_url)
        calls.append((request.get_method(), parsed.path))
        method = request.get_method()
        path = parsed.path
        if method == "GET" and path in {"/api/bots/bot-base/run-context", "/api/bots/bot-candidate/run-context"}:
            variant_name = (
                "confirmed-breakout-expanding-only"
                if path == "/api/bots/bot-base/run-context"
                else "expanding-range-contraction-max-3"
            )
            return _Response(
                json.dumps(
                    {
                        "schema_version": "bot_run_context.v1",
                        "strategy": {"strategy_variant_name": variant_name},
                        "execution": {"backtest_start": "old", "backtest_end": "old"},
                    }
                ).encode("utf-8")
            )
        if method == "POST" and path in {"/api/bots/bot-base/data-preflight", "/api/bots/bot-candidate/data-preflight"}:
            bot_id = path.split("/")[3]
            symbol = "BTC/USD" if bot_id == "bot-base" else "ETH/USD"
            return _Response(
                json.dumps(
                    {
                        "schema_version": "bot_data_preflight.v1",
                        "bot_id": bot_id,
                        "strategy": {
                            "strategy_variant_name": (
                                "confirmed-breakout-expanding-only"
                                if bot_id == "bot-base"
                                else "expanding-range-contraction-max-3"
                            )
                        },
                        "execution": {"provider": "test", "exchange": "paper", "timeframe": "1h"},
                        "status": "ok",
                        "checks": [
                            {
                                "schema_version": "candle_coverage_preflight.v1",
                                "symbol": symbol,
                                "provider": "test",
                                "exchange": "paper",
                                "timeframe": "1h",
                                "status": "ok",
                                "severity": "ok",
                                "row_count": 100,
                                "missing_ranges": [],
                            }
                        ],
                    }
                ).encode("utf-8")
            )
        if method == "PUT" and path in {"/api/bots/bot-base", "/api/bots/bot-candidate"}:
            body = json.loads(request.data.decode("utf-8"))
            assert body == {"backtest_start": "2026-01-01T00:00:00Z", "backtest_end": "2026-01-31T23:59:59Z"}
            return _Response(b'{"schema_version":"bot_response.v1"}')
        if method == "POST" and path in {"/api/bots/bot-base/runs/start", "/api/bots/bot-candidate/runs/start"}:
            run_id = "run-base" if "bot-base" in path else "run-candidate"
            return _Response(json.dumps({"schema_version": "bot_run_start.v1", "run_id": run_id, "status": "starting"}).encode("utf-8"))
        if method == "GET" and path in {"/api/bots/bot-base/runs/run-base/status", "/api/bots/bot-candidate/runs/run-candidate/status"}:
            run_id = path.split("/")[-2]
            return _Response(json.dumps({"schema_version": "bot_run_status.v1", "run_id": run_id, "status": "completed"}).encode("utf-8"))
        if method == "POST" and path in {"/api/reports/run-base/export", "/api/reports/run-candidate/export"}:
            run_id = path.split("/")[3]
            return _Response(b"zip-bytes", headers={"content-disposition": f'attachment; filename="{run_id}.zip"'})
        if method == "POST" and path in {"/api/reports/run-base/run-report/build", "/api/reports/run-candidate/run-report/build"}:
            run_id = path.split("/")[3]
            return _Response(json.dumps({"schema_version": "run_report_materialization_status.v1", "run_id": run_id, "report_status": {"status": "ready"}}).encode("utf-8"))
        if method == "GET" and path in {"/api/reports/run-base/research-summary", "/api/reports/run-candidate/research-summary"}:
            run_id = path.split("/")[3]
            metrics = (
                {"trade_count": 50, "profit_factor": 0.9, "max_drawdown_pct": 12.0}
                if run_id == "run-base"
                else {"trade_count": 30, "profit_factor": 1.2, "max_drawdown_pct": 10.0}
            )
            return _Response(json.dumps({"schema_version": "run_research_summary.v1", "metrics": metrics}).encode("utf-8"))
        if method == "GET" and path == "/api/reports/compare/summary":
            return _Response(
                b'{"schema_version":"run_report_comparison_summary.v1","left_run_id":"run-base","right_run_id":"run-candidate","comparison_status":"ready"}'
            )
        raise AssertionError(f"unexpected API call: {method} {request.full_url}")

    monkeypatch.setattr(urllib.request, "urlopen", fake_urlopen)

    exit_code = main(
        [
            "--log-root",
            str(tmp_path),
            "experiments",
            "run-plan",
            str(plan_path),
            "--experiment-id",
            "exp-1",
        ]
    )

    state_path = next((tmp_path / "experiments").glob("**/exp-1/state.json"))
    state = json.loads(state_path.read_text(encoding="utf-8"))
    result_path = Path(state["pass_gate_result_ref"])
    pass_result = json.loads(result_path.read_text(encoding="utf-8"))
    event_path = state_path.parent / "events.ndjson"
    assert exit_code == 0
    assert state["status"] == "COMPLETED"
    assert pass_result["status"] == "PASSED"
    assert event_path.exists()
    assert any(path.endswith("/api/reports/compare/summary") for _method, path in calls)
    assert list((state_path.parent / "artifacts" / "reports").glob("**/*.zip"))

    events_code = main(["--log-root", str(tmp_path), "experiments", "events", "exp-1", "--tail", "2"])
    doctor_code = main(["--log-root", str(tmp_path), "experiments", "doctor", "exp-1"])
    assert events_code == 0
    assert doctor_code == 0


def test_experiments_run_plan_requires_explicit_proceed_for_data_warnings(tmp_path, monkeypatch):
    plan_path = tmp_path / "plan.yaml"
    plan_path.write_text(_plan_text(), encoding="utf-8")

    def fake_urlopen(request, timeout):
        _ = timeout
        parsed = urllib.parse.urlparse(request.full_url)
        if request.get_method() == "POST" and parsed.path in {"/api/bots/bot-base/data-preflight", "/api/bots/bot-candidate/data-preflight"}:
            bot_id = parsed.path.split("/")[3]
            return _Response(
                json.dumps(
                    {
                        "schema_version": "bot_data_preflight.v1",
                        "bot_id": bot_id,
                        "strategy": {
                            "strategy_variant_name": (
                                "confirmed-breakout-expanding-only"
                                if bot_id == "bot-base"
                                else "expanding-range-contraction-max-3"
                            )
                        },
                        "status": "warning",
                        "checks": [
                            {
                                "schema_version": "candle_coverage_preflight.v1",
                                "symbol": "BTC/USD",
                                "provider": "test",
                                "exchange": "paper",
                                "timeframe": "1h",
                                "status": "warning",
                                "severity": "warning",
                                "missing_ranges": [{"start": "2026-01-01T00:00:00Z", "end": "2026-01-02T00:00:00Z"}],
                            }
                        ],
                    }
                ).encode("utf-8")
            )
        raise AssertionError(f"unexpected API call after warning preflight: {request.get_method()} {request.full_url}")

    monkeypatch.setattr(urllib.request, "urlopen", fake_urlopen)

    exit_code = main(["--log-root", str(tmp_path), "experiments", "run-plan", str(plan_path)])

    assert exit_code == 2
    assert not list((tmp_path / "experiments").glob("**/state.json"))


def test_pass_gates_resolve_trade_count_aliases():
    plan = {
        "windows": [{"id": "window_a"}],
        "comparisons": [{"baseline_variant_id": "baseline", "candidate_variant_id": "candidate"}],
        "pass_gates": {
            "gates": [
                {
                    "id": "min_trade_count_per_window",
                    "type": "candidate_metric_threshold",
                    "metric": "trade_count",
                    "operator": ">=",
                    "threshold": 3,
                },
                {
                    "id": "min_trade_count_ratio_vs_baseline",
                    "type": "baseline_candidate_ratio",
                    "baseline_metric": "trade_count",
                    "candidate_metric": "trade_count",
                    "operator": ">=",
                    "threshold": 0.5,
                },
            ]
        },
    }

    result = evaluate_pass_gates(
        plan=plan,
        summaries={
            ("window_a", "baseline"): {"metrics": {"total_trades": 6}},
            ("window_a", "candidate"): {"metrics": {"total_trades": 3}},
        },
        comparison_refs=[],
    )

    assert result["status"] == "PASSED"
    assert [gate["observed"] for gate in result["gates"]] == [3, 0.5]
