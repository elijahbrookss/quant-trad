from __future__ import annotations

import json
from pathlib import Path

import pytest

from cli.mcp_server import McpError, QuantTradMcpServer
from cli.experiments.state_store import ExperimentStateStore


class _FakeClient:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str, dict | None, dict | None]] = []

    def request_json(self, method: str, path: str, *, params=None, payload=None):
        self.calls.append((method, path, params, payload))
        if method == "GET" and path == "/api/health":
            return {"status": "ok"}
        if method == "GET" and path == "/api/bots/run-contexts":
            return {"items": [{"bot_id": "bot-1"}]}
        if method == "GET" and path == "/api/bots/bot-1/run-context":
            return {
                "schema_version": "bot_run_context.v1",
                "bot_id": "bot-1",
                "execution": {"backtest_start": "old-start", "backtest_end": "old-end"},
            }
        if method == "GET" and path == "/api/bots/bot-1/runs":
            return {"items": [{"run_id": "run-1"}], "limit": params["limit"]}
        if method == "GET" and path == "/api/strategies/":
            return {"items": [{"id": "strategy-1"}]}
        if method == "GET" and path == "/api/strategies/strategy-1":
            return {"schema_version": "strategy_definition.v1", "strategy": {"id": "strategy-1"}}
        if method == "GET" and path == "/api/strategies/strategy-1/bindings":
            return {"schema_version": "strategy_bindings.v1", "bindings": {"indicator_ids": ["ind-1"]}}
        if method == "GET" and path == "/api/strategies/strategy-1/rules":
            return {"schema_version": "strategy_rules.v1", "rules": [{"id": "rule-1"}]}
        if method == "GET" and path == "/api/strategies/strategy-1/variants":
            return {"schema_version": "strategy_variants.v1", "variants": [{"id": "variant-1"}]}
        if method == "GET" and path == "/api/strategies/strategy-1/effective":
            return {"schema_version": "effective_strategy.v1", "params": params}
        if method == "GET" and path == "/api/strategies/strategy-1/decision-inputs":
            return {"schema_version": "strategy_decision_inputs.v1", "params": params}
        if method == "GET" and path == "/api/indicators/":
            return [{"id": "ind-1", "type": "candle_stats"}]
        if method == "GET" and path == "/api/indicators/types":
            return ["candle_stats"]
        if method == "GET" and path == "/api/indicators/types/candle_stats":
            return {"type": "candle_stats", "runtime_supported": True}
        if method == "GET" and path == "/api/indicators/ind-1":
            return {"instance": {"id": "ind-1"}, "outputs": {"typed": []}}
        if method == "GET" and path == "/api/indicators/ind-1/strategies":
            return [{"id": "strategy-1"}]
        if method == "GET" and path == "/api/instruments/":
            return [{"id": "instrument-1", "symbol": "BTC/USD"}]
        if method == "GET" and path == "/api/instruments/instrument-1":
            return {"id": "instrument-1", "symbol": "BTC/USD"}
        if method == "GET" and path == "/api/instruments/instrument-1/runtime-profile":
            return {"schema_version": "series_execution_profile.v1", "params": params}
        if method == "GET" and path == "/api/reports/run-1/research-summary":
            return {
                "schema_version": "run_research_summary.v1",
                "run_id": "run-1",
                "dataset_identity": {
                    "data_snapshot_hash": "data-snapshot-hash",
                    "semantic_fingerprint": "semantic-fingerprint",
                },
                "readiness": {
                    "data_quality_status": "degraded",
                    "golden_blocking_reasons": ["provider_missing_data"],
                    "caveats": ["candle_continuity_provider_sparse"],
                },
            }
        if method == "GET" and path == "/api/reports/compare/summary":
            return {"schema_version": "run_report_comparison_summary.v1", **params}
        if method == "POST" and path == "/api/indicators/validate-config":
            return {"schema_version": "indicator_config_validation.v1", "payload": payload}
        if method == "POST" and path == "/api/indicators/":
            return {"schema_version": "indicator_response.v1", "payload": payload}
        if method == "POST" and path == "/api/indicators/ind-1/runtime-validation":
            return {"schema_version": "indicator_runtime_validation.v1", "status": "passed", "payload": payload}
        if method == "POST" and path == "/api/bots/bot-1/runs/start":
            return {"schema_version": "bot_run_start.v1", "run_id": "run-1", "payload": payload}
        if method == "PUT" and path == "/api/bots/bot-1":
            return {"schema_version": "bot_response.v1", "payload": payload}
        raise AssertionError(f"unexpected request: {method} {path}")


class _FakeCommandRunner:
    def __init__(self) -> None:
        self.calls: list[tuple[list[str], float | None]] = []

    def run(self, args: list[str], *, timeout_seconds: float | None = None):
        self.calls.append((args, timeout_seconds))
        return {"args": args, "timeout_seconds": timeout_seconds}


def _server(tmp_path: Path, client: _FakeClient | None = None, runner: _FakeCommandRunner | None = None) -> QuantTradMcpServer:
    fake_client = client or _FakeClient()
    return QuantTradMcpServer(
        api_url="http://backend.test",
        log_root=str(tmp_path),
        client_factory=lambda: fake_client,
        command_runner=runner or _FakeCommandRunner(),
    )


def _plan() -> dict:
    return {
        "schema_version": "experiment_plan.v1",
        "name": "mcp-smoke-plan",
        "hypothesis": "Candidate should not drift.",
        "windows": [{"id": "w1", "start": "2026-01-01T00:00:00Z", "end": "2026-01-31T23:59:59Z"}],
        "variants": [{"id": "baseline", "bot_id": "bot-1"}, {"id": "candidate", "bot_id": "bot-2"}],
        "comparisons": [{"baseline_variant_id": "baseline", "candidate_variant_id": "candidate"}],
    }


def test_mcp_initialize_and_tools_list_exclude_python_handlers(tmp_path):
    server = _server(tmp_path)

    initialize = server.handle({"jsonrpc": "2.0", "id": 1, "method": "initialize"})
    tools = server.handle({"jsonrpc": "2.0", "id": 2, "method": "tools/list"})

    assert initialize["result"]["serverInfo"]["name"] == "quant-trad-mcp"
    assert {tool["name"] for tool in tools["result"]["tools"]} >= {
        "run_experiment_plan",
        "compare_reports",
        "update_bot_backtest_window",
        "validate_indicator_config",
        "create_indicator",
        "validate_indicator_runtime",
        "check_data_coverage",
        "list_instruments",
        "get_instrument_runtime_profile",
        "get_strategy_bindings",
        "get_strategy_decision_inputs",
        "get_effective_strategy",
        "prepare_instrument_matrix_experiment",
        "summarize_experiment",
    }
    assert all("handler" not in tool for tool in tools["result"]["tools"])
    json.dumps(tools)


def test_mcp_resource_read_routes_to_backend_contracts(tmp_path):
    client = _FakeClient()
    server = _server(tmp_path, client=client)

    bots = server.read_resource("quanttrad://bots")
    runs = server.read_resource("quanttrad://bots/bot-1/runs?limit=7")
    summary = server.read_resource("quanttrad://reports/run-1/summary")

    assert bots["items"][0]["bot_id"] == "bot-1"
    assert runs["limit"] == 7
    assert summary["schema_version"] == "run_research_summary.v1"
    assert summary["dataset_identity"]["data_snapshot_hash"] == (
        "data-snapshot-hash"
    )
    assert summary["readiness"] == {
        "data_quality_status": "degraded",
        "golden_blocking_reasons": ["provider_missing_data"],
        "caveats": ["candle_continuity_provider_sparse"],
    }
    assert ("GET", "/api/bots/run-contexts", None, None) in client.calls
    assert ("GET", "/api/reports/run-1/research-summary", None, None) in client.calls


def test_mcp_experiment_summary_resource_reads_local_artifacts(tmp_path):
    store = ExperimentStateStore(tmp_path, experiment_id="exp-1")
    state = store.create_state(_plan())
    state["status"] = "COMPLETED"
    store.write_state(state)
    server = _server(tmp_path)

    summary = server.read_resource("quanttrad://experiments/exp-1/summary")

    assert summary["schema_version"] == "experiment_summary.v1"
    assert summary["experiment_id"] == "exp-1"
    assert summary["counts"]["variants"] == 2


def test_mcp_indicator_resources_route_to_backend_contracts(tmp_path):
    client = _FakeClient()
    server = _server(tmp_path, client=client)

    instances = server.read_resource("quanttrad://indicators")
    types = server.read_resource("quanttrad://indicators/types")
    type_detail = server.read_resource("quanttrad://indicators/types/candle_stats")
    detail = server.read_resource("quanttrad://indicators/ind-1")
    strategies = server.read_resource("quanttrad://indicators/ind-1/strategies")

    assert instances["items"] == [{"id": "ind-1", "type": "candle_stats"}]
    assert types["items"] == ["candle_stats"]
    assert type_detail["runtime_supported"] is True
    assert detail["instance"]["id"] == "ind-1"
    assert strategies["items"] == [{"id": "strategy-1"}]


def test_mcp_strategy_resources_route_to_split_backend_contracts(tmp_path):
    client = _FakeClient()
    server = _server(tmp_path, client=client)

    inventory = server.read_resource("quanttrad://strategies")
    definition = server.read_resource("quanttrad://strategies/strategy-1")
    bindings = server.read_resource("quanttrad://strategies/strategy-1/bindings")
    rules = server.read_resource("quanttrad://strategies/strategy-1/rules")
    variants = server.read_resource("quanttrad://strategies/strategy-1/variants")
    effective = server.read_resource("quanttrad://strategies/strategy-1/effective?variant_name=default")
    decision_inputs = server.read_resource("quanttrad://strategies/strategy-1/decision-inputs?variant_id=variant-1")

    assert inventory["items"] == [{"id": "strategy-1"}]
    assert definition["schema_version"] == "strategy_definition.v1"
    assert bindings["bindings"]["indicator_ids"] == ["ind-1"]
    assert rules["rules"] == [{"id": "rule-1"}]
    assert variants["variants"] == [{"id": "variant-1"}]
    assert effective["params"] == {"variant_name": "default"}
    assert decision_inputs["params"] == {"variant_id": "variant-1"}


def test_mcp_instrument_resources_route_to_backend_contracts(tmp_path):
    client = _FakeClient()
    server = _server(tmp_path, client=client)

    instruments = server.read_resource("quanttrad://instruments")
    detail = server.read_resource("quanttrad://instruments/instrument-1")
    profile = server.read_resource("quanttrad://instruments/instrument-1/runtime-profile?execution_semantics=proxy_derivative")

    assert instruments["items"] == [{"id": "instrument-1", "symbol": "BTC/USD"}]
    assert detail["symbol"] == "BTC/USD"
    assert profile["params"] == {"execution_semantics": "proxy_derivative"}


def test_mcp_drafts_plan_and_run_plan_defaults_to_dry_run(tmp_path):
    runner = _FakeCommandRunner()
    server = _server(tmp_path, runner=runner)

    draft = server.call_tool("draft_experiment_plan", {"plan": _plan(), "experiment_id": "exp-1"})
    path = Path(draft["plan_path"])
    run = server.call_tool("run_experiment_plan", {"plan_path": str(path), "experiment_id": "exp-1"})

    assert path.exists()
    assert draft["preview"]["step_count"] > 0
    assert run["args"] == ["experiments", "run-plan", str(path), "--experiment-id", "exp-1", "--dry-run"]


def test_mcp_run_plan_actual_requires_confirm(tmp_path):
    server = _server(tmp_path)
    plan_path = tmp_path / "plan.json"
    plan_path.write_text(json.dumps(_plan()), encoding="utf-8")

    with pytest.raises(McpError, match="confirm=true"):
        server.call_tool("run_experiment_plan", {"plan_path": str(plan_path), "dry_run": False})


def test_mcp_start_bot_run_is_guarded_and_defaults_to_backtest(tmp_path):
    client = _FakeClient()
    server = _server(tmp_path, client=client)

    with pytest.raises(McpError, match="confirm=true"):
        server.call_tool("start_bot_run", {"bot_id": "bot-1"})
    with pytest.raises(McpError, match="allow_non_backtest"):
        server.call_tool("start_bot_run", {"bot_id": "bot-1", "run_type": "paper", "confirm": True})
    with pytest.raises(McpError, match="dataset_id is required"):
        server.call_tool("start_bot_run", {"bot_id": "bot-1", "confirm": True})

    payload = server.call_tool(
        "start_bot_run",
        {
            "bot_id": "bot-1",
            "dataset_id": "mds-1",
            "request_id": "req-1",
            "confirm": True,
        },
    )

    assert payload["run_id"] == "run-1"
    assert client.calls[-1] == (
        "POST",
        "/api/bots/bot-1/runs/start",
        None,
        {
            "run_type": "backtest",
            "dataset_id": "mds-1",
            "request_id": "req-1",
        },
    )


def test_mcp_controlled_mutation_dry_run_and_apply(tmp_path):
    client = _FakeClient()
    server = _server(tmp_path, client=client)

    planned = server.call_tool(
        "update_bot_backtest_window",
        {
            "bot_id": "bot-1",
            "backtest_start": "2026-01-01T00:00:00Z",
            "backtest_end": "2026-02-01T00:00:00Z",
        },
    )
    with pytest.raises(McpError, match="confirm=true"):
        server.call_tool(
            "update_bot_backtest_window",
            {
                "bot_id": "bot-1",
                "backtest_start": "2026-01-01T00:00:00Z",
                "backtest_end": "2026-02-01T00:00:00Z",
                "apply": True,
            },
        )
    applied = server.call_tool(
        "update_bot_backtest_window",
        {
            "bot_id": "bot-1",
            "backtest_start": "2026-01-01T00:00:00Z",
            "backtest_end": "2026-02-01T00:00:00Z",
            "apply": True,
            "confirm": True,
        },
    )

    assert planned["apply"] is False
    assert planned["current"]["bot_id"] == "bot-1"
    assert applied["payload"] == {
        "backtest_start": "2026-01-01T00:00:00Z",
        "backtest_end": "2026-02-01T00:00:00Z",
    }


def test_mcp_indicator_create_is_planned_and_guarded(tmp_path):
    runner = _FakeCommandRunner()
    server = _server(tmp_path, runner=runner)
    arguments = {"type": "candle_stats", "params": {"warmup_bars": 20}}

    planned = server.call_tool("create_indicator", arguments)
    with pytest.raises(McpError, match="confirm=true"):
        server.call_tool("create_indicator", {**arguments, "apply": True})
    applied = server.call_tool("create_indicator", {**arguments, "apply": True, "confirm": True})

    assert planned["args"] == [
        "indicators",
        "create",
        "--payload-json",
        '{"params": {"warmup_bars": 20}, "type": "candle_stats"}',
    ]
    assert applied["args"] == [
        "indicators",
        "create",
        "--payload-json",
        '{"params": {"warmup_bars": 20}, "type": "candle_stats"}',
        "--apply",
        "--confirm",
    ]
    assert len(runner.calls) == 2


def test_mcp_validate_indicator_runtime_routes_payload(tmp_path):
    runner = _FakeCommandRunner()
    server = _server(tmp_path, runner=runner)

    payload = server.call_tool(
        "validate_indicator_runtime",
        {
            "indicator_id": "ind-1",
            "start": "2026-02-01T00:00:00Z",
            "end": "2026-02-01T02:00:00Z",
            "interval": "1h",
            "instrument_id": "instrument-1",
            "require_ready_by_end": True,
            "min_ready_bars": 1,
        },
    )

    assert payload["args"] == [
        "indicators",
        "validate-runtime",
        "ind-1",
        "--start",
        "2026-02-01T00:00:00Z",
        "--end",
        "2026-02-01T02:00:00Z",
        "--interval",
        "1h",
        "--instrument-id",
        "instrument-1",
        "--require-ready-by-end",
        "--min-ready-bars",
        "1",
    ]


def test_mcp_check_data_coverage_routes_to_qt(tmp_path):
    runner = _FakeCommandRunner()
    server = _server(tmp_path, runner=runner)

    payload = server.call_tool(
        "check_data_coverage",
        {
            "symbol": "BTC/USD",
            "datasource": "CCXT",
            "exchange": "coinbase",
            "start": "2026-01-01T00:00:00Z",
            "end": "2026-01-02T00:00:00Z",
            "timeframe": "1h",
        },
    )

    assert payload["args"] == [
        "data",
        "coverage",
        "--start",
        "2026-01-01T00:00:00Z",
        "--end",
        "2026-01-02T00:00:00Z",
        "--timeframe",
        "1h",
        "--symbol",
        "BTC/USD",
        "--datasource",
        "CCXT",
        "--exchange",
        "coinbase",
    ]


def test_mcp_instrument_tools_route_to_qt(tmp_path):
    runner = _FakeCommandRunner()
    server = _server(tmp_path, runner=runner)

    listed = server.call_tool("list_instruments", {"symbol": "BTC", "datasource": "CCXT"})
    detail = server.call_tool("get_instrument", {"instrument_id": "instrument-1"})
    profile = server.call_tool(
        "get_instrument_runtime_profile",
        {"instrument_id": "instrument-1", "execution_semantics": "proxy_derivative"},
    )

    assert listed["args"] == ["instruments", "list", "--datasource", "CCXT", "--symbol", "BTC"]
    assert detail["args"] == ["instruments", "get", "instrument-1"]
    assert profile["args"] == ["instruments", "profile", "instrument-1", "--execution-semantics", "proxy_derivative"]


def test_mcp_prepare_instrument_matrix_routes_to_qt_and_is_guarded(tmp_path):
    runner = _FakeCommandRunner()
    server = _server(tmp_path, runner=runner)
    request = {
        "schema_version": "instrument_matrix_experiment_request.v1",
        "name": "btc-matrix",
        "source_bot_id": "bot-1",
        "window": {"id": "w1", "start": "2026-01-01T00:00:00Z", "end": "2026-02-01T00:00:00Z"},
        "groups": [{"id": "btc", "spot_instrument_id": "spot-1", "derivative_instrument_id": "deriv-1"}],
    }

    planned = server.call_tool("prepare_instrument_matrix_experiment", {"request": request, "out_path": "plan.json"})
    with pytest.raises(McpError, match="confirm=true"):
        server.call_tool("prepare_instrument_matrix_experiment", {"request": request, "apply": True})
    applied = server.call_tool(
        "prepare_instrument_matrix_experiment",
        {"request": request, "out_path": "plan.json", "apply": True, "confirm": True},
    )

    assert planned["args"][:3] == ["experiments", "prepare-instrument-matrix", "--request-json"]
    assert planned["args"][-2:] == ["--out", "plan.json"]
    assert applied["args"][-4:] == ["--out", "plan.json", "--apply", "--confirm"]


def test_mcp_summarize_experiment_routes_to_qt(tmp_path):
    runner = _FakeCommandRunner()
    server = _server(tmp_path, runner=runner)

    payload = server.call_tool("summarize_experiment", {"ref": "exp-1", "out_path": "summary.json"})

    assert payload["args"] == ["experiments", "summarize", "exp-1", "--out", "summary.json"]
