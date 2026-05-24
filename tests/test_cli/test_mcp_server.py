from __future__ import annotations

import json
from pathlib import Path

import pytest

from cli.mcp_server import McpError, QuantTradMcpServer


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
        if method == "GET" and path == "/api/reports/run-1/research-summary":
            return {"schema_version": "run_research_summary.v1", "run_id": "run-1"}
        if method == "GET" and path == "/api/reports/compare/summary":
            return {"schema_version": "run_report_comparison_summary.v1", **params}
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
    assert ("GET", "/api/bots/run-contexts", None, None) in client.calls
    assert ("GET", "/api/reports/run-1/research-summary", None, None) in client.calls


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

    payload = server.call_tool("start_bot_run", {"bot_id": "bot-1", "request_id": "req-1", "confirm": True})

    assert payload["run_id"] == "run-1"
    assert client.calls[-1] == (
        "POST",
        "/api/bots/bot-1/runs/start",
        None,
        {"run_type": "backtest", "request_id": "req-1"},
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
