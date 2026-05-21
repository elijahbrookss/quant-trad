from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import subprocess
import sys
from typing import Any, Callable
from urllib.parse import parse_qs, unquote, urlencode, urlparse

from cli.api import ApiClient
from cli.audit import date_partition, safe_path_part, timestamp_slug
from cli.experiments.contracts import normalize_plan
from cli.experiments.event_log import read_events
from cli.experiments.plan_loader import plan_preview
from cli.experiments.state_store import ExperimentStateStore, find_experiment_dir


MCP_PROTOCOL_VERSION = "2024-11-05"
SERVER_NAME = "quant-trad-mcp"
SERVER_VERSION = "0.1.0"


class McpError(Exception):
    def __init__(self, message: str, *, code: int = -32000, data: Any | None = None) -> None:
        super().__init__(message)
        self.code = code
        self.data = data


class QtCommandRunner:
    def __init__(
        self,
        *,
        api_url: str,
        timeout: float,
        log_root: str,
        command_timeout_seconds: float,
    ) -> None:
        self.api_url = api_url
        self.timeout = float(timeout)
        self.log_root = log_root
        self.command_timeout_seconds = float(command_timeout_seconds)

    def run(self, args: list[str], *, timeout_seconds: float | None = None) -> dict[str, Any]:
        command = [
            sys.executable,
            "-m",
            "cli.main",
            "--api-url",
            self.api_url,
            "--timeout",
            str(self.timeout),
            "--log-root",
            self.log_root,
            *args,
        ]
        timeout = float(timeout_seconds or self.command_timeout_seconds)
        completed = subprocess.run(
            command,
            check=False,
            capture_output=True,
            text=True,
            timeout=timeout,
        )
        stdout = completed.stdout.strip()
        stderr = completed.stderr.strip()
        payload: dict[str, Any]
        if stdout:
            try:
                decoded = json.loads(stdout.splitlines()[-1])
            except json.JSONDecodeError as exc:
                raise McpError(
                    "qt command returned non-JSON output",
                    data={
                        "args": args,
                        "exit_code": completed.returncode,
                        "stdout": stdout[-4000:],
                        "stderr": stderr[-4000:],
                    },
                ) from exc
            if not isinstance(decoded, dict):
                raise McpError(
                    "qt command returned a non-object JSON payload",
                    data={"args": args, "exit_code": completed.returncode, "payload": decoded},
                )
            payload = decoded
        else:
            payload = {}
        if completed.returncode != 0:
            raise McpError(
                "qt command failed",
                data={
                    "args": args,
                    "exit_code": completed.returncode,
                    "payload": payload,
                    "stderr": stderr[-4000:],
                },
            )
        return payload


class QuantTradMcpServer:
    def __init__(
        self,
        *,
        api_url: str = "http://127.0.0.1:8000",
        timeout: float = 30.0,
        log_root: str = "logs",
        command_timeout_seconds: float = 7200.0,
        client_factory: Callable[[], ApiClient] | None = None,
        command_runner: Any | None = None,
    ) -> None:
        self.api_url = api_url
        self.timeout = float(timeout)
        self.log_root = str(log_root)
        self.command_timeout_seconds = float(command_timeout_seconds)
        self._client_factory = client_factory or (lambda: ApiClient(self.api_url, timeout=self.timeout))
        self._command_runner = command_runner or QtCommandRunner(
            api_url=self.api_url,
            timeout=self.timeout,
            log_root=self.log_root,
            command_timeout_seconds=self.command_timeout_seconds,
        )
        self._tools = self._build_tools()

    def handle(self, message: dict[str, Any]) -> dict[str, Any] | None:
        request_id = message.get("id")
        method = message.get("method")
        if not method:
            return self._error_response(request_id, -32600, "missing JSON-RPC method")
        try:
            result = self._dispatch(method, message.get("params") or {})
        except McpError as exc:
            if request_id is None:
                return None
            return self._error_response(request_id, exc.code, str(exc), exc.data)
        except Exception as exc:  # noqa: BLE001 - MCP must convert server errors to JSON-RPC.
            if request_id is None:
                return None
            return self._error_response(
                request_id,
                -32603,
                str(exc),
                {"type": type(exc).__name__},
            )
        if request_id is None:
            return None
        return {"jsonrpc": "2.0", "id": request_id, "result": result}

    def _dispatch(self, method: str, params: dict[str, Any]) -> dict[str, Any]:
        if method == "initialize":
            return {
                "protocolVersion": MCP_PROTOCOL_VERSION,
                "capabilities": {
                    "resources": {"listChanged": False},
                    "tools": {"listChanged": False},
                },
                "serverInfo": {"name": SERVER_NAME, "version": SERVER_VERSION},
            }
        if method == "notifications/initialized":
            return {}
        if method == "ping":
            return {}
        if method == "resources/list":
            return {"resources": self._resource_list()}
        if method == "resources/templates/list":
            return {"resourceTemplates": self._resource_templates()}
        if method == "resources/read":
            uri = _required_str(params, "uri")
            payload = self.read_resource(uri)
            return {
                "contents": [
                    {
                        "uri": uri,
                        "mimeType": "application/json",
                        "text": json.dumps(payload, indent=2, sort_keys=True, default=str),
                    }
                ]
            }
        if method == "tools/list":
            tools = []
            for name, spec in self._tools.items():
                public_spec = {key: value for key, value in spec.items() if key != "handler"}
                tools.append({"name": name, **public_spec})
            return {"tools": tools}
        if method == "tools/call":
            name = _required_str(params, "name")
            arguments = params.get("arguments") or {}
            if not isinstance(arguments, dict):
                raise McpError("tool arguments must be an object", code=-32602)
            payload = self.call_tool(name, arguments)
            return {
                "content": [
                    {
                        "type": "text",
                        "text": json.dumps(payload, indent=2, sort_keys=True, default=str),
                    }
                ],
                "isError": False,
            }
        raise McpError(f"unknown MCP method: {method}", code=-32601)

    def read_resource(self, uri: str) -> dict[str, Any]:
        parts, query = _parse_quanttrad_uri(uri)
        client = self._client_factory()
        if parts == ["health"]:
            return _ensure_object(client.request_json("GET", "/api/health"), "GET /api/health")
        if parts == ["bots"]:
            return _ensure_object(client.request_json("GET", "/api/bots/run-contexts"), "GET /api/bots/run-contexts")
        if len(parts) == 2 and parts[0] == "bots":
            bot_id = parts[1]
            return _ensure_object(client.request_json("GET", f"/api/bots/{bot_id}/run-context"), f"GET bot {bot_id}")
        if len(parts) == 3 and parts[0] == "bots" and parts[2] == "runs":
            limit = _query_int(query, "limit", 25)
            return _ensure_object(
                client.request_json("GET", f"/api/bots/{parts[1]}/runs", params={"limit": limit}),
                f"GET bot {parts[1]} runs",
            )
        if len(parts) == 3 and parts[0] == "bots" and parts[2] == "active-run":
            return _ensure_object(client.request_json("GET", f"/api/bots/{parts[1]}/active-run"), f"GET bot {parts[1]} active run")
        if parts == ["strategies"]:
            return _ensure_object(client.request_json("GET", "/api/strategies/"), "GET /api/strategies/")
        if len(parts) == 2 and parts[0] == "strategies":
            return _ensure_object(client.request_json("GET", f"/api/strategies/{parts[1]}"), f"GET strategy {parts[1]}")
        if len(parts) == 3 and parts[0] == "strategies" and parts[2] == "variants":
            return _ensure_object(client.request_json("GET", f"/api/strategies/{parts[1]}/variants"), f"GET strategy {parts[1]} variants")
        if parts == ["providers"]:
            return _ensure_object(client.request_json("GET", "/api/providers/"), "GET /api/providers/")
        if parts == ["reports"]:
            return _ensure_object(
                client.request_json(
                    "GET",
                    "/api/reports/",
                    params={
                        "type": _query_str(query, "type", "backtest"),
                        "status": _query_str(query, "status", "completed"),
                        "limit": _query_int(query, "limit", 50),
                        "offset": _query_int(query, "offset", 0),
                        "search": _query_str(query, "search", None),
                        "botId": _query_str(query, "botId", None),
                        "instrument": _query_str(query, "instrument", None),
                        "timeframe": _query_str(query, "timeframe", None),
                        "start": _query_str(query, "start", None),
                        "end": _query_str(query, "end", None),
                    },
                ),
                "GET /api/reports/",
            )
        if len(parts) == 3 and parts[0] == "reports":
            return self._read_report_resource(client, parts[1], parts[2])
        if len(parts) == 3 and parts[0] == "experiments" and parts[2] == "state":
            return self._read_experiment_state(parts[1])
        if len(parts) == 3 and parts[0] == "experiments" and parts[2] == "events":
            return self._read_experiment_events(parts[1], tail=_query_int(query, "tail", 100), event_type=_query_str(query, "type", None), status=_query_str(query, "status", None))
        raise McpError(f"unsupported resource URI: {uri}", code=-32602)

    def call_tool(self, name: str, arguments: dict[str, Any]) -> dict[str, Any]:
        handler = self._tools.get(name, {}).get("handler")
        if handler is None:
            raise McpError(f"unknown tool: {name}", code=-32602)
        return handler(arguments)

    def _read_report_resource(self, client: ApiClient, run_id: str, section: str) -> dict[str, Any]:
        paths = {
            "dataset": f"/api/reports/{run_id}",
            "readiness": f"/api/reports/{run_id}/readiness",
            "summary": f"/api/reports/{run_id}/research-summary",
            "sections": f"/api/reports/{run_id}/sections",
            "diagnostics": f"/api/reports/{run_id}/diagnostics",
            "metrics": f"/api/reports/{run_id}/metrics",
            "operational-health": f"/api/reports/{run_id}/operational-health",
            "run-report-status": f"/api/reports/{run_id}/run-report/status",
        }
        path = paths.get(section)
        if path is None:
            raise McpError(f"unsupported report section: {section}", code=-32602)
        return _ensure_object(client.request_json("GET", path), f"GET report {section} for {run_id}")

    def _read_experiment_state(self, ref: str) -> dict[str, Any]:
        path = find_experiment_dir(self.log_root, ref)
        store = ExperimentStateStore(self.log_root, path=path)
        return store.load_state()

    def _read_experiment_events(
        self,
        ref: str,
        *,
        tail: int | None = None,
        event_type: str | None = None,
        status: str | None = None,
    ) -> dict[str, Any]:
        path = find_experiment_dir(self.log_root, ref)
        store = ExperimentStateStore(self.log_root, path=path)
        return {
            "schema_version": "experiment_events_view.v1",
            "experiment_id": store.experiment_id,
            "events": read_events(store.events_path, tail=tail, event_type=event_type, status=status),
        }

    def _tool_health_check(self, _arguments: dict[str, Any]) -> dict[str, Any]:
        return self.read_resource("quanttrad://health")

    def _tool_list_bots(self, _arguments: dict[str, Any]) -> dict[str, Any]:
        return self.read_resource("quanttrad://bots")

    def _tool_get_bot(self, arguments: dict[str, Any]) -> dict[str, Any]:
        return self.read_resource(f"quanttrad://bots/{_required_str(arguments, 'bot_id')}")

    def _tool_list_bot_runs(self, arguments: dict[str, Any]) -> dict[str, Any]:
        bot_id = _required_str(arguments, "bot_id")
        limit = _optional_int(arguments, "limit", 25)
        return self.read_resource(f"quanttrad://bots/{bot_id}/runs?limit={limit}")

    def _tool_get_active_run(self, arguments: dict[str, Any]) -> dict[str, Any]:
        return self.read_resource(f"quanttrad://bots/{_required_str(arguments, 'bot_id')}/active-run")

    def _tool_list_strategies(self, _arguments: dict[str, Any]) -> dict[str, Any]:
        return self.read_resource("quanttrad://strategies")

    def _tool_get_strategy(self, arguments: dict[str, Any]) -> dict[str, Any]:
        return self.read_resource(f"quanttrad://strategies/{_required_str(arguments, 'strategy_id')}")

    def _tool_list_strategy_variants(self, arguments: dict[str, Any]) -> dict[str, Any]:
        return self.read_resource(f"quanttrad://strategies/{_required_str(arguments, 'strategy_id')}/variants")

    def _tool_list_reports(self, arguments: dict[str, Any]) -> dict[str, Any]:
        params = {
            "type": str(arguments.get("type") or "backtest"),
            "status": str(arguments.get("status") or "completed"),
            "limit": str(_optional_int(arguments, "limit", 50)),
            "offset": str(_optional_int(arguments, "offset", 0)),
        }
        for key in ("search", "botId", "instrument", "timeframe", "start", "end"):
            value = arguments.get(key)
            if value is not None:
                params[key] = str(value)
        query = urlencode(params)
        return self.read_resource(f"quanttrad://reports?{query}")

    def _tool_get_report_section(self, arguments: dict[str, Any]) -> dict[str, Any]:
        run_id = _required_str(arguments, "run_id")
        section = _required_str(arguments, "section")
        return self.read_resource(f"quanttrad://reports/{run_id}/{section}")

    def _tool_compare_reports(self, arguments: dict[str, Any]) -> dict[str, Any]:
        left_run_id = _required_str(arguments, "left_run_id")
        right_run_id = _required_str(arguments, "right_run_id")
        return _ensure_object(
            self._client_factory().request_json(
                "GET",
                "/api/reports/compare/summary",
                params={
                    "left_run_id": left_run_id,
                    "right_run_id": right_run_id,
                    "include_golden": _optional_bool(arguments, "include_golden", True),
                    "require_golden": _optional_bool(arguments, "require_golden", False),
                },
            ),
            "GET report comparison summary",
        )

    def _tool_list_providers(self, _arguments: dict[str, Any]) -> dict[str, Any]:
        return self.read_resource("quanttrad://providers")

    def _tool_draft_experiment_plan(self, arguments: dict[str, Any]) -> dict[str, Any]:
        plan = _required_object(arguments, "plan")
        normalized = normalize_plan(plan)
        path = self._write_plan(normalized, experiment_id=_optional_str(arguments, "experiment_id"))
        return {
            "schema_version": "mcp_experiment_plan_draft.v1",
            "plan_path": str(path),
            "preview": plan_preview(normalized),
        }

    def _tool_validate_experiment_plan(self, arguments: dict[str, Any]) -> dict[str, Any]:
        plan_path = self._plan_path_from_arguments(arguments)
        args = ["experiments", "validate-plan", str(plan_path)]
        if _optional_bool(arguments, "skip_data_preflight", False):
            args.append("--skip-data-preflight")
        return self._command_runner.run(args, timeout_seconds=_optional_float(arguments, "timeout_seconds", 300.0))

    def _tool_run_experiment_plan(self, arguments: dict[str, Any]) -> dict[str, Any]:
        plan_path = self._plan_path_from_arguments(arguments)
        dry_run = _optional_bool(arguments, "dry_run", True)
        if not dry_run:
            _require_confirm(arguments, "run_experiment_plan starts bot runs and may update bot backtest windows")
        args = ["experiments", "run-plan", str(plan_path)]
        if _optional_str(arguments, "experiment_id"):
            args.extend(["--experiment-id", _required_str(arguments, "experiment_id")])
        if dry_run:
            args.append("--dry-run")
        if _optional_bool(arguments, "skip_data_preflight", False):
            args.append("--skip-data-preflight")
        if _optional_bool(arguments, "proceed_with_data_warnings", False):
            args.append("--proceed-with-data-warnings")
        return self._command_runner.run(args, timeout_seconds=_optional_float(arguments, "timeout_seconds", self.command_timeout_seconds))

    def _tool_resume_experiment(self, arguments: dict[str, Any]) -> dict[str, Any]:
        _require_confirm(arguments, "resume_experiment may continue unfinished bot runs")
        return self._command_runner.run(
            ["experiments", "resume", _required_str(arguments, "ref")],
            timeout_seconds=_optional_float(arguments, "timeout_seconds", self.command_timeout_seconds),
        )

    def _tool_get_experiment_status(self, arguments: dict[str, Any]) -> dict[str, Any]:
        args = ["experiments", "status", _required_str(arguments, "ref")]
        if _optional_str(arguments, "bot_id"):
            args.extend(["--bot-id", _required_str(arguments, "bot_id")])
        return self._command_runner.run(args, timeout_seconds=_optional_float(arguments, "timeout_seconds", 300.0))

    def _tool_get_experiment_events(self, arguments: dict[str, Any]) -> dict[str, Any]:
        args = ["experiments", "events", _required_str(arguments, "ref")]
        if arguments.get("tail") is not None:
            args.extend(["--tail", str(_optional_int(arguments, "tail", 100))])
        if _optional_str(arguments, "type"):
            args.extend(["--type", _required_str(arguments, "type")])
        if _optional_str(arguments, "status"):
            args.extend(["--status", _required_str(arguments, "status")])
        return self._command_runner.run(args, timeout_seconds=_optional_float(arguments, "timeout_seconds", 300.0))

    def _tool_doctor_experiment(self, arguments: dict[str, Any]) -> dict[str, Any]:
        return self._command_runner.run(
            ["experiments", "doctor", _required_str(arguments, "ref")],
            timeout_seconds=_optional_float(arguments, "timeout_seconds", 300.0),
        )

    def _tool_collect_experiment(self, arguments: dict[str, Any]) -> dict[str, Any]:
        if _optional_bool(arguments, "wait", False) or _optional_bool(arguments, "export", False) or _optional_str(arguments, "compare_to"):
            _require_confirm(arguments, "collect_experiment can wait, export reports, and materialize comparisons")
        args = ["experiments", "collect", _required_str(arguments, "ref")]
        for key, flag in (
            ("bot_id", "--bot-id"),
            ("compare_to", "--compare-to"),
            ("out_dir", "--out-dir"),
        ):
            if _optional_str(arguments, key):
                args.extend([flag, _required_str(arguments, key)])
        for key, flag in (
            ("wait", "--wait"),
            ("print_each", "--print-each"),
            ("allow_non_completed", "--allow-non-completed"),
            ("export", "--export"),
            ("no_json", "--no-json"),
            ("no_csv", "--no-csv"),
            ("include_candles", "--include-candles"),
            ("no_golden", "--no-golden"),
            ("require_golden", "--require-golden"),
        ):
            if _optional_bool(arguments, key, False):
                args.append(flag)
        if arguments.get("wait_timeout") is not None:
            args.extend(["--wait-timeout", str(_optional_float(arguments, "wait_timeout", 3600.0))])
        if arguments.get("interval") is not None:
            args.extend(["--interval", str(_optional_float(arguments, "interval", 30.0))])
        return self._command_runner.run(args, timeout_seconds=_optional_float(arguments, "timeout_seconds", self.command_timeout_seconds))

    def _tool_start_bot_run(self, arguments: dict[str, Any]) -> dict[str, Any]:
        _require_confirm(arguments, "start_bot_run starts a bot run")
        bot_id = _required_str(arguments, "bot_id")
        run_type = str(arguments.get("run_type") or "backtest")
        if run_type not in {"backtest", "sim_trade", "paper", "live"}:
            raise McpError("run_type must be one of backtest, sim_trade, paper, live", code=-32602)
        if run_type in {"paper", "live"} and not _optional_bool(arguments, "allow_non_backtest", False):
            raise McpError("paper/live runs require allow_non_backtest=true", code=-32602)
        payload: dict[str, Any] = {"run_type": run_type}
        for key in ("request_id", "execution_behavior", "duration_seconds", "market_data_stream_policy"):
            if arguments.get(key) is not None:
                payload[key] = arguments[key]
        return _ensure_object(
            self._client_factory().request_json("POST", f"/api/bots/{bot_id}/runs/start", payload=payload),
            f"POST start bot {bot_id}",
        )

    def _tool_stop_bot_run(self, arguments: dict[str, Any]) -> dict[str, Any]:
        _require_confirm(arguments, "stop_bot_run stops a bot run")
        bot_id = _required_str(arguments, "bot_id")
        payload: dict[str, Any] = {"preserve_container": _optional_bool(arguments, "preserve_container", False)}
        for key in ("run_id", "request_id"):
            if _optional_str(arguments, key):
                payload[key] = _required_str(arguments, key)
        return _ensure_object(
            self._client_factory().request_json("POST", f"/api/bots/{bot_id}/stop", payload=payload),
            f"POST stop bot {bot_id}",
        )

    def _tool_update_bot_backtest_window(self, arguments: dict[str, Any]) -> dict[str, Any]:
        bot_id = _required_str(arguments, "bot_id")
        payload = {
            "backtest_start": _required_str(arguments, "backtest_start"),
            "backtest_end": _required_str(arguments, "backtest_end"),
        }
        if not _optional_bool(arguments, "apply", False):
            return {
                "schema_version": "mcp_planned_mutation.v1",
                "operation": "update_bot_backtest_window",
                "bot_id": bot_id,
                "apply": False,
                "payload": payload,
                "current": self.read_resource(f"quanttrad://bots/{bot_id}"),
            }
        _require_confirm(arguments, "update_bot_backtest_window mutates bot configuration")
        return _ensure_object(
            self._client_factory().request_json("PUT", f"/api/bots/{bot_id}", payload=payload),
            f"PUT bot {bot_id} backtest window",
        )

    def _tool_set_bot_strategy_variant(self, arguments: dict[str, Any]) -> dict[str, Any]:
        bot_id = _required_str(arguments, "bot_id")
        payload: dict[str, Any] = {}
        for arg_name, payload_name in (
            ("strategy_id", "strategy_id"),
            ("variant_id", "strategy_variant_id"),
            ("variant_name", "strategy_variant_name"),
        ):
            if _optional_str(arguments, arg_name):
                payload[payload_name] = _required_str(arguments, arg_name)
        if not payload:
            raise McpError("at least one of strategy_id, variant_id, or variant_name is required", code=-32602)
        if not _optional_bool(arguments, "apply", False):
            return {
                "schema_version": "mcp_planned_mutation.v1",
                "operation": "set_bot_strategy_variant",
                "bot_id": bot_id,
                "apply": False,
                "payload": payload,
                "current": self.read_resource(f"quanttrad://bots/{bot_id}"),
            }
        _require_confirm(arguments, "set_bot_strategy_variant mutates bot strategy selection")
        return _ensure_object(
            self._client_factory().request_json("PUT", f"/api/bots/{bot_id}", payload=payload),
            f"PUT bot {bot_id} strategy selection",
        )

    def _tool_create_strategy_variant(self, arguments: dict[str, Any]) -> dict[str, Any]:
        strategy_id = _required_str(arguments, "strategy_id")
        payload = {
            "name": _required_str(arguments, "name"),
            "description": arguments.get("description"),
            "output_filters": arguments.get("output_filters") or [],
            "is_default": _optional_bool(arguments, "is_default", False),
        }
        if not isinstance(payload["output_filters"], list):
            raise McpError("output_filters must be an array", code=-32602)
        if not _optional_bool(arguments, "apply", False):
            return {
                "schema_version": "mcp_planned_mutation.v1",
                "operation": "create_strategy_variant",
                "strategy_id": strategy_id,
                "apply": False,
                "payload": payload,
            }
        _require_confirm(arguments, "create_strategy_variant mutates strategy configuration")
        return _ensure_object(
            self._client_factory().request_json("POST", f"/api/strategies/{strategy_id}/variants", payload=payload),
            f"POST strategy {strategy_id} variant",
        )

    def _tool_update_strategy_variant(self, arguments: dict[str, Any]) -> dict[str, Any]:
        strategy_id = _required_str(arguments, "strategy_id")
        variant_id = _required_str(arguments, "variant_id")
        payload: dict[str, Any] = {}
        for key in ("name", "description"):
            if arguments.get(key) is not None:
                payload[key] = arguments[key]
        if arguments.get("output_filters") is not None:
            if not isinstance(arguments["output_filters"], list):
                raise McpError("output_filters must be an array", code=-32602)
            payload["output_filters"] = arguments["output_filters"]
        if arguments.get("is_default") is not None:
            payload["is_default"] = _optional_bool(arguments, "is_default", False)
        if not payload:
            raise McpError("at least one variant update field is required", code=-32602)
        if not _optional_bool(arguments, "apply", False):
            return {
                "schema_version": "mcp_planned_mutation.v1",
                "operation": "update_strategy_variant",
                "strategy_id": strategy_id,
                "variant_id": variant_id,
                "apply": False,
                "payload": payload,
            }
        _require_confirm(arguments, "update_strategy_variant mutates strategy configuration")
        return _ensure_object(
            self._client_factory().request_json("PUT", f"/api/strategies/{strategy_id}/variants/{variant_id}", payload=payload),
            f"PUT strategy {strategy_id} variant {variant_id}",
        )

    def _plan_path_from_arguments(self, arguments: dict[str, Any]) -> Path:
        if _optional_str(arguments, "plan_path"):
            return Path(_required_str(arguments, "plan_path")).expanduser()
        if isinstance(arguments.get("plan"), dict):
            normalized = normalize_plan(arguments["plan"])
            return self._write_plan(normalized, experiment_id=_optional_str(arguments, "experiment_id"))
        raise McpError("plan_path or plan object is required", code=-32602)

    def _write_plan(self, plan: dict[str, Any], *, experiment_id: str | None = None) -> Path:
        name = experiment_id or str(plan.get("name") or "experiment")
        filename = f"{timestamp_slug()}__{safe_path_part(name)}.json"
        path = Path(self.log_root).expanduser() / "experiments" / "plans" / date_partition() / filename
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(plan, indent=2, sort_keys=True, default=str) + "\n", encoding="utf-8")
        return path

    def _resource_list(self) -> list[dict[str, str]]:
        return [
            _resource("quanttrad://health", "Backend health"),
            _resource("quanttrad://bots", "Bot run contexts"),
            _resource("quanttrad://strategies", "Strategies"),
            _resource("quanttrad://providers", "Providers"),
            _resource("quanttrad://reports", "Recent completed reports"),
        ]

    def _resource_templates(self) -> list[dict[str, str]]:
        return [
            _template("quanttrad://bots/{bot_id}", "Bot run context"),
            _template("quanttrad://bots/{bot_id}/runs?limit={limit}", "Bot recent runs"),
            _template("quanttrad://bots/{bot_id}/active-run", "Bot active run"),
            _template("quanttrad://strategies/{strategy_id}", "Strategy detail"),
            _template("quanttrad://strategies/{strategy_id}/variants", "Strategy variants"),
            _template("quanttrad://reports/{run_id}/dataset", "Report dataset"),
            _template("quanttrad://reports/{run_id}/readiness", "Report readiness"),
            _template("quanttrad://reports/{run_id}/summary", "Report research summary"),
            _template("quanttrad://reports/{run_id}/diagnostics", "Report diagnostics"),
            _template("quanttrad://reports/{run_id}/metrics", "Report metrics"),
            _template("quanttrad://reports/{run_id}/operational-health", "Report operational health"),
            _template("quanttrad://reports/{run_id}/run-report-status", "Run report materialization status"),
            _template("quanttrad://experiments/{experiment_id}/state", "Local experiment suite state"),
            _template("quanttrad://experiments/{experiment_id}/events?tail={tail}", "Local experiment event log"),
        ]

    def _build_tools(self) -> dict[str, dict[str, Any]]:
        return {
            "health_check": {
                "description": "Read backend health through the Quant-Trad API.",
                "inputSchema": _object_schema(),
                "handler": self._tool_health_check,
            },
            "list_bots": {
                "description": "List bot run contexts.",
                "inputSchema": _object_schema(),
                "handler": self._tool_list_bots,
            },
            "get_bot": {
                "description": "Read one bot run context.",
                "inputSchema": _object_schema({"bot_id": _string_schema()}, required=["bot_id"]),
                "handler": self._tool_get_bot,
            },
            "list_bot_runs": {
                "description": "List recent runs for a bot.",
                "inputSchema": _object_schema({"bot_id": _string_schema(), "limit": _integer_schema(default=25)}, required=["bot_id"]),
                "handler": self._tool_list_bot_runs,
            },
            "get_active_run": {
                "description": "Read the active run for a bot.",
                "inputSchema": _object_schema({"bot_id": _string_schema()}, required=["bot_id"]),
                "handler": self._tool_get_active_run,
            },
            "list_strategies": {
                "description": "List strategies.",
                "inputSchema": _object_schema(),
                "handler": self._tool_list_strategies,
            },
            "get_strategy": {
                "description": "Read one strategy detail payload.",
                "inputSchema": _object_schema({"strategy_id": _string_schema()}, required=["strategy_id"]),
                "handler": self._tool_get_strategy,
            },
            "list_strategy_variants": {
                "description": "List variants for a strategy.",
                "inputSchema": _object_schema({"strategy_id": _string_schema()}, required=["strategy_id"]),
                "handler": self._tool_list_strategy_variants,
            },
            "list_reports": {
                "description": "List completed report summaries.",
                "inputSchema": _object_schema(
                    {
                        "type": _string_schema(default="backtest"),
                        "status": _string_schema(default="completed"),
                        "limit": _integer_schema(default=50),
                        "offset": _integer_schema(default=0),
                        "search": _string_schema(),
                        "botId": _string_schema(),
                        "instrument": _string_schema(),
                        "timeframe": _string_schema(),
                        "start": _string_schema(),
                        "end": _string_schema(),
                    }
                ),
                "handler": self._tool_list_reports,
            },
            "get_report_section": {
                "description": "Read a report section such as summary, diagnostics, metrics, readiness, or run-report-status.",
                "inputSchema": _object_schema(
                    {
                        "run_id": _string_schema(),
                        "section": {"type": "string", "enum": ["dataset", "readiness", "summary", "sections", "diagnostics", "metrics", "operational-health", "run-report-status"]},
                    },
                    required=["run_id", "section"],
                ),
                "handler": self._tool_get_report_section,
            },
            "compare_reports": {
                "description": "Compare two materialized run reports through the backend comparison summary route.",
                "inputSchema": _object_schema(
                    {
                        "left_run_id": _string_schema(),
                        "right_run_id": _string_schema(),
                        "include_golden": _boolean_schema(default=True),
                        "require_golden": _boolean_schema(default=False),
                    },
                    required=["left_run_id", "right_run_id"],
                ),
                "handler": self._tool_compare_reports,
            },
            "list_providers": {
                "description": "List providers and safe credential metadata.",
                "inputSchema": _object_schema(),
                "handler": self._tool_list_providers,
            },
            "draft_experiment_plan": {
                "description": "Normalize an experiment plan object, write it under logs, and return a step preview.",
                "inputSchema": _object_schema({"plan": _free_object_schema(), "experiment_id": _string_schema()}, required=["plan"]),
                "handler": self._tool_draft_experiment_plan,
            },
            "validate_experiment_plan": {
                "description": "Validate and preview an experiment plan through qt experiments validate-plan.",
                "inputSchema": _object_schema(
                    {
                        "plan_path": _string_schema(),
                        "plan": _free_object_schema(),
                        "experiment_id": _string_schema(),
                        "skip_data_preflight": _boolean_schema(default=False),
                        "timeout_seconds": _number_schema(default=300.0),
                    }
                ),
                "handler": self._tool_validate_experiment_plan,
            },
            "run_experiment_plan": {
                "description": "Run or dry-run a sequential experiment plan. Actual runs require dry_run=false and confirm=true.",
                "inputSchema": _object_schema(
                    {
                        "plan_path": _string_schema(),
                        "plan": _free_object_schema(),
                        "experiment_id": _string_schema(),
                        "dry_run": _boolean_schema(default=True),
                        "confirm": _boolean_schema(default=False),
                        "skip_data_preflight": _boolean_schema(default=False),
                        "proceed_with_data_warnings": _boolean_schema(default=False),
                        "timeout_seconds": _number_schema(default=7200.0),
                    }
                ),
                "handler": self._tool_run_experiment_plan,
            },
            "resume_experiment": {
                "description": "Resume a plan-based experiment from local state. Requires confirm=true.",
                "inputSchema": _object_schema({"ref": _string_schema(), "confirm": _boolean_schema(default=False), "timeout_seconds": _number_schema(default=7200.0)}, required=["ref"]),
                "handler": self._tool_resume_experiment,
            },
            "get_experiment_status": {
                "description": "Fetch compact status for a tracked experiment or raw run id.",
                "inputSchema": _object_schema({"ref": _string_schema(), "bot_id": _string_schema(), "timeout_seconds": _number_schema(default=300.0)}, required=["ref"]),
                "handler": self._tool_get_experiment_status,
            },
            "get_experiment_events": {
                "description": "Read local plan-based experiment events.",
                "inputSchema": _object_schema({"ref": _string_schema(), "tail": _integer_schema(), "type": _string_schema(), "status": _string_schema(), "timeout_seconds": _number_schema(default=300.0)}, required=["ref"]),
                "handler": self._tool_get_experiment_events,
            },
            "doctor_experiment": {
                "description": "Check local plan-based experiment state and artifact refs.",
                "inputSchema": _object_schema({"ref": _string_schema(), "timeout_seconds": _number_schema(default=300.0)}, required=["ref"]),
                "handler": self._tool_doctor_experiment,
            },
            "collect_experiment": {
                "description": "Collect status, optional report export, and optional comparison for a tracked experiment.",
                "inputSchema": _object_schema(
                    {
                        "ref": _string_schema(),
                        "bot_id": _string_schema(),
                        "wait": _boolean_schema(default=False),
                        "wait_timeout": _number_schema(default=3600.0),
                        "interval": _number_schema(default=30.0),
                        "print_each": _boolean_schema(default=False),
                        "allow_non_completed": _boolean_schema(default=False),
                        "export": _boolean_schema(default=False),
                        "out_dir": _string_schema(),
                        "no_json": _boolean_schema(default=False),
                        "no_csv": _boolean_schema(default=False),
                        "include_candles": _boolean_schema(default=False),
                        "compare_to": _string_schema(),
                        "no_golden": _boolean_schema(default=False),
                        "require_golden": _boolean_schema(default=False),
                        "confirm": _boolean_schema(default=False),
                        "timeout_seconds": _number_schema(default=7200.0),
                    },
                    required=["ref"],
                ),
                "handler": self._tool_collect_experiment,
            },
            "start_bot_run": {
                "description": "Start a bot run. Requires confirm=true; paper/live require allow_non_backtest=true.",
                "inputSchema": _object_schema(
                    {
                        "bot_id": _string_schema(),
                        "request_id": _string_schema(),
                        "run_type": {"type": "string", "enum": ["backtest", "sim_trade", "paper", "live"], "default": "backtest"},
                        "execution_behavior": {"type": "string", "enum": ["simulated", "observe-only"]},
                        "duration_seconds": _number_schema(),
                        "market_data_stream_policy": _free_object_schema(),
                        "allow_non_backtest": _boolean_schema(default=False),
                        "confirm": _boolean_schema(default=False),
                    },
                    required=["bot_id"],
                ),
                "handler": self._tool_start_bot_run,
            },
            "stop_bot_run": {
                "description": "Stop a bot run. Requires confirm=true.",
                "inputSchema": _object_schema({"bot_id": _string_schema(), "run_id": _string_schema(), "request_id": _string_schema(), "preserve_container": _boolean_schema(default=False), "confirm": _boolean_schema(default=False)}, required=["bot_id"]),
                "handler": self._tool_stop_bot_run,
            },
            "update_bot_backtest_window": {
                "description": "Plan or apply a bot backtest window update. Defaults to a dry planned mutation; apply requires apply=true and confirm=true.",
                "inputSchema": _object_schema({"bot_id": _string_schema(), "backtest_start": _string_schema(), "backtest_end": _string_schema(), "apply": _boolean_schema(default=False), "confirm": _boolean_schema(default=False)}, required=["bot_id", "backtest_start", "backtest_end"]),
                "handler": self._tool_update_bot_backtest_window,
            },
            "set_bot_strategy_variant": {
                "description": "Plan or apply a bot strategy/variant selection update. Defaults to a dry planned mutation; apply requires apply=true and confirm=true.",
                "inputSchema": _object_schema({"bot_id": _string_schema(), "strategy_id": _string_schema(), "variant_id": _string_schema(), "variant_name": _string_schema(), "apply": _boolean_schema(default=False), "confirm": _boolean_schema(default=False)}, required=["bot_id"]),
                "handler": self._tool_set_bot_strategy_variant,
            },
            "create_strategy_variant": {
                "description": "Plan or apply creation of a strategy variant. Defaults to a dry planned mutation; apply requires apply=true and confirm=true.",
                "inputSchema": _object_schema({"strategy_id": _string_schema(), "name": _string_schema(), "description": _string_schema(), "output_filters": {"type": "array", "items": _free_object_schema()}, "is_default": _boolean_schema(default=False), "apply": _boolean_schema(default=False), "confirm": _boolean_schema(default=False)}, required=["strategy_id", "name"]),
                "handler": self._tool_create_strategy_variant,
            },
            "update_strategy_variant": {
                "description": "Plan or apply update of a strategy variant. Defaults to a dry planned mutation; apply requires apply=true and confirm=true.",
                "inputSchema": _object_schema({"strategy_id": _string_schema(), "variant_id": _string_schema(), "name": _string_schema(), "description": _string_schema(), "output_filters": {"type": "array", "items": _free_object_schema()}, "is_default": _boolean_schema(), "apply": _boolean_schema(default=False), "confirm": _boolean_schema(default=False)}, required=["strategy_id", "variant_id"]),
                "handler": self._tool_update_strategy_variant,
            },
        }

    def _error_response(self, request_id: Any, code: int, message: str, data: Any | None = None) -> dict[str, Any]:
        error: dict[str, Any] = {"code": code, "message": message}
        if data is not None:
            error["data"] = data
        return {"jsonrpc": "2.0", "id": request_id, "error": error}


def serve_stdio(server: QuantTradMcpServer) -> int:
    for line in sys.stdin:
        raw = line.strip()
        if not raw:
            continue
        try:
            message = json.loads(raw)
        except json.JSONDecodeError as exc:
            response = {"jsonrpc": "2.0", "id": None, "error": {"code": -32700, "message": str(exc)}}
        else:
            if not isinstance(message, dict):
                response = {"jsonrpc": "2.0", "id": None, "error": {"code": -32600, "message": "JSON-RPC message must be an object"}}
            else:
                response = server.handle(message)
        if response is not None:
            sys.stdout.write(json.dumps(response, separators=(",", ":"), default=str) + "\n")
            sys.stdout.flush()
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Quant-Trad MCP stdio server.")
    parser.add_argument("--api-url", default=os.environ.get("QT_API_URL", "http://127.0.0.1:8000"))
    parser.add_argument("--timeout", type=float, default=float(os.environ.get("QT_API_TIMEOUT", "30")))
    parser.add_argument("--log-root", default=os.environ.get("QT_CLI_LOG_ROOT", "logs"))
    parser.add_argument(
        "--command-timeout",
        type=float,
        default=float(os.environ.get("QT_MCP_COMMAND_TIMEOUT_SECONDS", "7200")),
        help="Timeout for long-running qt command tools.",
    )
    args = parser.parse_args(argv)
    return serve_stdio(
        QuantTradMcpServer(
            api_url=args.api_url,
            timeout=args.timeout,
            log_root=args.log_root,
            command_timeout_seconds=args.command_timeout,
        )
    )


def _parse_quanttrad_uri(uri: str) -> tuple[list[str], dict[str, list[str]]]:
    parsed = urlparse(uri)
    if parsed.scheme != "quanttrad":
        raise McpError("resource URI must use quanttrad:// scheme", code=-32602)
    parts = [unquote(parsed.netloc), *[unquote(item) for item in parsed.path.split("/") if item]]
    return [part for part in parts if part], parse_qs(parsed.query)


def _resource(uri: str, name: str) -> dict[str, str]:
    return {"uri": uri, "name": name, "description": name, "mimeType": "application/json"}


def _template(uri_template: str, name: str) -> dict[str, str]:
    return {"uriTemplate": uri_template, "name": name, "description": name, "mimeType": "application/json"}


def _object_schema(properties: dict[str, Any] | None = None, *, required: list[str] | None = None) -> dict[str, Any]:
    schema: dict[str, Any] = {"type": "object", "properties": properties or {}, "additionalProperties": False}
    if required:
        schema["required"] = required
    return schema


def _free_object_schema() -> dict[str, Any]:
    return {"type": "object", "additionalProperties": True}


def _string_schema(default: str | None = None) -> dict[str, Any]:
    schema: dict[str, Any] = {"type": "string"}
    if default is not None:
        schema["default"] = default
    return schema


def _integer_schema(default: int | None = None) -> dict[str, Any]:
    schema: dict[str, Any] = {"type": "integer"}
    if default is not None:
        schema["default"] = default
    return schema


def _number_schema(default: float | None = None) -> dict[str, Any]:
    schema: dict[str, Any] = {"type": "number"}
    if default is not None:
        schema["default"] = default
    return schema


def _boolean_schema(default: bool | None = None) -> dict[str, Any]:
    schema: dict[str, Any] = {"type": "boolean"}
    if default is not None:
        schema["default"] = default
    return schema


def _ensure_object(payload: Any, context: str) -> dict[str, Any]:
    if not isinstance(payload, dict):
        raise McpError(f"{context} returned unexpected payload type: {type(payload).__name__}")
    return payload


def _required_str(arguments: dict[str, Any], key: str) -> str:
    value = arguments.get(key)
    if value is None or str(value).strip() == "":
        raise McpError(f"{key} is required", code=-32602)
    return str(value)


def _optional_str(arguments: dict[str, Any], key: str, default: str | None = None) -> str | None:
    value = arguments.get(key)
    if value is None or str(value).strip() == "":
        return default
    return str(value)


def _required_object(arguments: dict[str, Any], key: str) -> dict[str, Any]:
    value = arguments.get(key)
    if not isinstance(value, dict):
        raise McpError(f"{key} must be an object", code=-32602)
    return value


def _optional_int(arguments: dict[str, Any], key: str, default: int | None = None) -> int:
    value = arguments.get(key)
    if value is None:
        if default is None:
            raise McpError(f"{key} is required", code=-32602)
        return default
    return int(value)


def _optional_float(arguments: dict[str, Any], key: str, default: float) -> float:
    value = arguments.get(key)
    return float(default if value is None else value)


def _optional_bool(arguments: dict[str, Any], key: str, default: bool) -> bool:
    value = arguments.get(key)
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "y", "on"}
    return bool(value)


def _query_str(query: dict[str, list[str]], key: str, default: str | None = None) -> str | None:
    values = query.get(key)
    return values[0] if values else default


def _query_int(query: dict[str, list[str]], key: str, default: int | None = None) -> int:
    value = _query_str(query, key, None)
    if value is None:
        if default is None:
            raise McpError(f"{key} is required", code=-32602)
        return default
    return int(value)


def _require_confirm(arguments: dict[str, Any], message: str) -> None:
    if not _optional_bool(arguments, "confirm", False):
        raise McpError(f"{message}; pass confirm=true to proceed", code=-32602)


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
