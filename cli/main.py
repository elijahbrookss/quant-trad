from __future__ import annotations

import argparse
import getpass
import json
import os
import sys
import time
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Mapping
from urllib.parse import quote

from .api import ApiClient, ApiError, filename_from_content_disposition
from .audit import CliAuditLog, experiment_dir, report_export_dir, safe_path_part
from .experiments.data_preflight import data_preflight_requires_proceed, run_plan_data_preflight
from .experiments.doctor import doctor_experiment
from .experiments.event_log import read_events
from .experiments.instrument_matrix import prepare_instrument_matrix_experiment
from .experiments.plan_loader import load_plan, plan_preview
from .experiments.runner import ExperimentRunner
from .experiments.state_store import ExperimentStateStore, find_experiment_dir
from .experiments.summarize import summarize_experiment, write_experiment_summary
from .logs import DEFAULT_LOKI_URL, LokiClient, doctor_log_payload, query_log_payload, run_log_payload
from .research_operations import ResearchOperations
from .setup import setup_doctor_payload, setup_env_payload


TERMINAL_STATUSES = {
    "completed",
    "failed",
    "crashed",
    "canceled",
    "cancelled",
    "startup_failed",
    "degraded_terminal",
    "stopped",
}


def _print_json(payload: Any, *, indent: int = 2) -> None:
    print(json.dumps(payload, indent=indent, sort_keys=True, default=str), flush=True)


def _json_value(raw: str) -> Any:
    value = str(raw)
    try:
        return json.loads(value)
    except json.JSONDecodeError:
        return value


def _key_value_map(items: list[str] | None) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for item in items or []:
        if "=" not in item:
            raise ValueError(f"expected key=value, got {item!r}")
        key, value = item.split("=", 1)
        key = key.strip()
        if not key:
            raise ValueError(f"expected non-empty key in {item!r}")
        result[key] = _json_value(value)
    return result


def _read_json_object(path: str | None) -> dict[str, Any]:
    if not path:
        return {}
    raw = sys.stdin.read() if path == "-" else Path(path).expanduser().read_text(encoding="utf-8")
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ValueError(f"invalid JSON object in {path}: {exc}") from exc
    if not isinstance(payload, dict):
        raise ValueError(f"expected JSON object in {path}")
    return payload


def _read_json_object_arg(value: str | None, *, label: str) -> dict[str, Any]:
    if not value:
        return {}
    text = str(value).strip()
    raw = sys.stdin.read() if text == "-" else text if text.startswith("{") else Path(text).expanduser().read_text(encoding="utf-8")
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ValueError(f"invalid JSON object for {label}: {exc}") from exc
    if not isinstance(payload, dict):
        raise ValueError(f"{label} must be a JSON object")
    return payload


def _read_json_array_or_object_arg(value: str | None, *, label: str) -> Any:
    if not value:
        return None
    text = str(value).strip()
    raw = sys.stdin.read() if text == "-" else text if text.startswith(("{", "[")) else Path(text).expanduser().read_text(encoding="utf-8")
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ValueError(f"invalid JSON for {label}: {exc}") from exc
    if not isinstance(payload, (dict, list)):
        raise ValueError(f"{label} must be a JSON object or array")
    return payload


def _read_json_filters(path: str | None) -> list[dict[str, Any]]:
    if not path:
        return []
    raw = sys.stdin.read() if path == "-" else Path(path).expanduser().read_text(encoding="utf-8")
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ValueError(f"invalid JSON in {path}: {exc}") from exc
    if isinstance(payload, dict):
        return [payload]
    if isinstance(payload, list) and all(isinstance(item, dict) for item in payload):
        return list(payload)
    raise ValueError(f"expected JSON object or array of objects in {path}")


def _merge_json_object_and_params(path: str | None, params: list[str] | None) -> dict[str, Any]:
    payload = _read_json_object(path)
    payload.update(_key_value_map(params))
    return payload


def _build_output_filters(args: argparse.Namespace) -> list[dict[str, Any]]:
    output_filters = _read_json_filters(getattr(args, "filters_json", None))
    for raw_filter in getattr(args, "filter", None) or []:
        try:
            payload = json.loads(raw_filter)
        except json.JSONDecodeError as exc:
            raise ValueError(f"invalid --filter JSON object: {exc}") from exc
        if not isinstance(payload, dict):
            raise ValueError("--filter must be a JSON object")
        output_filters.append(payload)

    indicator_id = str(getattr(args, "indicator_id", "") or "").strip()
    output_name = str(getattr(args, "output_name", "") or "").strip()
    field = str(getattr(args, "field", "") or "").strip()
    value = getattr(args, "value", None)
    equals = getattr(args, "equals", None)
    if equals is not None:
        value = equals
        args.operator = "equals"
    if indicator_id or output_name or field or value is not None:
        if not indicator_id or not output_name or not field or value is None:
            raise ValueError("--indicator-id, --output-name, --field, and --value/--equals are required together")
        scope: dict[str, Any] = {}
        intents = [str(item).strip() for item in getattr(args, "intent", []) or [] if str(item).strip()]
        rule_ids = [str(item).strip() for item in getattr(args, "rule_id", []) or [] if str(item).strip()]
        if intents:
            scope["intent"] = intents
        if rule_ids:
            scope["rule_ids"] = rule_ids
        output_filters.append(
            {
                "scope": scope,
                "indicator_id": indicator_id,
                "output_name": output_name,
                "field": field,
                "operator": str(getattr(args, "operator", None) or "equals"),
                "value": _json_value(str(value)),
            }
        )
    return output_filters


def _client(args: argparse.Namespace) -> ApiClient:
    audit = getattr(args, "_audit_log", None)

    def _observe(event: str, fields: dict[str, Any]) -> None:
        if audit is not None:
            audit.record_event(event, **fields)

    return ApiClient(args.api_url, timeout=float(args.timeout), observer=_observe)


def _export_root(args: argparse.Namespace) -> str:
    if getattr(args, "out_dir", None):
        return str(args.out_dir)
    return str(Path(getattr(args, "log_root", "logs") or "logs") / "reports")


def _experiment_root(args: argparse.Namespace) -> Path:
    return Path(getattr(args, "log_root", "logs") or "logs")


def _experiment_record_file(args: argparse.Namespace, experiment_id: str) -> Path:
    return experiment_dir(_experiment_root(args), experiment_id=experiment_id) / "experiment.json"


def _write_experiment_record(args: argparse.Namespace, record: dict[str, Any]) -> dict[str, Any]:
    experiment_id = str(record.get("experiment_id") or record.get("request_id") or record.get("run_id") or "").strip()
    if not experiment_id:
        raise ValueError("experiment_id, request_id, or run_id is required for experiment record")
    path = _experiment_record_file(args, experiment_id)
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "schema_version": "qt_cli_experiment.v1",
        **record,
        "experiment_id": experiment_id,
        "paths": {
            **dict(record.get("paths") or {}),
            "experiment_dir": str(path.parent),
            "record": str(path),
        },
    }
    path.write_text(json.dumps(payload, indent=2, sort_keys=True, default=str) + "\n", encoding="utf-8")
    audit = getattr(args, "_audit_log", None)
    if audit is not None:
        audit.record_artifact(
            "experiment_record",
            path,
            experiment_id=experiment_id,
            bot_id=payload.get("bot_id"),
            run_id=payload.get("run_id"),
            request_id=payload.get("request_id"),
        )
    return payload


def _load_experiment_record(args: argparse.Namespace, ref: str, *, bot_id: str | None = None) -> dict[str, Any]:
    raw_ref = str(ref or "").strip()
    if not raw_ref:
        raise ValueError("experiment reference is required")
    candidate = Path(raw_ref).expanduser()
    if candidate.exists():
        path = candidate if candidate.is_file() else candidate / "experiment.json"
        if not path.exists():
            raise ValueError(f"experiment record not found at {path}")
        return json.loads(path.read_text(encoding="utf-8"))

    root = _experiment_root(args) / "experiments"
    safe_ref = safe_path_part(raw_ref)
    direct_matches = list(root.glob(f"**/{safe_ref}/experiment.json")) if root.exists() else []
    for path in direct_matches:
        return json.loads(path.read_text(encoding="utf-8"))
    if root.exists():
        for path in root.glob("**/experiment.json"):
            payload = json.loads(path.read_text(encoding="utf-8"))
            if raw_ref in {
                str(payload.get("experiment_id") or ""),
                str(payload.get("request_id") or ""),
                str(payload.get("run_id") or ""),
            }:
                return payload
    if bot_id:
        return {
            "schema_version": "qt_cli_experiment.v1",
            "experiment_id": raw_ref,
            "bot_id": bot_id,
            "run_id": raw_ref,
        }
    raise ValueError(f"experiment record not found for {raw_ref!r}; pass --bot-id to use a raw run id")


def _load_experiment_suite_state(args: argparse.Namespace, ref: str) -> dict[str, Any] | None:
    try:
        path = find_experiment_dir(_experiment_root(args), ref)
        store = ExperimentStateStore(_experiment_root(args), path=path)
        if not store.state_path.exists():
            return None
        return store.load_state()
    except ValueError:
        return None


def _validate_plan_payload(args: argparse.Namespace, plan: dict[str, Any]) -> dict[str, Any]:
    payload = plan_preview(plan)
    if not bool(getattr(args, "skip_data_preflight", False)):
        payload["data_preflight"] = run_plan_data_preflight(_client(args), plan)
    return payload


def _prompt_for_data_preflight(args: argparse.Namespace, data_preflight: dict[str, Any] | None) -> None:
    if not data_preflight or not data_preflight_requires_proceed(data_preflight):
        return
    if bool(getattr(args, "proceed_with_data_warnings", False)):
        return
    summary = data_preflight.get("summary") if isinstance(data_preflight.get("summary"), dict) else {}
    message = (
        f"Data preflight status={data_preflight.get('status')} "
        f"warnings={summary.get('warnings', 0)} errors={summary.get('errors', 0)}. Proceed? [y/N] "
    )
    if not sys.stdin.isatty():
        raise ValueError(
            "data preflight found warnings/errors; rerun with --proceed-with-data-warnings to start runs anyway"
        )
    answer = input(message).strip().lower()
    if answer not in {"y", "yes"}:
        raise ValueError("experiment run aborted by data preflight prompt")


def _terminal_status(payload: dict[str, Any]) -> str:
    status = payload.get("status")
    if status:
        return str(status).strip().lower()
    status = payload.get("run_status")
    if status:
        return str(status).strip().lower()
    summary = payload.get("summary")
    if isinstance(summary, dict):
        for key in ("run_status", "status", "phase"):
            value = summary.get(key)
            if value:
                return str(value).strip().lower()
    return ""


def _wait_for_run(
    client: ApiClient,
    *,
    bot_id: str,
    run_id: str,
    timeout: float,
    interval: float,
    print_each: bool,
    allow_non_completed: bool,
    emit_final: bool = True,
) -> tuple[int, dict[str, Any]]:
    deadline = time.monotonic() + float(timeout)
    last_payload: dict[str, Any] = {}
    while True:
        payload = client.request_json("GET", f"/api/bots/{bot_id}/runs/{run_id}/status")
        if not isinstance(payload, dict):
            raise ApiError(f"GET run status returned unexpected payload type: {type(payload).__name__}")
        last_payload = payload
        if print_each:
            _print_json(payload)
        status = _terminal_status(payload)
        if status in TERMINAL_STATUSES:
            if emit_final and not print_each:
                _print_json(payload)
            return (0 if status == "completed" or allow_non_completed else 1), payload
        if time.monotonic() >= deadline:
            timeout_payload = {**last_payload, "wait_status": "timeout", "timeout_seconds": timeout}
            if emit_final and not print_each:
                _print_json(timeout_payload)
            return 124, timeout_payload
        time.sleep(float(interval))


def _write_report_export(
    args: argparse.Namespace,
    client: ApiClient,
    *,
    run_id: str,
    include_json: bool,
    include_csv: bool,
    include_candles: bool,
) -> dict[str, Any]:
    response = client.request_bytes(
        "POST",
        f"/api/reports/{run_id}/export",
        payload={
            "include_json": include_json,
            "include_csv": include_csv,
            "include_candles": include_candles,
        },
    )
    headers = {key.lower(): value for key, value in response.headers.items()}
    filename = filename_from_content_disposition(
        headers.get("content-disposition"),
        f"run_{run_id}_report_export.zip",
    )
    output_dir = report_export_dir(_export_root(args), run_id=run_id)
    output_dir.mkdir(parents=True, exist_ok=True)
    path = output_dir / filename
    path.write_bytes(response.body)
    audit = getattr(args, "_audit_log", None)
    if audit is not None:
        audit.record_artifact(
            "report_export",
            path,
            run_id=run_id,
            filename=filename,
            size_bytes=len(response.body),
            include_json=include_json,
            include_csv=include_csv,
            include_candles=include_candles,
        )
    return {
        "run_id": run_id,
        "path": str(path),
        "partition": str(output_dir),
        "filename": filename,
        "size_bytes": len(response.body),
    }


def _cmd_health(args: argparse.Namespace) -> int:
    _print_json(_client(args).request_json("GET", "/api/health"))
    return 0


def _cmd_mcp_serve(args: argparse.Namespace) -> int:
    from cli.mcp_server import QuantTradMcpServer, serve_stdio

    return serve_stdio(
        QuantTradMcpServer(
            api_url=args.api_url,
            timeout=float(args.timeout),
            log_root=str(args.log_root),
            command_timeout_seconds=float(args.command_timeout),
        )
    )


def _cmd_bots_list(args: argparse.Namespace) -> int:
    _print_json(_client(args).request_json("GET", "/api/bots/run-contexts"))
    return 0


def _cmd_bots_get(args: argparse.Namespace) -> int:
    _print_json(_client(args).request_json("GET", f"/api/bots/{args.bot_id}/run-context"))
    return 0


def _cmd_bots_active(args: argparse.Namespace) -> int:
    _print_json(_client(args).request_json("GET", f"/api/bots/{args.bot_id}/active-run"))
    return 0


def _cmd_bots_runs(args: argparse.Namespace) -> int:
    _print_json(_client(args).request_json("GET", f"/api/bots/{args.bot_id}/runs", params={"limit": args.limit}))
    return 0


def _bot_write_payload(args: argparse.Namespace, *, require_name: bool = False) -> dict[str, Any]:
    payload = _read_json_object_arg(getattr(args, "payload_json", None), label="--payload-json")
    fields = {
        "name": "name",
        "strategy_id": "strategy_id",
        "variant_id": "strategy_variant_id",
        "variant_name": "strategy_variant_name",
        "atm_template_id": "atm_template_id",
        "datasource": "datasource",
        "exchange": "exchange",
        "mode": "mode",
        "execution_mode": "execution_mode",
        "execution_behavior": "execution_behavior",
        "run_type": "run_type",
        "backtest_start": "backtest_start",
        "backtest_end": "backtest_end",
        "snapshot_interval_ms": "snapshot_interval_ms",
        "execution_semantics": "execution_semantics",
    }
    for arg_name, payload_name in fields.items():
        value = getattr(args, arg_name, None)
        if value is not None:
            payload[payload_name] = value
    if getattr(args, "wallet_json", None):
        payload["wallet_config"] = _read_json_object_arg(args.wallet_json, label="--wallet-json")
    if getattr(args, "market_data_stream_policy_json", None):
        payload["market_data_stream_policy"] = _read_json_object_arg(
            args.market_data_stream_policy_json,
            label="--market-data-stream-policy-json",
        )
    if getattr(args, "risk_config_json", None):
        payload["risk_config"] = _read_json_object_arg(args.risk_config_json, label="--risk-config-json")
    if getattr(args, "bot_env_json", None):
        payload["bot_env"] = _read_json_object_arg(args.bot_env_json, label="--bot-env-json")
    if require_name and not str(payload.get("name") or "").strip():
        raise ValueError("name is required")
    return payload


def _cmd_bots_create(args: argparse.Namespace) -> int:
    payload = _bot_write_payload(args, require_name=True)
    _print_json(_client(args).request_json("POST", "/api/bots", payload=payload))
    return 0


def _cmd_bots_update(args: argparse.Namespace) -> int:
    payload = _bot_write_payload(args)
    if not payload:
        raise ValueError("at least one update field is required")
    _print_json(_client(args).request_json("PUT", f"/api/bots/{args.bot_id}", payload=payload))
    return 0


def _cmd_bots_start(args: argparse.Namespace) -> int:
    body = {"economic_claim_intent": str(args.economic_claim_intent)}
    if args.request_id:
        body["request_id"] = args.request_id
    if getattr(args, "run_type", None):
        body["run_type"] = args.run_type
    if getattr(args, "dataset_id", None):
        body["dataset_id"] = args.dataset_id
    if bool(getattr(args, "profile", False)):
        body["profile"] = True
    if getattr(args, "execution_behavior", None):
        body["execution_behavior"] = args.execution_behavior
    if getattr(args, "duration_seconds", None):
        body["duration_seconds"] = args.duration_seconds
    if getattr(args, "market_data_stream_policy_json", None):
        body["market_data_stream_policy"] = _read_json_object_arg(
            args.market_data_stream_policy_json,
            label="--market-data-stream-policy-json",
        )
    if getattr(args, "execution_assumptions_json", None):
        body["execution_assumptions"] = _read_json_object_arg(
            args.execution_assumptions_json,
            label="--execution-assumptions-json",
        )
    _print_json(_client(args).request_json("POST", f"/api/bots/{args.bot_id}/runs/start", payload=body))
    return 0


def _cmd_bots_stop(args: argparse.Namespace) -> int:
    payload: dict[str, Any] = {"preserve_container": bool(args.preserve_container)}
    if args.run_id:
        payload["run_id"] = args.run_id
    if args.request_id:
        payload["request_id"] = args.request_id
    _print_json(_client(args).request_json("POST", f"/api/bots/{args.bot_id}/stop", payload=payload))
    return 0


def _cmd_bots_set_strategy(args: argparse.Namespace) -> int:
    payload: dict[str, Any] = {}
    if args.strategy_id:
        payload["strategy_id"] = args.strategy_id
    if args.variant_id:
        payload["strategy_variant_id"] = args.variant_id
    if args.variant_name:
        payload["strategy_variant_name"] = args.variant_name
    if not payload:
        raise ValueError("at least one strategy or variant field is required")
    _print_json(_client(args).request_json("PUT", f"/api/bots/{args.bot_id}", payload=payload))
    return 0


def _cmd_runs_wait(args: argparse.Namespace) -> int:
    code, _payload = _wait_for_run(
        _client(args),
        bot_id=args.bot_id,
        run_id=args.run_id,
        timeout=args.wait_timeout,
        interval=args.interval,
        print_each=args.print_each,
        allow_non_completed=args.allow_non_completed,
        emit_final=True,
    )
    return code


def _loki_client(args: argparse.Namespace) -> LokiClient:
    return LokiClient(str(getattr(args, "loki_url", None) or DEFAULT_LOKI_URL), timeout=float(args.timeout))


def _cmd_logs_run(args: argparse.Namespace) -> int:
    payload = run_log_payload(
        client=_loki_client(args),
        run_id=args.run_id,
        bot_id=getattr(args, "bot_id", None),
        start=getattr(args, "start", None),
        end=getattr(args, "end", None),
        lookback_hours=float(getattr(args, "lookback_hours", 6.0)),
        limit=int(getattr(args, "limit", 500)),
    )
    _print_json(payload)
    return 0


def _cmd_logs_query(args: argparse.Namespace) -> int:
    payload = query_log_payload(
        client=_loki_client(args),
        logql=args.logql,
        start=getattr(args, "start", None),
        end=getattr(args, "end", None),
        lookback_hours=float(getattr(args, "lookback_hours", 6.0)),
        limit=int(getattr(args, "limit", 500)),
    )
    _print_json(payload)
    return 0


def _cmd_logs_doctor(args: argparse.Namespace) -> int:
    payload = doctor_log_payload(
        client=_loki_client(args),
        start=getattr(args, "start", None),
        end=getattr(args, "end", None),
        lookback_hours=float(getattr(args, "lookback_hours", 24.0)),
    )
    _print_json(payload)
    return 0


def _cmd_strategies_list(args: argparse.Namespace) -> int:
    _print_json(_client(args).request_json("GET", "/api/strategies/"))
    return 0


def _cmd_strategies_create(args: argparse.Namespace) -> int:
    payload = _read_json_object_arg(args.payload_json, label="--payload-json")
    _print_json(_client(args).request_json("POST", "/api/strategies/", payload=payload))
    return 0


def _cmd_strategies_get(args: argparse.Namespace) -> int:
    _print_json(_client(args).request_json("GET", f"/api/strategies/{args.strategy_id}"))
    return 0


def _cmd_strategies_bindings(args: argparse.Namespace) -> int:
    _print_json(_client(args).request_json("GET", f"/api/strategies/{args.strategy_id}/bindings"))
    return 0


def _cmd_strategies_rules(args: argparse.Namespace) -> int:
    _print_json(_client(args).request_json("GET", f"/api/strategies/{args.strategy_id}/rules"))
    return 0


def _cmd_strategies_rule_create(args: argparse.Namespace) -> int:
    payload = _read_json_object_arg(args.payload_json, label="--payload-json")
    _print_json(_client(args).request_json("POST", f"/api/strategies/{args.strategy_id}/rules", payload=payload))
    return 0


def _variant_query(args: argparse.Namespace) -> str:
    params: list[str] = []
    if getattr(args, "variant_id", None):
        params.append(f"variant_id={quote(str(args.variant_id), safe='')}")
    if getattr(args, "variant_name", None):
        params.append(f"variant_name={quote(str(args.variant_name), safe='')}")
    return f"?{'&'.join(params)}" if params else ""


def _cmd_strategies_effective(args: argparse.Namespace) -> int:
    _print_json(
        _client(args).request_json(
            "GET",
            f"/api/strategies/{args.strategy_id}/effective{_variant_query(args)}",
        )
    )
    return 0


def _cmd_strategies_decision_inputs(args: argparse.Namespace) -> int:
    _print_json(
        _client(args).request_json(
            "GET",
            f"/api/strategies/{args.strategy_id}/decision-inputs{_variant_query(args)}",
        )
    )
    return 0


def _cmd_strategies_compile(args: argparse.Namespace) -> int:
    payload: dict[str, Any] = {}
    if args.variant_id:
        payload["variant_id"] = args.variant_id
    if args.variant_name:
        payload["variant_name"] = args.variant_name
    _print_json(_client(args).request_json("POST", f"/api/strategies/{args.strategy_id}/compile", payload=payload))
    return 0


def _strategy_preview_request_payload(args: argparse.Namespace) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "start": args.start,
        "end": args.end,
        "interval": args.interval,
        "instrument_ids": list(args.instrument_id or []),
    }
    if args.variant_id:
        payload["variant_id"] = args.variant_id
    if args.variant_name:
        payload["variant_name"] = args.variant_name
    return payload


def _strategy_preview_signals_view(summary: Mapping[str, Any]) -> dict[str, Any]:
    signals: list[dict[str, Any]] = []
    instruments = summary.get("instruments")
    if not isinstance(instruments, dict):
        raise ValueError("strategy preview summary is missing instruments")
    for instrument in instruments.values():
        if not isinstance(instrument, dict):
            continue
        rows = instrument.get("signals_detail")
        if isinstance(rows, list):
            signals.extend(dict(row) for row in rows if isinstance(row, dict))
        else:
            signals.extend(dict(row) for row in instrument.get("examples") or [] if isinstance(row, dict))
    signals.sort(key=lambda row: (int(row.get("bar_epoch") or 0), str(row.get("signal_id") or "")))
    return {
        "schema_version": "strategy_preview_signals.v1",
        "preview_id": summary.get("preview_id"),
        "strategy_id": summary.get("strategy_id"),
        "strategy_name": summary.get("strategy_name"),
        "total": len(signals),
        "signals": signals,
    }


def _strategy_preview_empty_view(summary: Mapping[str, Any]) -> dict[str, Any]:
    instruments = summary.get("instruments")
    if not isinstance(instruments, dict):
        raise ValueError("strategy preview summary is missing instruments")
    return {
        "schema_version": "strategy_preview_empty_diagnostics.v1",
        "preview_id": summary.get("preview_id"),
        "strategy_id": summary.get("strategy_id"),
        "strategy_name": summary.get("strategy_name"),
        "instruments": {
            str(instrument_id): {
                "instrument_id": instrument.get("instrument_id"),
                "symbol": instrument.get("symbol"),
                "signals": instrument.get("signals"),
                "why_empty": list(instrument.get("why_empty") or []),
            }
            for instrument_id, instrument in instruments.items()
            if isinstance(instrument, dict)
        },
    }


def _cmd_strategies_preview(args: argparse.Namespace) -> int:
    if args.full and (args.signals or args.why_empty):
        raise ValueError("--full cannot be combined with --signals or --why-empty")
    payload = _strategy_preview_request_payload(args)
    if args.full:
        _print_json(_client(args).request_json("POST", f"/api/strategies/{args.strategy_id}/preview", payload=payload))
        return 0
    payload["max_examples"] = args.examples
    payload["include_signals"] = bool(args.signals)
    summary = _client(args).request_json("POST", f"/api/strategies/{args.strategy_id}/preview/summary", payload=payload)
    if args.signals:
        _print_json(_strategy_preview_signals_view(summary))
    elif args.why_empty:
        _print_json(_strategy_preview_empty_view(summary))
    else:
        _print_json(summary)
    return 0


def _parse_strategy_preview_case(raw: str) -> dict[str, Any]:
    text = str(raw or "").strip()
    if not text:
        raise ValueError("--case cannot be empty")
    label = None
    body = text
    if "=" in text:
        label, body = text.split("=", 1)
        label = label.strip() or None
    if ":" not in body:
        raise ValueError("--case must look like LABEL=STRATEGY_ID:INSTRUMENT_ID[,INSTRUMENT_ID]")
    strategy_id, instruments_raw = body.split(":", 1)
    instrument_ids = [item.strip() for item in instruments_raw.split(",") if item.strip()]
    if not strategy_id.strip() or not instrument_ids:
        raise ValueError("--case requires strategy_id and at least one instrument_id")
    payload: dict[str, Any] = {
        "strategy_id": strategy_id.strip(),
        "instrument_ids": instrument_ids,
    }
    if label:
        payload["label"] = label
    return payload


def _strategy_preview_compare_cases(args: argparse.Namespace) -> list[dict[str, Any]]:
    cases: list[dict[str, Any]] = []
    for raw in args.case or []:
        cases.append(_parse_strategy_preview_case(raw))
    for raw_json in args.case_json or []:
        payload = _read_json_array_or_object_arg(raw_json, label="--case-json")
        if isinstance(payload, list):
            for item in payload:
                if not isinstance(item, dict):
                    raise ValueError("--case-json array items must be JSON objects")
                cases.append(item)
        elif isinstance(payload, dict):
            cases.append(payload)
    if not cases:
        raise ValueError("at least one --case or --case-json is required")
    return cases


def _cmd_strategies_preview_compare(args: argparse.Namespace) -> int:
    payload: dict[str, Any] = {
        "start": args.start,
        "end": args.end,
        "interval": args.interval,
        "cases": _strategy_preview_compare_cases(args),
        "max_examples": args.examples,
        "include_signals": bool(args.signals),
    }
    _print_json(_client(args).request_json("POST", "/api/strategies/preview/compare", payload=payload))
    return 0


def _cmd_variants_list(args: argparse.Namespace) -> int:
    _print_json(_client(args).request_json("GET", f"/api/strategies/{args.strategy_id}/variants"))
    return 0


def _cmd_variants_create(args: argparse.Namespace) -> int:
    payload = {
        "name": args.name,
        "description": args.description,
        "output_filters": _build_output_filters(args),
        "is_default": args.is_default,
    }
    _print_json(_client(args).request_json("POST", f"/api/strategies/{args.strategy_id}/variants", payload=payload))
    return 0


def _cmd_variants_update(args: argparse.Namespace) -> int:
    payload: dict[str, Any] = {}
    if args.name is not None:
        payload["name"] = args.name
    if args.description is not None:
        payload["description"] = args.description
    if args.is_default:
        payload["is_default"] = True
    output_filters = _build_output_filters(args)
    if args.replace_filters or output_filters:
        payload["output_filters"] = output_filters
    if not payload:
        raise ValueError("at least one variant field is required")
    _print_json(
        _client(args).request_json(
            "PUT",
            f"/api/strategies/{args.strategy_id}/variants/{args.variant_id}",
            payload=payload,
        )
    )
    return 0


def _cmd_variants_delete(args: argparse.Namespace) -> int:
    _client(args).request_bytes("DELETE", f"/api/strategies/{args.strategy_id}/variants/{args.variant_id}")
    _print_json({"deleted": True, "strategy_id": args.strategy_id, "variant_id": args.variant_id})
    return 0


def _indicator_payload_from_args(args: argparse.Namespace, base: dict[str, Any] | None = None) -> dict[str, Any]:
    payload = dict(base or {})
    payload.update(_read_json_object_arg(getattr(args, "payload_json", None), label="--payload-json"))
    if getattr(args, "type", None) is not None:
        payload["type"] = args.type
    if getattr(args, "name", None) is not None:
        payload["name"] = args.name
    if getattr(args, "params_json", None) is not None:
        payload["params"] = _read_json_object_arg(args.params_json, label="--params-json")
    params = _key_value_map(getattr(args, "param", None))
    if params:
        payload.setdefault("params", {})
        if not isinstance(payload["params"], dict):
            raise ValueError("indicator params must be a JSON object")
        payload["params"].update(params)
    if getattr(args, "dependencies_json", None) is not None:
        raw_dependencies = sys.stdin.read() if args.dependencies_json == "-" else (
            args.dependencies_json
            if str(args.dependencies_json).strip().startswith("[")
            else Path(args.dependencies_json).expanduser().read_text(encoding="utf-8")
        )
        try:
            dependencies = json.loads(raw_dependencies)
        except json.JSONDecodeError as exc:
            raise ValueError(f"invalid JSON array for --dependencies-json: {exc}") from exc
        if not isinstance(dependencies, list):
            raise ValueError("--dependencies-json must be a JSON array")
        payload["dependencies"] = dependencies
    for key in ("color", "color_palette"):
        value = getattr(args, key, None)
        if value is not None:
            payload[key] = value
    if "type" not in payload or not str(payload.get("type") or "").strip():
        raise ValueError("indicator type is required")
    payload.setdefault("params", {})
    if not isinstance(payload.get("params"), dict):
        raise ValueError("indicator params must be a JSON object")
    return payload


def _cmd_indicators_types(args: argparse.Namespace) -> int:
    _print_json({"schema_version": "qt_indicator_types.v1", "items": _client(args).request_json("GET", "/api/indicators/types")})
    return 0


def _cmd_indicators_type(args: argparse.Namespace) -> int:
    _print_json(_client(args).request_json("GET", f"/api/indicators/types/{args.type_id}"))
    return 0


def _cmd_indicators_list(args: argparse.Namespace) -> int:
    _print_json({"schema_version": "qt_indicators_list.v1", "items": _client(args).request_json("GET", "/api/indicators/")})
    return 0


def _cmd_indicators_get(args: argparse.Namespace) -> int:
    _print_json(_client(args).request_json("GET", f"/api/indicators/{args.indicator_id}"))
    return 0


def _cmd_indicators_strategies(args: argparse.Namespace) -> int:
    _print_json(
        {
            "schema_version": "qt_indicator_strategies.v1",
            "indicator_id": args.indicator_id,
            "items": _client(args).request_json("GET", f"/api/indicators/{args.indicator_id}/strategies"),
        }
    )
    return 0


def _cmd_indicators_validate_config(args: argparse.Namespace) -> int:
    _print_json(_client(args).request_json("POST", "/api/indicators/validate-config", payload=_indicator_payload_from_args(args)))
    return 0


def _cmd_indicators_create(args: argparse.Namespace) -> int:
    payload = _indicator_payload_from_args(args)
    normalized = _client(args).request_json("POST", "/api/indicators/validate-config", payload=payload)
    if not bool(args.apply):
        _print_json(
            {
                "schema_version": "qt_planned_mutation.v1",
                "operation": "create_indicator",
                "apply": False,
                "payload": payload,
                "normalized": normalized,
            }
        )
        return 0
    if not bool(args.confirm):
        raise ValueError("create indicator requires --confirm when --apply is set")
    _print_json(_client(args).request_json("POST", "/api/indicators/", payload=payload))
    return 0


def _indicator_payload_from_read(read_payload: dict[str, Any]) -> dict[str, Any]:
    instance = read_payload.get("instance") if isinstance(read_payload.get("instance"), dict) else {}
    return {
        "type": instance.get("type"),
        "name": instance.get("name"),
        "params": dict(instance.get("params") or {}),
        "dependencies": list(instance.get("dependencies") or []),
        "color": instance.get("color"),
        "color_palette": instance.get("color_palette"),
    }


def _indicator_material_payload(payload: dict[str, Any]) -> dict[str, Any]:
    return {
        "type": payload.get("type"),
        "params": dict(payload.get("params") or {}),
        "dependencies": list(payload.get("dependencies") or []),
    }


def _cmd_indicators_clone(args: argparse.Namespace) -> int:
    client = _client(args)
    source = client.request_json("GET", f"/api/indicators/{args.indicator_id}")
    base = _indicator_payload_from_read(source)
    base["name"] = args.name or f"{base.get('name') or base.get('type')} Copy"
    payload = _indicator_payload_from_args(args, base=base)
    if payload.get("type") != base.get("type"):
        raise ValueError("indicator clone cannot change type; create a new indicator instead")
    normalized = client.request_json("POST", "/api/indicators/validate-config", payload=payload)
    if not bool(args.apply):
        _print_json(
            {
                "schema_version": "qt_planned_mutation.v1",
                "operation": "clone_indicator",
                "apply": False,
                "source_indicator_id": args.indicator_id,
                "payload": payload,
                "normalized": normalized,
            }
        )
        return 0
    if not bool(args.confirm):
        raise ValueError("clone indicator requires --confirm when --apply is set")
    _print_json(client.request_json("POST", "/api/indicators/", payload=payload))
    return 0


def _cmd_indicators_edit(args: argparse.Namespace) -> int:
    client = _client(args)
    current = client.request_json("GET", f"/api/indicators/{args.indicator_id}")
    before = _indicator_payload_from_read(current)
    payload = _indicator_payload_from_args(args, base=before)
    if payload.get("type") != before.get("type"):
        raise ValueError("indicator type cannot be edited; create a new indicator instead")
    normalized = client.request_json("POST", "/api/indicators/validate-config", payload=payload)
    material_changed = _indicator_material_payload(before) != _indicator_material_payload(payload)
    if bool(args.apply) and material_changed:
        strategies = client.request_json("GET", f"/api/indicators/{args.indicator_id}/strategies")
        if strategies:
            raise ValueError("strategy-bound indicator params/dependencies cannot be edited; clone instead")
    if not bool(args.apply):
        _print_json(
            {
                "schema_version": "qt_planned_mutation.v1",
                "operation": "edit_indicator",
                "apply": False,
                "indicator_id": args.indicator_id,
                "material_changed": material_changed,
                "before": before,
                "payload": payload,
                "normalized": normalized,
            }
        )
        return 0
    if not bool(args.confirm):
        raise ValueError("edit indicator requires --confirm when --apply is set")
    _print_json(client.request_json("PUT", f"/api/indicators/{args.indicator_id}", payload=payload))
    return 0


def _cmd_indicators_rm(args: argparse.Namespace) -> int:
    if not bool(args.confirm):
        raise ValueError("rm indicator requires --confirm")
    _client(args).request_bytes("DELETE", f"/api/indicators/{args.indicator_id}")
    _print_json({"deleted": True, "indicator_id": args.indicator_id})
    return 0


def _cmd_indicators_toggle(args: argparse.Namespace) -> int:
    _print_json(
        _client(args).request_json(
            "PATCH",
            f"/api/indicators/{args.indicator_id}/enabled",
            payload={"enabled": bool(args.enabled)},
        )
    )
    return 0


def _indicator_window_payload(args: argparse.Namespace) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "start": args.start,
        "end": args.end,
        "interval": args.interval,
    }
    for key in ("symbol", "datasource", "exchange", "instrument_id"):
        value = getattr(args, key, None)
        if value is not None:
            payload[key] = value
    return payload


def _cmd_indicators_validate_runtime(args: argparse.Namespace) -> int:
    payload = _indicator_window_payload(args)
    payload["require_ready_by_end"] = bool(args.require_ready_by_end)
    if args.min_ready_bars is not None:
        payload["min_ready_bars"] = args.min_ready_bars
    _print_json(
        _client(args).request_json(
            "POST",
            f"/api/indicators/{args.indicator_id}/runtime-validation",
            payload=payload,
        )
    )
    return 0


def _cmd_indicators_overlays(args: argparse.Namespace) -> int:
    payload = _indicator_window_payload(args)
    for key in ("visibility_epoch", "cursor_epoch", "cursor_time"):
        value = getattr(args, key, None)
        if value is not None:
            payload[key] = value
    _print_json(_client(args).request_json("POST", f"/api/indicators/{args.indicator_id}/overlays", payload=payload))
    return 0


def _cmd_indicators_signals(args: argparse.Namespace) -> int:
    payload = _indicator_window_payload(args)
    if getattr(args, "config_json", None):
        payload["config"] = _read_json_object_arg(args.config_json, label="--config-json")
    _print_json(_client(args).request_json("POST", f"/api/indicators/{args.indicator_id}/signals", payload=payload))
    return 0


def _cmd_reports_list(args: argparse.Namespace) -> int:
    _print_json(
        _client(args).request_json(
            "GET",
            "/api/reports/",
            params={
                "type": args.type,
                "status": args.status,
                "limit": args.limit,
                "offset": args.offset,
                "search": args.search,
                "botId": args.bot_id,
                "instrument": args.instrument,
                "timeframe": args.timeframe,
                "start": args.start,
                "end": args.end,
            },
        )
    )
    return 0


def _cmd_report_get(args: argparse.Namespace) -> int:
    paths = {
        "dataset": f"/api/reports/{args.run_id}",
        "readiness": f"/api/reports/{args.run_id}/readiness",
        "summary": f"/api/reports/{args.run_id}/research-summary",
        "sections": f"/api/reports/{args.run_id}/sections",
        "diagnostics": f"/api/reports/{args.run_id}/diagnostics",
        "metrics": f"/api/reports/{args.run_id}/metrics",
        "operational-health": f"/api/reports/{args.run_id}/operational-health",
        "run-report": f"/api/reports/{args.run_id}/run-report",
        "run-report-status": f"/api/reports/{args.run_id}/run-report/status",
        "instruments": f"/api/reports/{args.run_id}/instruments",
        "symbol-summary": f"/api/reports/{args.run_id}/symbol-summary",
    }
    params: dict[str, Any] = {}
    if args.report_section == "run-report":
        if bool(args.build or args.force_rebuild):
            _ensure_run_report_materialized(
                _client(args),
                args.run_id,
                force_rebuild=bool(args.force_rebuild),
            )
    _print_json(_client(args).request_json("GET", paths[args.report_section], params=params))
    return 0


def _cmd_reports_manifest(args: argparse.Namespace) -> int:
    _print_json(
        _client(args).request_json(
            "GET",
            f"/api/reports/{args.run_id}/export/manifest",
            params={"include_candles": args.include_candles},
        )
    )
    return 0


def _cmd_reports_export(args: argparse.Namespace) -> int:
    payload = _write_report_export(
        args,
        _client(args),
        run_id=args.run_id,
        include_json=not args.no_json,
        include_csv=not args.no_csv,
        include_candles=args.include_candles,
    )
    _print_json(payload)
    return 0


def _cmd_reports_compare(args: argparse.Namespace) -> int:
    _print_json(
        _client(args).request_json(
            "GET",
            "/api/reports/compare/summary",
            params={
                "left_run_id": args.left_run_id,
                "right_run_id": args.right_run_id,
                "include_golden": not args.no_golden,
                "require_golden": args.require_golden,
            },
        )
    )
    return 0


def _cmd_reports_page(args: argparse.Namespace) -> int:
    section = str(args.report_page_section)
    path = f"/api/reports/{args.run_id}/{section}"
    params: dict[str, Any] = {
        "limit": args.limit,
        "offset": args.offset,
        "symbol": getattr(args, "symbol", None),
        "instrumentId": getattr(args, "instrument_id", None),
    }
    if section == "decisions":
        params["state"] = args.state
    _print_json(_client(args).request_json("GET", path, params=params))
    return 0


def _cmd_reports_candle_catalog(args: argparse.Namespace) -> int:
    _print_json(_client(args).request_json("GET", f"/api/reports/{args.run_id}/candles/catalog"))
    return 0


def _cmd_reports_candles(args: argparse.Namespace) -> int:
    _print_json(
        _client(args).request_json(
            "GET",
            f"/api/reports/{args.run_id}/candles",
            params={
                "instrument_id": args.instrument_id,
                "timeframe": args.timeframe,
                "start": args.start,
                "end": args.end,
                "limit": args.limit,
                "offset": args.offset,
            },
        )
    )
    return 0


def _cmd_data_coverage(args: argparse.Namespace) -> int:
    payload = {
        "instrument_id": args.instrument_id,
        "symbol": args.symbol,
        "datasource": args.datasource,
        "exchange": args.exchange,
        "start": args.start,
        "end": args.end,
        "timeframe": args.timeframe,
    }
    if not payload["instrument_id"] and not payload["symbol"]:
        raise ValueError("--instrument-id or --symbol is required")
    result = _client(args).request_json("POST", "/api/candles/coverage", payload=payload)
    _print_json(result)
    status = str(result.get("status") or "").lower()
    if status in {"ok", "info"}:
        return 0
    if status == "warning" and not bool(args.fail_on_warning):
        return 0
    return 1


def _cmd_data_ingest_candles(args: argparse.Namespace) -> int:
    """Explicitly acquire and persist one bounded candle window."""

    payload = {
        "instrument_id": args.instrument_id,
        "start": args.start,
        "end": args.end,
        "timeframe": args.timeframe,
        "source_revision": args.source_revision,
    }
    _print_json(
        _client(args).request_json("POST", "/api/candles/ingest", payload=payload)
    )
    return 0


def _cmd_data_acquire_numeric_facts(args: argparse.Namespace) -> int:
    """Explicitly authorize one bounded manifest-driven numeric acquisition."""

    if args.mode == "historical" and (not args.start or not args.end):
        raise ValueError("--start and --end are required for historical mode")
    if args.mode == "current" and (args.start or args.end or args.repair):
        raise ValueError("current mode forbids --start, --end, and --repair")
    payload = {
        "manifest_path": args.manifest_path,
        "binding_id": args.binding_id,
        "mode": args.mode,
        "start": args.start,
        "end": args.end,
        "allow_network": bool(args.allow_network),
        "requested_by": args.requested_by,
        "reason": args.reason,
        "max_requests": args.max_requests,
        "max_logs": args.max_logs,
        "max_blocks": args.max_blocks,
        "max_retries": args.max_retries,
        "repair": bool(args.repair),
    }
    result = _client(args).request_json(
        "POST",
        "/api/market-data/numeric-facts/acquire",
        payload=payload,
    )
    _print_json(result)
    return 0 if bool(dict(result.get("result") or {}).get("complete")) else 1


def _cmd_data_series(args: argparse.Namespace) -> int:
    _print_json(
        _client(args).request_json(
            "GET",
            "/api/candles/series",
            params={"instrument_id": args.instrument_id},
        )
    )
    return 0



def _cmd_data_prepare_backtest_dataset(args: argparse.Namespace) -> int:
    """Prepare and freeze source facts without starting execution."""

    numeric_acquisition = _read_json_object_arg(
        args.numeric_acquisition_json,
        label="--numeric-acquisition-json",
    )
    if numeric_acquisition and not args.acquire_missing:
        raise ValueError(
            "--numeric-acquisition-json requires --acquire-missing; "
            "dataset preparation never contacts providers implicitly"
        )
    payload = {
        "evaluation_start": args.start,
        "evaluation_end": args.end,
        "acquire_missing": bool(args.acquire_missing),
        "created_by": args.created_by,
    }
    if numeric_acquisition:
        payload["numeric_acquisition"] = numeric_acquisition
    _print_json(
        _client(args).request_json(
            "POST",
            f"/api/bots/{quote(args.bot_id, safe='')}/backtest-dataset/prepare",
            payload=payload,
        )
    )
    return 0

def _cmd_data_freeze_dataset(args: argparse.Namespace) -> int:
    payload = _read_json_object_arg(args.request_json, label="--request-json")
    if not payload:
        missing = [
            name
            for name in ("instrument_id", "start", "end", "timeframe")
            if not getattr(args, name, None)
        ]
        if missing:
            raise ValueError(
                "--request-json or --instrument-id/--start/--end/--timeframe is required"
            )
        metadata = _read_json_object_arg(args.metadata_json, label="--metadata-json")
        payload = {
            "series": [
                {
                    "instrument_id": args.instrument_id,
                    "start": args.start,
                    "end": args.end,
                    "timeframe": args.timeframe,
                }
            ],
            "name": args.name,
            "purpose": args.purpose,
            "created_by": args.created_by,
            "metadata": metadata,
        }
    _print_json(
        _client(args).request_json(
            "POST", "/api/candles/datasets/freeze", payload=payload
        )
    )
    return 0


def _cmd_data_dataset(args: argparse.Namespace) -> int:
    _print_json(
        _client(args).request_json(
            "GET", f"/api/candles/datasets/{quote(args.dataset_id, safe='')}"
        )
    )
    return 0


def _cmd_data_collector_definitions_install_structured(
    args: argparse.Namespace,
) -> int:
    _print_json(
        _client(args).request_json(
            "POST",
            "/api/market-data/definitions/install-structured",
            payload={
                "manifest_path": args.manifest_path,
                "binding_id": args.binding_id,
                "enabled": bool(args.enabled),
                "max_attempts": args.max_attempts,
                "minimum_spacing_seconds": args.minimum_spacing_seconds,
            },
        )
    )
    return 0


def _cmd_data_collector_definitions_enroll_product(
    args: argparse.Namespace,
) -> int:
    provider = str(args.provider or "").strip().upper()
    venue = str(args.venue or "").strip().upper()
    product_id = str(args.product_id or "").strip().upper()
    if not bool(args.confirm):
        raise ValueError("product collector enrollment requires --confirm")
    actor_id = str(
        args.actor_id
        or os.environ.get("QT_ACTOR_ID")
        or f"qt:{getpass.getuser()}"
    )
    collector_types = args.collector_type or [
        "open_interest",
        "funding_rate",
        "market_trades",
        "level2",
    ]
    _print_json(
        _client(args).request_json(
            "POST",
            "/api/market-data/definitions/enroll-product",
            payload={
                "provider": provider,
                "venue": venue,
                "product_id": product_id,
                "collector_types": collector_types,
                "poll_interval_seconds": args.poll_interval_seconds,
                "request_id": args.request_id or f"qt-{uuid.uuid4().hex}",
                "actor_id": actor_id,
                "reason": args.reason,
                "confirmation": f"{provider}:{venue}:{product_id}:enroll",
            },
        )
    )
    return 0


def _cmd_data_collectors_fleet(args: argparse.Namespace) -> int:
    _print_json(
        _client(args).request_json(
            "GET",
            "/api/market-data/operations/collectors/snapshot",
            params={"attempt_limit": args.attempt_limit},
        )
    )
    return 0


def _collector_operation_path(args: argparse.Namespace, suffix: str = "") -> str:
    base = (
        "/api/market-data/operations/collectors/"
        f"{quote(args.collector_kind, safe='')}/"
        f"{quote(args.collector_id, safe='')}"
    )
    return base + suffix


def _cmd_data_collectors_detail(args: argparse.Namespace) -> int:
    _print_json(
        _client(args).request_json(
            "GET",
            _collector_operation_path(args),
            params={"limit": args.limit},
        )
    )
    return 0


def _cmd_data_collectors_inspect(args: argparse.Namespace) -> int:
    _print_json(
        _client(args).request_json(
            "GET",
            _collector_operation_path(args, f"/{args.inspect_surface}"),
            params={"limit": args.limit},
        )
    )
    return 0


def _cmd_data_collectors_diagnose(args: argparse.Namespace) -> int:
    _print_json(
        _client(args).request_json(
            "GET",
            _collector_operation_path(args, "/diagnostics"),
        )
    )
    return 0


def _cmd_data_collectors_action(args: argparse.Namespace) -> int:
    confirmation = (
        f"{args.collector_kind}:{args.collector_id}:{args.collector_action}"
        if bool(args.confirm)
        else None
    )
    actor_id = str(
        args.actor_id
        or os.environ.get("QT_ACTOR_ID")
        or f"qt:{getpass.getuser()}"
    )
    payload = {
        "request_id": args.request_id or f"qt-{uuid.uuid4().hex}",
        "actor_id": actor_id,
        "requested_at": datetime.now(UTC).isoformat(),
        "confirmation": confirmation,
        "context": {"surface": "qt", "reason": args.reason},
    }
    _print_json(
        _client(args).request_json(
            "POST",
            _collector_operation_path(
                args, f"/actions/{args.collector_action}"
            ),
            payload=payload,
        )
    )
    return 0


def _cmd_data_collectors_probe(args: argparse.Namespace) -> int:
    args.collector_action = "health_probe"
    args.confirm = False
    args.request_id = None
    args.actor_id = None
    args.reason = "Manual collector health probe"
    return _cmd_data_collectors_action(args)


def _cmd_data_collectors_plane(args: argparse.Namespace) -> int:
    _print_json(
        _client(args).request_json(
            "GET", "/api/market-data/operations/data-plane"
        )
    )
    return 0


def _cmd_data_open_interest_latest(args: argparse.Namespace) -> int:
    _print_json(
        _client(args).request_json(
            "GET",
            "/api/market-data/open-interest/latest",
            params={
                "instrument_id": args.instrument_id,
                "decision_time": args.decision_time,
                "max_staleness_seconds": args.max_staleness_seconds,
                "required": not bool(args.optional),
            },
        )
    )
    return 0


def _cmd_data_funding_rate_latest(args: argparse.Namespace) -> int:
    _print_json(
        _client(args).request_json(
            "GET",
            "/api/market-data/funding-rate/latest",
            params={
                "instrument_id": args.instrument_id,
                "decision_time": args.decision_time,
                "max_staleness_seconds": args.max_staleness_seconds,
                "required": not bool(args.optional),
            },
        )
    )
    return 0


def _cmd_data_market_structure_proof(args: argparse.Namespace) -> int:
    import asyncio

    from .market_structure_proof import (
        default_output_dir,
        run_coinbase_market_structure_proof,
    )

    output_dir = Path(args.output_dir).expanduser() if args.output_dir else default_output_dir()
    result = asyncio.run(
        run_coinbase_market_structure_proof(
            output_dir=output_dir,
            product_ids=args.product_id or ("BIP-20DEC30-CDE", "BTC-USD"),
            channels=args.channel or ("market_trades", "level2", "ticker"),
            auth_mode=args.auth_mode,
            duration_seconds=args.duration,
            reconnect_interval_seconds=args.reconnect_interval,
            sample_limit=args.sample_limit,
            rest_limit=args.rest_limit,
            max_annual_archive_gib=args.max_annual_archive_gib,
        )
    )
    _print_json(result)
    return 0 if result.get("status") == "completed" else 1


def _cmd_data_market_structure_enroll(args: argparse.Namespace) -> int:
    _print_json(
        _client(args).request_json(
            "POST",
            "/api/market-data/market-structure/enrollments/apply",
            payload={"manifest_path": args.manifest_path},
        )
    )
    return 0



def _cmd_data_market_structure_normalization_specs_install(
    args: argparse.Namespace,
) -> int:
    _print_json(
        _client(args).request_json(
            "POST",
            "/api/market-data/market-structure/normalization/specs/install",
            payload={"approved_by": args.approved_by},
        )
    )
    return 0


def _cmd_data_market_structure_normalization_specs(
    args: argparse.Namespace,
) -> int:
    _print_json(
        _client(args).request_json(
            "GET", "/api/market-data/market-structure/normalization/specs"
        )
    )
    return 0


def _normalization_cli_payload(args: argparse.Namespace) -> dict[str, Any]:
    return {
        "spec_id": args.spec_id,
        "source_series_id": args.source_series_id,
        "start": args.start,
        "end": args.end,
        "known_at": args.known_at,
        "as_of_commit_seq": args.as_of_commit_seq,
    }


def _cmd_data_market_structure_normalize(args: argparse.Namespace) -> int:
    _print_json(
        _client(args).request_json(
            "POST",
            "/api/market-data/market-structure/normalization/materialize",
            payload=_normalization_cli_payload(args),
        )
    )
    return 0


def _cmd_data_market_structure_normalization_compare(
    args: argparse.Namespace,
) -> int:
    _print_json(
        _client(args).request_json(
            "POST",
            "/api/market-data/market-structure/normalization/compare",
            payload=_normalization_cli_payload(args),
        )
    )
    return 0
def _cmd_data_market_structure_definitions(args: argparse.Namespace) -> int:
    _print_json(
        _client(args).request_json(
            "GET",
            "/api/market-data/market-structure/definitions",
            params={"definition_id": args.definition_id},
        )
    )
    return 0


def _cmd_data_market_structure_sessions(args: argparse.Namespace) -> int:
    _print_json(
        _client(args).request_json(
            "GET",
            "/api/market-data/market-structure/sessions",
            params={
                "definition_id": args.definition_id,
                "limit": args.limit,
            },
        )
    )
    return 0


def _cmd_data_market_structure_status(args: argparse.Namespace) -> int:
    _print_json(
        _client(args).request_json(
            "GET",
            f"/api/market-data/market-structure/definitions/{quote(args.definition_id, safe='')}/status",
        )
    )
    return 0


def _cmd_data_market_structure_capture(args: argparse.Namespace) -> int:
    _print_json(
        _client(args).request_json(
            "POST",
            f"/api/market-data/market-structure/definitions/{quote(args.definition_id, safe='')}/capture",
            payload={
                "duration_seconds": args.duration,
                "storage_root": args.storage_root,
                "owner_id": args.owner_id,
            },
        )
    )
    return 0


def _cmd_data_market_structure_safety_change(args: argparse.Namespace) -> int:
    _print_json(
        _client(args).request_json(
            "POST",
            f"/api/market-data/market-structure/safety/{args.safety_action}",
            payload={
                "request_id": args.request_id,
                "scope_type": args.scope_type,
                "scope_id": args.scope_id,
                "requested_by": args.requested_by,
                "reason": args.reason,
                "policy_hash": args.policy_hash,
                "evidence": _read_json_object_arg(args.evidence_json, label="--evidence-json") or None,
            },
        )
    )
    return 0


def _cmd_data_market_structure_safety_status(args: argparse.Namespace) -> int:
    _print_json(
        _client(args).request_json(
            "GET",
            f"/api/market-data/market-structure/safety?limit={int(args.limit)}",
        )
    )
    return 0


def _cmd_data_market_structure_continuous_evidence(
    args: argparse.Namespace,
) -> int:
    _print_json(
        _client(args).request_json(
            "GET",
            f"/api/market-data/market-structure/definitions/{quote(args.definition_id, safe='')}/continuous/validation/{quote(args.session_id, safe='')}",
        )
    )
    return 0


def _cmd_data_market_structure_replay(args: argparse.Namespace) -> int:
    _print_json(
        _client(args).request_json(
            "POST",
            f"/api/market-data/market-structure/manifests/{quote(args.manifest_id, safe='')}/replay",
            payload={"storage_root": args.storage_root},
        )
    )
    return 0


def _cmd_data_market_structure_replay_book(args: argparse.Namespace) -> int:
    _print_json(
        _client(args).request_json(
            "POST",
            f"/api/market-data/market-structure/definitions/{quote(args.definition_id, safe='')}/sessions/{quote(args.session_id, safe='')}/replay-book",
            payload={"storage_root": args.storage_root},
        )
    )
    return 0


def _cmd_data_market_structure_compact(args: argparse.Namespace) -> int:
    _print_json(
        _client(args).request_json(
            "POST",
            f"/api/market-data/market-structure/definitions/{quote(args.definition_id, safe='')}/sessions/{quote(args.session_id, safe='')}/compact",
            payload={
                "source_manifest_ids": args.manifest_id,
                "storage_root": args.storage_root,
                "owner_id": args.owner_id,
            },
        )
    )
    return 0


def _cmd_data_market_structure_retention_pin(args: argparse.Namespace) -> int:
    _print_json(
        _client(args).request_json(
            "POST",
            f"/api/market-data/market-structure/archive-retention/{quote(args.target_kind, safe='')}/{quote(args.target_id, safe='')}/pin",
            payload={
                "owner_kind": args.owner_kind,
                "owner_id": args.owner_id,
                "active": not args.release,
                "reason": args.reason,
            },
        )
    )
    return 0


def _cmd_data_market_structure_retention_status(args: argparse.Namespace) -> int:
    _print_json(
        _client(args).request_json(
            "GET",
            f"/api/market-data/market-structure/archive-retention/{quote(args.target_kind, safe='')}/{quote(args.target_id, safe='')}",
        )
    )
    return 0


def _cmd_data_market_structure_lifecycle_plan(args: argparse.Namespace) -> int:
    _print_json(
        _client(args).request_json(
            "GET",
            "/api/market-data/market-structure/storage-lifecycle/plan",
        )
    )
    return 0


def _cmd_data_market_structure_lifecycle_run(args: argparse.Namespace) -> int:
    _print_json(
        _client(args).request_json(
            "POST",
            "/api/market-data/market-structure/storage-lifecycle/run",
            payload={
                "execute": bool(args.execute),
                "storage_root": args.storage_root,
                "owner_id": args.owner_id,
            },
        )
    )
    return 0


def _cmd_data_market_structure_lifecycle_events(args: argparse.Namespace) -> int:
    _print_json(
        _client(args).request_json(
            "GET",
            "/api/market-data/market-structure/storage-lifecycle/events",
            params={"limit": args.limit},
        )
    )
    return 0


def _cmd_data_market_structure_reconcile_recent(args: argparse.Namespace) -> int:
    _print_json(
        _client(args).request_json(
            "POST",
            f"/api/market-data/market-structure/definitions/{quote(args.definition_id, safe='')}/reconcile-recent",
            params={"limit": args.limit},
        )
    )
    return 0


def _cmd_research_items_list(args: argparse.Namespace) -> int:
    payload = _client(args).request_json(
        "GET",
        "/api/research/items",
        params={
            "kind": args.kind,
            "status": args.status,
            "symbol": args.symbol,
            "timeframe": args.timeframe,
            "limit": args.limit,
        },
    )
    _print_json(payload)
    return 0


def _cmd_research_items_get(args: argparse.Namespace) -> int:
    _print_json(_client(args).request_json("GET", f"/api/research/items/{args.item_id}"))
    return 0


def _research_item_payload(
    args: argparse.Namespace,
    *,
    default_kind: str | None = None,
    default_status: str | None = None,
) -> dict[str, Any]:
    payload = _read_json_object_arg(getattr(args, "payload_json", None), label="--payload-json")
    if default_kind:
        payload.setdefault("kind", default_kind)
    if default_status:
        payload.setdefault("status", default_status)
    for key in (
        "kind",
        "status",
        "title",
        "body",
        "instrument_id",
        "symbol",
        "timeframe",
        "datasource",
        "exchange",
        "window_start",
        "window_end",
    ):
        value = getattr(args, key, None)
        if value is not None:
            payload[key] = value
    tags = list(getattr(args, "tag", None) or [])
    if tags:
        payload["tags"] = tags
    if getattr(args, "payload", None):
        payload["payload"] = _read_json_object_arg(args.payload, label="--payload")
    if not str(payload.get("kind") or "").strip():
        raise ValueError("kind is required")
    if not str(payload.get("title") or "").strip():
        raise ValueError("title is required")
    payload.setdefault("status", "draft")
    return payload


def _cmd_research_items_create(args: argparse.Namespace) -> int:
    _print_json(_client(args).request_json("POST", "/api/research/items", payload=_research_item_payload(args)))
    return 0


def _cmd_research_observe_create(args: argparse.Namespace) -> int:
    _print_json(
        _client(args).request_json(
            "POST",
            "/api/research/items",
            payload=_research_item_payload(args, default_kind="observation", default_status="active"),
        )
    )
    return 0


def _cmd_research_links_create(args: argparse.Namespace) -> int:
    payload = _read_json_object_arg(getattr(args, "payload_json", None), label="--payload-json")
    for key in ("source_item_id", "target_type", "target_id", "relation"):
        value = getattr(args, key, None)
        if value is not None:
            payload[key] = value
    if getattr(args, "metadata_json", None):
        payload["metadata"] = _read_json_object_arg(args.metadata_json, label="--metadata-json")
    _print_json(_client(args).request_json("POST", "/api/research/links", payload=payload))
    return 0


def _cmd_research_links_list(args: argparse.Namespace) -> int:
    _print_json(
        _client(args).request_json(
            "GET",
            f"/api/research/items/{args.item_id}/links",
            params={"include_inbound": args.include_inbound},
        )
    )
    return 0


def _research_scope_from_args(args: argparse.Namespace, *, include_indicator: bool = False, include_run: bool = False) -> dict[str, Any]:
    scope: dict[str, Any] = {}
    for key in ("instrument_id", "symbol", "datasource", "exchange", "timeframe", "start", "end"):
        value = getattr(args, key, None)
        if value is not None:
            scope[key] = value
    if include_indicator:
        scope["indicator_id"] = args.indicator_id
    if include_run:
        scope["run_id"] = args.run_id
    return scope


def _research_outcomes_from_args(args: argparse.Namespace) -> dict[str, Any]:
    outcomes: dict[str, Any] = {}
    if getattr(args, "forward_bars", None):
        outcomes["forward_bars"] = [int(item.strip()) for item in str(args.forward_bars).split(",") if item.strip()]
    if getattr(args, "entry_lag_bars", None) is not None:
        outcomes["entry_lag_bars"] = args.entry_lag_bars
    if getattr(args, "direction", None):
        outcomes["direction"] = args.direction
    if getattr(args, "min_sample_count", None) is not None:
        outcomes["min_sample_count"] = args.min_sample_count
    if getattr(args, "min_edge_pct", None) is not None:
        outcomes["min_edge_pct"] = args.min_edge_pct
    if getattr(args, "bucket_by", None):
        outcomes["bucket_by"] = [item.strip() for item in str(args.bucket_by).split(",") if item.strip()]
    if getattr(args, "max_examples", None) is not None:
        outcomes["max_examples"] = args.max_examples
    return outcomes


def _research_check_request_base(args: argparse.Namespace, *, title: str, check_family: str) -> dict[str, Any]:
    payload = _read_json_object_arg(getattr(args, "request_json", None), label="--request-json")
    payload.setdefault("title", title)
    payload["check_family"] = check_family
    if getattr(args, "title", None):
        payload["title"] = args.title
    if getattr(args, "body", None):
        payload["body"] = args.body
    if getattr(args, "observation_id", None):
        payload["observation_id"] = args.observation_id
    tags = list(getattr(args, "tag", None) or [])
    if tags:
        payload["tags"] = tags
    return payload


def _post_research_check(args: argparse.Namespace, payload: dict[str, Any]) -> int:
    operations = ResearchOperations(_client(args))
    mode = str(payload.get("mode") or "preview").strip().lower()
    if getattr(args, "dispatch", False):
        if mode != "evidence":
            raise ValueError(
                "legacy typed Check dispatch is deprecated for preview; use "
                "'qt research check run --request-json ... --dispatch' with an immutable input"
            )
        result = operations.dispatch_evidence(
            payload, dataset_id=payload.get("dataset_id")
        )
        _print_research_job_dispatch(result)
        return 0
    if mode == "evidence":
        result = operations.run_evidence(
            payload, dataset_id=payload.get("dataset_id")
        )
    else:
        result = operations.preview(payload)
    _print_json(result)
    return 0


def _canonical_research_request(args: argparse.Namespace) -> dict[str, Any]:
    payload = _read_json_object_arg(args.request_json, label="--request-json")
    if not payload:
        raise ValueError("--request-json is required")
    return payload


def _cmd_research_check_requirements(args: argparse.Namespace) -> int:
    _print_json(
        ResearchOperations(_client(args)).requirements(
            _canonical_research_request(args)
        )
    )
    return 0


def _cmd_research_check_preview(args: argparse.Namespace) -> int:
    _print_json(
        ResearchOperations(_client(args)).preview(
            _canonical_research_request(args)
        )
    )
    return 0


def _cmd_research_check_prepare(args: argparse.Namespace) -> int:
    _print_json(
        ResearchOperations(_client(args)).prepare(
            _canonical_research_request(args),
            freeze=bool(args.freeze),
            created_by=args.created_by,
            dataset_name=args.dataset_name,
        )
    )
    return 0


def _cmd_research_check_run(args: argparse.Namespace) -> int:
    operations = ResearchOperations(_client(args))
    request = _canonical_research_request(args)
    if args.dispatch:
        result = operations.dispatch_evidence(
            request, dataset_id=args.dataset_id
        )
        _print_research_job_dispatch(result)
    else:
        _print_json(
            operations.run_evidence(request, dataset_id=args.dataset_id)
        )
    return 0


def _cmd_research_check_replay(args: argparse.Namespace) -> int:
    _print_json(ResearchOperations(_client(args)).replay(args.check_id))
    return 0


def _cmd_research_observe_from_check(args: argparse.Namespace) -> int:
    payload = _read_json_object_arg(
        args.request_json, label="--request-json"
    )
    for field in ("title", "body", "status"):
        value = getattr(args, field, None)
        if value is not None:
            payload[field] = value
    tags = list(getattr(args, "tag", None) or [])
    if tags:
        payload["tags"] = tags
    _print_json(
        ResearchOperations(_client(args)).create_observation(
            args.check_id, payload
        )
    )
    return 0


def _cmd_research_check_raw(args: argparse.Namespace) -> int:
    detector = _read_json_object_arg(getattr(args, "detector_json", None), label="--detector-json")
    if not detector:
        if not getattr(args, "field", None):
            raise ValueError("raw check requires --field or --detector-json")
        detector = {
            "type": "raw_condition",
            "field": args.field,
            "operator": args.operator or "lt",
        }
        if getattr(args, "value_field", None):
            detector["value_field"] = args.value_field
        elif getattr(args, "value", None) is not None:
            detector["value"] = _json_value(str(args.value))
        else:
            raise ValueError("--value or --value-field is required with --field")
    payload = _research_check_request_base(
        args,
        title=f"Raw {detector.get('field')} {detector.get('operator', 'eq')} check",
        check_family="raw_forward_outcome",
    )
    payload["scope"] = {**dict(payload.get("scope") or {}), **_research_scope_from_args(args)}
    payload["detector"] = detector
    outcomes = {**dict(payload.get("outcomes") or {}), **_research_outcomes_from_args(args)}
    if outcomes:
        payload["outcomes"] = outcomes
    return _post_research_check(args, payload)


def _cmd_research_check_signal(args: argparse.Namespace) -> int:
    detector = _read_json_object_arg(getattr(args, "detector_json", None), label="--detector-json")
    if not detector:
        detector = {"type": "run_signal_match"}
        for attr, key in (
            ("output_name", "output_name"),
            ("event_key", "event_key"),
            ("symbol", "symbol"),
            ("direction", "direction"),
        ):
            value = getattr(args, attr, None)
            if value is not None:
                detector[key] = value
        if len(detector) == 1:
            raise ValueError("signal check requires --output-name, --event-key, --symbol, --direction, or --detector-json")
    payload = _research_check_request_base(
        args,
        title=f"Run signal check: {detector.get('output_name') or detector.get('event_key') or args.run_id}",
        check_family="run_signal_summary",
    )
    payload["scope"] = {**dict(payload.get("scope") or {}), **_research_scope_from_args(args, include_run=True)}
    payload["detector"] = detector
    outcomes = {**dict(payload.get("outcomes") or {}), **_research_outcomes_from_args(args)}
    if outcomes:
        payload["outcomes"] = outcomes
    return _post_research_check(args, payload)


def _cmd_research_check_decision(args: argparse.Namespace) -> int:
    detector = _read_json_object_arg(getattr(args, "detector_json", None), label="--detector-json")
    if not detector:
        detector = {"type": "run_decision_match"}
        if getattr(args, "state", None):
            detector["decision_state"] = args.state
        if getattr(args, "reason_code", None):
            detector["reason_code"] = args.reason_code
        if getattr(args, "symbol", None):
            detector["symbol"] = args.symbol
        if len(detector) == 1:
            raise ValueError("decision check requires --state, --reason-code, --symbol, or --detector-json")
    payload = _research_check_request_base(
        args,
        title=f"Run decision check: {detector.get('decision_state') or detector.get('reason_code') or args.run_id}",
        check_family="run_decision_trade_comparison",
    )
    payload["scope"] = {**dict(payload.get("scope") or {}), **_research_scope_from_args(args, include_run=True)}
    payload["detector"] = detector
    outcomes = {**dict(payload.get("outcomes") or {}), **_research_outcomes_from_args(args)}
    if outcomes:
        payload["outcomes"] = outcomes
    return _post_research_check(args, payload)


def _cmd_research_check_indicator(args: argparse.Namespace) -> int:
    detector = _read_json_object_arg(getattr(args, "detector_json", None), label="--detector-json")
    if not detector:
        if not getattr(args, "output", None):
            raise ValueError("indicator check requires --output or --detector-json")
        if getattr(args, "field", None):
            detector = {
                "type": "indicator_output_match",
                "output_name": args.output,
                "field": args.field,
                "operator": args.operator or "eq",
            }
            if getattr(args, "value_field", None):
                detector["value_field"] = args.value_field
            elif getattr(args, "value", None) is not None:
                detector["value"] = _json_value(str(args.value))
            else:
                raise ValueError("--value or --value-field is required with --field")
        else:
            detector = {
                "type": "indicator_event_match",
                "output_name": args.output,
            }
            if getattr(args, "event_key", None):
                detector["event_key"] = args.event_key
    payload = _research_check_request_base(
        args,
        title=f"Indicator check: {detector.get('output_name') or args.indicator_id}",
        check_family="indicator_forward_outcome",
    )
    payload["scope"] = {**dict(payload.get("scope") or {}), **_research_scope_from_args(args, include_indicator=True)}
    payload["detector"] = detector
    outcomes = {**dict(payload.get("outcomes") or {}), **_research_outcomes_from_args(args)}
    if outcomes:
        payload["outcomes"] = outcomes
    return _post_research_check(args, payload)


def _cmd_research_check_audit(args: argparse.Namespace) -> int:
    detector = _read_json_object_arg(getattr(args, "detector_json", None), label="--detector-json")
    if not detector:
        for field in ("source_output", "source_field", "signal_output", "event_key"):
            if not getattr(args, field, None):
                raise ValueError(f"audit check requires --{field.replace('_', '-')} or --detector-json")
        expectation_type = args.expectation_type or "transition"
        detector = {
            "type": "signal_audit",
            "expectation_type": expectation_type,
            "source_output": args.source_output,
            "source_field": args.source_field,
            "signal_output": args.signal_output,
            "event_key": args.event_key,
        }
        if args.name:
            detector["name"] = args.name
        if args.same_group_by:
            detector["same_group_by"] = [item.strip() for item in str(args.same_group_by).split(",") if item.strip()]
        if expectation_type == "transition":
            if getattr(args, "from_value", None) is None or getattr(args, "to_value", None) is None:
                raise ValueError("transition audit check requires --from and --to")
            detector["from"] = _json_value(str(args.from_value))
            detector["to"] = _json_value(str(args.to_value))
        else:
            detector["operator"] = args.operator or "eq"
            if getattr(args, "value_field", None):
                detector["value_field"] = args.value_field
            elif getattr(args, "value", None) is not None:
                detector["value"] = _json_value(str(args.value))
            elif str(detector["operator"]).lower() not in {"is_true", "true"}:
                raise ValueError("condition audit check requires --value or --value-field")
    payload = _research_check_request_base(
        args,
        title=f"Signal audit: {detector.get('event_key') or args.indicator_id}",
        check_family="signal_audit",
    )
    payload["scope"] = {**dict(payload.get("scope") or {}), **_research_scope_from_args(args, include_indicator=True)}
    payload["detector"] = detector
    outcomes = {**dict(payload.get("outcomes") or {}), **_research_outcomes_from_args(args)}
    if outcomes:
        payload["outcomes"] = outcomes
    return _post_research_check(args, payload)


def _cmd_research_check_lifecycle(args: argparse.Namespace) -> int:
    detector = _read_json_object_arg(getattr(args, "detector_json", None), label="--detector-json")
    if not detector:
        detector = {"type": "candidate_lifecycle"}
        for attr, key in (
            ("output_name", "output_name"),
            ("family", "family"),
            ("side", "side"),
            ("stage", "stage"),
            ("status", "status"),
            ("signal_output", "signal_output"),
            ("signal_event_key", "signal_event_key"),
        ):
            value = getattr(args, attr, None)
            if value is not None:
                detector[key] = value
        for attr, key in (
            ("funnel_stages", "funnel_stages"),
            ("terminal_stages", "terminal_stages"),
            ("signal_stages", "signal_stages"),
        ):
            value = getattr(args, attr, None)
            if value:
                detector[key] = [item.strip() for item in str(value).split(",") if item.strip()]
    payload = _research_check_request_base(
        args,
        title=f"Lifecycle check: {detector.get('family') or detector.get('output_name') or args.indicator_id}",
        check_family="candidate_lifecycle",
    )
    payload["scope"] = {**dict(payload.get("scope") or {}), **_research_scope_from_args(args, include_indicator=True)}
    payload["detector"] = detector
    outcomes = {**dict(payload.get("outcomes") or {}), **_research_outcomes_from_args(args)}
    if outcomes:
        payload["outcomes"] = outcomes
    return _post_research_check(args, payload)


def _research_sweep_variants_from_args(args: argparse.Namespace) -> list[dict[str, Any]]:
    variants: list[dict[str, Any]] = []
    for raw in getattr(args, "variant_json", None) or []:
        variants.append(_read_json_object_arg(raw, label="--variant-json"))
    for raw in getattr(args, "variant", None) or []:
        spec = str(raw or "").strip()
        if not spec:
            continue
        variant_id, _, raw_params = spec.partition(":")
        variant_id = variant_id.strip()
        if not variant_id:
            raise ValueError("--variant requires a non-empty variant id")
        param_overrides: dict[str, Any] = {}
        for item in [part.strip() for part in raw_params.split(",") if part.strip()]:
            if "=" not in item:
                raise ValueError(f"expected key=value in --variant params, got {item!r}")
            key, value = item.split("=", 1)
            key = key.strip()
            if not key:
                raise ValueError(f"expected non-empty key in --variant params, got {item!r}")
            param_overrides[key] = _json_value(value)
        variants.append({"id": variant_id, "param_overrides": param_overrides})
    return variants


def _cmd_research_check_sweep(args: argparse.Namespace) -> int:
    payload = _read_json_object_arg(getattr(args, "request_json", None), label="--request-json")
    if getattr(args, "title", None):
        payload["title"] = args.title
    if getattr(args, "check_family", None):
        payload["check_family"] = args.check_family
    if getattr(args, "detector_json", None):
        payload["detector"] = _read_json_object_arg(args.detector_json, label="--detector-json")
    if getattr(args, "outcomes_json", None):
        payload["outcomes"] = {
            **dict(payload.get("outcomes") or {}),
            **_read_json_object_arg(args.outcomes_json, label="--outcomes-json"),
        }
    outcomes = {**dict(payload.get("outcomes") or {}), **_research_outcomes_from_args(args)}
    if outcomes:
        payload["outcomes"] = outcomes
    scope_args = _research_scope_from_args(args, include_indicator=bool(getattr(args, "indicator_id", None)))
    if scope_args:
        if payload.get("scopes") not in (None, ""):
            raise ValueError("--scope flags cannot be combined with request.scopes")
        payload["scope"] = {**dict(payload.get("scope") or {}), **scope_args}
    variants = _research_sweep_variants_from_args(args)
    if variants:
        payload["variants"] = variants
    display_metrics = [str(item).strip() for item in getattr(args, "display_metric", None) or [] if str(item).strip()]
    ranking = dict(payload.get("ranking") or {})
    if getattr(args, "rank_by", None):
        ranking["rank_by"] = args.rank_by
    if getattr(args, "rank_direction", None):
        ranking["direction"] = args.rank_direction
    if display_metrics:
        ranking["display_metrics"] = display_metrics
    if ranking:
        payload["ranking"] = ranking

    missing = [key for key in ("check_family", "detector", "variants", "ranking") if key not in payload]
    if missing:
        raise ValueError(f"research check sweep missing required fields: {', '.join(missing)}")
    if "scope" not in payload and "scopes" not in payload:
        raise ValueError("research check sweep requires scope flags, request.scope, or request.scopes")

    if getattr(args, "dispatch", False):
        result = _client(args).request_json("POST", "/api/research/jobs/checks/sweep", payload=payload)
        _print_research_job_dispatch(result)
        return 0

    result = _client(args).request_json("POST", "/api/research/checks/sweep", payload=payload)
    if str(getattr(args, "format", "json") or "json") == "table":
        _print_research_leaderboard_table(result)
    else:
        _print_json(result)
    return 0


def _print_research_job_dispatch(payload: dict[str, Any]) -> None:
    job_id = str(payload.get("job_id") or "")
    status = str(payload.get("status") or "")
    job_type = str(payload.get("job_type") or "")
    reused = bool(payload.get("reused"))
    print(f"Research job {status}.", flush=True)
    print(f"Job id: {job_id}", flush=True)
    print(f"Type: {job_type}", flush=True)
    if reused:
        print("Reused existing in-flight job.", flush=True)
    print("", flush=True)
    print("Next:", flush=True)
    print(f"  ./scripts/qt research jobs status {job_id}", flush=True)
    print(f"  ./scripts/qt research jobs result {job_id} --format table", flush=True)


def _print_research_job_status(payload: dict[str, Any], *, show_next: bool = True) -> None:
    job_id = str(payload.get("job_id") or "")
    status = str(payload.get("status") or "")
    print(f"Research job: {job_id}", flush=True)
    print(f"Status: {status}", flush=True)
    print(f"Type: {payload.get('job_type') or ''}", flush=True)
    print(f"Attempts: {payload.get('attempts')}/{payload.get('max_attempts')}", flush=True)
    for label, key in (("Created", "created_at"), ("Started", "started_at"), ("Finished", "finished_at")):
        if payload.get(key):
            print(f"{label}: {payload[key]}", flush=True)
    if payload.get("error"):
        print(f"Error: {payload['error']}", flush=True)
    summary = payload.get("result_summary") if isinstance(payload.get("result_summary"), dict) else {}
    if summary:
        result_type = str(summary.get("result_type") or "")
        print(f"Result: {result_type}", flush=True)
        if summary.get("evaluation_count") is not None:
            print(f"Evaluations: {summary.get('evaluation_count')}", flush=True)
        if summary.get("sample_count") is not None:
            print(f"Samples: {summary.get('sample_count')}", flush=True)
        if summary.get("recommendation"):
            print(f"Recommendation: {summary.get('recommendation')}", flush=True)
    if not show_next:
        return
    if status != "succeeded":
        print("", flush=True)
        print("Next:", flush=True)
        print(f"  ./scripts/qt research jobs status {job_id}", flush=True)
    else:
        print("", flush=True)
        print("Next:", flush=True)
        print(f"  ./scripts/qt research jobs result {job_id} --format table", flush=True)


def _cmd_research_job_status(args: argparse.Namespace) -> int:
    payload = ResearchOperations(_client(args)).job_status(args.job_id)
    if getattr(args, "json", False):
        _print_json(payload)
    else:
        _print_research_job_status(payload)
    return 0


def _cmd_research_job_result(args: argparse.Namespace) -> int:
    payload = ResearchOperations(_client(args)).job_result(args.job_id)
    result = payload.get("result") if isinstance(payload.get("result"), dict) else {}
    output_format = str(getattr(args, "format", "auto") or "auto")
    result_schema = str(result.get("schema_version") or "")
    if output_format == "json":
        _print_json(result or payload)
    elif (output_format in {"auto", "table"}) and result_schema == "research_check_sweep.v1":
        _print_research_leaderboard_table(result)
    else:
        _print_research_job_status(payload, show_next=False)
    return 0


def _print_research_leaderboard_table(payload: dict[str, Any]) -> None:
    leaderboard = payload.get("leaderboard") if isinstance(payload.get("leaderboard"), dict) else {}
    rows = leaderboard.get("rows") if isinstance(leaderboard.get("rows"), list) else []
    rank_by = str(leaderboard.get("rank_by") or "")
    display_paths = [str(item) for item in leaderboard.get("display_metrics") or []]
    headers = ["rank", "variant", "scope", "status", "samples", rank_by, *display_paths, "recommendation", "caveats"]
    table_rows: list[list[str]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        display_by_path = {
            str(item.get("path")): item.get("value")
            for item in row.get("display_metrics") or []
            if isinstance(item, dict)
        }
        table_rows.append(
            [
                str(row.get("rank") or ""),
                str(row.get("variant_label") or row.get("variant_id") or ""),
                str(row.get("scope_id") or ""),
                str(row.get("status") or ""),
                str(row.get("sample_count") or ""),
                _format_metric_value((row.get("rank_metric") or {}).get("value")),
                *[_format_metric_value(display_by_path.get(path)) for path in display_paths],
                str(row.get("recommendation") or ""),
                str(row.get("caveat_count") or 0),
            ]
        )
    if not table_rows:
        print("No ranked rows.", flush=True)
        return
    widths = [
        max(len(str(header)), *(len(row[index]) for row in table_rows))
        for index, header in enumerate(headers)
    ]
    print("  ".join(str(header).ljust(widths[index]) for index, header in enumerate(headers)), flush=True)
    print("  ".join("-" * width for width in widths), flush=True)
    for row in table_rows:
        print("  ".join(value.ljust(widths[index]) for index, value in enumerate(row)), flush=True)


def _format_metric_value(value: Any) -> str:
    if value is None:
        return ""
    try:
        number = float(value)
    except (TypeError, ValueError):
        return str(value)
    return f"{number:.6g}"


def _cmd_research_trail(args: argparse.Namespace) -> int:
    _print_json(ResearchOperations(_client(args)).trail(args.item_id))
    return 0


def _cmd_research_run(args: argparse.Namespace) -> int:
    _print_json(_client(args).request_json("GET", f"/api/research/runs/{args.run_id}/evidence"))
    return 0


def _cmd_research_compare(args: argparse.Namespace) -> int:
    _print_json(
        _client(args).request_json(
            "GET",
            "/api/research/checks/compare",
            params={"left_check_id": args.left_check_id, "right_check_id": args.right_check_id},
        )
    )
    return 0


def _cmd_research_authority_post(args: argparse.Namespace) -> int:
    payload = _read_json_object_arg(args.payload_json, label="--payload-json")
    if not payload:
        raise ValueError("--payload-json is required")
    path = str(args.authority_path).format(**vars(args))
    _print_json(_client(args).request_json("POST", path, payload=payload))
    return 0


def _cmd_research_authority_protocol_get(args: argparse.Namespace) -> int:
    _print_json(
        _client(args).request_json(
            "GET", f"/api/research/authority/protocols/{args.protocol_id}"
        )
    )
    return 0


def _cmd_research_authority_family_evidence(args: argparse.Namespace) -> int:
    _print_json(
        _client(args).request_json(
            "GET", f"/api/research/authority/families/{args.family_id}/evidence"
        )
    )
    return 0


def _cmd_research_governance_case_get(args: argparse.Namespace) -> int:
    _print_json(
        _client(args).request_json(
            "GET", f"/api/research/governance/cases/{args.case_id}"
        )
    )
    return 0


def _cmd_instruments_list(args: argparse.Namespace) -> int:
    payload = _client(args).request_json("GET", "/api/instruments/")
    items = list(payload or [])
    if args.datasource:
        items = [row for row in items if str(row.get("datasource") or "").lower() == str(args.datasource).lower()]
    if args.exchange:
        items = [row for row in items if str(row.get("exchange") or "").lower() == str(args.exchange).lower()]
    if args.symbol:
        needle = str(args.symbol).upper()
        items = [row for row in items if needle in str(row.get("symbol") or "").upper()]
    _print_json({"schema_version": "qt_instruments_list.v1", "items": items, "total": len(items)})
    return 0


def _cmd_instruments_get(args: argparse.Namespace) -> int:
    _print_json(_client(args).request_json("GET", f"/api/instruments/{args.instrument_id}"))
    return 0


def _cmd_instruments_resolve(args: argparse.Namespace) -> int:
    payload = {
        "symbol": args.symbol,
        "datasource": args.datasource,
        "exchange": args.exchange,
        "provider_id": args.provider,
        "venue_id": args.venue,
        "force_refresh": args.force_refresh,
    }
    _print_json(_client(args).request_json("POST", "/api/instruments/resolve", payload=payload))
    return 0


def _cmd_instruments_profile(args: argparse.Namespace) -> int:
    _print_json(
        _client(args).request_json(
            "GET",
            f"/api/instruments/{args.instrument_id}/runtime-profile",
            params={"execution_semantics": args.execution_semantics},
        )
    )
    return 0


def _cmd_instruments_health(args: argparse.Namespace) -> int:
    _print_json(
        _client(args).request_json(
            "GET",
            "/api/instruments/health",
            params={"datasource": args.datasource, "exchange": args.exchange},
        )
    )
    return 0


def _optional_bool_arg(value: str | None) -> bool | None:
    if value is None:
        return None
    normalized = str(value).strip().lower()
    if normalized in {"1", "true", "yes", "y"}:
        return True
    if normalized in {"0", "false", "no", "n"}:
        return False
    raise ValueError(f"invalid boolean value: {value}")


def _cmd_instruments_coverage_matrix(args: argparse.Namespace) -> int:
    payload = {
        "start": args.start,
        "end": args.end,
        "timeframe": args.timeframe,
        "instrument_ids": list(args.instrument_id or []),
        "symbol": args.symbol,
        "datasource": args.datasource,
        "exchange": args.exchange,
        "instrument_type": args.instrument_type,
        "runtime_ready": _optional_bool_arg(args.runtime_ready),
        "research_ready": _optional_bool_arg(args.research_ready),
        "execution_semantics": args.execution_semantics,
    }
    _print_json(_client(args).request_json("POST", "/api/instruments/coverage-matrix", payload=payload))
    return 0


def _cmd_providers_stream_smoke(args: argparse.Namespace) -> int:
    audit = getattr(args, "_audit_log", None)

    def _observe(event: str, fields: dict[str, Any]) -> None:
        if audit is not None:
            audit.record_event(event, **fields)

    payload = {
        "provider_id": args.provider,
        "venue_id": args.venue,
        "symbol": args.symbol,
        "product_id": args.product_id,
        "channels": args.channel or None,
        "timeframe": args.timeframe,
        "auth_mode": args.auth_mode,
        "duration_seconds": args.duration,
        "sample_limit": args.sample_limit,
    }
    client = ApiClient(
        args.api_url,
        timeout=max(float(args.timeout), float(args.duration) + 10.0),
        observer=_observe,
    )
    result = client.request_json("POST", "/api/providers/stream-smoke", payload=payload)
    _print_json(result)
    return 0 if str(result.get("status") or "").lower() == "completed" else 1


def _cmd_providers_list(args: argparse.Namespace) -> int:
    _print_json(_client(args).request_json("GET", "/api/providers/"))
    return 0


def _cmd_provider_credentials_schema(args: argparse.Namespace) -> int:
    _print_json(
        _client(args).request_json(
            "GET",
            "/api/providers/credentials/schema",
            params={
                "provider_id": args.provider,
                "venue_id": args.venue,
                "environment": args.environment,
            },
        )
    )
    return 0


def _secret_env_map(items: list[str] | None) -> dict[str, str]:
    mapping: dict[str, str] = {}
    for item in items or []:
        if "=" not in item:
            raise ValueError(f"expected KEY=ENV_VAR for --secret-env, got {item!r}")
        key, env_name = item.split("=", 1)
        key = key.strip()
        env_name = env_name.strip()
        if not key or not env_name:
            raise ValueError(f"expected non-empty KEY=ENV_VAR for --secret-env, got {item!r}")
        value = os.environ.get(env_name)
        if value is not None and value != "":
            mapping[key] = value
    return mapping


def _collect_credential_values(args: argparse.Namespace, schema: dict[str, Any]) -> dict[str, str]:
    required = [str(key) for key in schema.get("required") or []]
    accepted = [str(key) for key in schema.get("accepted") or []]
    accepted_set = set(accepted)
    credentials: dict[str, str] = {}
    credentials.update({str(k): str(v) for k, v in _read_json_object_arg(args.secrets_json, label="secrets_json").items()})

    if args.from_env:
        for key in accepted:
            value = os.environ.get(key)
            if value is not None and value != "":
                credentials.setdefault(key, value)
    credentials.update(_secret_env_map(args.secret_env))

    unknown = sorted(key for key in credentials if key not in accepted_set)
    if unknown:
        raise ValueError(f"Credential keys are not accepted for this provider/venue: {', '.join(unknown)}")

    if not args.no_input and sys.stdin.isatty():
        for key in required:
            if not credentials.get(key):
                credentials[key] = getpass.getpass(f"{key}: ")

    missing = [key for key in required if not credentials.get(key)]
    if missing:
        raise ValueError(
            "Missing required secrets: "
            + ", ".join(missing)
            + ". Pass --secrets-json -, --from-env, --secret-env KEY=ENV_VAR, or run interactively."
        )
    if not any(credentials.get(key) for key in accepted):
        raise ValueError("No credential values provided.")
    return {key: str(value) for key, value in credentials.items() if str(value)}


def _coinbase_cdp_key_file_credentials(value: str) -> dict[str, str]:
    if str(value).lstrip().startswith("{"):
        raise ValueError(
            "--cdp-key-file accepts a file path or '-' for stdin, not inline JSON"
        )
    payload = _read_json_object_arg(value, label="cdp_key_file")
    api_key = str(payload.get("name") or payload.get("id") or "").strip()
    api_secret = str(payload.get("privateKey") or "").strip()
    missing = [
        field
        for field, present in (
            ("name or id", api_key),
            ("privateKey", api_secret),
        )
        if not present
    ]
    if missing:
        raise ValueError(
            "Coinbase CDP key file is missing required fields: "
            + ", ".join(missing)
        )
    return {
        "COINBASE_API_KEY": api_key,
        "COINBASE_API_SECRET": api_secret,
    }


def _validate_coinbase_signing_credentials(
    credentials: Mapping[str, str],
) -> dict[str, str]:
    try:
        from coinbase import jwt_generator as coinbase_jwt_generator
        import jwt

        token = coinbase_jwt_generator.build_ws_jwt(
            str(credentials.get("COINBASE_API_KEY") or ""),
            str(credentials.get("COINBASE_API_SECRET") or ""),
        )
        algorithm = str(jwt.get_unverified_header(token).get("alg") or "")
    except Exception as exc:
        raise ValueError(
            "Coinbase credentials cannot sign a WebSocket JWT. "
            "Use a current CDP Ed25519 key or a supported ECDSA key."
        ) from exc
    if algorithm not in {"EdDSA", "ES256"}:
        raise ValueError(
            "Coinbase credentials produced an unsupported JWT algorithm: "
            f"{algorithm or 'missing'}"
        )
    return {
        "status": "passed",
        "check": "local_websocket_jwt_signing",
        "algorithm": algorithm,
    }


def _cmd_provider_credentials_add(args: argparse.Namespace) -> int:
    client = _client(args)
    schema = client.request_json(
        "GET",
        "/api/providers/credentials/schema",
        params={
            "provider_id": args.provider,
            "venue_id": args.venue,
            "environment": args.environment,
        },
    )
    if not isinstance(schema, dict):
        raise ApiError("GET credential schema returned an unexpected payload")
    credentials = _collect_credential_values(args, schema)
    payload = {
        "provider_id": schema.get("provider_id"),
        "venue_id": schema.get("venue_id"),
        "credential_ref": args.ref or schema.get("default_credential_ref"),
        "environment": schema.get("environment") or args.environment,
        "display_name": args.display_name,
        "credentials": credentials,
    }
    _print_json(client.request_json("POST", "/api/providers/credentials", payload=payload))
    return 0


def _cmd_provider_credentials_list(args: argparse.Namespace) -> int:
    _print_json(
        _client(args).request_json(
            "GET",
            "/api/providers/credentials",
            params={
                "provider_id": args.provider,
                "venue_id": args.venue,
                "include_revoked": args.include_revoked,
            },
        )
    )
    return 0


def _cmd_provider_credentials_validate(args: argparse.Namespace) -> int:
    _print_json(
        _client(args).request_json(
            "POST",
            f"/api/providers/credentials/{args.credential_ref}/validate",
        )
    )
    return 0


def _cmd_provider_credentials_revoke(args: argparse.Namespace) -> int:
    _print_json(
        _client(args).request_json(
            "DELETE",
            f"/api/providers/credentials/{args.credential_ref}",
        )
    )
    return 0


def _cmd_setup_doctor(args: argparse.Namespace) -> int:
    payload = setup_doctor_payload(
        venv=args.venv,
        api_url=args.api_url,
        timeout=min(float(args.timeout), float(args.backend_timeout)),
        include_backend=not args.no_backend,
    )
    _print_json(payload)
    return 0 if payload.get("status") in {"ok", "degraded"} else 1


def _cmd_setup_env(args: argparse.Namespace) -> int:
    code, payload = setup_env_payload()
    _print_json(payload)
    return code


def _cmd_setup_provider_coinbase(args: argparse.Namespace) -> int:
    provider = "COINBASE"
    venue = "COINBASE_DIRECT"
    client = _client(args)
    schema = client.request_json(
        "GET",
        "/api/providers/credentials/schema",
        params={
            "provider_id": provider,
            "venue_id": venue,
            "environment": args.environment,
        },
    )
    if not isinstance(schema, dict):
        raise ApiError("GET credential schema returned an unexpected payload")

    if args.cdp_key_file:
        if args.secrets_json or args.from_env or args.secret_env:
            raise ValueError(
                "--cdp-key-file cannot be combined with --secrets-json, "
                "--from-env, or --secret-env"
            )
        credentials = _coinbase_cdp_key_file_credentials(args.cdp_key_file)
    else:
        credentials = _collect_credential_values(args, schema)
    signing_validation = _validate_coinbase_signing_credentials(credentials)
    save_payload = {
        "provider_id": provider,
        "venue_id": venue,
        "credential_ref": args.ref or schema.get("default_credential_ref"),
        "environment": schema.get("environment") or args.environment,
        "display_name": args.display_name,
        "credentials": credentials,
    }
    saved = client.request_json("POST", "/api/providers/credentials", payload=save_payload)
    credential_ref = (
        ((saved.get("credential") or {}) if isinstance(saved, dict) else {}).get("credential_ref")
        or args.ref
        or schema.get("default_credential_ref")
    )
    validation = client.request_json("POST", f"/api/providers/credentials/{credential_ref}/validate")

    result: dict[str, Any] = {
        "schema_version": "qt_setup_provider.v1",
        "operation": "provider",
        "provider": provider,
        "venue": venue,
        "environment": schema.get("environment") or args.environment,
        "status": "ok",
        "credential_ref": credential_ref,
        "credential": saved.get("credential") if isinstance(saved, dict) else saved,
        "validation": validation.get("credential") if isinstance(validation, dict) else validation,
        "signing_validation": signing_validation,
        "secrets_are_returned": False,
        "next_steps": [
            "Use this credential_ref in provider-backed paper/live workflows when a ref is required.",
            "Run `./scripts/qt providers stream-smoke --provider COINBASE --venue COINBASE_DIRECT --symbol <symbol> --auth-mode authenticated` when you want a live provider smoke check.",
        ],
    }

    if args.stream_smoke:
        if not args.symbol:
            raise ValueError("--symbol is required with --stream-smoke")
        smoke_payload = {
            "provider_id": provider,
            "venue_id": venue,
            "symbol": args.symbol,
            "product_id": args.product_id,
            "channels": args.channel or None,
            "timeframe": args.timeframe,
            "auth_mode": args.auth_mode,
            "duration_seconds": args.duration,
            "sample_limit": args.sample_limit,
        }
        smoke_client = ApiClient(
            args.api_url,
            timeout=max(float(args.timeout), float(args.duration) + 10.0),
        )
        smoke = smoke_client.request_json("POST", "/api/providers/stream-smoke", payload=smoke_payload)
        result["stream_smoke"] = smoke
        if str(smoke.get("status") or "").lower() != "completed":
            result["status"] = "needs_attention"

    _print_json(result)
    return 0 if result.get("status") == "ok" else 1


def _ensure_run_report_materialized(client: ApiClient, run_id: str, *, force_rebuild: bool = False) -> dict[str, Any]:
    payload = client.request_json(
        "POST",
        f"/api/reports/{run_id}/run-report/build",
        params={"async_build": False, "force_rebuild": force_rebuild},
    )
    if not isinstance(payload, dict):
        raise ApiError(f"POST run-report/build returned unexpected payload type: {type(payload).__name__}")
    return payload


def _start_experiment(args: argparse.Namespace, client: ApiClient) -> dict[str, Any]:
    start_body: dict[str, Any] = {
        "run_type": "backtest",
        "dataset_id": args.dataset_id,
        "economic_claim_intent": "exploration",
    }
    if bool(getattr(args, "profile", False)):
        start_body["profile"] = True
    if getattr(args, "request_id", None):
        start_body["request_id"] = args.request_id
    start_payload = client.request_json("POST", f"/api/bots/{args.bot_id}/runs/start", payload=start_body)
    if not isinstance(start_payload, dict):
        raise ApiError(f"POST start returned unexpected payload type: {type(start_payload).__name__}")
    run_id = str(start_payload.get("run_id") or "").strip()
    request_id = str(start_payload.get("request_id") or getattr(args, "request_id", None) or "").strip() or None
    experiment_id = request_id or run_id
    if not run_id:
        raise ValueError("start response did not include run_id")
    return _write_experiment_record(
        args,
        {
            "kind": "bot_run",
            "experiment_id": experiment_id,
            "request_id": request_id,
            "bot_id": args.bot_id,
            "run_id": run_id,
            "baseline_run_id": getattr(args, "baseline_run_id", None),
            "status": start_payload.get("status"),
            "phase": start_payload.get("phase"),
            "start": start_payload,
            "collect_defaults": {
                "export": bool(getattr(args, "export", False)),
                "include_json": not bool(getattr(args, "no_json", False)),
                "include_csv": not bool(getattr(args, "no_csv", False)),
                "include_candles": bool(getattr(args, "include_candles", False)),
            },
        },
    )


def _collect_experiment(args: argparse.Namespace, client: ApiClient, record: dict[str, Any]) -> tuple[int, dict[str, Any]]:
    bot_id = str(getattr(args, "bot_id", None) or record.get("bot_id") or "").strip()
    run_id = str(record.get("run_id") or "").strip()
    if not bot_id:
        raise ValueError("bot_id is required to collect a raw run id")
    if not run_id:
        raise ValueError("run_id is missing from experiment record")

    wait_code = 0
    status_payload: dict[str, Any]
    if getattr(args, "wait", False):
        wait_code, status_payload = _wait_for_run(
            client,
            bot_id=bot_id,
            run_id=run_id,
            timeout=args.wait_timeout,
            interval=args.interval,
            print_each=args.print_each,
            allow_non_completed=args.allow_non_completed,
            emit_final=False,
        )
    else:
        status_payload = client.request_json("GET", f"/api/bots/{bot_id}/runs/{run_id}/status")
        if not isinstance(status_payload, dict):
            raise ApiError(f"GET run status returned unexpected payload type: {type(status_payload).__name__}")
        wait_code = 0 if _terminal_status(status_payload) == "completed" or args.allow_non_completed else 1

    defaults = dict(record.get("collect_defaults") or {})
    export_requested = bool(getattr(args, "export", False) or defaults.get("export"))
    include_json = not bool(getattr(args, "no_json", False)) if getattr(args, "no_json", False) else bool(defaults.get("include_json", True))
    include_csv = not bool(getattr(args, "no_csv", False)) if getattr(args, "no_csv", False) else bool(defaults.get("include_csv", True))
    include_candles = bool(getattr(args, "include_candles", False) or defaults.get("include_candles"))
    completed = _terminal_status(status_payload) == "completed"
    result: dict[str, Any] = {
        "schema_version": "qt_cli_experiment_collect.v1",
        "experiment_id": record.get("experiment_id"),
        "bot_id": bot_id,
        "run_id": run_id,
        "status": status_payload,
    }
    if export_requested:
        if not completed:
            result["export"] = {"status": "skipped", "reason": "run_not_completed"}
        else:
            result["export"] = _write_report_export(
                args,
                client,
                run_id=run_id,
                include_json=include_json,
                include_csv=include_csv,
                include_candles=include_candles,
            )

    compare_to = str(getattr(args, "compare_to", None) or record.get("baseline_run_id") or "").strip()
    if compare_to and completed:
        result["materialization"] = {
            "baseline": _ensure_run_report_materialized(client, compare_to),
            "variant": _ensure_run_report_materialized(client, run_id),
        }
        result["comparison"] = client.request_json(
            "GET",
            "/api/reports/compare/summary",
            params={
                "left_run_id": compare_to,
                "right_run_id": run_id,
                "include_golden": not bool(getattr(args, "no_golden", False)),
                "require_golden": bool(getattr(args, "require_golden", False)),
            },
        )
    elif compare_to:
        result["comparison"] = {"status": "skipped", "reason": "run_not_completed", "baseline_run_id": compare_to}

    merged = _write_experiment_record(
        args,
        {
            **record,
            "bot_id": bot_id,
            "run_id": run_id,
            "status": status_payload.get("status"),
            "phase": status_payload.get("phase"),
            "collect": result,
        },
    )
    result["record"] = merged.get("paths", {}).get("record")
    return wait_code, result


def _cmd_experiments_start_bot(args: argparse.Namespace) -> int:
    _print_json(_start_experiment(args, _client(args)))
    return 0


def _cmd_experiments_validate_plan(args: argparse.Namespace) -> int:
    _print_json(_validate_plan_payload(args, load_plan(args.plan)))
    return 0


def _cmd_experiments_run_plan(args: argparse.Namespace) -> int:
    plan = load_plan(args.plan)
    validation = _validate_plan_payload(args, plan)
    if args.dry_run:
        _print_json(validation)
        return 0
    _prompt_for_data_preflight(args, validation.get("data_preflight") if isinstance(validation.get("data_preflight"), dict) else None)
    runner = ExperimentRunner(client=_client(args), log_root=_experiment_root(args))
    store, state = runner.create(plan, experiment_id=args.experiment_id)
    data_preflight = validation.get("data_preflight") if isinstance(validation.get("data_preflight"), dict) else None
    if data_preflight:
        preflight_path = store.artifacts_dir / "summaries" / "data_preflight.json"
        preflight_path.parent.mkdir(parents=True, exist_ok=True)
        preflight_path.write_text(json.dumps(data_preflight, indent=2, sort_keys=True, default=str) + "\n", encoding="utf-8")
        state["data_preflight_ref"] = str(preflight_path)
        store.write_state(state)
    state = runner.run(store, plan, state)
    _print_json(state)
    return 0 if state.get("status") == "COMPLETED" else 1


def _cmd_experiments_prepare_instrument_matrix(args: argparse.Namespace) -> int:
    request = _read_json_object_arg(args.request_json, label="--request-json")
    payload = prepare_instrument_matrix_experiment(
        client=_client(args),
        request=request,
        log_root=_experiment_root(args),
        out_path=args.out,
        apply=bool(args.apply),
        confirm=bool(args.confirm),
    )
    _print_json(payload)
    return 0


def _cmd_experiments_resume(args: argparse.Namespace) -> int:
    runner = ExperimentRunner(client=_client(args), log_root=_experiment_root(args))
    store, plan, state = runner.resume(args.ref)
    state = runner.run(store, plan, state)
    _print_json(state)
    return 0 if state.get("status") == "COMPLETED" else 1


def _cmd_experiments_status(args: argparse.Namespace) -> int:
    suite_state = _load_experiment_suite_state(args, args.ref)
    if suite_state is not None:
        _print_json(suite_state)
        return 0
    record = _load_experiment_record(args, args.ref, bot_id=args.bot_id)
    bot_id = str(args.bot_id or record.get("bot_id") or "").strip()
    run_id = str(record.get("run_id") or args.ref).strip()
    if not bot_id:
        raise ValueError("bot_id is required when status is requested by raw run id")
    _print_json(_client(args).request_json("GET", f"/api/bots/{bot_id}/runs/{run_id}/status"))
    return 0


def _cmd_experiments_watch(args: argparse.Namespace) -> int:
    deadline = time.monotonic() + float(args.watch_timeout)
    while True:
        state = _load_experiment_suite_state(args, args.ref)
        if state is None:
            raise ValueError(f"experiment suite state not found for {args.ref!r}")
        if args.print_each:
            _print_json(state)
        status = str(state.get("status") or "")
        if status in {"COMPLETED", "FAILED", "CANCELLED", "PARTIALLY_COMPLETED"}:
            if not args.print_each:
                _print_json(state)
            return 0 if status == "COMPLETED" else 1
        if time.monotonic() >= deadline:
            payload = {**state, "watch_status": "timeout", "timeout_seconds": args.watch_timeout}
            _print_json(payload)
            return 124
        time.sleep(float(args.interval))


def _cmd_experiments_events(args: argparse.Namespace) -> int:
    path = find_experiment_dir(_experiment_root(args), args.ref)
    store = ExperimentStateStore(_experiment_root(args), path=path)
    payload = {
        "schema_version": "experiment_events_view.v1",
        "experiment_id": store.experiment_id,
        "events": read_events(store.events_path, tail=args.tail, event_type=args.type, status=args.status),
    }
    _print_json(payload)
    return 0


def _cmd_experiments_doctor(args: argparse.Namespace) -> int:
    payload = doctor_experiment(_experiment_root(args), args.ref)
    _print_json(payload)
    return 0 if payload.get("status") == "ok" else 1


def _cmd_experiments_summarize(args: argparse.Namespace) -> int:
    payload = summarize_experiment(_experiment_root(args), args.ref)
    if args.out:
        path = Path(args.out).expanduser()
        payload = {**payload, "paths": {**dict(payload.get("paths") or {}), "summary": str(path)}}
        write_experiment_summary(path, payload)
    _print_json(payload)
    return 0


def _cmd_experiments_collect(args: argparse.Namespace) -> int:
    record = _load_experiment_record(args, args.ref, bot_id=args.bot_id)
    code, result = _collect_experiment(args, _client(args), record)
    if not args.print_each:
        _print_json(result)
    return code


def _cmd_experiments_run_bot(args: argparse.Namespace) -> int:
    compatibility = {
        "status": "deprecated_alias",
        "replacement": [
            "qt experiments start-bot BOT_ID --dataset-id DATASET_ID",
            "qt experiments collect REF --wait",
        ],
        "removal": "after the compatibility window",
    }
    if args.export and not args.wait:
        _print_json({
            "schema_version": "qt_cli_deprecation.v1",
            "compatibility": compatibility,
            "error": "--export requires --wait so the report is terminal before export",
        })
        return 2
    if args.print_each:
        _print_json(
            {
                "schema_version": "qt_cli_deprecation.v1",
                "command": "qt experiments run-bot",
                "compatibility": compatibility,
            }
        )
    client = _client(args)
    record = _start_experiment(args, client)
    if args.wait:
        wait_code, result = _collect_experiment(args, client, record)
        if not args.print_each:
            _print_json({**result, "compatibility": compatibility})
        return wait_code
    if not args.print_each:
        _print_json({**record, "compatibility": compatibility})
    return 0


def _add_global_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--api-url", default="http://127.0.0.1:8000", help="Backend API base URL.")
    parser.add_argument("--timeout", type=float, default=30.0, help="HTTP timeout in seconds.")
    parser.add_argument("--log-root", default=os.environ.get("QT_CLI_LOG_ROOT", "logs"), help="Root directory for CLI audit logs and report exports.")
    parser.add_argument("--no-audit-log", action="store_true", help="Disable the per-command CLI audit JSON log.")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Quant-Trad API-backed research CLI.")
    _add_global_args(parser)
    subparsers = parser.add_subparsers(dest="command", required=True)

    setup = subparsers.add_parser("setup", help="Canonical local onboarding and readiness checks.")
    setup_sub = setup.add_subparsers(dest="setup_command", required=True)
    setup_doctor = setup_sub.add_parser("doctor", help="Check local setup readiness.")
    setup_doctor.add_argument("--venv", default=".venv", help="Virtualenv path. Defaults to .venv.")
    setup_doctor.add_argument("--backend-timeout", type=float, default=2.0, help="Bounded backend probe timeout.")
    setup_doctor.add_argument("--no-backend", action="store_true", help="Skip the optional backend health probe.")
    setup_doctor.set_defaults(func=_cmd_setup_doctor)
    setup_env = setup_sub.add_parser("env", help="Create or repair the local operator secrets.env file.")
    setup_env.set_defaults(func=_cmd_setup_env)
    setup_provider = setup_sub.add_parser("provider", help="Provider-specific onboarding through canonical provider APIs.")
    setup_provider_sub = setup_provider.add_subparsers(dest="setup_provider_command", required=True)
    setup_coinbase = setup_provider_sub.add_parser("coinbase", help="Store and validate Coinbase Direct credential refs.")
    setup_coinbase.add_argument("--environment", default="paper")
    setup_coinbase.add_argument("--ref", help="Credential reference. Defaults to provider-venue-environment.")
    setup_coinbase.add_argument("--display-name")
    setup_coinbase.add_argument(
        "--secrets-json",
        help="Secret JSON object as a path, inline object, or '-' for stdin. Prefer '-' so secrets do not enter shell history.",
    )
    setup_coinbase.add_argument(
        "--cdp-key-file",
        help="Coinbase CDP JSON key file path or '-' for stdin. Accepts name/id and privateKey without shell transformation.",
    )
    setup_coinbase.add_argument(
        "--secret-env",
        action="append",
        default=[],
        help="Map a credential key to an environment variable, e.g. COINBASE_API_KEY=QT_COINBASE_KEY.",
    )
    setup_coinbase.add_argument("--from-env", action="store_true", help="Read accepted credential keys from matching environment variables.")
    setup_coinbase.add_argument("--no-input", action="store_true", help="Fail instead of prompting for missing required secrets.")
    setup_coinbase.add_argument("--stream-smoke", action="store_true", help="Run a bounded provider stream smoke check after saving credentials.")
    setup_coinbase.add_argument("--symbol", help="Required with --stream-smoke.")
    setup_coinbase.add_argument("--product-id", help="Provider product id. Defaults to --symbol.")
    setup_coinbase.add_argument("--channel", action="append", default=[], help="Provider channel. Repeat for multiple channels.")
    setup_coinbase.add_argument("--timeframe")
    setup_coinbase.add_argument("--auth-mode", default="authenticated")
    setup_coinbase.add_argument("--duration", type=float, default=10.0, help="Smoke duration in seconds.")
    setup_coinbase.add_argument("--sample-limit", type=int, default=10)
    setup_coinbase.set_defaults(func=_cmd_setup_provider_coinbase)

    health = subparsers.add_parser("health", help="Check backend API health.")
    health.set_defaults(func=_cmd_health)

    bots = subparsers.add_parser("bots", help="Bot inspection and control commands.")
    bots_sub = bots.add_subparsers(dest="bots_command", required=True)
    bots_list = bots_sub.add_parser("list", help="List bots.")
    bots_list.set_defaults(func=_cmd_bots_list)
    bots_create = bots_sub.add_parser("create", help="Create a bot through the backend API.")
    bots_create.add_argument("--payload-json", help="JSON object path, inline object, or '-' for the full create payload.")
    bots_create.add_argument("--name")
    bots_create.add_argument("--strategy-id")
    bots_create.add_argument("--variant-id")
    bots_create.add_argument("--variant-name")
    bots_create.add_argument("--atm-template-id")
    bots_create.add_argument("--datasource")
    bots_create.add_argument("--exchange")
    bots_create.add_argument("--mode")
    bots_create.add_argument("--execution-mode")
    bots_create.add_argument("--execution-behavior", choices=["simulated", "observe-only"])
    bots_create.add_argument("--run-type")
    bots_create.add_argument("--backtest-start")
    bots_create.add_argument("--backtest-end")
    bots_create.add_argument("--snapshot-interval-ms", type=int)
    bots_create.add_argument("--execution-semantics", choices=["spot", "derivative", "proxy_derivative"])
    bots_create.add_argument("--wallet-json", help="wallet_config JSON object path, inline object, or '-'.")
    bots_create.add_argument(
        "--market-data-stream-policy-json",
        help="market_data_stream_policy JSON object path, inline object, or '-'.",
    )
    bots_create.add_argument("--risk-config-json", help="risk_config JSON object path, inline object, or '-'.")
    bots_create.add_argument("--bot-env-json", help="bot_env JSON object path, inline object, or '-'.")
    bots_create.set_defaults(func=_cmd_bots_create)
    bots_get = bots_sub.add_parser("get", help="Get one bot.")
    bots_get.add_argument("bot_id")
    bots_get.set_defaults(func=_cmd_bots_get)
    bots_update = bots_sub.add_parser("update", help="Update bot configuration through the backend API.")
    bots_update.add_argument("bot_id")
    bots_update.add_argument("--payload-json", help="JSON object path, inline object, or '-' for update fields.")
    bots_update.add_argument("--name")
    bots_update.add_argument("--strategy-id")
    bots_update.add_argument("--variant-id")
    bots_update.add_argument("--variant-name")
    bots_update.add_argument("--atm-template-id")
    bots_update.add_argument("--datasource")
    bots_update.add_argument("--exchange")
    bots_update.add_argument("--mode")
    bots_update.add_argument("--execution-mode")
    bots_update.add_argument("--execution-behavior", choices=["simulated", "observe-only"])
    bots_update.add_argument("--run-type")
    bots_update.add_argument("--backtest-start")
    bots_update.add_argument("--backtest-end")
    bots_update.add_argument("--snapshot-interval-ms", type=int)
    bots_update.add_argument("--execution-semantics", choices=["spot", "derivative", "proxy_derivative"])
    bots_update.add_argument("--wallet-json", help="wallet_config JSON object path, inline object, or '-'.")
    bots_update.add_argument(
        "--market-data-stream-policy-json",
        help="market_data_stream_policy JSON object path, inline object, or '-'.",
    )
    bots_update.add_argument("--risk-config-json", help="risk_config JSON object path, inline object, or '-'.")
    bots_update.add_argument("--bot-env-json", help="bot_env JSON object path, inline object, or '-'.")
    bots_update.set_defaults(func=_cmd_bots_update)
    bots_active = bots_sub.add_parser("active", help="Get active run for a bot.")
    bots_active.add_argument("bot_id")
    bots_active.set_defaults(func=_cmd_bots_active)
    bots_runs = bots_sub.add_parser("runs", help="List recent runs for a bot.")
    bots_runs.add_argument("bot_id")
    bots_runs.add_argument("--limit", type=int, default=25)
    bots_runs.set_defaults(func=_cmd_bots_runs)
    bots_start = bots_sub.add_parser("start", help="Start a bot run through the backend API.")
    bots_start.add_argument("bot_id")
    bots_start.add_argument("--request-id")
    bots_start.add_argument(
        "--economic-claim-intent",
        choices=["exploration", "economic", "selection", "promotion"],
        default="exploration",
        help="Immutable economic interpretation for this run (default: exploration).",
    )
    bots_start.add_argument(
        "--execution-assumptions-json",
        help="Versioned execution_assumptions JSON object path, inline object, or '-'.",
    )
    bots_start.add_argument("--run-type", choices=["backtest", "sim_trade", "paper", "live"])
    bots_start.add_argument("--execution-behavior", "--execution", choices=["simulated", "observe-only"], dest="execution_behavior")
    bots_start.add_argument("--dataset-id", help="Required immutable dataset identity for backtest runs.")
    bots_start.add_argument(
        "--profile",
        action="store_true",
        help="Enable opt-in cProfile and process peak-RSS evidence for this backtest run.",
    )
    bots_start.add_argument("--duration-seconds", type=float, help="Optional bounded duration for observe-only paper runs.")
    bots_start.add_argument(
        "--market-data-stream-policy-json",
        help="market_data_stream_policy JSON object path, inline object, or '-'.",
    )
    bots_start.set_defaults(func=_cmd_bots_start)
    bots_stop = bots_sub.add_parser("stop", help="Stop a bot run through the backend API.")
    bots_stop.add_argument("bot_id")
    bots_stop.add_argument("--run-id")
    bots_stop.add_argument("--request-id")
    bots_stop.add_argument("--preserve-container", action="store_true")
    bots_stop.set_defaults(func=_cmd_bots_stop)
    bots_set_strategy = bots_sub.add_parser("set-strategy", help="Update a bot strategy or selected variant through the backend API.")
    bots_set_strategy.add_argument("bot_id")
    bots_set_strategy.add_argument("--strategy-id")
    bots_set_strategy.add_argument("--variant-id")
    bots_set_strategy.add_argument("--variant-name")
    bots_set_strategy.set_defaults(func=_cmd_bots_set_strategy)

    runs = subparsers.add_parser("runs", help="Run lifecycle helpers.")
    runs_sub = runs.add_subparsers(dest="runs_command", required=True)
    runs_wait = runs_sub.add_parser("wait", help="Wait for a bot run to reach a terminal lifecycle status.")
    runs_wait.add_argument("bot_id")
    runs_wait.add_argument("run_id")
    runs_wait.add_argument("--wait-timeout", type=float, default=3600.0)
    runs_wait.add_argument("--interval", type=float, default=30.0)
    runs_wait.add_argument("--print-each", action="store_true")
    runs_wait.add_argument("--allow-non-completed", action="store_true")
    runs_wait.set_defaults(func=_cmd_runs_wait)

    logs = subparsers.add_parser("logs", help="Structured Loki log inspection helpers.")
    logs.add_argument("--loki-url", default=os.environ.get("QT_LOKI_URL", DEFAULT_LOKI_URL))
    logs_sub = logs.add_subparsers(dest="logs_command", required=True)
    logs_run = logs_sub.add_parser("run", help="Fetch structured Loki logs for a run and nearby bot lifecycle.")
    logs_run.add_argument("run_id")
    logs_run.add_argument("--bot-id", help="Include nearby bot lifecycle logs when the run id is absent from those lines.")
    logs_run.add_argument("--start", help="RFC3339 start time. Defaults to --lookback-hours.")
    logs_run.add_argument("--end", help="RFC3339 end time. Defaults to now.")
    logs_run.add_argument("--lookback-hours", type=float, default=6.0)
    logs_run.add_argument("--limit", type=int, default=500)
    logs_run.set_defaults(func=_cmd_logs_run)
    logs_query = logs_sub.add_parser("query", help="Run a raw LogQL query and parse Quant-Trad structured lines.")
    logs_query.add_argument("logql")
    logs_query.add_argument("--start", help="RFC3339 start time. Defaults to --lookback-hours.")
    logs_query.add_argument("--end", help="RFC3339 end time. Defaults to now.")
    logs_query.add_argument("--lookback-hours", type=float, default=6.0)
    logs_query.add_argument("--limit", type=int, default=500)
    logs_query.set_defaults(func=_cmd_logs_query)
    logs_doctor = logs_sub.add_parser("doctor", help="Check Loki/Promtail label visibility for Quant-Trad logs.")
    logs_doctor.add_argument("--start", help="RFC3339 start time. Defaults to --lookback-hours.")
    logs_doctor.add_argument("--end", help="RFC3339 end time. Defaults to now.")
    logs_doctor.add_argument("--lookback-hours", type=float, default=24.0)
    logs_doctor.set_defaults(func=_cmd_logs_doctor)

    strategies = subparsers.add_parser("strategies", help="Strategy, variant, compile, and preview commands.")
    strategies_sub = strategies.add_subparsers(dest="strategies_command", required=True)
    strategies_list = strategies_sub.add_parser("list", help="List strategies.")
    strategies_list.set_defaults(func=_cmd_strategies_list)
    strategies_create = strategies_sub.add_parser("create", help="Create a strategy from the backend strategy JSON contract.")
    strategies_create.add_argument("--payload-json", required=True, help="Strategy create JSON object, path to JSON, or '-' for stdin.")
    strategies_create.set_defaults(func=_cmd_strategies_create)
    strategies_get = strategies_sub.add_parser("get", help="Get a strategy definition payload.")
    strategies_get.add_argument("strategy_id")
    strategies_get.set_defaults(func=_cmd_strategies_get)
    strategies_bindings = strategies_sub.add_parser("bindings", help="Get strategy instrument and indicator bindings.")
    strategies_bindings.add_argument("strategy_id")
    strategies_bindings.set_defaults(func=_cmd_strategies_bindings)
    strategies_rules = strategies_sub.add_parser("rules", help="Get stored strategy rules.")
    strategies_rules.add_argument("strategy_id")
    strategies_rules.set_defaults(func=_cmd_strategies_rules)
    strategies_rule_create = strategies_sub.add_parser("rule-create", help="Create a strategy rule from the backend rule JSON contract.")
    strategies_rule_create.add_argument("strategy_id")
    strategies_rule_create.add_argument("--payload-json", required=True, help="Strategy rule create JSON object, path to JSON, or '-' for stdin.")
    strategies_rule_create.set_defaults(func=_cmd_strategies_rule_create)
    strategies_effective = strategies_sub.add_parser("effective", help="Get the runtime-effective strategy contract.")
    strategies_effective.add_argument("strategy_id")
    strategies_effective.add_argument("--variant-id")
    strategies_effective.add_argument("--variant-name")
    strategies_effective.set_defaults(func=_cmd_strategies_effective)
    strategies_decision_inputs = strategies_sub.add_parser(
        "decision-inputs",
        help="Get attached indicator decision inputs and effective rule references.",
    )
    strategies_decision_inputs.add_argument("strategy_id")
    strategies_decision_inputs.add_argument("--variant-id")
    strategies_decision_inputs.add_argument("--variant-name")
    strategies_decision_inputs.set_defaults(func=_cmd_strategies_decision_inputs)
    strategies_compile = strategies_sub.add_parser("compile", help="Compile a strategy with the default or selected variant.")
    strategies_compile.add_argument("strategy_id")
    strategies_compile.add_argument("--variant-id")
    strategies_compile.add_argument("--variant-name")
    strategies_compile.set_defaults(func=_cmd_strategies_compile)
    strategies_preview = strategies_sub.add_parser("preview", help="Run a strategy preview through the backend API.")
    strategies_preview.add_argument("strategy_id")
    strategies_preview.add_argument("--start", required=True)
    strategies_preview.add_argument("--end", required=True)
    strategies_preview.add_argument("--interval", required=True)
    strategies_preview.add_argument("--instrument-id", action="append", default=[])
    strategies_preview.add_argument("--variant-id")
    strategies_preview.add_argument("--variant-name")
    strategies_preview.add_argument("--examples", type=int, default=5, help="Maximum compact signal examples in summary output.")
    strategies_preview.add_argument("--signals", action="store_true", help="Print compact signal rows instead of the summary.")
    strategies_preview.add_argument("--why-empty", action="store_true", help="Print empty-preview diagnostics instead of the summary.")
    strategies_preview.add_argument("--full", action="store_true", help="Print the full preview artifact.")
    strategies_preview.set_defaults(func=_cmd_strategies_preview)
    strategies_preview_compare = strategies_sub.add_parser("preview-compare", help="Compare compact strategy previews.")
    strategies_preview_compare.add_argument("--start", required=True)
    strategies_preview_compare.add_argument("--end", required=True)
    strategies_preview_compare.add_argument("--interval", required=True)
    strategies_preview_compare.add_argument(
        "--case",
        action="append",
        default=[],
        help="Preview case as LABEL=STRATEGY_ID:INSTRUMENT_ID[,INSTRUMENT_ID].",
    )
    strategies_preview_compare.add_argument(
        "--case-json",
        action="append",
        default=[],
        help="Preview case JSON object/array, inline or path.",
    )
    strategies_preview_compare.add_argument("--examples", type=int, default=5, help="Maximum compact signal examples per case.")
    strategies_preview_compare.add_argument("--signals", action="store_true", help="Include compact signal rows inside case summaries.")
    strategies_preview_compare.set_defaults(func=_cmd_strategies_preview_compare)

    variants = strategies_sub.add_parser("variants", help="Strategy variant commands.")
    variants_sub = variants.add_subparsers(dest="variants_command", required=True)
    variants_list = variants_sub.add_parser("list", help="List variants for a strategy.")
    variants_list.add_argument("strategy_id")
    variants_list.set_defaults(func=_cmd_variants_list)
    variants_create = variants_sub.add_parser("create", help="Create a strategy variant.")
    variants_create.add_argument("strategy_id")
    variants_create.add_argument("--name", required=True)
    variants_create.add_argument("--description")
    variants_create.add_argument("--filters-json", help="Path to a JSON object or array of output filters, or '-' for stdin.")
    variants_create.add_argument("--filter", action="append", default=[], help="Output filter as a JSON object.")
    variants_create.add_argument("--intent", action="append", default=[], help="Rule intent scope for a single output filter.")
    variants_create.add_argument("--rule-id", action="append", default=[], help="Rule ID scope for a single output filter.")
    variants_create.add_argument("--indicator-id", help="Attached indicator ID for a single output filter.")
    variants_create.add_argument("--output-name", help="Indicator output name for a single output filter.")
    variants_create.add_argument("--field", help="Output field for a single output filter.")
    variants_create.add_argument("--operator", default="equals", help="Output filter operator, e.g. equals, >, >=, <, <=, ==, !=.")
    variants_create.add_argument("--value", help="Output filter value. JSON scalar/list values are accepted.")
    variants_create.add_argument("--equals", help="Shortcut for --operator equals --value VALUE.")
    variants_create.add_argument("--is-default", action="store_true")
    variants_create.set_defaults(func=_cmd_variants_create)
    variants_update = variants_sub.add_parser("update", help="Update a strategy variant.")
    variants_update.add_argument("strategy_id")
    variants_update.add_argument("variant_id")
    variants_update.add_argument("--name")
    variants_update.add_argument("--description")
    variants_update.add_argument("--filters-json", help="Path to a replacement JSON object or array of output filters, or '-' for stdin.")
    variants_update.add_argument("--filter", action="append", default=[], help="Replacement output filter as a JSON object.")
    variants_update.add_argument("--intent", action="append", default=[], help="Rule intent scope for a single replacement output filter.")
    variants_update.add_argument("--rule-id", action="append", default=[], help="Rule ID scope for a single replacement output filter.")
    variants_update.add_argument("--indicator-id", help="Attached indicator ID for a single replacement output filter.")
    variants_update.add_argument("--output-name", help="Indicator output name for a single replacement output filter.")
    variants_update.add_argument("--field", help="Output field for a single replacement output filter.")
    variants_update.add_argument("--operator", default="equals", help="Output filter operator, e.g. equals, >, >=, <, <=, ==, !=.")
    variants_update.add_argument("--value", help="Output filter value. JSON scalar/list values are accepted.")
    variants_update.add_argument("--equals", help="Shortcut for --operator equals --value VALUE.")
    variants_update.add_argument("--replace-filters", action="store_true", help="Replace filters with an empty list when no filters are provided.")
    variants_update.add_argument("--is-default", action="store_true")
    variants_update.set_defaults(func=_cmd_variants_update)
    variants_delete = variants_sub.add_parser("delete", help="Delete a non-default strategy variant.")
    variants_delete.add_argument("strategy_id")
    variants_delete.add_argument("variant_id")
    variants_delete.set_defaults(func=_cmd_variants_delete)

    indicators = subparsers.add_parser("indicators", help="Indicator catalog, config, and runtime validation commands.")
    indicators_sub = indicators.add_subparsers(dest="indicators_command", required=True)
    indicators_types = indicators_sub.add_parser("types", help="List registered indicator types.")
    indicators_types.set_defaults(func=_cmd_indicators_types)
    indicators_type = indicators_sub.add_parser("type", help="Fetch one indicator type manifest.")
    indicators_type.add_argument("type_id")
    indicators_type.set_defaults(func=_cmd_indicators_type)
    indicators_list = indicators_sub.add_parser("list", help="List persisted indicator instances.")
    indicators_list.set_defaults(func=_cmd_indicators_list)
    indicators_get = indicators_sub.add_parser("get", help="Fetch one indicator instance.")
    indicators_get.add_argument("indicator_id")
    indicators_get.set_defaults(func=_cmd_indicators_get)
    indicators_strategies = indicators_sub.add_parser("strategies", help="List strategies that reference an indicator.")
    indicators_strategies.add_argument("indicator_id")
    indicators_strategies.set_defaults(func=_cmd_indicators_strategies)

    def add_indicator_payload_args(command: argparse.ArgumentParser) -> None:
        command.add_argument("--payload-json", help="JSON object path, inline object, or '-' for the full indicator payload.")
        command.add_argument("--type")
        command.add_argument("--name")
        command.add_argument("--params-json", help="Indicator params JSON object path, inline object, or '-'.")
        command.add_argument("--param", action="append", default=[], help="Indicator param as key=value. JSON scalar values are accepted.")
        command.add_argument("--dependencies-json", help="Indicator dependencies JSON array path, inline array, or '-'.")
        command.add_argument("--color")
        command.add_argument("--color-palette", dest="color_palette")

    indicators_validate_config = indicators_sub.add_parser(
        "validate-config",
        help="Validate and normalize an indicator config without persisting it.",
    )
    add_indicator_payload_args(indicators_validate_config)
    indicators_validate_config.set_defaults(func=_cmd_indicators_validate_config)
    indicators_create = indicators_sub.add_parser("create", help="Plan or apply indicator creation.")
    add_indicator_payload_args(indicators_create)
    indicators_create.add_argument("--apply", action="store_true")
    indicators_create.add_argument("--confirm", action="store_true")
    indicators_create.set_defaults(func=_cmd_indicators_create)
    indicators_clone = indicators_sub.add_parser("clone", help="Plan or apply a cloned indicator instance.")
    indicators_clone.add_argument("indicator_id")
    add_indicator_payload_args(indicators_clone)
    indicators_clone.add_argument("--apply", action="store_true")
    indicators_clone.add_argument("--confirm", action="store_true")
    indicators_clone.set_defaults(func=_cmd_indicators_clone)
    indicators_edit = indicators_sub.add_parser("edit", help="Plan or apply an indicator edit.")
    indicators_edit.add_argument("indicator_id")
    add_indicator_payload_args(indicators_edit)
    indicators_edit.add_argument("--apply", action="store_true")
    indicators_edit.add_argument("--confirm", action="store_true")
    indicators_edit.set_defaults(func=_cmd_indicators_edit)
    indicators_rm = indicators_sub.add_parser("rm", help="Remove an indicator instance.")
    indicators_rm.add_argument("indicator_id")
    indicators_rm.add_argument("--confirm", action="store_true")
    indicators_rm.set_defaults(func=_cmd_indicators_rm)
    indicators_on = indicators_sub.add_parser("on", help="Enable an indicator instance.")
    indicators_on.add_argument("indicator_id")
    indicators_on.set_defaults(func=_cmd_indicators_toggle, enabled=True)
    indicators_off = indicators_sub.add_parser("off", help="Disable an indicator instance.")
    indicators_off.add_argument("indicator_id")
    indicators_off.set_defaults(func=_cmd_indicators_toggle, enabled=False)

    def add_indicator_window_args(command: argparse.ArgumentParser) -> None:
        command.add_argument("indicator_id")
        command.add_argument("--start", required=True)
        command.add_argument("--end", required=True)
        command.add_argument("--interval", required=True)
        command.add_argument("--instrument-id")
        command.add_argument("--symbol")
        command.add_argument("--datasource")
        command.add_argument("--exchange")

    indicators_validate_runtime = indicators_sub.add_parser(
        "validate-runtime",
        help="Replay a persisted indicator over a market window and validate typed outputs.",
    )
    add_indicator_window_args(indicators_validate_runtime)
    indicators_validate_runtime.add_argument("--require-ready-by-end", action="store_true")
    indicators_validate_runtime.add_argument("--min-ready-bars", type=int)
    indicators_validate_runtime.set_defaults(func=_cmd_indicators_validate_runtime)
    indicators_overlays = indicators_sub.add_parser("overlays", help="Compute indicator overlays for a window.")
    add_indicator_window_args(indicators_overlays)
    indicators_overlays.add_argument("--visibility-epoch", type=int)
    indicators_overlays.add_argument("--cursor-epoch", type=int)
    indicators_overlays.add_argument("--cursor-time")
    indicators_overlays.set_defaults(func=_cmd_indicators_overlays)
    indicators_signals = indicators_sub.add_parser("signals", help="Compute indicator signals for a window.")
    add_indicator_window_args(indicators_signals)
    indicators_signals.add_argument("--config-json", help="Signal config JSON object path, inline object, or '-'.")
    indicators_signals.set_defaults(func=_cmd_indicators_signals)

    reports = subparsers.add_parser("reports", help="Report, export, and comparison commands.")
    reports_sub = reports.add_subparsers(dest="reports_command", required=True)
    reports_list = reports_sub.add_parser("list", help="List completed report summaries.")
    reports_list.add_argument("--type", default="backtest")
    reports_list.add_argument("--status", default="completed")
    reports_list.add_argument("--limit", type=int, default=50)
    reports_list.add_argument("--offset", type=int, default=0)
    reports_list.add_argument("--search")
    reports_list.add_argument("--bot-id")
    reports_list.add_argument("--instrument")
    reports_list.add_argument("--timeframe")
    reports_list.add_argument("--start")
    reports_list.add_argument("--end")
    reports_list.set_defaults(func=_cmd_reports_list)
    for section in (
        "dataset",
        "readiness",
        "summary",
        "sections",
        "diagnostics",
        "metrics",
        "operational-health",
        "run-report",
        "run-report-status",
        "instruments",
        "symbol-summary",
    ):
        command = reports_sub.add_parser(section, help=f"Fetch report {section}.")
        command.add_argument("run_id")
        if section == "run-report":
            command.add_argument("--build", dest="build", action="store_true", default=False)
            command.add_argument("--no-build", dest="build", action="store_false")
            command.add_argument("--force-rebuild", action="store_true")
        command.set_defaults(func=_cmd_report_get, report_section=section)
    manifest = reports_sub.add_parser("manifest", help="Fetch report export manifest.")
    manifest.add_argument("run_id")
    manifest.add_argument("--include-candles", action="store_true")
    manifest.set_defaults(func=_cmd_reports_manifest)
    export = reports_sub.add_parser("export", help="Export a report zip through the backend API.")
    export.add_argument("run_id")
    export.add_argument("--out-dir", help="Report export root. Defaults to --log-root.")
    export.add_argument("--no-json", action="store_true")
    export.add_argument("--no-csv", action="store_true")
    export.add_argument("--include-candles", action="store_true")
    export.set_defaults(func=_cmd_reports_export)
    compare = reports_sub.add_parser("compare", help="Compare two ready materialized run reports.")
    compare.add_argument("left_run_id")
    compare.add_argument("right_run_id")
    compare.add_argument("--no-golden", action="store_true")
    compare.add_argument("--require-golden", action="store_true")
    compare.set_defaults(func=_cmd_reports_compare)
    for section in ("trades", "decisions", "signals"):
        page = reports_sub.add_parser(section, help=f"Fetch report {section}.")
        page.add_argument("run_id")
        page.add_argument("--limit", type=int, default=100)
        page.add_argument("--offset", type=int, default=0)
        page.add_argument("--symbol")
        page.add_argument("--instrument-id")
        if section == "decisions":
            page.add_argument("--state", choices=["accepted", "rejected"])
        page.set_defaults(func=_cmd_reports_page, report_page_section=section)
    candle_catalog = reports_sub.add_parser("candle-catalog", help="Fetch the report candle catalog.")
    candle_catalog.add_argument("run_id")
    candle_catalog.set_defaults(func=_cmd_reports_candle_catalog)
    candles = reports_sub.add_parser("candles", help="Fetch bounded report candles.")
    candles.add_argument("run_id")
    candles.add_argument("--instrument-id", required=True)
    candles.add_argument("--timeframe", required=True)
    candles.add_argument("--start", required=True)
    candles.add_argument("--end", required=True)
    candles.add_argument("--limit", type=int, default=1000)
    candles.add_argument("--offset", type=int, default=0)
    candles.set_defaults(func=_cmd_reports_candles)

    data = subparsers.add_parser("data", help="Market data coverage and availability commands.")
    data_sub = data.add_subparsers(dest="data_command", required=True)
    data_coverage = data_sub.add_parser("coverage", help="Check candle coverage for a canonical instrument/window.")
    data_coverage.add_argument("--instrument-id")
    data_coverage.add_argument("--symbol")
    data_coverage.add_argument("--datasource")
    data_coverage.add_argument("--exchange")
    data_coverage.add_argument("--start", required=True)
    data_coverage.add_argument("--end", required=True)
    data_coverage.add_argument("--timeframe", required=True)
    data_coverage.add_argument("--fail-on-warning", action="store_true")
    data_coverage.set_defaults(func=_cmd_data_coverage)
    data_ingest = data_sub.add_parser(
        "ingest-candles",
        help="Explicitly fetch a bounded provider window and persist accepted candles.",
    )
    data_ingest.add_argument("--instrument-id", required=True)
    data_ingest.add_argument("--start", required=True)
    data_ingest.add_argument("--end", required=True)
    data_ingest.add_argument("--timeframe", required=True)
    data_ingest.add_argument("--source-revision")
    data_ingest.set_defaults(func=_cmd_data_ingest_candles)
    data_numeric = data_sub.add_parser(
        "acquire-numeric-facts",
        help=(
            "Explicitly acquire one bounded manifest-driven numeric fact range; "
            "no network access occurs without --allow-network."
        ),
    )
    data_numeric.add_argument("--manifest-path", required=True)
    data_numeric.add_argument("--binding-id", required=True)
    data_numeric.add_argument(
        "--mode", choices=["current", "historical"], required=True
    )
    data_numeric.add_argument("--start")
    data_numeric.add_argument("--end")
    data_numeric.add_argument("--allow-network", action="store_true")
    data_numeric.add_argument("--requested-by", required=True)
    data_numeric.add_argument("--reason", required=True)
    data_numeric.add_argument("--max-requests", type=int, required=True)
    data_numeric.add_argument("--max-logs", type=int, required=True)
    data_numeric.add_argument("--max-blocks", type=int, required=True)
    data_numeric.add_argument("--max-retries", type=int, default=2)
    data_numeric.add_argument("--repair", action="store_true")
    data_numeric.set_defaults(func=_cmd_data_acquire_numeric_facts)
    data_series = data_sub.add_parser(
        "series", help="Inspect canonical logical market-data series."
    )
    data_series.add_argument("--instrument-id")
    data_series.set_defaults(func=_cmd_data_series)
    data_prepare = data_sub.add_parser(
        "prepare-backtest-dataset",
        help="Resolve requirements, optionally acquire missing facts, and freeze a backtest dataset.",
    )
    data_prepare.add_argument("--bot-id", required=True)
    data_prepare.add_argument("--start", required=True)
    data_prepare.add_argument("--end", required=True)
    data_prepare.add_argument("--acquire-missing", action="store_true")
    data_prepare.add_argument(
        "--numeric-acquisition-json",
        help=(
            "Path, inline object, or '-' containing explicit numeric source "
            "bindings, network authorization, and request budget. Requires "
            "--acquire-missing."
        ),
    )
    data_prepare.add_argument("--created-by")
    data_prepare.set_defaults(func=_cmd_data_prepare_backtest_dataset)

    data_freeze = data_sub.add_parser(
        "freeze-dataset",
        help="Freeze exact typed market facts, provenance, and quality evidence.",
    )
    data_freeze.add_argument(
        "--request-json",
        help="Full market dataset request as a path, inline object, or '-' for stdin.",
    )
    data_freeze.add_argument("--instrument-id")
    data_freeze.add_argument("--start")
    data_freeze.add_argument("--end")
    data_freeze.add_argument("--timeframe")
    data_freeze.add_argument("--name")
    data_freeze.add_argument("--purpose", default="research")
    data_freeze.add_argument("--created-by")
    data_freeze.add_argument(
        "--metadata-json", help="Optional metadata object as a path or inline JSON."
    )
    data_freeze.set_defaults(func=_cmd_data_freeze_dataset)
    data_dataset = data_sub.add_parser(
        "dataset", help="Inspect an immutable market dataset manifest."
    )
    data_dataset.add_argument("dataset_id")
    data_dataset.set_defaults(func=_cmd_data_dataset)

    data_collector_definitions = data_sub.add_parser(
        "collector-definitions",
        help=(
            "Install definitions through reviewed manifests or registered "
            "adapter packs; lifecycle remains separate."
        ),
    )
    data_collector_definitions_sub = data_collector_definitions.add_subparsers(
        dest="data_collector_definitions_command", required=True
    )
    install_structured = data_collector_definitions_sub.add_parser(
        "install-structured",
        help="Install one binding from a checked-in structured Fact manifest.",
    )
    install_structured.add_argument("--manifest-path", required=True)
    install_structured.add_argument("--binding-id", required=True)
    install_structured.add_argument("--enabled", action="store_true")
    install_structured.add_argument("--max-attempts", type=int, default=3)
    install_structured.add_argument(
        "--minimum-spacing-seconds", type=float, default=1.0
    )
    install_structured.set_defaults(
        func=_cmd_data_collector_definitions_install_structured
    )
    enroll_product = data_collector_definitions_sub.add_parser(
        "enroll-product",
        help=(
            "Validate and enroll one provider product through an existing "
            "deployed collector adapter pack."
        ),
    )
    enroll_product.add_argument("--provider", default="COINBASE")
    enroll_product.add_argument("--venue", default="COINBASE_DIRECT")
    enroll_product.add_argument("--product-id", required=True)
    enroll_product.add_argument(
        "--collector",
        dest="collector_type",
        action="append",
        choices=["open_interest", "funding_rate", "market_trades", "level2"],
        help="Collector type to enroll. Repeat; defaults to all supported types.",
    )
    enroll_product.add_argument(
        "--poll-interval-seconds", type=int, default=60
    )
    enroll_product.add_argument("--request-id")
    enroll_product.add_argument("--actor-id")
    enroll_product.add_argument("--reason", required=True)
    enroll_product.add_argument(
        "--confirm",
        action="store_true",
        help="Confirm provider validation and collector definition enrollment.",
    )
    enroll_product.set_defaults(
        func=_cmd_data_collector_definitions_enroll_product
    )
    data_collectors = data_sub.add_parser(
        "collectors", help="Inspect and safely operate registered collectors."
    )
    data_collectors_sub = data_collectors.add_subparsers(
        dest="data_collectors_command", required=True
    )
    data_collectors_fleet = data_collectors_sub.add_parser(
        "fleet", help="Inspect the canonical collector fleet snapshot."
    )
    data_collectors_fleet.add_argument("--attempt-limit", type=int, default=5)
    data_collectors_fleet.set_defaults(func=_cmd_data_collectors_fleet)
    data_collectors_plane = data_collectors_sub.add_parser(
        "plane", help="Inspect aggregate market-data-plane readiness."
    )
    data_collectors_plane.set_defaults(func=_cmd_data_collectors_plane)
    collector_kinds = ("scheduled_fact", "continuous_stream")
    data_collectors_detail = data_collectors_sub.add_parser(
        "detail", help="Inspect runtime, acquisition, facts, gaps, and configuration."
    )
    data_collectors_detail.add_argument("collector_kind", choices=collector_kinds)
    data_collectors_detail.add_argument("collector_id")
    data_collectors_detail.add_argument("--limit", type=int, default=100)
    data_collectors_detail.set_defaults(func=_cmd_data_collectors_detail)
    data_collectors_diagnose = data_collectors_sub.add_parser(
        "diagnose", help="Diagnose likely collector failure boundaries."
    )
    data_collectors_diagnose.add_argument(
        "collector_kind", choices=collector_kinds
    )
    data_collectors_diagnose.add_argument("collector_id")
    data_collectors_diagnose.set_defaults(func=_cmd_data_collectors_diagnose)
    data_collectors_probe = data_collectors_sub.add_parser(
        "probe", help="Run a read-only collector health probe."
    )
    data_collectors_probe.add_argument("collector_kind", choices=collector_kinds)
    data_collectors_probe.add_argument("collector_id")
    data_collectors_probe.set_defaults(func=_cmd_data_collectors_probe)
    for inspect_surface in ("events", "gaps"):
        command = data_collectors_sub.add_parser(
            inspect_surface,
            help=f"Inspect collector {inspect_surface} evidence.",
        )
        command.add_argument("collector_kind", choices=collector_kinds)
        command.add_argument("collector_id")
        command.add_argument("--limit", type=int, default=100)
        command.set_defaults(
            func=_cmd_data_collectors_inspect,
            inspect_surface=inspect_surface,
        )
    for collector_action in ("start", "stop", "restart", "pause", "resume"):
        command = data_collectors_sub.add_parser(
            collector_action,
            help=f"Request an audited collector {collector_action}.",
        )
        command.add_argument("collector_kind", choices=collector_kinds)
        command.add_argument("collector_id")
        command.add_argument("--request-id")
        command.add_argument("--actor-id")
        command.add_argument("--reason", required=True)
        command.add_argument(
            "--confirm",
            action="store_true",
            help="Confirm disruptive stop or restart operations.",
        )
        command.set_defaults(
            func=_cmd_data_collectors_action,
            collector_action=collector_action,
        )

    data_oi_latest = data_sub.add_parser(
        "open-interest-latest",
        help="Read latest causally known stored OI without provider fallback.",
    )
    data_oi_latest.add_argument("--instrument-id", required=True)
    data_oi_latest.add_argument("--decision-time", required=True)
    data_oi_latest.add_argument("--max-staleness-seconds", type=int, required=True)
    data_oi_latest.add_argument("--optional", action="store_true")
    data_oi_latest.set_defaults(func=_cmd_data_open_interest_latest)
    data_funding_latest = data_sub.add_parser(
        "funding-rate-latest",
        help="Read latest causally known stored funding without provider fallback.",
    )
    data_funding_latest.add_argument("--instrument-id", required=True)
    data_funding_latest.add_argument("--decision-time", required=True)
    data_funding_latest.add_argument(
        "--max-staleness-seconds", type=int, required=True
    )
    data_funding_latest.add_argument("--optional", action="store_true")
    data_funding_latest.set_defaults(func=_cmd_data_funding_rate_latest)
    data_market_structure_proof = data_sub.add_parser(
        "market-structure-proof",
        help="Capture bounded Coinbase trade/L2 proof evidence and capacity metrics locally.",
    )
    data_market_structure_proof.add_argument(
        "--product-id",
        action="append",
        default=[],
        help="Allowlisted provider product ID. Repeat; defaults to BIP/BTC-USD.",
    )
    data_market_structure_proof.add_argument(
        "--channel",
        action="append",
        default=[],
        choices=["market_trades", "level2", "ticker"],
        help="Channel subscribed on each product proof connection. Repeat for multiple channels.",
    )
    data_market_structure_proof.add_argument(
        "--auth-mode",
        choices=["public", "authenticated"],
        default="public",
    )
    data_market_structure_proof.add_argument(
        "--duration",
        type=float,
        default=60.0,
        help="Capture duration in seconds, from 1 through 86400.",
    )
    data_market_structure_proof.add_argument(
        "--reconnect-interval",
        type=float,
        help="Deliberately reconnect each stream at this interval for reset/resnapshot proof.",
    )
    data_market_structure_proof.add_argument("--sample-limit", type=int, default=3)
    data_market_structure_proof.add_argument("--rest-limit", type=int, default=20)
    data_market_structure_proof.add_argument(
        "--max-annual-archive-gib",
        type=float,
        help="Explicit operator-approved annual archive byte budget for this captured scope.",
    )
    data_market_structure_proof.add_argument(
        "--output-dir",
        help="Local proof output directory; defaults under logs/market-structure-proof/.",
    )
    data_market_structure_proof.set_defaults(func=_cmd_data_market_structure_proof)

    data_market_structure = data_sub.add_parser(
        "market-structure",
        help="Configure, capture, inspect, and replay the bounded market-structure plane.",
    )
    data_market_structure_sub = data_market_structure.add_subparsers(
        dest="data_market_structure_command",
        required=True,
    )
    data_market_structure_enroll = data_market_structure_sub.add_parser(
        "enroll",
        help="Apply a validated product and stream enrollment manifest.",
    )
    data_market_structure_enroll.add_argument("--manifest-path")
    data_market_structure_enroll.set_defaults(func=_cmd_data_market_structure_enroll)
    data_normalization_specs_install = data_market_structure_sub.add_parser(
        "normalization-specs-install",
        help="Install the immutable approved normalization specs.",
    )
    data_normalization_specs_install.add_argument("--approved-by", required=True)
    data_normalization_specs_install.set_defaults(
        func=_cmd_data_market_structure_normalization_specs_install
    )
    data_normalization_specs = data_market_structure_sub.add_parser(
        "normalization-specs",
        help="List installed immutable normalization specs.",
    )
    data_normalization_specs.set_defaults(
        func=_cmd_data_market_structure_normalization_specs
    )
    for command, help_text, handler in (
        (
            "normalize",
            "Materialize one causal normalized series from canonical stored facts.",
            _cmd_data_market_structure_normalize,
        ),
        (
            "normalization-compare",
            "Recompute and compare normalized facts with persisted revisions.",
            _cmd_data_market_structure_normalization_compare,
        ),
    ):
        normalization_parser = data_market_structure_sub.add_parser(
            command, help=help_text
        )
        normalization_parser.add_argument("spec_id")
        normalization_parser.add_argument("source_series_id", type=int)
        normalization_parser.add_argument("--start", required=True)
        normalization_parser.add_argument("--end", required=True)
        normalization_parser.add_argument("--known-at", required=True)
        normalization_parser.add_argument("--as-of-commit-seq", type=int)
        normalization_parser.set_defaults(func=handler)
    data_market_structure_definitions = data_market_structure_sub.add_parser(
        "definitions",
        help="Inspect stream definitions, runtime state, and leases.",
    )
    data_market_structure_definitions.add_argument("--definition-id")
    data_market_structure_definitions.set_defaults(
        func=_cmd_data_market_structure_definitions
    )
    data_market_structure_sessions = data_market_structure_sub.add_parser(
        "sessions",
        help="Inspect immutable session lifecycle evidence.",
    )
    data_market_structure_sessions.add_argument("--definition-id")
    data_market_structure_sessions.add_argument("--limit", type=int, default=100)
    data_market_structure_sessions.set_defaults(func=_cmd_data_market_structure_sessions)
    data_market_structure_status = data_market_structure_sub.add_parser(
        "status",
        help="Inspect archive, quality, coverage, and capacity blockers.",
    )
    data_market_structure_status.add_argument("definition_id")
    data_market_structure_status.set_defaults(func=_cmd_data_market_structure_status)
    data_market_structure_capture = data_market_structure_sub.add_parser(
        "capture",
        help="Run one explicitly bounded trade or Level 2 capture.",
    )
    data_market_structure_capture.add_argument("definition_id")
    data_market_structure_capture.add_argument("--duration", type=float, default=60.0)
    data_market_structure_capture.add_argument("--storage-root")
    data_market_structure_capture.add_argument("--owner-id")
    data_market_structure_capture.set_defaults(func=_cmd_data_market_structure_capture)
    for safety_action in ("halt", "acknowledge"):
        safety_parser = data_market_structure_sub.add_parser(
            f"safety-{safety_action}",
            help=(
                "Latch collection off at a global, fleet, or stream scope."
                if safety_action == "halt"
                else "Acknowledge and release a persistent collector safety latch."
            ),
        )
        safety_parser.add_argument(
            "--scope-type", choices=["global", "fleet", "stream"], required=True
        )
        safety_parser.add_argument("--scope-id", required=True)
        safety_parser.add_argument("--request-id", required=True)
        safety_parser.add_argument("--requested-by", required=True)
        safety_parser.add_argument("--reason", required=True)
        safety_parser.add_argument("--policy-hash", required=True)
        safety_parser.add_argument("--evidence-json")
        safety_parser.set_defaults(
            func=_cmd_data_market_structure_safety_change,
            safety_action=safety_action,
        )
    data_market_structure_safety_status = data_market_structure_sub.add_parser(
        "safety-status",
        help="Inspect persistent collector safety latches and immutable events.",
    )
    data_market_structure_safety_status.add_argument("--limit", type=int, default=100)
    data_market_structure_safety_status.set_defaults(
        func=_cmd_data_market_structure_safety_status
    )
    data_market_structure_continuous_evidence = data_market_structure_sub.add_parser(
        "continuous-evidence",
        help="Inspect system-derived validation, archive, mapping, and coverage blockers.",
    )
    data_market_structure_continuous_evidence.add_argument("definition_id")
    data_market_structure_continuous_evidence.add_argument("session_id")
    data_market_structure_continuous_evidence.set_defaults(
        func=_cmd_data_market_structure_continuous_evidence
    )
    data_market_structure_replay = data_market_structure_sub.add_parser(
        "replay",
        help="Verify one acknowledged raw manifest and deterministic trade replay.",
    )
    data_market_structure_replay.add_argument("manifest_id")
    data_market_structure_replay.add_argument("--storage-root")
    data_market_structure_replay.set_defaults(func=_cmd_data_market_structure_replay)
    data_market_structure_replay_book = data_market_structure_sub.add_parser(
        "replay-book",
        help="Verify raw-to-book and checkpoint-plus-delta replay for one Level 2 session.",
    )
    data_market_structure_replay_book.add_argument("definition_id")
    data_market_structure_replay_book.add_argument("session_id")
    data_market_structure_replay_book.add_argument("--storage-root")
    data_market_structure_replay_book.set_defaults(
        func=_cmd_data_market_structure_replay_book
    )
    data_market_structure_compact = data_market_structure_sub.add_parser(
        "compact",
        help="Compact an explicit contiguous active raw-manifest set without deleting sources.",
    )
    data_market_structure_compact.add_argument("definition_id")
    data_market_structure_compact.add_argument("session_id")
    data_market_structure_compact.add_argument(
        "--manifest-id", action="append", required=True
    )
    data_market_structure_compact.add_argument("--storage-root")
    data_market_structure_compact.add_argument("--owner-id")
    data_market_structure_compact.set_defaults(
        func=_cmd_data_market_structure_compact
    )
    data_market_structure_retention_pin = data_market_structure_sub.add_parser(
        "retention-pin",
        help="Append an explicit archive/checkpoint retention pin or release revision.",
    )
    data_market_structure_retention_pin.add_argument(
        "target_kind", choices=("raw_manifest", "book_checkpoint")
    )
    data_market_structure_retention_pin.add_argument("target_id")
    data_market_structure_retention_pin.add_argument("--owner-kind", required=True)
    data_market_structure_retention_pin.add_argument("--owner-id", required=True)
    data_market_structure_retention_pin.add_argument("--reason", required=True)
    data_market_structure_retention_pin.add_argument("--release", action="store_true")
    data_market_structure_retention_pin.set_defaults(
        func=_cmd_data_market_structure_retention_pin
    )
    data_market_structure_retention_status = data_market_structure_sub.add_parser(
        "retention-status",
        help="Inspect dataset/explicit pins and ordinary-retention eligibility.",
    )
    data_market_structure_retention_status.add_argument(
        "target_kind", choices=("raw_manifest", "book_checkpoint")
    )
    data_market_structure_retention_status.add_argument("target_id")
    data_market_structure_retention_status.set_defaults(
        func=_cmd_data_market_structure_retention_status
    )
    data_market_structure_lifecycle_plan = data_market_structure_sub.add_parser(
        "lifecycle-plan",
        help="Plan pin-safe archive and Timescale lifecycle work without mutation.",
    )
    data_market_structure_lifecycle_plan.set_defaults(
        func=_cmd_data_market_structure_lifecycle_plan
    )
    data_market_structure_lifecycle_run = data_market_structure_sub.add_parser(
        "lifecycle-run",
        help="Run lifecycle work; remains a dry-run unless --execute is supplied and enabled.",
    )
    data_market_structure_lifecycle_run.add_argument(
        "--execute", action="store_true"
    )
    data_market_structure_lifecycle_run.add_argument("--storage-root")
    data_market_structure_lifecycle_run.add_argument("--owner-id")
    data_market_structure_lifecycle_run.set_defaults(
        func=_cmd_data_market_structure_lifecycle_run
    )
    data_market_structure_lifecycle_events = data_market_structure_sub.add_parser(
        "lifecycle-events",
        help="Inspect immutable lifecycle completion, skip, and failure evidence.",
    )
    data_market_structure_lifecycle_events.add_argument(
        "--limit", type=int, default=200
    )
    data_market_structure_lifecycle_events.set_defaults(
        func=_cmd_data_market_structure_lifecycle_events
    )
    data_market_structure_reconcile = data_market_structure_sub.add_parser(
        "reconcile-recent",
        help="Compare a bounded recent Coinbase REST window with canonical trade IDs.",
    )
    data_market_structure_reconcile.add_argument("definition_id")
    data_market_structure_reconcile.add_argument("--limit", type=int, default=100)
    data_market_structure_reconcile.set_defaults(
        func=_cmd_data_market_structure_reconcile_recent
    )

    research = subparsers.add_parser("research", help="Research memory and lightweight historical checks.")
    research_sub = research.add_subparsers(dest="research_command", required=True)
    research_items = research_sub.add_parser("items", help="Research memory item commands.")
    research_items_sub = research_items.add_subparsers(dest="research_items_command", required=True)
    research_items_list = research_items_sub.add_parser("list", help="List research memory items.")
    research_items_list.add_argument("--kind", choices=["observation", "research_check", "hypothesis", "study"])
    research_items_list.add_argument("--status")
    research_items_list.add_argument("--symbol")
    research_items_list.add_argument("--timeframe")
    research_items_list.add_argument("--limit", type=int, default=100)
    research_items_list.set_defaults(func=_cmd_research_items_list)
    research_items_get = research_items_sub.add_parser("get", help="Fetch one research memory item.")
    research_items_get.add_argument("item_id")
    research_items_get.set_defaults(func=_cmd_research_items_get)
    research_items_create = research_items_sub.add_parser("create", help="Create a research memory item.")
    research_items_create.add_argument("--payload-json", help="JSON object path, inline object, or '-' for the full item payload.")
    research_items_create.add_argument("--kind", choices=["observation", "research_check", "hypothesis", "study"])
    research_items_create.add_argument("--status")
    research_items_create.add_argument("--title")
    research_items_create.add_argument("--body")
    research_items_create.add_argument("--instrument-id")
    research_items_create.add_argument("--symbol")
    research_items_create.add_argument("--timeframe")
    research_items_create.add_argument("--datasource")
    research_items_create.add_argument("--exchange")
    research_items_create.add_argument("--window-start")
    research_items_create.add_argument("--window-end")
    research_items_create.add_argument("--tag", action="append", default=[])
    research_items_create.add_argument("--payload", help="Item payload JSON object path, inline object, or '-'.")
    research_items_create.set_defaults(func=_cmd_research_items_create)

    research_observe = research_sub.add_parser("observe", help="Observation capture commands.")
    research_observe_sub = research_observe.add_subparsers(dest="research_observe_command", required=True)
    research_observe_create = research_observe_sub.add_parser("create", help="Capture a market observation.")
    research_observe_create.add_argument("--payload-json", help="JSON object path, inline object, or '-' for the full item payload.")
    research_observe_create.add_argument("--status")
    research_observe_create.add_argument("--title")
    research_observe_create.add_argument("--body")
    research_observe_create.add_argument("--instrument-id")
    research_observe_create.add_argument("--symbol")
    research_observe_create.add_argument("--timeframe")
    research_observe_create.add_argument("--datasource")
    research_observe_create.add_argument("--exchange")
    research_observe_create.add_argument("--window-start")
    research_observe_create.add_argument("--window-end")
    research_observe_create.add_argument("--tag", action="append", default=[])
    research_observe_create.add_argument("--payload", help="Observation payload JSON object path, inline object, or '-'.")
    research_observe_create.set_defaults(func=_cmd_research_observe_create)
    research_observe_from_check = research_observe_sub.add_parser(
        "from-check",
        help="Create an evidence-bearing Observation from a durable Check result.",
    )
    research_observe_from_check.add_argument("check_id")
    research_observe_from_check.add_argument(
        "--request-json",
        help="Observation metadata JSON object path, inline object, or '-'.",
    )
    research_observe_from_check.add_argument("--title")
    research_observe_from_check.add_argument("--body")
    research_observe_from_check.add_argument("--status")
    research_observe_from_check.add_argument("--tag", action="append", default=[])
    research_observe_from_check.set_defaults(
        func=_cmd_research_observe_from_check
    )

    research_links = research_sub.add_parser("links", help="Research memory link commands.")
    research_links_sub = research_links.add_subparsers(dest="research_links_command", required=True)
    research_links_create = research_links_sub.add_parser("create", help="Create or update a research memory link.")
    research_links_create.add_argument("--payload-json", help="JSON object path, inline object, or '-' for the full link payload.")
    research_links_create.add_argument("--source-item-id")
    research_links_create.add_argument("--target-type")
    research_links_create.add_argument("--target-id")
    research_links_create.add_argument("--relation")
    research_links_create.add_argument("--metadata-json", help="Link metadata JSON object path, inline object, or '-'.")
    research_links_create.set_defaults(func=_cmd_research_links_create)
    research_links_list = research_links_sub.add_parser("list", help="List links connected to a research item.")
    research_links_list.add_argument("item_id")
    research_links_list.add_argument("--outbound-only", action="store_false", dest="include_inbound")
    research_links_list.set_defaults(func=_cmd_research_links_list)

    research_jobs = research_sub.add_parser("jobs", help="Asynchronous research check job commands.")
    research_jobs_sub = research_jobs.add_subparsers(dest="research_jobs_command", required=True)
    research_jobs_status = research_jobs_sub.add_parser("status", help="Show a dispatched research job status.")
    research_jobs_status.add_argument("job_id")
    research_jobs_status.add_argument("--json", action="store_true", help="Print the machine-readable status payload.")
    research_jobs_status.set_defaults(func=_cmd_research_job_status)
    research_jobs_result = research_jobs_sub.add_parser("result", help="Print a completed research job result.")
    research_jobs_result.add_argument("job_id")
    research_jobs_result.add_argument("--format", choices=["auto", "json", "table", "summary"], default="auto")
    research_jobs_result.set_defaults(func=_cmd_research_job_result)

    def add_research_check_base_args(command: argparse.ArgumentParser) -> None:
        command.add_argument("--request-json", help="Check operation JSON object path, inline object, or '-'.")
        command.add_argument("--title")
        command.add_argument("--body")
        command.add_argument("--observation-id")
        command.add_argument("--detector-json", help="Detector JSON object path, inline object, or '-'.")
        command.add_argument("--min-sample-count", type=int)
        command.add_argument("--bucket-by", help="Comma-separated bucket fields.")
        command.add_argument("--max-examples", type=int)
        command.add_argument("--tag", action="append", default=[])
        command.add_argument("--dispatch", action="store_true", help="Queue the check as an async research job and return the job id.")

    def add_research_window_args(command: argparse.ArgumentParser) -> None:
        command.add_argument("--instrument-id")
        command.add_argument("--symbol")
        command.add_argument("--datasource")
        command.add_argument("--exchange")
        command.add_argument("--timeframe", required=True)
        command.add_argument("--start", required=True)
        command.add_argument("--end", required=True)

    def add_forward_outcome_args(command: argparse.ArgumentParser) -> None:
        command.add_argument("--forward-bars")
        command.add_argument("--entry-lag-bars", type=int)
        command.add_argument("--direction", choices=["long", "short"])
        command.add_argument("--min-edge-pct", type=float)

    research_check = research_sub.add_parser(
        "check", help="Plan, preview, execute, and replay canonical analytical Checks."
    )
    research_check_sub = research_check.add_subparsers(dest="research_check_command", required=True)
    research_check_requirements = research_check_sub.add_parser(
        "requirements",
        help="Resolve direct and transitive Check requirements without acquisition.",
    )
    research_check_requirements.add_argument(
        "--request-json", required=True, help="Check request JSON object path, inline object, or '-'."
    )
    research_check_requirements.set_defaults(
        func=_cmd_research_check_requirements
    )

    research_check_preview = research_check_sub.add_parser(
        "preview",
        help="Run an ephemeral watermark-pinned Check preview.",
    )
    research_check_preview.add_argument(
        "--request-json", required=True, help="Check request JSON object path, inline object, or '-'."
    )
    research_check_preview.set_defaults(func=_cmd_research_check_preview)

    research_check_prepare = research_check_sub.add_parser(
        "prepare",
        help="Resolve evidence requirements and optionally freeze known facts and gaps.",
    )
    research_check_prepare.add_argument(
        "--request-json", required=True, help="Check request JSON object path, inline object, or '-'."
    )
    research_check_prepare.add_argument(
        "--freeze", action="store_true", help="Create or reuse the immutable Dataset after resolution."
    )
    research_check_prepare.add_argument("--created-by")
    research_check_prepare.add_argument("--dataset-name")
    research_check_prepare.set_defaults(func=_cmd_research_check_prepare)

    research_check_run = research_check_sub.add_parser(
        "run",
        help="Persist provider-free Check evidence against an immutable input.",
    )
    research_check_run.add_argument(
        "--request-json", required=True, help="Check request JSON object path, inline object, or '-'."
    )
    research_check_run.add_argument("--dataset-id")
    research_check_run.add_argument(
        "--dispatch", action="store_true", help="Queue evidence execution and return the research job id."
    )
    research_check_run.set_defaults(func=_cmd_research_check_run)

    research_check_replay = research_check_sub.add_parser(
        "replay", help="Replay durable Check evidence through the canonical execution path."
    )
    research_check_replay.add_argument("check_id")
    research_check_replay.set_defaults(func=_cmd_research_check_replay)

    research_check_raw = research_check_sub.add_parser("raw", help="Check raw OHLCV conditions against forward outcomes.")
    add_research_check_base_args(research_check_raw)
    add_research_window_args(research_check_raw)
    add_forward_outcome_args(research_check_raw)
    research_check_raw.add_argument("--field", help="Raw field: open, high, low, close, volume, or previous_*.")
    research_check_raw.add_argument("--operator", default="lt", help="Detector operator, e.g. lt, lte, gt, gte, eq, between.")
    research_check_raw.add_argument("--value", help="Detector comparison value. JSON scalar/list values are accepted.")
    research_check_raw.add_argument("--value-field", help="Compare detector field to another raw field.")
    research_check_raw.set_defaults(func=_cmd_research_check_raw)

    research_check_indicator = research_check_sub.add_parser("indicator", help="Check persisted indicator output events against forward outcomes.")
    add_research_check_base_args(research_check_indicator)
    add_research_window_args(research_check_indicator)
    add_forward_outcome_args(research_check_indicator)
    research_check_indicator.add_argument("--indicator-id", required=True)
    research_check_indicator.add_argument("--output", help="Indicator output name.")
    research_check_indicator.add_argument("--event-key")
    research_check_indicator.add_argument("--field", help="Indicator output value field.")
    research_check_indicator.add_argument("--operator", default="eq")
    research_check_indicator.add_argument("--value")
    research_check_indicator.add_argument("--value-field")
    research_check_indicator.set_defaults(func=_cmd_research_check_indicator)

    research_check_audit = research_check_sub.add_parser("audit", help="Audit signal emissions against public indicator output expectations.")
    add_research_check_base_args(research_check_audit)
    add_research_window_args(research_check_audit)
    research_check_audit.add_argument("--indicator-id", required=True)
    research_check_audit.add_argument("--name")
    research_check_audit.add_argument("--expectation-type", choices=["transition", "condition"], default="transition")
    research_check_audit.add_argument("--source-output")
    research_check_audit.add_argument("--source-field")
    research_check_audit.add_argument("--from", dest="from_value")
    research_check_audit.add_argument("--to", dest="to_value")
    research_check_audit.add_argument("--same-group-by", help="Comma-separated source-output fields that must stay equal across a transition.")
    research_check_audit.add_argument("--signal-output")
    research_check_audit.add_argument("--event-key")
    research_check_audit.add_argument("--operator", default="eq")
    research_check_audit.add_argument("--value")
    research_check_audit.add_argument("--value-field")
    research_check_audit.set_defaults(func=_cmd_research_check_audit)

    research_check_lifecycle = research_check_sub.add_parser("lifecycle", help="Audit generic indicator candidate lifecycle funnels.")
    add_research_check_base_args(research_check_lifecycle)
    add_research_window_args(research_check_lifecycle)
    research_check_lifecycle.add_argument("--indicator-id", required=True)
    research_check_lifecycle.add_argument("--output-name", help="Lifecycle output name.")
    research_check_lifecycle.add_argument("--family")
    research_check_lifecycle.add_argument("--side")
    research_check_lifecycle.add_argument("--stage")
    research_check_lifecycle.add_argument("--status")
    research_check_lifecycle.add_argument("--signal-output")
    research_check_lifecycle.add_argument("--signal-event-key")
    research_check_lifecycle.add_argument("--funnel-stages", help="Comma-separated ordered lifecycle stages for funnel reporting.")
    research_check_lifecycle.add_argument("--terminal-stages", help="Comma-separated stages that close a candidate.")
    research_check_lifecycle.add_argument("--signal-stages", help="Comma-separated lifecycle stages that should reconcile to emitted signals.")
    research_check_lifecycle.set_defaults(func=_cmd_research_check_lifecycle)

    research_check_sweep = research_check_sub.add_parser("sweep", help="Run non-persisted indicator research check variants and rank emitted metrics.")
    research_check_sweep.add_argument("--request-json", help="research_check_sweep.v1 JSON object path, inline object, or '-'.")
    research_check_sweep.add_argument("--title")
    research_check_sweep.add_argument("--check-family", choices=["indicator_forward_outcome", "signal_audit", "candidate_lifecycle"])
    research_check_sweep.add_argument("--indicator-id")
    research_check_sweep.add_argument("--instrument-id")
    research_check_sweep.add_argument("--symbol")
    research_check_sweep.add_argument("--datasource")
    research_check_sweep.add_argument("--exchange")
    research_check_sweep.add_argument("--timeframe")
    research_check_sweep.add_argument("--start")
    research_check_sweep.add_argument("--end")
    research_check_sweep.add_argument("--detector-json", help="Detector JSON object path, inline object, or '-'.")
    research_check_sweep.add_argument("--outcomes-json", help="Outcomes JSON object path, inline object, or '-'.")
    research_check_sweep.add_argument("--forward-bars")
    research_check_sweep.add_argument("--entry-lag-bars", type=int)
    research_check_sweep.add_argument("--direction", choices=["long", "short"])
    research_check_sweep.add_argument("--min-sample-count", type=int)
    research_check_sweep.add_argument("--min-edge-pct", type=float)
    research_check_sweep.add_argument("--bucket-by", help="Comma-separated bucket fields.")
    research_check_sweep.add_argument("--max-examples", type=int)
    research_check_sweep.add_argument(
        "--variant",
        action="append",
        default=[],
        help="Variant id with optional param overrides, e.g. base or tol04:touch=0.4,window=12.",
    )
    research_check_sweep.add_argument("--variant-json", action="append", default=[], help="Variant JSON object path, inline object, or '-'.")
    research_check_sweep.add_argument("--rank-by", help="Explicit numeric result path used for ranking.")
    research_check_sweep.add_argument("--rank-direction", choices=["asc", "desc"], help="Ranking direction for --rank-by.")
    research_check_sweep.add_argument("--display-metric", action="append", default=[], help="Additional numeric result path to include in the leaderboard.")
    research_check_sweep.add_argument("--format", choices=["json", "table"], default="json")
    research_check_sweep.add_argument("--dispatch", action="store_true", help="Queue the sweep as an async research job and return the job id.")
    research_check_sweep.set_defaults(func=_cmd_research_check_sweep)

    research_check_signal = research_check_sub.add_parser("signal", help="Check completed run signal evidence.")
    add_research_check_base_args(research_check_signal)
    research_check_signal.add_argument("--run-id", required=True)
    research_check_signal.add_argument("--output-name")
    research_check_signal.add_argument("--event-key")
    research_check_signal.add_argument("--symbol")
    research_check_signal.add_argument("--direction")
    research_check_signal.set_defaults(func=_cmd_research_check_signal)

    research_check_decision = research_check_sub.add_parser("decision", help="Check completed run decision/trade evidence.")
    add_research_check_base_args(research_check_decision)
    research_check_decision.add_argument("--run-id", required=True)
    research_check_decision.add_argument("--state")
    research_check_decision.add_argument("--reason-code")
    research_check_decision.add_argument("--symbol")
    research_check_decision.set_defaults(func=_cmd_research_check_decision)

    research_trail = research_sub.add_parser("trail", help="Inspect the reasoning trail around a research item.")
    research_trail.add_argument("item_id")
    research_trail.set_defaults(func=_cmd_research_trail)

    research_run = research_sub.add_parser("run", help="Inspect available research evidence for a completed run.")
    research_run.add_argument("run_id")
    research_run.set_defaults(func=_cmd_research_run)

    research_compare = research_sub.add_parser("compare", help="Compare two persisted research checks.")
    research_compare.add_argument("left_check_id")
    research_compare.add_argument("right_check_id")
    research_compare.set_defaults(func=_cmd_research_compare)

    research_authority = research_sub.add_parser(
        "authority",
        help="Protocol-bound offline scientific search and holdout operations.",
    )
    research_authority_sub = research_authority.add_subparsers(
        dest="research_authority_command", required=True
    )

    def add_authority_post(
        name: str, help_text: str, path: str
    ) -> argparse.ArgumentParser:
        command = research_authority_sub.add_parser(name, help=help_text)
        command.add_argument(
            "--payload-json",
            required=True,
            help="Exact request JSON object as a path, inline object, or '-'.",
        )
        command.set_defaults(
            func=_cmd_research_authority_post, authority_path=path
        )
        return command

    add_authority_post(
        "protocol-create", "Create one immutable scientific protocol.",
        "/api/research/authority/protocols",
    )
    protocol_get = research_authority_sub.add_parser(
        "protocol-get", help="Read a protocol with its holdout binding redacted."
    )
    protocol_get.add_argument("protocol_id")
    protocol_get.set_defaults(func=_cmd_research_authority_protocol_get)
    add_authority_post(
        "family-create", "Open a protocol-bound experiment family.",
        "/api/research/authority/families",
    )
    add_authority_post(
        "attempt-register", "Register a budgeted train or validation attempt.",
        "/api/research/authority/attempts",
    )
    attempt_complete = add_authority_post(
        "attempt-complete", "Persist a terminal attempt outcome.",
        "/api/research/authority/attempts/{attempt_id}/complete",
    )
    attempt_complete.add_argument("attempt_id")
    add_authority_post(
        "candidate-freeze", "Freeze an immutable validation candidate.",
        "/api/research/authority/candidates",
    )
    add_authority_post(
        "strategy-graph-create",
        "Create a typed graph and consume family search budget.",
        "/api/research/authority/strategy-graphs",
    )
    add_authority_post(
        "family-close", "Close search before final holdout access.",
        "/api/research/authority/families/{family_id}/close",
    ).add_argument("family_id")
    add_authority_post(
        "holdout-reserve", "Reserve the family holdout exactly once.",
        "/api/research/authority/holdouts/reserve",
    )
    add_authority_post(
        "family-certify", "Issue scientific evidence after sealed evaluation.",
        "/api/research/authority/families/{family_id}/certify",
    ).add_argument("family_id")
    family_evidence = research_authority_sub.add_parser(
        "family-evidence", help="Read public family lineage and released evidence."
    )
    family_evidence.add_argument("family_id")
    family_evidence.set_defaults(func=_cmd_research_authority_family_evidence)
    add_authority_post(
        "governance-case-create",
        "Open an offline governance case from a persisted observation.",
        "/api/research/governance/cases",
    )
    add_authority_post(
        "transition-propose",
        "Propose one evidence-linked offline state transition.",
        "/api/research/governance/proposals",
    )
    transition_decide = add_authority_post(
        "transition-decide",
        "Authorize or reject a transition as a distinct actor.",
        "/api/research/governance/proposals/{proposal_id}/decide",
    )
    transition_decide.add_argument("proposal_id")
    governance_case_get = research_authority_sub.add_parser(
        "governance-case-get", help="Read the append-only offline governance trail."
    )
    governance_case_get.add_argument("case_id")
    governance_case_get.set_defaults(func=_cmd_research_governance_case_get)

    instruments = subparsers.add_parser("instruments", help="Instrument metadata and runtime profiles.")
    instruments_sub = instruments.add_subparsers(dest="instruments_command", required=True)
    instruments_list = instruments_sub.add_parser("list", help="List canonical instruments.")
    instruments_list.add_argument("--datasource")
    instruments_list.add_argument("--exchange")
    instruments_list.add_argument("--symbol")
    instruments_list.set_defaults(func=_cmd_instruments_list)
    instruments_get = instruments_sub.add_parser("get", help="Fetch one canonical instrument.")
    instruments_get.add_argument("instrument_id")
    instruments_get.set_defaults(func=_cmd_instruments_get)
    instruments_resolve = instruments_sub.add_parser("resolve", help="Validate and persist an instrument through the provider layer.")
    instruments_resolve.add_argument("--symbol", required=True)
    instruments_resolve.add_argument("--datasource")
    instruments_resolve.add_argument("--exchange")
    instruments_resolve.add_argument("--provider")
    instruments_resolve.add_argument("--venue")
    instruments_resolve.add_argument("--force-refresh", action="store_true")
    instruments_resolve.set_defaults(func=_cmd_instruments_resolve)
    instruments_profile = instruments_sub.add_parser("profile", help="Compile an instrument runtime execution profile.")
    instruments_profile.add_argument("instrument_id")
    instruments_profile.add_argument("--execution-semantics", choices=["spot", "derivative", "proxy_derivative"])
    instruments_profile.set_defaults(func=_cmd_instruments_profile)
    instruments_health = instruments_sub.add_parser("health", help="Check stored instrument metadata readiness.")
    instruments_health.add_argument("--datasource")
    instruments_health.add_argument("--exchange")
    instruments_health.set_defaults(func=_cmd_instruments_health)
    instruments_coverage_matrix = instruments_sub.add_parser(
        "coverage-matrix",
        help="Check candle coverage across a filtered instrument set.",
    )
    instruments_coverage_matrix.add_argument("--start", required=True)
    instruments_coverage_matrix.add_argument("--end", required=True)
    instruments_coverage_matrix.add_argument("--timeframe", required=True)
    instruments_coverage_matrix.add_argument("--instrument-id", action="append", default=[])
    instruments_coverage_matrix.add_argument("--symbol")
    instruments_coverage_matrix.add_argument("--datasource")
    instruments_coverage_matrix.add_argument("--exchange")
    instruments_coverage_matrix.add_argument("--instrument-type")
    instruments_coverage_matrix.add_argument("--runtime-ready", choices=["true", "false", "yes", "no", "1", "0"])
    instruments_coverage_matrix.add_argument("--research-ready", choices=["true", "false", "yes", "no", "1", "0"])
    instruments_coverage_matrix.add_argument("--execution-semantics", choices=["spot", "derivative", "proxy_derivative"])
    instruments_coverage_matrix.set_defaults(func=_cmd_instruments_coverage_matrix)

    providers = subparsers.add_parser("providers", help="Provider metadata and stream checks.")
    providers_sub = providers.add_subparsers(dest="providers_command", required=True)
    providers_list = providers_sub.add_parser("list", help="List providers and safe credential metadata.")
    providers_list.set_defaults(func=_cmd_providers_list)
    stream_smoke = providers_sub.add_parser("stream-smoke", help="Run a bounded read-only provider stream smoke check.")
    stream_smoke.add_argument("--provider", default="COINBASE")
    stream_smoke.add_argument("--venue", default="COINBASE_DIRECT")
    stream_smoke.add_argument("--symbol", required=True)
    stream_smoke.add_argument("--product-id", help="Provider product id. Defaults to --symbol.")
    stream_smoke.add_argument("--channel", action="append", default=[], help="Provider channel. Repeat for multiple channels.")
    stream_smoke.add_argument("--timeframe")
    stream_smoke.add_argument("--auth-mode", default="public")
    stream_smoke.add_argument("--duration", type=float, default=10.0, help="Smoke duration in seconds.")
    stream_smoke.add_argument("--sample-limit", type=int, default=10)
    stream_smoke.set_defaults(func=_cmd_providers_stream_smoke)
    credentials = providers_sub.add_parser("credentials", help="Manage encrypted provider credential references.")
    credentials_sub = credentials.add_subparsers(dest="credentials_command", required=True)
    credentials_schema = credentials_sub.add_parser("schema", help="Show accepted credential fields for a provider/venue.")
    credentials_schema.add_argument("--provider", required=True)
    credentials_schema.add_argument("--venue")
    credentials_schema.add_argument("--environment", default="paper")
    credentials_schema.set_defaults(func=_cmd_provider_credentials_schema)
    credentials_add = credentials_sub.add_parser("add", help="Add or rotate a provider credential reference.")
    credentials_add.add_argument("--provider", required=True)
    credentials_add.add_argument("--venue")
    credentials_add.add_argument("--environment", default="paper")
    credentials_add.add_argument("--ref", help="Credential reference. Defaults to provider-venue-environment.")
    credentials_add.add_argument("--display-name")
    credentials_add.add_argument(
        "--secrets-json",
        help="Secret JSON object as a path, inline object, or '-' for stdin. Prefer '-' so secrets do not enter shell history.",
    )
    credentials_add.add_argument(
        "--secret-env",
        action="append",
        default=[],
        help="Map a credential key to an environment variable, e.g. COINBASE_API_KEY=QT_COINBASE_KEY.",
    )
    credentials_add.add_argument("--from-env", action="store_true", help="Read accepted credential keys from matching environment variables.")
    credentials_add.add_argument("--no-input", action="store_true", help="Fail instead of prompting for missing required secrets.")
    credentials_add.set_defaults(func=_cmd_provider_credentials_add)
    credentials_list = credentials_sub.add_parser("list", help="List credential reference metadata. Secret values are never printed.")
    credentials_list.add_argument("--provider")
    credentials_list.add_argument("--venue")
    credentials_list.add_argument("--include-revoked", action="store_true")
    credentials_list.set_defaults(func=_cmd_provider_credentials_list)
    credentials_validate = credentials_sub.add_parser("validate", help="Validate that a credential reference decrypts and has required keys.")
    credentials_validate.add_argument("credential_ref")
    credentials_validate.set_defaults(func=_cmd_provider_credentials_validate)
    credentials_revoke = credentials_sub.add_parser("revoke", help="Revoke a credential reference.")
    credentials_revoke.add_argument("credential_ref")
    credentials_revoke.set_defaults(func=_cmd_provider_credentials_revoke)

    experiments = subparsers.add_parser("experiments", help="Small API-composed research workflows.")
    experiments_sub = experiments.add_subparsers(dest="experiments_command", required=True)
    prepare_matrix = experiments_sub.add_parser(
        "prepare-instrument-matrix",
        help="Prepare solo strategy/bot cases and an experiment plan for instrument comparisons.",
    )
    prepare_matrix.add_argument(
        "--request-json",
        required=True,
        help="instrument_matrix_experiment_request.v1 JSON object as a path, inline object, or '-' for stdin.",
    )
    prepare_matrix.add_argument("--out", help="Plan path to write when --apply is set. Defaults under logs/experiments/plans.")
    prepare_matrix.add_argument("--apply", action="store_true", help="Create strategies/bots and write the experiment plan.")
    prepare_matrix.add_argument("--confirm", action="store_true", help="Required with --apply.")
    prepare_matrix.set_defaults(func=_cmd_experiments_prepare_instrument_matrix)
    validate_plan = experiments_sub.add_parser("validate-plan", help="Validate and preview a sequential experiment plan.")
    validate_plan.add_argument("plan", help="YAML or JSON experiment plan path, or '-' for stdin.")
    validate_plan.add_argument("--skip-data-preflight", action="store_true", help="Skip backend candle coverage checks.")
    validate_plan.set_defaults(func=_cmd_experiments_validate_plan)
    run_plan = experiments_sub.add_parser("run-plan", help="Run a sequential experiment plan with local resumable state.")
    run_plan.add_argument("plan", help="YAML or JSON experiment plan path, or '-' for stdin.")
    run_plan.add_argument("--experiment-id", help="Override the generated experiment id.")
    run_plan.add_argument("--dry-run", action="store_true", help="Validate and print the planned steps without calling backend routes.")
    run_plan.add_argument("--skip-data-preflight", action="store_true", help="Skip backend candle coverage checks.")
    run_plan.add_argument(
        "--proceed-with-data-warnings",
        "--yes",
        action="store_true",
        help="Start runs even when data preflight reports warnings/errors.",
    )
    run_plan.set_defaults(func=_cmd_experiments_run_plan)
    resume = experiments_sub.add_parser("resume", help="Resume a plan-based experiment from local state.")
    resume.add_argument("ref", help="Experiment id, state path, or experiment directory.")
    resume.set_defaults(func=_cmd_experiments_resume)
    start_bot = experiments_sub.add_parser("start-bot", help="Start a bot run and write a resumable experiment record.")
    start_bot.add_argument("bot_id")
    start_bot.add_argument("--dataset-id", required=True, help="Prepared immutable dataset identity.")
    start_bot.add_argument(
        "--profile",
        action="store_true",
        help="Enable opt-in cProfile and process peak-RSS evidence for this backtest run.",
    )
    start_bot.add_argument("--request-id")
    start_bot.add_argument("--baseline-run-id")
    start_bot.add_argument("--export", action="store_true", help="Record export as a default for collect.")
    start_bot.add_argument("--out-dir", help="Report export root. Defaults to --log-root.")
    start_bot.add_argument("--no-json", action="store_true")
    start_bot.add_argument("--no-csv", action="store_true")
    start_bot.add_argument("--include-candles", action="store_true")
    start_bot.set_defaults(func=_cmd_experiments_start_bot)
    status = experiments_sub.add_parser("status", help="Fetch compact status for a tracked experiment or raw run id.")
    status.add_argument("ref", help="Experiment record path, experiment id, request id, or run id.")
    status.add_argument("--bot-id", help="Required when ref is a raw run id with no local experiment record.")
    status.set_defaults(func=_cmd_experiments_status)
    watch = experiments_sub.add_parser("watch", help="Watch a plan-based experiment state file until terminal.")
    watch.add_argument("ref", help="Experiment id, state path, or experiment directory.")
    watch.add_argument("--watch-timeout", type=float, default=3600.0)
    watch.add_argument("--interval", type=float, default=30.0)
    watch.add_argument("--print-each", action="store_true")
    watch.set_defaults(func=_cmd_experiments_watch)
    events = experiments_sub.add_parser("events", help="Read a plan-based experiment events.ndjson log.")
    events.add_argument("ref", help="Experiment id, state path, or experiment directory.")
    events.add_argument("--tail", type=int)
    events.add_argument("--type", help="Filter by event_type.")
    events.add_argument("--status", help="Filter by event status.")
    events.set_defaults(func=_cmd_experiments_events)
    doctor = experiments_sub.add_parser("doctor", help="Check local plan-based experiment state and artifact refs.")
    doctor.add_argument("ref", help="Experiment id, state path, or experiment directory.")
    doctor.set_defaults(func=_cmd_experiments_doctor)
    summarize = experiments_sub.add_parser("summarize", help="Summarize local plan-based experiment artifacts.")
    summarize.add_argument("ref", help="Experiment id, state path, or experiment directory.")
    summarize.add_argument("--out", help="Optional path to write the compact experiment_summary.v1 artifact.")
    summarize.set_defaults(func=_cmd_experiments_summarize)
    collect = experiments_sub.add_parser("collect", help="Collect report export and optional comparison for a tracked experiment.")
    collect.add_argument("ref", help="Experiment record path, experiment id, request id, or run id.")
    collect.add_argument("--bot-id", help="Required when ref is a raw run id with no local experiment record.")
    collect.add_argument("--wait", action="store_true")
    collect.add_argument("--wait-timeout", type=float, default=3600.0)
    collect.add_argument("--interval", type=float, default=30.0)
    collect.add_argument("--print-each", action="store_true")
    collect.add_argument("--allow-non-completed", action="store_true")
    collect.add_argument("--export", action="store_true")
    collect.add_argument("--out-dir", help="Report export root. Defaults to --log-root.")
    collect.add_argument("--no-json", action="store_true")
    collect.add_argument("--no-csv", action="store_true")
    collect.add_argument("--include-candles", action="store_true")
    collect.add_argument("--compare-to", help="Baseline run id to compare against after report materialization.")
    collect.add_argument("--no-golden", action="store_true")
    collect.add_argument("--require-golden", action="store_true")
    collect.set_defaults(func=_cmd_experiments_collect)
    run_bot = experiments_sub.add_parser(
        "run-bot",
        help="Deprecated alias for start-bot followed by collect.",
    )
    run_bot.add_argument("bot_id")
    run_bot.add_argument(
        "--dataset-id", required=True, help="Prepared immutable dataset identity."
    )
    run_bot.add_argument(
        "--profile",
        action="store_true",
        help="Enable opt-in cProfile and process peak-RSS evidence for this backtest run.",
    )
    run_bot.add_argument("--request-id")
    run_bot.add_argument("--baseline-run-id")
    run_bot.add_argument("--wait", action="store_true")
    run_bot.add_argument("--wait-timeout", type=float, default=3600.0)
    run_bot.add_argument("--interval", type=float, default=30.0)
    run_bot.add_argument("--print-each", action="store_true")
    run_bot.add_argument("--allow-non-completed", action="store_true")
    run_bot.add_argument("--export", action="store_true")
    run_bot.add_argument("--out-dir", help="Report export root. Defaults to --log-root.")
    run_bot.add_argument("--no-json", action="store_true")
    run_bot.add_argument("--no-csv", action="store_true")
    run_bot.add_argument("--include-candles", action="store_true")
    run_bot.add_argument("--compare-to", help="Baseline run id to compare against after report materialization.")
    run_bot.add_argument("--no-golden", action="store_true")
    run_bot.add_argument("--require-golden", action="store_true")
    run_bot.set_defaults(func=_cmd_experiments_run_bot)

    mcp = subparsers.add_parser("mcp", help="MCP server entrypoint for agent/tool hosts.")
    mcp_sub = mcp.add_subparsers(dest="mcp_command", required=True)
    mcp_serve = mcp_sub.add_parser("serve", help="Run the Quant-Trad MCP stdio server.")
    mcp_serve.add_argument(
        "--command-timeout",
        type=float,
        default=float(os.environ.get("QT_MCP_COMMAND_TIMEOUT_SECONDS", "7200")),
        help="Timeout for long-running qt command tools.",
    )
    mcp_serve.set_defaults(func=_cmd_mcp_serve)

    return parser


def main(argv: list[str] | None = None) -> int:
    raw_argv = list(argv if argv is not None else sys.argv[1:])
    parser = build_parser()
    args = parser.parse_args(raw_argv)
    audit = CliAuditLog(
        root=getattr(args, "log_root", "logs"),
        args=args,
        argv=raw_argv,
        enabled=not bool(getattr(args, "no_audit_log", False)),
    )
    args._audit_log = audit
    audit.record_event("command_started")
    try:
        exit_code = int(args.func(args))
        audit.finish(exit_code=exit_code)
        return exit_code
    except ValueError as exc:
        error = {"error": str(exc)}
        _print_json(error)
        audit.finish(exit_code=2, error=error)
        return 2
    except ApiError as exc:
        error: dict[str, Any] = {"error": str(exc)}
        if exc.status is not None:
            error["status"] = exc.status
        if exc.body:
            error["body"] = exc.body
        _print_json(error)
        audit.finish(exit_code=1, error=error)
        return 1
    except Exception as exc:
        audit.finish(exit_code=1, error={"error": str(exc), "type": type(exc).__name__})
        raise


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
