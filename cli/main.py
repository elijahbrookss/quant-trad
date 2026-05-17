from __future__ import annotations

import argparse
import json
import os
import sys
import time
from pathlib import Path
from typing import Any

from .api import ApiClient, ApiError, filename_from_content_disposition
from .audit import CliAuditLog, experiment_dir, report_export_dir, safe_path_part
from .experiments.data_preflight import data_preflight_requires_proceed, run_plan_data_preflight
from .experiments.doctor import doctor_experiment
from .experiments.event_log import read_events
from .experiments.plan_loader import load_plan, plan_preview
from .experiments.runner import ExperimentRunner
from .experiments.state_store import ExperimentStateStore, find_experiment_dir


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
        "run_type": "run_type",
        "backtest_start": "backtest_start",
        "backtest_end": "backtest_end",
        "snapshot_interval_ms": "snapshot_interval_ms",
        "instrument_type": "instrument_type",
    }
    for arg_name, payload_name in fields.items():
        value = getattr(args, arg_name, None)
        if value is not None:
            payload[payload_name] = value
    if getattr(args, "wallet_json", None):
        payload["wallet_config"] = _read_json_object_arg(args.wallet_json, label="--wallet-json")
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
    body = {"request_id": args.request_id} if args.request_id else {}
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


def _cmd_strategies_list(args: argparse.Namespace) -> int:
    _print_json(_client(args).request_json("GET", "/api/strategies/"))
    return 0


def _cmd_strategies_get(args: argparse.Namespace) -> int:
    _print_json(_client(args).request_json("GET", f"/api/strategies/{args.strategy_id}"))
    return 0


def _cmd_strategies_compile(args: argparse.Namespace) -> int:
    payload: dict[str, Any] = {}
    if args.variant_id:
        payload["variant_id"] = args.variant_id
    if args.variant_name:
        payload["variant_name"] = args.variant_name
    _print_json(_client(args).request_json("POST", f"/api/strategies/{args.strategy_id}/compile", payload=payload))
    return 0


def _cmd_strategies_preview(args: argparse.Namespace) -> int:
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
    _print_json(_client(args).request_json("POST", f"/api/strategies/{args.strategy_id}/preview", payload=payload))
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
    }
    params: dict[str, Any] = {}
    if args.report_section == "run-report":
        params = {"build": args.build, "force_rebuild": args.force_rebuild}
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
    start_body = {"request_id": args.request_id} if getattr(args, "request_id", None) else {}
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
    last_state: dict[str, Any] | None = None
    while True:
        state = _load_experiment_suite_state(args, args.ref)
        if state is None:
            raise ValueError(f"experiment suite state not found for {args.ref!r}")
        last_state = state
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


def _cmd_experiments_collect(args: argparse.Namespace) -> int:
    record = _load_experiment_record(args, args.ref, bot_id=args.bot_id)
    code, result = _collect_experiment(args, _client(args), record)
    if not args.print_each:
        _print_json(result)
    return code


def _cmd_experiments_run_bot(args: argparse.Namespace) -> int:
    client = _client(args)
    record = _start_experiment(args, client)
    if args.export and not args.wait:
        _print_json({**record, "error": "--export requires --wait so the report is terminal before export"})
        return 2
    if args.wait:
        wait_code, result = _collect_experiment(args, client, record)
        if not args.print_each:
            _print_json(result)
        return wait_code
    if not args.print_each:
        _print_json(record)
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
    bots_create.add_argument("--run-type")
    bots_create.add_argument("--backtest-start")
    bots_create.add_argument("--backtest-end")
    bots_create.add_argument("--snapshot-interval-ms", type=int)
    bots_create.add_argument("--instrument-type")
    bots_create.add_argument("--wallet-json", help="wallet_config JSON object path, inline object, or '-'.")
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
    bots_update.add_argument("--run-type")
    bots_update.add_argument("--backtest-start")
    bots_update.add_argument("--backtest-end")
    bots_update.add_argument("--snapshot-interval-ms", type=int)
    bots_update.add_argument("--instrument-type")
    bots_update.add_argument("--wallet-json", help="wallet_config JSON object path, inline object, or '-'.")
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

    strategies = subparsers.add_parser("strategies", help="Strategy, variant, compile, and preview commands.")
    strategies_sub = strategies.add_subparsers(dest="strategies_command", required=True)
    strategies_list = strategies_sub.add_parser("list", help="List strategies.")
    strategies_list.set_defaults(func=_cmd_strategies_list)
    strategies_get = strategies_sub.add_parser("get", help="Get a strategy detail payload.")
    strategies_get.add_argument("strategy_id")
    strategies_get.set_defaults(func=_cmd_strategies_get)
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
    strategies_preview.set_defaults(func=_cmd_strategies_preview)

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
    ):
        command = reports_sub.add_parser(section, help=f"Fetch report {section}.")
        command.add_argument("run_id")
        if section == "run-report":
            command.add_argument("--no-build", dest="build", action="store_false", default=True)
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

    experiments = subparsers.add_parser("experiments", help="Small API-composed research workflows.")
    experiments_sub = experiments.add_subparsers(dest="experiments_command", required=True)
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
    run_bot = experiments_sub.add_parser("run-bot", help="Start a bot run, optionally wait, and export the report.")
    run_bot.add_argument("bot_id")
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
