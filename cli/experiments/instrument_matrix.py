from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

from cli.api import ApiClient, ApiError
from cli.audit import date_partition, safe_path_part, timestamp_slug

from .contracts import json_safe, normalize_plan
from .plan_loader import plan_preview


REQUEST_SCHEMA = "instrument_matrix_experiment_request.v1"
RESULT_SCHEMA = "instrument_matrix_experiment_prepare.v1"


def _as_mapping(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _as_list(value: Any) -> list[Any]:
    return list(value) if isinstance(value, list) else []


def _clean_text(value: Any) -> str | None:
    text = str(value or "").strip()
    return text or None


def _required_text(mapping: Mapping[str, Any], key: str, label: str | None = None) -> str:
    value = _clean_text(mapping.get(key))
    if value is None:
        raise ValueError(f"{label or key} is required")
    return value


def _write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(json_safe(payload), indent=2, sort_keys=True, default=str) + "\n", encoding="utf-8")


def _plan_path(log_root: str | Path, plan: Mapping[str, Any], out_path: str | Path | None) -> Path:
    if out_path:
        return Path(out_path).expanduser()
    name = safe_path_part(str(plan.get("name") or "instrument-matrix"))
    filename = f"{timestamp_slug()}__{name}.json"
    return Path(log_root).expanduser() / "experiments" / "plans" / date_partition() / filename


def _normalize_windows(request: Mapping[str, Any]) -> list[dict[str, Any]]:
    raw_windows = request.get("windows")
    if raw_windows is None and request.get("window") is not None:
        raw_windows = [request.get("window")]
    windows: list[dict[str, Any]] = []
    for raw in _as_list(raw_windows):
        window = _as_mapping(raw)
        windows.append(
            {
                "id": _required_text(window, "id", "window.id"),
                "start": _required_text(window, "start", "window.start"),
                "end": _required_text(window, "end", "window.end"),
                **{key: value for key, value in window.items() if key not in {"id", "start", "end"}},
            }
        )
    if not windows:
        raise ValueError("window or windows is required")
    return windows


def _cases_from_groups(groups: Sequence[Any]) -> list[dict[str, Any]]:
    cases: list[dict[str, Any]] = []
    for raw in groups:
        group = _as_mapping(raw)
        group_id = _required_text(group, "id", "groups[].id")
        label = _clean_text(group.get("label")) or group_id.upper()
        spot_id = _clean_text(group.get("spot_instrument_id") or group.get("spot_proxy_instrument_id"))
        derivative_id = _clean_text(group.get("derivative_instrument_id"))
        if spot_id:
            cases.append(
                {
                    "id": f"{safe_path_part(group_id)}_spot_proxy",
                    "label": f"{label} spot proxy",
                    "instrument_id": spot_id,
                    "role": "spot_proxy",
                    "execution_semantics": "proxy_derivative",
                    "comparison_group": group_id,
                }
            )
        if derivative_id:
            cases.append(
                {
                    "id": f"{safe_path_part(group_id)}_derivative",
                    "label": f"{label} derivative",
                    "instrument_id": derivative_id,
                    "role": "derivative",
                    "execution_semantics": "derivative",
                    "comparison_group": group_id,
                }
            )
    return cases


def _normalize_cases(request: Mapping[str, Any]) -> list[dict[str, Any]]:
    raw_cases = _as_list(request.get("cases"))
    cases = [_as_mapping(item) for item in raw_cases]
    if not cases and request.get("groups") is not None:
        cases = _cases_from_groups(_as_list(request.get("groups")))
    normalized: list[dict[str, Any]] = []
    seen: set[str] = set()
    for case in cases:
        case_id = _required_text(case, "id", "cases[].id")
        if case_id in seen:
            raise ValueError(f"duplicate case id: {case_id}")
        seen.add(case_id)
        execution_semantics = _required_text(case, "execution_semantics", "cases[].execution_semantics")
        if execution_semantics not in {"spot", "derivative", "proxy_derivative"}:
            raise ValueError(f"unsupported execution_semantics for case {case_id}: {execution_semantics}")
        normalized.append(
            {
                "id": case_id,
                "label": _clean_text(case.get("label")) or case_id,
                "instrument_id": _clean_text(case.get("instrument_id")),
                "symbol": _clean_text(case.get("symbol")),
                "datasource": _clean_text(case.get("datasource")),
                "exchange": _clean_text(case.get("exchange")),
                "role": _clean_text(case.get("role")) or execution_semantics,
                "execution_semantics": execution_semantics,
                "comparison_group": _clean_text(case.get("comparison_group")),
                **{
                    key: value
                    for key, value in case.items()
                    if key
                    not in {
                        "id",
                        "label",
                        "instrument_id",
                        "symbol",
                        "datasource",
                        "exchange",
                        "role",
                        "execution_semantics",
                        "comparison_group",
                    }
                },
            }
        )
    if not normalized:
        raise ValueError("cases or groups is required")
    return normalized


def _instrument_matches(record: Mapping[str, Any], case: Mapping[str, Any]) -> bool:
    symbol = _clean_text(case.get("symbol"))
    if symbol and str(record.get("symbol") or "").strip().upper() != symbol.upper():
        return False
    datasource = _clean_text(case.get("datasource"))
    if datasource and str(record.get("datasource") or "").strip().lower() != datasource.lower():
        return False
    exchange = _clean_text(case.get("exchange"))
    if exchange and str(record.get("exchange") or "").strip().lower() != exchange.lower():
        return False
    return True


def _resolve_instrument(client: ApiClient, case: Mapping[str, Any]) -> dict[str, Any]:
    instrument_id = _clean_text(case.get("instrument_id"))
    if instrument_id:
        payload = client.request_json("GET", f"/api/instruments/{instrument_id}")
        if not isinstance(payload, Mapping):
            raise ApiError(f"GET instrument {instrument_id} returned an unexpected payload")
        return dict(payload)

    if not _clean_text(case.get("symbol")):
        raise ValueError(f"case {case.get('id')} requires instrument_id or symbol")
    payload = client.request_json("GET", "/api/instruments/")
    matches = [dict(row) for row in _as_list(payload) if isinstance(row, Mapping) and _instrument_matches(row, case)]
    if not matches:
        raise ValueError(f"case {case.get('id')} did not match any stored instrument")
    if len(matches) > 1:
        ids = ", ".join(str(row.get("id")) for row in matches)
        raise ValueError(f"case {case.get('id')} matched multiple instruments: {ids}")
    return matches[0]


def _instrument_type(record: Mapping[str, Any]) -> str:
    return str(record.get("instrument_type") or record.get("type") or "").strip().lower()


def _validate_case_profile(client: ApiClient, case: Mapping[str, Any], instrument: Mapping[str, Any]) -> dict[str, Any]:
    instrument_id = _required_text(instrument, "id", "instrument.id")
    execution_semantics = _required_text(case, "execution_semantics")
    if execution_semantics == "proxy_derivative" and _instrument_type(instrument) != "spot":
        raise ValueError(
            f"case {case.get('id')} uses proxy_derivative but instrument {instrument_id} is not spot"
        )
    payload = client.request_json(
        "GET",
        f"/api/instruments/{instrument_id}/runtime-profile",
        params={"execution_semantics": execution_semantics},
    )
    if not isinstance(payload, Mapping):
        raise ApiError(f"GET runtime profile for instrument {instrument_id} returned an unexpected payload")
    return dict(payload)


def _strategy_core(strategy_detail: Mapping[str, Any]) -> dict[str, Any]:
    core = _as_mapping(strategy_detail.get("strategy"))
    if not core and any(key in strategy_detail for key in ("id", "name", "timeframe")):
        core = dict(strategy_detail)
    return core


def _strategy_bindings(strategy_detail: Mapping[str, Any]) -> dict[str, Any]:
    return _as_mapping(strategy_detail.get("bindings"))


def _strategy_rules(strategy_detail: Mapping[str, Any]) -> list[dict[str, Any]]:
    decision = _as_mapping(strategy_detail.get("decision"))
    return [dict(rule) for rule in _as_list(decision.get("rules")) if isinstance(rule, Mapping)]


def _strategy_variants(strategy_detail: Mapping[str, Any]) -> list[dict[str, Any]]:
    return [dict(variant) for variant in _as_list(strategy_detail.get("variants")) if isinstance(variant, Mapping)]


def _selected_variant_name(source_bot: Mapping[str, Any], source_context: Mapping[str, Any], source_variants: Sequence[Mapping[str, Any]]) -> str | None:
    context_strategy = _as_mapping(source_context.get("strategy"))
    selected = (
        _clean_text(source_bot.get("strategy_variant_name"))
        or _clean_text(context_strategy.get("strategy_variant_name"))
    )
    if selected:
        return selected
    selected_id = _clean_text(source_bot.get("strategy_variant_id") or context_strategy.get("strategy_variant_id"))
    if selected_id:
        for variant in source_variants:
            if selected_id == _clean_text(variant.get("id")):
                return _clean_text(variant.get("name"))
    for variant in source_variants:
        if bool(variant.get("is_default")):
            return _clean_text(variant.get("name"))
    return None


def _single_instrument_slot(instrument: Mapping[str, Any], case: Mapping[str, Any]) -> dict[str, Any]:
    symbol = _required_text(instrument, "symbol", "instrument.symbol")
    datasource = _clean_text(instrument.get("datasource"))
    exchange = _clean_text(instrument.get("exchange"))
    instrument_id = _required_text(instrument, "id", "instrument.id")
    metadata = {
        "instrument_id": instrument_id,
        "datasource": datasource,
        "exchange": exchange,
        "execution_semantics": case.get("execution_semantics"),
        "research_market_role": case.get("role"),
        "instrument_matrix_case_id": case.get("id"),
    }
    return {
        "symbol": symbol,
        "instrument_id": instrument_id,
        "datasource": datasource,
        "exchange": exchange,
        "metadata": {key: value for key, value in metadata.items() if value is not None},
    }


def _strategy_create_payload(
    *,
    source_strategy: Mapping[str, Any],
    instrument: Mapping[str, Any],
    case: Mapping[str, Any],
    name_prefix: str,
) -> dict[str, Any]:
    core = _strategy_core(source_strategy)
    bindings = _strategy_bindings(source_strategy)
    datasource = _clean_text(instrument.get("datasource")) or _clean_text(core.get("datasource"))
    exchange = _clean_text(instrument.get("exchange")) or _clean_text(core.get("exchange"))
    return {
        "name": f"{name_prefix}{case.get('label')}",
        "description": f"Solo instrument matrix clone of {core.get('name') or core.get('id')} for {case.get('label')}.",
        "timeframe": _required_text(core, "timeframe"),
        "datasource": datasource,
        "exchange": exchange,
        "instrument_slots": [_single_instrument_slot(instrument, case)],
        "indicator_ids": list(bindings.get("indicator_ids") or []),
        "atm_template_id": core.get("atm_template_id"),
        "risk_config": dict(core.get("risk_config") or {}),
    }


def _rule_create_payload(rule: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "name": _clean_text(rule.get("name")) or _clean_text(rule.get("id")) or "rule",
        "intent": _clean_text(rule.get("intent")) or "enter_long",
        "priority": int(rule.get("priority") or 0),
        "trigger": dict(rule.get("trigger") or {}),
        "guards": [dict(item) for item in _as_list(rule.get("guards")) if isinstance(item, Mapping)],
        "description": rule.get("description"),
        "enabled": bool(rule.get("enabled", True)),
    }


def _copy_strategy_variants(
    client: ApiClient,
    *,
    new_strategy_id: str,
    source_variants: Sequence[Mapping[str, Any]],
    create_response: Mapping[str, Any],
) -> dict[str, str]:
    variant_id_by_source_id: dict[str, str] = {}
    created_variants = _strategy_variants(create_response)
    default_target = next((variant for variant in created_variants if bool(variant.get("is_default"))), None)
    for source_variant in source_variants:
        payload = {
            "name": _clean_text(source_variant.get("name")) or "default",
            "description": source_variant.get("description"),
            "output_filters": list(source_variant.get("output_filters") or []),
            "is_default": bool(source_variant.get("is_default", False)),
        }
        if payload["is_default"] and default_target and _clean_text(default_target.get("id")):
            target_id = _required_text(default_target, "id", "target variant id")
            updated = client.request_json(
                "PUT",
                f"/api/strategies/{new_strategy_id}/variants/{target_id}",
                payload=payload,
            )
            if isinstance(updated, Mapping):
                variant_id_by_source_id[_required_text(source_variant, "id", "source variant id")] = _clean_text(updated.get("id")) or target_id
            continue
        created = client.request_json(
            "POST",
            f"/api/strategies/{new_strategy_id}/variants",
            payload=payload,
        )
        if isinstance(created, Mapping):
            source_id = _clean_text(source_variant.get("id"))
            created_id = _clean_text(created.get("id"))
            if source_id and created_id:
                variant_id_by_source_id[source_id] = created_id
    return variant_id_by_source_id


def _bot_create_payload(
    *,
    source_bot: Mapping[str, Any],
    new_strategy_id: str,
    selected_variant_name: str | None,
    instrument: Mapping[str, Any],
    case: Mapping[str, Any],
    first_window: Mapping[str, Any],
    name_prefix: str,
) -> dict[str, Any]:
    return {
        "name": f"{name_prefix}{case.get('label')}",
        "strategy_id": new_strategy_id,
        "strategy_variant_name": selected_variant_name,
        "atm_template_id": source_bot.get("atm_template_id"),
        "risk_config": dict(source_bot.get("risk_config") or {}),
        "datasource": _clean_text(instrument.get("datasource")),
        "exchange": _clean_text(instrument.get("exchange")),
        "mode": source_bot.get("mode") or "instant",
        "execution_mode": source_bot.get("execution_mode") or "fast",
        "execution_behavior": source_bot.get("execution_behavior") or "simulated",
        "run_type": "backtest",
        "backtest_start": first_window.get("start"),
        "backtest_end": first_window.get("end"),
        "wallet_config": dict(source_bot.get("wallet_config") or {}),
        "market_data_stream_policy": dict(source_bot.get("market_data_stream_policy") or {}),
        "snapshot_interval_ms": int(source_bot.get("snapshot_interval_ms") or 1000),
        "bot_env": dict(source_bot.get("bot_env") or {}),
        "execution_semantics": case.get("execution_semantics"),
    }


def _planned_case(
    *,
    case: Mapping[str, Any],
    instrument: Mapping[str, Any],
    profile: Mapping[str, Any],
    strategy_payload: Mapping[str, Any],
    bot_payload: Mapping[str, Any],
    strategy_id: str | None = None,
    bot_id: str | None = None,
) -> dict[str, Any]:
    profile_view = _as_mapping(profile.get("profile")) or dict(profile)
    profile_instrument = _as_mapping(profile_view.get("instrument"))
    return {
        "case_id": case.get("id"),
        "label": case.get("label"),
        "comparison_group": case.get("comparison_group"),
        "role": case.get("role"),
        "execution_semantics": case.get("execution_semantics"),
        "instrument": {
            "id": instrument.get("id"),
            "symbol": instrument.get("symbol"),
            "datasource": instrument.get("datasource"),
            "exchange": instrument.get("exchange"),
            "instrument_type": instrument.get("instrument_type"),
        },
        "runtime_profile": {
            "schema_version": profile.get("schema_version"),
            "execution_semantics": profile_view.get("execution_semantics")
            or profile_instrument.get("execution_semantics"),
            "instrument_type": profile_view.get("instrument_type")
            or profile_instrument.get("instrument_type"),
            "source_instrument_type": profile_view.get("source_instrument_type")
            or profile_instrument.get("source_instrument_type"),
        },
        "strategy_payload": dict(strategy_payload),
        "bot_payload": dict(bot_payload),
        "strategy_id": strategy_id,
        "bot_id": bot_id,
    }


def _comparison_plan(request: Mapping[str, Any], cases: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    explicit = _as_list(request.get("comparisons"))
    if explicit:
        comparisons: list[dict[str, Any]] = []
        for raw in explicit:
            item = _as_mapping(raw)
            baseline = _required_text(item, "baseline_case_id", "comparisons[].baseline_case_id")
            candidate = _required_text(item, "candidate_case_id", "comparisons[].candidate_case_id")
            comparisons.append(
                {
                    "id": _clean_text(item.get("id")) or f"{baseline}_vs_{candidate}",
                    "baseline_variant_id": baseline,
                    "candidate_variant_id": candidate,
                    "compare_per_window": bool(item.get("compare_per_window", True)),
                    "aggregate_summary": bool(item.get("aggregate_summary", True)),
                }
            )
        return comparisons

    by_group: dict[str, dict[str, str]] = {}
    for case in cases:
        group = _clean_text(case.get("comparison_group"))
        role = _clean_text(case.get("role"))
        case_id = _clean_text(case.get("id"))
        if group and role and case_id:
            by_group.setdefault(group, {})[role] = case_id
    comparisons = []
    for group, roles in sorted(by_group.items()):
        baseline = roles.get("derivative")
        candidate = roles.get("spot_proxy") or roles.get("proxy_derivative")
        if baseline and candidate:
            comparisons.append(
                {
                    "id": f"{safe_path_part(group)}_spot_proxy_vs_derivative",
                    "baseline_variant_id": baseline,
                    "candidate_variant_id": candidate,
                    "compare_per_window": True,
                    "aggregate_summary": True,
                }
            )
    return comparisons


def _experiment_plan(
    request: Mapping[str, Any],
    *,
    windows: Sequence[Mapping[str, Any]],
    cases: Sequence[Mapping[str, Any]],
    bot_ids_by_case: Mapping[str, str],
    source_bot_id: str,
    source_strategy_id: str,
) -> dict[str, Any]:
    variants = []
    for case in cases:
        case_id = _required_text(case, "id", "case id")
        bot_id = bot_ids_by_case.get(case_id) or f"planned:{case_id}"
        variants.append(
            {
                "id": case_id,
                "bot_id": bot_id,
                "label": case.get("label"),
                "role": case.get("role"),
                "comparison_group": case.get("comparison_group"),
                "execution_semantics": case.get("execution_semantics"),
                "expected_strategy_variant": request.get("selected_strategy_variant_name"),
            }
        )
    plan = {
        "schema_version": "experiment_plan.v1",
        "name": _required_text(request, "name"),
        "hypothesis": request.get("hypothesis")
        or "Compare solo spot proxy and derivative instruments through bot runtime/report truth.",
        "windows": [dict(window) for window in windows],
        "variants": variants,
        "comparisons": _comparison_plan(request, cases),
        "run_policy": {
            "mode": "sequential",
            "stop_on_first_failure": False,
            "poll_interval_seconds": 30,
            "run_timeout_seconds": 7200,
            "update_bot_window": True,
            **dict(request.get("run_policy") or {}),
        },
        "export_policy": {
            "enabled": True,
            "include_json": True,
            "include_csv": True,
            "include_candles": False,
            **dict(request.get("export_policy") or {}),
        },
        "materialization_policy": {
            "build": True,
            "require_ready": True,
            "force_rebuild": True,
            **dict(request.get("materialization_policy") or {}),
        },
        "comparison_policy": {
            "include_golden": False,
            "require_golden": False,
            **dict(request.get("comparison_policy") or {}),
        },
        "pass_gates": dict(request.get("pass_gates") or {}),
        "notification_policy": dict(request.get("notification_policy") or {}),
        "metadata": {
            **dict(request.get("metadata") or {}),
            "instrument_matrix": {
                "schema_version": REQUEST_SCHEMA,
                "source_bot_id": source_bot_id,
                "source_strategy_id": source_strategy_id,
                "cases": [
                    {
                        "case_id": case.get("id"),
                        "instrument_id": case.get("instrument_id"),
                        "role": case.get("role"),
                        "comparison_group": case.get("comparison_group"),
                        "execution_semantics": case.get("execution_semantics"),
                    }
                    for case in cases
                ],
            },
        },
    }
    return normalize_plan(plan)


def prepare_instrument_matrix_experiment(
    *,
    client: ApiClient,
    request: Mapping[str, Any],
    log_root: str | Path,
    out_path: str | Path | None = None,
    apply: bool = False,
    confirm: bool = False,
) -> dict[str, Any]:
    schema_version = str(request.get("schema_version") or REQUEST_SCHEMA)
    if schema_version != REQUEST_SCHEMA:
        raise ValueError(f"unsupported instrument matrix schema_version: {schema_version}")
    if apply and not confirm:
        raise ValueError("prepare-instrument-matrix requires --confirm when --apply is set")

    windows = _normalize_windows(request)
    first_window = windows[0]
    cases = _normalize_cases(request)
    source_bot_id = _required_text(request, "source_bot_id")

    source_bot_raw = client.request_json("GET", f"/api/bots/{source_bot_id}")
    if not isinstance(source_bot_raw, Mapping):
        raise ApiError(f"GET bot {source_bot_id} returned an unexpected payload")
    source_bot = dict(source_bot_raw)
    source_context_raw = client.request_json("GET", f"/api/bots/{source_bot_id}/run-context")
    source_context = _as_mapping(source_context_raw)
    source_strategy_id = (
        _clean_text(request.get("source_strategy_id"))
        or _clean_text(source_bot.get("strategy_id"))
        or _clean_text(_as_mapping(source_context.get("strategy")).get("strategy_id"))
    )
    if not source_strategy_id:
        raise ValueError("source strategy id could not be resolved from request or source bot")
    source_strategy_raw = client.request_json("GET", f"/api/strategies/{source_strategy_id}")
    if not isinstance(source_strategy_raw, Mapping):
        raise ApiError(f"GET strategy {source_strategy_id} returned an unexpected payload")
    source_strategy = dict(source_strategy_raw)
    source_variants = _strategy_variants(source_strategy)
    selected_variant_name = _selected_variant_name(source_bot, source_context, source_variants)

    request_with_selected = {**dict(request), "selected_strategy_variant_name": selected_variant_name}
    strategy_prefix = _clean_text(request.get("strategy_name_prefix")) or f"{request.get('name')} / "
    bot_prefix = _clean_text(request.get("bot_name_prefix")) or f"{request.get('name')} / "
    rules = _strategy_rules(source_strategy)

    planned_cases: list[dict[str, Any]] = []
    bot_ids_by_case: dict[str, str] = {}
    created_strategy_ids: dict[str, str] = {}
    for case in cases:
        instrument = _resolve_instrument(client, case)
        case["instrument_id"] = _required_text(instrument, "id", "instrument.id")
        profile = _validate_case_profile(client, case, instrument)
        strategy_payload = _strategy_create_payload(
            source_strategy=source_strategy,
            instrument=instrument,
            case=case,
            name_prefix=strategy_prefix,
        )
        bot_payload = _bot_create_payload(
            source_bot=source_bot,
            new_strategy_id=f"planned:{case.get('id')}",
            selected_variant_name=selected_variant_name,
            instrument=instrument,
            case=case,
            first_window=first_window,
            name_prefix=bot_prefix,
        )
        if apply:
            created_strategy = client.request_json("POST", "/api/strategies/", payload=strategy_payload)
            if not isinstance(created_strategy, Mapping):
                raise ApiError("POST /api/strategies/ returned an unexpected payload")
            new_strategy_id = _required_text(_strategy_core(created_strategy), "id")
            created_strategy_ids[_required_text(case, "id", "case id")] = new_strategy_id
            for rule in rules:
                client.request_json("POST", f"/api/strategies/{new_strategy_id}/rules", payload=_rule_create_payload(rule))
            _copy_strategy_variants(
                client,
                new_strategy_id=new_strategy_id,
                source_variants=source_variants,
                create_response=created_strategy,
            )
            bot_payload = {**bot_payload, "strategy_id": new_strategy_id}
            created_bot = client.request_json("POST", "/api/bots", payload=bot_payload)
            if not isinstance(created_bot, Mapping):
                raise ApiError("POST /api/bots returned an unexpected payload")
            new_bot_id = _required_text(created_bot, "id", "created bot id")
            bot_ids_by_case[_required_text(case, "id", "case id")] = new_bot_id
            planned_cases.append(
                _planned_case(
                    case=case,
                    instrument=instrument,
                    profile=profile,
                    strategy_payload=strategy_payload,
                    bot_payload=bot_payload,
                    strategy_id=new_strategy_id,
                    bot_id=new_bot_id,
                )
            )
        else:
            planned_cases.append(
                _planned_case(
                    case=case,
                    instrument=instrument,
                    profile=profile,
                    strategy_payload=strategy_payload,
                    bot_payload=bot_payload,
                )
            )

    plan = _experiment_plan(
        request_with_selected,
        windows=windows,
        cases=cases,
        bot_ids_by_case=bot_ids_by_case,
        source_bot_id=source_bot_id,
        source_strategy_id=source_strategy_id,
    )
    result: dict[str, Any] = {
        "schema_version": RESULT_SCHEMA,
        "apply": bool(apply),
        "source_bot_id": source_bot_id,
        "source_strategy_id": source_strategy_id,
        "selected_strategy_variant_name": selected_variant_name,
        "cases": planned_cases,
        "plan": plan,
        "preview": plan_preview(plan),
    }
    if apply:
        path = _plan_path(log_root, plan, out_path)
        _write_json(path, plan)
        result["plan_path"] = str(path)
        result["created"] = {
            "strategy_ids_by_case": created_strategy_ids,
            "bot_ids_by_case": bot_ids_by_case,
        }
    return result
