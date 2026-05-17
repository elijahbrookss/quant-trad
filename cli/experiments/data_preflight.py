from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from cli.api import ApiClient, ApiError


def _expected_variant_mismatch(payload: Mapping[str, Any], variant: Mapping[str, Any]) -> dict[str, Any] | None:
    strategy = payload.get("strategy") if isinstance(payload.get("strategy"), Mapping) else {}
    expected_id = str(variant.get("expected_strategy_variant_id") or "").strip()
    expected_name = str(variant.get("expected_strategy_variant") or variant.get("expected_strategy_variant_name") or "").strip()
    actual_id = str(strategy.get("strategy_variant_id") or "").strip()
    actual_name = str(strategy.get("strategy_variant_name") or "").strip()
    if expected_id and expected_id != actual_id:
        return {
            "severity": "error",
            "status": "error",
            "message": "Bot selected strategy variant id does not match plan expectation.",
            "expected_strategy_variant_id": expected_id,
            "actual_strategy_variant_id": actual_id or None,
        }
    if expected_name and expected_name != actual_name:
        return {
            "severity": "error",
            "status": "error",
            "message": "Bot selected strategy variant name does not match plan expectation.",
            "expected_strategy_variant": expected_name,
            "actual_strategy_variant": actual_name or None,
        }
    return None


def run_plan_data_preflight(client: ApiClient, plan: Mapping[str, Any]) -> dict[str, Any]:
    checks: list[dict[str, Any]] = []
    route_errors: list[dict[str, Any]] = []
    for window in plan.get("windows") or []:
        window_id = str(window.get("id") or "")
        for variant in plan.get("variants") or []:
            variant_id = str(variant.get("id") or "")
            bot_id = str(variant.get("bot_id") or "")
            target = {
                "window_id": window_id,
                "variant_id": variant_id,
                "bot_id": bot_id,
                "requested_start": window.get("start"),
                "requested_end": window.get("end"),
            }
            try:
                payload = client.request_json(
                    "POST",
                    f"/api/bots/{bot_id}/data-preflight",
                    payload={"start": window.get("start"), "end": window.get("end")},
                )
                if not isinstance(payload, dict):
                    raise ApiError("data preflight returned unexpected payload type")
            except Exception as exc:  # noqa: BLE001 - validate-plan should surface preflight failure without blocking.
                route_errors.append({**target, "severity": "warning", "status": "unavailable", "message": str(exc)})
                continue
            mismatch = _expected_variant_mismatch(payload, variant)
            if mismatch:
                checks.append({**target, **mismatch})
            for check in payload.get("checks") or []:
                if isinstance(check, Mapping):
                    checks.append({**target, **dict(check)})
            if not payload.get("checks"):
                checks.append({**target, "severity": "warning", "status": "warning", "message": "Data preflight returned no instrument checks."})
    all_rows = [*checks, *route_errors]
    severities = {str(item.get("severity") or item.get("status") or "").lower() for item in all_rows}
    status = "error" if "error" in severities else "warning" if ("warning" in severities or "unavailable" in severities) else "ok"
    return {
        "schema_version": "experiment_data_preflight.v1",
        "status": status,
        "checks": checks,
        "route_errors": route_errors,
        "summary": {
            "checks": len(checks),
            "route_errors": len(route_errors),
            "warnings": sum(1 for item in all_rows if str(item.get("severity") or item.get("status") or "").lower() in {"warning", "unavailable"}),
            "errors": sum(1 for item in all_rows if str(item.get("severity") or item.get("status") or "").lower() == "error"),
        },
    }


def data_preflight_requires_proceed(payload: Mapping[str, Any]) -> bool:
    return str(payload.get("status") or "").lower() in {"warning", "error"}

