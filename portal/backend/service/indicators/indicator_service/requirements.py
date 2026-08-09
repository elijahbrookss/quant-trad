"""Indicator-owned transitive runtime requirement planning."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Mapping, Sequence

from data_providers.utils.ohlcv import interval_to_timedelta
from indicators.manifest import serialize_indicator_manifest
from indicators.registry import get_indicator_definition, get_indicator_manifest
from market_data.fact_registry import get_fact_contract
from market_data.frozen import semantic_hash

from .context import IndicatorServiceContext, _context
from .runtime_graph import collect_runtime_indicator_metas


INDICATOR_REQUIREMENT_PLAN_VERSION = "indicator_requirement_plan.v1"


def _iso_utc(value: Any, *, field: str) -> str:
    raw = str(value or "").strip()
    if not raw:
        raise ValueError(f"indicator_requirement_plan_invalid: {field} is required")
    if raw.endswith("Z"):
        raw = f"{raw[:-1]}+00:00"
    try:
        parsed = datetime.fromisoformat(raw)
    except ValueError as exc:
        raise ValueError(
            f"indicator_requirement_plan_invalid: {field} must be ISO-8601"
        ) from exc
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _seconds(timeframe: str) -> int:
    seconds = int(interval_to_timedelta(str(timeframe or "").strip()).total_seconds())
    if seconds <= 0:
        raise ValueError("indicator_requirement_plan_invalid: timeframe must be positive")
    return seconds


def plan_runtime_requirements_for_indicators(
    indicator_ids: Sequence[str],
    *,
    timeframe: str,
    start: str,
    end: str,
    param_overrides_by_id: Mapping[str, Mapping[str, Any]] | None = None,
    preloaded_metas: Mapping[str, Mapping[str, Any]] | None = None,
    ctx: IndicatorServiceContext = _context,
) -> dict[str, Any]:
    """Resolve exact direct/transitive manifests and their typed market inputs."""

    roots = tuple(
        dict.fromkeys(str(value or "").strip() for value in indicator_ids if str(value or "").strip())
    )
    if not roots:
        return {
            "schema_version": INDICATOR_REQUIREMENT_PLAN_VERSION,
            "root_indicator_ids": [],
            "indicators": [],
            "requirements": [],
            "warmup_bars": 0,
            "graph_hash": semantic_hash(
                {
                    "schema_version": INDICATOR_REQUIREMENT_PLAN_VERSION,
                    "root_indicator_ids": [],
                    "indicators": [],
                }
            ),
        }
    normalized_start = _iso_utc(start, field="start")
    normalized_end = _iso_utc(end, field="end")
    if normalized_end <= normalized_start:
        raise ValueError("indicator_requirement_plan_invalid: end must be after start")
    interval_seconds = _seconds(timeframe)
    overrides = dict(param_overrides_by_id or {})
    if preloaded_metas is None:
        metas = collect_runtime_indicator_metas(roots, ctx=ctx)
    else:
        metas = collect_runtime_indicator_metas(
            roots,
            ctx=ctx,
            preloaded_metas=preloaded_metas,
            require_preloaded_metas=True,
        )
    indicator_rows: list[dict[str, Any]] = []
    requirements: list[dict[str, Any]] = []
    warmup_bars = 0

    for indicator_id, raw_meta in metas.items():
        meta = dict(raw_meta)
        indicator_type = str(meta.get("type") or "").strip()
        definition = get_indicator_definition(indicator_type)
        params = dict(meta.get("params") or {})
        if indicator_id in overrides:
            params.update(dict(overrides[indicator_id] or {}))
        params = definition.resolve_config(params, strict_unknown=True)
        meta["params"] = params
        manifest = get_indicator_manifest(indicator_type)
        runtime_plan = ctx.factory.build_runtime_input_plan(
            meta,
            strategy_interval=timeframe,
            start=normalized_start,
            end=normalized_end,
        )
        source_timeframe = str(
            runtime_plan.get("source_timeframe") or timeframe
        ).strip()
        source_seconds = _seconds(source_timeframe)
        required_bars = int(params.get("warmup_bars") or 0)
        warmup_bars = max(warmup_bars, required_bars)
        manifest_payload = serialize_indicator_manifest(manifest)
        indicator_material = {
            "indicator_id": indicator_id,
            "indicator_type": indicator_type,
            "attachment_role": "root" if indicator_id in roots else "dependency",
            "params": params,
            "dependencies": list(meta.get("dependencies") or []),
            "manifest": manifest_payload,
            "runtime_input_plan": dict(runtime_plan),
        }
        indicator_rows.append(
            {
                **indicator_material,
                "configuration_hash": semantic_hash(indicator_material),
            }
        )
        for market_input in manifest.market_inputs:
            contract = get_fact_contract(market_input.fact_type)
            requirement = market_input.to_requirement(
                timeframe_seconds=(
                    source_seconds if contract.timeframe_mode != "forbidden" else None
                ),
                lookback_bars=(
                    int(runtime_plan["lookback_bars"])
                    if runtime_plan.get("lookback_bars") not in (None, "")
                    else None
                ),
                lookback_seconds=(
                    int(runtime_plan["lookback_days"]) * 86400
                    if runtime_plan.get("lookback_days") not in (None, "")
                    else None
                ),
            )
            requirements.append(
                {
                    "consumer_id": indicator_id,
                    "source_timeframe": source_timeframe,
                    "required_start": _iso_utc(
                        runtime_plan.get("start") or normalized_start,
                        field=f"indicator[{indicator_id}].required_start",
                    ),
                    "input": requirement.to_dict(),
                }
            )

    indicator_rows.sort(key=lambda row: str(row["indicator_id"]))
    requirements.sort(
        key=lambda row: (
            str(row["consumer_id"]),
            str(row["input"].get("key") or ""),
        )
    )
    graph_material = {
        "schema_version": INDICATOR_REQUIREMENT_PLAN_VERSION,
        "root_indicator_ids": list(roots),
        "timeframe": timeframe,
        "timeframe_seconds": interval_seconds,
        "indicators": indicator_rows,
    }
    return {
        **graph_material,
        "requirements": requirements,
        "warmup_bars": warmup_bars,
        "graph_hash": semantic_hash(graph_material),
    }


__all__ = [
    "INDICATOR_REQUIREMENT_PLAN_VERSION",
    "plan_runtime_requirements_for_indicators",
]
