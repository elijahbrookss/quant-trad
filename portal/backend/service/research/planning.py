"""Requirement planning for preview and durable Check execution."""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from datetime import datetime, timedelta, timezone
from typing import Any

from data_providers.utils.ohlcv import interval_to_timedelta
from market_data.contracts import (
    CANDLE_FACT_TYPE,
    CANDLE_FACT_VERSION,
    MarketDataRequirement,
)
from market_data.fact_registry import get_fact_contract
from market_data.requirements import (
    InstrumentResolutionContext,
    MarketDataPlanResolver,
)
from market_data.store import MarketDataStore
from research_science.check import (
    CHECK_MODE_EVIDENCE,
    CHECK_PLAN_SCHEMA_VERSION,
    CheckDefinition,
    CheckRequest,
    ResolvedCheckPlan,
)

from portal.backend.service.indicators.indicator_service import (
    plan_runtime_requirements_for_indicators,
)
from portal.backend.service.market import instrument_service
from portal.backend.service.storage.repos.market_data import market_data_repo

from . import checks
from .registry import EVENT_FACT_ANALYSIS


_MARKET_CHECK_FAMILIES = frozenset(
    {
        checks.RAW_FORWARD_OUTCOME,
        checks.INDICATOR_FORWARD_OUTCOME,
        checks.SIGNAL_AUDIT,
        checks.CANDIDATE_LIFECYCLE,
        EVENT_FACT_ANALYSIS,
    }
)
_INDICATOR_CHECK_FAMILIES = frozenset(
    {
        checks.INDICATOR_FORWARD_OUTCOME,
        checks.SIGNAL_AUDIT,
        checks.CANDIDATE_LIFECYCLE,
        EVENT_FACT_ANALYSIS,
    }
)


def _utc(value: Any, *, field: str) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    else:
        raw = str(value or "").strip()
        if not raw:
            raise ValueError(f"check_requirement_plan_invalid: {field} is required")
        if raw.endswith("Z"):
            raw = f"{raw[:-1]}+00:00"
        try:
            parsed = datetime.fromisoformat(raw)
        except ValueError as exc:
            raise ValueError(
                f"check_requirement_plan_invalid: {field} must be ISO-8601"
            ) from exc
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _iso(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat(timespec="microseconds").replace(
        "+00:00", "Z"
    )


def _timeframe_seconds(value: Any) -> int:
    seconds = int(interval_to_timedelta(str(value or "").strip()).total_seconds())
    if seconds <= 0:
        raise ValueError("check_requirement_plan_invalid: timeframe must be positive")
    return seconds


def _positive_ints(value: Any) -> list[int]:
    if value in (None, ""):
        return []
    if isinstance(value, (int, str)):
        value = [value]
    if not isinstance(value, Sequence):
        raise ValueError("check outcome horizons must be a list")
    result = sorted({int(item) for item in value})
    if any(item <= 0 for item in result):
        raise ValueError("check outcome horizons must be positive")
    return result


def _scope_instrument(scope: Mapping[str, Any]) -> tuple[str, dict[str, Any]]:
    instrument_id = str(scope.get("instrument_id") or "").strip()
    if not instrument_id:
        symbol = str(scope.get("symbol") or "").strip()
        if not symbol:
            raise ValueError(
                "check_requirement_plan_invalid: scope.instrument_id or scope.symbol is required"
            )
        instrument_id = instrument_service.require_instrument_id(
            scope.get("datasource"), scope.get("exchange"), symbol
        )
    return instrument_id, dict(instrument_service.get_instrument_record(instrument_id))


def _requirement_from_payload(raw: Mapping[str, Any]) -> MarketDataRequirement:
    values = dict(raw)
    if "alias" in values:
        values["key"] = values.pop("alias")
    for extra in (
        "instrument_id",
        "source_policy",
        "series_required",
        "frame_missing_policy",
        "required_start",
        "required_end",
        "consumer_id",
        "source_timeframe",
    ):
        values.pop(extra, None)
    values["required_fields"] = tuple(values.get("required_fields") or ())
    return MarketDataRequirement(**values)


def _coverage_for_requirement(
    requirement: Mapping[str, Any],
    *,
    store: MarketDataStore,
    as_of_commit_seq: int,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    candidates = []
    dimensions = get_fact_contract(str(requirement["fact_type"])).normalize_dimensions(
        requirement.get("dimensions")
    )
    for row in store.list_series(instrument_id=str(requirement["instrument_id"])):
        if (
            str(row.get("fact_type") or "").strip().lower()
            == str(requirement["fact_type"])
            and str(row.get("contract_version") or "").strip()
            == str(requirement["contract_version"])
            and (
                int(row["timeframe_seconds"])
                if row.get("timeframe_seconds") is not None
                else None
            )
            == requirement.get("timeframe_seconds")
            and dict(row.get("dimensions") or {}) == dimensions
        ):
            candidates.append(dict(row))
    missing: list[dict[str, Any]] = []
    quality: list[dict[str, Any]] = []
    if not candidates:
        missing.append(
            {
                "alias": requirement["alias"],
                "reason": "series_not_registered",
                "instrument_id": requirement["instrument_id"],
                "fact_type": requirement["fact_type"],
            }
        )
        return missing, quality
    if len(candidates) > 1:
        missing.append(
            {
                "alias": requirement["alias"],
                "reason": "series_resolution_ambiguous",
                "series_ids": sorted(int(row["series_id"]) for row in candidates),
            }
        )
        return missing, quality
    candidate = candidates[0]
    records = store.read_series_records(
        series_id=int(candidate["series_id"]),
        start=_utc(requirement["required_start"], field="required_start"),
        end=_utc(requirement["required_end"], field="required_end"),
        as_of_commit_seq=as_of_commit_seq,
    )
    if not records:
        missing.append(
            {
                "alias": requirement["alias"],
                "reason": "no_facts_in_required_range",
                "series_id": int(candidate["series_id"]),
            }
        )
    quality.extend(
        dict(row)
        for row in store.list_gap_evidence(
            series_id=int(candidate["series_id"]),
            start=_utc(requirement["required_start"], field="required_start"),
            end=_utc(requirement["required_end"], field="required_end"),
            as_of_commit_seq=as_of_commit_seq,
        )
    )
    return missing, quality


def plan_research_check(
    definition: CheckDefinition,
    request: CheckRequest,
    *,
    store: MarketDataStore = market_data_repo,
    indicator_planner: Callable[..., Mapping[str, Any]] = plan_runtime_requirements_for_indicators,
    inspect_coverage: bool = True,
) -> ResolvedCheckPlan:
    """Resolve explicit and transitive Check inputs without acquiring providers."""

    scope = dict(request.scope)
    family = definition.definition_id
    if family not in _MARKET_CHECK_FAMILIES:
        run_window = dict(scope.get("window") or {})
        start = _utc(
            scope.get("start") or run_window.get("start"), field="scope.start"
        )
        end = _utc(scope.get("end") or run_window.get("end"), field="scope.end")
        return ResolvedCheckPlan(
            schema_version=CHECK_PLAN_SCHEMA_VERSION,
            request_hash=request.request_hash,
            market_data_requirements=(),
            indicator_graph=(),
            evaluation_range={"start": _iso(start), "end_exclusive": _iso(end)},
            materialization_range={"start": _iso(start), "end_exclusive": _iso(end)},
            warmup={"bars": 0, "seconds": 0},
            outcome_tail={"bars": 0, "seconds": 0, "horizon_kind": "none"},
            gap_policy=request.parameters["gap_policy"],
        )

    instrument_id, _instrument = _scope_instrument(scope)
    timeframe = str(scope.get("timeframe") or scope.get("interval") or "").strip()
    timeframe_seconds = _timeframe_seconds(timeframe)
    evaluation_start = _utc(scope.get("start"), field="scope.start")
    evaluation_end = _utc(scope.get("end"), field="scope.end")
    if evaluation_end <= evaluation_start:
        raise ValueError("check_requirement_plan_invalid: end must be after start")

    outcomes = dict(request.parameters.get("outcomes") or {})
    horizons = _positive_ints(
        outcomes.get("forward_bars") or outcomes.get("horizons") or []
    )
    horizon_kind = str(outcomes.get("horizon_kind") or "bars").strip().lower()
    if horizon_kind not in {"bars", "elapsed_time"}:
        raise ValueError("check outcome horizon_kind must be bars or elapsed_time")
    outcome_tail_seconds = (
        max(horizons, default=0) * timeframe_seconds
        if horizon_kind == "bars"
        else max(horizons, default=0)
    )
    configured_warmup = int(scope.get("warmup_bars") or 0)
    feature_lookback = int(scope.get("feature_lookback_bars") or 0)
    warmup_bars = max(14, configured_warmup, feature_lookback)

    indicator_ids: list[str] = []
    indicator_id = str(scope.get("indicator_id") or "").strip()
    if family in _INDICATOR_CHECK_FAMILIES:
        if not indicator_id:
            raise ValueError(
                "check_requirement_plan_invalid: indicator_id is required"
            )
        indicator_ids.append(indicator_id)
    override_map = (
        {indicator_id: dict(scope.get("indicator_param_overrides") or {})}
        if indicator_id and scope.get("indicator_param_overrides") is not None
        else {}
    )
    indicator_plan = indicator_planner(
        indicator_ids,
        timeframe=timeframe,
        start=_iso(evaluation_start),
        end=_iso(evaluation_end + timedelta(seconds=outcome_tail_seconds)),
        param_overrides_by_id=override_map,
    )
    warmup_bars = max(warmup_bars, int(indicator_plan.get("warmup_bars") or 0))
    materialization_start = evaluation_start - timedelta(
        seconds=warmup_bars * timeframe_seconds
    )
    materialization_end = evaluation_end + timedelta(seconds=outcome_tail_seconds)

    requirements: list[dict[str, Any]] = [
        {
            "alias": "primary_bars",
            "consumer_id": "check",
            "instrument_id": instrument_id,
            "fact_type": CANDLE_FACT_TYPE,
            "contract_version": CANDLE_FACT_VERSION,
            "timeframe_seconds": timeframe_seconds,
            "dimensions": {},
            "alignment": "exact_interval",
            "max_staleness_seconds": None,
            "known_at_required": True,
            "required_fields": ["open", "high", "low", "close", "known_at"],
            "series_required": True,
            "frame_missing_policy": "indicator_owned",
            "source_policy": {
                "mode": (
                    "exact" if request.mode == CHECK_MODE_EVIDENCE else "current"
                )
            },
            "required_start": _iso(materialization_start),
            "required_end": _iso(materialization_end),
        }
    ]

    for raw in request.parameters.get("inputs") or []:
        requirement = _requirement_from_payload(raw)
        explicit_instrument = str(raw.get("instrument_id") or instrument_id).strip()
        lookback_seconds = max(
            int(requirement.lookback_seconds or 0),
            int(requirement.max_staleness_seconds or 0),
            int(requirement.lookback_bars or 0)
            * int(requirement.timeframe_seconds or timeframe_seconds),
        )
        requirements.append(
            {
                **requirement.to_dict(),
                "alias": requirement.key,
                "consumer_id": "check",
                "instrument_id": explicit_instrument,
                "series_required": bool(raw.get("series_required", True)),
                "frame_missing_policy": str(
                    raw.get("frame_missing_policy") or "exclude_and_count"
                ),
                "source_policy": dict(raw.get("source_policy") or {}),
                "required_start": _iso(
                    materialization_start - timedelta(seconds=lookback_seconds)
                ),
                "required_end": _iso(materialization_end),
            }
        )

    raw_bindings = scope.get("market_data_bindings")
    binding_config = dict(raw_bindings) if isinstance(raw_bindings, Mapping) else {}
    declarations: list[tuple[str, tuple[MarketDataRequirement, ...]]] = []
    indicator_required_start: dict[tuple[str, str], str] = {}
    for raw in indicator_plan.get("requirements") or []:
        consumer_id = str(raw["consumer_id"])
        requirement = _requirement_from_payload(raw["input"])
        declarations.append((consumer_id, (requirement,)))
        indicator_required_start[(consumer_id, requirement.key)] = str(
            raw["required_start"]
        )
    if declarations:
        resolved = MarketDataPlanResolver().resolve(
            declarations,
            instruments=InstrumentResolutionContext(
                primary_instrument_ids=(instrument_id,),
                underlying_by_primary=dict(
                    binding_config.get("underlying_by_primary") or {}
                ),
                benchmarks=dict(binding_config.get("benchmarks") or {}),
            ),
        )
        for series in resolved.series:
            for binding in series.bindings:
                alias = f"indicator:{binding.consumer_id}:{binding.requirement.key}"
                required_start = indicator_required_start[
                    (binding.consumer_id, binding.requirement.key)
                ]
                requirements.append(
                    {
                        **series.to_dict(),
                        "alias": alias,
                        "consumer_id": binding.consumer_id,
                        "instrument_id": series.instrument_id,
                        "series_required": bool(series.required),
                        "frame_missing_policy": (
                            "fail" if series.required else "unavailable"
                        ),
                        "source_policy": {
                            "mode": (
                                "exact"
                                if request.mode == CHECK_MODE_EVIDENCE
                                else "current"
                            )
                        },
                        "required_start": required_start,
                        "required_end": _iso(materialization_end),
                    }
                )

    aliases: set[str] = set()
    for requirement in requirements:
        alias = str(requirement["alias"])
        if alias in aliases:
            raise ValueError(f"check_requirement_plan_duplicate_alias: {alias}")
        aliases.add(alias)

    missing: list[dict[str, Any]] = []
    quality: list[dict[str, Any]] = []
    watermark = int(store.current_commit_seq()) if inspect_coverage else 0
    if inspect_coverage:
        for requirement in requirements:
            requirement_missing, requirement_quality = _coverage_for_requirement(
                requirement,
                store=store,
                as_of_commit_seq=watermark,
            )
            missing.extend(requirement_missing)
            quality.extend(requirement_quality)

    return ResolvedCheckPlan(
        schema_version=CHECK_PLAN_SCHEMA_VERSION,
        request_hash=request.request_hash,
        market_data_requirements=tuple(requirements),
        indicator_graph=tuple(indicator_plan.get("indicators") or ()),
        evaluation_range={
            "start": _iso(evaluation_start),
            "end_exclusive": _iso(evaluation_end),
        },
        materialization_range={
            "start": _iso(materialization_start),
            "end_exclusive": _iso(materialization_end),
            "as_of_commit_seq": watermark or None,
        },
        warmup={
            "bars": warmup_bars,
            "seconds": warmup_bars * timeframe_seconds,
            "timeframe_seconds": timeframe_seconds,
        },
        outcome_tail={
            "horizons": horizons,
            "bars": max(horizons, default=0) if horizon_kind == "bars" else None,
            "seconds": outcome_tail_seconds,
            "horizon_kind": horizon_kind,
        },
        gap_policy=request.parameters["gap_policy"],
        missing_coverage=tuple(missing),
        quality_evidence=tuple(quality),
    )


__all__ = ["plan_research_check"]
