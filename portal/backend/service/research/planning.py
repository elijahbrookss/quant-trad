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
from market_data.acquisition_coverage import (
    missing_complete_coverage,
    normalize_acquisition_coverage,
)
from market_data.fact_registry import get_fact_contract
from market_data.gaps import recorded_gaps_cover_interval
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
from portal.backend.service.market.frozen_dataset_service import (
    matching_source_identity_keys,
)
from portal.backend.service.storage.repos.market_data import market_data_repo

from .registry import CHECK_REGISTRY


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


def _scope_instrument(
    scope: Mapping[str, Any],
    *,
    instrument_loader: Callable[[str], Mapping[str, Any]],
) -> tuple[str, dict[str, Any]]:
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
    return instrument_id, dict(instrument_loader(instrument_id))


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


def _listed_series(row: Mapping[str, Any]) -> dict[str, Any]:
    normalized = dict(row)
    raw_id = normalized.get("series_id") or normalized.get("id")
    if raw_id in (None, ""):
        raise ValueError(
            "check_requirement_plan_invalid: listed series has no identity"
        )
    normalized["series_id"] = int(raw_id)
    return normalized


def _requested_source_keys(policy: Mapping[str, Any]) -> tuple[str, ...]:
    mode = str(policy.get("mode") or "current").strip().lower()
    if mode == "exact":
        key = str(policy.get("source_identity_key") or "").strip()
        return (key,) if key else ()
    if mode == "allowlist":
        return tuple(
            sorted(
                {
                    str(value).strip()
                    for value in policy.get("source_identity_keys") or []
                    if str(value).strip()
                }
            )
        )
    return ()


def _record_time(record: Any) -> datetime:
    fact = record.fact
    value = (
        getattr(fact, "open_time", None)
        or getattr(fact, "sample_time", None)
        or getattr(fact, "observation_time", None)
        or getattr(fact, "effective_at", None)
    )
    return _utc(value, field="record.time")


def _unrecorded_interval_gaps(
    *,
    records: Sequence[Any],
    start: datetime,
    end: datetime,
    timeframe_seconds: int,
    recorded_gaps: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    observed = {_record_time(record) for record in records}
    step = timedelta(seconds=int(timeframe_seconds))
    cursor = start
    missing: list[dict[str, Any]] = []
    while cursor < end:
        if cursor not in observed:
            gap_end = min(cursor + step, end)
            recorded = recorded_gaps_cover_interval(
                start=cursor,
                end=gap_end,
                evidence=recorded_gaps,
            )
            if not recorded:
                missing.append(
                    {
                        "start": _iso(cursor),
                        "end": _iso(gap_end),
                        "reason": "source_bound_interval_unrecorded",
                    }
                )
        cursor += step
    return missing


def _coverage_for_requirement(
    requirement: Mapping[str, Any],
    *,
    store: MarketDataStore,
    as_of_commit_seq: int,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    candidates = []
    fact_contract = get_fact_contract(str(requirement["fact_type"]))
    dimensions = fact_contract.normalize_dimensions(requirement.get("dimensions"))
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
            candidates.append(_listed_series(row))
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
    source_policy = dict(requirement.get("source_policy") or {})
    requested_series_id = source_policy.get("series_id")
    if requested_series_id not in (None, ""):
        semantic_candidate_ids = sorted(
            int(row["series_id"]) for row in candidates
        )
        candidates = [
            row
            for row in candidates
            if int(row["series_id"]) == int(requested_series_id)
        ]
        if not candidates:
            missing.append(
                {
                    "alias": requirement["alias"],
                    "reason": "source_series_binding_unresolved",
                    "requested_series_id": int(requested_series_id),
                    "candidate_series_ids": semantic_candidate_ids,
                }
            )
            return missing, quality
    records_by_series: dict[int, list[Any]] = {}
    policy_mode = str(source_policy.get("mode") or "current").strip().lower()
    source_resolved_candidates: list[dict[str, Any]] = []
    resolved_source_keys_by_series: dict[int, tuple[str, ...]] = {}
    for candidate in candidates:
        requested_source_keys = _requested_source_keys(source_policy)
        records = list(
            store.read_series_records(
                series_id=int(candidate["series_id"]),
                start=_utc(requirement["required_start"], field="required_start"),
                end=_utc(requirement["required_end"], field="required_end"),
                as_of_commit_seq=as_of_commit_seq,
                source_identity_keys=requested_source_keys,
            )
        )
        if policy_mode == "current":
            selected_source_keys: tuple[str, ...] = ()
        elif policy_mode == "exact" and requested_source_keys:
            selected_source_keys = requested_source_keys
        else:
            selected_source_keys = tuple(
                matching_source_identity_keys(records, source_policy)
            )
        if policy_mode == "current" or selected_source_keys:
            if selected_source_keys:
                records = list(
                    store.read_series_records(
                        series_id=int(candidate["series_id"]),
                        start=_utc(
                            requirement["required_start"], field="required_start"
                        ),
                        end=_utc(requirement["required_end"], field="required_end"),
                        as_of_commit_seq=as_of_commit_seq,
                        source_identity_keys=selected_source_keys,
                    )
                )
            records_by_series[int(candidate["series_id"])] = records
            resolved_source_keys_by_series[int(candidate["series_id"])] = (
                selected_source_keys
            )
            source_resolved_candidates.append(candidate)
    if not source_resolved_candidates:
        missing.append(
            {
                "alias": requirement["alias"],
                "reason": "source_binding_unresolved",
                "candidate_series_ids": sorted(
                    int(row["series_id"]) for row in candidates
                ),
                "source_policy": source_policy,
            }
        )
        return missing, quality
    if len(source_resolved_candidates) > 1:
        missing.append(
            {
                "alias": requirement["alias"],
                "reason": "series_resolution_ambiguous",
                "series_ids": sorted(
                    int(row["series_id"]) for row in source_resolved_candidates
                ),
            }
        )
        return missing, quality
    candidate = source_resolved_candidates[0]
    records = records_by_series[int(candidate["series_id"])]
    recorded_quality = list(
        store.list_gap_evidence(
            series_id=int(candidate["series_id"]),
            start=_utc(requirement["required_start"], field="required_start"),
            end=_utc(requirement["required_end"], field="required_end"),
            as_of_commit_seq=as_of_commit_seq,
            include_source_identity=True,
        )
    )
    resolved_source_keys = set(
        resolved_source_keys_by_series.get(int(candidate["series_id"]), ())
    )
    if policy_mode in {"exact", "allowlist"}:
        recorded_quality = [
            row
            for row in recorded_quality
            if str(row.get("source_identity_key") or "").strip()
            in resolved_source_keys
        ]
    acquisition_coverage_proves_range = False
    if fact_contract.uses_exact_numeric_storage and policy_mode in {
        "exact",
        "allowlist",
    }:
        coverage_reader = getattr(store, "list_source_acquisition_coverage", None)
        if not callable(coverage_reader):
            missing.append(
                {
                    "alias": requirement["alias"],
                    "series_id": int(candidate["series_id"]),
                    "reason": "acquisition_coverage_reader_unavailable",
                }
            )
        else:
            coverage = normalize_acquisition_coverage(
                coverage_reader(
                    series_id=int(candidate["series_id"]),
                    source_identity_keys=tuple(sorted(resolved_source_keys)),
                    start=_utc(requirement["required_start"], field="required_start"),
                    end=_utc(requirement["required_end"], field="required_end"),
                )
            )
            uncovered_ranges = missing_complete_coverage(
                start=requirement["required_start"],
                end=requirement["required_end"],
                source_identity_keys=tuple(sorted(resolved_source_keys)),
                coverage=coverage,
            )
            acquisition_coverage_proves_range = bool(resolved_source_keys) and not (
                uncovered_ranges
            )
            for uncovered in uncovered_ranges:
                missing.append(
                    {
                        "alias": requirement["alias"],
                        "series_id": int(candidate["series_id"]),
                        "reason": "source_acquisition_coverage_missing",
                        **uncovered,
                    }
                )
            quality.extend(
                {
                    "alias": requirement["alias"],
                    "series_id": int(candidate["series_id"]),
                    "classification": "source_acquisition_coverage",
                    "start": row["range_start"],
                    "end": row["range_end"],
                    "coverage": row,
                }
                for row in coverage
            )
    if not records and not acquisition_coverage_proves_range:
        missing.append(
            {
                "alias": requirement["alias"],
                "reason": "no_facts_in_required_range",
                "series_id": int(candidate["series_id"]),
            }
        )
    elif records and (
        str(requirement.get("alignment") or "") == "exact_interval"
        and requirement.get("timeframe_seconds") is not None
    ):
        for gap in _unrecorded_interval_gaps(
            records=records,
            start=_utc(requirement["required_start"], field="required_start"),
            end=_utc(requirement["required_end"], field="required_end"),
            timeframe_seconds=int(requirement["timeframe_seconds"]),
            recorded_gaps=recorded_quality,
        ):
            missing.append(
                {
                    "alias": requirement["alias"],
                    "series_id": int(candidate["series_id"]),
                    **gap,
                }
            )
    quality.extend(
        {
            "alias": requirement["alias"],
            "series_id": int(candidate["series_id"]),
            **dict(row),
        }
        for row in recorded_quality
    )
    return missing, quality


def plan_research_check(
    definition: CheckDefinition,
    request: CheckRequest,
    *,
    store: MarketDataStore = market_data_repo,
    indicator_planner: Callable[..., Mapping[str, Any]] = plan_runtime_requirements_for_indicators,
    instrument_loader: Callable[[str], Mapping[str, Any]] | None = None,
    inspect_coverage: bool = True,
    require_durable_sources: bool = False,
) -> ResolvedCheckPlan:
    """Resolve explicit and transitive Check inputs without acquiring providers."""

    scope = dict(request.scope)
    instrument_loader = instrument_loader or instrument_service.get_instrument_record
    evaluator = CHECK_REGISTRY.resolve_evaluator(definition)
    declaration = dict(
        evaluator.declare_requirements(definition=definition, request=request)
    )
    durable_sources = request.mode == CHECK_MODE_EVIDENCE or bool(
        require_durable_sources
    )
    if str(declaration.get("input_kind") or "") == "immutable_run_evidence":
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
            execution=declaration,
        )

    instrument_id, _instrument = _scope_instrument(
        scope, instrument_loader=instrument_loader
    )
    timeframe = str(scope.get("timeframe") or scope.get("interval") or "").strip()
    timeframe_seconds = _timeframe_seconds(timeframe)
    evaluation_start = _utc(scope.get("start"), field="scope.start")
    evaluation_end = _utc(scope.get("end"), field="scope.end")
    if evaluation_end <= evaluation_start:
        raise ValueError("check_requirement_plan_invalid: end must be after start")

    horizons = _positive_ints(declaration.get("outcome_horizons") or [])
    horizon_kind = str(declaration.get("horizon_kind") or "bars").strip().lower()
    if horizon_kind not in {"bars", "elapsed_time"}:
        raise ValueError("check outcome horizon_kind must be bars or elapsed_time")
    outcome_tail_seconds = (
        max(horizons, default=0) * timeframe_seconds
        if horizon_kind == "bars"
        else max(horizons, default=0)
    )
    entry_lag_bars = int(declaration.get("entry_lag_bars") or 0)
    invalidation_max_bars = int(declaration.get("invalidation_max_bars") or 0)
    if entry_lag_bars < 0 or invalidation_max_bars < 0:
        raise ValueError(
            "check_requirement_plan_invalid: outcome tail bars must be nonnegative"
        )
    outcome_tail_seconds = entry_lag_bars * timeframe_seconds + max(
        outcome_tail_seconds,
        invalidation_max_bars * timeframe_seconds,
    )
    configured_warmup = int(scope.get("warmup_bars") or 0)
    feature_lookback = int(declaration.get("feature_lookback_bars") or 0)
    warmup_bars = max(
        int(declaration.get("warmup_floor_bars") or 0),
        configured_warmup,
        feature_lookback,
    )

    indicator_ids = [
        str(value)
        for value in declaration.get("indicator_ids") or []
        if str(value).strip()
    ]
    indicator_id = indicator_ids[0] if indicator_ids else ""
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
                    "exact" if durable_sources else "current"
                )
            },
            "required_start": _iso(materialization_start),
            "required_end": _iso(materialization_end),
        }
    ]

    raw_bindings = scope.get("market_data_bindings")
    binding_config = dict(raw_bindings) if isinstance(raw_bindings, Mapping) else {}
    shared_resolution_context = InstrumentResolutionContext(
        primary_instrument_ids=(instrument_id,),
        underlying_by_primary=dict(
            binding_config.get("underlying_by_primary") or {}
        ),
        benchmarks=dict(binding_config.get("benchmarks") or {}),
    )
    feature_windows = dict(
        declaration.get("feature_windows_seconds_by_alias") or {}
    )
    feature_predecessor_aliases = {
        str(value)
        for value in declaration.get("feature_predecessor_aliases") or []
    }
    for raw in request.parameters.get("inputs") or []:
        requirement = _requirement_from_payload(raw)
        explicit_instrument = str(raw.get("instrument_id") or "").strip()
        role = requirement.instrument_role.value
        if explicit_instrument and role == "primary":
            resolution_context = InstrumentResolutionContext(
                primary_instrument_ids=(explicit_instrument,),
                underlying_by_primary={},
                benchmarks={},
            )
        elif explicit_instrument and role == "underlying":
            resolution_context = InstrumentResolutionContext(
                primary_instrument_ids=(instrument_id,),
                underlying_by_primary={instrument_id: explicit_instrument},
                benchmarks={},
            )
        elif explicit_instrument and role == "benchmark":
            benchmark_key = str(requirement.instrument_ref or "").strip()
            if not benchmark_key:
                raise ValueError(
                    "check_requirement_plan_invalid: explicit benchmark input requires instrument_ref"
                )
            resolution_context = InstrumentResolutionContext(
                primary_instrument_ids=(instrument_id,),
                underlying_by_primary={},
                benchmarks={benchmark_key: explicit_instrument},
            )
        else:
            resolution_context = shared_resolution_context
        resolved_explicit = MarketDataPlanResolver().resolve(
            [("check", (requirement,))],
            instruments=resolution_context,
        )
        if len(resolved_explicit.series) != 1:
            raise RuntimeError(
                "check_requirement_plan_invalid: explicit input did not resolve one semantic series"
            )
        resolved_series = resolved_explicit.series[0]
        feature_window_seconds = int(feature_windows.get(requirement.key) or 0)
        lookback_seconds = max(
            int(requirement.lookback_seconds or 0),
            int(requirement.max_staleness_seconds or 0),
            int(requirement.lookback_bars or 0)
            * int(requirement.timeframe_seconds or timeframe_seconds),
            feature_window_seconds,
        )
        if requirement.key in feature_predecessor_aliases:
            lookback_seconds += max(lookback_seconds, timeframe_seconds)
        requirements.append(
            {
                **resolved_series.to_dict(),
                "alias": requirement.key,
                "consumer_id": "check",
                "instrument_id": resolved_series.instrument_id,
                "series_required": bool(raw.get("series_required", True)),
                "frame_missing_policy": str(
                    raw.get("frame_missing_policy") or "exclude_and_count"
                ),
                "source_policy": dict(raw.get("source_policy") or {}),
                "required_start": _iso(
                    evaluation_start - timedelta(seconds=lookback_seconds)
                ),
                # Check facts are sampled only at decision times. Outcome and
                # invalidation tails require primary candles, not more high-rate
                # fact history after the final decision boundary.
                "required_end": _iso(evaluation_end),
            }
        )

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
            instruments=shared_resolution_context,
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
                                if durable_sources
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
            "required_horizons": list(
                declaration.get("required_outcome_horizons") or horizons
            ),
            "bars": (
                entry_lag_bars
                + max(max(horizons, default=0), invalidation_max_bars)
                if horizon_kind == "bars"
                else None
            ),
            "seconds": outcome_tail_seconds,
            "horizon_kind": horizon_kind,
            "entry_lag_bars": entry_lag_bars,
            "invalidation_max_bars": invalidation_max_bars,
        },
        gap_policy=request.parameters["gap_policy"],
        execution=declaration,
        missing_coverage=tuple(missing),
        quality_evidence=tuple(quality),
    )


def rederive_research_check_plan_from_pinned_inputs(
    definition: CheckDefinition,
    request: CheckRequest,
    persisted_plan: ResolvedCheckPlan,
    *,
    subject_snapshots: Mapping[str, Mapping[str, Any]],
) -> ResolvedCheckPlan:
    """Rebuild plan semantics without consulting mutable Indicator/instrument rows."""

    graph_rows = [dict(row) for row in persisted_plan.indicator_graph]
    preloaded_metas: dict[str, dict[str, Any]] = {}
    for row in graph_rows:
        indicator_id = str(row.get("indicator_id") or "").strip()
        if not indicator_id or indicator_id in preloaded_metas:
            raise ValueError(
                "check_evidence_payload_invalid: pinned Indicator graph identities are invalid"
            )
        preloaded_metas[indicator_id] = {
            "id": indicator_id,
            "type": str(row.get("indicator_type") or ""),
            "params": dict(row.get("params") or {}),
            "dependencies": list(row.get("dependencies") or []),
            "enabled": True,
            "runtime_supported": True,
        }

    def pinned_indicator_planner(indicator_ids, **kwargs):
        return plan_runtime_requirements_for_indicators(
            indicator_ids,
            **kwargs,
            preloaded_metas=preloaded_metas,
        )

    def pinned_instrument_loader(instrument_id: str) -> Mapping[str, Any]:
        snapshot = subject_snapshots.get(str(instrument_id))
        if snapshot is None:
            raise ValueError(
                "check_evidence_payload_invalid: plan subject is absent from frozen binding"
            )
        return dict(snapshot)

    return plan_research_check(
        definition,
        request,
        indicator_planner=pinned_indicator_planner,
        instrument_loader=pinned_instrument_loader,
        inspect_coverage=False,
    )


__all__ = [
    "plan_research_check",
    "rederive_research_check_plan_from_pinned_inputs",
]
