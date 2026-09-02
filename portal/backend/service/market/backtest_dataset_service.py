"""Canonical preparation and admission for immutable backtest datasets."""

from __future__ import annotations

import hashlib
import json
import time
from collections import Counter
from collections.abc import Callable, Mapping, Sequence
from copy import deepcopy
from dataclasses import asdict, is_dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

from data_providers.numeric_facts import (
    NumericAcquisitionBudget,
    load_numeric_fact_manifest,
)
from data_providers.utils.ohlcv import interval_to_timedelta
from indicators.registry import get_indicator_manifest
from market_data.backtest import (
    BACKTEST_DATASET_BINDING_VERSION,
    BACKTEST_DATASET_PLAN_VERSION,
    build_backtest_execution_config_hash,
    build_backtest_execution_instrument,
    iso_utc,
    normalize_backtest_dataset_binding,
    normalize_backtest_execution_instruments,
    resolve_backtest_warmup_bars,
)
from market_data.canonical import (
    CanonicalFactRecord,
    build_canonical_fact_provenance_hash,
    build_canonical_fact_series_material_hash,
)
from market_data.contracts import (
    CANDLE_FACT_TYPE,
    CANDLE_FACT_VERSION,
    DATASET_IDENTITY_HASH_VERSION,
    OPEN_INTEREST_FACT_TYPE,
    OPEN_INTEREST_FACT_VERSION,
    FUNDING_RATE_FACT_TYPE,
    DatasetSeriesRequest,
    MarketDataRequirement,
    TypedFeatureRecord,
    build_candle_material_hash,
    build_dataset_identity_hash,
    dataset_series_identity_payload,
    build_numeric_fact_material_hash,
    build_open_interest_material_hash,
    build_provenance_hash,
    build_funding_rate_material_hash,
    build_typed_feature_material_hash,
    record_effective_time,
    build_quality_hash,
)
from market_data.fact_registry import get_fact_contract
from market_data.structure import (
    MARKET_TRADE_FACT_TYPE,
    TRADE_FLOW_FACT_TYPE,
    build_market_trade_material_hash,
    build_trade_flow_material_hash,
)
from market_data.requirements import (
    InstrumentResolutionContext,
    MarketDataPlanResolver,
    UnavailableMarketData,
    causal_numeric_fact_records,
    latest_known_record,
)
from market_data.store import MarketDataStore
from strategies.compiler import compile_strategy

from ..indicators.dependency_bindings import normalize_dependency_bindings
from ..indicators.indicator_service import (
    get_instance_meta,
    runtime_input_plan_for_instance,
)
from . import instrument_service
from .candle_service import preflight_candle_coverage_by_instrument
from .feed_service import HistoricalCandleIngestor, historical_candle_ingestor
from .numeric_fact_acquisition import (
    NumericAcquisitionAuthorization,
    NumericFactAcquisitionService,
    numeric_fact_acquisition_service,
)
from ..storage.repos.market_data import market_data_repo


_ALLOWED_DISCLOSED_GAP_CLASSIFICATIONS = frozenset(
    {"expected_sparse", "market_closure", "planned_closure", "provider_closure"}
)


def _utc(value: Any, *, field: str) -> datetime:
    raw = iso_utc(value, field=field)
    return datetime.fromisoformat(raw.replace("Z", "+00:00")).astimezone(timezone.utc)


def _timeframe_seconds(value: str) -> int:
    try:
        seconds = int(interval_to_timedelta(str(value or "").strip()).total_seconds())
    except Exception as exc:
        raise ValueError(
            f"backtest_dataset_invalid: unsupported timeframe {value!r}"
        ) from exc
    if seconds <= 0:
        raise ValueError("backtest_dataset_invalid: timeframe must be positive")
    return seconds


def _phase_start() -> tuple[float, float]:
    return time.perf_counter(), time.process_time()


def _phase_finish(
    timings: dict[str, dict[str, Any]],
    name: str,
    started: tuple[float, float],
    **evidence: Any,
) -> None:
    wall_started, cpu_started = started
    timings[name] = {
        "wall_seconds": max(time.perf_counter() - wall_started, 0.0),
        "cpu_seconds": max(time.process_time() - cpu_started, 0.0),
        **evidence,
    }


def _declared_strategy_hash(strategy: Any) -> str | None:
    for payload in (
        getattr(strategy, "run_strategy_snapshot", None),
        getattr(strategy, "effective_strategy_config", None),
    ):
        if not isinstance(payload, Mapping):
            continue
        value = str(payload.get("strategy_hash") or "").strip()
        if value:
            return value
    return None


def _contract_payload(values: Sequence[Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for value in values:
        if is_dataclass(value):
            rows.append(asdict(value))
        elif isinstance(value, Mapping):
            rows.append(dict(value))
        else:
            rows.append({"value": str(value)})
    return rows


def _stable_semantic_hash(payload: Mapping[str, Any]) -> str:
    encoded = json.dumps(
        dict(payload),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        default=str,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _strategy_indicator_ids(strategy: Any) -> list[str]:
    return list(
        dict.fromkeys(
            str(value).strip()
            for value in list(getattr(strategy, "indicator_ids", None) or [])
            if str(value).strip()
        )
    )


def _resolve_indicator_graph(
    strategy: Any,
    *,
    meta_loader: Callable[..., Mapping[str, Any]],
) -> list[tuple[str, dict[str, Any], Any, str]]:
    """Resolve and validate every direct and transitive runtime indicator."""

    roots = _strategy_indicator_ids(strategy)
    root_ids = set(roots)
    loaded: dict[str, dict[str, Any]] = {}
    resolved: dict[str, dict[str, Any]] = {}
    manifests: dict[str, Any] = {}
    visiting: list[str] = []

    def load(indicator_id: str) -> dict[str, Any]:
        normalized_id = str(indicator_id or "").strip()
        if not normalized_id:
            raise ValueError(
                "backtest_strategy_identity_invalid: indicator id is required"
            )
        if normalized_id not in loaded:
            meta = dict(meta_loader(normalized_id))
            declared_id = str(meta.get("id") or "").strip()
            if declared_id and declared_id != normalized_id:
                raise RuntimeError(
                    "backtest_strategy_identity_disagreement: indicator metadata "
                    f"substitution requested={normalized_id} actual={declared_id}"
                )
            loaded[normalized_id] = meta
        return loaded[normalized_id]

    def visit(indicator_id: str) -> None:
        normalized_id = str(indicator_id or "").strip()
        if normalized_id in resolved:
            return
        if normalized_id in visiting:
            cycle = " -> ".join([*visiting, normalized_id])
            raise ValueError(
                f"backtest_indicator_dependency_cycle: {cycle}"
            )
        visiting.append(normalized_id)
        meta = dict(load(normalized_id))
        indicator_type = str(meta.get("type") or "").strip()
        if not indicator_type:
            raise ValueError(
                "backtest_strategy_identity_invalid: "
                f"indicator {normalized_id} has no type"
            )
        manifest = get_indicator_manifest(indicator_type)
        bindings = normalize_dependency_bindings(meta.get("dependencies"))
        expected_dependencies = list(manifest.dependencies)
        resolved_bindings: list[dict[str, str]] = []
        matched_indexes: set[int] = set()
        for dependency in expected_dependencies:
            candidates = [
                (index, binding)
                for index, binding in enumerate(bindings)
                if str(binding.get("output_name") or "").strip()
                == str(dependency.output_name or "").strip()
            ]
            if len(candidates) != 1:
                reason = "missing" if not candidates else "ambiguous"
                raise ValueError(
                    "backtest_indicator_dependency_invalid: "
                    f"indicator_id={normalized_id} output={dependency.output_name} "
                    f"binding={reason}"
                )
            index, binding = candidates[0]
            matched_indexes.add(index)
            dependency_id = str(binding.get("indicator_id") or "").strip()
            visit(dependency_id)
            dependency_type = str(
                resolved[dependency_id].get("type") or ""
            ).strip()
            expected_type = str(dependency.indicator_type or "").strip()
            declared_type = str(binding.get("indicator_type") or "").strip()
            if dependency_type != expected_type or (
                declared_type and declared_type != dependency_type
            ):
                raise ValueError(
                    "backtest_indicator_dependency_invalid: "
                    f"indicator_id={normalized_id} dependency_id={dependency_id} "
                    f"expected_type={expected_type} actual_type={dependency_type}"
                )
            resolved_bindings.append(
                {
                    "indicator_id": dependency_id,
                    "indicator_type": dependency_type,
                    "output_name": str(dependency.output_name),
                }
            )
        extras = [
            binding
            for index, binding in enumerate(bindings)
            if index not in matched_indexes
        ]
        if extras:
            raise ValueError(
                "backtest_indicator_dependency_invalid: "
                f"indicator_id={normalized_id} unexpected_bindings={extras}"
            )
        visiting.pop()
        resolved[normalized_id] = {**meta, "dependencies": resolved_bindings}
        manifests[normalized_id] = manifest

    for indicator_id in roots:
        visit(indicator_id)
    return [
        (
            indicator_id,
            resolved[indicator_id],
            manifests[indicator_id],
            "strategy" if indicator_id in root_ids else "dependency",
        )
        for indicator_id in sorted(resolved)
    ]


def resolve_backtest_strategy_identity(
    strategy: Any,
    *,
    indicator_meta_loader: Callable[..., Mapping[str, Any]] = get_instance_meta,
) -> dict[str, Any]:
    """Resolve the exact compiled strategy and indicator configuration identity."""

    strategy_id = str(getattr(strategy, "id", "") or "").strip()
    timeframe = str(getattr(strategy, "timeframe", "") or "").strip()
    if not strategy_id or not timeframe:
        raise ValueError(
            "backtest_strategy_identity_invalid: strategy id and timeframe are required"
        )
    indicator_ids = _strategy_indicator_ids(strategy)
    indicator_graph = _resolve_indicator_graph(
        strategy,
        meta_loader=indicator_meta_loader,
    )
    metas = {indicator_id: meta for indicator_id, meta, _manifest, _role in indicator_graph}

    def load_meta(indicator_id: str) -> Mapping[str, Any]:
        normalized_id = str(indicator_id or "").strip()
        if normalized_id not in metas:
            raise KeyError(f"Indicator not found in admitted runtime graph: {normalized_id}")
        return metas[normalized_id]

    compilation_inputs = getattr(strategy, "compilation_inputs", None)
    if callable(compilation_inputs):
        rules, params = compilation_inputs()
    else:
        rules = getattr(strategy, "rules", {}) or {}
        params = getattr(strategy, "resolved_params", {}) or {}
    compiled = compile_strategy(
        strategy_id=strategy_id,
        timeframe=timeframe,
        rules=rules,
        attached_indicator_ids=indicator_ids,
        indicator_meta_getter=load_meta,
        params=params,
    )
    declared_hash = _declared_strategy_hash(strategy)
    if declared_hash and declared_hash != compiled.strategy_hash:
        raise RuntimeError(
            "backtest_strategy_identity_disagreement: declared strategy hash differs "
            "from the canonical compiled strategy"
        )

    indicator_contracts: list[dict[str, Any]] = []
    for indicator_id, meta, manifest, attachment_role in indicator_graph:
        indicator_type = str(meta.get("type") or "").strip()
        indicator_contracts.append(
            {
                "indicator_id": indicator_id,
                "indicator_type": indicator_type,
                "attachment_role": attachment_role,
                "params": dict(meta.get("params") or {}),
                "enabled": bool(meta.get("enabled", True)),
                "dependency_bindings": list(meta.get("dependencies") or []),
                "manifest_type": manifest.type,
                "manifest_version": manifest.version,
                "market_inputs": _contract_payload(manifest.market_inputs),
                "runtime_inputs": _contract_payload(manifest.runtime_inputs),
                "dependencies": _contract_payload(manifest.dependencies),
            }
        )
    indicator_contracts.sort(key=lambda row: str(row["indicator_id"]))
    effective_hash = None
    for payload in (
        getattr(strategy, "run_strategy_snapshot", None),
        getattr(strategy, "effective_strategy_config", None),
    ):
        if isinstance(payload, Mapping):
            candidate = str(
                payload.get("effective_strategy_config_hash") or ""
            ).strip()
            if candidate:
                effective_hash = candidate
                break
    execution_policy_hash = _stable_semantic_hash(
        {
            "schema_version": "backtest_execution_policy.v1",
            "atm_template_id": getattr(strategy, "atm_template_id", None),
            "atm_template": dict(getattr(strategy, "atm_template", None) or {}),
            "risk_config": dict(getattr(strategy, "risk_config", None) or {}),
        }
    )
    return {
        "strategy_id": strategy_id,
        "strategy_hash": compiled.strategy_hash,
        "effective_strategy_config_hash": effective_hash,
        "indicator_config_hash": _stable_semantic_hash(
            {
                "schema_version": "backtest_indicator_configuration.v1",
                "strategy_indicator_ids": indicator_ids,
                "indicators": indicator_contracts,
            }
        ),
        "execution_policy_hash": execution_policy_hash,
        "indicator_count": len(indicator_contracts),
        "direct_indicator_count": len(indicator_ids),
    }


def _indicator_requirements(
    strategy: Any,
    *,
    evaluation_start: datetime,
    meta_loader: Callable[..., Mapping[str, Any]],
    input_plan_loader: Callable[..., Mapping[str, Any]],
) -> tuple[
    list[dict[str, Any]],
    list[dict[str, Any]],
    list[tuple[str, tuple[MarketDataRequirement, ...]]],
]:
    warmup: list[dict[str, Any]] = []
    inputs: list[dict[str, Any]] = []
    declarations: list[tuple[str, tuple[MarketDataRequirement, ...]]] = []
    indicator_graph = _resolve_indicator_graph(
        strategy,
        meta_loader=meta_loader,
    )
    for indicator_id, meta, manifest, attachment_role in indicator_graph:
        if not indicator_id:
            continue
        indicator_type = str(meta.get("type") or "").strip()
        if not indicator_type:
            raise ValueError(
                f"backtest_dataset_invalid: indicator {indicator_id} has no type"
            )
        params = meta.get("params") if isinstance(meta.get("params"), Mapping) else {}
        if "warmup_bars" in params:
            raw_bars = params.get("warmup_bars")
            if isinstance(raw_bars, bool):
                raise ValueError(
                    f"backtest_dataset_invalid: indicator {indicator_id} warmup_bars is malformed"
                )
            try:
                required_bars = int(raw_bars)
            except (TypeError, ValueError) as exc:
                raise ValueError(
                    f"backtest_dataset_invalid: indicator {indicator_id} warmup_bars is malformed"
                ) from exc
            if required_bars <= 0:
                raise ValueError(
                    f"backtest_dataset_invalid: indicator {indicator_id} warmup_bars must be positive"
                )
            warmup.append(
                {
                    "indicator_id": indicator_id,
                    "indicator_type": indicator_type,
                    "attachment_role": attachment_role,
                    "required_bars": required_bars,
                }
            )
        point = iso_utc(evaluation_start)
        plan = dict(
            input_plan_loader(
                indicator_id,
                strategy_interval=strategy.timeframe,
                start=point,
                end=point,
            )
        )
        source_timeframe = str(
            plan.get("source_timeframe") or strategy.timeframe
        ).strip()
        lookback_bars = plan.get("lookback_bars")
        lookback_days = plan.get("lookback_days")
        lookback_seconds = (
            int(lookback_days) * 86400 if lookback_days not in (None, "") else None
        )
        requirements = tuple(
            market_input.to_requirement(
                timeframe_seconds=(
                    _timeframe_seconds(source_timeframe)
                    if get_fact_contract(market_input.fact_type).timeframe_mode != "forbidden"
                    else None
                ),
                lookback_bars=(
                    int(lookback_bars)
                    if lookback_bars not in (None, "")
                    else None
                ),
                lookback_seconds=lookback_seconds,
            )
            for market_input in manifest.market_inputs
        )
        declarations.append((indicator_id, requirements))
        inputs.append(
            {
                "indicator_id": indicator_id,
                "indicator_type": indicator_type,
                "attachment_role": attachment_role,
                "source_timeframe": source_timeframe,
                "required_start": iso_utc(
                    plan.get("start") or evaluation_start,
                    field=f"indicator[{indicator_id}].required_start",
                ),
                "lookback_bars": plan.get("lookback_bars"),
                "lookback_days": plan.get("lookback_days"),
                "requirements": [item.to_dict() for item in requirements],
                "market_inputs": [
                    {
                        "key": item.key,
                        "fact_type": item.fact_type,
                        "contract_version": item.contract_version,
                        "instrument_role": (
                            item.instrument_role.value
                            if hasattr(item.instrument_role, "value")
                            else str(item.instrument_role)
                        ),
                        "instrument_ref": item.instrument_ref,
                        **(
                            {"dimensions": dict(item.dimensions)}
                            if item.dimensions
                            else {}
                        ),
                        "alignment": (
                            item.alignment.value
                            if hasattr(item.alignment, "value")
                            else item.alignment
                        ),
                        "max_staleness_seconds": item.max_staleness_seconds,
                        "required": bool(item.required),
                        "allow_gaps": bool(item.allow_gaps),
                        "required_fields": list(item.required_fields),
                        "known_at_required": bool(item.known_at_required),
                    }
                    for item in manifest.market_inputs
                ],
            }
        )
    return warmup, inputs, declarations


def derive_backtest_dataset_plan(
    *,
    bot: Mapping[str, Any],
    strategy: Any,
    evaluation_start: Any,
    evaluation_end: Any,
    indicator_meta_loader: Callable[..., Mapping[str, Any]] = get_instance_meta,
    indicator_input_plan_loader: Callable[..., Mapping[str, Any]] = runtime_input_plan_for_instance,
    instrument_loader: Callable[[str], Mapping[str, Any]] = instrument_service.get_instrument_record,
) -> dict[str, Any]:
    """Derive exact typed source-fact ranges before acquisition or execution."""

    start = _utc(evaluation_start, field="evaluation_start")
    end = _utc(evaluation_end, field="evaluation_end")
    if end <= start:
        raise ValueError(
            "backtest_dataset_invalid: evaluation_end must be after evaluation_start"
        )
    strategy_timeframe = str(getattr(strategy, "timeframe", "") or "").strip()
    strategy_seconds = _timeframe_seconds(strategy_timeframe)
    if int(start.timestamp()) % strategy_seconds or int(end.timestamp()) % strategy_seconds:
        raise ValueError(
            "backtest_dataset_invalid: evaluation range must align to strategy timeframe"
        )
    if not getattr(strategy, "instrument_links", None):
        raise ValueError("backtest_dataset_invalid: strategy has no instruments")

    warmup_requirements, indicator_inputs, indicator_declarations = _indicator_requirements(
        strategy,
        evaluation_start=start,
        meta_loader=indicator_meta_loader,
        input_plan_loader=indicator_input_plan_loader,
    )
    strategy_identity = resolve_backtest_strategy_identity(
        strategy,
        indicator_meta_loader=indicator_meta_loader,
    )
    warmup_bars = resolve_backtest_warmup_bars(
        warmup_requirements,
        configured_bars=bot.get("backtest_warmup_bars"),
    )
    base_warmup_start = start - timedelta(seconds=strategy_seconds * warmup_bars)

    merged: dict[
        tuple[str, str, str, int | None, tuple[tuple[str, str], ...]],
        dict[str, Any],
    ] = {}
    instruments: list[dict[str, Any]] = []
    instrument_cache: dict[str, dict[str, Any]] = {}
    primary_instrument_ids: list[str] = []
    for link in strategy.instrument_links:
        instrument_id = str(getattr(link, "instrument_id", "") or "").strip()
        if not instrument_id:
            raise ValueError(
                "backtest_dataset_invalid: strategy instrument link has no instrument_id"
            )
        instrument = dict(instrument_loader(instrument_id))
        instrument_cache[instrument_id] = instrument
        primary_instrument_ids.append(instrument_id)
        instruments.append(
            build_backtest_execution_instrument(instrument_id, instrument)
        )

    def load_data_instrument(instrument_id: str) -> dict[str, Any]:
        if instrument_id not in instrument_cache:
            instrument_cache[instrument_id] = dict(instrument_loader(instrument_id))
        return instrument_cache[instrument_id]

    def merge_series(
        *,
        instrument_id: str,
        fact_type: str,
        contract_version: str,
        timeframe: str | None,
        timeframe_seconds: int | None,
        dimensions: Mapping[str, Any],
        required_start: datetime,
        role: str,
        indicator_id: str | None,
        required: bool,
        allow_gaps: bool,
        alignment: str,
        max_staleness_seconds: int | None,
        bindings: list[dict[str, Any]],
    ) -> None:
        instrument = load_data_instrument(instrument_id)
        normalized_dimensions = get_fact_contract(fact_type).normalize_dimensions(
            dimensions
        )
        dimension_key = tuple(sorted(normalized_dimensions.items()))
        key = (
            instrument_id,
            fact_type,
            contract_version,
            timeframe_seconds,
            dimension_key,
        )
        existing = merged.get(key)
        if existing is None:
            existing = {
                "instrument_id": instrument_id,
                "symbol": instrument.get("symbol"),
                "provider": instrument.get("datasource"),
                "venue": instrument.get("exchange"),
                "fact_type": fact_type,
                "contract_version": contract_version,
                "timeframe": timeframe,
                "timeframe_seconds": timeframe_seconds,
                "range_start": iso_utc(required_start),
                "range_end": iso_utc(end),
                "required": bool(required),
                "allow_gaps": bool(allow_gaps),
                "alignment": alignment,
                "max_staleness_seconds": max_staleness_seconds,
                "roles": [],
                "indicator_ids": [],
                "bindings": [],
            }
            if normalized_dimensions:
                existing["dimensions"] = normalized_dimensions
            merged[key] = existing
        else:
            existing["required"] = bool(existing["required"] or required)
            existing["allow_gaps"] = bool(existing["allow_gaps"] and allow_gaps)
        if required_start < _utc(existing["range_start"], field="range_start"):
            existing["range_start"] = iso_utc(required_start)
        if role not in existing["roles"]:
            existing["roles"].append(role)
        if indicator_id and indicator_id not in existing["indicator_ids"]:
            existing["indicator_ids"].append(indicator_id)
        known_bindings = {
            (row.get("consumer_id"), row.get("input", {}).get("key"), row.get("primary_instrument_id"))
            for row in existing["bindings"]
        }
        for binding in bindings:
            binding_key = (
                binding.get("consumer_id"),
                binding.get("input", {}).get("key"),
                binding.get("primary_instrument_id"),
            )
            if binding_key not in known_bindings:
                existing["bindings"].append(binding)
                known_bindings.add(binding_key)

    for instrument_id in primary_instrument_ids:
        merge_series(
            instrument_id=instrument_id,
            fact_type=CANDLE_FACT_TYPE,
            contract_version=CANDLE_FACT_VERSION,
            timeframe=strategy_timeframe,
            timeframe_seconds=strategy_seconds,
            dimensions={},
            required_start=base_warmup_start,
            role="strategy_primary_bars",
            indicator_id=None,
            required=True,
            allow_gaps=False,
            alignment="exact_interval",
            max_staleness_seconds=None,
            bindings=[],
        )

    if indicator_declarations:
        raw_bindings = bot.get("market_data_bindings")
        binding_config = dict(raw_bindings) if isinstance(raw_bindings, Mapping) else {}
        underlying = binding_config.get("underlying_by_primary")
        benchmarks = binding_config.get("benchmarks")
        resolved_plan = MarketDataPlanResolver().resolve(
            indicator_declarations,
            instruments=InstrumentResolutionContext(
                primary_instrument_ids=tuple(primary_instrument_ids),
                underlying_by_primary=(
                    dict(underlying) if isinstance(underlying, Mapping) else {}
                ),
                benchmarks=(
                    dict(benchmarks) if isinstance(benchmarks, Mapping) else {}
                ),
            ),
        )
        input_by_id = {
            str(item["indicator_id"]): item for item in indicator_inputs
        }
        for resolved in resolved_plan.series:
            bindings = [binding.to_dict() for binding in resolved.bindings]
            indicator_ids = sorted(
                {binding.consumer_id for binding in resolved.bindings}
            )
            if resolved.alignment.value == "exact_interval":
                starts = [
                    _utc(
                        input_by_id[indicator_id]["required_start"],
                        field=f"indicator[{indicator_id}].required_start",
                    )
                    for indicator_id in indicator_ids
                ]
                required_start = min(starts)
                timeframe = str(
                    input_by_id[indicator_ids[0]]["source_timeframe"]
                )
            elif resolved.alignment.value == "latest_known":
                causal_history = max(
                    int(resolved.max_staleness_seconds or 0),
                    int(resolved.lookback_seconds or 0),
                )
                if causal_history <= 0:
                    raise ValueError(
                        "backtest_dataset_invalid: latest-known facts require causal history"
                    )
                required_start = base_warmup_start - timedelta(
                    seconds=causal_history
                )
                timeframe = (
                    str(input_by_id[indicator_ids[0]]["source_timeframe"])
                    if resolved.timeframe_seconds is not None
                    else None
                )
            else:
                raise ValueError(
                    "backtest_dataset_unsupported_alignment: "
                    f"fact_type={resolved.fact_type} alignment={resolved.alignment.value}"
                )
            merge_series(
                instrument_id=resolved.instrument_id,
                fact_type=resolved.fact_type,
                contract_version=resolved.contract_version,
                timeframe=timeframe,
                timeframe_seconds=resolved.timeframe_seconds,
                dimensions=resolved.dimensions,
                required_start=required_start,
                role="indicator_input",
                indicator_id=indicator_ids[0] if len(indicator_ids) == 1 else None,
                required=resolved.required,
                allow_gaps=resolved.allow_gaps,
                alignment=resolved.alignment.value,
                max_staleness_seconds=resolved.max_staleness_seconds,
                bindings=bindings,
            )
            for indicator_id in indicator_ids[1:]:
                if indicator_id not in merged[
                    (
                        resolved.instrument_id,
                        resolved.fact_type,
                        resolved.contract_version,
                        resolved.timeframe_seconds,
                        tuple(sorted(dict(resolved.dimensions).items())),
                    )
                ]["indicator_ids"]:
                    merged[
                        (
                            resolved.instrument_id,
                            resolved.fact_type,
                            resolved.contract_version,
                            resolved.timeframe_seconds,
                            tuple(sorted(dict(resolved.dimensions).items())),
                        )
                    ]["indicator_ids"].append(indicator_id)

    series = sorted(
        merged.values(),
        key=lambda row: (
            str(row["instrument_id"]),
            str(row["fact_type"]),
            int(row["timeframe_seconds"] or -1),
        ),
    )
    materialization_start = min(
        _utc(item["range_start"], field="range_start") for item in series
    )
    instruments, instrument_config_hash = normalize_backtest_execution_instruments(
        instruments
    )
    execution_config_hash = build_backtest_execution_config_hash(
        bot=bot,
        strategy_identity=strategy_identity,
        instrument_config_hash=instrument_config_hash,
    )
    return {
        "schema_version": BACKTEST_DATASET_PLAN_VERSION,
        "strategy": {
            **strategy_identity,
            "strategy_name": getattr(strategy, "name", None),
            "variant_id": getattr(strategy, "variant_id", None),
            "variant_name": getattr(strategy, "variant_name", None),
        },
        "evaluation_range": {
            "start": iso_utc(start),
            "end_exclusive": iso_utc(end),
        },
        "warmup_range": {
            "start": iso_utc(base_warmup_start),
            "end_exclusive": iso_utc(start),
            "required_bars": warmup_bars,
            "indicator_requirements": warmup_requirements,
            "runtime_derived_atr_bars": 14,
        },
        "materialization_range": {
            "start": iso_utc(materialization_start),
            "end_exclusive": iso_utc(end),
        },
        "decision_range": {
            "start": iso_utc(start),
            "end_exclusive": iso_utc(end),
        },
        "instruments": instruments,
        "instrument_config_hash": instrument_config_hash,
        "execution_config_hash": execution_config_hash,
        "indicator_inputs": indicator_inputs,
        "series": series,
        "execution_assumptions": {
            "run_type": "backtest",
            "execution_mode": bot.get("execution_mode"),
            "execution_semantics": bot.get("execution_semantics"),
            "wallet_config": dict(bot.get("wallet_config") or {}),
            "fee_model": (bot.get("risk_config") or {}).get("fees")
            if isinstance(bot.get("risk_config"), Mapping)
            else None,
            "slippage_model": (bot.get("risk_config") or {}).get("slippage")
            if isinstance(bot.get("risk_config"), Mapping)
            else None,
            "execution_policy_hash": strategy_identity["execution_policy_hash"],
            "instrument_config_hash": instrument_config_hash,
            "execution_config_hash": execution_config_hash,
        },
    }


def dataset_manifest_hash_payload(entry: Mapping[str, Any]) -> dict[str, Any]:
    """Compatibility alias for the core Dataset identity projection."""

    return dataset_series_identity_payload(entry)


def _gap_is_disclosed(
    start: datetime,
    end: datetime,
    quality: Sequence[Mapping[str, Any]],
    *,
    allow_any_classification: bool = False,
) -> bool:
    for row in quality:
        classification = str(row.get("classification") or "").strip().lower()
        if (
            not allow_any_classification
            and classification not in _ALLOWED_DISCLOSED_GAP_CLASSIFICATIONS
        ):
            continue
        evidence_start = _utc(row.get("start"), field="gap.start")
        evidence_end = _utc(row.get("end"), field="gap.end")
        if evidence_start <= start and evidence_end >= end:
            return True
    return False


def validate_frozen_dataset_series(
    *,
    store: MarketDataStore,
    entry: Mapping[str, Any],
    allow_any_recorded_gap: bool = False,
) -> tuple[dict[str, Any], list[dict[str, Any]], list[Any]]:
    series_id = int(entry["series_id"])
    range_start = _utc(entry["range_start"], field="range_start")
    range_end = _utc(entry["range_end"], field="range_end")
    fact_type = str(entry.get("fact_type") or "")
    contract_version = str(entry.get("contract_version") or "")
    timeframe_seconds = (
        int(entry["timeframe_seconds"])
        if entry.get("timeframe_seconds") is not None
        else None
    )
    try:
        contract = get_fact_contract(fact_type)
        contract.validate(
            contract_version=contract_version,
            timeframe_seconds=timeframe_seconds,
        )
        if not contract.dataset_eligible:
            raise ValueError(
                f"fact type is not dataset eligible: {fact_type}"
            )
    except (TypeError, ValueError) as exc:
        raise ValueError(
            "backtest_dataset_contract_mismatch: "
            f"series_id={series_id} contract={entry.get('contract_version')}"
        ) from exc
    record_selection = str(
        dict(entry.get("source_summary") or {}).get("record_selection") or ""
    )
    if record_selection == "all_canonical_revisions.v1":
        records = store.read_dataset_fact_revisions(
            dataset_id=str(entry["dataset_id"]),
            series_id=series_id,
            start=range_start,
            end=range_end,
        )
    else:
        records = store.read_dataset_series(
            dataset_id=str(entry["dataset_id"]),
            series_id=series_id,
            start=range_start,
            end=range_end,
        )
    if len(records) != int(entry["row_count"]):
        raise RuntimeError(
            "backtest_dataset_snapshot_disagreement: "
            f"series_id={series_id} manifest_rows={entry['row_count']} loaded_rows={len(records)}"
        )
    raw_quality = entry.get("quality_evidence")
    if isinstance(raw_quality, list):
        quality = [dict(row) for row in raw_quality]
    elif build_quality_hash([]) == str(entry["quality_hash"]):
        quality = []
    else:
        raise RuntimeError(
            "backtest_dataset_quality_unpinned: Dataset predates exact quality "
            f"materialization series_id={series_id}"
        )
    if not records:
        raise RuntimeError(
            f"backtest_dataset_incomplete: series_id={series_id} contains no facts"
        )
    gaps: list[dict[str, Any]] = []
    if fact_type == CANDLE_FACT_TYPE:
        assert timeframe_seconds is not None
        expected = range_start
        for record in records:
            fact = record.fact
            if fact.open_time < expected:
                raise RuntimeError(
                    f"backtest_dataset_malformed: duplicate or unordered candle series_id={series_id}"
                )
            if fact.open_time > expected:
                if not _gap_is_disclosed(
                    expected,
                    fact.open_time,
                    quality,
                    allow_any_classification=allow_any_recorded_gap,
                ):
                    raise RuntimeError(
                        "backtest_dataset_unacceptable_gap: "
                        f"series_id={series_id} start={iso_utc(expected)} end={iso_utc(fact.open_time)}"
                    )
                gaps.append(
                    {
                        "start": iso_utc(expected),
                        "end": iso_utc(fact.open_time),
                        "classification": "disclosed_closure",
                    }
                )
            expected_close = fact.open_time + timedelta(seconds=timeframe_seconds)
            if fact.close_time != expected_close:
                raise RuntimeError(
                    "backtest_dataset_malformed: candle duration mismatch "
                    f"series_id={series_id} open_time={iso_utc(fact.open_time)}"
                )
            if fact.known_at < fact.close_time:
                raise RuntimeError(
                    "backtest_dataset_malformed: known_at precedes close "
                    f"series_id={series_id} open_time={iso_utc(fact.open_time)}"
                )
            if int(record.market_commit_seq) > int(entry["max_commit_seq"]):
                raise RuntimeError(
                    "backtest_dataset_revision_disagreement: loaded revision exceeds frozen watermark"
                )
            expected = fact.close_time
        if expected < range_end:
            if not _gap_is_disclosed(
                expected,
                range_end,
                quality,
                allow_any_classification=allow_any_recorded_gap,
            ):
                raise RuntimeError(
                    "backtest_dataset_unacceptable_gap: "
                    f"series_id={series_id} start={iso_utc(expected)} end={iso_utc(range_end)}"
                )
            gaps.append(
                {
                    "start": iso_utc(expected),
                    "end": iso_utc(range_end),
                    "classification": "disclosed_closure",
                }
            )
        elif expected > range_end:
            raise RuntimeError(
                "backtest_dataset_malformed: loaded candle extends beyond frozen range"
            )
        loaded_range = {
            "start": iso_utc(records[0].fact.open_time),
            "end_exclusive": iso_utc(records[-1].fact.close_time),
        }
    elif fact_type in {OPEN_INTEREST_FACT_TYPE, FUNDING_RATE_FACT_TYPE}:
        previous_sample: datetime | None = None
        for record in records:
            fact = record.fact
            if previous_sample is not None and fact.sample_time <= previous_sample:
                raise RuntimeError(
                    "backtest_dataset_malformed: duplicate or unordered scheduled fact "
                    f"series_id={series_id}"
                )
            if fact.known_at < fact.sample_time:
                raise RuntimeError(
                    "backtest_dataset_malformed: known_at precedes scheduled sample"
                )
            if int(record.market_commit_seq) > int(entry["max_commit_seq"]):
                raise RuntimeError(
                    "backtest_dataset_revision_disagreement: loaded revision exceeds frozen watermark"
                )
            previous_sample = fact.sample_time
        loaded_range = {
            "first_sample": iso_utc(records[0].fact.sample_time),
            "last_sample": iso_utc(records[-1].fact.sample_time),
            "first_known_at": iso_utc(records[0].fact.known_at),
            "last_known_at": iso_utc(records[-1].fact.known_at),
        }
    else:
        previous_time: datetime | None = None
        version_ids: set[str] = set()
        for record in records:
            effective_at = record_effective_time(record)
            if previous_time is not None and effective_at < previous_time:
                raise RuntimeError(
                    "backtest_dataset_malformed: typed facts are unordered "
                    f"series_id={series_id}"
                )
            if record.fact.known_at < effective_at:
                raise RuntimeError(
                    "backtest_dataset_malformed: known_at precedes effective time"
                )
            if int(record.market_commit_seq) > int(entry["max_commit_seq"]):
                raise RuntimeError(
                    "backtest_dataset_revision_disagreement: loaded revision exceeds frozen watermark"
                )
            version_id = str(getattr(record, "version_id", ""))
            if version_id and version_id in version_ids:
                raise RuntimeError("backtest_dataset_malformed: duplicate typed version")
            if version_id:
                version_ids.add(version_id)
            previous_time = effective_at
        loaded_range = {
            "first_effective_at": iso_utc(record_effective_time(records[0])),
            "last_effective_at": iso_utc(record_effective_time(records[-1])),
            "first_known_at": iso_utc(records[0].fact.known_at),
            "last_known_at": iso_utc(records[-1].fact.known_at),
        }

    if fact_type == MARKET_TRADE_FACT_TYPE:
        quality = [
            *quality,
            *[
                {
                    "classification": (
                        "covered_trade"
                        if record.fact.coverage_interval_id
                        else "uncovered_snapshot_delivery"
                    ),
                    "provider_product_id": record.fact.provider_product_id,
                    "provider_trade_id": record.fact.provider_trade_id,
                    "raw_record_id": record.fact.raw_record_id,
                    "coverage_interval_id": record.fact.coverage_interval_id,
                }
                for record in records
            ],
        ]
    elif fact_type == TRADE_FLOW_FACT_TYPE:
        quality = [
            *quality,
            *[
                {
                    "classification": (
                        "complete"
                        if record.fact.aggregate_complete
                        else "incomplete_trade_coverage"
                    ),
                    "bucket_start": iso_utc(record.fact.bucket_start),
                    "archive_complete": record.fact.archive_complete,
                    "canonicalization_complete": record.fact.canonicalization_complete,
                    "coverage_interval_id": record.fact.coverage_interval_id,
                    "coverage_revision": record.fact.coverage_revision,
                }
                for record in records
            ],
        ]
    elif all(isinstance(record, TypedFeatureRecord) for record in records):
        quality = [
            *quality,
            *[
                {
                    "classification": str(
                        record.quality.get("classification") or "valid"
                    ),
                    "fact_time": iso_utc(record_effective_time(record)),
                    "known_at": iso_utc(record.fact.known_at),
                    "material_hash": record.fact.material_hash,
                    "valid": record.quality.get("valid", True),
                    "reason": record.quality.get("reason"),
                }
                for record in records
            ],
        ]

    series_identity = {
        "identity_key": str(entry["identity_key"]),
        "instrument_id": str(entry["instrument_id"]),
        "fact_type": str(entry["fact_type"]),
        "timeframe_seconds": timeframe_seconds,
        "contract_version": str(entry["contract_version"]),
    }
    dimensions = dict(entry.get("dimensions") or {})
    if dimensions:
        series_identity["dimensions"] = dimensions
    if records and all(
        isinstance(record, CanonicalFactRecord) for record in records
    ):
        material_hash = build_canonical_fact_series_material_hash(
            series_identity=series_identity,
            records=records,
        )
    elif fact_type == CANDLE_FACT_TYPE:
        material_hash = build_candle_material_hash(
            series_identity=series_identity,
            records=records,
        )
    elif fact_type == OPEN_INTEREST_FACT_TYPE:
        material_hash = build_open_interest_material_hash(
            series_identity=series_identity,
            records=records,
        )
    elif fact_type == FUNDING_RATE_FACT_TYPE:
        material_hash = build_funding_rate_material_hash(
            series_identity=series_identity,
            records=records,
        )
    elif contract.uses_exact_numeric_storage:
        material_hash = build_numeric_fact_material_hash(
            series_identity=series_identity,
            records=records,
        )
    elif fact_type == MARKET_TRADE_FACT_TYPE:
        material_hash = build_market_trade_material_hash(
            series_identity=series_identity,
            records=records,
        )
    elif fact_type == TRADE_FLOW_FACT_TYPE:
        material_hash = build_trade_flow_material_hash(
            series_identity=series_identity,
            records=records,
        )
    else:
        material_hash = build_typed_feature_material_hash(
            series_identity=series_identity,
            records=records,
        )
    provenance_hash = (
        build_canonical_fact_provenance_hash(records)
        if records
        and all(isinstance(record, CanonicalFactRecord) for record in records)
        else build_provenance_hash(records)
    )
    quality_hash = build_quality_hash(quality)
    disagreements = {
        "material_hash": (material_hash, str(entry["material_hash"])),
        "provenance_hash": (provenance_hash, str(entry["provenance_hash"])),
        "quality_hash": (quality_hash, str(entry["quality_hash"])),
    }
    mismatches = [
        name for name, (actual, expected_hash) in disagreements.items() if actual != expected_hash
    ]
    if mismatches:
        raise RuntimeError(
            "backtest_dataset_hash_disagreement: "
            f"series_id={series_id} fields={','.join(mismatches)}"
        )
    return (
        {
            **dict(entry),
            "dataset_id": str(entry["dataset_id"]),
            "range_start": iso_utc(range_start),
            "range_end": iso_utc(range_end),
            "row_count": len(records),
            "material_hash": material_hash,
            "provenance_hash": provenance_hash,
            "quality_hash": quality_hash,
            "loaded_range": loaded_range,
            "disclosed_gaps": gaps,
            "quality_evidence": [dict(row) for row in quality],
        },
        [dict(row) for row in quality],
        list(records),
    )


def validate_backtest_dataset(
    *,
    dataset_id: str,
    bot: Mapping[str, Any],
    strategy: Any,
    store: MarketDataStore = market_data_repo,
    indicator_meta_loader: Callable[..., Mapping[str, Any]] = get_instance_meta,
    indicator_input_plan_loader: Callable[..., Mapping[str, Any]] = runtime_input_plan_for_instance,
    instrument_loader: Callable[[str], Mapping[str, Any]] = instrument_service.get_instrument_record,
) -> dict[str, Any]:
    """Admit one exact frozen dataset for one exact backtest request."""

    normalized_id = str(dataset_id or "").strip()
    if not normalized_id:
        raise ValueError("backtest_dataset_required: dataset_id is required")
    plan = derive_backtest_dataset_plan(
        bot=bot,
        strategy=strategy,
        evaluation_start=bot.get("backtest_start"),
        evaluation_end=bot.get("backtest_end"),
        indicator_meta_loader=indicator_meta_loader,
        indicator_input_plan_loader=indicator_input_plan_loader,
        instrument_loader=instrument_loader,
    )
    decision_step_seconds = _timeframe_seconds(
        str(getattr(strategy, "timeframe", "") or "")
    )
    dataset = store.get_dataset(normalized_id)
    if dataset.contract_version != DATASET_IDENTITY_HASH_VERSION:
        raise ValueError(
            "backtest_dataset_contract_mismatch: "
            f"expected={DATASET_IDENTITY_HASH_VERSION} actual={dataset.contract_version}"
        )
    if dataset.dataset_id != f"mds_{dataset.dataset_hash[:32]}":
        raise RuntimeError(
            "backtest_dataset_identity_disagreement: dataset ID does not match hash"
        )

    expected = {
        (
            str(row["instrument_id"]),
            str(row["fact_type"]),
            str(row["contract_version"]),
            (
                int(row["timeframe_seconds"])
                if row.get("timeframe_seconds") is not None
                else None
            ),
            tuple(sorted(dict(row.get("dimensions") or {}).items())),
        ): row
        for row in plan["series"]
    }
    actual = {
        (
            str(row.get("instrument_id") or ""),
            str(row.get("fact_type") or ""),
            str(row.get("contract_version") or ""),
            (
                int(row["timeframe_seconds"])
                if row.get("timeframe_seconds") is not None
                else None
            ),
            tuple(sorted(dict(row.get("dimensions") or {}).items())),
        ): row
        for row in dataset.series
    }
    missing = set(expected) - set(actual)
    required_missing = sorted(
        key for key in missing if bool(expected[key].get("required", True))
    )
    extra = sorted(set(actual) - set(expected))
    if required_missing or extra:
        raise ValueError(
            "backtest_dataset_series_mismatch: "
            f"missing={required_missing} unexpected={extra}"
        )

    admitted: list[dict[str, Any]] = []
    all_quality: list[dict[str, Any]] = []
    unavailable_inputs = [
        {
            "instrument_id": key[0],
            "fact_type": key[1],
            "contract_version": key[2],
            "timeframe_seconds": key[3],
            **({"dimensions": dict(key[4])} if key[4] else {}),
            "required": False,
            "classification": "optional_series_not_frozen",
            "reason": "optional_series_not_frozen",
            "bindings": list(expected[key].get("bindings") or []),
        }
        for key in sorted(missing)
    ]
    raw_normalization_refs = dataset.metadata.get("normalization_refs") or []
    normalization_refs_by_output: dict[int, list[Mapping[str, Any]]] = {}
    for reference in raw_normalization_refs:
        normalization_refs_by_output.setdefault(
            int(reference["output_series_id"]), []
        ).append(reference)
    archive_refs = list(dataset.metadata.get("archive_refs") or [])
    for key, requirement in expected.items():
        if key not in actual:
            continue
        entry = {**dict(actual[key]), "dataset_id": dataset.dataset_id}
        if str(entry.get("fact_type") or "") != str(requirement["fact_type"]):
            raise ValueError(
                f"backtest_dataset_fact_type_mismatch: instrument={key[0]}"
            )
        if str(entry.get("contract_version") or "") != str(
            requirement["contract_version"]
        ):
            raise ValueError(
                f"backtest_dataset_contract_mismatch: instrument={key[0]}"
            )
        if _utc(entry["range_start"], field="range_start") != _utc(
            requirement["range_start"], field="required_range_start"
        ) or _utc(entry["range_end"], field="range_end") != _utc(
            requirement["range_end"], field="required_range_end"
        ):
            raise ValueError(
                "backtest_dataset_range_mismatch: "
                f"instrument={key[0]} fact_type={key[1]} timeframe_seconds={key[3]}"
            )
        verified, quality, records = validate_frozen_dataset_series(
            store=store, entry=entry
        )
        if key[1].startswith("market.normalized."):
            references = normalization_refs_by_output.get(int(entry["series_id"]), [])
            if len(references) != 1:
                raise RuntimeError(
                    "backtest_dataset_normalization_ref_disagreement: "
                    f"series_id={entry['series_id']} count={len(references)}"
                )
            reference = dict(references[0])
            spec_id = str(reference.get("spec_id") or "")
            expected_contract = f"market.normalized_feature.v1/{spec_id}"
            disagreements = {
                "contract_version": (
                    str(entry["contract_version"]),
                    expected_contract,
                ),
                "range_start": (
                    _utc(reference["range_start"], field="normalization.range_start"),
                    _utc(entry["range_start"], field="series.range_start"),
                ),
                "range_end": (
                    _utc(reference["range_end"], field="normalization.range_end"),
                    _utc(entry["range_end"], field="series.range_end"),
                ),
                "material_hash": (
                    str(reference["material_hash"]),
                    str(entry["material_hash"]),
                ),
                "provenance_hash": (
                    str(reference["provenance_hash"]),
                    str(entry["provenance_hash"]),
                ),
                "quality_hash": (
                    str(reference["quality_hash"]),
                    str(entry["quality_hash"]),
                ),
                "row_count": (int(reference["row_count"]), len(records)),
            }
            mismatches = [
                name
                for name, (actual_value, expected_value) in disagreements.items()
                if actual_value != expected_value
            ]
            if mismatches:
                raise RuntimeError(
                    "backtest_dataset_normalization_ref_disagreement: "
                    f"series_id={entry['series_id']} fields={','.join(mismatches)}"
                )
            source_fingerprints = dict(
                reference.get("source_dataset_fingerprints") or {}
            )
            source_series_ids = [
                int(value) for value in (reference.get("source_series_ids") or [])
            ]
            if not source_series_ids or set(source_fingerprints) != {
                str(value) for value in source_series_ids
            }:
                raise RuntimeError(
                    "backtest_dataset_normalization_ref_disagreement: "
                    f"series_id={entry['series_id']} fields=source_series_ids"
                )
            source_entries = {
                int(row["series_id"]): row for row in dataset.series
            }
            source_count = 0
            for source_series_id in source_series_ids:
                source_entry = source_entries.get(source_series_id)
                if source_entry is None:
                    raise RuntimeError(
                        "backtest_dataset_normalization_source_missing: "
                        f"output_series_id={entry['series_id']} "
                        f"source_series_id={source_series_id}"
                    )
                source_count += int(source_entry["row_count"])
                if (
                    str(source_entry["material_hash"])
                    != str(source_fingerprints[str(source_series_id)])
                    or _utc(source_entry["range_start"], field="source.range_start")
                    > _utc(reference["input_range_start"], field="normalization.input_start")
                    or _utc(source_entry["range_end"], field="source.range_end")
                    <= _utc(reference["input_range_end"], field="normalization.input_end")
                ):
                    raise RuntimeError(
                        "backtest_dataset_normalization_source_disagreement: "
                        f"output_series_id={entry['series_id']} "
                        f"source_series_id={source_series_id}"
                    )
            if source_count != int(reference["input_count"]):
                raise RuntimeError(
                    "backtest_dataset_normalization_ref_disagreement: "
                    f"series_id={entry['series_id']} fields=input_count"
                )
            verified["normalization_ref"] = reference
        if get_fact_contract(key[1]).archive_policy == "raw_required":
            if not archive_refs:
                raise RuntimeError(
                    "backtest_dataset_archive_ref_disagreement: "
                    f"series_id={entry['series_id']} has no frozen raw archive references"
                )
            verified["archive_ref_count"] = len(archive_refs)
        verified["roles"] = list(requirement.get("roles") or [])
        verified["indicator_ids"] = list(requirement.get("indicator_ids") or [])
        verified["bindings"] = list(requirement.get("bindings") or [])
        verified["required"] = bool(requirement.get("required", True))
        verified["allow_gaps"] = bool(requirement.get("allow_gaps", False))
        verified["alignment"] = requirement.get("alignment")
        verified["max_staleness_seconds"] = requirement.get(
            "max_staleness_seconds"
        )
        if requirement.get("dimensions"):
            verified["dimensions"] = dict(requirement["dimensions"])
        if (
            str(requirement.get("alignment") or "") == "exact_interval"
            and key[1] != CANDLE_FACT_TYPE
        ):
            timeframe = int(requirement.get("timeframe_seconds") or 0)
            if timeframe <= 0:
                raise ValueError(
                    "backtest_dataset_invalid: exact-interval fact requires timeframe"
                )
            step = timedelta(seconds=timeframe)
            by_effective = {
                record_effective_time(record): record for record in records
            }
            decision = _utc(entry["range_start"], field="range_start")
            decision_end = _utc(entry["range_end"], field="range_end")
            unavailable: list[dict[str, Any]] = []
            while decision < decision_end:
                record = by_effective.get(decision)
                reason = None
                if record is None:
                    reason = "missing_interval"
                elif record.fact.known_at > decision + step:
                    reason = "not_known_by_interval_close"
                elif not _record_has_usable_quality(record):
                    reason = "invalid_source_fact"
                if reason:
                    unavailable.append(
                        {"decision_time": iso_utc(decision + step), "reason": reason}
                    )
                decision += step
            verified["causal_coverage"] = {
                "decision_count": int(
                    (decision_end - _utc(entry["range_start"], field="range_start"))
                    .total_seconds()
                    // timeframe
                ),
                "unavailable_count": len(unavailable),
                "timeframe_seconds": timeframe,
                "first_unavailable": unavailable[0] if unavailable else None,
            }
            if (
                unavailable
                and bool(requirement.get("required", True))
                and not bool(requirement.get("allow_gaps", False))
            ):
                raise RuntimeError(
                    "backtest_dataset_required_fact_unavailable: "
                    f"series_id={entry['series_id']} count={len(unavailable)} "
                    f"first={unavailable[0]}"
                )
            if unavailable:
                quality.append(
                    {
                        "classification": "exact_interval_unavailable",
                        "series_id": int(entry["series_id"]),
                        "expected_count": len(unavailable),
                        "observed_count": 0,
                        "evidence": verified["causal_coverage"],
                    }
                )
        elif str(requirement.get("alignment") or "") == "latest_known":
            max_staleness = int(requirement.get("max_staleness_seconds") or 0)
            if max_staleness <= 0:
                raise ValueError(
                    "backtest_dataset_invalid: latest-known max staleness must be positive"
                )
            decision = _utc(
                plan["warmup_range"]["start"], field="warmup_range.start"
            )
            decision_end = _utc(
                plan["decision_range"]["end_exclusive"],
                field="decision_range.end_exclusive",
            )
            ordered = sorted(
                records,
                key=lambda record: (
                    record.fact.known_at,
                    int(record.market_commit_seq),
                ),
            )
            cursor = 0
            latest = None
            unavailable: list[dict[str, Any]] = []
            while decision < decision_end:
                while (
                    cursor < len(ordered)
                    and ordered[cursor].fact.known_at <= decision
                ):
                    latest = ordered[cursor]
                    cursor += 1
                reason = None
                if latest is None:
                    reason = "no_known_fact"
                elif (
                    decision - latest.fact.known_at
                    > timedelta(seconds=max_staleness)
                ):
                    reason = "stale"
                elif not _record_has_usable_quality(latest):
                    reason = "invalid_source_fact"
                if reason:
                    unavailable.append(
                        {
                            "decision_time": iso_utc(decision),
                            "reason": reason,
                            "latest_known_at": (
                                iso_utc(latest.fact.known_at)
                                if latest is not None
                                else None
                            ),
                        }
                    )
                decision += timedelta(seconds=decision_step_seconds)
            verified["causal_coverage"] = {
                "decision_count": int(
                    (
                        _utc(
                            plan["decision_range"]["end_exclusive"],
                            field="decision_range.end_exclusive",
                        )
                        - _utc(
                            plan["warmup_range"]["start"],
                            field="warmup_range.start",
                        )
                    ).total_seconds()
                    // decision_step_seconds
                ),
                "unavailable_count": len(unavailable),
                "max_staleness_seconds": max_staleness,
                "first_unavailable": unavailable[0] if unavailable else None,
            }
            if unavailable and bool(requirement.get("required", True)):
                raise RuntimeError(
                    "backtest_dataset_required_fact_unavailable: "
                    f"series_id={entry['series_id']} count={len(unavailable)} "
                    f"first={unavailable[0]}"
                )
            if unavailable:
                derived = {
                    "classification": "optional_fact_unavailable",
                    "series_id": int(entry["series_id"]),
                    "expected_count": len(unavailable),
                    "observed_count": 0,
                    "evidence": verified["causal_coverage"],
                }
                quality.append(derived)
        admitted.append(verified)
        all_quality.extend(quality)

    manifest_payload = [dataset_manifest_hash_payload(row) for row in admitted]
    reconstructed_hash = build_dataset_identity_hash(manifest_payload)
    if reconstructed_hash != dataset.dataset_hash:
        raise RuntimeError(
            "backtest_dataset_identity_disagreement: reconstructed dataset hash differs"
        )
    binding = {
        "schema_version": BACKTEST_DATASET_BINDING_VERSION,
        "dataset_contract_version": DATASET_IDENTITY_HASH_VERSION,
        "dataset_id": dataset.dataset_id,
        "dataset_hash": dataset.dataset_hash,
        "max_commit_seq": dataset.max_commit_seq,
        "strategy_id": plan["strategy"]["strategy_id"],
        "strategy_hash": plan["strategy"]["strategy_hash"],
        "effective_strategy_config_hash": plan["strategy"].get(
            "effective_strategy_config_hash"
        ),
        "indicator_config_hash": plan["strategy"]["indicator_config_hash"],
        "execution_policy_hash": plan["strategy"]["execution_policy_hash"],
        "instrument_config_hash": plan["instrument_config_hash"],
        "execution_config_hash": plan["execution_config_hash"],
        "instruments": list(plan["instruments"]),
        "evaluation_range": dict(plan["evaluation_range"]),
        "warmup_range": dict(plan["warmup_range"]),
        "materialization_range": dict(plan["materialization_range"]),
        "decision_range": dict(plan["decision_range"]),
        "series": admitted,
        "quality": {
            "status": (
                "ready_with_caveats"
                if all_quality or unavailable_inputs
                else "ready"
            ),
            "evidence_count": len(all_quality) + len(unavailable_inputs),
            "classifications": dict(
                sorted(
                    Counter(
                        str(row.get("classification") or "unknown")
                        for row in [*all_quality, *unavailable_inputs]
                    ).items()
                )
            ),
        },
        "unavailable_inputs": unavailable_inputs,
        "provider_call_performed": False,
        "validation_status": "ready",
    }
    return normalize_backtest_dataset_binding(binding)



def _record_has_usable_quality(record: Any) -> bool:
    if not isinstance(record, TypedFeatureRecord):
        return True
    if record.quality.get("valid") is False:
        return False
    status = getattr(record.fact, "status", None)
    if status is None:
        return True
    return str(getattr(status, "value", status)) == "valid"


def _coalesce_missing_points(
    points: Sequence[datetime], *, step_seconds: int
) -> list[dict[str, str]]:
    ranges: list[dict[str, str]] = []
    step = timedelta(seconds=int(step_seconds))
    for point in points:
        if ranges and _utc(ranges[-1]["end"], field="missing.end") == point:
            ranges[-1]["end"] = iso_utc(point + step)
        else:
            ranges.append(
                {"start": iso_utc(point), "end": iso_utc(point + step)}
            )
    return ranges


def _generic_fact_coverage(
    requirement: Mapping[str, Any],
    *,
    decision_start: datetime,
    decision_end: datetime,
    decision_step: int,
    store: MarketDataStore,
) -> dict[str, Any]:
    fact_type = str(requirement["fact_type"])
    timeframe_seconds = (
        int(requirement["timeframe_seconds"])
        if requirement.get("timeframe_seconds") is not None
        else None
    )
    contract = get_fact_contract(fact_type)
    revision_records: Sequence[Any] = ()
    try:
        series_id = store.resolve_series_id(
            instrument_id=str(requirement["instrument_id"]),
            fact_type=fact_type,
            timeframe_seconds=timeframe_seconds,
            contract_version=str(requirement["contract_version"]),
            dimensions=dict(requirement.get("dimensions") or {}),
        )
        range_start = _utc(requirement["range_start"], field="range_start")
        range_end = _utc(requirement["range_end"], field="range_end")
        if contract.uses_exact_numeric_storage:
            revision_records = store.read_numeric_fact_revisions(
                series_id=series_id,
                start=range_start,
                end=range_end,
            )
            records = list(
                causal_numeric_fact_records(
                    revision_records,
                    evaluation_time=decision_end,
                )
            )
        else:
            records = store.read_series_records(
                series_id=series_id,
                start=range_start,
                end=range_end,
            )
    except ValueError:
        series_id = None
        records = []

    alignment = str(requirement.get("alignment") or "")
    missing_points: list[datetime] = []
    if alignment == "exact_interval":
        if timeframe_seconds is None or timeframe_seconds <= 0:
            raise ValueError(
                "backtest_dataset_invalid: exact-interval fact requires timeframe"
            )
        step = timedelta(seconds=timeframe_seconds)
        by_effective = {
            record_effective_time(record): record
            for record in records
        }
        point = _utc(requirement["range_start"], field="range_start")
        range_end = _utc(requirement["range_end"], field="range_end")
        while point < range_end:
            record = by_effective.get(point)
            if (
                record is None
                or record.fact.known_at > point + step
                or not _record_has_usable_quality(record)
            ):
                missing_points.append(point)
            point += step
        coverage_step = timeframe_seconds
        decision_count = int(
            (range_end - _utc(requirement["range_start"], field="range_start"))
            .total_seconds()
            // timeframe_seconds
        )
    elif alignment == "latest_known":
        max_staleness = int(requirement.get("max_staleness_seconds") or 0)
        if max_staleness <= 0:
            raise ValueError(
                "backtest_dataset_invalid: latest-known fact requires max staleness"
            )
        point = decision_start
        if contract.uses_exact_numeric_storage:
            while point < decision_end:
                selected = latest_known_record(
                    causal_numeric_fact_records(
                        revision_records,
                        evaluation_time=point,
                    ),
                    evaluation_time=point,
                    max_staleness_seconds=max_staleness,
                )
                if isinstance(selected, UnavailableMarketData):
                    missing_points.append(point)
                point += timedelta(seconds=decision_step)
        else:
            ordered = sorted(
                records,
                key=lambda record: (
                    record.fact.known_at,
                    int(record.market_commit_seq),
                ),
            )
            cursor = 0
            latest = None
            while point < decision_end:
                while cursor < len(ordered) and ordered[cursor].fact.known_at <= point:
                    latest = ordered[cursor]
                    cursor += 1
                if (
                    latest is None
                    or point - latest.fact.known_at > timedelta(seconds=max_staleness)
                    or not _record_has_usable_quality(latest)
                ):
                    missing_points.append(point)
                point += timedelta(seconds=decision_step)
        coverage_step = decision_step
        decision_count = int(
            (decision_end - decision_start).total_seconds() // decision_step
        )
    else:
        raise ValueError(
            "backtest_dataset_unsupported_alignment: "
            f"fact_type={fact_type} alignment={alignment or '<missing>'}"
        )

    return {
        "schema_version": "market_fact_coverage.v1",
        "series_id": series_id,
        "row_count": len(records),
        "decision_count": decision_count,
        "missing_decision_count": len(missing_points),
        "missing_ranges": _coalesce_missing_points(
            missing_points, step_seconds=coverage_step
        ),
        "max_staleness_seconds": requirement.get("max_staleness_seconds"),
        "provider_call_performed": False,
    }


def _coverage_for_plan(
    plan: Mapping[str, Any],
    *,
    coverage_loader: Callable[[str, str, str, str], Mapping[str, Any]],
    store: MarketDataStore,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    primary_steps = [
        int(row["timeframe_seconds"])
        for row in plan["series"]
        if "strategy_primary_bars" in list(row.get("roles") or [])
    ]
    if not primary_steps:
        raise ValueError("backtest_dataset_invalid: primary decision series is missing")
    decision_step = min(primary_steps)
    decision_start = _utc(
        plan["decision_range"]["start"], field="decision_range.start"
    )
    decision_end = _utc(
        plan["decision_range"]["end_exclusive"],
        field="decision_range.end_exclusive",
    )
    for requirement in plan["series"]:
        if str(requirement["fact_type"]) == CANDLE_FACT_TYPE:
            payload = dict(
                coverage_loader(
                    str(requirement["instrument_id"]),
                    str(requirement["range_start"]),
                    str(requirement["range_end"]),
                    str(requirement["timeframe"]),
                )
            )
        elif str(requirement["fact_type"]) == OPEN_INTEREST_FACT_TYPE:
            try:
                series_id = store.resolve_series_id(
                    instrument_id=str(requirement["instrument_id"]),
                    fact_type=OPEN_INTEREST_FACT_TYPE,
                    timeframe_seconds=None,
                    contract_version=OPEN_INTEREST_FACT_VERSION,
                )
                records = store.read_open_interest(
                    series_id=series_id,
                    start=_utc(requirement["range_start"], field="range_start"),
                    end=_utc(requirement["range_end"], field="range_end"),
                )
            except ValueError:
                series_id = None
                records = []
            max_staleness = int(requirement.get("max_staleness_seconds") or 0)
            ordered = sorted(
                records,
                key=lambda record: (
                    record.fact.known_at,
                    int(record.market_commit_seq),
                ),
            )
            cursor = 0
            latest = None
            missing_points: list[datetime] = []
            decision = decision_start
            while decision < decision_end:
                while (
                    cursor < len(ordered)
                    and ordered[cursor].fact.known_at <= decision
                ):
                    latest = ordered[cursor]
                    cursor += 1
                if latest is None or (
                    decision - latest.fact.known_at
                    > timedelta(seconds=max_staleness)
                ):
                    missing_points.append(decision)
                decision += timedelta(seconds=decision_step)
            missing_ranges: list[dict[str, str]] = []
            for point in missing_points:
                if (
                    missing_ranges
                    and _utc(missing_ranges[-1]["end"], field="missing.end")
                    == point
                ):
                    missing_ranges[-1]["end"] = iso_utc(
                        point + timedelta(seconds=decision_step)
                    )
                else:
                    missing_ranges.append(
                        {
                            "start": iso_utc(point),
                            "end": iso_utc(
                                point + timedelta(seconds=decision_step)
                            ),
                        }
                    )
            payload = {
                "schema_version": "market_fact_coverage.v1",
                "series_id": series_id,
                "row_count": len(records),
                "decision_count": int(
                    (decision_end - decision_start).total_seconds()
                    // decision_step
                ),
                "missing_decision_count": len(missing_points),
                "missing_ranges": missing_ranges,
                "max_staleness_seconds": max_staleness,
                "provider_call_performed": False,
            }
        else:
            payload = _generic_fact_coverage(
                requirement,
                decision_start=decision_start,
                decision_end=decision_end,
                decision_step=decision_step,
                store=store,
            )
        rows.append({**dict(requirement), "coverage": payload})
    return rows


def _numeric_acquisition_context(
    raw: Mapping[str, Any] | None,
) -> tuple[
    dict[
        tuple[str, str, str, tuple[tuple[str, str], ...]],
        tuple[str, str],
    ],
    NumericAcquisitionAuthorization,
    NumericAcquisitionBudget,
]:
    """Validate explicit pre-freeze numeric acquisition authority and bindings."""

    if not isinstance(raw, Mapping):
        raise RuntimeError(
            "backtest_dataset_numeric_acquisition_required: missing explicit "
            "numeric_acquisition configuration"
        )
    allowed = {"bindings", "authorization", "budget"}
    unexpected = sorted(set(raw) - allowed)
    if unexpected:
        raise ValueError(
            "backtest_dataset_numeric_acquisition_invalid: unexpected fields="
            + ",".join(unexpected)
        )
    raw_authorization = raw.get("authorization")
    raw_budget = raw.get("budget")
    raw_bindings = raw.get("bindings")
    if not isinstance(raw_authorization, Mapping):
        raise ValueError(
            "backtest_dataset_numeric_acquisition_invalid: authorization is required"
        )
    if not isinstance(raw_budget, Mapping):
        raise ValueError(
            "backtest_dataset_numeric_acquisition_invalid: budget is required"
        )
    if not isinstance(raw_bindings, Sequence) or isinstance(
        raw_bindings, (str, bytes)
    ) or not raw_bindings:
        raise ValueError(
            "backtest_dataset_numeric_acquisition_invalid: bindings are required"
        )
    if set(raw_authorization) != {"network_allowed", "actor", "reason"}:
        raise ValueError(
            "backtest_dataset_numeric_acquisition_invalid: authorization fields "
            "must be network_allowed, actor, and reason"
        )
    if not isinstance(raw_authorization["network_allowed"], bool):
        raise ValueError(
            "backtest_dataset_numeric_acquisition_invalid: network_allowed must "
            "be boolean"
        )
    required_budget = {"max_requests", "max_logs", "max_blocks"}
    if not required_budget.issubset(raw_budget) or set(raw_budget) - (
        required_budget | {"max_retries"}
    ):
        raise ValueError(
            "backtest_dataset_numeric_acquisition_invalid: budget fields must be "
            "max_requests, max_logs, max_blocks, and optional max_retries"
        )
    authorization = NumericAcquisitionAuthorization(
        network_allowed=raw_authorization["network_allowed"],
        actor=str(raw_authorization.get("actor") or "").strip(),
        reason=str(raw_authorization.get("reason") or "").strip(),
    )
    budget = NumericAcquisitionBudget(
        max_requests=int(raw_budget.get("max_requests") or 0),
        max_logs=int(raw_budget.get("max_logs") or 0),
        max_blocks=int(raw_budget.get("max_blocks") or 0),
        max_retries=int(raw_budget.get("max_retries", 2)),
    )
    indexed: dict[
        tuple[str, str, str, tuple[tuple[str, str], ...]],
        tuple[str, str],
    ] = {}
    for item in raw_bindings:
        if not isinstance(item, Mapping) or set(item) != {
            "manifest_path",
            "binding_id",
        }:
            raise ValueError(
                "backtest_dataset_numeric_acquisition_invalid: each binding must "
                "declare only manifest_path and binding_id"
            )
        manifest_path = str(item.get("manifest_path") or "").strip()
        binding_id = str(item.get("binding_id") or "").strip()
        manifest = load_numeric_fact_manifest(manifest_path)
        binding = manifest.binding(binding_id, require_enabled=True)
        key = (
            binding.instrument_id,
            binding.fact_type,
            binding.contract_version,
            tuple(sorted(dict(binding.dimensions).items())),
        )
        if key in indexed:
            raise ValueError(
                "backtest_dataset_numeric_acquisition_invalid: duplicate binding "
                f"for instrument={key[0]} fact_type={key[1]} dimensions={dict(key[3])}"
            )
        indexed[key] = (manifest_path, binding_id)
    return indexed, authorization, budget


def prepare_backtest_dataset(
    *,
    bot: Mapping[str, Any],
    strategy: Any,
    evaluation_start: Any,
    evaluation_end: Any,
    acquire_missing: bool,
    created_by: str | None = None,
    numeric_acquisition: Mapping[str, Any] | None = None,
    store: MarketDataStore = market_data_repo,
    ingestor: HistoricalCandleIngestor = historical_candle_ingestor,
    coverage_loader: Callable[[str, str, str, str], Mapping[str, Any]] = preflight_candle_coverage_by_instrument,
    indicator_meta_loader: Callable[..., Mapping[str, Any]] = get_instance_meta,
    indicator_input_plan_loader: Callable[..., Mapping[str, Any]] = runtime_input_plan_for_instance,
    instrument_loader: Callable[[str], Mapping[str, Any]] = instrument_service.get_instrument_record,
    numeric_acquirer: NumericFactAcquisitionService = numeric_fact_acquisition_service,
) -> dict[str, Any]:
    """Prepare missing facts explicitly, freeze them, then admit the result."""

    prepared_instruments: dict[str, dict[str, Any]] = {}

    def prepared_instrument_loader(instrument_id: str) -> Mapping[str, Any]:
        normalized_id = str(instrument_id or "").strip()
        if normalized_id not in prepared_instruments:
            prepared_instruments[normalized_id] = deepcopy(
                dict(instrument_loader(normalized_id))
            )
        return deepcopy(prepared_instruments[normalized_id])

    timings: dict[str, dict[str, Any]] = {}
    phase = _phase_start()
    windowed_bot = {
        **dict(bot),
        "run_type": "backtest",
        "backtest_start": iso_utc(evaluation_start, field="evaluation_start"),
        "backtest_end": iso_utc(evaluation_end, field="evaluation_end"),
    }
    plan = derive_backtest_dataset_plan(
        bot=windowed_bot,
        strategy=strategy,
        evaluation_start=evaluation_start,
        evaluation_end=evaluation_end,
        indicator_meta_loader=indicator_meta_loader,
        indicator_input_plan_loader=indicator_input_plan_loader,
        instrument_loader=prepared_instrument_loader,
    )
    _phase_finish(
        timings,
        "requirement_resolution",
        phase,
        series_count=len(plan["series"]),
        required_warmup_bars=plan["warmup_range"]["required_bars"],
    )

    phase = _phase_start()
    coverage_before = _coverage_for_plan(
        plan, coverage_loader=coverage_loader, store=store
    )
    _phase_finish(
        timings,
        "coverage_inspection",
        phase,
        series_count=len(coverage_before),
        rows_available=sum(
            int((row["coverage"] or {}).get("row_count") or 0)
            for row in coverage_before
        ),
    )

    missing = [
        (row, gap)
        for row in coverage_before
        if bool(row.get("required", True)) and not bool(row.get("allow_gaps", False))
        for gap in list((row["coverage"] or {}).get("missing_ranges") or [])
    ]
    if missing and not acquire_missing:
        raise RuntimeError(
            "backtest_dataset_preparation_incomplete: canonical storage is missing "
            f"{len(missing)} range(s); rerun with explicit acquisition enabled"
        )

    acquisitions: list[dict[str, Any]] = []
    incomplete_acquisitions: list[dict[str, Any]] = []
    numeric_context: tuple[
        dict[
            tuple[str, str, str, tuple[tuple[str, str], ...]],
            tuple[str, str],
        ],
        NumericAcquisitionAuthorization,
        NumericAcquisitionBudget,
    ] | None = None
    numeric_series_requested: set[
        tuple[str, str, str, tuple[tuple[str, str], ...]]
    ] = set()
    numeric_budget_remaining: list[int] | None = None
    phase = _phase_start()
    for row, gap in missing:
        fact_type = str(row["fact_type"])
        if fact_type == CANDLE_FACT_TYPE:
            instrument = dict(prepared_instrument_loader(str(row["instrument_id"])))
            result = ingestor.ingest_by_instrument(
                instrument,
                start=gap["start"],
                end=gap["end"],
                interval=str(row["timeframe"]),
            )
            acquisitions.append(
                {
                    "instrument_id": row["instrument_id"],
                    "fact_type": fact_type,
                    "timeframe": row["timeframe"],
                    "start": iso_utc(gap["start"], field="gap.start"),
                    "end_exclusive": iso_utc(gap["end"], field="gap.end"),
                    "source_id": result.source_id,
                    "series_id": result.series_id,
                    "ingestion_run_id": result.outcome.ingestion_run_id,
                    "requested_count": result.outcome.requested_count,
                    "inserted_count": result.outcome.inserted_count,
                    "corrected_count": result.outcome.corrected_count,
                    "invalidated_count": 0,
                    "noop_count": result.outcome.noop_count,
                    "gap_evidence_count": result.gap_evidence_count,
                    "complete": True,
                }
            )
            continue
        contract = get_fact_contract(fact_type)
        if not contract.uses_exact_numeric_storage:
            raise RuntimeError(
                "backtest_dataset_acquisition_unsupported: historical acquisition "
                f"is unavailable for fact_type={row['fact_type']}; allow the collector "
                "to accumulate venue observations before freezing this range"
            )
        if numeric_context is None:
            numeric_context = _numeric_acquisition_context(numeric_acquisition)
            numeric_budget_remaining = [
                numeric_context[2].max_requests,
                numeric_context[2].max_logs,
                numeric_context[2].max_blocks,
            ]
        binding_index, authorization, budget = numeric_context
        binding_key = (
            str(row["instrument_id"]),
            fact_type,
            str(row["contract_version"]),
            tuple(sorted(dict(row.get("dimensions") or {}).items())),
        )
        binding_ref = binding_index.get(binding_key)
        if binding_ref is None:
            raise RuntimeError(
                "backtest_dataset_numeric_binding_missing: "
                f"instrument_id={binding_key[0]} fact_type={binding_key[1]} "
                f"dimensions={dict(binding_key[3])}"
            )
        if binding_key in numeric_series_requested:
            continue
        numeric_series_requested.add(binding_key)
        acquisition_start = _utc(row["range_start"], field="range_start")
        acquisition_end = _utc(row["range_end"], field="range_end")
        assert numeric_budget_remaining is not None
        if any(value <= 0 for value in numeric_budget_remaining):
            raise RuntimeError(
                "backtest_dataset_numeric_budget_exhausted: no budget remains "
                f"before instrument_id={binding_key[0]} fact_type={binding_key[1]}"
            )
        call_budget = NumericAcquisitionBudget(
            max_requests=numeric_budget_remaining[0],
            max_logs=numeric_budget_remaining[1],
            max_blocks=numeric_budget_remaining[2],
            max_retries=budget.max_retries,
        )
        numeric_result = numeric_acquirer.acquire_history(
            manifest_path=binding_ref[0],
            binding_id=binding_ref[1],
            start=acquisition_start,
            end=acquisition_end,
            authorization=authorization,
            budget=call_budget,
            repair=False,
        )
        used_budget = [
            numeric_result.requests_used,
            numeric_result.logs_used,
            numeric_result.blocks_scanned,
        ]
        if any(
            used > available
            for used, available in zip(used_budget, numeric_budget_remaining)
        ):
            raise RuntimeError(
                "backtest_dataset_numeric_budget_disagreement: "
                f"used={used_budget} available={numeric_budget_remaining}"
            )
        numeric_budget_remaining = [
            available - used
            for used, available in zip(numeric_budget_remaining, used_budget)
        ]
        acquisition = {
            "instrument_id": row["instrument_id"],
            "fact_type": fact_type,
            "contract_version": row["contract_version"],
            "dimensions": dict(row.get("dimensions") or {}),
            "start": iso_utc(acquisition_start),
            "end_exclusive": iso_utc(acquisition_end),
            "manifest_id": numeric_result.manifest_id,
            "binding_id": numeric_result.binding_id,
            "source_id": numeric_result.source_id,
            "series_id": numeric_result.series_id,
            "requested_count": (
                numeric_result.inserted_count
                + numeric_result.corrected_count
                + numeric_result.noop_count
            ),
            "inserted_count": numeric_result.inserted_count,
            "corrected_count": numeric_result.corrected_count,
            "invalidated_count": numeric_result.invalidated_count,
            "noop_count": numeric_result.noop_count,
            "gap_evidence_count": numeric_result.gap_count,
            "requests_used": numeric_result.requests_used,
            "logs_used": numeric_result.logs_used,
            "blocks_scanned": numeric_result.blocks_scanned,
            "complete": numeric_result.complete,
            "cached": not numeric_result.acquired_ranges,
        }
        acquisitions.append(acquisition)
        if not numeric_result.complete:
            incomplete_acquisitions.append(acquisition)
    _phase_finish(
        timings,
        "provider_acquisition",
        phase,
        request_count=len(acquisitions),
        rows_requested=sum(int(row["requested_count"]) for row in acquisitions),
        rows_inserted=sum(int(row["inserted_count"]) for row in acquisitions),
        rows_corrected=sum(int(row["corrected_count"]) for row in acquisitions),
    )

    if incomplete_acquisitions:
        raise RuntimeError(
            "backtest_dataset_preparation_incomplete: numeric acquisition returned "
            f"partial coverage acquisitions={incomplete_acquisitions}"
        )

    phase = _phase_start()
    coverage_after = _coverage_for_plan(
        plan, coverage_loader=coverage_loader, store=store
    )
    remaining = [
        (row["instrument_id"], row["fact_type"], row.get("timeframe"), gap)
        for row in coverage_after
        if bool(row.get("required", True)) and not bool(row.get("allow_gaps", False))
        for gap in list((row["coverage"] or {}).get("missing_ranges") or [])
    ]
    if remaining:
        raise RuntimeError(
            "backtest_dataset_preparation_incomplete: missing material remains "
            f"after acquisition ranges={remaining}"
        )
    _phase_finish(
        timings,
        "ingestion_validation",
        phase,
        series_count=len(coverage_after),
        rows_available=sum(
            int((row["coverage"] or {}).get("row_count") or 0)
            for row in coverage_after
        ),
    )

    phase = _phase_start()
    requests: list[DatasetSeriesRequest] = []
    optional_unavailable: list[dict[str, Any]] = []
    coverage_by_series = {
        (
            str(item["instrument_id"]),
            str(item["fact_type"]),
            str(item["contract_version"]),
            item.get("timeframe_seconds"),
            tuple(sorted(dict(item.get("dimensions") or {}).items())),
        ): dict(item.get("coverage") or {})
        for item in coverage_after
    }
    for row in plan["series"]:
        coverage = coverage_by_series[
            (
                str(row["instrument_id"]),
                str(row["fact_type"]),
                str(row["contract_version"]),
                row.get("timeframe_seconds"),
                tuple(sorted(dict(row.get("dimensions") or {}).items())),
            )
        ]
        if not bool(row.get("required", True)) and int(
            coverage.get("row_count") or 0
        ) == 0:
            optional_unavailable.append(
                {
                    "instrument_id": row["instrument_id"],
                    "fact_type": row["fact_type"],
                    "contract_version": row["contract_version"],
                    "reason": "optional_series_has_no_facts",
                }
            )
            continue
        try:
            series_id = store.resolve_series_id(
                instrument_id=str(row["instrument_id"]),
                fact_type=str(row["fact_type"]),
                timeframe_seconds=(
                    int(row["timeframe_seconds"])
                    if row.get("timeframe_seconds") is not None
                    else None
                ),
                contract_version=str(row["contract_version"]),
                dimensions=dict(row.get("dimensions") or {}),
            )
        except ValueError as exc:
            if bool(row.get("required", True)):
                raise
            optional_unavailable.append(
                {
                    "instrument_id": row["instrument_id"],
                    "fact_type": row["fact_type"],
                    "contract_version": row["contract_version"],
                    "reason": "optional_series_missing",
                    "error": str(exc),
                }
            )
            continue
        requests.append(
            DatasetSeriesRequest(
                series_id=series_id,
                start=_utc(row["range_start"], field="range_start"),
                end=_utc(row["range_end"], field="range_end"),
            )
        )
    dataset = store.freeze_dataset(
        requests,
        name=(
            f"{getattr(strategy, 'name', None) or getattr(strategy, 'id', 'strategy')} "
            f"{plan['evaluation_range']['start']}..{plan['evaluation_range']['end_exclusive']}"
        ),
        purpose="backtest",
        created_by=created_by,
        metadata={
            "schema_version": "backtest_dataset_preparation_request.v1",
            "strategy": dict(plan["strategy"]),
            "evaluation_range": dict(plan["evaluation_range"]),
            "warmup_range": dict(plan["warmup_range"]),
            "materialization_range": dict(plan["materialization_range"]),
            "optional_unavailable": optional_unavailable,
        },
    )
    _phase_finish(
        timings,
        "dataset_hashing_and_freezing",
        phase,
        dataset_id=dataset.dataset_id,
        dataset_hash=dataset.dataset_hash,
        series_count=len(dataset.series),
        rows_frozen=sum(int(row["row_count"]) for row in dataset.series),
    )

    phase = _phase_start()
    binding = validate_backtest_dataset(
        dataset_id=dataset.dataset_id,
        bot=windowed_bot,
        strategy=strategy,
        store=store,
        indicator_meta_loader=indicator_meta_loader,
        indicator_input_plan_loader=indicator_input_plan_loader,
        instrument_loader=prepared_instrument_loader,
    )
    _phase_finish(
        timings,
        "dataset_admission",
        phase,
        dataset_id=dataset.dataset_id,
        rows_validated=sum(int(row["row_count"]) for row in binding["series"]),
        quality_status=binding["quality"]["status"],
    )
    return {
        "schema_version": "backtest_dataset_preparation.v1",
        "status": "ready",
        "plan": plan,
        "coverage_before": coverage_before,
        "acquisitions": acquisitions,
        "coverage_after": coverage_after,
        "dataset": {
            "dataset_id": dataset.dataset_id,
            "dataset_hash": dataset.dataset_hash,
            "max_commit_seq": dataset.max_commit_seq,
            "contract_version": dataset.contract_version,
            "reused_content_identity": bool(dataset.reused_existing),
        },
        "binding": binding,
        "performance": {
            "schema_version": "backtest_preparation_performance.v1",
            "phases": timings,
            "total_wall_seconds": sum(
                float(row["wall_seconds"]) for row in timings.values()
            ),
            "total_cpu_seconds": sum(
                float(row["cpu_seconds"]) for row in timings.values()
            ),
        },
    }


__all__ = [
    "derive_backtest_dataset_plan",
    "prepare_backtest_dataset",
    "validate_backtest_dataset",
]
