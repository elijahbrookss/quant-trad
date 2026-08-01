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
from market_data.contracts import (
    CANDLE_FACT_TYPE,
    CANDLE_FACT_VERSION,
    DATASET_IDENTITY_HASH_VERSION,
    DatasetSeriesRequest,
    build_candle_material_hash,
    build_dataset_identity_hash,
    build_provenance_hash,
    build_quality_hash,
)
from market_data.store import FrozenDataset, MarketDataStore
from strategies.compiler import compile_strategy

from ..indicators.dependency_bindings import normalize_dependency_bindings
from ..indicators.indicator_service import (
    get_instance_meta,
    runtime_input_plan_for_instance,
)
from . import instrument_service
from .candle_service import preflight_candle_coverage_by_instrument
from .feed_service import HistoricalCandleIngestor, historical_candle_ingestor
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
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    warmup: list[dict[str, Any]] = []
    inputs: list[dict[str, Any]] = []
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
        for market_input in manifest.market_inputs:
            if (
                str(market_input.fact_type).strip().lower() != CANDLE_FACT_TYPE
                or str(market_input.contract_version).strip() != CANDLE_FACT_VERSION
            ):
                raise ValueError(
                    "backtest_dataset_unsupported_fact_contract: "
                    f"indicator_id={indicator_id} fact_type={market_input.fact_type} "
                    f"contract_version={market_input.contract_version}"
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
        inputs.append(
            {
                "indicator_id": indicator_id,
                "indicator_type": indicator_type,
                "attachment_role": attachment_role,
                "source_timeframe": str(
                    plan.get("source_timeframe") or strategy.timeframe
                ).strip(),
                "required_start": iso_utc(
                    plan.get("start") or evaluation_start,
                    field=f"indicator[{indicator_id}].required_start",
                ),
                "lookback_bars": plan.get("lookback_bars"),
                "lookback_days": plan.get("lookback_days"),
                "market_inputs": [
                    {
                        "role": item.role,
                        "fact_type": item.fact_type,
                        "contract_version": item.contract_version,
                        "required_fields": list(item.required_fields),
                        "known_at_required": bool(item.known_at_required),
                    }
                    for item in manifest.market_inputs
                ],
            }
        )
    return warmup, inputs


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

    warmup_requirements, indicator_inputs = _indicator_requirements(
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

    merged: dict[tuple[str, int], dict[str, Any]] = {}
    instruments: list[dict[str, Any]] = []
    for link in strategy.instrument_links:
        instrument_id = str(getattr(link, "instrument_id", "") or "").strip()
        if not instrument_id:
            raise ValueError(
                "backtest_dataset_invalid: strategy instrument link has no instrument_id"
            )
        instrument = dict(instrument_loader(instrument_id))
        instruments.append(
            build_backtest_execution_instrument(instrument_id, instrument)
        )

        candidate_inputs = [
            {
                "source_timeframe": strategy_timeframe,
                "required_start": iso_utc(base_warmup_start),
                "role": "strategy_primary_bars",
                "indicator_id": None,
            },
            *[
                {
                    "source_timeframe": item["source_timeframe"],
                    "required_start": item["required_start"],
                    "role": "indicator_input",
                    "indicator_id": item["indicator_id"],
                }
                for item in indicator_inputs
            ],
        ]
        for item in candidate_inputs:
            timeframe = str(item["source_timeframe"] or "").strip()
            seconds = _timeframe_seconds(timeframe)
            required_start = _utc(
                item["required_start"], field=f"{instrument_id}.{timeframe}.required_start"
            )
            if timeframe == strategy_timeframe:
                required_start = min(required_start, base_warmup_start)
            key = (instrument_id, seconds)
            existing = merged.get(key)
            if existing is None:
                existing = {
                    "instrument_id": instrument_id,
                    "symbol": instrument.get("symbol"),
                    "provider": instrument.get("datasource"),
                    "venue": instrument.get("exchange"),
                    "fact_type": CANDLE_FACT_TYPE,
                    "contract_version": CANDLE_FACT_VERSION,
                    "timeframe": timeframe,
                    "timeframe_seconds": seconds,
                    "range_start": iso_utc(required_start),
                    "range_end": iso_utc(end),
                    "roles": [],
                    "indicator_ids": [],
                }
                merged[key] = existing
            elif required_start < _utc(existing["range_start"], field="range_start"):
                existing["range_start"] = iso_utc(required_start)
            if item["role"] not in existing["roles"]:
                existing["roles"].append(item["role"])
            if item["indicator_id"] and item["indicator_id"] not in existing["indicator_ids"]:
                existing["indicator_ids"].append(item["indicator_id"])

    series = sorted(
        merged.values(),
        key=lambda row: (
            str(row["instrument_id"]),
            int(row["timeframe_seconds"]),
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


def _manifest_hash_payload(entry: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "series_id": int(entry["series_id"]),
        "range_start": iso_utc(entry["range_start"], field="range_start"),
        "range_end": iso_utc(entry["range_end"], field="range_end"),
        "max_commit_seq": int(entry["max_commit_seq"]),
        "row_count": int(entry["row_count"]),
        "material_hash": str(entry["material_hash"]),
        "provenance_hash": str(entry["provenance_hash"]),
        "source_summary": dict(entry.get("source_summary") or {}),
        "quality_hash": str(entry["quality_hash"]),
        "quality_summary": dict(entry.get("quality_summary") or {}),
    }


def _gap_is_disclosed(
    start: datetime,
    end: datetime,
    quality: Sequence[Mapping[str, Any]],
) -> bool:
    for row in quality:
        classification = str(row.get("classification") or "").strip().lower()
        if classification not in _ALLOWED_DISCLOSED_GAP_CLASSIFICATIONS:
            continue
        evidence_start = _utc(row.get("start"), field="gap.start")
        evidence_end = _utc(row.get("end"), field="gap.end")
        if evidence_start <= start and evidence_end >= end:
            return True
    return False


def _validate_dataset_series(
    *,
    store: MarketDataStore,
    entry: Mapping[str, Any],
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    series_id = int(entry["series_id"])
    range_start = _utc(entry["range_start"], field="range_start")
    range_end = _utc(entry["range_end"], field="range_end")
    timeframe_seconds = int(entry["timeframe_seconds"])
    if str(entry.get("fact_type") or "") != CANDLE_FACT_TYPE:
        raise ValueError(
            f"backtest_dataset_unsupported_fact_contract: series_id={series_id}"
        )
    if str(entry.get("contract_version") or "") != CANDLE_FACT_VERSION:
        raise ValueError(
            "backtest_dataset_contract_mismatch: "
            f"series_id={series_id} contract={entry.get('contract_version')}"
        )
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
    quality = store.list_gap_evidence(
        series_id=series_id,
        start=range_start,
        end=range_end,
        as_of_commit_seq=int(entry["max_commit_seq"]),
    )
    if not records:
        raise RuntimeError(
            f"backtest_dataset_incomplete: series_id={series_id} contains no facts"
        )
    expected = range_start
    gaps: list[dict[str, Any]] = []
    for record in records:
        fact = record.fact
        if fact.open_time < expected:
            raise RuntimeError(
                f"backtest_dataset_malformed: duplicate or unordered candle series_id={series_id}"
            )
        if fact.open_time > expected:
            gap = {"start": expected, "end": fact.open_time}
            if not _gap_is_disclosed(expected, fact.open_time, quality):
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
        if not _gap_is_disclosed(expected, range_end, quality):
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

    series_identity = {
        "identity_key": str(entry["identity_key"]),
        "instrument_id": str(entry["instrument_id"]),
        "fact_type": str(entry["fact_type"]),
        "timeframe_seconds": timeframe_seconds,
        "contract_version": str(entry["contract_version"]),
    }
    material_hash = build_candle_material_hash(
        series_identity=series_identity,
        records=records,
    )
    provenance_hash = build_provenance_hash(records)
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
            "loaded_range": {
                "start": iso_utc(records[0].fact.open_time),
                "end_exclusive": iso_utc(records[-1].fact.close_time),
            },
            "disclosed_gaps": gaps,
            "quality_evidence": [dict(row) for row in quality],
        },
        [dict(row) for row in quality],
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
        (str(row["instrument_id"]), int(row["timeframe_seconds"])): row
        for row in plan["series"]
    }
    actual = {
        (str(row.get("instrument_id") or ""), int(row.get("timeframe_seconds") or 0)): row
        for row in dataset.series
    }
    if set(actual) != set(expected):
        missing = sorted(set(expected) - set(actual))
        extra = sorted(set(actual) - set(expected))
        raise ValueError(
            "backtest_dataset_series_mismatch: "
            f"missing={missing} unexpected={extra}"
        )

    admitted: list[dict[str, Any]] = []
    all_quality: list[dict[str, Any]] = []
    for key, requirement in expected.items():
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
                f"instrument={key[0]} timeframe_seconds={key[1]}"
            )
        verified, quality = _validate_dataset_series(store=store, entry=entry)
        verified["roles"] = list(requirement.get("roles") or [])
        verified["indicator_ids"] = list(requirement.get("indicator_ids") or [])
        admitted.append(verified)
        all_quality.extend(quality)

    manifest_payload = [_manifest_hash_payload(row) for row in admitted]
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
            "status": "ready_with_caveats" if all_quality else "ready",
            "evidence_count": len(all_quality),
            "classifications": dict(
                sorted(
                    Counter(
                        str(row.get("classification") or "unknown")
                        for row in all_quality
                    ).items()
                )
            ),
        },
        "provider_call_performed": False,
        "validation_status": "ready",
    }
    return normalize_backtest_dataset_binding(binding)


def _coverage_for_plan(
    plan: Mapping[str, Any],
    *,
    coverage_loader: Callable[[str, str, str, str], Mapping[str, Any]],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for requirement in plan["series"]:
        payload = dict(
            coverage_loader(
                str(requirement["instrument_id"]),
                str(requirement["range_start"]),
                str(requirement["range_end"]),
                str(requirement["timeframe"]),
            )
        )
        rows.append({**dict(requirement), "coverage": payload})
    return rows


def prepare_backtest_dataset(
    *,
    bot: Mapping[str, Any],
    strategy: Any,
    evaluation_start: Any,
    evaluation_end: Any,
    acquire_missing: bool,
    created_by: str | None = None,
    store: MarketDataStore = market_data_repo,
    ingestor: HistoricalCandleIngestor = historical_candle_ingestor,
    coverage_loader: Callable[[str, str, str, str], Mapping[str, Any]] = preflight_candle_coverage_by_instrument,
    indicator_meta_loader: Callable[..., Mapping[str, Any]] = get_instance_meta,
    indicator_input_plan_loader: Callable[..., Mapping[str, Any]] = runtime_input_plan_for_instance,
    instrument_loader: Callable[[str], Mapping[str, Any]] = instrument_service.get_instrument_record,
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
    coverage_before = _coverage_for_plan(plan, coverage_loader=coverage_loader)
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
        for gap in list((row["coverage"] or {}).get("missing_ranges") or [])
    ]
    if missing and not acquire_missing:
        raise RuntimeError(
            "backtest_dataset_preparation_incomplete: canonical storage is missing "
            f"{len(missing)} range(s); rerun with explicit acquisition enabled"
        )

    acquisitions: list[dict[str, Any]] = []
    phase = _phase_start()
    for row, gap in missing:
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
                "timeframe": row["timeframe"],
                "start": iso_utc(gap["start"], field="gap.start"),
                "end_exclusive": iso_utc(gap["end"], field="gap.end"),
                "source_id": result.source_id,
                "series_id": result.series_id,
                "ingestion_run_id": result.outcome.ingestion_run_id,
                "requested_count": result.outcome.requested_count,
                "inserted_count": result.outcome.inserted_count,
                "corrected_count": result.outcome.corrected_count,
                "noop_count": result.outcome.noop_count,
                "gap_evidence_count": result.gap_evidence_count,
            }
        )
    _phase_finish(
        timings,
        "provider_acquisition",
        phase,
        request_count=len(acquisitions),
        rows_requested=sum(int(row["requested_count"]) for row in acquisitions),
        rows_inserted=sum(int(row["inserted_count"]) for row in acquisitions),
        rows_corrected=sum(int(row["corrected_count"]) for row in acquisitions),
    )

    phase = _phase_start()
    coverage_after = _coverage_for_plan(plan, coverage_loader=coverage_loader)
    remaining = [
        (row["instrument_id"], row["timeframe"], gap)
        for row in coverage_after
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
    for row in plan["series"]:
        series_id = store.resolve_series_id(
            instrument_id=str(row["instrument_id"]),
            fact_type=str(row["fact_type"]),
            timeframe_seconds=int(row["timeframe_seconds"]),
            contract_version=str(row["contract_version"]),
        )
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
