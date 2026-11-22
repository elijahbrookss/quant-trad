"""Generic paired comparison over causal typed candle-state features."""
from __future__ import annotations

from collections import Counter, defaultdict
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import json
import math
import time
from typing import Any

import numpy as np
import pandas as pd

from core.execution_control import execution_checkpoint
from data_providers.utils.ohlcv import interval_to_timedelta
from engines.bot_runtime.core.domain import Candle
from indicators.candle_stats.definition import CandleStatsIndicator
from market_data.frozen import semantic_hash
from research_science.check import CheckDefinition, CheckRequest, ResolvedCheckPlan

from . import checks
from .directional_model import fit_paired_fold, _auc, _log_loss
from .event_features import baseline_features, prepare_atr
from .raw_candle_evaluator import _clock, _finite, _iso, _prepare


SCHEMA = "candle_state_paired_comparison.v2"
RESULT_SCHEMA = "candle_state_paired_comparison_result.v2"
SELECTION_AUDIT_SCHEMA = "candle_state_selection_audit.v1"
MAX_CANDLES = 19_104
MAX_INPUT_BYTES = 256 * 1024 * 1024
MAX_RESULT_BYTES = 16 * 1024 * 1024
MAX_SECONDS = 120.0
ORDINARY_OPERATORS = frozenset({"lagged_return", "atr_fraction", "volume_ratio"})
INDICATOR_OPERATOR = "indicator_metric"
INDICATOR_TYPE = "candle_stats"
INDICATOR_VERSION = "v1"
INDICATOR_OUTPUT = "candle_stats"
PUBLIC_METRICS = frozenset({"directional_efficiency"})
TRANSFORMS = frozenset({"identity", "times_return_sign"})

RULES = {
    "population": "ordinary_native_candle_closes.v2",
    "pairing": "identical_complete_rows.v1",
    "feature_binding": "typed_candle_state_or_public_indicator.v1",
    "selection_audit": "causal_excluded_clock_context.v1",
    "labels": "exact_elapsed_endpoint.v1",
    "gap_policy": "reset_rewarm",
    "model": "training_only_standardized_l2_logistic.v1",
    "inference": "fixed_utc_calendar_cluster_bootstrap.v1",
    "limits": {
        "candles": MAX_CANDLES,
        "input_bytes": MAX_INPUT_BYTES,
        "result_bytes": MAX_RESULT_BYTES,
        "method_seconds": MAX_SECONDS,
    },
}


def _mapping(value: Any, *, field: str) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise ValueError(f"candle_state_comparison_invalid: {field} must be an object")
    return dict(value)


def _list(value: Any, *, field: str, minimum: int = 1, maximum: int = 64) -> list[Any]:
    if not isinstance(value, list) or not minimum <= len(value) <= maximum:
        raise ValueError(
            f"candle_state_comparison_invalid: {field} must contain {minimum}..{maximum} entries"
        )
    return list(value)


def _integer(value: Any, *, field: str, minimum: int, maximum: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or not minimum <= value <= maximum:
        raise ValueError(
            f"candle_state_comparison_invalid: {field} must be an integer in [{minimum},{maximum}]"
        )
    return value


def _number(value: Any, *, field: str, minimum: float | None = None) -> float:
    if isinstance(value, bool):
        raise ValueError(f"candle_state_comparison_invalid: {field} must be finite")
    try:
        result = float(value)
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError(f"candle_state_comparison_invalid: {field} must be finite") from exc
    if not math.isfinite(result) or minimum is not None and result < minimum:
        raise ValueError(f"candle_state_comparison_invalid: {field} must be finite and >= {minimum}")
    return result


def _timestamp(value: Any, *, field: str) -> str:
    return _iso(_clock(value, field))


def _normalize_feature(
    spec: Any,
    *,
    side: str,
    allow_ordinary_transforms: bool = False,
) -> dict[str, Any]:
    row = _mapping(spec, field="statistics.features")
    name = str(row.get("name") or "").strip()
    operator = str(row.get("operator") or "").strip()
    if not name:
        raise ValueError("candle_state_comparison_invalid: feature name is required")
    if operator in ORDINARY_OPERATORS:
        allowed = {"name", "operator"}
        if operator in {"lagged_return", "volume_ratio"}:
            row["lookback_bars"] = _integer(
                row.get("lookback_bars"), field=f"feature.{name}.lookback_bars", minimum=1, maximum=10_000
            )
            allowed.add("lookback_bars")
        if operator == "atr_fraction":
            row["period"] = _integer(
                row.get("period"), field=f"feature.{name}.period", minimum=1, maximum=10_000
            )
            allowed.add("period")
        if allow_ordinary_transforms:
            transform = str(row.get("transform") or "identity").strip()
            if transform not in TRANSFORMS:
                raise ValueError("candle_state_comparison_invalid: unsupported candle feature transform")
            row["transform"] = transform
            allowed.add("transform")
            if transform == "times_return_sign":
                row["sign_lookback_bars"] = _integer(
                    row.get("sign_lookback_bars"),
                    field=f"feature.{name}.sign_lookback_bars",
                    minimum=1,
                    maximum=10_000,
                )
                if str(row.get("zero_sign") or "zero") != "zero":
                    raise ValueError(
                        "candle_state_comparison_invalid: exact flat historical return must map to zero"
                    )
                row["zero_sign"] = "zero"
                allowed.update({"sign_lookback_bars", "zero_sign"})
        if set(row) - allowed:
            raise ValueError(f"candle_state_comparison_invalid: unsupported fields for feature {name}")
        return {key: row[key] for key in sorted(allowed)}

    if side != "enriched":
        raise ValueError(
            "candle_state_comparison_invalid: baseline features require typed candle operators"
        )
    if operator != INDICATOR_OPERATOR:
        raise ValueError("candle_state_comparison_invalid: unsupported enriched feature operator")
    binding = _mapping(row.get("indicator"), field=f"feature.{name}.indicator")
    if (
        str(binding.get("type") or "") != INDICATOR_TYPE
        or str(binding.get("version") or "") != INDICATOR_VERSION
        or str(binding.get("output") or "") != INDICATOR_OUTPUT
        or str(binding.get("field") or "") not in PUBLIC_METRICS
    ):
        raise ValueError("candle_state_comparison_invalid: unsupported public Indicator metric binding")
    params = CandleStatsIndicator.resolve_config(
        _mapping(binding.get("params"), field=f"feature.{name}.indicator.params"),
        strict_unknown=True,
    )
    transform = str(row.get("transform") or "identity").strip()
    if transform not in TRANSFORMS:
        raise ValueError("candle_state_comparison_invalid: unsupported Indicator metric transform")
    result = {
        "name": name,
        "operator": operator,
        "indicator": {
            "type": INDICATOR_TYPE,
            "version": INDICATOR_VERSION,
            "output": INDICATOR_OUTPUT,
            "field": str(binding["field"]),
            "params": params,
        },
        "transform": transform,
    }
    if transform == "times_return_sign":
        result["sign_lookback_bars"] = _integer(
            row.get("sign_lookback_bars"),
            field=f"feature.{name}.sign_lookback_bars",
            minimum=1,
            maximum=10_000,
        )
        if str(row.get("zero_sign") or "zero") != "zero":
            raise ValueError("candle_state_comparison_invalid: exact flat historical return must map to zero")
        result["zero_sign"] = "zero"
    allowed = {"name", "operator", "indicator", "transform", "sign_lookback_bars", "zero_sign"}
    if set(row) - allowed:
        raise ValueError(f"candle_state_comparison_invalid: unsupported fields for feature {name}")
    return result


def _normalize_statistics(
    value: Any,
    *,
    timeframe_seconds: int,
    allow_ordinary_transforms: bool = False,
) -> dict[str, Any]:
    config = _mapping(value, field="statistics")
    if set(config) - {"features", "folds", "model", "purge_bars", "embargo_bars", "bootstrap", "eligibility", "criteria", "elapsed_buffer_seconds"}:
        raise ValueError("candle_state_comparison_invalid: unsupported statistics fields")
    features = _mapping(config.get("features"), field="statistics.features")
    baseline = [
        _normalize_feature(
            row,
            side="baseline",
            allow_ordinary_transforms=allow_ordinary_transforms,
        )
        for row in _list(features.get("baseline"), field="features.baseline")
    ]
    enriched = [
        _normalize_feature(
            row,
            side="enriched",
            allow_ordinary_transforms=allow_ordinary_transforms,
        )
        for row in _list(features.get("enriched"), field="features.enriched")
    ]
    names = [row["name"] for row in baseline + enriched]
    if len(names) != len(set(names)):
        raise ValueError("candle_state_comparison_invalid: feature names must be unique")

    folds: list[dict[str, Any]] = []
    for raw in _list(config.get("folds"), field="statistics.folds", maximum=20):
        fold = _mapping(raw, field="fold")
        if set(fold) != {"id", "train", "validation"}:
            raise ValueError("candle_state_comparison_invalid: fold requires id, train and validation")
        train = _mapping(fold["train"], field="fold.train")
        validation = _mapping(fold["validation"], field="fold.validation")
        normalized = {
            "id": str(fold["id"] or "").strip(),
            "train": {"start": _timestamp(train.get("start"), field="fold.train.start"), "end": _timestamp(train.get("end"), field="fold.train.end")},
            "validation": {"start": _timestamp(validation.get("start"), field="fold.validation.start"), "end": _timestamp(validation.get("end"), field="fold.validation.end")},
        }
        if not normalized["id"] or not (
            _clock(normalized["train"]["start"], "fold.train.start")
            < _clock(normalized["train"]["end"], "fold.train.end")
            <= _clock(normalized["validation"]["start"], "fold.validation.start")
            < _clock(normalized["validation"]["end"], "fold.validation.end")
        ):
            raise ValueError("candle_state_comparison_invalid: ordered nonoverlapping fold clocks required")
        folds.append(normalized)
    if len({row["id"] for row in folds}) != len(folds):
        raise ValueError("candle_state_comparison_invalid: fold ids must be unique")

    model = _mapping(config.get("model"), field="statistics.model")
    expected_model = {
        "type": "standardized_l2_logistic",
        "c": _number(model.get("c"), field="model.c", minimum=1e-12),
        "seed": _integer(model.get("seed"), field="model.seed", minimum=0, maximum=2**31 - 1),
        "fit_intercept": model.get("fit_intercept"),
        "tolerance": _number(model.get("tolerance"), field="model.tolerance", minimum=1e-15),
        "max_iterations": _integer(model.get("max_iterations"), field="model.max_iterations", minimum=1, maximum=100_000),
    }
    if str(model.get("type")) != expected_model["type"] or model.get("fit_intercept") is not True or set(model) != set(expected_model):
        raise ValueError("candle_state_comparison_invalid: explicit standardized L2 logistic model required")

    bootstrap = _mapping(config.get("bootstrap"), field="statistics.bootstrap")
    cluster_days = _integer(bootstrap.get("cluster_days"), field="bootstrap.cluster_days", minimum=1, maximum=365)
    sensitivity = [_integer(v, field="bootstrap.sensitivity_cluster_days", minimum=1, maximum=365) for v in _list(bootstrap.get("sensitivity_cluster_days"), field="bootstrap.sensitivity_cluster_days")]
    if cluster_days in sensitivity:
        raise ValueError("candle_state_comparison_invalid: bootstrap sensitivity must be distinct")
    normalized_bootstrap = {
        "cluster_days": cluster_days,
        "sensitivity_cluster_days": sorted(set(sensitivity)),
        "anchor": _timestamp(bootstrap.get("anchor"), field="bootstrap.anchor"),
        "replicates": _integer(bootstrap.get("replicates"), field="bootstrap.replicates", minimum=100, maximum=100_000),
        "confidence": _number(bootstrap.get("confidence"), field="bootstrap.confidence", minimum=0.5),
        "seed": _integer(bootstrap.get("seed"), field="bootstrap.seed", minimum=0, maximum=2**31 - 1),
    }
    if not normalized_bootstrap["confidence"] < 1.0:
        raise ValueError("candle_state_comparison_invalid: bootstrap confidence must be below one")

    eligibility = _mapping(config.get("eligibility"), field="statistics.eligibility")
    normalized_eligibility = {
        "min_train_rows": _integer(eligibility.get("min_train_rows"), field="eligibility.min_train_rows", minimum=1, maximum=MAX_CANDLES),
        "min_validation_rows": _integer(eligibility.get("min_validation_rows"), field="eligibility.min_validation_rows", minimum=1, maximum=MAX_CANDLES),
        "min_train_each_class": _integer(eligibility.get("min_train_each_class"), field="eligibility.min_train_each_class", minimum=1, maximum=MAX_CANDLES),
        "min_validation_each_class": _integer(eligibility.get("min_validation_each_class"), field="eligibility.min_validation_each_class", minimum=1, maximum=MAX_CANDLES),
        "min_validation_utc_days": _integer(eligibility.get("min_validation_utc_days"), field="eligibility.min_validation_utc_days", minimum=1, maximum=3650),
        "min_valid_folds": _integer(eligibility.get("min_valid_folds"), field="eligibility.min_valid_folds", minimum=1, maximum=len(folds)),
        "min_nonempty_clusters": _integer(eligibility.get("min_nonempty_clusters"), field="eligibility.min_nonempty_clusters", minimum=2, maximum=MAX_CANDLES),
    }
    criteria = _mapping(config.get("criteria"), field="statistics.criteria")
    normalized_criteria = {
        "min_delta_log_loss": _number(criteria.get("min_delta_log_loss"), field="criteria.min_delta_log_loss", minimum=0.0),
        "require_interval_above_zero": bool(criteria.get("require_interval_above_zero")),
        "min_positive_folds": _integer(criteria.get("min_positive_folds"), field="criteria.min_positive_folds", minimum=1, maximum=len(folds)),
        "interpretation": str(criteria.get("interpretation") or "").strip(),
    }
    if normalized_criteria["interpretation"] != "forecast_screen_only":
        raise ValueError("candle_state_comparison_invalid: criteria must remain a forecast screen")

    purge = _integer(config.get("purge_bars"), field="statistics.purge_bars", minimum=0, maximum=10_000)
    embargo = _integer(config.get("embargo_bars"), field="statistics.embargo_bars", minimum=0, maximum=10_000)
    return {
        "features": {"baseline": baseline, "enriched": enriched},
        "folds": folds,
        "model": expected_model,
        "purge_bars": purge,
        "embargo_bars": embargo,
        "elapsed_buffer_seconds": max(purge, embargo) * timeframe_seconds,
        "bootstrap": normalized_bootstrap,
        "eligibility": normalized_eligibility,
        "criteria": normalized_criteria,
    }


def _normalize_configuration(
    *,
    detector,
    outcomes,
    statistics,
    inputs,
    scope,
    gap_policy,
    schema_version: str,
    allow_ordinary_transforms: bool,
):
    detector = _mapping(detector, field="detector")
    if detector != {"type": "candle_close", "sampling": "ordinary_native_close"}:
        raise ValueError("candle_state_comparison_invalid: ordinary native candle-close sampling required")
    if inputs or scope.get("indicator_id") or scope.get("indicator_param_overrides"):
        raise ValueError("candle_state_comparison_invalid: method binds its typed public Indicator metric")
    if gap_policy != "reset_rewarm":
        raise ValueError("candle_state_comparison_invalid: reset_rewarm gap policy required")
    timeframe = str(scope.get("timeframe") or scope.get("interval") or "")
    step = interval_to_timedelta(timeframe)
    seconds = int(step.total_seconds())
    if seconds <= 0:
        raise ValueError("candle_state_comparison_invalid: positive native timeframe required")
    config = _mapping(outcomes, field="outcomes")
    allowed = {"schema_version", "horizon_seconds", "horizon_bars", "primary", "continuation", "magnitude", "outcome_boundary"}
    if set(config) - allowed or config.get("schema_version") != schema_version:
        raise ValueError("candle_state_comparison_invalid: explicit candle-state schema required")
    horizon = _integer(config.get("horizon_seconds"), field="outcomes.horizon_seconds", minimum=seconds, maximum=86400 * 30)
    if horizon % seconds:
        raise ValueError("candle_state_comparison_invalid: horizon must be an exact number of native intervals")
    if "horizon_bars" in config and config["horizon_bars"] != horizon // seconds:
        raise ValueError("candle_state_comparison_invalid: derived horizon_bars mismatch")
    if _mapping(config.get("primary"), field="outcomes.primary") != {
        "type": "forward_close_gt_current", "flat": "class_zero"
    }:
        raise ValueError("candle_state_comparison_invalid: primary must be up versus non-up with flats retained")
    continuation = _mapping(config.get("continuation"), field="outcomes.continuation")
    normalized_continuation = {
        "lookback_bars": _integer(continuation.get("lookback_bars"), field="continuation.lookback_bars", minimum=1, maximum=10_000),
        "zero_prior": str(continuation.get("zero_prior") or ""),
        "future_flat": str(continuation.get("future_flat") or ""),
    }
    if normalized_continuation["zero_prior"] != "undefined_prior_direction" or normalized_continuation["future_flat"] != "separate":
        raise ValueError("candle_state_comparison_invalid: explicit continuation flat rules required")
    if _mapping(config.get("magnitude"), field="outcomes.magnitude") != {
        "endpoint": "absolute_log_return", "path": "sum_squared_native_log_returns"
    }:
        raise ValueError("candle_state_comparison_invalid: fixed secondary magnitude definitions required")
    if str(config.get("outcome_boundary")) != "evaluation_end_exclusive":
        raise ValueError("candle_state_comparison_invalid: strict evaluation boundary required")
    normalized_outcomes = {
        "schema_version": schema_version,
        "horizon_seconds": horizon,
        "horizon_bars": horizon // seconds,
        "primary": {"type": "forward_close_gt_current", "flat": "class_zero"},
        "continuation": normalized_continuation,
        "magnitude": {"endpoint": "absolute_log_return", "path": "sum_squared_native_log_returns"},
        "outcome_boundary": "evaluation_end_exclusive",
    }
    return normalized_outcomes, _normalize_statistics(
        statistics,
        timeframe_seconds=seconds,
        allow_ordinary_transforms=allow_ordinary_transforms,
    )


def normalize_configuration(*, detector, outcomes, statistics, inputs, scope, gap_policy):
    return _normalize_configuration(
        detector=detector,
        outcomes=outcomes,
        statistics=statistics,
        inputs=inputs,
        scope=scope,
        gap_policy=gap_policy,
        schema_version=SCHEMA,
        allow_ordinary_transforms=False,
    )


def _cluster_ci(rows: Sequence[Mapping[str, Any]], *, cluster_days: int, anchor: str,
                replicates: int, confidence: float, seed: int) -> dict[str, Any]:
    anchor_time = _clock(anchor, "bootstrap.anchor").normalize()
    by_cluster: dict[int, list[float]] = defaultdict(list)
    occupied_days: set[int] = set()
    for row in rows:
        decision = _clock(row["decision_time"], "decision_time")
        day = int((decision.normalize() - anchor_time) / pd.Timedelta(days=1))
        occupied_days.add(day)
        by_cluster[day // cluster_days].append(float(row["paired_log_loss_delta"]))
    keys = sorted(by_cluster)
    if len(keys) < 2:
        return {"status": "indeterminate", "reason": "fewer_than_two_nonempty_clusters", "cluster_days": cluster_days, "cluster_count": len(keys)}
    rng = np.random.default_rng(seed)
    estimates = []
    for index in range(replicates):
        if index % 256 == 0:
            execution_checkpoint()
        sampled = rng.choice(keys, size=len(keys), replace=True)
        values = [value for key in sampled for value in by_cluster[int(key)]]
        estimates.append(float(np.mean(np.asarray(values, dtype=float))))
    alpha = (1.0 - confidence) / 2.0
    first, last = min(occupied_days), max(occupied_days)
    possible = len(set(day // cluster_days for day in range(first, last + 1)))
    return {
        "status": "completed", "cluster_days": cluster_days, "anchor": _iso(anchor_time),
        "cluster_count": len(keys), "empty_calendar_cluster_count": possible - len(keys),
        "replicates": replicates, "confidence": confidence,
        "lower": float(np.quantile(estimates, alpha)),
        "upper": float(np.quantile(estimates, 1.0 - alpha)),
    }


def _indicator_runtime(spec: Mapping[str, Any]):
    binding = spec["indicator"]
    return CandleStatsIndicator.build_runtime_indicator(
        indicator_id="check.candle_state.public_metric",
        meta={"version": binding["version"]},
        resolved_params=binding["params"],
        strategy_indicator_metas={},
    )


def _candle(row: Mapping[str, Any]) -> Candle:
    return Candle(
        time=row["open_time"].to_pydatetime(),
        end=row["close_time"].to_pydatetime(),
        known_at=row["known_at"].to_pydatetime(),
        open=float(row["open"]), high=float(row["high"]),
        low=float(row["low"]), close=float(row["close"]),
        volume=_finite(row.get("volume")),
    )


def _segmented_atr(rows: Sequence[Mapping[str, Any]], *, step: timedelta,
                   baseline: Sequence[Mapping[str, Any]]) -> dict[int, dict[datetime, float]]:
    periods = sorted({int(row["period"]) for row in baseline if row["operator"] == "atr_fraction"})
    merged = {period: {} for period in periods}
    if not periods:
        return merged
    segment: list[Mapping[str, Any]] = []
    for row in rows:
        if segment and row["open_time"] != segment[-1]["open_time"] + step:
            for period, values in prepare_atr(segment, baseline).items():
                merged[period].update(values)
            segment = []
        segment.append(row)
    if segment:
        for period, values in prepare_atr(segment, baseline).items():
            merged[period].update(values)
    return merged


def _class_counts(rows: Sequence[Mapping[str, Any]]) -> dict[str, int]:
    positive = sum(bool(row["outcomes"]["primary"]["positive"]) for row in rows)
    return {"up": positive, "non_up": len(rows) - positive}


def _distribution(values: Sequence[float]) -> dict[str, Any]:
    array = np.asarray([float(value) for value in values], dtype=float)
    return {
        "count": int(array.size),
        "mean": float(np.mean(array)) if array.size else None,
        "median": float(np.median(array)) if array.size else None,
        "p10": float(np.quantile(array, .1)) if array.size else None,
        "p90": float(np.quantile(array, .9)) if array.size else None,
    }


def _ordinary_feature_values(
    specs: Sequence[Mapping[str, Any]],
    *,
    current: Mapping[str, Any],
    decision: pd.Timestamp,
    step: timedelta,
    by_open: Mapping[pd.Timestamp, Mapping[str, Any]],
    candle_by_open: Mapping[datetime, Mapping[str, Any]],
    atr_by_period: Mapping[int, Mapping[datetime, float]],
    episode_known_at: pd.Timestamp,
) -> tuple[dict[str, float | None], list[str]]:
    """Compute typed past-only values and reject source state unavailable at decision time."""
    if not specs:
        return {}, []
    raw = baseline_features(
        baseline_specs=specs,
        output={},
        direction=1.0,
        event_time=current["open_time"].to_pydatetime(),
        step=step,
        event_close=float(current["close"]),
        event_candle=current,
        candle_by_open=candle_by_open,
        atr_by_period=atr_by_period,
    )
    values: dict[str, float | None] = {}
    unavailable: list[str] = []
    for spec in specs:
        name = str(spec["name"])
        operator = str(spec["operator"])
        value = _finite(raw.get(name))
        causal = True
        if operator in {"lagged_return", "volume_ratio"}:
            lookback = int(spec["lookback_bars"])
            sources = [current] + [
                by_open.get(current["open_time"] - step * offset)
                for offset in range(1, lookback + 1)
            ]
            causal = all(
                row is not None and row["known_at"] <= decision
                for row in sources
            )
            if operator == "volume_ratio":
                causal = causal and all(
                    _finite(row.get("volume")) is not None
                    and float(row["volume"]) >= 0.0
                    for row in sources
                    if row is not None
                )
        elif operator == "atr_fraction":
            causal = episode_known_at <= decision
        transform = str(spec.get("transform") or "identity")
        if causal and value is not None and transform == "times_return_sign":
            sign_lookback = int(spec["sign_lookback_bars"])
            sign_sources = [
                by_open.get(current["open_time"] - step * offset)
                for offset in range(0, sign_lookback + 1)
            ]
            causal = all(
                row is not None and row["known_at"] <= decision
                for row in sign_sources
            )
            if causal:
                prior = sign_sources[-1]
                delta = float(current["close"]) - float(prior["close"])
                value *= 1.0 if delta > 0 else -1.0 if delta < 0 else 0.0
        if not causal or value is None:
            values[name] = None
            unavailable.append(name)
        else:
            values[name] = float(value)
    return values, unavailable


def _legacy_directional_scoring(
    *,
    complete_rows: Sequence[Mapping[str, Any]],
    statistics: Mapping[str, Any],
    seconds: int,
    checkpoint,
) -> dict[str, Any]:
    """Preserve the v21-v23 paired directional result contract verbatim."""
    baseline_specs = statistics["features"]["baseline"]
    enriched_specs = statistics["features"]["enriched"]
    baseline_names = [row["name"] for row in baseline_specs]
    enriched_names = baseline_names + [row["name"] for row in enriched_specs]
    fold_results: list[dict[str, Any]] = []
    paired_predictions: list[dict[str, Any]] = []
    eligibility = statistics["eligibility"]
    buffer_seconds = statistics["purge_bars"] * seconds
    embargo_seconds = statistics["embargo_bars"] * seconds
    for fold in statistics["folds"]:
        checkpoint()
        train_start = _clock(fold["train"]["start"], "fold.train.start")
        train_end = _clock(fold["train"]["end"], "fold.train.end")
        validation_start = _clock(fold["validation"]["start"], "fold.validation.start")
        validation_end = _clock(fold["validation"]["end"], "fold.validation.end")
        effective_train_end = train_end - timedelta(seconds=buffer_seconds)
        effective_validation_start = validation_start + timedelta(seconds=embargo_seconds)
        train_rows = [row for row in complete_rows
            if train_start <= _clock(row["decision_time"], "decision_time") < effective_train_end
            and _clock(row["target_time"], "target_time") < train_end]
        validation_rows = [row for row in complete_rows
            if effective_validation_start <= _clock(row["decision_time"], "decision_time") < validation_end
            and _clock(row["target_time"], "target_time") < validation_end]
        train_classes = _class_counts(train_rows)
        validation_classes = _class_counts(validation_rows)
        validation_days = len({row["decision_time"][:10] for row in validation_rows})
        reasons = []
        if len(train_rows) < eligibility["min_train_rows"]: reasons.append("minimum_train_rows_not_met")
        if len(validation_rows) < eligibility["min_validation_rows"]: reasons.append("minimum_validation_rows_not_met")
        if min(train_classes.values(), default=0) < eligibility["min_train_each_class"]: reasons.append("minimum_train_class_not_met")
        if min(validation_classes.values(), default=0) < eligibility["min_validation_each_class"]: reasons.append("minimum_validation_class_not_met")
        if validation_days < eligibility["min_validation_utc_days"]: reasons.append("minimum_validation_days_not_met")
        result = {
            "id": fold["id"], "status": "unsupported" if reasons else "valid", "reasons": reasons,
            "train_count": len(train_rows), "validation_count": len(validation_rows),
            "train_classes": train_classes, "validation_classes": validation_classes,
            "validation_utc_days": validation_days,
            "effective_train_end": _iso(effective_train_end),
            "effective_validation_start": _iso(effective_validation_start),
            "train_target_boundary": _iso(train_end), "validation_target_boundary": _iso(validation_end),
        }
        if not reasons:
            metrics, predictions = fit_paired_fold(
                train_rows=train_rows, validation_rows=validation_rows,
                baseline_names=baseline_names, enriched_names=enriched_names,
                primary_key="primary", model=statistics["model"],
            )
            result.update(metrics)
            for row in predictions:
                row["fold_id"] = fold["id"]
            paired_predictions.extend(predictions)
        fold_results.append(result)

    valid_folds = [row for row in fold_results if row["status"] == "valid"]
    bootstrap = statistics["bootstrap"]
    primary_ci = _cluster_ci(
        paired_predictions, cluster_days=bootstrap["cluster_days"], anchor=bootstrap["anchor"],
        replicates=bootstrap["replicates"], confidence=bootstrap["confidence"], seed=bootstrap["seed"],
    ) if paired_predictions else {"status": "indeterminate", "reason": "no_paired_predictions"}
    sensitivity = [
        _cluster_ci(
            paired_predictions, cluster_days=days, anchor=bootstrap["anchor"],
            replicates=bootstrap["replicates"], confidence=bootstrap["confidence"], seed=bootstrap["seed"],
        )
        for days in bootstrap["sensitivity_cluster_days"]
    ] if paired_predictions else []
    support_reasons = []
    if len(valid_folds) < eligibility["min_valid_folds"]:
        support_reasons.append("minimum_valid_folds_not_met")
    if primary_ci.get("cluster_count", 0) < eligibility["min_nonempty_clusters"]:
        support_reasons.append("minimum_nonempty_clusters_not_met")
    comparison = None
    criteria = statistics["criteria"]
    if paired_predictions:
        actual = np.asarray([row["actual"] for row in paired_predictions], dtype=float)
        baseline_probability = np.asarray([row["baseline_probability"] for row in paired_predictions], dtype=float)
        enriched_probability = np.asarray([row["enriched_probability"] for row in paired_predictions], dtype=float)
        delta = float(np.mean([row["paired_log_loss_delta"] for row in paired_predictions]))
        comparison = {
            "paired_row_count": len(paired_predictions),
            "baseline_features": baseline_names, "enriched_features": enriched_names,
            "baseline": {"log_loss": _log_loss(actual, baseline_probability), "roc_auc": _auc(actual, baseline_probability)},
            "enriched": {"log_loss": _log_loss(actual, enriched_probability), "roc_auc": _auc(actual, enriched_probability)},
            "delta_log_loss": delta,
            "positive_delta_fold_count": sum(float(row.get("delta_log_loss") or 0.0) > 0 for row in valid_folds),
            "bootstrap": primary_ci, "bootstrap_sensitivity": sensitivity,
            "prediction_hash": semantic_hash({"paired_predictions": paired_predictions}),
            "screen": {
                "status": "unsupported" if support_reasons else "completed",
                "support_reasons": support_reasons,
                "effect_floor_met": delta >= criteria["min_delta_log_loss"],
                "interval_above_zero": primary_ci.get("lower") is not None and float(primary_ci["lower"]) > 0.0,
                "positive_fold_gate_met": sum(float(row.get("delta_log_loss") or 0.0) > 0 for row in valid_folds) >= criteria["min_positive_folds"],
                "interpretation": criteria["interpretation"],
            },
        }
    return {"folds": fold_results, "comparison": comparison}


@dataclass(frozen=True)
class CandleStateComparisonV2Evaluator:
    evaluator_id: str = checks.RAW_FORWARD_OUTCOME
    version: str = "22"
    result_schema_version: str = RESULT_SCHEMA

    def _complete_row_extensions(
        self,
        *,
        current: Mapping[str, Any],
        future: Sequence[Mapping[str, Any]],
        target: Mapping[str, Any],
    ) -> dict[str, Any]:
        del current, future, target
        return {}

    def _score_complete_rows(
        self,
        *,
        complete_rows: Sequence[Mapping[str, Any]],
        statistics: Mapping[str, Any],
        seconds: int,
        checkpoint,
    ) -> dict[str, Any]:
        return _legacy_directional_scoring(
            complete_rows=complete_rows,
            statistics=statistics,
            seconds=seconds,
            checkpoint=checkpoint,
        )

    def declare_requirements(self, *, definition: CheckDefinition, request: CheckRequest):
        del definition
        outcomes = request.parameters["outcomes"]
        statistics = request.parameters["statistics"]
        baseline = statistics["features"]["baseline"]
        enriched = statistics["features"]["enriched"]
        all_features = baseline + enriched
        indicator_features = [
            row for row in enriched if row["operator"] == INDICATOR_OPERATOR
        ]
        lookbacks = [
            int(row.get("lookback_bars") or row.get("period") or 0)
            for row in all_features
        ] + [
            int(row.get("sign_lookback_bars") or 0)
            for row in all_features
        ] + [
            int(row["indicator"]["params"]["warmup_bars"])
            for row in indicator_features
        ] + [int(outcomes["continuation"]["lookback_bars"])]
        start = _clock(request.scope.get("start"), "scope.start")
        end = _clock(request.scope.get("end"), "scope.end")
        step = interval_to_timedelta(str(request.scope.get("timeframe") or request.scope.get("interval") or ""))
        warmup = max(lookbacks, default=0)
        nominal = int((end - start) / pd.Timedelta(step))
        if start >= end or nominal + warmup + int(outcomes["horizon_bars"]) > MAX_CANDLES:
            raise ValueError("candle_state_comparison_invalid: bounded 19104-candle materialization exceeded")
        return {
            "input_kind": "market_data", "indicator_ids": [],
            "warmup_floor_bars": warmup, "feature_lookback_bars": warmup,
            "feature_windows_seconds_by_alias": {},
            "outcome_horizons": [int(outcomes["horizon_seconds"])],
            "required_outcome_horizons": [int(outcomes["horizon_seconds"])],
            "horizon_kind": "elapsed_time", "entry_lag_bars": 0,
            "invalidation_max_bars": 0, "outcome_boundary": "evaluation_end_exclusive",
            "primary_candle_required_fields": ["open", "high", "low", "close", "volume", "known_at"],
            "public_indicator_metrics": [
                {"type": row["indicator"]["type"], "version": row["indicator"]["version"],
                 "output": row["indicator"]["output"], "field": row["indicator"]["field"]}
                for row in indicator_features
            ],
        }

    def evaluate(self, *, plan: ResolvedCheckPlan, inputs: Mapping[str, Any]):
        started = time.monotonic()

        def checkpoint() -> None:
            execution_checkpoint()
            if time.monotonic() - started > MAX_SECONDS:
                raise RuntimeError("candle_state_comparison_limit: 120-second method budget exceeded")

        outcomes = inputs["outcomes"]
        statistics = inputs["statistics"]
        baseline_specs = statistics["features"]["baseline"]
        enriched_specs = statistics["features"]["enriched"]
        baseline_ordinary = [
            row for row in baseline_specs if row["operator"] in ORDINARY_OPERATORS
        ]
        enriched_ordinary = [
            row for row in enriched_specs if row["operator"] in ORDINARY_OPERATORS
        ]
        indicator_specs = [
            row for row in enriched_specs if row["operator"] == INDICATOR_OPERATOR
        ]
        ordinary_specs = baseline_ordinary + enriched_ordinary
        frame = inputs.get("candles")
        if not isinstance(frame, pd.DataFrame):
            raise ValueError("candle_state_comparison_input_invalid: canonical candle frame required")
        if len(frame) > MAX_CANDLES:
            raise ValueError("candle_state_comparison_limit: maximum 19104 candles exceeded")
        input_bytes = int(frame.memory_usage(index=True, deep=True).sum())
        if input_bytes > MAX_INPUT_BYTES:
            raise ValueError("candle_state_comparison_limit: 256 MiB input budget exceeded")
        primary = next(row for row in plan.market_data_requirements if row["alias"] == "primary_bars")
        seconds = int(primary["timeframe_seconds"])
        step = timedelta(seconds=seconds)
        if outcomes["horizon_seconds"] % seconds or plan.gap_policy != "reset_rewarm":
            raise ValueError("candle_state_comparison_input_invalid: pinned elapsed horizon/gap policy mismatch")
        start = _clock(plan.evaluation_range["start"], "evaluation_range.start")
        end = _clock(plan.evaluation_range["end_exclusive"], "evaluation_range.end_exclusive")
        prepared, rows = _prepare(frame, step=step, volume_required=True)
        del prepared
        if len(rows) > MAX_CANDLES:
            raise ValueError("candle_state_comparison_limit: maximum 19104 candles exceeded")
        by_open = {row["open_time"]: row for row in rows}
        by_close = {row["close_time"]: row for row in rows}
        candle_by_open = {key.to_pydatetime(): value for key, value in by_open.items()}
        atr_by_period = _segmented_atr(rows, step=step, baseline=ordinary_specs)

        indicator_identity = indicator_specs[0]["indicator"] if indicator_specs else None
        if indicator_identity is not None and any(
            row["indicator"] != indicator_identity for row in indicator_specs
        ):
            raise ValueError("candle_state_comparison_invalid: one exact public Indicator configuration required")
        runtime = _indicator_runtime(indicator_specs[0]) if indicator_specs else None
        public_values: dict[pd.Timestamp, Mapping[str, Any]] = {}
        episode_context: dict[pd.Timestamp, Mapping[str, Any]] = {}
        previous_open: pd.Timestamp | None = None
        episode_known_at: pd.Timestamp | None = None
        episode_start: pd.Timestamp | None = None
        episode_index = -1
        bars_since_reset = 0
        for index, row in enumerate(rows):
            if index % 128 == 0:
                checkpoint()
            gap = previous_open is None or row["open_time"] != previous_open + step
            if gap:
                if runtime is not None and previous_open is not None:
                    runtime.handle_gap(
                        policy="reset_rewarm", gap={"classification": "missing_native_interval"},
                        next_bar_time=row["open_time"].to_pydatetime(),
                        rewarm_bars=int(indicator_identity["params"]["warmup_bars"]),
                    )
                episode_known_at = None
                episode_start = row["open_time"]
                episode_index += 1
                bars_since_reset = 0
            episode_known_at = max(filter(None, (episode_known_at, row["known_at"])))
            episode_context[row["open_time"]] = {
                "episode_index": episode_index,
                "episode_start_time": episode_start,
                "bars_since_reset": bars_since_reset,
                "episode_known_at": episode_known_at,
            }
            if runtime is not None:
                runtime.apply_bar(_candle(row), {})
                output = runtime.snapshot()[indicator_identity["output"]]
                if output.ready:
                    public_values[row["open_time"]] = {
                        **dict(output.value), "__known_at__": episode_known_at,
                    }
            previous_open = row["open_time"]
            bars_since_reset += 1

        horizon_bars = int(outcomes["horizon_bars"])
        continuation = outcomes.get("continuation")
        continuation_lookback = int(continuation["lookback_bars"]) if continuation else 0
        opportunities = Counter()
        exclusions = Counter()
        complete_rows: list[dict[str, Any]] = []
        selection_audit_rows: list[dict[str, Any]] = []
        selection_reason_counts = Counter()
        selection_field_missingness = Counter()
        diagnostics = Counter()
        magnitudes: list[float] = []
        path_variations: list[float] = []
        nominal_slots = int((end - start) / pd.Timedelta(step))
        opportunities["nominal_utc_grid_slots"] = nominal_slots
        feature_names = [row["name"] for row in baseline_specs + enriched_specs]
        observed_decisions: set[pd.Timestamp] = set()

        for index, current in enumerate(rows):
            if index % 128 == 0:
                checkpoint()
            decision = current["close_time"]
            if not start <= decision < end:
                continue
            opportunities["observed_native_closes"] += 1
            observed_decisions.add(decision)
            episode = episode_context[current["open_time"]]
            reasons: list[str] = []

            def add_reason(reason: str) -> None:
                if reason not in reasons:
                    reasons.append(reason)

            candle_causal = current["known_at"] <= decision
            if not candle_causal:
                add_reason("candle_not_known_at_decision")
            baseline_values, baseline_missing = _ordinary_feature_values(
                baseline_ordinary,
                current=current,
                decision=decision,
                step=step,
                by_open=by_open,
                candle_by_open=candle_by_open,
                atr_by_period=atr_by_period,
                episode_known_at=episode["episode_known_at"],
            )
            enriched_values, enriched_missing = _ordinary_feature_values(
                enriched_ordinary,
                current=current,
                decision=decision,
                step=step,
                by_open=by_open,
                candle_by_open=candle_by_open,
                atr_by_period=atr_by_period,
                episode_known_at=episode["episode_known_at"],
            )
            metric = public_values.get(current["open_time"])
            if indicator_specs:
                if metric is None:
                    add_reason("indicator_not_ready")
                elif metric["__known_at__"] > decision:
                    add_reason("indicator_not_known_at_decision")
                else:
                    opportunities["indicator_ready_rows"] += 1
                for spec in indicator_specs:
                    value = (
                        None
                        if metric is None or metric["__known_at__"] > decision
                        else _finite(metric.get(spec["indicator"]["field"]))
                    )
                    if value is not None and spec["transform"] == "times_return_sign":
                        prior = by_open.get(
                            current["open_time"] - step * int(spec["sign_lookback_bars"])
                        )
                        if prior is None or prior["known_at"] > decision or not candle_causal:
                            value = None
                        else:
                            delta = float(current["close"]) - float(prior["close"])
                            value *= 1.0 if delta > 0 else -1.0 if delta < 0 else 0.0
                    enriched_values[spec["name"]] = None if value is None else float(value)
                    if value is None:
                        enriched_missing.append(spec["name"])
            else:
                opportunities["indicator_not_required_rows"] += 1

            if baseline_missing:
                if any(
                    row["operator"] == "volume_ratio" and row["name"] in baseline_missing
                    for row in baseline_ordinary
                ):
                    add_reason("baseline_volume_history_unavailable")
                add_reason("baseline_feature_unavailable")
            else:
                opportunities["baseline_complete_rows"] += 1
            if enriched_missing:
                if any(
                    row["operator"] == "volume_ratio" and row["name"] in enriched_missing
                    for row in enriched_ordinary
                ):
                    add_reason("enriched_volume_history_unavailable")
                add_reason("enriched_feature_unavailable")
            else:
                opportunities["enriched_complete_rows"] += 1

            feature_context = {**baseline_values, **enriched_values}
            for name in feature_names:
                if feature_context.get(name) is None:
                    selection_field_missingness[name] += 1
            if not candle_causal:
                selection_field_missingness["candle.close"] += 1
            if not candle_causal or _finite(current.get("volume")) is None:
                selection_field_missingness["candle.volume"] += 1
            features = {
                name: float(value)
                for name, value in feature_context.items()
                if value is not None
            }

            future = [by_open.get(current["open_time"] + step * offset) for offset in range(1, horizon_bars + 1)]
            target = by_close.get(decision + timedelta(seconds=int(outcomes["horizon_seconds"])))
            if target is None or any(row is None for row in future) or future[-1] is not target:
                add_reason("exact_future_path_missing")
            elif target["close_time"] >= end:
                add_reason("target_outside_evaluation")
            else:
                opportunities["future_path_resolved_rows"] += 1

            selection_audit_rows.append({
                "decision_time": _iso(decision),
                "observed_native_close": True,
                "source_identity_key": current["source_identity_key"],
                "episode_index": int(episode["episode_index"]),
                "episode_start_time": _iso(episode["episode_start_time"]),
                "bars_since_reset": int(episode["bars_since_reset"]),
                "known_at": _iso(current["known_at"]),
                "known_at_state": "available" if candle_causal else "after_decision",
                "known_at_method": current["known_at_method"],
                "candle_context": {
                    "close": float(current["close"]) if candle_causal else None,
                    "volume": _finite(current.get("volume")) if candle_causal else None,
                },
                "feature_context": feature_context,
                "selected_for_paired_model": not reasons,
                "primary_exclusion_reason": reasons[0] if reasons else None,
                "reason_flags": reasons,
            })
            selection_reason_counts.update(reasons)
            if reasons:
                exclusions[reasons[0]] += 1
                continue

            current_close, target_close = float(current["close"]), float(target["close"])
            forward_change = target_close - current_close
            forward_log_return = math.log(target_close / current_close)
            prior = (
                by_open.get(current["open_time"] - step * continuation_lookback)
                if continuation_lookback
                else None
            )
            if not continuation_lookback:
                continuation = "not_configured"
            elif prior is None:
                continuation = "prior_path_unavailable"
            else:
                past_change = current_close - float(prior["close"])
                if past_change == 0:
                    continuation = "undefined_prior_direction"
                elif forward_change == 0:
                    continuation = "future_flat"
                elif past_change * forward_change > 0:
                    continuation = "continued"
                else:
                    continuation = "reversed"
            diagnostics[continuation] += 1
            if forward_change == 0:
                diagnostics["primary_flat"] += 1
            elif forward_change < 0:
                diagnostics["primary_down"] += 1
            else:
                diagnostics["primary_up"] += 1
            path_closes = [current_close] + [float(row["close"]) for row in future]
            variation = sum(math.log(path_closes[i] / path_closes[i - 1]) ** 2 for i in range(1, len(path_closes)))
            magnitudes.append(abs(forward_log_return))
            path_variations.append(variation)
            complete_row = {
                "event_time": _iso(current["open_time"]), "decision_time": _iso(decision),
                "target_time": _iso(target["close_time"]),
                "features": features,
                "outcomes": {"primary": {"positive": forward_change > 0, "flat": forward_change == 0,
                    "log_return": forward_log_return, "magnitude": abs(forward_log_return),
                    "path_variation": variation, "continuation": continuation}},
            }
            complete_row.update(self._complete_row_extensions(
                current=current,
                future=future,
                target=target,
            ))
            complete_rows.append(complete_row)

        for offset in range(nominal_slots):
            decision = start + pd.Timedelta(step) * offset
            if decision in observed_decisions:
                continue
            missing_context = {name: None for name in feature_names}
            selection_field_missingness.update(feature_names)
            selection_field_missingness.update(["candle.close", "candle.volume"])
            selection_reason_counts["missing_native_interval"] += 1
            selection_audit_rows.append({
                "decision_time": _iso(decision),
                "observed_native_close": False,
                "source_identity_key": None,
                "episode_index": None,
                "episode_start_time": None,
                "bars_since_reset": None,
                "known_at": None,
                "known_at_state": "unavailable",
                "known_at_method": None,
                "candle_context": {"close": None, "volume": None},
                "feature_context": missing_context,
                "selected_for_paired_model": False,
                "primary_exclusion_reason": "missing_native_interval",
                "reason_flags": ["missing_native_interval"],
            })
        selection_audit_rows.sort(key=lambda row: row["decision_time"])
        opportunities["paired_complete_rows"] = len(complete_rows)
        selection_audit = {
            "schema_version": SELECTION_AUDIT_SCHEMA,
            "nominal_slot_count": nominal_slots,
            "observed_close_count": int(opportunities["observed_native_closes"]),
            "selected_count": len(complete_rows),
            "missing_native_slot_count": int(selection_reason_counts["missing_native_interval"]),
            "reason_counts": dict(selection_reason_counts),
            "field_missingness": dict(selection_field_missingness),
            "rows": selection_audit_rows,
            "row_hash": semantic_hash({"rows": selection_audit_rows}),
        }

        scoring = self._score_complete_rows(
            complete_rows=complete_rows,
            statistics=statistics,
            seconds=seconds,
            checkpoint=checkpoint,
        )
        fold_results = scoring["folds"]
        comparison = scoring["comparison"]

        result = {
            "schema_version": self.result_schema_version, "check_family": checks.RAW_FORWARD_OUTCOME,
            "status": "completed", "event_ownership": "check", "method_version": self.version,
            "population": "ordinary_native_candle_closes", "sample_count": len(complete_rows),
            "opportunities": dict(opportunities), "exclusions": dict(exclusions),
            "detector": dict(inputs["detector"]), "outcomes": dict(outcomes),
            "statistics_configuration": dict(statistics), "folds": fold_results,
            "comparison": comparison,
            "selection_audit": selection_audit,
            "diagnostics": {
                "direction_and_continuation": dict(diagnostics),
                "absolute_log_return": _distribution(magnitudes),
                "sum_squared_native_log_returns": _distribution(path_variations),
            },
            "input": {
                "candle_count": len(rows), "input_bytes": input_bytes,
                "known_at_methods": dict(Counter(row["known_at_method"] for row in rows)),
                "source_identity_hash": semantic_hash(sorted({row["source_identity_key"] for row in rows})),
                "data_quality": dict(inputs.get("data_quality") or {}),
            },
            "provenance": {
                "indicator": indicator_identity,
                "indicator_binding_hash": (
                    semantic_hash(indicator_identity) if indicator_identity is not None else None
                ),
                "feature_binding_hash": semantic_hash({
                    "baseline": baseline_specs,
                    "enriched": enriched_specs,
                }),
                "population_hash": semantic_hash({"rows": complete_rows}),
                "selection_audit_hash": selection_audit["row_hash"],
                "ordinary_close_sampling": True,
                "fit_and_standardization": "training_rows_only_same_paired_population",
                "label_endpoint": "exact_elapsed_utc",
            },
            "dependence": {
                "overlapping_outcomes_retained": True,
                "cluster_unit": "fixed_utc_calendar_days",
                "recursive_indicator_state_crosses_folds": bool(indicator_specs),
                "buffer_is_not_independence_guarantee": True,
            },
            "limits": {**RULES["limits"], "elapsed_seconds": time.monotonic() - started},
            "caveats": [
                "Forecast comparison only; it is not a profitability, execution or causal claim.",
                "Availability uses retained candle known-at evidence; interval-close inference is not measured publication latency.",
                "Configured purge and embargo do not make recursive Indicator state or overlapping outcomes independent.",
                "Selection-audit context is causal and outcome-blind; missing native slots do not imply market closure.",
            ],
        }
        extensions = dict(scoring.get("result_extensions") or {})
        result["caveats"].extend(extensions.pop("additional_caveats", []))
        result.update(extensions)
        encoded = json.dumps(result, sort_keys=True, separators=(",", ":"), allow_nan=False).encode("utf-8")
        if len(encoded) > MAX_RESULT_BYTES:
            raise RuntimeError("candle_state_comparison_limit: 16 MiB result budget exceeded")
        checkpoint()
        return result
