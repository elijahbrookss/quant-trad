"""Causal rolling benchmark scoring for ordinary native candle targets."""
from __future__ import annotations

from collections import Counter, defaultdict
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import hashlib
import heapq
import math
from typing import Any

import numpy as np
import pandas as pd

from data_providers.utils.ohlcv import interval_to_timedelta
from market_data.frozen import semantic_hash
from research_science.check import CheckDefinition, CheckRequest

from . import checks
from . import candle_state_comparison_v2_evaluator as v2
from .directional_model import _fit_logistic


SCHEMA = "candle_target_benchmark_comparison.v1"
RESULT_SCHEMA = "candle_target_benchmark_comparison_result.v1"
TARGET_DIRECTION = "terminal_direction"
TARGET_VARIANCE = "native_log_return_sum_squares"
RULES = {
    **v2.RULES,
    "population": "ordinary_native_candle_closes.v3",
    "feature_binding": "typed_candle_state.v1",
    "labels": "exact_complete_native_path_with_known_at.v2",
    "model": "target_specific_fold_frozen_forecaster.v1",
    "benchmarks": "strictly_historical_rolling_target_benchmarks.v1",
    "inference": "paired_utc_calendar_cluster_loss_bootstrap.v2",
}


def _folds(value: Any) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    for raw in v2._list(value, field="statistics.folds", maximum=20):
        fold = v2._mapping(raw, field="fold")
        if set(fold) != {"id", "train", "validation"}:
            raise ValueError("candle_target_benchmark_invalid: fold requires id, train and validation")
        train = v2._mapping(fold["train"], field="fold.train")
        validation = v2._mapping(fold["validation"], field="fold.validation")
        normalized = {
            "id": str(fold["id"] or "").strip(),
            "train": {
                "start": v2._timestamp(train.get("start"), field="fold.train.start"),
                "end": v2._timestamp(train.get("end"), field="fold.train.end"),
            },
            "validation": {
                "start": v2._timestamp(validation.get("start"), field="fold.validation.start"),
                "end": v2._timestamp(validation.get("end"), field="fold.validation.end"),
            },
        }
        if not normalized["id"] or not (
            v2._clock(normalized["train"]["start"], "fold.train.start")
            < v2._clock(normalized["train"]["end"], "fold.train.end")
            <= v2._clock(normalized["validation"]["start"], "fold.validation.start")
            < v2._clock(normalized["validation"]["end"], "fold.validation.end")
        ):
            raise ValueError("candle_target_benchmark_invalid: ordered nonoverlapping fold clocks required")
        result.append(normalized)
    if len({row["id"] for row in result}) != len(result):
        raise ValueError("candle_target_benchmark_invalid: fold ids must be unique")
    return result


def _normalize_forecaster(value: Any, *, target: str, names: set[str]) -> dict[str, Any]:
    row = v2._mapping(value, field="statistics.forecaster")
    kind = str(row.get("type") or "")
    if target == TARGET_DIRECTION:
        expected = {
            "type": "standardized_l2_logistic",
            "features": [str(name) for name in v2._list(row.get("features"), field="forecaster.features")],
            "c": v2._number(row.get("c"), field="forecaster.c", minimum=1e-12),
            "fit_intercept": row.get("fit_intercept"),
            "seed": v2._integer(row.get("seed"), field="forecaster.seed", minimum=0, maximum=2**31 - 1),
            "tolerance": v2._number(row.get("tolerance"), field="forecaster.tolerance", minimum=1e-15),
            "max_iterations": v2._integer(row.get("max_iterations"), field="forecaster.max_iterations", minimum=1, maximum=100_000),
            "freeze": str(row.get("freeze") or ""),
        }
        if kind != expected["type"] or row.get("fit_intercept") is not True or set(row) != set(expected):
            raise ValueError("candle_target_benchmark_invalid: explicit standardized logistic forecaster required")
        if len(expected["features"]) != len(set(expected["features"])) or not set(expected["features"]) <= names:
            raise ValueError("candle_target_benchmark_invalid: forecaster features must be unique configured features")
        if expected["freeze"] != "nominal_training_end":
            raise ValueError("candle_target_benchmark_invalid: forecaster must remain fold frozen")
        return expected
    expected = {
        "type": "nonnegative_scaled_feature",
        "feature": str(row.get("feature") or ""),
        "power": v2._integer(row.get("power"), field="forecaster.power", minimum=1, maximum=8),
        "input_scale": str(row.get("input_scale") or ""),
        "fit_intercept": row.get("fit_intercept"),
        "calibration": str(row.get("calibration") or ""),
        "freeze": str(row.get("freeze") or ""),
    }
    if (
        kind != expected["type"]
        or set(row) != set(expected)
        or expected["feature"] not in names
        or expected["input_scale"] != "raw"
        or expected["fit_intercept"] is not False
        or expected["calibration"] != "sum_xy_over_sum_x_squared"
        or expected["freeze"] != "nominal_training_end"
    ):
        raise ValueError("candle_target_benchmark_invalid: explicit nonnegative scaled-feature forecaster required")
    return expected


def normalize_configuration(*, detector, outcomes, statistics, inputs, scope, gap_policy):
    detector = v2._mapping(detector, field="detector")
    if detector != {"type": "candle_close", "sampling": "ordinary_native_close"}:
        raise ValueError("candle_target_benchmark_invalid: ordinary native close sampling required")
    if inputs or scope.get("indicator_id") or scope.get("indicator_param_overrides"):
        raise ValueError("candle_target_benchmark_invalid: candle-only input required")
    if gap_policy != "reset_rewarm":
        raise ValueError("candle_target_benchmark_invalid: reset_rewarm gap policy required")
    timeframe = str(scope.get("timeframe") or scope.get("interval") or "")
    seconds = int(interval_to_timedelta(timeframe).total_seconds())
    if seconds <= 0:
        raise ValueError("candle_target_benchmark_invalid: positive native timeframe required")

    outcome = v2._mapping(outcomes, field="outcomes")
    if set(outcome) != {"schema_version", "horizon_seconds", "horizon_bars", "outcome_boundary", "complete_path", "target"}:
        raise ValueError("candle_target_benchmark_invalid: exact outcomes fields required")
    horizon = v2._integer(outcome.get("horizon_seconds"), field="outcomes.horizon_seconds", minimum=seconds, maximum=86400 * 30)
    if outcome.get("schema_version") != SCHEMA or horizon % seconds:
        raise ValueError("candle_target_benchmark_invalid: versioned native-interval horizon required")
    if outcome.get("horizon_bars") != horizon // seconds or outcome.get("complete_path") is not True or outcome.get("outcome_boundary") != "evaluation_end_exclusive":
        raise ValueError("candle_target_benchmark_invalid: exact complete-path boundary required")
    target = v2._mapping(outcome.get("target"), field="outcomes.target")
    operator = str(target.get("operator") or "")
    if operator == TARGET_DIRECTION:
        if target != {"operator": TARGET_DIRECTION, "definition_version": "terminal_direction.v1", "flat": "class_zero"}:
            raise ValueError("candle_target_benchmark_invalid: terminal direction target contract mismatch")
    elif operator == TARGET_VARIANCE:
        allowed = {"operator", "definition_version", "study_profile", "meaning"}
        if set(target) - allowed or target.get("definition_version") != "native_log_return_sum_squares.v1":
            raise ValueError("candle_target_benchmark_invalid: native return sum-squares target contract mismatch")
        target = {key: target[key] for key in sorted(target)}
    else:
        raise ValueError("candle_target_benchmark_invalid: unsupported target")
    normalized_outcome = {
        "schema_version": SCHEMA,
        "horizon_seconds": horizon,
        "horizon_bars": horizon // seconds,
        "outcome_boundary": "evaluation_end_exclusive",
        "complete_path": True,
        "target": target,
    }

    required = {
        "features",
        "forecaster",
        "rolling_history",
        "benchmarks",
        "folds",
        "purge_bars",
        "embargo_bars",
        "eligibility",
        "bootstrap",
        "loss",
        "criteria",
    }
    canonical_required = required | {"target_operator", "elapsed_buffer_seconds"}
    config = v2._mapping(statistics, field="statistics")
    config_fields = frozenset(config)
    canonical_input = config_fields == frozenset(canonical_required)
    if config_fields not in {
        frozenset(required),
        frozenset(canonical_required),
    }:
        raise ValueError("candle_target_benchmark_invalid: exact statistics fields required")
    feature_input = config.get("features")
    if canonical_input:
        if str(config.get("target_operator") or "") != operator:
            raise ValueError("candle_target_benchmark_invalid: canonical target operator mismatch")
        feature_groups = v2._mapping(feature_input, field="statistics.features")
        if (
            set(feature_groups) != {"baseline", "enriched"}
            or feature_groups.get("enriched") != []
        ):
            raise ValueError("candle_target_benchmark_invalid: canonical candle-only feature groups required")
        feature_input = feature_groups.get("baseline")
    features = [
        v2._normalize_feature(row, side="baseline", allow_ordinary_transforms=True)
        for row in v2._list(feature_input, field="statistics.features", maximum=32)
    ]
    names = [row["name"] for row in features]
    if len(names) != len(set(names)):
        raise ValueError("candle_target_benchmark_invalid: feature names must be unique")
    forecaster = _normalize_forecaster(config.get("forecaster"), target=operator, names=set(names))

    history = v2._mapping(config.get("rolling_history"), field="statistics.rolling_history")
    normalized_history = {
        "window_seconds": v2._integer(history.get("window_seconds"), field="rolling_history.window_seconds", minimum=seconds, maximum=86400 * 3650),
        "anchor_window": str(history.get("anchor_window") or ""),
        "label_availability": str(history.get("label_availability") or ""),
        "minimum_rows": v2._integer(history.get("minimum_rows"), field="rolling_history.minimum_rows", minimum=1, maximum=v2.MAX_CANDLES),
        "minimum_distinct_utc_dates": v2._integer(history.get("minimum_distinct_utc_dates"), field="rolling_history.minimum_distinct_utc_dates", minimum=1, maximum=3650),
        "history_eligibility": str(history.get("history_eligibility") or ""),
        "include_realized_validation_history": history.get("include_realized_validation_history"),
        "insufficient_history": str(history.get("insufficient_history") or ""),
    }
    if set(history) != set(normalized_history) or normalized_history != {
        **normalized_history,
        "anchor_window": "decision_time_left_inclusive_right_exclusive",
        "label_availability": "outcome_known_at_strictly_before_prediction",
        "history_eligibility": "shared_causal_context_complete_future_path",
        "include_realized_validation_history": True,
        "insufficient_history": "exclude_common_scoring_clock_with_reason",
    }:
        raise ValueError("candle_target_benchmark_invalid: causal rolling history contract required")

    benchmark_rows = v2._list(config.get("benchmarks"), field="statistics.benchmarks", minimum=2, maximum=2)
    benchmark_by_name = {str(row.get("name") or ""): v2._mapping(row, field="benchmark") for row in benchmark_rows}
    if set(benchmark_by_name) != {"rolling_global", "rolling_calendar"}:
        raise ValueError("candle_target_benchmark_invalid: global and calendar benchmarks required")
    global_row = benchmark_by_name["rolling_global"]
    calendar = benchmark_by_name["rolling_calendar"]
    global_prior = v2._mapping(global_row.get("prior"), field="rolling_global.prior")
    normalized_global = {
        "name": "rolling_global", "type": "rolling_target_mean",
        "prior": {
            "pseudo_count": v2._number(global_prior.get("pseudo_count"), field="rolling_global.prior.pseudo_count", minimum=0.0),
            "pseudo_mean": v2._number(global_prior.get("pseudo_mean"), field="rolling_global.prior.pseudo_mean", minimum=0.0),
        },
    }
    normalized_calendar = {
        "name": "rolling_calendar", "type": "rolling_calendar_target_mean",
        "timezone": str(calendar.get("timezone") or ""),
        "bin_seconds": v2._integer(calendar.get("bin_seconds"), field="rolling_calendar.bin_seconds", minimum=seconds, maximum=86400),
        "bin_anchor": v2._timestamp(calendar.get("bin_anchor"), field="rolling_calendar.bin_anchor"),
        "minimum_rows": v2._integer(calendar.get("minimum_rows"), field="rolling_calendar.minimum_rows", minimum=1, maximum=v2.MAX_CANDLES),
        "minimum_distinct_utc_dates": v2._integer(calendar.get("minimum_distinct_utc_dates"), field="rolling_calendar.minimum_distinct_utc_dates", minimum=1, maximum=3650),
        "shrinkage_pseudo_count": v2._number(calendar.get("shrinkage_pseudo_count"), field="rolling_calendar.shrinkage_pseudo_count", minimum=0.0),
        "fallback": str(calendar.get("fallback") or ""),
    }
    if set(global_row) != set(normalized_global) or set(calendar) != set(normalized_calendar):
        raise ValueError("candle_target_benchmark_invalid: unsupported benchmark fields")
    if (
        normalized_calendar["timezone"] != "UTC"
        or normalized_calendar["bin_seconds"] != 21600
        or normalized_calendar["bin_anchor"] != "1970-01-01T00:00:00Z"
        or normalized_calendar["fallback"] != "current_rolling_global"
    ):
        raise ValueError("candle_target_benchmark_invalid: fixed UTC calendar fallback required")
    if operator == TARGET_DIRECTION:
        if normalized_global["prior"] != {"pseudo_count": 1.0, "pseudo_mean": 0.5}:
            raise ValueError("candle_target_benchmark_invalid: Jeffreys-smoothed direction baseline required")
    elif normalized_global["prior"] != {"pseudo_count": 0.0, "pseudo_mean": 0.0}:
        raise ValueError("candle_target_benchmark_invalid: unsmoothed variance mean required")

    eligibility = v2._mapping(config.get("eligibility"), field="statistics.eligibility")
    normalized_eligibility = {
        "min_train_rows": v2._integer(eligibility.get("min_train_rows"), field="eligibility.min_train_rows", minimum=1, maximum=v2.MAX_CANDLES),
        "min_validation_rows": v2._integer(eligibility.get("min_validation_rows"), field="eligibility.min_validation_rows", minimum=1, maximum=v2.MAX_CANDLES),
        "min_train_direction_each_class": v2._integer(eligibility.get("min_train_direction_each_class"), field="eligibility.min_train_direction_each_class", minimum=1, maximum=v2.MAX_CANDLES),
        "min_validation_direction_each_class": v2._integer(eligibility.get("min_validation_direction_each_class"), field="eligibility.min_validation_direction_each_class", minimum=1, maximum=v2.MAX_CANDLES),
        "min_validation_utc_days": v2._integer(eligibility.get("min_validation_utc_days"), field="eligibility.min_validation_utc_days", minimum=1, maximum=3650),
        "min_valid_folds": v2._integer(eligibility.get("min_valid_folds"), field="eligibility.min_valid_folds", minimum=1, maximum=20),
        "min_nonempty_clusters": v2._integer(eligibility.get("min_nonempty_clusters"), field="eligibility.min_nonempty_clusters", minimum=2, maximum=v2.MAX_CANDLES),
    }
    if set(eligibility) != set(normalized_eligibility):
        raise ValueError("candle_target_benchmark_invalid: unsupported eligibility fields")

    bootstrap = v2._mapping(config.get("bootstrap"), field="statistics.bootstrap")
    normalized_bootstrap = {
        "cluster_days": v2._integer(bootstrap.get("cluster_days"), field="bootstrap.cluster_days", minimum=1, maximum=365),
        "anchor": v2._timestamp(bootstrap.get("anchor"), field="bootstrap.anchor"),
        "replicates": v2._integer(bootstrap.get("replicates"), field="bootstrap.replicates", minimum=100, maximum=100_000),
        "seed": v2._integer(bootstrap.get("seed"), field="bootstrap.seed", minimum=0, maximum=2**31 - 1),
        "confidence": v2._number(bootstrap.get("confidence"), field="bootstrap.confidence", minimum=0.5),
        "sensitivity_cluster_days": sorted(set(v2._integer(value, field="bootstrap.sensitivity_cluster_days", minimum=1, maximum=365) for value in v2._list(bootstrap.get("sensitivity_cluster_days"), field="bootstrap.sensitivity_cluster_days"))),
        "resampling_unit": str(bootstrap.get("resampling_unit") or ""),
        "undefined_ratio_draw": str(bootstrap.get("undefined_ratio_draw") or ""),
    }
    if set(bootstrap) != set(normalized_bootstrap) or normalized_bootstrap["confidence"] >= 1.0 or normalized_bootstrap["resampling_unit"] != "paired_scoring_losses" or normalized_bootstrap["undefined_ratio_draw"] != "unsupported_interval":
        raise ValueError("candle_target_benchmark_invalid: paired calendar bootstrap required")

    loss = v2._mapping(config.get("loss"), field="statistics.loss")
    criteria = v2._mapping(config.get("criteria"), field="statistics.criteria")
    if operator == TARGET_DIRECTION:
        normalized_loss = {
            "type": "bernoulli_log_loss",
            "probability_epsilon": v2._number(loss.get("probability_epsilon"), field="loss.probability_epsilon", minimum=1e-15),
            "gain": str(loss.get("gain") or ""), "skill": str(loss.get("skill") or ""),
        }
        normalized_criteria = {
            "against": sorted(str(value) for value in v2._list(criteria.get("against"), field="criteria.against", minimum=2, maximum=2)),
            "minimum_gain": v2._number(criteria.get("minimum_gain"), field="criteria.minimum_gain", minimum=0.0),
            "require_gain_ci_lower_above_zero": criteria.get("require_gain_ci_lower_above_zero"),
            "minimum_positive_folds": v2._integer(criteria.get("minimum_positive_folds"), field="criteria.minimum_positive_folds", minimum=1, maximum=20),
        }
    else:
        normalized_loss = {
            "type": "mean_squared_error", "gain": str(loss.get("gain") or ""),
            "skill": str(loss.get("skill") or ""), "zero_denominator": str(loss.get("zero_denominator") or ""),
            "secondary_normalization": str(loss.get("secondary_normalization") or ""),
        }
        normalized_criteria = {
            "against": sorted(str(value) for value in v2._list(criteria.get("against"), field="criteria.against", minimum=2, maximum=2)),
            "minimum_skill": v2._number(criteria.get("minimum_skill"), field="criteria.minimum_skill", minimum=0.0),
            "require_skill_ci_lower_above_zero": criteria.get("require_skill_ci_lower_above_zero"),
            "minimum_positive_folds": v2._integer(criteria.get("minimum_positive_folds"), field="criteria.minimum_positive_folds", minimum=1, maximum=20),
        }
    required_interval_flag = (
        normalized_criteria["require_gain_ci_lower_above_zero"]
        if operator == TARGET_DIRECTION
        else normalized_criteria["require_skill_ci_lower_above_zero"]
    )
    if (
        set(loss) != set(normalized_loss)
        or set(criteria) != set(normalized_criteria)
        or normalized_criteria["against"] != ["rolling_calendar", "rolling_global"]
        or required_interval_flag is not True
    ):
        raise ValueError("candle_target_benchmark_invalid: target-specific loss and criteria required")
    if normalized_loss["gain"] != "benchmark_minus_forecaster" or normalized_loss["skill"] != "one_minus_forecaster_mean_loss_over_benchmark_mean_loss":
        raise ValueError("candle_target_benchmark_invalid: paired gain and ratio skill required")

    normalized_statistics = {
        "target_operator": operator,
        "features": {"baseline": features, "enriched": []},
        "forecaster": forecaster,
        "rolling_history": normalized_history,
        "benchmarks": [normalized_global, normalized_calendar],
        "folds": _folds(config.get("folds")),
        "purge_bars": v2._integer(config.get("purge_bars"), field="statistics.purge_bars", minimum=0, maximum=10_000),
        "embargo_bars": v2._integer(config.get("embargo_bars"), field="statistics.embargo_bars", minimum=0, maximum=10_000),
        "eligibility": normalized_eligibility,
        "bootstrap": normalized_bootstrap,
        "loss": normalized_loss,
        "criteria": normalized_criteria,
    }
    normalized_statistics["elapsed_buffer_seconds"] = max(
        normalized_statistics["purge_bars"],
        normalized_statistics["embargo_bars"],
    ) * seconds
    if canonical_input and config != normalized_statistics:
        raise ValueError("candle_target_benchmark_invalid: canonical statistics disagree")
    return normalized_outcome, normalized_statistics


def _target_value(row: Mapping[str, Any], target: str) -> float:
    primary = row["outcomes"]["primary"]
    return float(primary["positive"]) if target == TARGET_DIRECTION else float(primary["path_variation"])


def _classes(rows: Sequence[Mapping[str, Any]]) -> dict[str, int]:
    result = Counter("up" if row["outcomes"]["primary"]["positive"] else "non_up" for row in rows)
    return {"up": int(result["up"]), "non_up": int(result["non_up"])}


def _loss(actual: float, prediction: float, *, target: str, epsilon: float) -> float:
    if target == TARGET_DIRECTION:
        probability = min(max(prediction, epsilon), 1.0 - epsilon)
        return -actual * math.log(probability) - (1.0 - actual) * math.log(1.0 - probability)
    return (actual - prediction) ** 2


def _block_id(value: str, *, days: int, anchor: str) -> int:
    clock = v2._clock(value, "decision_time")
    base = v2._clock(anchor, "bootstrap.anchor")
    return int((clock - base).total_seconds() // (days * 86400))


def _bootstrap(
    rows: Sequence[Mapping[str, Any]],
    *,
    config: Mapping[str, Any],
    days: int,
    checkpoint,
) -> dict[str, Any]:
    grouped: dict[int, list[Mapping[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[_block_id(row["decision_time"], days=days, anchor=config["anchor"])].append(row)
    keys = sorted(grouped)
    if len(keys) < 2:
        return {"status": "unsupported", "reason": "fewer_than_two_clusters", "cluster_count": len(keys)}
    rng = np.random.default_rng(int(config["seed"]))
    draws: dict[str, dict[str, list[float] | int]] = {
        name: {"gain": [], "skill": [], "undefined_skill_draws": 0}
        for name in ("rolling_global", "rolling_calendar")
    }
    for draw_index in range(int(config["replicates"])):
        if draw_index % 64 == 0:
            checkpoint()
        sampled = rng.choice(keys, size=len(keys), replace=True)
        selected = [row for key in sampled for row in grouped[int(key)]]
        model = float(np.mean([row["forecaster_loss"] for row in selected]))
        for name in draws:
            benchmark = float(np.mean([row[f"{name}_loss"] for row in selected]))
            draws[name]["gain"].append(benchmark - model)
            if benchmark == 0.0:
                draws[name]["undefined_skill_draws"] += 1
            else:
                draws[name]["skill"].append(1.0 - model / benchmark)
    alpha = (1.0 - float(config["confidence"])) / 2.0
    result: dict[str, Any] = {"status": "completed", "cluster_days": days, "cluster_count": len(keys), "replicates": int(config["replicates"]), "benchmarks": {}}
    for name, values in draws.items():
        gain = np.asarray(values["gain"], dtype=float)
        skill = np.asarray(values["skill"], dtype=float)
        result["benchmarks"][name] = {
            "gain": {"lower": float(np.quantile(gain, alpha)), "upper": float(np.quantile(gain, 1.0 - alpha))},
            "skill": ({
                "status": "unsupported",
                "reason": "undefined_ratio_draw",
                "defined_draws": int(skill.size),
                "undefined_draws": int(values["undefined_skill_draws"]),
            } if values["undefined_skill_draws"] else {
                "status": "completed",
                "lower": float(np.quantile(skill, alpha)),
                "upper": float(np.quantile(skill, 1.0 - alpha)),
                "defined_draws": int(skill.size),
                "undefined_draws": 0,
            }),
        }
    return result


def _score_summary(rows: Sequence[Mapping[str, Any]], *, name: str) -> dict[str, Any]:
    model_loss = float(np.mean([row["forecaster_loss"] for row in rows]))
    benchmark_loss = float(np.mean([row[f"{name}_loss"] for row in rows]))
    return {
        "forecaster_mean_loss": model_loss,
        "benchmark_mean_loss": benchmark_loss,
        "gain": benchmark_loss - model_loss,
        "skill": None if benchmark_loss == 0.0 else 1.0 - model_loss / benchmark_loss,
    }


def _rolling_prediction(
    *,
    row: Mapping[str, Any],
    population: Sequence[Mapping[str, Any]],
    statistics: Mapping[str, Any],
    target: str,
) -> tuple[dict[str, Any] | None, str | None]:
    decision = v2._clock(row["decision_time"], "decision_time")
    window_start = decision - timedelta(seconds=int(statistics["rolling_history"]["window_seconds"]))
    history = sorted((
        candidate for candidate in population
        if window_start <= v2._clock(candidate["decision_time"], "history.decision_time") < decision
        and v2._clock(candidate["outcome_known_at"], "history.outcome_known_at") < decision
    ), key=lambda candidate: candidate["decision_time"])
    history_dates = {candidate["decision_time"][:10] for candidate in history}
    history_config = statistics["rolling_history"]
    if len(history) < history_config["minimum_rows"] or len(history_dates) < history_config["minimum_distinct_utc_dates"]:
        return None, "rolling_global_history_insufficient"
    values = [_target_value(candidate, target) for candidate in history]
    global_config = statistics["benchmarks"][0]
    prior = global_config["prior"]
    global_prediction = (
        (sum(values) + prior["pseudo_count"] * prior["pseudo_mean"])
        / (len(values) + prior["pseudo_count"])
    )
    current_bin = decision.hour // 6
    binned = [candidate for candidate in history if v2._clock(candidate["decision_time"], "history.decision_time").hour // 6 == current_bin]
    bin_dates = {candidate["decision_time"][:10] for candidate in binned}
    calendar_config = statistics["benchmarks"][1]
    fallback_reasons = []
    if len(binned) < calendar_config["minimum_rows"]:
        fallback_reasons.append("minimum_calendar_rows_not_met")
    if len(bin_dates) < calendar_config["minimum_distinct_utc_dates"]:
        fallback_reasons.append("minimum_calendar_dates_not_met")
    calendar_prediction = global_prediction
    if not fallback_reasons:
        pseudo = float(calendar_config["shrinkage_pseudo_count"])
        calendar_prediction = (sum(_target_value(candidate, target) for candidate in binned) + pseudo * global_prediction) / (len(binned) + pseudo)
    return {
        "rolling_global_prediction": float(global_prediction),
        "rolling_calendar_prediction": float(calendar_prediction),
        "global_history_count": len(history),
        "global_history_distinct_dates": len(history_dates),
        "calendar_history_count": len(binned),
        "calendar_history_distinct_dates": len(bin_dates),
        "history_window_start": v2._iso(window_start),
        "earliest_admitted_anchor": history[0]["decision_time"],
        "latest_admitted_anchor": history[-1]["decision_time"],
        "maximum_admitted_label_known_at": max(candidate["outcome_known_at"] for candidate in history),
        "calendar_bin": current_bin,
        "calendar_fallback_reason": fallback_reasons or None,
        "history_membership_hash": _membership_hash(history),
    }, None


def _membership_token(row: Mapping[str, Any]) -> int:
    payload = f'{row["decision_time"]}\x1f{row["outcome_known_at"]}'.encode("utf-8")
    return int.from_bytes(hashlib.sha256(payload).digest(), "big")


def _membership_hash(rows: Sequence[Mapping[str, Any]]) -> str:
    accumulator = 0
    for row in rows:
        accumulator ^= _membership_token(row)
    return semantic_hash({
        "membership_count": len(rows),
        "membership_xor_sha256": f"{accumulator:064x}",
    })


def _rolling_predictions(
    *,
    rows: Sequence[Mapping[str, Any]],
    population: Sequence[Mapping[str, Any]],
    statistics: Mapping[str, Any],
    target: str,
    checkpoint,
) -> dict[str, tuple[dict[str, Any] | None, str | None]]:
    """Compute the same causal rolling forecasts once with bounded incremental state."""
    requested = sorted({row["decision_time"] for row in rows})
    pending = sorted(
        population,
        key=lambda row: (row["outcome_known_at"], row["decision_time"]),
    )
    active: dict[str, Mapping[str, Any]] = {}
    expiry_heap: list[tuple[datetime, str]] = []
    latest_heap: list[tuple[float, str]] = []
    known_heap: list[tuple[float, str]] = []
    date_counts: Counter[str] = Counter()
    bin_counts: Counter[int] = Counter()
    bin_date_counts: dict[int, Counter[str]] = defaultdict(Counter)
    bin_sums: Counter[int] = Counter()
    total_sum = 0.0
    membership_xor = 0
    pending_index = 0
    history_config = statistics["rolling_history"]
    calendar_config = statistics["benchmarks"][1]
    global_config = statistics["benchmarks"][0]
    receipts: dict[str, tuple[dict[str, Any] | None, str | None]] = {}

    def remove(key: str) -> None:
        nonlocal total_sum, membership_xor
        candidate = active.pop(key, None)
        if candidate is None:
            return
        value = _target_value(candidate, target)
        day = candidate["decision_time"][:10]
        hour_bin = v2._clock(candidate["decision_time"], "history.decision_time").hour // 6
        total_sum -= value
        bin_sums[hour_bin] -= value
        date_counts[day] -= 1
        if date_counts[day] == 0:
            del date_counts[day]
        bin_counts[hour_bin] -= 1
        bin_date_counts[hour_bin][day] -= 1
        if bin_date_counts[hour_bin][day] == 0:
            del bin_date_counts[hour_bin][day]
        membership_xor ^= _membership_token(candidate)

    for request_index, decision_text in enumerate(requested):
        if request_index % 64 == 0:
            checkpoint()
        decision = v2._clock(decision_text, "decision_time")
        window_start = decision - timedelta(seconds=int(history_config["window_seconds"]))
        while pending_index < len(pending):
            candidate = pending[pending_index]
            known = v2._clock(candidate["outcome_known_at"], "history.outcome_known_at")
            if known >= decision:
                break
            pending_index += 1
            candidate_decision = v2._clock(candidate["decision_time"], "history.decision_time")
            if candidate_decision >= decision:
                raise RuntimeError("candle_target_benchmark_invalid: label known before its decision")
            if candidate_decision < window_start:
                continue
            key = candidate["decision_time"]
            if key in active:
                raise RuntimeError("candle_target_benchmark_invalid: duplicate history decision")
            active[key] = candidate
            value = _target_value(candidate, target)
            day = key[:10]
            hour_bin = candidate_decision.hour // 6
            total_sum += value
            bin_sums[hour_bin] += value
            date_counts[day] += 1
            bin_counts[hour_bin] += 1
            bin_date_counts[hour_bin][day] += 1
            membership_xor ^= _membership_token(candidate)
            heapq.heappush(expiry_heap, (candidate_decision, key))
            heapq.heappush(latest_heap, (-candidate_decision.timestamp(), key))
            heapq.heappush(known_heap, (-known.timestamp(), key))
        while expiry_heap and expiry_heap[0][0] < window_start:
            _, key = heapq.heappop(expiry_heap)
            remove(key)
        if len(active) < history_config["minimum_rows"] or len(date_counts) < history_config["minimum_distinct_utc_dates"]:
            receipts[decision_text] = (None, "rolling_global_history_insufficient")
            continue
        prior = global_config["prior"]
        global_prediction = (
            total_sum + prior["pseudo_count"] * prior["pseudo_mean"]
        ) / (len(active) + prior["pseudo_count"])
        current_bin = decision.hour // 6
        fallback_reasons = []
        if bin_counts[current_bin] < calendar_config["minimum_rows"]:
            fallback_reasons.append("minimum_calendar_rows_not_met")
        if len(bin_date_counts[current_bin]) < calendar_config["minimum_distinct_utc_dates"]:
            fallback_reasons.append("minimum_calendar_dates_not_met")
        calendar_prediction = global_prediction
        if not fallback_reasons:
            pseudo = float(calendar_config["shrinkage_pseudo_count"])
            calendar_prediction = (
                bin_sums[current_bin] + pseudo * global_prediction
            ) / (bin_counts[current_bin] + pseudo)
        while latest_heap and latest_heap[0][1] not in active:
            heapq.heappop(latest_heap)
        while known_heap and known_heap[0][1] not in active:
            heapq.heappop(known_heap)
        earliest_key = expiry_heap[0][1]
        latest_key = latest_heap[0][1]
        maximum_known_key = known_heap[0][1]
        receipts[decision_text] = ({
            "rolling_global_prediction": float(global_prediction),
            "rolling_calendar_prediction": float(calendar_prediction),
            "global_history_count": len(active),
            "global_history_distinct_dates": len(date_counts),
            "calendar_history_count": int(bin_counts[current_bin]),
            "calendar_history_distinct_dates": len(bin_date_counts[current_bin]),
            "history_window_start": v2._iso(window_start),
            "earliest_admitted_anchor": active[earliest_key]["decision_time"],
            "latest_admitted_anchor": active[latest_key]["decision_time"],
            "maximum_admitted_label_known_at": active[maximum_known_key]["outcome_known_at"],
            "calendar_bin": current_bin,
            "calendar_fallback_reason": fallback_reasons or None,
            "history_membership_hash": semantic_hash({
                "membership_count": len(active),
                "membership_xor_sha256": f"{membership_xor:064x}",
            }),
        }, None)
    return receipts


def _diagnostic_distribution(values: Sequence[float]) -> dict[str, Any]:
    array = np.asarray([float(value) for value in values], dtype=float)
    return {
        "count": int(array.size),
        "mean": float(np.mean(array)) if array.size else None,
        "median": float(np.median(array)) if array.size else None,
        "p90": float(np.quantile(array, 0.90)) if array.size else None,
        "p99": float(np.quantile(array, 0.99)) if array.size else None,
        "maximum": float(np.max(array)) if array.size else None,
        "zero_count": int(np.count_nonzero(array == 0.0)),
    }


def _concentration_bucket() -> dict[str, Any]:
    return {
        "rows": 0,
        "target_sum": 0.0,
        "forecaster_loss_sum": 0.0,
        "rolling_global_signed_loss_difference": 0.0,
        "rolling_global_absolute_loss_difference": 0.0,
        "rolling_calendar_signed_loss_difference": 0.0,
        "rolling_calendar_absolute_loss_difference": 0.0,
    }


def _add_concentration_row(bucket: dict[str, Any], row: Mapping[str, Any]) -> None:
    bucket["rows"] += 1
    bucket["target_sum"] += row["actual_target"]
    bucket["forecaster_loss_sum"] += row["forecaster_loss"]
    for name in ("rolling_global", "rolling_calendar"):
        difference = row[f"{name}_loss"] - row["forecaster_loss"]
        bucket[f"{name}_signed_loss_difference"] += difference
        bucket[f"{name}_absolute_loss_difference"] += abs(difference)


def _concentration(rows: Sequence[Mapping[str, Any]], *, bootstrap: Mapping[str, Any]) -> dict[str, Any]:
    if not rows:
        return {"status": "unsupported", "reason": "no_scoring_rows"}
    count = max(1, math.ceil(0.01 * len(rows)))
    top_target = sorted(rows, key=lambda row: (-row["actual_target"], row["decision_time"], row["fold_id"]))[:count]
    top_squared_error = sorted(
        rows,
        key=lambda row: (
            -((row["actual_target"] - row["forecaster_prediction"]) ** 2),
            row["decision_time"],
            row["fold_id"],
        ),
    )[:count]
    by_day: dict[str, dict[str, Any]] = defaultdict(_concentration_bucket)
    by_block: dict[int, dict[str, Any]] = defaultdict(_concentration_bucket)
    totals = _concentration_bucket()
    for row in rows:
        _add_concentration_row(totals, row)
        for bucket in (by_day[row["decision_time"][:10]], by_block[_block_id(row["decision_time"], days=3, anchor=bootstrap["anchor"]) ]):
            _add_concentration_row(bucket, row)
    for buckets in (by_day, by_block):
        for bucket in buckets.values():
            for name in ("rolling_global", "rolling_calendar"):
                denominator = totals[f"{name}_absolute_loss_difference"]
                bucket[f"{name}_absolute_difference_mass_share"] = (
                    None if denominator == 0.0
                    else bucket[f"{name}_absolute_loss_difference"] / denominator
                )
    return {
        "status": "completed", "top_one_percent_count": count,
        "top_target_clock_hash": semantic_hash({"rows": [(row["fold_id"], row["decision_time"]) for row in top_target]}),
        "top_forecaster_squared_error_clock_hash": semantic_hash({
            "rows": [(row["fold_id"], row["decision_time"]) for row in top_squared_error]
        }),
        "target": _diagnostic_distribution([row["actual_target"] for row in rows]),
        "forecaster_loss": _diagnostic_distribution([row["forecaster_loss"] for row in rows]),
        "top_target": _concentration_summary(top_target),
        "top_forecaster_squared_error": _concentration_summary(top_squared_error),
        "totals": totals,
        "utc_days": dict(by_day), "three_day_blocks": {str(key): value for key, value in by_block.items()},
    }


def _concentration_summary(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    bucket = _concentration_bucket()
    for row in rows:
        _add_concentration_row(bucket, row)
    return bucket


def score_target_benchmarks(
    *,
    complete_rows: Sequence[Mapping[str, Any]],
    statistics: Mapping[str, Any],
    target: str,
    seconds: int,
    checkpoint,
) -> dict[str, Any]:
    eligibility = statistics["eligibility"]
    folds: list[dict[str, Any]] = []
    scoring_rows: list[dict[str, Any]] = []
    exclusions = Counter()
    rolling_by_decision = _rolling_predictions(
        rows=complete_rows,
        population=complete_rows,
        statistics=statistics,
        target=target,
        checkpoint=checkpoint,
    )
    for fold in statistics["folds"]:
        checkpoint()
        train_start = v2._clock(fold["train"]["start"], "fold.train.start")
        train_end = v2._clock(fold["train"]["end"], "fold.train.end")
        validation_start = v2._clock(fold["validation"]["start"], "fold.validation.start")
        validation_end = v2._clock(fold["validation"]["end"], "fold.validation.end")
        effective_train_end = train_end - timedelta(seconds=statistics["purge_bars"] * seconds)
        effective_validation_start = validation_start + timedelta(seconds=statistics["embargo_bars"] * seconds)
        train = [row for row in complete_rows if train_start <= v2._clock(row["decision_time"], "decision_time") < effective_train_end and v2._clock(row["target_time"], "target_time") < train_end]
        validation = [row for row in complete_rows if effective_validation_start <= v2._clock(row["decision_time"], "decision_time") < validation_end and v2._clock(row["target_time"], "target_time") < validation_end]
        train_classes = _classes(train)
        validation_classes = _classes(validation)
        base_reasons = []
        if len(train) < eligibility["min_train_rows"]: base_reasons.append("minimum_train_rows_not_met")
        if min(train_classes.values(), default=0) < eligibility["min_train_direction_each_class"]: base_reasons.append("minimum_train_direction_class_not_met")
        predictions: list[float] = []
        model_receipt: dict[str, Any] = {}
        forecaster = statistics["forecaster"]
        if not validation:
            base_reasons.append("validation_population_empty")
        if not base_reasons and target == TARGET_DIRECTION:
            train_x = np.asarray([[row["features"][name] for name in forecaster["features"]] for row in train], dtype=float)
            validation_x = np.asarray([[row["features"][name] for name in forecaster["features"]] for row in validation], dtype=float)
            train_y = np.asarray([_target_value(row, target) for row in train], dtype=float)
            probabilities, model_receipt = _fit_logistic(train_x, train_y, validation_x, forecaster, include_parameters=True)
            predictions = [float(value) for value in probabilities]
        elif not base_reasons:
            feature = forecaster["feature"]
            power = int(forecaster["power"])
            train_x = np.asarray([float(row["features"][feature]) ** power for row in train], dtype=float)
            train_y = np.asarray([_target_value(row, target) for row in train], dtype=float)
            denominator = float(np.sum(train_x * train_x))
            numerator = float(np.sum(train_x * train_y))
            if not math.isfinite(denominator) or denominator <= 0.0 or not math.isfinite(numerator):
                base_reasons.append("variance_calibration_denominator_invalid")
            else:
                coefficient = max(0.0, numerator / denominator)
                predictions = [coefficient * float(row["features"][feature]) ** power for row in validation]
                model_receipt = {"coefficient": coefficient, "training_count": len(train), "training_digest": semantic_hash({"rows": [(row["decision_time"], row["features"][feature], _target_value(row, target)) for row in train]})}

        if model_receipt and "training_digest" not in model_receipt:
            used_features = forecaster.get("features") or [forecaster["feature"]]
            model_receipt["training_digest"] = semantic_hash({
                "rows": [
                    (
                        row["decision_time"],
                        [row["features"][name] for name in used_features],
                        _target_value(row, target),
                    )
                    for row in train
                ]
            })

        fold_scoring: list[dict[str, Any]] = []
        epsilon = float(statistics["loss"].get("probability_epsilon", 1e-12))
        if not base_reasons:
            for row_index, (source, prediction) in enumerate(zip(validation, predictions, strict=True)):
                if row_index % 64 == 0:
                    checkpoint()
                rolling, reason = rolling_by_decision[source["decision_time"]]
                if reason:
                    exclusions[reason] += 1
                    continue
                actual = _target_value(source, target)
                scored = {
                    "fold_id": fold["id"], "decision_time": source["decision_time"],
                    "target_time": source["target_time"], "outcome_known_at": source["outcome_known_at"],
                    "actual_target": actual, "forecaster_prediction": prediction,
                    "model_training_digest": model_receipt["training_digest"],
                    **rolling,
                }
                scored["forecaster_loss"] = _loss(actual, prediction, target=target, epsilon=epsilon)
                for name in ("rolling_global", "rolling_calendar"):
                    scored[f"{name}_loss"] = _loss(actual, scored[f"{name}_prediction"], target=target, epsilon=epsilon)
                fold_scoring.append(scored)
        scoring_classes = _classes([
            row for row in validation
            if row["decision_time"] in {item["decision_time"] for item in fold_scoring}
        ])
        scoring_days = len({row["decision_time"][:10] for row in fold_scoring})
        reasons = list(base_reasons)
        if len(fold_scoring) < eligibility["min_validation_rows"]: reasons.append("minimum_validation_rows_not_met")
        if min(scoring_classes.values(), default=0) < eligibility["min_validation_direction_each_class"]: reasons.append("minimum_validation_direction_class_not_met")
        if scoring_days < eligibility["min_validation_utc_days"]: reasons.append("minimum_validation_days_not_met")
        if not reasons:
            scoring_rows.extend(fold_scoring)
        fold_result = {
            "id": fold["id"], "status": "unsupported" if reasons else "valid", "reasons": reasons,
            "train_count": len(train), "validation_structural_count": len(validation), "scoring_count": len(fold_scoring),
            "train_direction_classes": train_classes, "scoring_direction_classes": scoring_classes,
            "scoring_utc_days": scoring_days, "effective_train_end": v2._iso(effective_train_end),
            "effective_validation_start": v2._iso(effective_validation_start),
            "model": model_receipt,
            "training_clock_hash": semantic_hash({"clocks": [row["decision_time"] for row in train]}),
            "validation_clock_hash": semantic_hash({"clocks": [row["decision_time"] for row in fold_scoring]}),
        }
        if fold_scoring:
            fold_result["scores"] = {name: _score_summary(fold_scoring, name=name) for name in ("rolling_global", "rolling_calendar")}
            train_mean = float(np.mean([_target_value(row, target) for row in train])) if train else None
            if target == TARGET_VARIANCE:
                model_mse = float(np.mean([row["forecaster_loss"] for row in fold_scoring]))
                fold_result["diagnostic_mse_over_training_mean_squared"] = None if not train_mean else model_mse / (train_mean ** 2)
        folds.append(fold_result)

    valid_folds = [row for row in folds if row["status"] == "valid"]
    primary_bootstrap = _bootstrap(
        scoring_rows,
        config=statistics["bootstrap"],
        days=statistics["bootstrap"]["cluster_days"],
        checkpoint=checkpoint,
    ) if scoring_rows else {"status": "unsupported", "reason": "no_scoring_rows", "cluster_count": 0}
    sensitivities = [
        _bootstrap(
            scoring_rows,
            config=statistics["bootstrap"],
            days=days,
            checkpoint=checkpoint,
        )
        for days in statistics["bootstrap"]["sensitivity_cluster_days"]
    ] if scoring_rows else []
    comparisons = {name: _score_summary(scoring_rows, name=name) for name in ("rolling_global", "rolling_calendar")} if scoring_rows else {}
    support_reasons = []
    if len(valid_folds) < eligibility["min_valid_folds"]: support_reasons.append("minimum_valid_folds_not_met")
    if primary_bootstrap.get("cluster_count", 0) < eligibility["min_nonempty_clusters"]: support_reasons.append("minimum_nonempty_clusters_not_met")
    criteria = statistics["criteria"]
    criteria_by_benchmark = {}
    for name, score in comparisons.items():
        interval = (primary_bootstrap.get("benchmarks") or {}).get(name, {})
        positive_folds = sum(
            (fold.get("scores") or {}).get(name, {}).get("gain", 0.0) > 0.0
            if target == TARGET_DIRECTION else (fold.get("scores") or {}).get(name, {}).get("skill", -math.inf) > 0.0
            for fold in valid_folds
        )
        if target == TARGET_DIRECTION:
            criteria_by_benchmark[name] = {
                "effect_floor_met": score["gain"] >= criteria["minimum_gain"],
                "interval_above_zero": (interval.get("gain") or {}).get("lower") is not None and interval["gain"]["lower"] > 0.0,
                "positive_fold_gate_met": positive_folds >= criteria["minimum_positive_folds"],
            }
        else:
            skill_interval = interval.get("skill") or {}
            criteria_by_benchmark[name] = {
                "effect_floor_met": score["skill"] is not None and score["skill"] >= criteria["minimum_skill"],
                "interval_above_zero": skill_interval.get("lower") is not None and skill_interval["lower"] > 0.0,
                "positive_fold_gate_met": positive_folds >= criteria["minimum_positive_folds"],
            }
    all_gates = bool(criteria_by_benchmark) and all(all(value.values()) for value in criteria_by_benchmark.values())
    label_manifest = [
        {"decision_time": row["decision_time"], "target_time": row["target_time"], "outcome_known_at": row["outcome_known_at"],
         "direction": bool(row["outcomes"]["primary"]["positive"]), "sampled_variance": float(row["outcomes"]["primary"]["path_variation"])}
        for row in complete_rows
    ]
    comparison = {
        "target": target, "paired_row_count": len(scoring_rows), "benchmarks": comparisons,
        "calendar_vs_global_descriptive_gain": (
            comparisons.get("rolling_global", {}).get("benchmark_mean_loss", 0.0)
            - comparisons.get("rolling_calendar", {}).get("benchmark_mean_loss", 0.0)
        ) if comparisons else None,
        "bootstrap": primary_bootstrap, "bootstrap_sensitivity": sensitivities,
        "prediction_hash": semantic_hash({"scoring_rows": scoring_rows}),
        "screen": {"status": "unsupported" if support_reasons else "completed", "support_reasons": support_reasons,
                   "criteria_by_benchmark": criteria_by_benchmark, "all_criteria_met": all_gates},
    }
    return {
        "folds": folds, "comparison": comparison,
        "result_extensions": {
            "status": "unsupported" if support_reasons else "completed",
            "scoring_rows": scoring_rows,
            "scoring_exclusions": dict(exclusions),
            "scoring_opportunities": {
                "shared_context_complete_future_path_rows": len(complete_rows),
                "model_training_rows_by_fold": {row["id"]: row["train_count"] for row in folds},
                "structural_validation_rows_by_fold": {row["id"]: row["validation_structural_count"] for row in folds},
                "common_scoring_rows_by_fold": {row["id"]: row["scoring_count"] for row in folds},
                "pooled_common_scoring_rows": len(scoring_rows),
            },
            "manifests": {
                "shared_context_clock_hash": semantic_hash({"clocks": [row["decision_time"] for row in complete_rows]}),
                "common_training_clock_hash_by_fold": {row["id"]: row["training_clock_hash"] for row in folds},
                "common_validation_clock_hash_by_fold": {row["id"]: row["validation_clock_hash"] for row in folds},
                "paired_direction_variance_label_manifest_hash": semantic_hash({"labels": label_manifest}),
                "prediction_hash": comparison["prediction_hash"],
            },
            "concentration": _concentration(scoring_rows, bootstrap=statistics["bootstrap"]),
            "additional_caveats": [
                "Rolling benchmarks update prospectively from labels whose complete outcome was known strictly before each prediction.",
                "Bootstrap intervals are conditional on the fitted fold models and the realized benchmark forecast sequence.",
                "Losses from different target definitions are not directly comparable measures of intrinsic predictability.",
            ],
        },
    }


@dataclass(frozen=True)
class CandleTargetBenchmarkEvaluator(v2.CandleStateComparisonV2Evaluator):
    version: str = "24"
    result_schema_version: str = RESULT_SCHEMA

    def declare_requirements(self, *, definition: CheckDefinition, request: CheckRequest):
        del definition
        outcomes = request.parameters["outcomes"]
        statistics = request.parameters["statistics"]
        features = statistics["features"]["baseline"]
        start = v2._clock(request.scope.get("start"), "scope.start")
        end = v2._clock(request.scope.get("end"), "scope.end")
        step = interval_to_timedelta(str(request.scope.get("timeframe") or request.scope.get("interval") or ""))
        lookback = max([int(row.get("lookback_bars") or row.get("period") or 0) for row in features] + [int(row.get("sign_lookback_bars") or 0) for row in features], default=0)
        nominal = int((end - start) / pd.Timedelta(step))
        if start >= end or nominal + lookback + int(outcomes["horizon_bars"]) > v2.MAX_CANDLES:
            raise ValueError("candle_target_benchmark_invalid: bounded 19104-candle materialization exceeded")
        return {
            "input_kind": "market_data", "indicator_ids": [], "warmup_floor_bars": lookback,
            "feature_lookback_bars": lookback, "feature_windows_seconds_by_alias": {},
            "outcome_horizons": [int(outcomes["horizon_seconds"])], "required_outcome_horizons": [int(outcomes["horizon_seconds"])],
            "horizon_kind": "elapsed_time", "entry_lag_bars": 0, "invalidation_max_bars": 0,
            "outcome_boundary": "evaluation_end_exclusive",
            "primary_candle_required_fields": ["open", "high", "low", "close", "volume", "known_at"],
            "public_indicator_metrics": [],
        }

    def _complete_row_extensions(self, *, current, future, target):
        known_at = max([current["known_at"], target["close_time"], *(row["known_at"] for row in future)])
        return {"outcome_known_at": v2._iso(known_at)}

    def _score_complete_rows(self, *, complete_rows, statistics, seconds, checkpoint):
        target = statistics["target_operator"]
        return score_target_benchmarks(
            complete_rows=complete_rows,
            statistics=statistics,
            target=target,
            seconds=seconds,
            checkpoint=checkpoint,
        )
