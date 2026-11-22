"""Existing event feature distributions, folds, bins and direct tests."""
from __future__ import annotations
import math
from collections import defaultdict
from collections.abc import Mapping, Sequence
from datetime import timedelta
from statistics import fmean, median
from typing import Any
import numpy as np
from scipy.stats import pearsonr, pointbiserialr
from core.execution_control import execution_checkpoint
from market_data.frozen import semantic_hash
from research_science import adjusted_p_values
from .event_time import _utc, _iso
from .directional_model import fit_paired_fold, _auc, _log_loss


def calculate_event_statistics(
    *, statistics, outcomes, interval_seconds, baseline_specs, enriched_specs,
    population_rows, complete_rows, eligibility_reasons, eligibility,
    folds, primary_key, minimum_validation,
):
    baseline_names = [str(row["name"]) for row in baseline_specs]
    enriched_feature_names = [str(row["name"]) for row in enriched_specs]
    distributions = {
        name: _distribution(
            [float(row["features"][name]) for row in population_rows]
        )
        for name in baseline_names
    }
    if not eligibility_reasons:
        distributions.update(
            {
                name: _distribution(
                    [float(row["features"][name]) for row in complete_rows]
                )
                for name in enriched_feature_names
            }
        )

    statistical_result: dict[str, Any] = {
        "feature_distributions": distributions,
        "feature_bins": [],
        "model": None,
        "direct_tests": [],
    }
    statistical_result["model"], fold_results = fit_event_model_comparison(
        statistics=statistics,
        outcomes=outcomes,
        interval_seconds=interval_seconds,
        baseline_names=baseline_names,
        enriched_feature_names=enriched_feature_names,
        eligibility_reasons=eligibility_reasons,
        eligibility=eligibility,
        folds=folds,
        complete_rows=complete_rows,
        primary_key=primary_key,
        minimum_validation=minimum_validation,
    )

    statistical_result["feature_bins"] = summarize_feature_bins(
        statistics=statistics,
        complete_rows=complete_rows,
        eligibility_reasons=eligibility_reasons,
        enriched_specs=enriched_specs,
        primary_key=primary_key,
    )

    statistical_result["direct_tests"] = calculate_direct_tests(
        statistics=statistics,
        complete_rows=complete_rows,
        eligibility_reasons=eligibility_reasons,
        enriched_specs=enriched_specs,
        primary_key=primary_key,
    )
    return statistical_result, fold_results
def _distribution(values: Sequence[float]) -> dict[str, Any]:
    finite = [float(value) for value in values if math.isfinite(float(value))]
    if not finite:
        return {"count": 0, "mean": None, "median": None, "std": None, "min": None, "max": None}
    return {
        "count": len(finite),
        "mean": fmean(finite),
        "median": median(finite),
        "std": float(np.std(np.asarray(finite, dtype=float), ddof=0)),
        "min": min(finite),
        "max": max(finite),
    }


def _utc_day_cluster_ci(
    rows: Sequence[Mapping[str, Any]],
    *,
    replicates: int,
    confidence: float,
    seed: int,
) -> dict[str, Any]:
    by_day: dict[str, list[float]] = defaultdict(list)
    for row in rows:
        by_day[str(row["decision_time"])[:10]].append(
            float(row["paired_log_loss_delta"])
        )
    days = sorted(by_day)
    if len(days) < 2:
        return {
            "status": "indeterminate",
            "reason": "fewer_than_two_utc_day_clusters",
            "cluster_count": len(days),
        }
    rng = np.random.default_rng(int(seed))
    estimates: list[float] = []
    for _ in range(int(replicates)):
        sampled_days = rng.choice(days, size=len(days), replace=True)
        values = [value for day in sampled_days for value in by_day[str(day)]]
        estimates.append(fmean(values))
    estimates.sort()
    alpha = (1.0 - float(confidence)) / 2.0
    return {
        "status": "completed",
        "cluster_count": len(days),
        "replicates": int(replicates),
        "confidence": float(confidence),
        "lower": float(np.quantile(estimates, alpha)),
        "upper": float(np.quantile(estimates, 1.0 - alpha)),
    }


def _descriptive_outcomes(
    population: Sequence[Mapping[str, Any]], horizons: Sequence[int]
) -> dict[str, Any]:
    """Describe eligible outcomes without asserting independence or significance."""
    by_horizon: dict[str, Any] = {}
    for horizon in horizons:
        resolved = [
            row["outcomes"][str(horizon)]
            for row in population
            if row["outcomes"].get(str(horizon), {}).get("status") == "resolved"
        ]
        returns = [float(row["direction_signed_forward_return"]) for row in resolved]
        by_horizon[str(horizon)] = {
            "resolved_count": len(resolved),
            "unresolved_count": len(population) - len(resolved),
            "positive_count": sum(value > 0 for value in returns),
            "mean_direction_signed_return": float(np.mean(returns)) if returns else None,
            "median_direction_signed_return": float(np.median(returns)) if returns else None,
            "mean_favorable_excursion": (
                float(np.mean([row["maximum_favorable_excursion"] for row in resolved]))
                if resolved else None
            ),
            "mean_adverse_excursion": (
                float(np.mean([row["maximum_adverse_excursion"] for row in resolved]))
                if resolved else None
            ),
        }
    return {
        "schema_version": "eligible_population_descriptive.v1",
        "population": "eligible_events_before_feature_complete_case_filter",
        "population_count": len(population),
        "inference": "descriptive_only_no_significance_or_execution_claim",
        "horizons": by_horizon,
    }



def fit_event_model_comparison(
    *,
    statistics,
    outcomes,
    interval_seconds,
    baseline_names,
    enriched_feature_names,
    eligibility_reasons,
    eligibility,
    folds,
    complete_rows,
    primary_key,
    minimum_validation,
):
    comparison = None
    model = dict(statistics.get("model") or {})
    fold_results: list[dict[str, Any]] = []
    paired_rows: list[dict[str, Any]] = []
    if model and not eligibility_reasons:
        enriched_names = [
            *baseline_names,
            *enriched_feature_names,
        ]
        primary_horizon_seconds = (
            int(outcomes["primary_horizon"]) * interval_seconds
            if outcomes["horizon_kind"] == "bars"
            else int(outcomes["primary_horizon"])
        )
        for fold in folds:
            execution_checkpoint()
            train_start = _utc(fold["train"]["start"], field="fold.train.start")
            train_end = _utc(fold["train"]["end"], field="fold.train.end")
            validation_start = _utc(
                fold["validation"]["start"], field="fold.validation.start"
            )
            validation_end = _utc(
                fold["validation"]["end"], field="fold.validation.end"
            )
            purge_bars = int(statistics.get("purge_bars") or 0)
            embargo_bars = int(statistics.get("embargo_bars") or 0)
            effective_train_end = min(
                train_end,
                validation_start - timedelta(seconds=purge_bars * interval_seconds),
            )
            effective_validation_start = max(
                validation_start,
                train_end + timedelta(seconds=embargo_bars * interval_seconds),
            )
            train_rows = [
                row
                for row in complete_rows
                if train_start
                <= _utc(row["decision_time"], field="decision_time")
                < effective_train_end
                and _utc(
                    row.get("entry_time") or row["event_time"],
                    field="entry_time",
                )
                + timedelta(seconds=primary_horizon_seconds)
                <= effective_validation_start
            ]
            validation_rows = [
                row
                for row in complete_rows
                if effective_validation_start
                <= _utc(row["decision_time"], field="decision_time")
                < validation_end
            ]
            fold_result: dict[str, Any] = {
                "id": fold["id"],
                "train_count": len(train_rows),
                "validation_count": len(validation_rows),
                "status": "invalid",
                "reason": None,
                "purge_bars": purge_bars,
                "embargo_bars": embargo_bars,
                "effective_train_end": _iso(effective_train_end),
                "effective_validation_start": _iso(
                    effective_validation_start
                ),
            }
            train_labels = {
                bool(row["outcomes"][primary_key]["positive"])
                for row in train_rows
            }
            if len(validation_rows) < minimum_validation:
                fold_result["reason"] = "minimum_validation_samples_not_met"
            elif len(train_labels) < 2:
                fold_result["reason"] = "training_class_missing"
            elif not baseline_names or not enriched_names:
                fold_result["reason"] = "feature_set_empty"
            else:
                metrics, fold_pairs = fit_paired_fold(
                    train_rows=train_rows, validation_rows=validation_rows,
                    baseline_names=baseline_names, enriched_names=enriched_names,
                    primary_key=primary_key, model=model,
                )
                fold_result.update(metrics)
                paired_rows.extend(fold_pairs)
            fold_results.append(fold_result)
        valid_folds = [row for row in fold_results if row["status"] == "valid"]
        if len(valid_folds) < int(eligibility.get("min_valid_folds") or 0):
            eligibility_reasons.append("minimum_valid_folds_not_met")
        if paired_rows and not eligibility_reasons:
            y_all = np.asarray([row["actual"] for row in paired_rows], dtype=float)
            baseline_all = np.asarray(
                [row["baseline_probability"] for row in paired_rows], dtype=float
            )
            enriched_all = np.asarray(
                [row["enriched_probability"] for row in paired_rows], dtype=float
            )
            bootstrap = dict(statistics.get("bootstrap") or {})
            aggregate_baseline_brier = float(
                np.mean((baseline_all - y_all) ** 2)
            )
            aggregate_enriched_brier = float(
                np.mean((enriched_all - y_all) ** 2)
            )
            aggregate_baseline_auc = _auc(y_all, baseline_all)
            aggregate_enriched_auc = _auc(y_all, enriched_all)
            comparison = {
                "implementation": dict(model),
                "baseline_features": baseline_names,
                "enriched_features": enriched_names,
                "folds": fold_results,
                "valid_fold_count": len(valid_folds),
                "positive_delta_fold_count": sum(
                    1
                    for row in valid_folds
                    if float(row.get("delta_log_loss") or 0.0) > 0.0
                ),
                "oos_count": len(paired_rows),
                "baseline": {
                    "log_loss": _log_loss(y_all, baseline_all),
                    "brier": aggregate_baseline_brier,
                    "roc_auc": aggregate_baseline_auc,
                },
                "enriched": {
                    "log_loss": _log_loss(y_all, enriched_all),
                    "brier": aggregate_enriched_brier,
                    "roc_auc": aggregate_enriched_auc,
                },
                "delta_log_loss": fmean(
                    [row["paired_log_loss_delta"] for row in paired_rows]
                ),
                "delta_brier": (
                    aggregate_baseline_brier - aggregate_enriched_brier
                ),
                "delta_roc_auc": (
                    aggregate_enriched_auc - aggregate_baseline_auc
                    if aggregate_baseline_auc is not None
                    and aggregate_enriched_auc is not None
                    else None
                ),
                "bootstrap": (
                    _utc_day_cluster_ci(
                        paired_rows,
                        replicates=int(bootstrap["replicates"]),
                        confidence=float(bootstrap["confidence"]),
                        seed=int(bootstrap["seed"]),
                    )
                    if bootstrap
                    else None
                ),
                "prediction_hash": semantic_hash(
                    {"paired_predictions": paired_rows}
                ),
            }

    return comparison, fold_results


def summarize_feature_bins(
    *,
    statistics,
    complete_rows,
    eligibility_reasons,
    enriched_specs,
    primary_key,
):
    feature_bins = []
    bin_config = dict(statistics.get("feature_bins") or {})
    if bin_config and complete_rows and not eligibility_reasons:
        for spec in enriched_specs:
            execution_checkpoint()
            name = str(spec["name"])
            values = np.asarray(
                [float(row["features"][name]) for row in complete_rows],
                dtype=float,
            )
            edges = sorted(
                {
                    float(value)
                    for value in np.quantile(
                        values,
                        list(bin_config["quantiles"]),
                        method="linear",
                    ).tolist()
                }
            )
            bins: list[dict[str, Any]] = []
            for index in range(len(edges) + 1):
                members = [
                    row
                    for row in complete_rows
                    if int(
                        np.searchsorted(
                            np.asarray(edges, dtype=float),
                            float(row["features"][name]),
                            side="right",
                        )
                    )
                    == index
                ]
                positives_in_bin = sum(
                    1
                    for row in members
                    if bool(row["outcomes"][primary_key]["positive"])
                )
                bins.append(
                    {
                        "index": index,
                        "lower_inclusive": edges[index - 1] if index else None,
                        "upper_exclusive": edges[index] if index < len(edges) else None,
                        "count": len(members),
                        "positive_count": positives_in_bin,
                        "success_rate": (
                            positives_in_bin / len(members) if members else None
                        ),
                    }
                )
            feature_bins.append(
                {
                    "feature": name,
                    "method": dict(bin_config),
                    "collapsed_edges": edges,
                    "bins": bins,
                }
            )

    return feature_bins


def calculate_direct_tests(
    *,
    statistics,
    complete_rows,
    eligibility_reasons,
    enriched_specs,
    primary_key,
):
    direct_tests = []
    direct_config = dict(statistics.get("direct_tests") or {})
    if direct_config and complete_rows and not eligibility_reasons:
        y = np.asarray(
            [
                (
                    1.0 if row["outcomes"][primary_key]["positive"] else 0.0
                )
                if direct_config["target"] == "primary_binary"
                else float(
                    row["outcomes"][primary_key][
                        (
                            "forward_return"
                            if direct_config["target"]
                            == "primary_forward_return"
                            else "direction_signed_forward_return"
                        )
                    ]
                )
                for row in complete_rows
            ],
            dtype=float,
        )
        raw_tests: list[dict[str, Any]] = []
        for spec in enriched_specs:
            name = str(spec["name"])
            values = np.asarray(
                [float(row["features"][name]) for row in complete_rows],
                dtype=float,
            )
            if len(set(values.tolist())) < 2 or len(set(y.tolist())) < 2:
                effect, p_value = 0.0, 1.0
            else:
                result = (
                    pointbiserialr(y, values)
                    if direct_config["method"] == "point_biserial"
                    else pearsonr(y, values)
                )
                effect = float(result.statistic)
                p_value = float(result.pvalue)
            raw_tests.append(
                {"feature": name, "effect": effect, "p_value": p_value}
            )
        adjusted = adjusted_p_values(
            [row["p_value"] for row in raw_tests], method="holm"
        )
        direct_tests = [
                {**row, "adjusted_p_value": adjusted[index]}
            for index, row in enumerate(raw_tests)
        ]

    return direct_tests

