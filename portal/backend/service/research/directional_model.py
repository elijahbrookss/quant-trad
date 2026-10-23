"""Paired directional fitting and scoring on procedure-admitted fold rows."""
from __future__ import annotations
from collections.abc import Mapping, Sequence
from typing import Any
import math
import numpy as np
from scipy.optimize import minimize

def _auc(y: np.ndarray, probability: np.ndarray) -> float | None:
    positives = int(np.sum(y == 1.0))
    negatives = int(np.sum(y == 0.0))
    if not positives or not negatives:
        return None
    order = np.argsort(probability, kind="mergesort")
    ranks = np.empty(len(probability), dtype=float)
    cursor = 0
    while cursor < len(order):
        end = cursor + 1
        while end < len(order) and probability[order[end]] == probability[order[cursor]]:
            end += 1
        rank = (cursor + 1 + end) / 2.0
        ranks[order[cursor:end]] = rank
        cursor = end
    rank_sum = float(np.sum(ranks[y == 1.0]))
    return (rank_sum - positives * (positives + 1) / 2.0) / (
        positives * negatives
    )


def _fit_logistic(
    x_train: np.ndarray,
    y_train: np.ndarray,
    x_validation: np.ndarray,
    model: Mapping[str, Any],
) -> tuple[np.ndarray, dict[str, Any]]:
    means = np.mean(x_train, axis=0)
    scales = np.std(x_train, axis=0, ddof=0)
    scales = np.where(scales == 0.0, 1.0, scales)
    train = (x_train - means) / scales
    validation = (x_validation - means) / scales
    fit_intercept = bool(model["fit_intercept"])
    design = (
        np.column_stack([np.ones(len(train)), train])
        if fit_intercept
        else train
    )
    validation_design = (
        np.column_stack([np.ones(len(validation)), validation])
        if fit_intercept
        else validation
    )
    c_value = float(model["c"])

    def objective(beta: np.ndarray) -> tuple[float, np.ndarray]:
        logits = np.clip(design @ beta, -40.0, 40.0)
        probabilities = 1.0 / (1.0 + np.exp(-logits))
        epsilon = 1e-15
        loss = -float(
            np.sum(
                y_train * np.log(np.clip(probabilities, epsilon, 1.0))
                + (1.0 - y_train)
                * np.log(np.clip(1.0 - probabilities, epsilon, 1.0))
            )
        )
        penalty_start = 1 if fit_intercept else 0
        penalty = beta.copy()
        penalty[:penalty_start] = 0.0
        loss += 0.5 * float(np.dot(penalty, penalty)) / c_value
        gradient = design.T @ (probabilities - y_train) + penalty / c_value
        return loss, gradient

    optimized = minimize(
        objective,
        np.zeros(design.shape[1], dtype=float),
        method="L-BFGS-B",
        jac=True,
        options={
            "ftol": float(model["tolerance"]),
            "gtol": float(model["tolerance"]),
            "maxiter": int(model["max_iterations"]),
        },
    )
    if not optimized.success:
        raise RuntimeError(
            f"event_fact_logistic_failed: {optimized.message}"
        )
    logits = np.clip(validation_design @ optimized.x, -40.0, 40.0)
    probabilities = 1.0 / (1.0 + np.exp(-logits))
    return probabilities, {
        "iterations": int(optimized.nit),
        "objective": float(optimized.fun),
        "converged": True,
    }


def _log_loss(y: np.ndarray, probability: np.ndarray) -> float:
    p = np.clip(probability, 1e-15, 1.0 - 1e-15)
    return -float(np.mean(y * np.log(p) + (1.0 - y) * np.log(1.0 - p)))


def fit_paired_fold(
    *,
    train_rows: Sequence[Mapping[str, Any]],
    validation_rows: Sequence[Mapping[str, Any]],
    baseline_names: Sequence[str],
    enriched_names: Sequence[str],
    primary_key: str,
    model: Mapping[str, Any],
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    metrics: dict[str, Any] = {}
    paired_rows: list[dict[str, Any]] = []
    y_train = np.asarray(
        [
            1.0 if row["outcomes"][primary_key]["positive"] else 0.0
            for row in train_rows
        ],
        dtype=float,
    )
    y_validation = np.asarray(
        [
            1.0 if row["outcomes"][primary_key]["positive"] else 0.0
            for row in validation_rows
        ],
        dtype=float,
    )
    baseline_train = np.asarray(
        [[row["features"][name] for name in baseline_names] for row in train_rows],
        dtype=float,
    )
    baseline_validation = np.asarray(
        [[row["features"][name] for name in baseline_names] for row in validation_rows],
        dtype=float,
    )
    enriched_train = np.asarray(
        [[row["features"][name] for name in enriched_names] for row in train_rows],
        dtype=float,
    )
    enriched_validation = np.asarray(
        [[row["features"][name] for name in enriched_names] for row in validation_rows],
        dtype=float,
    )
    baseline_probability, baseline_fit = _fit_logistic(
        baseline_train, y_train, baseline_validation, model
    )
    enriched_probability, enriched_fit = _fit_logistic(
        enriched_train, y_train, enriched_validation, model
    )
    baseline_loss = _log_loss(y_validation, baseline_probability)
    enriched_loss = _log_loss(y_validation, enriched_probability)
    baseline_brier = float(
        np.mean((baseline_probability - y_validation) ** 2)
    )
    enriched_brier = float(
        np.mean((enriched_probability - y_validation) ** 2)
    )
    baseline_auc = _auc(y_validation, baseline_probability)
    enriched_auc = _auc(y_validation, enriched_probability)
    metrics.update(
        {
            "status": "valid",
            "baseline": {
                "log_loss": baseline_loss,
                "brier": baseline_brier,
                "roc_auc": baseline_auc,
                "fit": baseline_fit,
            },
            "enriched": {
                "log_loss": enriched_loss,
                "brier": enriched_brier,
                "roc_auc": enriched_auc,
                "fit": enriched_fit,
            },
            "delta_log_loss": baseline_loss - enriched_loss,
            "delta_brier": baseline_brier - enriched_brier,
            "delta_roc_auc": (
                enriched_auc - baseline_auc
                if baseline_auc is not None
                and enriched_auc is not None
                else None
            ),
        }
    )
    for index, source_row in enumerate(validation_rows):
        actual = float(y_validation[index])
        baseline_p = float(baseline_probability[index])
        enriched_p = float(enriched_probability[index])
        paired_rows.append(
            {
                "event_time": source_row["event_time"],
                "decision_time": source_row["decision_time"],
                "actual": actual,
                "baseline_probability": baseline_p,
                "enriched_probability": enriched_p,
                "paired_log_loss_delta": (
                    -actual * math.log(max(baseline_p, 1e-15))
                    - (1.0 - actual)
                    * math.log(max(1.0 - baseline_p, 1e-15))
                    + actual * math.log(max(enriched_p, 1e-15))
                    + (1.0 - actual)
                    * math.log(max(1.0 - enriched_p, 1e-15))
                ),
            }
        )
    return metrics, paired_rows
