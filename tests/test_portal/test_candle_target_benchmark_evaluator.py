from copy import deepcopy

import pytest

from portal.backend.service.research import catalog
from portal.backend.service.research.candle_target_benchmark_evaluator import (
    RESULT_SCHEMA,
    SCHEMA,
    TARGET_DIRECTION,
    TARGET_VARIANCE,
    _bootstrap,
    _rolling_prediction,
    _rolling_predictions,
)
from portal.backend.service.research.registry import (
    CHECK_REGISTRY,
    definition_allows_evidence,
    materialize_check_definition,
)
from tests.test_portal.test_candle_state_comparison_v2 import (
    BASE,
    STEP,
    candles,
    contract,
    iso,
    run,
)


def payload(*, target=TARGET_DIRECTION):
    start = BASE + STEP * 20
    result = {
        "check_family": "raw_forward_outcome", "mode": "evidence",
        "dataset_id": "mds_" + "a" * 32,
        "scope": {"instrument_id": "instrument-1", "timeframe": "15m",
                  "start": iso(start), "end": iso(BASE + STEP * 370)},
        "detector": {"type": "candle_close", "sampling": "ordinary_native_close"},
        "outcomes": {"schema_version": SCHEMA, "horizon_seconds": 3600,
                     "horizon_bars": 4, "outcome_boundary": "evaluation_end_exclusive",
                     "complete_path": True, "target": {}},
        "statistics": {
            "features": [
                {"name": "r1", "operator": "lagged_return", "lookback_bars": 1},
                {"name": "r4", "operator": "lagged_return", "lookback_bars": 4},
                {"name": "atr3", "operator": "atr_fraction", "period": 3}],
            "forecaster": {},
            "rolling_history": {
                "window_seconds": 172800,
                "anchor_window": "decision_time_left_inclusive_right_exclusive",
                "label_availability": "outcome_known_at_strictly_before_prediction",
                "minimum_rows": 20, "minimum_distinct_utc_dates": 1,
                "history_eligibility": "shared_causal_context_complete_future_path",
                "include_realized_validation_history": True,
                "insufficient_history": "exclude_common_scoring_clock_with_reason"},
            "benchmarks": [],
            "folds": [
                {"id": "F1", "train": {"start": iso(start), "end": iso(BASE + STEP * 160)},
                 "validation": {"start": iso(BASE + STEP * 160), "end": iso(BASE + STEP * 240)}},
                {"id": "F2", "train": {"start": iso(start), "end": iso(BASE + STEP * 240)},
                 "validation": {"start": iso(BASE + STEP * 240), "end": iso(BASE + STEP * 350)}}],
            "purge_bars": 4, "embargo_bars": 4,
            "eligibility": {"min_train_rows": 20, "min_validation_rows": 10,
                            "min_train_direction_each_class": 2,
                            "min_validation_direction_each_class": 2,
                            "min_validation_utc_days": 1, "min_valid_folds": 2,
                            "min_nonempty_clusters": 2},
            "bootstrap": {"cluster_days": 1, "anchor": "1970-01-01T00:00:00Z",
                          "replicates": 100, "seed": 7, "confidence": .95,
                          "sensitivity_cluster_days": [2],
                          "resampling_unit": "paired_scoring_losses",
                          "undefined_ratio_draw": "unsupported_interval"},
            "loss": {}, "criteria": {}},
        "gap_policy": "reset_rewarm",
    }
    if target == TARGET_DIRECTION:
        result["outcomes"]["target"] = {
            "operator": TARGET_DIRECTION, "definition_version": "terminal_direction.v1",
            "flat": "class_zero"}
        result["statistics"]["forecaster"] = {
            "type": "standardized_l2_logistic", "features": ["r1", "r4", "atr3"],
            "c": 1.0, "fit_intercept": True, "seed": 0, "tolerance": 1e-9,
            "max_iterations": 1000, "freeze": "nominal_training_end"}
        result["statistics"]["benchmarks"] = [
            {"name": "rolling_global", "type": "rolling_target_mean",
             "prior": {"pseudo_count": 1.0, "pseudo_mean": .5}},
            {"name": "rolling_calendar", "type": "rolling_calendar_target_mean",
             "timezone": "UTC", "bin_seconds": 21600,
             "bin_anchor": "1970-01-01T00:00:00Z", "minimum_rows": 5,
             "minimum_distinct_utc_dates": 1, "shrinkage_pseudo_count": 20,
             "fallback": "current_rolling_global"}]
        result["statistics"]["loss"] = {
            "type": "bernoulli_log_loss", "probability_epsilon": 1e-12,
            "gain": "benchmark_minus_forecaster",
            "skill": "one_minus_forecaster_mean_loss_over_benchmark_mean_loss"}
        result["statistics"]["criteria"] = {
            "against": ["rolling_global", "rolling_calendar"], "minimum_gain": 0.0,
            "require_gain_ci_lower_above_zero": True, "minimum_positive_folds": 1}
    else:
        result["outcomes"]["target"] = {
            "operator": TARGET_VARIANCE,
            "definition_version": "native_log_return_sum_squares.v1",
            "study_profile": "four_native_return_sum_squares.v1",
            "meaning": "configured native horizon"}
        result["statistics"]["forecaster"] = {
            "type": "nonnegative_scaled_feature", "feature": "atr3", "power": 2,
            "input_scale": "raw", "fit_intercept": False,
            "calibration": "sum_xy_over_sum_x_squared", "freeze": "nominal_training_end"}
        result["statistics"]["benchmarks"] = [
            {"name": "rolling_global", "type": "rolling_target_mean",
             "prior": {"pseudo_count": 0.0, "pseudo_mean": 0.0}},
            {"name": "rolling_calendar", "type": "rolling_calendar_target_mean",
             "timezone": "UTC", "bin_seconds": 21600,
             "bin_anchor": "1970-01-01T00:00:00Z", "minimum_rows": 5,
             "minimum_distinct_utc_dates": 1, "shrinkage_pseudo_count": 20,
             "fallback": "current_rolling_global"}]
        result["statistics"]["loss"] = {
            "type": "mean_squared_error", "gain": "benchmark_minus_forecaster",
            "skill": "one_minus_forecaster_mean_loss_over_benchmark_mean_loss",
            "zero_denominator": "undefined_unsupported",
            "secondary_normalization": "fold_training_target_mean_squared"}
        result["statistics"]["criteria"] = {
            "against": ["rolling_global", "rolling_calendar"], "minimum_skill": 0.0,
            "require_skill_ci_lower_above_zero": True, "minimum_positive_folds": 1}
    return result


def test_v24_registers_without_changing_historical_contracts():
    definition, request, _ = contract(payload())
    assert definition.definition_version.startswith("24+")
    assert definition.evaluator_version == "24"
    assert definition_allows_evidence(definition)
    assert request.parameters["outcomes"]["target"]["operator"] == TARGET_DIRECTION
    assert catalog.get_check_definition("raw_forward_outcome", "24")["eligibility"]["modes"] == ["evidence"]
    assert CHECK_REGISTRY.resolve_definition("raw_forward_outcome", "21").definition_hash == "b6a2a10f1ae19edb7636bcdee13e0fe3cf229c9e99e82673f369d0e0a5efff97"
    assert CHECK_REGISTRY.resolve_definition("raw_forward_outcome", "22").definition_hash == "41f489e72c0a3d52da338b9e4d16296188c64786f6bccba581f450fb8b63570b"
    assert CHECK_REGISTRY.resolve_definition("raw_forward_outcome", "23").definition_hash == "36df82b862d58aa7f645727347557387b679314436603091f9e084f0c3b7aba8"


@pytest.mark.parametrize("target", [TARGET_DIRECTION, TARGET_VARIANCE])
def test_target_scoring_is_deterministic_causal_and_retains_typed_rows(target):
    frame = candles()
    result = run(frame, p=payload(target=target))
    assert result["schema_version"] == RESULT_SCHEMA
    assert result["method_version"] == "24"
    assert len(result["scoring_rows"]) > 20
    assert len(result["folds"]) == 2
    assert result["comparison"]["target"] == target
    for row in result["scoring_rows"]:
        assert row["maximum_admitted_label_known_at"] < row["decision_time"]
        assert row["global_history_count"] >= 20
        assert set(("forecaster_loss", "rolling_global_loss", "rolling_calendar_loss",
                    "model_training_digest")) <= set(row)
    again = run(frame, p=payload(target=target))
    assert result["comparison"]["prediction_hash"] == again["comparison"]["prediction_hash"]
    assert set(result["manifests"]) == {
        "shared_context_clock_hash", "common_training_clock_hash_by_fold",
        "common_validation_clock_hash_by_fold",
        "paired_direction_variance_label_manifest_hash", "prediction_hash",
    }
    assert result["concentration"]["target"]["p99"] is not None
    assert result["concentration"]["top_forecaster_squared_error_clock_hash"]


def test_sampled_variance_uses_all_four_returns_not_endpoint_or_square_root():
    frame = candles()
    # Force a round-trip path: the one-hour endpoint is unchanged but path variance is positive.
    current = float(frame.loc[199, "close"])
    for index, close in zip(range(200, 204), [current + 2.0, current - 1.0, current + 1.5, current], strict=True):
        opened = float(frame.loc[index, "open"])
        frame.loc[index, "close"] = close
        frame.loc[index, "high"] = max(opened, close) + .1
        frame.loc[index, "low"] = min(opened, close) - .1
    result = run(frame, p=payload(target=TARGET_VARIANCE))
    decision = iso(BASE + STEP * 200)
    row = next(item for item in result["scoring_rows"] if item["decision_time"] == decision)
    assert float(frame.loc[203, "close"]) == current
    assert row["actual_target"] > 0.0


def test_rolling_history_strictly_rejects_equal_or_unfinished_labels_and_falls_back():
    config = payload()["statistics"]
    normalized = materialize_check_definition(payload(), mode="evidence").material_rules
    del normalized  # Definition hashing is covered above; exercise the rolling algorithm directly.
    _, request, _ = contract(payload())
    statistics = request.parameters["statistics"]
    decision = BASE + STEP * 200
    population = []
    for index in range(25):
        anchor = decision - STEP * (index + 5)
        known = decision - STEP if index else decision
        population.append({
            "decision_time": iso(anchor), "target_time": iso(anchor + STEP * 4),
            "outcome_known_at": iso(known), "features": {},
            "outcomes": {"primary": {"positive": index % 2 == 0, "path_variation": float(index + 1)}}})
    current = {"decision_time": iso(decision), "target_time": iso(decision + STEP * 4),
               "outcome_known_at": iso(decision + STEP * 4), "features": {},
               "outcomes": {"primary": {"positive": True, "path_variation": 1.0}}}
    receipt, reason = _rolling_prediction(row=current, population=population,
                                          statistics=statistics, target=TARGET_DIRECTION)
    assert reason is None
    assert receipt["global_history_count"] == 24
    assert receipt["maximum_admitted_label_known_at"] < current["decision_time"]
    assert receipt["calendar_fallback_reason"] is not None
    incremental = _rolling_predictions(
        rows=[current], population=population, statistics=statistics,
        target=TARGET_DIRECTION, checkpoint=lambda: None,
    )[current["decision_time"]]
    assert incremental == (receipt, reason)


def test_variance_calibration_is_frozen_before_validation_outcomes():
    frame = candles()
    baseline = run(frame, p=payload(target=TARGET_VARIANCE))
    poisoned = frame.copy(deep=True)
    for index in range(190, len(poisoned)):
        close = float(poisoned.loc[index, "close"]) * (1.03 if index % 2 else .97)
        opened = float(poisoned.loc[index, "open"])
        poisoned.loc[index, "close"] = close
        poisoned.loc[index, "high"] = max(opened, close) + .1
        poisoned.loc[index, "low"] = min(opened, close) - .1
    changed = run(poisoned, p=payload(target=TARGET_VARIANCE))
    baseline_f1 = next(row for row in baseline["folds"] if row["id"] == "F1")
    changed_f1 = next(row for row in changed["folds"] if row["id"] == "F1")
    assert baseline_f1["model"] == changed_f1["model"]
    assert baseline_f1["scores"] != changed_f1["scores"]


def test_any_undefined_bootstrap_skill_draw_marks_the_interval_unsupported():
    rows = [
        {"decision_time": f"2024-01-0{day}T01:00:00Z", "forecaster_loss": 0.0,
         "rolling_global_loss": 0.0, "rolling_calendar_loss": 0.0}
        for day in (1, 2, 3)
    ]
    result = _bootstrap(
        rows,
        config={"anchor": "1970-01-01T00:00:00Z", "seed": 7,
                "replicates": 100, "confidence": .95},
        days=1,
        checkpoint=lambda: None,
    )
    assert result["status"] == "completed"
    for benchmark in result["benchmarks"].values():
        assert benchmark["skill"]["status"] == "unsupported"
        assert benchmark["skill"]["undefined_draws"] == 100


def test_future_poison_does_not_change_earlier_rolling_forecasts():
    frame = candles()
    first = run(frame, p=payload())
    poisoned = frame.copy(deep=True)
    poisoned.loc[360:, "close"] *= 2.0
    poisoned.loc[360:, "high"] = poisoned.loc[360:, ["open", "close"]].max(axis=1) + .1
    poisoned.loc[360:, "low"] = poisoned.loc[360:, ["open", "close"]].min(axis=1) - .1
    second = run(poisoned, p=payload())
    cutoff = iso(BASE + STEP * 330)
    first_prefix = [row for row in first["scoring_rows"] if row["decision_time"] < cutoff]
    second_prefix = [row for row in second["scoring_rows"] if row["decision_time"] < cutoff]
    assert first_prefix == second_prefix


def test_target_configuration_changes_definition_hash_and_v23_refuses_it():
    direction = materialize_check_definition(payload(), mode="evidence")
    variance = materialize_check_definition(payload(target=TARGET_VARIANCE), mode="evidence")
    assert direction.definition_hash != variance.definition_hash
    old = deepcopy(payload())
    old["outcomes"]["schema_version"] = "candle_state_paired_comparison.v3"
    with pytest.raises(ValueError):
        materialize_check_definition(old, mode="evidence")
