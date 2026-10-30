from copy import deepcopy
from datetime import UTC, datetime, timedelta

import pandas as pd
import pytest

from research_science.check import CHECK_PLAN_SCHEMA_VERSION, ResolvedCheckPlan
from portal.backend.service.research import catalog
from portal.backend.service.research import candle_state_comparison_evaluator as evaluator_module
from portal.backend.service.research.candle_state_comparison_evaluator import (
    CandleStateComparisonEvaluator, RESULT_SCHEMA, SCHEMA, _cluster_ci,
)
from portal.backend.service.research.registry import (
    CHECK_REGISTRY, definition_allows_evidence, materialize_check_definition,
    normalize_check_request, pin_check_definition_to_plan,
    reconstruct_pinned_check_definition,
)


BASE = datetime(2024, 1, 1, tzinfo=UTC)
STEP = timedelta(minutes=15)


def iso(value):
    return value.isoformat().replace("+00:00", "Z")


def params():
    return {
        "atr_short_window": 3, "atr_long_window": 5, "atr_z_window": 5,
        "directional_efficiency_window": 4, "slope_window": 3,
        "range_window": 4, "expansion_window": 3, "volume_window": 5,
        "overlap_window": 3, "slope_stability_lookback": 4,
        "warmup_bars": 10, "atr_expansion_signal_threshold": 2.0,
    }


def payload():
    start = BASE + STEP * 80
    f1_end = BASE + STEP * 180
    f1_validation_end = BASE + STEP * 240
    f2_validation_end = BASE + STEP * 330
    indicator = {"type": "candle_stats", "version": "v1", "output": "candle_stats",
                 "field": "directional_efficiency", "params": params()}
    return {
        "check_family": "raw_forward_outcome", "mode": "evidence",
        "dataset_id": "mds_" + "a" * 32,
        "scope": {"instrument_id": "instrument-1", "timeframe": "15m",
                  "start": iso(start), "end": iso(BASE + STEP * 370)},
        "detector": {"type": "candle_close", "sampling": "ordinary_native_close"},
        "outcomes": {"schema_version": SCHEMA, "horizon_seconds": 3600,
            "primary": {"type": "forward_close_gt_current", "flat": "class_zero"},
            "continuation": {"lookback_bars": 4, "zero_prior": "undefined_prior_direction",
                             "future_flat": "separate"},
            "magnitude": {"endpoint": "absolute_log_return",
                          "path": "sum_squared_native_log_returns"},
            "outcome_boundary": "evaluation_end_exclusive"},
        "statistics": {
            "features": {
                "baseline": [
                    {"name": "r1", "operator": "lagged_return", "lookback_bars": 1},
                    {"name": "r4", "operator": "lagged_return", "lookback_bars": 4},
                    {"name": "atr3", "operator": "atr_fraction", "period": 3},
                    {"name": "vol5", "operator": "volume_ratio", "lookback_bars": 5}],
                "enriched": [
                    {"name": "eff4", "operator": "indicator_metric", "indicator": indicator,
                     "transform": "identity"},
                    {"name": "signed_eff4", "operator": "indicator_metric", "indicator": indicator,
                     "transform": "times_return_sign", "sign_lookback_bars": 4,
                     "zero_sign": "zero"}]},
            "folds": [
                {"id": "F1", "train": {"start": iso(start), "end": iso(f1_end)},
                 "validation": {"start": iso(f1_end), "end": iso(f1_validation_end)}},
                {"id": "F2", "train": {"start": iso(start), "end": iso(f1_validation_end)},
                 "validation": {"start": iso(f1_validation_end), "end": iso(f2_validation_end)}}],
            "model": {"type": "standardized_l2_logistic", "c": 1.0, "seed": 0,
                      "fit_intercept": True, "tolerance": 1e-9, "max_iterations": 1000},
            "purge_bars": 4, "embargo_bars": 4,
            "bootstrap": {"cluster_days": 1, "sensitivity_cluster_days": [2],
                          "anchor": "1970-01-01T00:00:00Z", "replicates": 100,
                          "confidence": .95, "seed": 7},
            "eligibility": {"min_train_rows": 20, "min_validation_rows": 10,
                            "min_train_each_class": 2, "min_validation_each_class": 2,
                            "min_validation_utc_days": 1, "min_valid_folds": 2,
                            "min_nonempty_clusters": 2},
            "criteria": {"min_delta_log_loss": 0.0, "require_interval_above_zero": True,
                         "min_positive_folds": 1, "interpretation": "forecast_screen_only"}},
        "gap_policy": "reset_rewarm",
    }


def candles(count=390):
    rows = []
    close = 100.0
    for index in range(count):
        # Deterministic alternating regimes provide both outcome classes.
        move = (.24 if (index // 11) % 2 == 0 else -.21) + ((index % 5) - 2) * .015
        opened = close
        close = max(10.0, opened + move)
        stamp = BASE + STEP * index
        rows.append({
            "timestamp": iso(stamp), "close_time": iso(stamp + STEP),
            "known_at": iso(stamp + STEP), "known_at_method": "interval_close_inferred",
            "source_identity_key": "source-1", "open": opened,
            "high": max(opened, close) + .1, "low": min(opened, close) - .1,
            "close": close, "volume": float(100 + index % 13),
        })
    return pd.DataFrame(rows).astype({key: "float64" for key in ("open", "high", "low", "close", "volume")})


def contract(p=None):
    p = p or payload()
    definition, request = normalize_check_request(p)
    evaluator = CHECK_REGISTRY.resolve_evaluator(definition)
    declaration = evaluator.declare_requirements(definition=definition, request=request)
    plan = ResolvedCheckPlan(
        schema_version=CHECK_PLAN_SCHEMA_VERSION, request_hash=request.request_hash,
        market_data_requirements=({"alias": "primary_bars", "instrument_id": "instrument-1",
            "timeframe_seconds": 900,
            "required_fields": declaration["primary_candle_required_fields"]},),
        indicator_graph=(), evaluation_range={"start": p["scope"]["start"],
            "end_exclusive": p["scope"]["end"]},
        materialization_range={"start": iso(BASE), "end_exclusive": p["scope"]["end"]},
        warmup={"bars": declaration["feature_lookback_bars"], "timeframe_seconds": 900},
        outcome_tail={"bars": None, "seconds": 3600, "horizon_kind": "elapsed_time"},
        gap_policy="reset_rewarm",
    )
    return definition, request, plan


def run(frame=None, p=None):
    definition, request, plan = contract(p)
    return CHECK_REGISTRY.resolve_evaluator(definition).evaluate(
        plan=plan, inputs={**request.parameters, "candles": frame if frame is not None else candles(),
                           "data_quality": {"status": "clean"}})


def test_registered_generic_contract_and_catalog_are_exact():
    definition, request, plan = contract()
    assert definition.definition_version.startswith("21+")
    assert definition.evaluator_version == "21" and definition_allows_evidence(definition)
    assert request.parameters["outcomes"]["horizon_bars"] == 4
    assert request.parameters["statistics"]["elapsed_buffer_seconds"] == 3600
    evaluator = CHECK_REGISTRY.resolve_evaluator(definition)
    declaration = evaluator.declare_requirements(definition=definition, request=request)
    assert declaration["feature_lookback_bars"] == 10
    assert declaration["horizon_kind"] == "elapsed_time"
    definition, request, plan = pin_check_definition_to_plan(definition, request, plan)
    assert reconstruct_pinned_check_definition(request, plan).to_dict() == definition.to_dict()
    detail = catalog.get_check_definition("raw_forward_outcome", "21")
    assert detail["result_shape"]["analysis_field"] == "comparison"
    assert detail["eligibility"]["modes"] == ["evidence"]


def test_ordinary_close_paired_comparison_is_deterministic_and_nonmutating():
    frame = candles(); before = frame.copy(deep=True)
    result = run(frame)
    pd.testing.assert_frame_equal(frame, before)
    assert result["schema_version"] == RESULT_SCHEMA
    assert result["population"] == "ordinary_native_candle_closes"
    assert result["sample_count"] == result["opportunities"]["paired_complete_rows"] > 100
    assert result["opportunities"]["observed_native_closes"] > result["sample_count"]
    assert len(result["folds"]) == 2 and all(row["status"] == "valid" for row in result["folds"])
    assert result["comparison"]["paired_row_count"] > 20
    assert result["comparison"]["bootstrap"]["cluster_days"] == 1
    assert result["comparison"]["bootstrap_sensitivity"][0]["cluster_days"] == 2
    assert result["provenance"]["fit_and_standardization"] == "training_rows_only_same_paired_population"
    assert result["dependence"]["buffer_is_not_independence_guarantee"] is True
    assert result["provenance"]["population_hash"] == run(frame)["provenance"]["population_hash"]


def test_gap_resets_state_and_exact_elapsed_path_never_bridges_surviving_bars():
    frame = candles()
    missing_open = iso(BASE + STEP * 210)
    changed = run(frame[frame.timestamp != missing_open].reset_index(drop=True))
    assert changed["exclusions"]["exact_future_path_missing"] >= 4
    assert changed["exclusions"]["indicator_not_ready"] >= params()["warmup_bars"]
    assert changed["opportunities"]["nominal_utc_grid_slots"] > changed["opportunities"]["observed_native_closes"]


def test_late_known_indicator_state_and_invalid_volume_are_explicitly_excluded():
    late = candles()
    late.loc[120, "known_at"] = iso(BASE + STEP * 170)
    late_result = run(late)
    assert late_result["exclusions"]["indicator_not_known_at_decision"] > 0
    invalid_volume = candles()
    invalid_volume.loc[210, "volume"] = None
    volume_result = run(invalid_volume)
    assert volume_result["exclusions"]["baseline_volume_history_unavailable"] > 0


def test_exact_flat_outcomes_and_flat_prior_direction_remain_visible():
    frame = candles()
    flat_close = float(frame.loc[196, "close"])
    for index in range(197, 205):
        frame.loc[index, ["open", "close"]] = flat_close
        frame.loc[index, "high"] = flat_close + .1
        frame.loc[index, "low"] = flat_close - .1
    result = run(frame)
    counts = result["diagnostics"]["direction_and_continuation"]
    assert counts["primary_flat"] > 0
    assert counts["undefined_prior_direction"] > 0


def test_cooperative_cancellation_propagates_without_mutating_input(monkeypatch):
    frame = candles(); before = frame.copy(deep=True)

    def cancelled():
        raise RuntimeError("cancelled-by-test")

    monkeypatch.setattr(evaluator_module, "execution_checkpoint", cancelled)
    with pytest.raises(RuntimeError, match="cancelled-by-test"):
        run(frame)
    pd.testing.assert_frame_equal(frame, before)


@pytest.mark.parametrize("change,match", [
    (("detector",), "ordinary native"),
    (("gap_policy",), "reset_rewarm"),
    (("metric",), "unsupported public Indicator"),
    (("flat",), "up versus non-up"),
])
def test_incompatible_selection_metric_gap_or_flat_semantics_fail_closed(change, match):
    p = payload()
    if change == ("detector",): p["detector"] = {"type": "indicator_event"}
    if change == ("gap_policy",): p["gap_policy"] = "continue_degraded"
    if change == ("metric",): p["statistics"]["features"]["enriched"][0]["indicator"]["field"] = "atr_ratio"
    if change == ("flat",): p["outcomes"]["primary"]["flat"] = "exclude"
    with pytest.raises(ValueError, match=match):
        normalize_check_request(p)


def test_fixed_calendar_clusters_preserve_gaps_instead_of_row_blocking():
    rows = [
        {"decision_time": "1970-01-01T12:00:00Z", "paired_log_loss_delta": .1},
        {"decision_time": "1970-01-02T12:00:00Z", "paired_log_loss_delta": .2},
        {"decision_time": "1970-01-10T12:00:00Z", "paired_log_loss_delta": -.1},
    ]
    result = _cluster_ci(rows, cluster_days=3, anchor="1970-01-01T00:00:00Z",
                         replicates=100, confidence=.95, seed=3)
    assert result["cluster_count"] == 2
    assert result["empty_calendar_cluster_count"] == 2


def test_configuration_is_hashed_and_not_hardcoded_to_campaign_choices():
    first = materialize_check_definition(payload(), mode="evidence")
    changed = payload()
    changed["outcomes"]["horizon_seconds"] = 1800
    changed["outcomes"]["continuation"]["lookback_bars"] = 8
    changed["statistics"]["features"]["enriched"][1]["sign_lookback_bars"] = 8
    second = materialize_check_definition(changed, mode="evidence")
    assert first.definition_hash != second.definition_hash
    assert second.material_rules["outcomes"]["horizon_bars"] == 2
    assert second.material_rules["outcomes"]["continuation"]["lookback_bars"] == 8
