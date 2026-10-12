from copy import deepcopy
from datetime import UTC, datetime, timedelta

import pytest

from market_data.frozen import semantic_hash
from research_science.check import CHECK_PLAN_SCHEMA_VERSION, ResolvedCheckPlan
from portal.backend.service.research.matched_origin_evaluator import MatchedOriginEvaluator
from portal.backend.service.research.registry import (
    CHECK_REGISTRY, materialize_check_definition, normalize_check_request,
)

START = datetime(2026, 1, 1, tzinfo=UTC)


def _time(index):
    return (START + timedelta(hours=index)).isoformat()


def _payload():
    return {
        "check_family": "event_fact_analysis", "mode": "evidence",
        "dataset_id": "mds_" + "a" * 32, "inputs": [], "gap_policy": "reject",
        "scope": {"indicator_id": "profile-1", "instrument_id": "instrument-1", "timeframe": "1h", "start": _time(0), "end": _time(6)},
        "detector": {"type": "indicator_event", "output_name": "raw", "event_keys": [{"key": "raw_long", "direction": "long"}]},
        "outcomes": {
            "horizons": [1, 2], "primary_horizon": 2, "entry_lag_bars": 1,
            "matched_origin": {
                "detector": {"type": "indicator_event", "output_name": "confirmed", "event_keys": [{"key": "confirmed_long", "direction": "long"}]},
                "origin_time_path": "metadata.breakout_time",
                "origin_event_key_path": "metadata.breakout_event_key",
                "reference_path": "metadata.reference",
            },
        },
        "statistics": {"eligibility": {"min_samples": 1}},
    }


def _plan():
    return ResolvedCheckPlan(
        schema_version=CHECK_PLAN_SCHEMA_VERSION, request_hash="test-request",
        market_data_requirements=(), indicator_graph=(),
        evaluation_range={"start": _time(0), "end_exclusive": _time(6)},
        materialization_range={"start": _time(0), "end_exclusive": _time(10)},
        warmup={"bars": 0, "seconds": 0, "timeframe_seconds": 3600},
        outcome_tail={"horizons": [1, 2], "horizon_kind": "bars", "bars": 3, "seconds": 10800},
        gap_policy="reject", quality_evidence=(),
    )


def _inputs():
    _, request = normalize_check_request(_payload())
    candles = [{"time": _time(i), "open_time": _time(i), "close_time": _time(i + 1), "known_at": _time(i + 1), "open": p, "high": p + 1, "low": p - 1, "close": p, "volume": 10} for i, p in enumerate([100, 110, 120, 90, 140, 130, 145, 150, 160, 170])]
    reference = {"key": "fixed-profile", "source": "market_profile", "price": 100}
    def event(index, confirmed=False):
        output = "confirmed" if confirmed else "raw"
        metadata = {"reference": dict(reference)}
        if confirmed:
            metadata.update(breakout_time=START.timestamp(), breakout_event_key="raw_long")
        return {"time": _time(index), "indicator_id": "profile-1", "output_type": "signal", "output_name": output, "event_key": output + "_long", "event": {"key": output + "_long", "direction": "long", "known_at": _time(index + 1), "metadata": metadata}}
    return {**request.parameters, "indicator_evidence": {"candles": candles, "outputs": [event(0), event(4), event(1, True)]}, "data_quality": {"status": "clean"}}


def _run(inputs=None):
    return MatchedOriginEvaluator().evaluate(plan=_plan(), inputs=inputs or _inputs())


def test_exact_origin_attribution_holds_endpoint_and_reconciles_mean_gap():
    inputs = _inputs()
    before = deepcopy(inputs)
    result = _run(inputs)
    attr = result["matched_origin_attribution"]
    assert inputs == before
    assert (attr["origin_count"], attr["followup_count"], attr["matched_count"], attr["unmatched_origin_count"]) == (2, 1, 1, 1)
    h = attr["horizons"]["2"]
    assert h["raw_all_mean"] == pytest.approx(((90 / 110 - 1) + (150 / 130 - 1)) / 2)
    assert h["raw_matched_mean"] == pytest.approx(90 / 110 - 1)
    assert h["common_endpoint_mean"] == pytest.approx(90 / 120 - 1)
    assert h["followup_matched_mean"] == pytest.approx(140 / 120 - 1)
    assert sum(h[key] for key in ("selection_difference", "entry_repricing_difference", "endpoint_extension_difference", "unmatched_followup_residual")) == pytest.approx(h["observed_mean_difference"])
    assert h["followup_population_reconciled"] is True
    assert attr["origins"][0]["horizons"]["1"]["common_endpoint_signed_return"] == 0
    pair = attr["origins"][0]["horizons"]["2"]
    assert pair["origin_clock"]["entry_sample_close"] == _time(2)
    assert pair["followup_clock"]["entry_sample_close"] == _time(3)
    assert pair["common_endpoint_close"] == _time(4)
    assert pair["followup_endpoint_close"] == _time(5)
    assert result["events"][0]["features"] == {}
    assert "matched_origin_attribution" not in result["followup_result"]
    assert semantic_hash(result) == semantic_hash(_run())
    assert result["hashes"]["matched_origin_attribution_hash"] == semantic_hash(attr)


def test_short_direction_and_entry_state_are_not_relabelled():
    inputs = _inputs()
    inputs["detector"]["event_keys"][0].update(key="raw_short", direction="short")
    inputs["outcomes"]["matched_origin"]["detector"]["event_keys"][0].update(key="confirmed_short", direction="short")
    for row in inputs["indicator_evidence"]["outputs"]:
        row["event_key"] = row["event_key"].replace("long", "short")
        row["event"]["direction"] = "short"
        row["event"]["key"] = row["event_key"]
        if row["output_name"] == "confirmed":
            row["event"]["metadata"]["breakout_event_key"] = "raw_short"
    attr = _run(inputs)["matched_origin_attribution"]
    assert attr["horizons"]["2"]["common_endpoint_mean"] == pytest.approx(0.25)
    assert attr["origins"][0]["origin_entry_state"] == "inside"
    assert attr["origins"][0]["followup_entry_state"] == "inside"


def test_entry_at_boundary_is_distinct_from_inside_and_outside():
    inputs = _inputs()
    for row in inputs["indicator_evidence"]["outputs"]:
        row["event"]["metadata"]["reference"]["price"] = 120
    row = _run(inputs)["matched_origin_attribution"]["origins"][0]
    assert row["origin_entry_state"] == "inside"
    assert row["followup_entry_state"] == "at_boundary"


@pytest.mark.parametrize("mutation,reason", [("profile", "origin_reference_changed"), ("time", "origin_absent_in_evaluation_window"), ("indicator", "origin_absent_in_evaluation_window")])
def test_mismatched_reference_origin_time_or_indicator_never_uses_nearest_event(mutation, reason):
    inputs = _inputs()
    follow = inputs["indicator_evidence"]["outputs"][2]
    if mutation == "profile":
        follow["event"]["metadata"]["reference"]["key"] = "changed-profile"
    elif mutation == "time":
        follow["event"]["metadata"]["breakout_time"] = (START + timedelta(minutes=1)).timestamp()
    else:
        follow["indicator_id"] = "other-indicator"
    attr = _run(inputs)["matched_origin_attribution"]
    assert attr["matched_count"] == 0
    assert attr["unmatched_followups"][0]["reason"] == reason
    h = attr["horizons"]["2"]
    assert h["common_endpoint_mean"] is None
    assert h["selection_difference"] is None
    assert h["followup_population_reconciled"] is False


@pytest.mark.parametrize("index", [0, 2])
def test_duplicate_origin_or_followup_fails_loud(index):
    inputs = _inputs()
    inputs["indicator_evidence"]["outputs"].append(deepcopy(inputs["indicator_evidence"]["outputs"][index]))
    with pytest.raises(ValueError, match="ambiguous duplicate"):
        _run(inputs)


def test_missing_origin_link_fails_instead_of_guessing():
    inputs = _inputs()
    del inputs["indicator_evidence"]["outputs"][2]["event"]["metadata"]["breakout_time"]
    with pytest.raises(ValueError, match="required event identity is missing"):
        _run(inputs)


def test_unmatched_followup_residual_is_explicit_for_partial_population():
    inputs = _inputs()
    follow = deepcopy(inputs["indicator_evidence"]["outputs"][2])
    follow["time"] = _time(3)
    follow["event"]["known_at"] = _time(4)
    follow["event"]["metadata"]["breakout_time"] = (START - timedelta(hours=1)).timestamp()
    inputs["indicator_evidence"]["outputs"].append(follow)
    attr = _run(inputs)["matched_origin_attribution"]
    h = attr["horizons"]["2"]
    assert (h["followup_count"], h["common_pair_count"]) == (2, 1)
    assert h["followup_population_reconciled"] is False
    assert h["unmatched_followup_residual"] != 0
    assert sum(h[key] for key in ("selection_difference", "entry_repricing_difference", "endpoint_extension_difference", "unmatched_followup_residual")) == pytest.approx(h["observed_mean_difference"])


def test_delayed_followup_after_common_endpoint_is_unresolved_not_negative_duration():
    inputs = _inputs()
    follow = inputs["indicator_evidence"]["outputs"][2]
    follow["time"] = _time(4)
    follow["event"]["known_at"] = _time(5)
    attr = _run(inputs)["matched_origin_attribution"]
    assert attr["matched_count"] == 1
    assert attr["horizons"]["2"]["unresolved_pair_reasons"] == {"followup_entry_after_common_endpoint": 1}
    assert attr["horizons"]["2"]["common_endpoint_mean"] is None


def test_gap_and_evaluation_cutoff_preserve_unresolved_and_unmatched_cases():
    inputs = _inputs()
    inputs["indicator_evidence"]["candles"] = [row for row in inputs["indicator_evidence"]["candles"] if row["open_time"] != _time(3)]
    attr = _run(inputs)["matched_origin_attribution"]
    assert attr["horizons"]["2"]["unresolved_pair_reasons"] == {"arm_population_ineligible": 1}
    inputs = _inputs()
    inputs["indicator_evidence"]["outputs"][2]["event"]["known_at"] = _time(7)
    attr = _run(inputs)["matched_origin_attribution"]
    assert attr["matched_count"] == 0
    assert attr["followup_count"] == 0
    assert attr["unmatched_origin_count"] == 2
    assert "not_proof" in attr["followup_scope"]


def test_noncontemporaneous_entry_and_nonfinite_prices_are_rejected():
    inputs = _inputs()
    inputs["indicator_evidence"]["candles"][2]["known_at"] = _time(4)
    with pytest.raises(ValueError, match="contemporaneously available"):
        _run(inputs)
    inputs = _inputs()
    inputs["indicator_evidence"]["outputs"][0]["event"]["metadata"]["reference"]["price"] = float("nan")
    with pytest.raises(ValueError, match="reference.price must be finite"):
        _run(inputs)


def test_recorded_gap_blocks_before_pairing():
    inputs = _inputs()
    inputs["indicator_gap_rejection"] = {"reason": "test_gap"}
    result = _run(inputs)
    assert result["status"] == "blocked"
    assert "matched_origin_attribution" not in result


def test_version_seven_routes_only_explicit_attribution_and_preserves_old_requirements():
    payload = _payload()
    definition, request = normalize_check_request(payload)
    assert definition.definition_version.startswith("7+")
    assert definition.evaluator_version == "6"
    evaluator = CHECK_REGISTRY.resolve_evaluator(definition)
    requirements = evaluator.declare_requirements(definition=definition, request=request)
    legacy = deepcopy(payload)
    del legacy["outcomes"]["matched_origin"]
    old_definition, old_request = normalize_check_request(legacy)
    assert old_definition.definition_version.startswith("6+")
    old = CHECK_REGISTRY.resolve_evaluator(old_definition)
    assert requirements == old.declare_requirements(definition=old_definition, request=old_request)
    assert request.parameters["outcomes"]["matched_origin"]["origin_time_path"] == "metadata.breakout_time"
    with pytest.raises(ValueError, match="requires.*version 7"):
        materialize_check_definition(payload, mode="evidence", base_version="6")
    changed = deepcopy(payload)
    changed["outcomes"]["matched_origin"]["origin_time_path"] = "metadata.other_origin"
    assert materialize_check_definition(changed, mode="evidence").definition_hash != definition.definition_hash


@pytest.mark.parametrize("mutation", ["features", "elapsed", "unknown", "same_output", "empty"])
def test_admission_rejects_unimplemented_or_silently_ignored_configuration(mutation):
    payload = _payload()
    if mutation == "features":
        payload["statistics"]["features"] = {"baseline": [{"name": "direction", "operator": "event_direction"}]}
    elif mutation == "elapsed":
        payload["outcomes"]["horizon_kind"] = "elapsed_time"
    elif mutation == "unknown":
        payload["outcomes"]["matched_origin"]["nearest_time"] = True
    elif mutation == "same_output":
        payload["outcomes"]["matched_origin"]["detector"]["output_name"] = "raw"
    else:
        payload["outcomes"]["matched_origin"] = {}
    with pytest.raises(ValueError, match="matched_origin_invalid"):
        normalize_check_request(payload)


def test_unmatched_contributing_entry_clock_is_validated_too():
    inputs = _inputs()
    inputs["indicator_evidence"]["candles"][5]["known_at"] = _time(7)
    with pytest.raises(ValueError, match="contemporaneously available"):
        _run(inputs)


def test_followup_and_pair_eligibility_cannot_inherit_raw_qualification():
    inputs = _inputs()
    inputs["statistics"]["eligibility"]["min_samples"] = 2
    result = _run(inputs)
    assert result["analysis_status"] == "insufficient_evidence"
    attr = result["matched_origin_attribution"]
    assert attr["eligibility"]["origin"]["eligible"] is True
    assert attr["eligibility"]["followup"]["eligible"] is False
    assert attr["horizons"]["2"]["paired_eligibility"]["eligible"] is False
    assert result["eligibility"]["eligible"] is False
    # Enough events in both arms does not imply enough exact common pairs.
    follow = deepcopy(inputs["indicator_evidence"]["outputs"][2])
    follow["time"] = _time(3)
    follow["event"]["known_at"] = _time(4)
    follow["event"]["metadata"]["breakout_time"] = (START - timedelta(hours=1)).timestamp()
    inputs["indicator_evidence"]["outputs"].append(follow)
    result = _run(inputs)
    attr = result["matched_origin_attribution"]
    assert attr["eligibility"]["followup"]["eligible"] is True
    assert result["analysis_status"] == "insufficient_evidence"
    assert attr["horizons"]["2"]["common_endpoint_mean"] is not None
    assert "paired:2:minimum_common_pair_count_not_met" in result["eligibility"]["reasons"]


@pytest.mark.parametrize("price", [0, -1, float("inf"), float("nan")])
def test_invalid_primary_close_prices_fail_before_return_calculation(price):
    inputs = _inputs()
    inputs["indicator_evidence"]["candles"][1]["close"] = price
    with pytest.raises(ValueError, match="candle.close must be finite and positive"):
        _run(inputs)
