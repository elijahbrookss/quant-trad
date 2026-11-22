"""Complete-result fixtures pinned before the computation-only refactor.

Expected hashes were produced by dfdb6a92, not by the extracted helpers.
"""
from copy import deepcopy
from dataclasses import replace
from datetime import UTC, datetime, timedelta
import math
import json
from pathlib import Path

import pytest
from market_data.frozen import semantic_hash
from portal.backend.service.research.event_fact_evaluator import EventFactEvaluator, normalize_event_fact_configuration
from tests.test_portal.test_event_fact_check import _inputs, _plan, _record, _iso
from tests.test_portal.test_first_return_check import run as first_return


def model_case(*, fit_intercept=True):
    inputs = _inputs()
    start = datetime(2026, 1, 1, tzinfo=UTC)
    template = deepcopy(inputs["indicator_evidence"]["outputs"][0])
    candles, outputs, records = [], [], []
    for index in range(64):
        opened = start + timedelta(hours=index)
        close = 100 + math.sin(index * 1.7) * 3 + index * .05
        candles.append({"open_time": _iso(opened), "close_time": _iso(opened+timedelta(hours=1)),
                        "open": close-.1, "close": close, "high": close+1, "low": close-1, "volume": index+10})
        output = deepcopy(template)
        output["time"] = _iso(opened)
        output["event"]["known_at"] = _iso(opened+timedelta(hours=1))
        outputs.append(output)
        records.append(_record(index, value=str(100+index*.1), known_at=opened+timedelta(hours=1)))
    inputs["indicator_evidence"].update(candles=candles, outputs=outputs)
    inputs["fact_records_by_alias"]["reference_price"] = records
    baseline = [
        {"name":"direction", "operator":"event_direction"},
        {"name":"metadata", "operator":"event_metadata_number", "path":"metadata.distance_from_reference_pct", "scale":10000},
        {"name":"signed", "operator":"direction_signed_return", "lookback_bars":2},
        {"name":"lagged", "operator":"lagged_return", "lookback_bars":2},
        {"name":"atr", "operator":"atr_fraction", "period":3},
        {"name":"volume", "operator":"volume_ratio", "lookback_bars":2},
    ]
    detector, outcomes, statistics = normalize_event_fact_configuration(
        detector=inputs["detector"], outcomes=inputs["outcomes"], statistics={
            "features":{"baseline":baseline,"enriched":[{"name":"reference", "operator":"latest_value", "input_alias":"reference_price"}]},
            "model":{"fit_intercept":fit_intercept},
            "folds":[{"id":"chronological", "train":{"start":_iso(start+timedelta(hours=3)), "end":_iso(start+timedelta(hours=30))},
                      "validation":{"start":_iso(start+timedelta(hours=32)), "end":_iso(start+timedelta(hours=60))}}],
            "eligibility":{"min_samples":1, "min_validation_samples":1, "min_valid_folds":1},
        })
    inputs.update(detector=detector, outcomes=outcomes, statistics=statistics)
    plan = replace(_plan(gap_policy="continue_degraded"), plan_hash="",
        evaluation_range={"start":_iso(start), "end_exclusive":_iso(start+timedelta(hours=62))})
    return inputs, plan


def _assert_model_values(actual, expected):
    if isinstance(expected, dict):
        assert actual.keys() == expected.keys()
        for key, value in expected.items():
            if key == "iterations":
                assert 0 < actual[key] <= 1000
            else:
                _assert_model_values(actual[key], value)
    elif isinstance(expected, list):
        assert len(actual) == len(expected)
        for item, value in zip(actual, expected):
            _assert_model_values(item, value)
    elif isinstance(expected, float):
        assert actual == pytest.approx(expected, rel=1e-10, abs=1e-12)
    else:
        assert actual == expected


@pytest.mark.parametrize("fit_intercept", [True, False])
def test_feature_and_paired_model_result_preserved(fit_intercept):
    inputs, plan = model_case(fit_intercept=fit_intercept)
    original = deepcopy(inputs)
    result = EventFactEvaluator().evaluate(inputs=inputs, plan=plan)
    expected = json.loads((Path(__file__).parent / "fixtures/research_computation_model_baseline.json").read_text())["expected"][str(fit_intercept)]
    assert inputs == original
    model = result["statistics"].pop("model")
    model.pop("prediction_hash")  # Exact same-environment hashes are retained in the handoff receipt.
    _assert_model_values(model, expected["model"])
    result["hashes"].pop("statistics_hash")
    # This hash also contains optimizer metrics, not just discrete fold membership.
    result["hashes"].pop("fold_assignment_hash")
    assert semantic_hash(result) == expected["non_model_result_hash"]


def test_first_return_complete_result_preserved():
    assert semantic_hash(first_return()) == "9bb1d1d7a20214af2577f602762538e993badadb5446e5a87df4e3b1439d32d5"


def test_model_convergence_failure_remains_loud():
    inputs, plan = model_case()
    inputs["statistics"]["model"]["max_iterations"] = 1
    with pytest.raises(RuntimeError, match="event_fact_logistic_failed"):
        EventFactEvaluator().evaluate(inputs=inputs, plan=plan)


@pytest.mark.parametrize("procedure", ["matched", "landmark", "first_return"])
def test_owned_procedure_prepares_candles_once_without_mutating_inputs(monkeypatch, procedure):
    from portal.backend.service.research import event_preparation
    from tests.test_portal import test_matched_origin_check as matched
    from tests.test_portal import test_shared_landmark_check as landmark
    from tests.test_portal import test_first_return_check as first
    cases = {"matched": (matched._inputs, matched._plan, matched.MatchedOriginEvaluator),
             "landmark": (landmark._case, None, landmark.SharedLandmarkEvaluator),
             "first_return": (first.case, None, first.FirstReturnEvaluator)}
    builder, plan_builder, evaluator = cases[procedure]
    if plan_builder:
        inputs, plan = builder(), plan_builder()
    else:
        inputs, plan = builder()
    original = deepcopy(inputs)
    convert = event_preparation._candle_rows
    calls = []
    def count_conversion(value, *, field):
        calls.append(field)
        return convert(value, field=field)
    monkeypatch.setattr(event_preparation, "_candle_rows", count_conversion)
    evaluator().evaluate(inputs=inputs, plan=plan)
    assert calls == ["indicator_evidence.candles"]
    assert inputs == original


def test_preparation_is_fresh_for_each_public_evaluation():
    from tests.test_portal import test_first_return_check as first
    inputs, plan = first.case()
    evaluator = first.FirstReturnEvaluator()
    before = evaluator.evaluate(inputs=inputs, plan=plan)
    candle = inputs["indicator_evidence"]["candles"][1]
    for field in ["open", "high", "low", "close"]:
        candle[field] += 1
    after = evaluator.evaluate(inputs=inputs, plan=plan)
    assert after["events"][0]["entry_price"] == before["events"][0]["entry_price"] + 1
