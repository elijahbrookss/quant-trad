from copy import deepcopy

import pytest

from portal.backend.service.research import catalog
from portal.backend.service.research.candle_state_comparison_v3_evaluator import (
    RESULT_SCHEMA,
    SCHEMA,
)
from portal.backend.service.research.registry import (
    CHECK_REGISTRY,
    definition_allows_evidence,
    materialize_check_definition,
    normalize_check_request,
)
from tests.test_portal.test_candle_state_comparison_v2 import (
    BASE,
    STEP,
    candles,
    contract,
    iso,
    payload as v2_payload,
    run,
)


def payload():
    result = deepcopy(v2_payload())
    result["outcomes"]["schema_version"] = SCHEMA
    result["statistics"]["features"] = {
        "baseline": [
            {"name": "r1", "operator": "lagged_return", "lookback_bars": 1},
            {"name": "r4", "operator": "lagged_return", "lookback_bars": 4},
            {"name": "atr3", "operator": "atr_fraction", "period": 3},
        ],
        "enriched": [
            {"name": "relative_volume_5", "operator": "volume_ratio",
             "lookback_bars": 5, "transform": "identity"},
            {"name": "signed_relative_volume_5", "operator": "volume_ratio",
             "lookback_bars": 5, "transform": "times_return_sign",
             "sign_lookback_bars": 4, "zero_sign": "zero"},
        ],
    }
    return result


def transformed_return_payload():
    result = payload()
    result["statistics"]["features"]["enriched"] = [
        {"name": "signed_r1", "operator": "lagged_return", "lookback_bars": 1,
         "transform": "times_return_sign", "sign_lookback_bars": 4,
         "zero_sign": "zero"},
    ]
    return result


def audit_row(result, candle_index):
    decision = iso(BASE + STEP * (candle_index + 1))
    return next(
        row for row in result["selection_audit"]["rows"]
        if row["decision_time"] == decision
    )


def test_v23_registers_without_changing_v21_or_v22_contracts():
    definition, request, _ = contract(payload())
    assert definition.definition_version.startswith("23+")
    assert definition.evaluator_version == "23"
    assert definition_allows_evidence(definition)
    assert request.parameters["outcomes"]["schema_version"] == SCHEMA
    assert CHECK_REGISTRY.resolve_definition("raw_forward_outcome", "21").definition_hash == (
        "b6a2a10f1ae19edb7636bcdee13e0fe3cf229c9e99e82673f369d0e0a5efff97"
    )
    assert CHECK_REGISTRY.resolve_definition("raw_forward_outcome", "22").definition_hash == (
        "41f489e72c0a3d52da338b9e4d16296188c64786f6bccba581f450fb8b63570b"
    )
    detail = catalog.get_check_definition("raw_forward_outcome", "23")
    assert detail["eligibility"]["modes"] == ["evidence"]
    assert detail["result_shape"]["analysis_field"] == "comparison"


def test_v22_still_refuses_ordinary_feature_transforms():
    request = v2_payload()
    request["statistics"]["features"]["baseline"][0].update({
        "transform": "times_return_sign", "sign_lookback_bars": 4,
        "zero_sign": "zero",
    })
    with pytest.raises(ValueError, match="unsupported fields for feature"):
        normalize_check_request(request)


def test_v23_retains_transform_configuration_and_full_warmup():
    request = transformed_return_payload()
    request["statistics"]["features"]["enriched"][0]["sign_lookback_bars"] = 60
    definition, normalized, _ = contract(request)
    declaration = CHECK_REGISTRY.resolve_evaluator(definition).declare_requirements(
        definition=definition, request=normalized,
    )
    feature = normalized.parameters["statistics"]["features"]["enriched"][0]
    assert feature == {
        "lookback_bars": 1,
        "name": "signed_r1",
        "operator": "lagged_return",
        "sign_lookback_bars": 60,
        "transform": "times_return_sign",
        "zero_sign": "zero",
    }
    assert declaration["feature_lookback_bars"] == 60
    first = materialize_check_definition(payload(), mode="evidence")
    changed = payload()
    changed["statistics"]["features"]["enriched"][1]["sign_lookback_bars"] = 8
    assert first.definition_hash != materialize_check_definition(
        changed, mode="evidence",
    ).definition_hash


def test_v23_admits_historical_sign_on_either_comparison_side():
    request = payload()
    request["statistics"]["features"]["baseline"][0].update({
        "transform": "times_return_sign",
        "sign_lookback_bars": 4,
        "zero_sign": "zero",
    })
    definition, normalized, _ = contract(request)
    assert definition.evaluator_version == "23"
    assert normalized.parameters["statistics"]["features"]["baseline"][0] == {
        "lookback_bars": 1,
        "name": "r1",
        "operator": "lagged_return",
        "sign_lookback_bars": 4,
        "transform": "times_return_sign",
        "zero_sign": "zero",
    }


def test_signed_volume_uses_causal_historical_direction_and_flat_maps_zero():
    frame = candles()
    result = run(frame, p=payload())
    row = audit_row(result, 120)
    unsigned = row["feature_context"]["relative_volume_5"]
    signed = row["feature_context"]["signed_relative_volume_5"]
    current = float(frame.loc[120, "close"])
    prior = float(frame.loc[116, "close"])
    expected_sign = 1.0 if current > prior else -1.0 if current < prior else 0.0
    assert signed == pytest.approx(unsigned * expected_sign)

    flat = candles()
    flat.loc[120, "close"] = float(flat.loc[116, "close"])
    flat.loc[120, "high"] = max(float(flat.loc[120, "open"]), float(flat.loc[120, "close"])) + .1
    flat.loc[120, "low"] = min(float(flat.loc[120, "open"]), float(flat.loc[120, "close"])) - .1
    flat_row = audit_row(run(flat, p=payload()), 120)
    assert flat_row["feature_context"]["signed_relative_volume_5"] == 0.0


@pytest.mark.parametrize("late_known", [False, True])
def test_sign_history_requires_every_causal_native_candle(late_known):
    frame = candles()
    if late_known:
        frame.loc[118, "known_at"] = iso(BASE + STEP * 170)
    else:
        frame = frame.drop(index=118).reset_index(drop=True)
    result = run(frame, p=transformed_return_payload())
    row = audit_row(result, 120)
    assert row["feature_context"]["signed_r1"] is None
    assert "enriched_feature_unavailable" in row["reason_flags"]


def test_v23_result_and_selection_audit_retain_transformed_context():
    frame = candles()
    before = frame.copy(deep=True)
    result = run(frame, p=payload())
    assert result["schema_version"] == RESULT_SCHEMA
    assert result["method_version"] == "23"
    assert result["sample_count"] > 100
    assert result["selection_audit"]["schema_version"] == "candle_state_selection_audit.v1"
    assert result["selection_audit"]["selected_count"] == result["sample_count"]
    selected = next(
        row for row in result["selection_audit"]["rows"]
        if row["selected_for_paired_model"]
    )
    assert "signed_relative_volume_5" in selected["feature_context"]
    assert result["comparison"]["enriched_features"][-2:] == [
        "relative_volume_5", "signed_relative_volume_5",
    ]
    assert result["dependence"]["recursive_indicator_state_crosses_folds"] is False
    assert frame.equals(before)
