"""Registered generic Indicator-event barrier Check; no scientific run."""
from copy import deepcopy
from datetime import UTC, datetime, timedelta

import pytest

from portal.backend.service.research import catalog
from portal.backend.service.research.planning import plan_research_check
from portal.backend.service.research.registry import (
    CHECK_REGISTRY,
    normalize_check_preparation_request,
    normalize_check_request,
)
from research_science.recognition import plain, utc
from tests.test_portal.test_reversal_population_evidence import (
    SOURCE,
    native_binding,
)

ORIGIN = datetime(2024, 1, 1, tzinfo=UTC)
STEP = timedelta(minutes=15)


def payload():
    request = deepcopy(
        catalog.get_check_definition("event_fact_analysis", "15")["examples"][0][
            "request"
        ]
    )
    request["scope"].update(
        instrument_id="synthetic-instrument",
        indicator_id="synthetic-breakout-indicator",
        start=plain(ORIGIN + 4 * STEP),
        end=plain(ORIGIN + 20 * STEP),
    )
    request["outcomes"]["input_origin"] = plain(ORIGIN)
    return request


def planned():
    raw = payload()
    definition, request = normalize_check_request(raw)
    plan = plan_research_check(
        definition,
        request,
        inspect_coverage=False,
        instrument_loader=lambda key: {"id": key, "symbol": "SYNTHETIC"},
        indicator_planner=lambda ids, **kwargs: {
            "indicators": [
                {
                    "indicator_id": ids[0],
                    "indicator_type": "synthetic_breakout",
                    "parameters": {},
                }
            ],
            "requirements": [],
            "warmup_bars": 4,
        },
    )
    return definition, request, plan


def candle_rows(changes=None):
    changes = changes or {}
    return [
        {
            "open_time": plain(ORIGIN + index * STEP),
            "close_time": plain(ORIGIN + (index + 1) * STEP),
            "known_at": plain(ORIGIN + (index + 1) * STEP),
            "source_identity_key": SOURCE,
            "open": 10.0,
            "high": changes.get(index, {}).get("high", 10.5),
            "low": changes.get(index, {}).get("low", 9.5),
            "close": 10.0,
        }
        for index in range(20)
    ]


def breakout_event():
    at = ORIGIN + 4 * STEP
    return {
        "indicator_id": "synthetic-breakout-indicator",
        "output_type": "signal",
        "output_name": "breakouts",
        "event_key": "breakout_long",
        "time": plain(at - STEP),
        "known_at": plain(at),
        "event": {
            "pattern_id": "breakout-one",
            "direction": "long",
            "known_at": plain(at),
            "metadata": {
                "failure_price": 9.0,
                "target_price": 11.0,
                "strength": 2.0,
            },
        },
    }


def inputs_for(request, plan):
    binding = native_binding(request, plan, 20)
    return {
        **request.parameters,
        "frozen_input_binding": binding,
        "data_quality": {"recorded_gaps": []},
        "indicator_evidence": {
            "schema_version": "indicator_output_evidence.v1",
            "candles": candle_rows({4: {"low": 8.5}, 5: {"high": 11.5}}),
            "outputs": [breakout_event()],
            "gap_transitions": [],
        },
    }


def test_registered_request_plans_one_complete_origin_and_evidence_only():
    definition, request, plan = planned()
    evaluator = CHECK_REGISTRY.resolve_evaluator(definition)
    assert definition.definition_version.startswith("15+")
    assert evaluator.version == "15"
    assert plan.execution["indicator_ids"] == ["synthetic-breakout-indicator"]
    assert plan.execution["warmup_floor_bars"] == 4
    assert plan.outcome_tail["seconds"] == 0
    assert utc(plan.materialization_range["start"]) == ORIGIN
    assert catalog.get_check_definition("event_fact_analysis", "15")["settings"][
        "defaults"
    ]["mode"] == "evidence"
    normalize_check_preparation_request(payload())


@pytest.mark.parametrize(
    "change",
    [
        lambda request: request.update(mode="preview"),
        lambda request: request.update(gap_policy="continue_degraded"),
        lambda request: request.update(
            assertions=[
                {"metric_path": "sample_count", "operator": "gte", "threshold": 1}
            ]
        ),
        lambda request: request["detector"]["event_keys"][0].update(
            direction="short"
        ),
        lambda request: request["outcomes"].update(horizon_bars=13),
        lambda request: request["outcomes"]["failure"].update(
            path="payload.private"
        ),
    ],
)
def test_registered_request_rejects_scope_or_operator_relaxation(change):
    request = payload()
    change(request)
    with pytest.raises(ValueError):
        normalize_check_request(request)


def test_registered_evaluator_uses_non_reversal_public_event_and_same_result_followup():
    definition, request, plan = planned()
    evaluator = CHECK_REGISTRY.resolve_evaluator(definition)
    result = evaluator.evaluate(plan=plan, inputs=inputs_for(request, plan))
    case = result["cases"][0]
    assert result["stage"] == "first_passage_and_strictly_later_target"
    assert case["event"]["metadata"]["strength"] == 2.0
    assert case["outcome"]["classification"] == "failure"
    assert case["outcome"]["observed_bars"] == 1
    assert case["later_target"]["status"] == "positive"
    assert case["later_target"]["first_later_target"]["bar_index"] == 2
    assert result["later_target_summary"]["statuses"]["positive"][
        "share_of_failure_mass"
    ] == 1.0
    assert result["rules"]["event_ownership"] == "indicator"
    assert result["rules"]["trading_authority"] is False


def test_registered_evaluator_refuses_binding_event_and_clock_substitution():
    definition, request, plan = planned()
    evaluator = CHECK_REGISTRY.resolve_evaluator(definition)
    inputs = inputs_for(request, plan)
    missing = deepcopy(inputs)
    missing.pop("frozen_input_binding")
    with pytest.raises(ValueError, match="actual frozen binding"):
        evaluator.evaluate(plan=plan, inputs=missing)
    wrong_range = deepcopy(inputs)
    wrong_range["frozen_input_binding"]["series"][0]["range_start"] = plain(
        ORIGIN + STEP
    )
    with pytest.raises(ValueError, match="primary range differs"):
        evaluator.evaluate(plan=plan, inputs=wrong_range)
    wrong_indicator = deepcopy(inputs)
    wrong_indicator["indicator_evidence"]["outputs"][0][
        "indicator_id"
    ] = "different-indicator"
    with pytest.raises(ValueError, match="Indicator identity differs"):
        evaluator.evaluate(plan=plan, inputs=wrong_indicator)
    wrong_clock = deepcopy(inputs)
    wrong_clock["indicator_evidence"]["outputs"][0]["known_at"] = plain(
        ORIGIN + 5 * STEP
    )
    with pytest.raises(ValueError, match="known_at differs"):
        evaluator.evaluate(plan=plan, inputs=wrong_clock)
