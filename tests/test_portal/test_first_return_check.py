from copy import deepcopy
from dataclasses import replace

import pytest

from market_data.frozen import semantic_hash
from portal.backend.service.research.first_return_evaluator import FirstReturnEvaluator, RETURNED, OUTSIDE
from portal.backend.service.research.registry import normalize_check_request, materialize_check_definition
from tests.test_portal.test_matched_origin_check import _inputs, _payload, _plan, _time, START


def request():
    payload = _payload()
    payload["detector"] = {"type":"indicator_event", "output_name":"balance_breakout", "event_keys":[{"key":"balance_breakout_long","direction":"long"}]}
    payload["outcomes"].pop("matched_origin")
    payload["outcomes"].update(horizons=[2,3], required_horizons=[2], primary_horizon=2,
        first_return={"classification_lag_bars":1,"sample_lag_bars":2,
            "readiness_contract":"market_profile.first_return_state.v2","dependence":"leave_one_original_profile_out.v1"})
    return payload


def case():
    inputs = _inputs()
    _, req = normalize_check_request(request()); inputs.update(req.parameters)
    outputs = inputs["indicator_evidence"]["outputs"]
    reference = {"source":"market_profile", "key":"fixed-profile", "price":101,
                 "context":{"active_value_area":{"val":99,"vah":101,"poc":100}}}
    for output in outputs[:2]:
        output.update(output_name="balance_breakout",event_key="balance_breakout_long")
        output["event"].update(key="balance_breakout_long")
        output["event"]["metadata"]["reference"] = deepcopy(reference)
    outputs[2].update(output_name="first_value_return",event_key="first_value_return_long")
    outputs[2]["event"].update(key="first_value_return_long")
    outputs[2]["event"]["metadata"].update(reference=deepcopy(reference),breakout_event_key="balance_breakout_long")
    for i in range(8):
        origin_index = 0 if i<4 else 4
        outputs.append({"indicator_id":"profile-1", "time":_time(i), "output_name":"first_return_state", "output_type":"context",
            "value":{"state_key":"observed", "fields":{"prior_atr":10.,"prior_atr_ready":True,"prior_atr_known_at":_time(i),"lifetime_bars":6,
                "origins":[{"breakout_time":_time(origin_index),"breakout_event_key":"balance_breakout_long", "reference":deepcopy(reference),
                    "observed_known_at":_time(i+1),"status":"returned" if i in (1,2,3) else "no_return_observed",
                    "location":"inside" if i in (1,2,3) else "above","first_return_time":_time(1) if i in (1,2,3) else None}]}}})
    return inputs, replace(_plan(), plan_hash="", evaluation_range={"start":_time(0),"end_exclusive":_time(8)})


def run(inputs=None, plan=None):
    default_inputs, default_plan = case()
    return FirstReturnEvaluator().evaluate(inputs=inputs or default_inputs,plan=plan or default_plan)


def test_explicit_new_definition_and_legacy_registration_stable():
    before = materialize_check_definition(_payload(),mode="evidence")
    definition, _ = normalize_check_request(request())
    assert definition.definition_version.startswith("9+") and definition.evaluator_version == "8"
    assert before == materialize_check_definition(_payload(),mode="evidence")
    with pytest.raises(ValueError,match="first_return_invalid"):
        materialize_check_definition(request(),mode="evidence",base_version="8")


def test_same_clock_original_endpoint_and_poc_distance_not_signed_return():
    inputs, plan = case(); original = deepcopy(inputs)
    result = run(inputs,plan); analysis=result["first_return_comparison"]
    first, second = analysis["origins"]
    assert first["classification"] == RETURNED and second["classification"] == OUTSIDE
    assert first["classification_cutoff"] == _time(2)
    assert first["entry_price"] == 120 and first["prior_atr"] == 10
    assert first["outcomes"]["2"]["distance_reduction_atr"] == 1  # distance20 ->10
    assert second["outcomes"]["2"]["distance_reduction_atr"] == -.5  #45 ->50
    assert analysis["horizons"]["2"]["comparison"]["returned_minus_outside"] == 1.5
    assert first["outcomes"]["2"]["center_crossing_status"] == "crossed"
    assert inputs == original
    assert result["hashes"]["first_return_comparison_hash"] == semantic_hash(analysis)
    assert semantic_hash(result) == semantic_hash(run(inputs,plan))


@pytest.mark.parametrize("mutation,reason",[("missing_signal","public_return_signal_unavailable"),("late_signal","return_not_known_by_landmark"),
    ("no_context","public_origin_state_unavailable"),("changed_reference","original_reference_disagrees"),
    ("gap","candle_gap"),("late_candle","classification_candle_late")])
def test_unknown_or_late_observations_never_become_negative_group(mutation,reason):
    inputs, plan=case(); outputs=inputs["indicator_evidence"]["outputs"]
    context=next(o for o in outputs if o["output_name"]=="first_return_state" and o["time"]==_time(1))
    state=context["value"]["fields"]["origins"][0]
    if mutation=="missing_signal": outputs.pop(2)
    if mutation=="late_signal": outputs[2]["event"]["known_at"]=_time(3)
    if mutation=="no_context": outputs.remove(context)
    if mutation=="changed_reference": state["reference"]["key"]="different"
    if mutation=="gap": state.update(status="censored",reason="candle_gap")
    if mutation=="late_candle": inputs["indicator_evidence"]["candles"][1]["known_at"]=_time(3)
    row=run(inputs,plan)["first_return_comparison"]["origins"][0]
    assert row["classification"]=="unresolved" and row["classification_reason"]==reason
    assert row["outcomes"]["2"]["status"]=="unresolved"


def test_equality_at_cutoff_is_separate_and_prior_atr_unready_is_not_zero():
    inputs, plan=case(); outputs=inputs["indicator_evidence"]["outputs"]
    cutoff=next(o for o in outputs if o["output_name"]=="first_return_state" and o["time"]==_time(5))
    cutoff["value"]["fields"]["origins"][0].update(location="at_boundary")
    sample=next(o for o in outputs if o["output_name"]=="first_return_state" and o["time"]==_time(2))
    sample["value"]["fields"].update(prior_atr=None,prior_atr_ready=False)
    first,second=run(inputs,plan)["first_return_comparison"]["origins"]
    assert first["outcomes"]["2"]["reason"]=="strictly_prior_atr_unready"
    assert second["classification"]=="at_boundary_at_landmark"


def test_already_crossed_center_is_not_a_new_crossing():
    inputs,plan=case(); inputs["indicator_evidence"]["candles"][2].update(close=100)
    row=run(inputs,plan)["first_return_comparison"]["origins"][0]
    assert row["center_entry_state"]=="already_at_or_beyond_center"
    assert row["outcomes"]["2"]["first_center_crossing_close"] is None


def test_group_gate_preserves_values_without_qualifying_them():
    inputs,plan=case();inputs["statistics"]["eligibility"]["min_samples"]=30
    result=run(inputs,plan)
    assert result["first_return_comparison"]["horizons"]["2"]["comparison"]["returned_minus_outside"]==1.5
    assert not result["eligibility"]["eligible"]


def test_late_prior_atr_and_short_origin_lifetime_are_explicitly_unresolved():
    inputs,plan=case(); contexts=[o for o in inputs["indicator_evidence"]["outputs"] if o["output_name"]=="first_return_state"]
    next(o for o in contexts if o["time"]==_time(2))["value"]["fields"]["prior_atr_known_at"]=_time(3)
    next(o for o in contexts if o["time"]==_time(5))["value"]["fields"]["lifetime_bars"]=0
    first,second=run(inputs,plan)["first_return_comparison"]["origins"]
    assert first["outcomes"]["2"]["reason"]=="strictly_prior_atr_unready"
    assert second["classification_reason"]=="classification_exceeds_origin_lifetime"


def test_later_return_does_not_relabel_complete_cutoff_outside_state():
    inputs,plan=case(); outputs=inputs["indicator_evidence"]["outputs"]
    original=next(o for o in outputs if o["output_name"]=="first_value_return")
    future=deepcopy(original); future["time"]=_time(6); future["event"]["known_at"]=_time(7)
    future["event"]["metadata"]["breakout_time"]=_time(4)
    outputs.append(future)
    assert run(inputs,plan)["first_return_comparison"]["origins"][1]["classification"]==OUTSIDE
