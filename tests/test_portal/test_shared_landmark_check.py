from copy import deepcopy
from dataclasses import replace
from datetime import timedelta

import pytest
from market_data.frozen import semantic_hash
from portal.backend.service.research.shared_landmark_evaluator import SharedLandmarkEvaluator, _summaries
from portal.backend.service.research.matched_origin_evaluator import MatchedOriginEvaluator
from portal.backend.service.research.registry import normalize_check_request, materialize_check_definition
from tests.test_portal.test_matched_origin_check import _inputs, _payload, _plan, _time, START


def _request():
    payload = _payload()
    payload["outcomes"].update(horizons=[2, 3], required_horizons=[2, 3], primary_horizon=2,
        shared_landmark={"classification_lag_bars": 1, "sample_lag_bars": 2,
            "readiness_contract": "market_profile.value_location.v1", "dependence": "leave_one_original_profile_out.v1"})
    return payload


def _case():
    inputs = _inputs()
    _, request = normalize_check_request(_request())
    inputs.update(request.parameters)
    inputs["indicator_evidence"]["outputs"][1]["event"]["metadata"]["reference"]["key"] = "other-profile"
    for i in range(8):
        inputs["indicator_evidence"]["outputs"].append({"indicator_id": "profile-1", "time": _time(i),
            "output_name": "value_location", "output_type": "context",
            "value": {"state_key": "above_value", "fields": {"active_profile_key": "fixed-profile" if i < 4 else "other-profile"}}})
    return inputs, replace(_plan(), plan_hash="", evaluation_range={"start": _time(0), "end_exclusive": _time(8)})


def _run(inputs=None, plan=None):
    default_inputs, default_plan = _case()
    return SharedLandmarkEvaluator().evaluate(inputs=inputs or default_inputs, plan=plan or default_plan)


def test_registration_pins_new_semantics_and_preserves_legacy_definition():
    legacy = materialize_check_definition(_payload(), mode="evidence")
    newer, request = normalize_check_request(_request())
    assert legacy.definition_version.startswith("7+") and legacy.evaluator_version == "6"
    assert newer.definition_version.startswith("8+") and newer.evaluator_version == "7"
    assert request.parameters["outcomes"]["shared_landmark"]["sample_lag_bars"] == 2
    assert materialize_check_definition(_payload(), mode="evidence") == legacy
    bad = _request()
    with pytest.raises(ValueError, match="shared_landmark_invalid"):
        materialize_check_definition(bad, mode="evidence", base_version="7")


def test_fixed_landmark_complement_uses_common_prices_and_original_endpoints():
    result = _run(); analysis = result["shared_landmark_comparison"]
    assert analysis["classification_counts"] == {"confirmed_by_landmark": 1, "no_confirmation_by_landmark_complete_observation": 1}
    confirmed, complement = analysis["origins"]
    assert confirmed["classification_cutoff"] == _time(2)
    assert confirmed["sample_known_at"] == "2026-01-01T03:00:00.000000Z"
    assert confirmed["entry_price"] == 120
    assert confirmed["outcomes"]["2"]["signed_return"] == pytest.approx(90 / 120 - 1)
    assert complement["entry_price"] == 145
    assert complement["outcomes"]["2"]["signed_return"] == pytest.approx(150 / 145 - 1)
    comparison = analysis["horizons"]["2"]["comparisons"]["all"]
    assert comparison["confirmed_minus_complement"] == pytest.approx((90 / 120 - 1) - (150 / 145 - 1))
    assert analysis["horizons"]["2"]["leave_one_profile_out"]["defined_deletion_count"] == 0
    assert result["hashes"]["shared_landmark_comparison_hash"] == semantic_hash(analysis)
    assert semantic_hash(_run()) == semantic_hash(result)


@pytest.mark.parametrize("mutation,classification,reason", [
    ("edge", "incomplete_classification_window", "landmark_at_or_beyond_signal_observation_end"),
    ("context_missing", "incomplete_classification_window", "public_readiness_context_unavailable"),
    ("profile_changed", "unresolved_identity_or_context", "original_profile_changed"),
    ("late_signal", "incomplete_classification_window", "followup_not_known_by_landmark"),
])
def test_incomplete_and_context_changes_never_become_negative_labels(mutation, classification, reason):
    inputs, plan = _case()
    if mutation == "edge": plan = replace(plan, plan_hash="", evaluation_range={"start": _time(0), "end_exclusive": _time(2)})
    if mutation == "context_missing": inputs["indicator_evidence"]["outputs"] = [r for r in inputs["indicator_evidence"]["outputs"] if not (r["output_name"] == "value_location" and r["time"] == _time(1))]
    if mutation == "profile_changed":
        next(r for r in inputs["indicator_evidence"]["outputs"] if r["output_name"] == "value_location" and r["time"] == _time(1))["value"]["fields"]["active_profile_key"] = "changed"
    if mutation == "late_signal": inputs["indicator_evidence"]["outputs"][2]["event"]["known_at"] = (START + timedelta(hours=2, seconds=1)).isoformat()
    row = _run(inputs, plan)["shared_landmark_comparison"]["origins"][0]
    assert (row["classification"], row["classification_reason"]) == (classification, reason)
    assert row["outcomes"]["2"]["status"] == "unresolved"


def test_later_confirmation_does_not_rewrite_fixed_landmark_label():
    inputs, plan = _case(); follow = inputs["indicator_evidence"]["outputs"][2]
    follow["time"] = _time(2); follow["event"]["known_at"] = _time(3)
    row = _run(inputs, plan)["shared_landmark_comparison"]["origins"][0]
    assert row["classification"] == "no_confirmation_by_landmark_complete_observation"
    assert row["followup_index"] == 0


def test_membership_does_not_require_resolved_confirmation_future_outcomes():
    inputs, plan = _case()
    inputs["indicator_evidence"]["candles"] = inputs["indicator_evidence"]["candles"][:5]
    row = _run(inputs, plan)["shared_landmark_comparison"]["origins"][0]
    assert row["classification"] == "confirmed_by_landmark"


def test_entry_state_retains_equality_and_original_boundary():
    inputs, plan = _case()
    for output in inputs["indicator_evidence"]["outputs"]:
        if output["output_type"] == "signal" and output["event"]["metadata"]["reference"]["key"] == "fixed-profile":
            output["event"]["metadata"]["reference"]["price"] = 120
    analysis = _run(inputs, plan)["shared_landmark_comparison"]
    assert analysis["origins"][0]["entry_state"] == "at_boundary"
    assert analysis["horizons"]["2"]["comparisons"]["outside"]["undefined_reason"] == "empty_comparison_group"


def test_duplicate_context_fails_loud_and_missing_outcome_does_not_become_zero():
    inputs, plan = _case(); inputs["indicator_evidence"]["outputs"].append(deepcopy(inputs["indicator_evidence"]["outputs"][-1]))
    with pytest.raises(ValueError, match="duplicate public context"): _run(inputs, plan)


@pytest.mark.parametrize("field,value", [("sample_lag_bars", 1), ("classification_lag_bars", True), ("readiness_contract", "guessed"), ("dependence", "iid_bootstrap")])
def test_invalid_declared_methods_fail_before_execution(field, value):
    payload = _request(); payload["outcomes"]["shared_landmark"][field] = value
    with pytest.raises(ValueError, match="shared_landmark_invalid"): normalize_check_request(payload)


def test_nonreject_gap_policy_is_not_silently_admitted():
    payload = _request(); payload["gap_policy"] = "continue_degraded"
    with pytest.raises(ValueError, match="gap_policy=reject"): normalize_check_request(payload)


def test_profile_influence_deletes_both_groups_and_reports_overlap_without_iid_claim():
    rows=[]
    for i,(cluster,label,value) in enumerate([("a","confirmed_by_landmark",.10),("a","no_confirmation_by_landmark_complete_observation",.01),("b","confirmed_by_landmark",-.02),("b","no_confirmation_by_landmark_complete_observation",.02)]):
        rows.append({"origin_index":i,"origin_time":_time(i),"origin_utc_day":"2026-01-01","profile_cluster":cluster,"profile_key":cluster,"classification":label,"entry_state":"outside","sample_known_at":_time(i+1),"outcomes":{"2":{"status":"resolved","reason":None,"signed_return":value,"endpoint_close":_time(i+4)}}})
    summary=_summaries(rows,[2],{})["2"]
    assert summary["comparisons"]["all"]["confirmed_minus_complement"] == pytest.approx(.025)
    deletions=summary["leave_one_profile_out"]["deletions"]
    assert deletions[0]["removed_event_count"] == 2
    assert deletions[0]["comparisons"]["all"]["confirmed_minus_complement"] == pytest.approx(-.04)
    assert deletions[1]["comparisons"]["all"]["confirmed_minus_complement"] == pytest.approx(.09)
    assert summary["overlap"]["overlapping_pair_count"] == 5
    assert summary["overlap"]["cross_profile_pair_count"] == 3
    assert summary["profile_day_contributions"][0]["groups"]["confirmed_by_landmark"]["contribution_to_full_group_mean"] == pytest.approx(.05)


def test_future_reference_mismatch_does_not_change_landmark_complement():
    inputs, plan = _case()
    follow = inputs["indicator_evidence"]["outputs"][2]
    follow["time"] = _time(2)
    follow["event"]["known_at"] = _time(3)
    follow["event"]["metadata"]["reference"]["price"] = 101
    row = _run(inputs, plan)["shared_landmark_comparison"]["origins"][0]
    assert row["classification"] == "no_confirmation_by_landmark_complete_observation"
    follow["time"] = _time(1)
    follow["event"]["known_at"] = _time(2)
    row = _run(inputs, plan)["shared_landmark_comparison"]["origins"][0]
    assert row["classification"] == "unresolved_identity_or_context"


def test_shared_complement_threshold_controls_qualification_not_descriptive_values():
    inputs, plan = _case()
    inputs["statistics"]["eligibility"]["min_samples"] = 2
    result = _run(inputs, plan)
    assert result["shared_landmark_comparison"]["analysis_status"] == "insufficient_evidence"
    assert result["shared_landmark_comparison"]["horizons"]["2"]["comparisons"]["all"]["confirmed_minus_complement"] is not None
    assert not result["eligibility"]["eligible"]


def test_noncontributing_profile_is_not_a_leave_one_out_robustness_observation():
    inputs, plan = _case()
    for r in inputs["indicator_evidence"]["outputs"]:
        if r["output_name"] == "value_location" and r["time"] == _time(5): r["value"]["fields"]["active_profile_key"] = "different"
    analysis = _run(inputs, plan)["shared_landmark_comparison"]
    assert analysis["horizons"]["2"]["leave_one_profile_out"]["cluster_count"] == 1
    assert analysis["horizons"]["2"]["leave_one_profile_out"]["defined_deletion_count"] == 0


def test_intermediate_readiness_is_required_for_longer_declared_cutoff():
    inputs, plan = _case()
    inputs["outcomes"]["shared_landmark"].update(classification_lag_bars=2, sample_lag_bars=3)
    inputs["outcomes"].update(horizons=[3, 4], required_horizons=[3, 4], primary_horizon=3)
    inputs["indicator_evidence"]["outputs"] = [r for r in inputs["indicator_evidence"]["outputs"] if not (r["output_name"] == "value_location" and r["time"] == _time(1))]
    row = _run(inputs, plan)["shared_landmark_comparison"]["origins"][0]
    assert row["classification"] == "incomplete_classification_window"
