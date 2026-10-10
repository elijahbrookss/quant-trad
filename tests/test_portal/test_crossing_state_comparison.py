from copy import deepcopy

import numpy as np
import pytest
import pandas as pd

from market_data.frozen import semantic_hash
from portal.backend.service.research import crossing_state_comparison as owner
from portal.backend.service.research.forward_risk_evaluator import ForwardRiskEvaluator, _overlap
from portal.backend.service.research.registry import normalize_check_request, materialize_check_definition
from tests.test_portal.test_forward_risk_check import case, clock, payload


def paired_payload():
    result = payload()
    result["outcomes"]["forward_risk"].update(schema_version="candle_risk_matched_state.v1",
        matching_contract=owner.CONTRACT)
    return result


def fixture():
    n = 900
    times = np.arange(n, dtype=np.int64)*60
    metric = np.ones(n)
    metric[30:35] = 3
    metric[240:250] = 3
    metric[480:490] = 3
    shock = np.zeros(n, dtype=bool); shock[[30, 240, 480]] = True
    return dict(times=times, known=times+60, observable=np.ones(n, dtype=bool),
        metric=metric, shock=shock, segment=np.zeros(n, dtype=int), baseline=np.full(n, 1e-6),
        sample_times=times+60, strata=np.zeros(n, dtype=int),
        months=np.full(n, "2022-01", dtype=object), stop=(n+1)*60, evaluation_year=2022)


def finish(data, prepared, values=None):
    n = len(data["times"])
    values = np.full(n, 2e-6) if values is None else values
    return owner.finish_pairs(prepared=prepared, times=data["times"], known=data["known"],
        metric=data["metric"], atr_ratio=np.ones(n), baseline=data["baseline"],
        sample_times=data["sample_times"], months=data["months"],
        days=np.array([f"day-{i//144}" for i in range(n)]),
        weeks=np.array([f"week-{i//288}" for i in range(n)]), values=values,
        outcome_known=data["sample_times"]+owner.HORIZON,
        outcome_reasons=np.where(np.isfinite(values), "", "missing_future"), overlap=_overlap)


def test_new_explicit_definition_and_no_legacy_reinterpretation():
    old = materialize_check_definition(payload(), mode="evidence")
    definition, request = normalize_check_request(paired_payload())
    assert definition.definition_version.startswith("11+")
    assert definition.evaluator_version == "10"
    assert materialize_check_definition(payload(), mode="evidence") == old
    assert request.parameters["outcomes"]["forward_risk"]["matching_contract"] == owner.CONTRACT
    for version in ["6", "10"]:
        with pytest.raises(ValueError, match="forward_risk_invalid"):
            materialize_check_definition(paired_payload(), mode="evidence", base_version=version)
    changed = paired_payload(); changed["outcomes"]["forward_risk"]["z_caliper"] = .5
    with pytest.raises(ValueError, match="forward_risk_invalid"):
        normalize_check_request(changed)


def test_closed_form_matching_order_no_reuse_episode_or_within_pair_overlap():
    data = fixture(); result = owner.prepare_pairs(**data)
    assert result["pairs"].tolist() == [[30, 241], [240, 34], [480, 249]]
    assert len(set(result["pairs"][:, 1])) == len(result["pairs"])
    assert all(result["episodes"][i] != result["episodes"][j] for i, j in result["pairs"])
    assert all(abs(data["sample_times"][i]-data["sample_times"][j]) >= 7200 for i,j in result["pairs"])
    summary = finish(data, result)
    assert summary["primary"]["raw_risk_contrast"] == 0
    assert summary["primary"]["ratio_contrast"] == 0
    assert summary["analysis_status"] == "unsupported"
    assert not summary["support_gates"]["every_month_matching"]
    assert summary["matching"]["candidate_comparisons"] > 0
    assert semantic_hash(finish(data, owner.prepare_pairs(**data))) == semantic_hash(summary)


def test_future_mutation_cannot_change_pairing_and_missingness_never_rematches():
    data = fixture(); prepared = owner.prepare_pairs(**data)
    original = finish(data, prepared)
    values = np.full(900, 2e-6)
    values[prepared["pairs"][0, 1]] = np.nan
    actual = finish(data, prepared, values)
    assert actual["matching"] == original["matching"]
    assert actual["outcome_missingness"]["dropped_whole_pairs"] == 1
    assert actual["primary"]["pair_count"] == original["primary"]["pair_count"]-1
    assert actual["outcome_missingness"]["control_reasons"] == {"missing_future": 1}
    assert actual["outcome_missingness"]["rematched"] is False
    values[:] = 50
    assert finish(data, prepared, values)["matching"] == original["matching"]


def test_synthetic_fixed_pair_label_swap_reverses_sign():
    pairs = np.array([[0,1],[2,3]])
    values = np.array([4.,1.,6.,2.]); baseline = np.ones(4)
    forward = owner._summary(pairs, values, baseline)
    reverse = owner._summary(pairs[:, ::-1], values, baseline)
    assert forward["raw_risk_contrast"] == -reverse["raw_risk_contrast"] == 3.5


def test_joint_spacing_checks_every_arm_and_preserves_touching_intervals():
    starts = np.array([0, 20000, 7200, 21000, 14400, 40000])
    pairs = np.array([[0,1],[2,3],[4,5]])
    assert owner.joint_nonoverlap(pairs, starts).tolist() == [[0,1]]
    starts[4] = 27200
    assert owner.joint_nonoverlap(pairs, starts).tolist() == [[0,1],[4,5]]


def test_whole_pair_deletion_uses_either_arm():
    data = fixture(); prepared = owner.prepare_pairs(**data)
    actual = finish(data, prepared)
    removed = next(r for r in actual["leave_one_day_out"] if r["removed"] == "day-0")
    assert removed["removed_pairs"] == 2
    assert removed["pair_count"] == 1


@pytest.mark.parametrize("change", ["caliper", "zero", "delayed", "segment", "unobservable"])
def test_exclusions_and_no_support_are_explicit(change):
    data = fixture()
    if change == "caliper": data["metric"][~data["shock"] & (data["metric"] > 2)] = 4
    if change == "zero": data["baseline"][:] = 0
    if change == "delayed": data["sample_times"] += 60
    if change == "segment": data["segment"] = np.arange(900)
    if change == "unobservable": data["observable"][:] = False
    actual = owner.prepare_pairs(**data)
    assert len(actual["pairs"]) == 0
    assert finish(data, actual)["analysis_status"] == "unsupported"


def test_caliper_inclusive_and_exact_context_pinned():
    data = fixture()
    data["metric"][~data["shock"] & (data["metric"] > 2)] = 3.25
    assert len(owner.prepare_pairs(**data)["pairs"]) == 3
    data["metric"][~data["shock"] & (data["metric"] > 2)] = np.nextafter(3.25, 4)
    assert len(owner.prepare_pairs(**data)["pairs"]) == 0
    data = fixture(); data["strata"][~data["shock"]] = 1
    assert len(owner.prepare_pairs(**data)["pairs"]) == 0


def test_event_metric_contradiction_fails_loud_and_comparison_cap_stops(monkeypatch):
    data = fixture(); data["shock"][31] = True
    with pytest.raises(ValueError, match="contradicts"):
        owner.prepare_pairs(**data)
    monkeypatch.setattr(owner, "MAX_COMPARISONS", 1)
    with pytest.raises(ValueError, match="resource_limit"):
        owner.prepare_pairs(**fixture())


def test_cancellation_is_observed(monkeypatch):
    from core.execution_control import ExecutionControl, ExecutionCancelledError, controlled_execution
    control = ExecutionControl(); control.stop(ExecutionCancelledError("stop matching"))
    with pytest.raises(ExecutionCancelledError, match="stop matching"):
        with controlled_execution(control): owner.prepare_pairs(**fixture())


def test_full_evaluator_freezes_new_pairs_and_preserves_legacy_result():
    inputs, plan = case(); legacy = ForwardRiskEvaluator().evaluate(inputs=inputs, plan=plan)
    before = deepcopy(inputs)
    _, request = normalize_check_request(paired_payload())
    new = {**request.parameters, "indicator_evidence": deepcopy(inputs["indicator_evidence"])}
    outputs = []
    for output in new["indicator_evidence"]["outputs"]:
        if output["output_name"] != "candle_stats": continue
        i = int((pd.Timestamp(output["time"])-pd.Timestamp(clock(0))).total_seconds()/60)
        output["value"] = {"atr_zscore": 3 if 30 <= i < 35 or 240 <= i < 250 else 1, "atr_ratio": 1.2}
        outputs.append(output)
        if i in (30,240):
            outputs.append({"indicator_id":"stats-1", "time":clock(i), "known_at":clock(i+1),
                "output_name":"atr_expansion", "event_key":"atr_expansion_long"})
    new["indicator_evidence"]["outputs"] = outputs
    evaluator = ForwardRiskEvaluator(version="10", result_schema_version="event_fact_analysis_result.v10", paired_state_enabled=True)
    result = evaluator.evaluate(inputs=new, plan=plan)
    assert result["schema_version"] == "event_fact_analysis_result.v10"
    assert result["crossing_state_comparison"]["primary"]["pair_count"] == 2
    assert result["crossing_state_comparison"]["primary"]["raw_risk_contrast"] == pytest.approx(0, abs=1e-18)
    assert result["promotion_authority"] is False
    assert inputs == before
    assert ForwardRiskEvaluator().evaluate(inputs=inputs, plan=plan) == legacy
    with pytest.raises(ValueError, match="forward_risk_invalid"):
        ForwardRiskEvaluator().evaluate(inputs=new, plan=plan)
