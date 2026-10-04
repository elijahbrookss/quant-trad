from copy import deepcopy
from dataclasses import replace
from datetime import UTC, datetime, timedelta
import math

import numpy as np
import pytest

from market_data.frozen import semantic_hash
from portal.backend.service.research.forward_risk_evaluator import ForwardRiskEvaluator, _stratified, _overlap
from portal.backend.service.research.registry import normalize_check_request, materialize_check_definition
from tests.test_portal.test_matched_origin_check import _payload, _plan

START = datetime(2022, 12, 30, tzinfo=UTC)


def clock(i):
    return (START + timedelta(minutes=i)).isoformat().replace("+00:00", "Z")


def payload():
    value = _payload()
    value.update(gap_policy="reset_rewarm", statistics={})
    value["scope"].update(indicator_id="stats-1", timeframe="1m", start=clock(0), end=clock(2880), warmup_bars=200)
    value["detector"] = {"type": "indicator_event", "output_name": "atr_expansion", "event_keys": [{"key": "atr_expansion_long", "direction": "long"}]}
    value["outcomes"] = {"horizon_kind": "elapsed_time", "horizons": [1800, 7200, 21600], "primary_horizon": 7200, "entry_lag_bars": 0,
        "forward_risk": {"schema_version": "candle_risk_comparison.v1", "baseline_bars": 120, "readiness_contract": "candle_stats.public_outputs.v1", "outcome_boundary": "evaluation_end_exclusive"}}
    return value


def case():
    _, request = normalize_check_request(payload())
    plan = replace(_plan(), plan_hash="", evaluation_range={"start": clock(0), "end_exclusive": clock(2880)},
        materialization_range={"start": clock(-200), "end_exclusive": clock(2880)},
        warmup={"bars": 200, "seconds": 12000, "timeframe_seconds": 60}, gap_policy="reset_rewarm")
    candles = []
    outputs = []
    for i in range(-200, 2880):
        price = 100 * math.exp(i * .001)
        candles.append({"open_time": clock(i), "close_time": clock(i+1), "known_at": clock(i+1), "close": price, "high": price * 1.01, "low": price * .99})
        if i >= 0:
            outputs.append({"indicator_id": "stats-1", "time": clock(i), "output_name": "candle_stats", "value": {"atr_zscore": 3 if i in (0,1,2,2860) else 1}})
            if i in (0,2860):
                outputs.append({"indicator_id": "stats-1", "time": clock(i), "known_at": clock(i+1), "output_name": "atr_expansion", "event_key": "atr_expansion_long"})
    evidence = {"indicator": {"id": "stats-1", "type": "candle_stats", "params": {}}, "candles": candles, "outputs": outputs,
        "output_readiness": {"schema_version": "indicator_output_readiness.v1", "indicator_id": "stats-1", "intervals": [
            {"start": clock(-200), "end_exclusive": clock(2880), "ready_outputs": {"candle_stats": True, "atr_expansion": True}, "segment": 0}]}}
    return {**request.parameters, "indicator_evidence": evidence}, plan


def run(inputs=None, plan=None):
    if inputs is None: inputs, plan = case()
    return ForwardRiskEvaluator().evaluate(inputs=inputs, plan=plan)


def test_registered_version_cannot_be_ignored_by_legacy_evaluator():
    before = materialize_check_definition(_payload(), mode="evidence")
    definition, request = normalize_check_request(payload())
    assert definition.definition_version.startswith("10+") and definition.evaluator_version == "9"
    assert materialize_check_definition(_payload(), mode="evidence") == before
    with pytest.raises(ValueError, match="forward_risk_invalid"):
        materialize_check_definition(payload(), mode="evidence", base_version="6")
    declaration = ForwardRiskEvaluator().declare_requirements(definition=definition, request=request)
    assert declaration["capture_output_readiness"] and declaration["outcome_boundary"] == "evaluation_end_exclusive"
    assert declaration["warmup_floor_bars"] == 200


def test_closed_form_log_risk_and_ratio_replay_and_cohort_accounting():
    inputs, plan = case(); before = deepcopy(inputs)
    result = run(inputs, plan); analysis = result["forward_risk_comparison"]
    groups = analysis["horizons"]["7200"]["cohorts"]
    assert inputs == before
    assert groups["shock_crossing"]["candidate_count"] == 2
    assert groups["persistent_high"]["candidate_count"] == 2
    assert groups["ordinary"]["candidate_count"] == 2875
    for group in groups.values():
        assert group["raw_risk"]["mean"] == pytest.approx(1e-6)
        assert group["future_prior_risk_ratio"]["mean"] == pytest.approx(1)
    assert groups["shock_crossing"]["unresolved_reasons"] == {"administrative_end_of_discovery": 1}
    assert analysis["coverage"]["first_blocking_stage_counts"] == {"decision_at_or_after_evaluation_end": 1}
    assert semantic_hash(run(inputs, plan)) == semantic_hash(result)
    assert result["hashes"]["forward_risk_comparison_hash"] == semantic_hash(analysis)
    assert result["promotion_authority"] is False


def test_trigger_and_sample_high_low_excluded_from_future_path_and_prior_baseline():
    inputs, plan = case()
    trigger = inputs["indicator_evidence"]["candles"][200]
    trigger.update(high=100000, low=.001)
    analysis = run(inputs, plan)["forward_risk_comparison"]
    first = analysis["observation_ledger"]["examples"]["shock_crossing"][0]
    assert first[4] == pytest.approx(1e-6)
    assert first[5] == int((START+timedelta(minutes=1)).timestamp())
    assert first[7] == pytest.approx((100*math.exp(30*.001)*1.01 - 100*math.exp(.001)*.99)/100)


def test_missing_future_minute_only_invalidates_dependent_horizons_and_recovery_unknown():
    inputs, plan = case(); evidence = inputs["indicator_evidence"]
    evidence["candles"] = [r for r in evidence["candles"] if r["open_time"] != clock(40)]
    evidence["outputs"] = [r for r in evidence["outputs"] if r["time"] != clock(40)]
    evidence["output_readiness"]["intervals"] = [
        {"start": clock(-200), "end_exclusive": clock(40), "ready_outputs": {"candle_stats": True, "atr_expansion": True}, "segment": 0},
        {"start": clock(41), "end_exclusive": clock(241), "ready_outputs": {"candle_stats": False, "atr_expansion": False}, "segment": 1},
        {"start": clock(241), "end_exclusive": clock(2880), "ready_outputs": {"candle_stats": True, "atr_expansion": True}, "segment": 1}]
    analysis = run(inputs, plan)["forward_risk_comparison"]
    assert analysis["horizons"]["1800"]["cohorts"]["shock_crossing"]["raw_risk"]["count"] == 1
    assert analysis["horizons"]["7200"]["cohorts"]["shock_crossing"]["raw_risk"]["count"] == 0
    reasons = analysis["coverage"]["first_blocking_stage_counts"]
    assert reasons["missing_source_candle"] == 1 and reasons["indicator_not_ready"] == 200
    assert analysis["coverage"]["unknown_event_count"] is None


def test_zero_prior_baseline_preserves_raw_risk_and_separately_censors_ratio():
    inputs, plan = case()
    for candle in inputs["indicator_evidence"]["candles"][:200]:
        candle.update(close=100, high=101, low=99)
    groups = run(inputs, plan)["forward_risk_comparison"]["horizons"]["7200"]["cohorts"]
    shock = groups["shock_crossing"]
    assert shock["raw_risk"]["count"] == 1
    assert shock["future_prior_risk_ratio"]["count"] == 0
    assert shock["risk_ratio_unresolved_reasons"] == {"zero_prior_risk": 1, "administrative_end_of_discovery": 1}


def test_invalid_future_high_low_does_not_erase_valid_close_risk():
    inputs, plan = case(); inputs["indicator_evidence"]["candles"][210]["low"] = -1
    shock = run(inputs, plan)["forward_risk_comparison"]["horizons"]["7200"]["cohorts"]["shock_crossing"]
    assert shock["raw_risk"]["count"] == 1 and shock["path_range"]["count"] == 0
    assert shock["path_range_unresolved_reasons"]["missing_or_invalid_future_high_low_path"] == 1


def test_late_known_at_uses_later_sample_and_propagates_unavailable_state_history():
    inputs, plan = case(); evidence = inputs["indicator_evidence"]
    evidence["candles"][200]["known_at"] = clock(3)
    next(o for o in evidence["outputs"] if o["output_name"] == "atr_expansion")["known_at"] = clock(3)
    analysis = run(inputs, plan)["forward_risk_comparison"]
    assert analysis["observation_ledger"]["examples"]["shock_crossing"][0][5] == int((START+timedelta(minutes=3)).timestamp())
    assert analysis["coverage"]["first_blocking_stage_counts"]["state_history_not_known_at_decision"] == 1


@pytest.mark.parametrize("mutation", ["identity", "params", "readiness", "duplicate", "tail", "seed"])
def test_malformed_or_unpinned_evidence_fails_loud(mutation):
    inputs, plan = case(); evidence = inputs["indicator_evidence"]
    if mutation == "identity": evidence["outputs"][0]["indicator_id"] = "other"
    if mutation == "params": evidence["indicator"]["params"]["atr_short_window"] = 7
    if mutation == "readiness": evidence.pop("output_readiness")
    if mutation == "duplicate": evidence["outputs"].append(deepcopy(evidence["outputs"][0]))
    if mutation == "tail": plan = replace(plan, plan_hash="", materialization_range={"start":clock(-200),"end_exclusive":clock(2881)})
    if mutation == "seed": plan = replace(plan, plan_hash="", materialization_range={"start":clock(-201),"end_exclusive":clock(2880)})
    with pytest.raises(ValueError, match="forward_risk_invalid"): run(inputs, plan)


def test_stratification_deletes_whole_days_from_both_groups_and_retains_unmatched():
    values = np.array([1.,3.,2.,6.,100.])
    cohorts = np.array([0,1,0,1,1]); strata = np.array([0,0,0,0,1])
    summary = _stratified(values, cohorts, strata, np.array(["a","a","b","b","b"]), np.array(["w","w","w","w","w"]))
    assert summary["shock_weighted_within_stratum_contrast"] == 3
    assert summary["unmatched_shock_count"] == 1
    assert summary["leave_one_day_out"][0]["shock_weighted_within_stratum_contrast"] == 4
    assert summary["leave_one_day_out"][1]["shock_weighted_within_stratum_contrast"] == 2
    assert summary["leave_one_week_out"][0]["shock_weighted_within_stratum_contrast"] is None
    assert _overlap(np.array([0,1,4]), np.array([3,4,5])) == {"positive_duration_overlapping_pairs":1,"connected_episodes":2,"largest_episode_observations":2,"effective_sample_size":None}


def test_late_complete_retrospective_path_keeps_its_availability_clock():
    inputs, plan = case()
    inputs["indicator_evidence"]["candles"][220]["known_at"] = clock(100)
    a = run(inputs, plan)["forward_risk_comparison"]["horizons"]
    assert a["1800"]["cohorts"]["shock_crossing"]["raw_risk"]["count"] == 1
    assert a["1800"]["cohorts"]["shock_crossing"]["outcome_known_after_target_count"] == 1
    assert a["1800"]["cohorts"]["shock_crossing"]["outcome_availability_delay_seconds"]["maximum"] == 69*60
    assert a["7200"]["cohorts"]["shock_crossing"]["outcome_known_after_target_count"] == 0
    assert a["7200"]["cohorts"]["shock_crossing"]["raw_risk"]["count"] == 1


def test_planner_pins_seed_and_never_extends_into_reserved_year():
    from portal.backend.service.research.planning import plan_research_check
    from tests.test_portal.test_research_check_planning import _Store
    definition, request = normalize_check_request(payload())
    def indicator_plan(*args, **kwargs):
        return {"warmup_bars": 200, "indicators": [], "requirements": []}
    plan = plan_research_check(definition, request, store=_Store(), indicator_planner=indicator_plan,
        instrument_loader=lambda instrument_id: {"id": instrument_id}, inspect_coverage=False)
    assert plan.materialization_range["end_exclusive"] == "2023-01-01T00:00:00.000000Z"
    assert plan.materialization_range["start"] == "2022-12-29T20:40:00.000000Z"
    assert plan.warmup["bars"] == 200
    assert plan.outcome_tail["seconds"] == 0
    assert plan.outcome_tail["boundary"] == "evaluation_end_exclusive"
    assert plan.execution["capture_output_readiness"] is True
    assert all(r["required_end"] <= "2023-01-01T00:00:00.000000Z" for r in plan.market_data_requirements)


def test_canonical_engine_readiness_segments_survive_filter(monkeypatch):
    import pandas as pd
    from indicators.candle_stats.definition import CandleStatsIndicator
    from portal.backend.service.indicators.indicator_service import runtime_validation as rv
    from portal.backend.service.research.execution import _filter_indicator_evidence
    meta = {"id":"stats-1", "type":"candle_stats", "name":"Candle Stats", "params":CandleStatsIndicator.resolve_config({}), "dependencies":[], "runtime_supported":True}
    monkeypatch.setattr(rv, "load_indicator_record", lambda *args, **kwargs: meta)
    monkeypatch.setattr(rv, "build_meta_from_record", lambda *args, **kwargs: meta)
    def graph(*args, **kwargs):
        indicator = CandleStatsIndicator.build_runtime_indicator(indicator_id="stats-1", meta=meta, resolved_params=meta["params"], strategy_indicator_metas={})
        return {"stats-1":meta}, [indicator]
    monkeypatch.setattr(rv, "build_runtime_indicator_graph", graph)
    inputs, plan = case()
    candles = inputs["indicator_evidence"]["candles"]
    for row in candles:
        row.update(open=row["close"],volume=10)
    frame = pd.DataFrame(candles)
    frame.index = pd.to_datetime(frame.pop("open_time"), utc=True)
    frame = frame.drop(pd.Timestamp(clock(40)))
    evidence = rv.collect_runtime_output_evidence_for_instance("stats-1", clock(-200), clock(2880), "1m",
        instrument_id="instrument-1", instrument_snapshot={"id":"instrument-1","symbol":"TEST","datasource":"TEST","exchange":"TEST"},
        candle_frame=frame, capture_output_readiness=True, gap_policy="reset_rewarm", require_recorded_discontinuities=True,
        recorded_gap_evidence=[{"start":clock(40),"end":clock(41),"classification":"provider_missing_data"}])
    for page_size in (1, 37, 512):
        streamed = rv.collect_runtime_output_evidence_for_instance("stats-1", clock(-200), clock(2880), "1m",
            instrument_id="instrument-1", instrument_snapshot={"id":"instrument-1","symbol":"TEST","datasource":"TEST","exchange":"TEST"},
            candle_frames=(frame.iloc[i:i+page_size] for i in range(0, len(frame), page_size)),
            capture_output_readiness=True, gap_policy="reset_rewarm", require_recorded_discontinuities=True,
            recorded_gap_evidence=[{"start":clock(40),"end":clock(41),"classification":"provider_missing_data"}])
        assert {k:v for k,v in streamed.items() if k != "perf"} == {k:v for k,v in evidence.items() if k != "perf"}
    intervals = evidence["output_readiness"]["intervals"]
    assert len(intervals) == 4
    assert intervals[0]["ready_outputs"] == {"atr_expansion":False,"candle_stats":False}
    reset = intervals[2]
    assert reset["segment"] == 1 and reset["start"] == clock(41) and reset["end_exclusive"] == clock(241)
    inputs["indicator_evidence"] = _filter_indicator_evidence(evidence, plan)
    analysis = run(inputs, plan)["forward_risk_comparison"]
    assert analysis["coverage"]["first_blocking_stage_counts"]["indicator_not_ready"] == 200
    assert analysis["coverage"]["first_blocking_stage_counts"]["missing_source_candle"] == 1
    assert sum(g["candidate_count"] for g in analysis["horizons"]["7200"]["cohorts"].values()) == analysis["coverage"]["detector_observable_minutes"]


def test_readiness_is_hash_bound_without_changing_legacy_hash_material():
    from portal.backend.service.research.execution import _execution_input_hashes
    inputs, plan = case()
    first = _execution_input_hashes(plan, inputs)
    inputs["indicator_evidence"]["output_readiness"]["intervals"][0]["segment"] = 1
    assert _execution_input_hashes(plan, inputs)["indicator_output_hash"] != first["indicator_output_hash"]
    inputs["indicator_evidence"].pop("output_readiness")
    evidence = inputs["indicator_evidence"]
    legacy = semantic_hash({"schema_version":"check_indicator_output_material.v1", "runtime_path":None,
        "indicator_graph_hash":semantic_hash({"indicators":[]}), "window":{}, "output_types":{}, "ready_counts":{}, "not_ready_counts":{}, "outputs":evidence["outputs"]})
    assert _execution_input_hashes(plan, inputs)["indicator_output_hash"] == legacy


def test_decision_calendar_clock_and_source_coverage_are_explicit():
    inputs, plan = case()
    analysis = run(inputs, plan)["forward_risk_comparison"]
    first = analysis["observation_ledger"]["examples"]["shock_crossing"][0]
    assert first[-1] == int((START+timedelta(minutes=1)).timestamp())
    assert analysis["coverage"]["cohort_calendar_clock"] == "decision_known_at"
    assert analysis["coverage"]["coverage_clock"] == "source_candle_open"
    assert sum(r["observable_decisions"] for r in analysis["months"]) == analysis["coverage"]["detector_observable_minutes"]
    assert analysis["coverage"]["undetectable_intervals"][-1]["preceding_observable_context"]["known_at"] == clock(2879)
