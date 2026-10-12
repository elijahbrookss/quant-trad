from copy import deepcopy

import numpy as np
import pandas as pd
import pytest

from market_data.frozen import semantic_hash
from portal.backend.service.research import catalog, crossing_state_comparison as owner
from portal.backend.service.research.forward_risk_evaluator import ForwardRiskEvaluator, _overlap
from portal.backend.service.research.registry import materialize_check_definition, normalize_check_request
from tests.test_portal.test_crossing_state_comparison import fixture, finish, paired_payload
from tests.test_portal.test_forward_risk_check import case, clock


def filtered(data, prepared, ratios):
    return owner.filter_pairs_by_atr_ratio(prepared=prepared, atr_ratio=ratios,
        times=data["times"], known=data["known"], months=data["months"])


def complete(data, prepared, values=None):
    n = len(data["times"])
    values = np.full(n, 2e-6) if values is None else values
    return owner.finish_atr_ratio_pairs(prepared=prepared,
        times=data["times"], known=data["known"], metric=data["metric"],
        atr_ratio=np.ones(n), baseline=data["baseline"], sample_times=data["sample_times"],
        months=data["months"], days=np.array([f"day-{i//144}" for i in range(n)]),
        weeks=np.array([f"week-{i//288}" for i in range(n)]), values=values,
        outcome_known=data["sample_times"]+owner.HORIZON,
        outcome_reasons=np.where(np.isfinite(values), "", "missing_future"), overlap=_overlap)


@pytest.mark.parametrize("swapped", [False, True])
@pytest.mark.parametrize("ratio,accepted", [(np.nextafter(1.05, 0), True), (1.05, True), (np.nextafter(1.05, 2), False)])
def test_binary64_boundary_inclusive_symmetric_without_epsilon(swapped, ratio, accepted):
    data = fixture(); original = owner.prepare_pairs(**data)
    ratios = np.ones(900, dtype=object)
    i, j = original["pairs"][0]
    ratios[j if swapped else i] = ratio
    result = filtered(data, original, ratios)["atr_ratio_filter"]
    assert result["pairs"][0]["accepted"] is accepted
    assert result["pairs"][0]["max_min_ratio"] == ratio
    assert result["accepted_pairs"] == 2+int(accepted)
    assert result["pairs"][0]["reason"] == ("accepted" if accepted else "above_1_05")


@pytest.mark.parametrize("value,reason", [(None, "missing"), (np.nan, "nonfinite"),
    (np.inf, "nonfinite"), (-np.inf, "nonfinite"), (0., "nonpositive"),
    (-1., "nonpositive"), ("1.0", "invalid_type"), (True, "invalid_type")])
def test_invalid_arm_excludes_whole_pair_with_reason(value, reason):
    data = fixture(); original = owner.prepare_pairs(**data)
    ratios = np.ones(900, dtype=object); ratios[original["pairs"][0, 1]] = value
    prepared = filtered(data, original, ratios)
    assert prepared["pairs"].tolist() == original["pairs"][1:].tolist()
    evidence = prepared["atr_ratio_filter"]
    assert evidence["reason_counts"] == {reason: 1, "accepted": 2}
    assert evidence["pairs"][0]["persistent_high"]["atr_ratio_exclusion"] == reason
    assert evidence["pairs"][0]["crossing"]["atr_ratio_exclusion"] is None
    assert "raw_risk" not in str(evidence)
    assert "NaN" not in __import__("json").dumps(evidence, allow_nan=False)


def test_disjoint_reasons_original_map_and_legacy_golden_are_unchanged():
    data = fixture(); original = owner.prepare_pairs(**data)
    old = finish(data, original)
    assert semantic_hash(old) == "f81add44afe124b3f15f0edc1060d252aa1e5f2c330b1a2be1efe1dc937fb00b"
    ratios = np.ones(900, dtype=object)
    i, j = original["pairs"][0]; ratios[i] = None; ratios[j] = -1.
    prepared = filtered(data, original, ratios)
    assert original["pairs"].tolist() == [[30, 241], [240, 34], [480, 249]]
    assert finish(data, original) == old
    evidence = prepared["atr_ratio_filter"]
    assert evidence["original_pair_map_sha256_json_lines"] == original["accounting"]["feature_pair_map_sha256_json_lines"]
    assert evidence["reason_counts"] == {"missing": 1, "accepted": 2}
    assert evidence["pairs"][0]["persistent_high"]["atr_ratio_exclusion"] == "nonpositive"
    assert evidence["accepted_pairs"] + evidence["excluded_pairs"] == evidence["original_matched_pairs"]
    assert sum(evidence["reason_counts"].values()) == evidence["original_matched_pairs"]
    assert complete(data, prepared)["analysis_status"] == "unsupported"


def test_outcomes_never_change_filter_or_rematch_and_deletions_use_filtered_pairs():
    data = fixture(); original = owner.prepare_pairs(**data)
    ratios = np.ones(900); ratios[30] = 2
    prepared = filtered(data, original, ratios)
    result = complete(data, prepared)
    values = np.full(900, 9e-6); values[34] = np.nan
    changed = complete(data, prepared, values)
    assert changed["matching"] == result["matching"] == original["accounting"]
    assert changed["atr_ratio_filter"] == result["atr_ratio_filter"]
    assert changed["outcome_missingness"]["dropped_whole_pairs"] == 1
    assert changed["outcome_missingness"]["complete_fraction"] == .5
    assert changed["primary"]["pair_count"] == 1
    assert changed["outcome_missingness"]["rematched"] is False
    assert all(row["crossing"]["source_open_epoch"] != 30*60 for row in result["pairs"])
    assert result["monthly"][0]["matched_pairs"] == 3
    assert result["monthly"][0]["atr_accepted_pairs"] == 2
    assert result["monthly"][0]["atr_excluded_pairs"] == 1
    assert len(result["leave_one_day_out"]) > 0
    assert all(row["pair_count"] + row["removed_pairs"] == 2 for row in result["leave_one_day_out"])
    assert result["joint_nonoverlap"]["pair_count"] <= 2
    assert result["dependence"]["distinct_high_state_episodes"] == len(set(prepared["episodes"][prepared["pairs"].flatten()]))
    assert semantic_hash(complete(data, filtered(data, original, ratios))) == semantic_hash(result)


def supported_fixture():
    # Closed-form complete year: twenty fixed pairs per month, spaced arms.
    n = 480; times = np.arange(n, dtype=np.int64)*20000
    months = np.array([f"2022-{i//40+1:02d}" for i in range(n)])
    data = dict(times=times, known=times+60, sample_times=times+60,
        metric=np.ones(n)*3, baseline=np.ones(n), months=months)
    pairs = np.arange(n).reshape(-1, 2)
    accounting = {"eligible_crossings": 240, "matched_pairs": 240, "match_fraction": 1.,
        "feature_pair_map_sha256_json_lines": "synthetic-original-map",
        "monthly": [{"month": f"2022-{month:02d}", "eligible_crossings": 20, "matched_pairs": 20, "fraction": 1.} for month in range(1, 13)]}
    prepared = {"pairs": pairs, "accounting": accounting,
        "rms": np.ones(n), "episodes": np.arange(n)}
    return data, prepared


def test_locked_support_denominators_balance_and_no_rescue():
    data, original = supported_fixture(); ratios = np.ones(480)
    passed = complete(data, filtered(data, original, ratios))
    assert all(passed["support_gates"].values())
    assert passed["analysis_status"] == "descriptive_only"
    ratios[:42:2] = 2  # Twenty first-month pairs and one next-month pair removed.
    result = complete(data, filtered(data, original, ratios))
    assert result["support_gates"]["overall_matching"] is True
    assert result["support_gates"]["every_month_matching"] is True
    assert result["support_gates"]["overall_atr_filter_retention"] is True
    assert result["support_gates"]["every_month_atr_filter_retention"] is False
    assert result["analysis_status"] == "unsupported"
    assert result["per_asset_investment_criteria_met"] is False
    ratios[:] = 1.; ratios[::2] = 1.04
    imbalanced = complete(data, filtered(data, original, ratios))
    assert imbalanced["atr_ratio_filter"]["accepted_pairs"] == 240
    assert imbalanced["support_gates"]["atr_ratio_balance"] is False
    assert imbalanced["analysis_status"] == "unsupported"


def test_empty_filter_and_cancellation_are_explicit():
    from core.execution_control import ExecutionControl, ExecutionCancelledError, controlled_execution
    data = fixture(); original = owner.prepare_pairs(**data)
    result = complete(data, filtered(data, original, np.full(900, None)))
    assert result["primary"]["pair_count"] == 0
    assert result["atr_ratio_filter"]["retention_fraction"] == 0
    assert result["analysis_status"] == "unsupported"
    control = ExecutionControl(); control.stop(ExecutionCancelledError("stop ATR filter"))
    with pytest.raises(ExecutionCancelledError), controlled_execution(control):
        filtered(data, original, np.ones(900))


def test_new_catalog_method_is_explicit_fixed_and_rejects_legacy_versions():
    detail = catalog.get_check_definition("event_fact_analysis", "12")
    payload = detail["examples"][0]["request"]
    definition, _ = normalize_check_request(payload)
    assert definition.evaluator_version == "11"
    assert definition.definition_version.startswith("12+")
    assert detail["result_shape"]["payload_schema"] == "event_fact_analysis_result.v11"
    assert detail["settings"]["fixed"]["atr_ratio_filter"]["max_min_ratio_max"] == 1.05
    for version in ["6", "10", "11"]:
        with pytest.raises(ValueError, match="forward_risk_invalid"):
            materialize_check_definition(payload, mode="evidence", base_version=version)
    changed = deepcopy(payload); changed["outcomes"]["forward_risk"]["tolerance"] = .06
    with pytest.raises(ValueError, match="forward_risk_invalid"):
        normalize_check_request(changed)
    assert normalize_check_request(paired_payload())[0].evaluator_version == "10"


def test_full_evaluator_filters_public_ratios_and_preserves_old_result():
    inputs, plan = case()
    _, request = normalize_check_request(paired_payload())
    old_inputs = {**request.parameters, "indicator_evidence": deepcopy(inputs["indicator_evidence"])}
    outputs = []
    for output in old_inputs["indicator_evidence"]["outputs"]:
        if output["output_name"] != "candle_stats": continue
        i = int((pd.Timestamp(output["time"])-pd.Timestamp(clock(0))).total_seconds()/60)
        output["value"] = {"atr_zscore": 3 if 30 <= i < 35 or 240 <= i < 250 else 1, "atr_ratio": 1.}
        outputs.append(output)
        if i in (30, 240):
            outputs.append({"indicator_id": "stats-1", "time": clock(i), "known_at": clock(i+1),
                "output_name": "atr_expansion", "event_key": "atr_expansion_long"})
    old_inputs["indicator_evidence"]["outputs"] = outputs
    old_owner = ForwardRiskEvaluator(version="10", result_schema_version="event_fact_analysis_result.v10", paired_state_enabled=True)
    old = old_owner.evaluate(inputs=old_inputs, plan=plan)
    new_inputs = deepcopy(old_inputs)
    new_inputs["outcomes"]["forward_risk"].update(schema_version="candle_risk_matched_state.v2", filter_contract=owner.ATR_FILTER_CONTRACT)
    for output in new_inputs["indicator_evidence"]["outputs"]:
        if output["output_name"] == "candle_stats" and output["time"] == clock(30):
            output["value"]["atr_ratio"] = None
    evaluator = ForwardRiskEvaluator(version="11", result_schema_version="event_fact_analysis_result.v11", paired_state_enabled=True, atr_ratio_filter_enabled=True)
    result = evaluator.evaluate(inputs=new_inputs, plan=plan)
    comparison = result["crossing_state_comparison"]
    assert comparison["matching"] == old["crossing_state_comparison"]["matching"]
    assert comparison["atr_ratio_filter"]["reason_counts"] == {"missing": 1, "accepted": 1}
    assert comparison["primary"]["pair_count"] == 1
    assert comparison["pairs"][0]["crossing"]["atr_ratio"] == 1.
    assert result == evaluator.evaluate(inputs=deepcopy(new_inputs), plan=plan)
    assert old_owner.evaluate(inputs=old_inputs, plan=plan) == old
    assert not result["promotion_authority"] and not result["execution_authority"]



def test_quotient_overflow_is_rejected_with_json_safe_evidence():
    data = fixture(); original = owner.prepare_pairs(**data)
    ratios = np.ones(900)
    i, j = original["pairs"][0]; ratios[i] = 1e308; ratios[j] = 1e-308
    evidence = filtered(data, original, ratios)["atr_ratio_filter"]
    row = evidence["pairs"][0]
    assert row["accepted"] is False and row["reason"] == "above_1_05"
    assert row["max_min_ratio_overflow"] is True and row["max_min_ratio"] is None
    __import__("json").dumps(evidence, allow_nan=False)


def test_overall_monthly_retention_and_completeness_exact_thresholds():
    data, original = supported_fixture(); ratios = np.ones(480)
    # Exactly 168/240 pairs: six exclusions in every twenty-pair month.
    for month in range(12): ratios[month*40:month*40+12:2] = 2.
    result = complete(data, filtered(data, original, ratios))
    assert result["atr_ratio_filter"]["retention_fraction"] == .70
    assert result["support_gates"]["overall_atr_filter_retention"] is True
    ratios[12] = 2.
    assert complete(data, filtered(data, original, ratios))["support_gates"]["overall_atr_filter_retention"] is False
    ratios[:] = 1.; ratios[:20:2] = 2.
    assert complete(data, filtered(data, original, ratios))["support_gates"]["every_month_atr_filter_retention"] is True
    ratios[20] = 2.
    assert complete(data, filtered(data, original, ratios))["support_gates"]["every_month_atr_filter_retention"] is False
    prepared = filtered(data, original, np.ones(480)); values = np.ones(480)
    values[::40] = np.nan  # One missing whole-pair outcome per month: 228/240.
    result = complete(data, prepared, values)
    assert result["outcome_missingness"]["complete_fraction"] == .95
    assert result["support_gates"]["outcome_completeness"] is True
    values[2] = np.nan
    assert complete(data, prepared, values)["support_gates"]["outcome_completeness"] is False
