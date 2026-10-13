"""Explicit completed-bucket association; never a historical availability claim."""
from collections.abc import Mapping
from datetime import datetime, timedelta
from core.execution_control import execution_checkpoint
from .event_time import _utc, _iso
from market_data.frozen import semantic_hash
from market_data.fact_registry import get_fact_contract
from .event_facts import (_record_known_at, _record_effective_at,
    _record_event_key, _record_revision_key, _record_is_active, _record_matches_where)

SCHEMA = "retrospective_numeric_comparison.v1"


def normalize_retrospective(raw, *, detector, outcomes, statistics, scope, inputs):
    if not isinstance(raw, Mapping) or set(raw) != {"schema_version", "analysis_as_of", "bucket_time_interpretation"}:
        raise ValueError("retrospective_invalid: explicit schema, analysis_as_of and bucket interpretation required")
    if raw["schema_version"] != SCHEMA or raw["bucket_time_interpretation"] != "bucket_start_unverified":
        raise ValueError("retrospective_invalid: unsupported contract or bucket interpretation")
    cutoff = _utc(raw["analysis_as_of"], field="retrospective.analysis_as_of")
    start, end = (_utc(scope.get(k), field="scope."+k) for k in ("start", "end"))
    if scope.get("timeframe") != "1h" or not timedelta(0) < end-start <= timedelta(days=60) or cutoff < end:
        raise ValueError("retrospective_invalid: hourly range <=60 days with analysis_as_of >= end required")
    if any(t.timestamp() % 3600 for t in (start, end)) or scope.get("indicator_id") or scope.get("indicator_param_overrides"):
        raise ValueError("retrospective_invalid: aligned UTC hours and no Indicator required")
    if detector.get("type") != "fact_snapshot" or detector.get("evaluation_trigger", "primary_available") != "primary_available" or detector.get("where"):
        raise ValueError("retrospective_invalid: unconditional primary-close fact_snapshot required")
    if outcomes["horizons"] != [1] or outcomes["horizon_kind"] != "bars" or outcomes["entry_lag_bars"] != 0 or outcomes.get("invalidation"):
        raise ValueError("retrospective_invalid: one next-hour raw-return direction outcome required")
    baseline = statistics["features"]["baseline"]
    enriched = statistics["features"]["enriched"]
    if (len(baseline) != 2 or {f["operator"] for f in baseline} != {"lagged_return", "atr_fraction"}
            or any(f.get("lookback_bars") != 1 for f in baseline if f["operator"] == "lagged_return")
            or any(f.get("period") != 14 for f in baseline if f["operator"] == "atr_fraction")):
        raise ValueError("retrospective_invalid: baseline is recent one-bar return and ATR fraction period 14")
    if any(set(f) - {"name", "operator", "input_alias"} for f in enriched):
        raise ValueError("retrospective_invalid: untransformed latest numeric values required")
    if not 1 <= len(enriched) <= 2 or any(f["operator"] != "latest_value" for f in enriched):
        raise ValueError("retrospective_invalid: one or two completed-bucket exact numeric features required")
    aliases = {f["input_alias"] for f in enriched}
    if set(i["alias"] for i in inputs) != aliases or detector["input_alias"] not in aliases:
        raise ValueError("retrospective_invalid: inputs must be exactly the numeric feature aliases")
    for i in inputs:
        if not get_fact_contract(i["fact_type"]).uses_exact_numeric_storage or i.get("known_at_required") is False:
            raise ValueError("retrospective_invalid: exact numeric source with retained known_at required")
        if i.get("alignment") != "exact_interval" or int(i.get("dimensions", {}).get("bucket_seconds") or 0) != 3600 or i.get("max_staleness_seconds") != 3600:
            raise ValueError("retrospective_invalid: exact hourly interval and explicit 3600s staleness required")
    if not statistics["model"] or len(statistics["folds"]) != 1 or statistics["bootstrap"] or statistics["direct_tests"] or statistics["feature_bins"]:
        raise ValueError("retrospective_invalid: one fixed paired logistic comparison; no extra tests/bins/bootstrap")
    if statistics["purge_bars"] != 1 or statistics["embargo_bars"] != 0:
        raise ValueError("retrospective_invalid: one-bar purge and zero embargo required")
    fold = statistics["folds"][0]
    if _utc(fold["train"]["start"], field="train.start") < start or _utc(fold["validation"]["end"], field="validation.end") > end:
        raise ValueError("retrospective_invalid: comparison windows must fit declared sample")
    return {"schema_version": SCHEMA, "analysis_as_of": _iso(cutoff),
            "bucket_time_interpretation": "bucket_start_unverified"}


def retrospective_snapshots(records, *, snapshot_requests, where, alias, analysis_as_of):
    """Select latest revision per identity at real analysis cutoff, then exact bucket.

    Source clocks are read-only. Unlike causal selection, this admits records
    received after the historical sample and discloses that limitation.
    """
    if len(records) > 10000:
        raise ValueError("retrospective_resource_limit: at most 10000 frozen revisions per alias")
    latest = {}
    for record in records:
        execution_checkpoint()
        if _record_known_at(record) > analysis_as_of:
            continue
        key = (int(record.series_id), _record_event_key(record))
        if key not in latest or _record_revision_key(record) > _record_revision_key(latest[key]):
            latest[key] = record
    by_end = {}
    for record in latest.values():
        if not _record_is_active(record) or not _record_matches_where(record, where):
            continue
        fact = record.fact
        if not hasattr(fact, "value") or not hasattr(record, "provenance"):
            raise ValueError(f"retrospective_invalid: exact numeric bucket required alias={alias}")
        lo = _record_effective_at(record)
        hi = lo + timedelta(hours=1)
        raw_end = record.provenance.get("bucket_end")
        declared_end = (_utc(raw_end, field="provenance.bucket_end") if not isinstance(raw_end, (int, float))
                        else datetime.fromtimestamp(raw_end, tz=lo.tzinfo))
        if lo.timestamp() % 3600 or declared_end != hi or _record_known_at(record) < hi:
            raise ValueError(f"retrospective_invalid: inconsistent hourly bucket alias={alias}")
        if hi in by_end:
            raise ValueError(f"retrospective_invalid: ambiguous exact bucket alias={alias} end={_iso(hi)}")
        by_end[hi] = record
    return {(decision, sample): by_end.get(sample) for decision, sample in snapshot_requests}


def retrospective_events(candles, *, start, end):
    # Every primary candle is a candidate, independent of enriched availability.
    rows = []
    for candle in candles:
        opened = _utc(candle.get("open_time") or candle.get("time"), field="candle.open_time")
        if start <= opened < end:
            sample = _utc(candle["close_time"], field="candle.close_time")
            rows.append({"time": _iso(opened), "known_at": _iso(sample),
                "output_name": "fact_snapshot", "output_type": "check_occurrence",
                "event_key": "retrospective_primary_close", "event": {
                    "known_at": _iso(sample), "direction": "neutral", "metadata": {
                        "sampling": "primary_bar_close", "selection_rule": SCHEMA,
                        "historical_decision": False}}})
    return rows


def qualify_result(result, contract):
    result["analysis_contract"] = dict(contract)
    result["evidence_class"] = "retrospective_conditional_association"
    result["point_in_time_predictive"] = False
    result["backtest_certified"] = False
    result["scientific_certification_eligible"] = False
    result["caveats"] = [*result.get("caveats", []),
        "Retrospective association using late-received frozen revisions; no point-in-time predictive, backtest or certified evidence.",
        "Chronological train/validation windows do not restore historical availability or establish a fresh holdout.",
        "Provider bucket-start interpretation remains unverified; results are conditional on that interpretation.",
        "Recorded source gaps and completeness uncertainty remain visible; missing buckets never become zero."]
    for row in result.get("events", []):
        row["sample_time"] = row.pop("decision_time")
        row["event"]["sample_time"] = row["event"].pop("known_at")
        row["historical_decision"] = False
        row["analysis_as_of"] = contract["analysis_as_of"]
        row["late_known_fact_count"] = sum(ref["known_at"] > row["sample_time"]
            for refs in row.get("fact_references", {}).values() for ref in refs)
    model = result.get("statistics", {}).get("model")
    if model is not None:
        model["evidence_class"] = "retrospective_conditional_association"
        model["point_in_time_predictive"] = False
        if "oos_count" in model:
            model["retrospective_validation_count"] = model.pop("oos_count")
        if "prediction_hash" in model:
            model["fitted_probability_hash"] = model.pop("prediction_hash")
    if "hashes" in result:
        result["hashes"]["statistics_hash"] = semantic_hash(result["statistics"])
        result["hashes"]["event_population_hash"] = semantic_hash({"events":result["events"]})
        result["hashes"]["feature_matrix_hash"] = semantic_hash({"rows":[{
            "event_time":row["event_time"], "sample_time":row["sample_time"],
            "features":row["features"], "primary_outcome":row["outcomes"].get("1"),
            "eligible":row["eligible"]} for row in result["events"]]})
    return result
