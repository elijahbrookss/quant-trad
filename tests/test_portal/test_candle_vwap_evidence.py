"""Real graph, requirement planner, output collector and registered Check integration."""
from copy import deepcopy
from datetime import timedelta

import pandas as pd
import pytest

from engines.indicator_engine.contracts import IndicatorGapRejectedError
from market_data.frozen import semantic_hash
from portal.backend.service.indicators.indicator_service import runtime_validation
from portal.backend.service.indicators.indicator_service.requirements import plan_runtime_requirements_for_indicators
from portal.backend.service.indicators.indicator_service.utils import build_meta_from_record
from portal.backend.service.research import catalog
from portal.backend.service.research.planning import plan_research_check
from portal.backend.service.research.registry import CHECK_REGISTRY, normalize_check_request
from tests.test_indicators.test_candle_vwap_runtime import ORIGIN, STEP


RECORD = {"id": "vwap-1", "type": "candle_vwap", "version": "v1", "name": "Candle VWAP",
          "params": {"source_timeframe": "15m"}, "dependencies": [], "enabled": True}


def source():
    times = pd.date_range(ORIGIN, periods=8, freq="15min")
    prices = [100., 110., 90., 90., 110., 100., 105., 108.]
    return pd.DataFrame({"open": prices, "high": prices, "low": prices, "close": prices,
        "volume": [2.]*8, "close_time": times+STEP, "known_at": times+STEP,
        "source_identity_key": "synthetic-native-source", "known_at_method": "interval_close_inferred"}, index=times)


def collect(monkeypatch, *, frame=None, pages=None, **kwargs):
    monkeypatch.setattr(runtime_validation, "load_indicator_record", lambda *a, **k: deepcopy(RECORD))
    return runtime_validation.collect_runtime_output_evidence_for_instance(
        "vwap-1", ORIGIN.isoformat(), (ORIGIN+8*STEP).isoformat(), "15m",
        instrument_id="synthetic-1", instrument_snapshot={"id": "synthetic-1", "symbol": "SYNTHETIC"},
        candle_frame=source() if frame is None and pages is None else frame,
        candle_frames=pages, gap_policy="reject", capture_output_readiness=True, **kwargs)


def planner(ids, **kwargs):
    return plan_runtime_requirements_for_indicators(ids, **kwargs,
        preloaded_metas={"vwap-1": build_meta_from_record(RECORD)})


def test_real_graph_collects_public_context_metrics_and_causal_cross_events(monkeypatch):
    evidence = collect(monkeypatch)
    assert evidence["bars_evaluated"] == 8
    assert evidence["ready_counts"] == {"vwap_context": 8, "vwap_cross": 8, "vwap_metrics": 8}
    metrics = [r for r in evidence["outputs"] if r["output_name"] == "vwap_metrics"]
    contexts = [r for r in evidence["outputs"] if r["output_name"] == "vwap_context"]
    events = [r for r in evidence["outputs"] if r["output_type"] == "signal"]
    assert len(metrics) == len(contexts) == 8
    assert metrics[1]["value"]["vwap"] == pytest.approx(105)
    assert contexts[1]["value"]["fields"]["session_timezone"] == "UTC"
    assert {r["event_key"] for r in events} == {"close_cross_above_vwap", "close_cross_below_vwap"}
    for row in events:
        assert pd.Timestamp(row["known_at"]) == pd.Timestamp(row["time"])+STEP
    paged = collect(monkeypatch, pages=iter([source().iloc[:3], source().iloc[3:]]))
    for key in ("outputs", "candles", "ready_counts", "not_ready_counts", "output_readiness"):
        assert paged[key] == evidence[key]


def test_collector_rejects_missing_volume_and_recorded_or_unrecorded_gaps(monkeypatch):
    with pytest.raises(ValueError, match="volume"):
        collect(monkeypatch, frame=source().drop(columns=["volume"]))
    with pytest.raises(IndicatorGapRejectedError):
        collect(monkeypatch, frame=source().drop(source().index[2]))


def test_existing_versioned_check_consumes_runtime_events_without_family_changes(monkeypatch):
    raw = deepcopy(catalog.get_check_definition("event_fact_analysis", "4")["examples"][0]["request"])
    raw["scope"] = {"indicator_id": "vwap-1", "instrument_id": "synthetic-1", "timeframe": "15m",
                    "start": ORIGIN.isoformat(), "end": (ORIGIN+6*STEP).isoformat()}
    raw["inputs"] = []
    raw["detector"] = {"type": "indicator_event", "output_name": "vwap_cross", "event_keys": [
        {"key": "close_cross_above_vwap", "direction": "long"},
        {"key": "close_cross_below_vwap", "direction": "short"}]}
    raw["outcomes"] = {"horizons": [1], "primary_horizon": 1}
    raw["statistics"] = {"features": {"baseline": [
        {"name": "distance", "operator": "event_metadata_number", "path": "metadata.close_distance"}]}}
    definition, request = normalize_check_request(raw)
    plan = plan_research_check(definition, request, inspect_coverage=False,
        instrument_loader=lambda key: {"id": key, "symbol": "SYNTHETIC"}, indicator_planner=planner)
    assert plan.indicator_graph[0]["manifest"]["type"] == "candle_vwap"
    assert any("volume" in row["required_fields"] for row in plan.market_data_requirements)
    evidence = collect(monkeypatch)
    evaluator = CHECK_REGISTRY.resolve_evaluator(definition)
    inputs = {**request.parameters, "indicator_evidence": evidence, "data_quality": {"status": "clean"}}
    result = evaluator.evaluate(plan=plan, inputs=inputs)
    assert result["candidate_count"] >= 2 and result["sample_count"] >= 2
    assert result["event_ownership"] == "indicator"
    assert result["events"][0]["features"]["distance"] == pytest.approx(5)
    assert semantic_hash(result) == semantic_hash(evaluator.evaluate(plan=plan, inputs=inputs))
    # A later-known event cannot be relabeled as visible at the original cutoff.
    delayed = deepcopy(inputs)
    events = [r for r in delayed["indicator_evidence"]["outputs"] if r["output_type"] == "signal"]
    events[0]["known_at"] = events[0]["event"]["known_at"] = (ORIGIN+timedelta(days=1)).isoformat()
    later = evaluator.evaluate(plan=plan, inputs=delayed)
    assert later["candidate_count"] == result["candidate_count"]-1
