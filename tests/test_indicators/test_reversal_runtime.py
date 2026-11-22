"""Synthetic factory/runtime contracts; no provider, DB or research dispatch."""
from dataclasses import replace
from datetime import datetime, timedelta, timezone
import copy

import pandas as pd
import pytest

from engines.bot_runtime.core.domain import Candle
from engines.indicator_engine import IndicatorExecutionEngine
from engines.indicator_engine.contracts import IndicatorGapRejectedError, validate_runtime_output
from indicators.config import IndicatorExecutionContext
from indicators.definition_contract import definition_supports_runtime, validate_indicator_definition
from indicators.manifest import validate_indicator_manifest
from indicators.reversal.definition import ReversalIndicator
from indicators.reversal.manifest import MANIFEST
from research_science.recognition import RecognitionCandle, RecognitionContext
from research_science.reversal_recognition import ReversalRecognizer, ReversalSettings
from research_science.completed_event_context import CompletedEventContext
from research_science.reversal_completed_recovery import recovery_candles

ORIGIN = datetime(2022, 1, 1, tzinfo=timezone.utc)
STEP = timedelta(minutes=15)


def frame(closes=(10., 9., 9.2, 8.8, 8.7, 9.6, 9.9, 10.)):
    rows = []
    for i, close in enumerate(closes):
        opened = closes[i-1] if i else close+.1
        high = max(opened, close)+.1
        if i == 2:
            high = 9.5
        if i == 3:
            high = max(high, 9.7)
        rows.append({"timestamp": ORIGIN+i*STEP, "open": opened, "high": high,
            "low": min(opened, close)-.1, "close": close,
            "close_time": ORIGIN+(i+1)*STEP, "known_at": ORIGIN+(i+1)*STEP,
            "source_identity_key": "canonical-source-v1", "instrument_id": "instrument-actual-id",
            "interval": "15m"})
    return pd.DataFrame(rows).set_index("timestamp", drop=False)


def build(source, **params):
    context = IndicatorExecutionContext(symbol="BTC/USD", start=ORIGIN.isoformat(),
        end=(ORIGIN+len(source)*STEP).isoformat(), interval="15m",
        instrument_id="instrument-actual-id")
    resolved = ReversalIndicator.resolve_config({"computation_origin": ORIGIN.isoformat(), **params},
                                                strict_unknown=True)
    request = ReversalIndicator.build_runtime_data_request(resolved_params=resolved, execution_context=context)
    assert request.start_utc().to_pydatetime() == ORIGIN
    facts = ReversalIndicator.build_runtime_source_facts(resolved_params=resolved,
        execution_context=context, source_frame=source)
    from portal.backend.service.indicators.indicator_factory import runtime_indicator_builder_for_type
    factory = runtime_indicator_builder_for_type("reversal")
    runtime = factory(indicator_id="reversal-1",
        meta={"version": "v1"}, resolved_params=resolved, strategy_indicator_metas={},
        execution_context=context, source_facts=facts)
    bars = [Candle(time=c.open_time, end=c.close_time, known_at=c.known_at,
        open=c.open, high=c.high, low=c.low, close=c.close) for c in facts["candles"]]
    return runtime, bars, facts, context, resolved


def test_actual_definition_factory_and_typed_engine_emit_current_completions_only():
    validate_indicator_manifest(MANIFEST)
    validate_indicator_definition("reversal", ReversalIndicator)
    assert definition_supports_runtime(ReversalIndicator)
    runtime, bars, facts, _, _ = build(frame())
    assert all(not x.ready for x in runtime.snapshot().values())
    engine = IndicatorExecutionEngine([runtime])
    signals = []
    for bar in bars:
        result = engine.step(bar=bar, bar_time=bar.time)
        current = runtime.snapshot()
        assert set(current) == {"provisional_reversals", "candidate_lifecycle", "reversal_context"}
        for definition in runtime.runtime_spec.outputs:
            validate_runtime_output(definition=definition, output=current[definition.name],
                                    bar_time=bar.time, dependency_inputs={})
        for event in current["provisional_reversals"].value["events"]:
            assert event["known_at"] == bar.known_at.isoformat().replace("+00:00", "Z")
            assert event["metadata"]["strategy_decision_claim"] is False
            assert event["metadata"]["independent_event_claim"] is False
            assert event["metadata"]["recognition"]["context"]["instrument_id"] == "instrument-actual-id"
            signals.append(event)
        for event in current["candidate_lifecycle"].value["events"]:
            assert event["known_at"] == bar.known_at.isoformat().replace("+00:00", "Z")
    assert len(signals) == 1
    assert runtime.snapshot()["provisional_reversals"].value["events"] == []
    assert runtime.snapshot()["candidate_lifecycle"].value["events"] == []
    assert not any(word in str(signals).lower() for word in ("classification", "outcome", "first_passage"))
    snapshot = runtime.snapshot()
    snapshot["reversal_context"].value["fields"]["processed_bars"] = -1
    assert runtime.snapshot()["reversal_context"].value["fields"]["processed_bars"] == len(bars)


def test_every_prefix_has_identical_causal_outputs_and_no_precomputed_recognition():
    source = frame()
    whole, bars, _, _, _ = build(source)
    assert whole._recognizer.snapshot().processed_bars == 0
    outputs = []
    for bar in bars:
        whole.apply_bar(bar, {})
        outputs.append(copy.deepcopy(whole.snapshot()))
    for n in range(1, len(source)+1):
        prefix, prefix_bars, _, _, _ = build(source.iloc[:n])
        for bar in prefix_bars:
            prefix.apply_bar(bar, {})
        assert prefix.snapshot() == outputs[n-1]


def test_same_clock_chains_retain_distinct_identities_and_shared_context():
    source = frame((15., 14., 14.2, 13.8, 14., 13.5, 13.2, 14.8))
    # Remove the price-specific reference overrides in the default fixture.
    source["high"] = source[["open", "close"]].max(axis=1)+.1
    runtime, bars, facts, _, _ = build(source)
    recognizer = ReversalRecognizer(facts["context"],
        ReversalSettings("rising_leg_plus_retreat", "retain_all_provisional", 19104, 10000))
    for bar, raw in zip(bars, facts["candles"]):
        runtime.apply_bar(bar, {})
        recognizer.apply_bar(raw)
    events = runtime.snapshot()["provisional_reversals"].value["events"]
    expected = recognizer.snapshot().candidates
    assert len(events) == len(expected) == 2
    assert len({x["pattern_id"] for x in events}) == 2
    assert {x["pattern_id"] for x in events} == {x.event_id for x in expected}
    assert {x["metadata"]["recovery_candles"] for x in events} == {recovery_candles(x) for x in expected}


def test_strict_low_tie_keeps_original_low_and_recovery_n():
    source = frame((10., 9., 9.2, 8.8, 8.7, 8.9, 9.9))
    source.loc[source.index[5], "low"] = source.iloc[4]["low"]
    runtime, bars, _, _, _ = build(source)
    for bar in bars:
        runtime.apply_bar(bar, {})
    event, = runtime.snapshot()["provisional_reversals"].value["events"]
    stages = event["metadata"]["recognition"]["stages"]
    low, = [x for x in stages if x["name"] == "lowest_push"]
    assert low["observed_at"] == (ORIGIN+5*STEP).isoformat().replace("+00:00", "Z")
    assert event["metadata"]["recovery_candles"] == 3


def test_completion_uses_existing_strict_prior_context_without_future_tail():
    closes = [100.+i*.01 for i in range(110)]+[101., 100., 100.2, 99.8, 99.7, 100.6, 101.9]
    source = frame(closes)
    # All source bars remain well formed at this different price level.
    source["high"] = source[["open", "close"]].max(axis=1)+.1
    runtime, bars, facts, _, _ = build(source)
    events = []
    for bar in bars:
        runtime.apply_bar(bar, {})
        events.extend(runtime.snapshot()["provisional_reversals"].value["events"])
    assert events
    snapshot = runtime._recognizer.snapshot()
    context = CompletedEventContext(facts["candles"], snapshot.candidates,
        evaluation_start=ORIGIN, evaluation_end=bars[-1].known_at+STEP)
    for event in events:
        original = next(x for x in snapshot.candidates if x.event_id == event["pattern_id"])
        i = context.by_close[original.known_at]
        n = i-context.by_open[original.formation_start]+1
        assert event["metadata"]["context"] == context.geometry(i, n)
        assert event["metadata"]["context_missing_reason"] is None


@pytest.mark.parametrize("column,value", [
    ("source_identity_key", ""), ("source_identity_key", "different-source"),
    ("instrument_id", "different-instrument"), ("known_at", ORIGIN+timedelta(days=9)),
    ("close_time", ORIGIN+timedelta(days=9)), ("interval", "1m")])
def test_projection_refuses_missing_mixed_identity_or_non_native_clock(column, value):
    source = frame()
    source.loc[source.index[1], column] = value
    with pytest.raises(ValueError, match="reversal_source|recognition_invalid"):
        build(source)


def test_missing_source_and_symbol_only_identity_fail_loudly():
    source = frame().drop(columns="source_identity_key")
    with pytest.raises(ValueError, match="canonical instrument/source"):
        build(source)
    runtime, bars, facts, context, params = build(frame())
    with pytest.raises(ValueError, match="instrument_id"):
        ReversalIndicator.build_runtime_indicator(indicator_id="x", meta={}, resolved_params=params,
            strategy_indicator_metas={}, execution_context=replace(context, instrument_id=None), source_facts=facts)


@pytest.mark.parametrize("params", [{"max_bars": True}, {"max_bars": 19105}, {"max_candidates": 10001},
    {"max_candidates": "10"}, {"ownership": "latest"}, {"reference_basis": "rising_leg_only"}])
def test_resource_and_semantic_bounds_fail_closed(params):
    with pytest.raises((ValueError, RuntimeError)):
        ReversalIndicator.resolve_config({"computation_origin": ORIGIN.isoformat(), **params}, strict_unknown=True)


def test_gap_and_old_or_changed_runtime_candle_refuse_without_reset():
    runtime, bars, _, _, _ = build(frame())
    runtime.apply_bar(bars[0], {})
    with pytest.raises(ValueError, match="disagrees"):
        runtime.apply_bar(bars[2], {})
    with pytest.raises(RuntimeError, match="runtime_failed"):
        runtime.apply_bar(bars[1], {})
    runtime, bars, _, _, _ = build(frame())
    with pytest.raises(IndicatorGapRejectedError):
        runtime.handle_gap(policy="continue_degraded", gap={"classification": "missing"},
                           next_bar_time=bars[1].time, rewarm_bars=0)
    with pytest.raises(RuntimeError, match="runtime_failed"):
        runtime.apply_bar(bars[0], {})
    with pytest.raises(ValueError, match="origin/gap"):
        build(frame().iloc[1:])


def test_candidate_and_bar_limits_do_not_truncate_or_reuse_stale_deltas():
    runtime, bars, _, _, _ = build(frame((10., 9., 9.2, 8.8, 9., 8.5)), max_candidates=1)
    with pytest.raises(ValueError, match="maximum references"):
        for bar in bars:
            runtime.apply_bar(bar, {})
    source = frame().iloc[:3]
    runtime, bars, _, _, _ = build(source, max_bars=3)
    for bar in bars:
        runtime.apply_bar(bar, {})
    with pytest.raises(ValueError, match="source projection exhausted|maximum bars"):
        runtime.apply_bar(replace(bars[-1], time=bars[-1].end, end=bars[-1].end+STEP), {})
