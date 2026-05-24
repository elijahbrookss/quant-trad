from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import pytest

from engines.indicator_engine.contracts import RuntimeOutput
from strategies.compiler import IndicatorMetaGetter, compile_strategy
from strategies.evaluator import (
    BULKY_DECISION_DETAIL_FIELDS,
    DECISION_DETAIL_FIELD_CLASSIFICATION,
    MINIMAL_DECISION_ARTIFACT_FIELDS,
    DecisionEvaluationState,
    StrategyOutputHistoryRecord,
    advance_decision_state,
    evaluate_strategy_bar,
)


_SIGNAL_OUTPUT = {"name": "sig", "type": "signal", "event_keys": ["breakout_long"]}
_CONTEXT_OUTPUT = {"name": "ctx", "type": "context", "fields": ["state"]}
_METRIC_OUTPUT = {"name": "metric", "type": "metric", "fields": ["score"]}


def _make_meta_getter(outputs: list[dict[str, Any]]) -> IndicatorMetaGetter:
    def getter(indicator_id: str) -> dict[str, Any]:
        _ = indicator_id
        return {"typed_outputs": outputs}

    return getter


def _trigger() -> dict[str, Any]:
    return {
        "type": "signal_match",
        "indicator_id": "ind-1",
        "output_name": "sig",
        "event_key": "breakout_long",
    }


def _trigger_for(event_key: str) -> dict[str, Any]:
    return {
        "type": "signal_match",
        "indicator_id": "ind-1",
        "output_name": "sig",
        "event_key": event_key,
    }


def _outputs(bar_time: datetime) -> dict[str, RuntimeOutput]:
    return {
        "ind-1.sig": RuntimeOutput(
            bar_time=bar_time,
            ready=True,
            value={"events": [{"key": "breakout_long"}]},
        ),
        "ind-1.ctx": RuntimeOutput(
            bar_time=bar_time,
            ready=True,
            value={"state_key": "trend", "fields": {"state": "trend"}},
        ),
        "ind-1.metric": RuntimeOutput(
            bar_time=bar_time,
            ready=True,
            value={"score": 12.5},
        ),
    }


def _signal_output(event_keys: list[str]) -> dict[str, Any]:
    return {"name": "sig", "type": "signal", "event_keys": event_keys}


def _signal_runtime_output(bar_time: datetime, event_keys: list[str]) -> RuntimeOutput:
    return RuntimeOutput(
        bar_time=bar_time,
        ready=True,
        value={
            "events": [{"key": key, "payload": {"large": ["not-history"]}} for key in event_keys],
            "debug": {"blob": ["not-history"]},
            "details": {"trace": ["not-history"]},
            "overlays": [{"overlay_id": "not-history"}],
        },
    )


def test_evaluator_selects_highest_priority_matching_rule() -> None:
    compiled = compile_strategy(
        strategy_id="strategy-1",
        timeframe="1m",
        rules=[
            {
                "id": "rule-b",
                "name": "Lower priority",
                "intent": "enter_long",
                "priority": 10,
                "trigger": _trigger(),
                "guards": [],
            },
            {
                "id": "rule-a",
                "name": "Higher priority",
                "intent": "enter_long",
                "priority": 50,
                "trigger": _trigger(),
                "guards": [],
            },
        ],
        attached_indicator_ids=["ind-1"],
        indicator_meta_getter=_make_meta_getter([_SIGNAL_OUTPUT]),
    )

    bar_time = datetime(2026, 4, 4, 12, 0, tzinfo=timezone.utc)
    result = evaluate_strategy_bar(
        compiled_strategy=compiled,
        state=DecisionEvaluationState(),
        outputs={
            "ind-1.sig": RuntimeOutput(
                bar_time=bar_time,
                ready=True,
                value={"events": [{"key": "breakout_long"}]},
            )
        },
        output_types={"ind-1.sig": "signal"},
        instrument_id="instrument-1",
        symbol="BTCUSD",
        timeframe="1m",
        bar_time=bar_time,
    )

    assert result.selected_artifact is not None
    assert result.selected_artifact["rule_id"] == "rule-a"
    assert result.selected_artifact["strategy_hash"] == compiled.strategy_hash
    suppressed = next(artifact for artifact in result.artifacts if artifact["rule_id"] == "rule-b")
    assert suppressed["evaluation_result"] == "matched_suppressed"
    assert suppressed["suppression_reason"] == "higher_priority_rule_selected"


def test_evaluator_does_not_select_disabled_rule_even_when_it_matches() -> None:
    compiled = compile_strategy(
        strategy_id="strategy-1",
        timeframe="1m",
        rules=[
            {
                "id": "rule-a",
                "name": "Disabled",
                "intent": "enter_long",
                "priority": 100,
                "enabled": False,
                "trigger": _trigger(),
                "guards": [],
            },
            {
                "id": "rule-b",
                "name": "Enabled",
                "intent": "enter_long",
                "priority": 0,
                "trigger": _trigger(),
                "guards": [],
            },
        ],
        attached_indicator_ids=["ind-1"],
        indicator_meta_getter=_make_meta_getter([_SIGNAL_OUTPUT]),
    )

    bar_time = datetime(2026, 4, 4, 12, 0, tzinfo=timezone.utc)
    result = evaluate_strategy_bar(
        compiled_strategy=compiled,
        state=DecisionEvaluationState(),
        outputs={
            "ind-1.sig": RuntimeOutput(
                bar_time=bar_time,
                ready=True,
                value={"events": [{"key": "breakout_long"}]},
            )
        },
        output_types={"ind-1.sig": "signal"},
        instrument_id="instrument-1",
        symbol="BTCUSD",
        timeframe="1m",
        bar_time=bar_time,
    )

    assert result.selected_artifact is not None
    assert result.selected_artifact["rule_id"] == "rule-b"
    assert result.selected_artifact["strategy_hash"] == compiled.strategy_hash
    disabled = next(artifact for artifact in result.artifacts if artifact["rule_id"] == "rule-a")
    assert disabled["enabled"] is False
    assert disabled["evaluation_result"] == "not_matched"


def test_decision_detail_field_classification_separates_runtime_contract_from_debug_details() -> None:
    assert DECISION_DETAIL_FIELD_CLASSIFICATION["decision_context"] == "A"
    assert DECISION_DETAIL_FIELD_CLASSIFICATION["artifact_summary"] == "B"
    assert DECISION_DETAIL_FIELD_CLASSIFICATION["trigger"] == "C"
    assert DECISION_DETAIL_FIELD_CLASSIFICATION["guard_results"] == "C"
    assert DECISION_DETAIL_FIELD_CLASSIFICATION["matched"] == "D"
    assert {"decision_context", "artifact_summary", "decision_id", "emitted_intent"}.issubset(
        MINIMAL_DECISION_ARTIFACT_FIELDS
    )
    assert BULKY_DECISION_DETAIL_FIELDS == {"trigger", "guard_results"}


def test_minimal_decision_details_preserve_selected_decision_without_debug_blobs() -> None:
    compiled = compile_strategy(
        strategy_id="strategy-1",
        timeframe="1m",
        rules=[
            {
                "id": "rule-a",
                "name": "Breakout",
                "intent": "enter_long",
                "priority": 50,
                "trigger": _trigger(),
                "guards": [
                    {
                        "type": "context_match",
                        "indicator_id": "ind-1",
                        "output_name": "ctx",
                        "field": "state",
                        "value": ["trend"],
                    },
                    {
                        "type": "holds_for_bars",
                        "bars": 2,
                        "guard": {
                            "type": "metric_match",
                            "indicator_id": "ind-1",
                            "output_name": "metric",
                            "field": "score",
                            "operator": ">=",
                            "value": 10,
                        },
                    },
                ],
            }
        ],
        attached_indicator_ids=["ind-1"],
        indicator_meta_getter=_make_meta_getter([_SIGNAL_OUTPUT, _CONTEXT_OUTPUT, _METRIC_OUTPUT]),
    )
    first_time = datetime(2026, 4, 4, 12, 0, tzinfo=timezone.utc)
    second_time = datetime(2026, 4, 4, 12, 1, tzinfo=timezone.utc)
    full_state = DecisionEvaluationState()
    minimal_state = DecisionEvaluationState()
    output_types = {"ind-1.sig": "signal", "ind-1.ctx": "context", "ind-1.metric": "metric"}

    evaluate_strategy_bar(
        compiled_strategy=compiled,
        state=full_state,
        outputs=_outputs(first_time),
        output_types=output_types,
        instrument_id="instrument-1",
        symbol="BTCUSD",
        timeframe="1m",
        bar_time=first_time,
    )
    evaluate_strategy_bar(
        compiled_strategy=compiled,
        state=minimal_state,
        outputs=_outputs(first_time),
        output_types=output_types,
        instrument_id="instrument-1",
        symbol="BTCUSD",
        timeframe="1m",
        bar_time=first_time,
        minimal_decision_details=True,
    )

    full = evaluate_strategy_bar(
        compiled_strategy=compiled,
        state=full_state,
        outputs=_outputs(second_time),
        output_types=output_types,
        instrument_id="instrument-1",
        symbol="BTCUSD",
        timeframe="1m",
        bar_time=second_time,
    )
    minimal = evaluate_strategy_bar(
        compiled_strategy=compiled,
        state=minimal_state,
        outputs=_outputs(second_time),
        output_types=output_types,
        instrument_id="instrument-1",
        symbol="BTCUSD",
        timeframe="1m",
        bar_time=second_time,
        minimal_decision_details=True,
    )

    assert minimal.selected_artifact is not None
    assert full.selected_artifact is not None
    assert minimal.selected_artifact["decision_id"] == full.selected_artifact["decision_id"]
    assert minimal.selected_artifact["evaluation_result"] == full.selected_artifact["evaluation_result"]
    assert minimal.selected_artifact["emitted_intent"] == full.selected_artifact["emitted_intent"]
    assert minimal.selected_artifact["decision_context"] == {
        "trigger_output_ref": "ind-1.sig",
        "event_key": "breakout_long",
        "intent": "enter_long",
        "direction": "long",
    }
    assert sorted(minimal.selected_artifact["referenced_outputs"]) == [
        "ind-1.ctx",
        "ind-1.metric",
        "ind-1.sig",
    ]
    assert minimal.selected_artifact["referenced_outputs"]["ind-1.ctx"] == {
        "output_ref": "ind-1.ctx",
        "indicator_id": "ind-1",
        "output_name": "ctx",
        "type": "context",
        "output_type": "context",
        "ready": True,
        "bar_time": "2026-04-04T12:01:00Z",
        "indicator_commit_seq": 0,
        "indicator_commit_seq_status": "unassigned",
        "state_key": "trend",
        "fields": {"state": "trend"},
    }
    assert minimal.selected_artifact["referenced_outputs"]["ind-1.metric"]["fields"] == {"score": 12.5}
    assert minimal.selected_artifact["referenced_outputs"]["ind-1.sig"]["event_keys"] == ["breakout_long"]
    assert minimal.selected_artifact["referenced_outputs"]["ind-1.sig"]["events"] == [{"key": "breakout_long"}]
    assert sorted(minimal.selected_artifact["observed_outputs"]) == [
        "ind-1.ctx",
        "ind-1.metric",
        "ind-1.sig",
    ]
    assert minimal.selected_artifact["observed_outputs"]["ind-1.ctx"]["fields"] == {"state": "trend"}
    assert minimal.selected_artifact["observed_outputs"]["ind-1.metric"]["fields"] == {"score": 12.5}
    assert minimal.selected_artifact["artifact_summary"] == {
        "trigger_ready": True,
        "trigger_matched": True,
        "guard_count": 2,
        "guards_ready": 2,
        "guards_matched": 2,
        "matched": True,
    }
    assert not BULKY_DECISION_DETAIL_FIELDS.intersection(minimal.selected_artifact)
    assert set(minimal.selected_artifact).issubset(MINIMAL_DECISION_ARTIFACT_FIELDS)
    assert "window_results" in full.selected_artifact["guard_results"][1]


def test_output_filter_trace_is_compact_audit_lineage() -> None:
    compiled = compile_strategy(
        strategy_id="strategy-1",
        timeframe="1m",
        rules=[
            {
                "id": "rule-a",
                "name": "Breakout",
                "intent": "enter_long",
                "priority": 50,
                "trigger": _trigger(),
                "guards": [
                    {
                        "type": "metric_match",
                        "indicator_id": "ind-1",
                        "output_name": "metric",
                        "field": "score",
                        "operator": ">=",
                        "value": 10,
                        "source": {
                            "type": "variant_output_filter",
                            "filter_index": 0,
                            "filter_hash": "filter-hash-1",
                            "operator": ">=",
                            "scope": {"intent": ["enter_long"]},
                        },
                    }
                ],
            }
        ],
        attached_indicator_ids=["ind-1"],
        indicator_meta_getter=_make_meta_getter([_SIGNAL_OUTPUT, _METRIC_OUTPUT]),
    )
    bar_time = datetime(2026, 4, 4, 12, 0, tzinfo=timezone.utc)

    result = evaluate_strategy_bar(
        compiled_strategy=compiled,
        state=DecisionEvaluationState(),
        outputs={
            "ind-1.sig": _outputs(bar_time)["ind-1.sig"],
            "ind-1.metric": _outputs(bar_time)["ind-1.metric"],
        },
        output_types={"ind-1.sig": "signal", "ind-1.metric": "metric"},
        instrument_id="instrument-1",
        symbol="BTCUSD",
        timeframe="1m",
        bar_time=bar_time,
        minimal_decision_details=True,
    )

    assert result.selected_artifact is not None
    assert "guard_results" not in result.selected_artifact
    assert result.selected_artifact["output_filter_trace"] == {
        "schema_version": "strategy_output_filter_trace.v1",
        "filter_count": 1,
        "ready_count": 1,
        "matched_count": 1,
        "all_matched": True,
        "items": [
            {
                "filter_index": 0,
                "filter_hash": "filter-hash-1",
                "scope": {"intent": ["enter_long"]},
                "guard_type": "metric_match",
                "output_ref": "ind-1.metric",
                "field": "score",
                "operator": ">=",
                "expected": 10.0,
                "actual": 12.5,
                "ready": True,
                "matched": True,
            }
        ],
    }


def test_compact_history_stores_only_guard_needed_fields(monkeypatch: pytest.MonkeyPatch) -> None:
    def fail_copy(_self: RuntimeOutput) -> RuntimeOutput:
        raise AssertionError("RuntimeOutput.copy must not be used for strategy history")

    monkeypatch.setattr(RuntimeOutput, "copy", fail_copy)
    state = DecisionEvaluationState()
    bar_time = datetime(2026, 4, 4, 12, 0, tzinfo=timezone.utc)

    advance_decision_state(
        state,
        outputs={"ind-1.sig": _signal_runtime_output(bar_time, ["setup_seen"])},
        output_types={"ind-1.sig": "signal"},
        max_history_bars=3,
    )

    record = state.output_history["ind-1.sig"][0]
    assert isinstance(record, StrategyOutputHistoryRecord)
    assert record.output_key == "ind-1.sig"
    assert record.indicator_id == "ind-1"
    assert record.output_name == "sig"
    assert record.output_type == "signal"
    assert record.bar_time == bar_time
    assert record.ready is True
    assert dict(record.value) == {"event_keys": ("setup_seen",)}


def test_runtime_output_debug_details_and_overlays_do_not_enter_strategy_history() -> None:
    state = DecisionEvaluationState()
    bar_time = datetime(2026, 4, 4, 12, 0, tzinfo=timezone.utc)

    advance_decision_state(
        state,
        outputs={
            "ind-1.ctx": RuntimeOutput(
                bar_time=bar_time,
                ready=True,
                value={
                    "state_key": "trend",
                    "fields": {"state": "trend", "nested": {"debug": True}},
                    "details": {"trace": ["not-history"]},
                    "debug": {"large": ["not-history"]},
                    "overlays": [{"overlay_id": "not-history"}],
                },
            ),
            "ind-1.metric": RuntimeOutput(
                bar_time=bar_time,
                ready=True,
                value={"score": 12.5, "debug": {"large": ["not-history"]}},
            ),
        },
        output_types={"ind-1.ctx": "context", "ind-1.metric": "metric"},
        max_history_bars=2,
    )

    context_record = state.output_history["ind-1.ctx"][0]
    metric_record = state.output_history["ind-1.metric"][0]
    assert set(context_record.value) == {"state_key", "fields"}
    assert dict(context_record.value["fields"])["state"] == "trend"
    assert "details" not in context_record.value
    assert "debug" not in context_record.value
    assert "overlays" not in context_record.value
    assert dict(metric_record.value) == {"score": 12.5}


def test_signal_seen_window_behavior_unchanged_with_compact_history() -> None:
    compiled = compile_strategy(
        strategy_id="strategy-1",
        timeframe="1m",
        rules=[
            {
                "id": "rule-a",
                "name": "Setup seen",
                "intent": "enter_long",
                "priority": 1,
                "trigger": _trigger_for("breakout_long"),
                "guards": [
                    {
                        "type": "signal_seen_within_bars",
                        "indicator_id": "ind-1",
                        "output_name": "sig",
                        "event_key": "setup_seen",
                        "lookback_bars": 2,
                    }
                ],
            }
        ],
        attached_indicator_ids=["ind-1"],
        indicator_meta_getter=_make_meta_getter([_signal_output(["breakout_long", "setup_seen"])]),
    )
    state = DecisionEvaluationState()
    output_types = {"ind-1.sig": "signal"}
    first_time = datetime(2026, 4, 4, 12, 0, tzinfo=timezone.utc)
    second_time = datetime(2026, 4, 4, 12, 1, tzinfo=timezone.utc)

    first = _signal_runtime_output(first_time, ["setup_seen"])
    evaluate_strategy_bar(
        compiled_strategy=compiled,
        state=state,
        outputs={"ind-1.sig": first},
        output_types=output_types,
        instrument_id="instrument-1",
        symbol="BTCUSD",
        timeframe="1m",
        bar_time=first_time,
    )
    first.value["events"][0]["key"] = "mutated_after_history"
    second = evaluate_strategy_bar(
        compiled_strategy=compiled,
        state=state,
        outputs={"ind-1.sig": _signal_runtime_output(second_time, ["breakout_long"])},
        output_types=output_types,
        instrument_id="instrument-1",
        symbol="BTCUSD",
        timeframe="1m",
        bar_time=second_time,
    )

    assert second.selected_artifact is not None
    assert second.selected_artifact["rule_id"] == "rule-a"


def test_signal_absent_window_behavior_unchanged_with_compact_history() -> None:
    compiled = compile_strategy(
        strategy_id="strategy-1",
        timeframe="1m",
        rules=[
            {
                "id": "rule-a",
                "name": "No block",
                "intent": "enter_long",
                "priority": 1,
                "trigger": _trigger_for("breakout_long"),
                "guards": [
                    {
                        "type": "signal_absent_within_bars",
                        "indicator_id": "ind-1",
                        "output_name": "sig",
                        "event_key": "blocked",
                        "lookback_bars": 2,
                    }
                ],
            }
        ],
        attached_indicator_ids=["ind-1"],
        indicator_meta_getter=_make_meta_getter([_signal_output(["breakout_long", "blocked"])]),
    )
    output_types = {"ind-1.sig": "signal"}
    first_time = datetime(2026, 4, 4, 12, 0, tzinfo=timezone.utc)
    second_time = datetime(2026, 4, 4, 12, 1, tzinfo=timezone.utc)
    absent_state = DecisionEvaluationState()
    blocked_state = DecisionEvaluationState()

    evaluate_strategy_bar(
        compiled_strategy=compiled,
        state=absent_state,
        outputs={"ind-1.sig": _signal_runtime_output(first_time, [])},
        output_types=output_types,
        instrument_id="instrument-1",
        symbol="BTCUSD",
        timeframe="1m",
        bar_time=first_time,
    )
    absent = evaluate_strategy_bar(
        compiled_strategy=compiled,
        state=absent_state,
        outputs={"ind-1.sig": _signal_runtime_output(second_time, ["breakout_long"])},
        output_types=output_types,
        instrument_id="instrument-1",
        symbol="BTCUSD",
        timeframe="1m",
        bar_time=second_time,
    )

    evaluate_strategy_bar(
        compiled_strategy=compiled,
        state=blocked_state,
        outputs={"ind-1.sig": _signal_runtime_output(first_time, ["blocked"])},
        output_types=output_types,
        instrument_id="instrument-1",
        symbol="BTCUSD",
        timeframe="1m",
        bar_time=first_time,
    )
    blocked = evaluate_strategy_bar(
        compiled_strategy=compiled,
        state=blocked_state,
        outputs={"ind-1.sig": _signal_runtime_output(second_time, ["breakout_long"])},
        output_types=output_types,
        instrument_id="instrument-1",
        symbol="BTCUSD",
        timeframe="1m",
        bar_time=second_time,
    )

    assert absent.selected_artifact is not None
    assert blocked.selected_artifact is None


def test_temporal_window_guard_behavior_and_mutation_isolation_are_preserved() -> None:
    compiled = compile_strategy(
        strategy_id="strategy-1",
        timeframe="1m",
        rules=[
            {
                "id": "rule-a",
                "name": "Score holds",
                "intent": "enter_long",
                "priority": 1,
                "trigger": _trigger(),
                "guards": [
                    {
                        "type": "holds_for_bars",
                        "bars": 2,
                        "guard": {
                            "type": "metric_match",
                            "indicator_id": "ind-1",
                            "output_name": "metric",
                            "field": "score",
                            "operator": ">=",
                            "value": 10,
                        },
                    }
                ],
            }
        ],
        attached_indicator_ids=["ind-1"],
        indicator_meta_getter=_make_meta_getter([_SIGNAL_OUTPUT, _METRIC_OUTPUT]),
    )
    state = DecisionEvaluationState()
    output_types = {"ind-1.sig": "signal", "ind-1.metric": "metric"}
    first_time = datetime(2026, 4, 4, 12, 0, tzinfo=timezone.utc)
    second_time = datetime(2026, 4, 4, 12, 1, tzinfo=timezone.utc)
    first_metric = RuntimeOutput(bar_time=first_time, ready=True, value={"score": 12.5})

    first = evaluate_strategy_bar(
        compiled_strategy=compiled,
        state=state,
        outputs={"ind-1.sig": _signal_runtime_output(first_time, ["breakout_long"]), "ind-1.metric": first_metric},
        output_types=output_types,
        instrument_id="instrument-1",
        symbol="BTCUSD",
        timeframe="1m",
        bar_time=first_time,
    )
    first_metric.value["score"] = 1.0
    second = evaluate_strategy_bar(
        compiled_strategy=compiled,
        state=state,
        outputs={
            "ind-1.sig": _signal_runtime_output(second_time, ["breakout_long"]),
            "ind-1.metric": RuntimeOutput(bar_time=second_time, ready=True, value={"score": 12.0}),
        },
        output_types=output_types,
        instrument_id="instrument-1",
        symbol="BTCUSD",
        timeframe="1m",
        bar_time=second_time,
    )

    assert first.selected_artifact is None
    assert second.selected_artifact is not None
    history_record = state.output_history["ind-1.metric"][0]
    with pytest.raises(TypeError):
        history_record.value["score"] = 4.0  # type: ignore[index]


def test_multi_output_strategy_behavior_unchanged_with_compact_history() -> None:
    compiled = compile_strategy(
        strategy_id="strategy-1",
        timeframe="1m",
        rules=[
            {
                "id": "rule-a",
                "name": "Multi output",
                "intent": "enter_long",
                "priority": 1,
                "trigger": _trigger(),
                "guards": [
                    {
                        "type": "context_match",
                        "indicator_id": "ind-1",
                        "output_name": "ctx",
                        "field": "state",
                        "value": ["trend"],
                    },
                    {
                        "type": "metric_match",
                        "indicator_id": "ind-1",
                        "output_name": "metric",
                        "field": "score",
                        "operator": ">",
                        "value": 10,
                    },
                ],
            }
        ],
        attached_indicator_ids=["ind-1"],
        indicator_meta_getter=_make_meta_getter([_SIGNAL_OUTPUT, _CONTEXT_OUTPUT, _METRIC_OUTPUT]),
    )
    bar_time = datetime(2026, 4, 4, 12, 0, tzinfo=timezone.utc)

    result = evaluate_strategy_bar(
        compiled_strategy=compiled,
        state=DecisionEvaluationState(),
        outputs=_outputs(bar_time),
        output_types={"ind-1.sig": "signal", "ind-1.ctx": "context", "ind-1.metric": "metric"},
        instrument_id="instrument-1",
        symbol="BTCUSD",
        timeframe="1m",
        bar_time=bar_time,
    )

    assert result.selected_artifact is not None
    assert result.selected_artifact["rule_id"] == "rule-a"


def test_missing_output_behavior_unchanged_with_compact_history() -> None:
    compiled = compile_strategy(
        strategy_id="strategy-1",
        timeframe="1m",
        rules=[
            {
                "id": "rule-a",
                "name": "Missing metric",
                "intent": "enter_long",
                "priority": 1,
                "trigger": _trigger(),
                "guards": [
                    {
                        "type": "metric_match",
                        "indicator_id": "ind-1",
                        "output_name": "metric",
                        "field": "score",
                        "operator": ">",
                        "value": 10,
                    }
                ],
            }
        ],
        attached_indicator_ids=["ind-1"],
        indicator_meta_getter=_make_meta_getter([_SIGNAL_OUTPUT, _METRIC_OUTPUT]),
    )
    bar_time = datetime(2026, 4, 4, 12, 0, tzinfo=timezone.utc)

    with pytest.raises(RuntimeError, match="strategy_output_missing: output=ind-1.metric"):
        evaluate_strategy_bar(
            compiled_strategy=compiled,
            state=DecisionEvaluationState(),
            outputs={"ind-1.sig": _signal_runtime_output(bar_time, ["breakout_long"])},
            output_types={"ind-1.sig": "signal", "ind-1.metric": "metric"},
            instrument_id="instrument-1",
            symbol="BTCUSD",
            timeframe="1m",
            bar_time=bar_time,
        )
