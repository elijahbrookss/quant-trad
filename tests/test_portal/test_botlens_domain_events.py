from __future__ import annotations

import pytest

import portal.backend.service.bots.botlens_domain_events as botlens_domain_events
from portal.backend.service.bots.botlens_domain_events import (
    BotLensCandle,
    CandleObservedContext,
    DecisionEmittedContext,
    DiagnosticRecordedContext,
    SignalEmittedContext,
    TradeLifecycleContext,
    build_botlens_domain_events_from_fact_batch,
    build_botlens_domain_events_from_lifecycle,
    deserialize_botlens_domain_event,
    serialize_botlens_domain_event,
)
from portal.backend.service.bots.botlens_projection_batches import projection_batch_from_payload, runtime_event_rows_from_batch


def test_diagnostic_recorded_uses_structured_fields_without_raw_context_blob() -> None:
    events = build_botlens_domain_events_from_fact_batch(
        bot_id="bot-1",
        run_id="run-1",
        payload={
            "known_at": "2026-02-01T00:00:00Z",
            "facts": [
                {
                    "fact_type": "series_state_observed",
                    "series_key": "instr-1|1m",
                    "instrument_id": "instr-1",
                    "symbol": "BTCUSD",
                    "timeframe": "1m",
                },
                {
                    "fact_type": "log_emitted",
                    "log": {
                        "id": "diag-1",
                        "level": "warn",
                        "event": "indicator_soft_limit",
                        "message": "overlay payload soft limit exceeded",
                        "owner": "indicator_guard",
                        "context": {
                            "component": "indicator_guard",
                            "operation": "overlay_snapshot",
                            "status": "degraded",
                            "failure_mode": "payload_limit",
                            "request_id": "req-123",
                            "trace_id": "trace-123",
                            "traceback": "should-not-survive",
                            "raw": {"nested": "should-not-survive"},
                        },
                    },
                },
            ],
        },
    )

    diagnostic = next(event for event in events if event.event_name.value == "DIAGNOSTIC_RECORDED")
    payload = diagnostic.serialize()["context"]

    assert payload["diagnostic_id"] == "diag-1"
    assert payload["level"] == "WARN"
    assert payload["component"] == "indicator_guard"
    assert payload["diagnostic_event"] == "indicator_soft_limit"
    assert payload["operation"] == "overlay_snapshot"
    assert payload["status"] == "degraded"
    assert payload["failure_mode"] == "payload_limit"
    assert payload["request_id"] == "req-123"
    assert payload["trace_id"] == "trace-123"
    assert "attributes" not in payload
    assert "owner" not in payload
    assert "indicator_id" not in payload
    assert "traceback" not in payload
    assert "raw" not in payload


def test_fault_recorded_uses_structured_fields_without_raw_failure_blob() -> None:
    events = build_botlens_domain_events_from_lifecycle(
        bot_id="bot-1",
        run_id="run-1",
        lifecycle={
            "phase": "startup_failed",
            "status": "startup_failed",
            "owner": "runtime",
            "message": "worker crashed during startup",
            "checkpoint_at": "2026-02-01T00:00:00Z",
            "failure": {
                "reason_code": "worker_exit",
                "type": "worker_exception",
                "phase": "startup_failed",
                "owner": "runtime",
                "message": "worker crashed during startup",
                "worker_id": "worker-1",
                "symbol": "BTCUSD",
                "symbols": ["BTCUSD", "ETHUSD"],
                "exception_type": "ValueError",
                "exit_code": 17,
                "traceback": "should-not-survive",
                "stderr_tail": "should-not-survive",
            },
        },
    )

    fault = next(event for event in events if event.event_name.value == "FAULT_RECORDED")
    payload = fault.serialize()["context"]

    assert payload["fault_code"] == "worker_exit"
    assert payload["failure_type"] == "worker_exception"
    assert payload["failure_phase"] == "startup_failed"
    assert payload["component"] == "runtime"
    assert payload["worker_id"] == "worker-1"
    assert payload["symbol"] == "BTCUSD"
    assert payload["affected_symbols"] == ["BTCUSD", "ETHUSD"]
    assert payload["exception_type"] == "ValueError"
    assert payload["exit_code"] == 17
    assert "attributes" not in payload
    assert "traceback" not in payload
    assert "stderr_tail" not in payload


def test_accepted_decision_requires_signal_price() -> None:
    with pytest.raises(ValueError, match="signal_price"):
        build_botlens_domain_events_from_fact_batch(
            bot_id="bot-1",
            run_id="run-1",
            payload={
                "known_at": "2026-02-01T00:00:00Z",
                "facts": [
                    {
                        "fact_type": "series_state_observed",
                        "series_key": "instr-1|1m",
                        "instrument_id": "instr-1",
                        "symbol": "BTCUSD",
                        "timeframe": "1m",
                    },
                    {
                        "fact_type": "decision_emitted",
                        "decision": {
                            "event_id": "evt-1",
                            "event_ts": "2026-02-01T00:00:00Z",
                            "event_name": "DECISION_ACCEPTED",
                            "root_id": "evt-signal-1",
                            "parent_id": "evt-signal-1",
                            "correlation_id": "corr-1",
                            "context": {
                                "run_id": "run-1",
                                "bot_id": "bot-1",
                                "strategy_id": "strategy-1",
                                "symbol": "BTCUSD",
                                "timeframe": "1m",
                                "bar_ts": "2026-02-01T00:00:00Z",
                                "decision_id": "decision-1",
                                "signal_id": "signal-1",
                                "direction": "long",
                            },
                        },
                    },
                ],
            },
        )


def test_decision_events_keep_signal_domain_root_correlation() -> None:
    events = build_botlens_domain_events_from_fact_batch(
        bot_id="bot-1",
        run_id="run-1",
        payload={
            "known_at": "2026-02-01T00:00:00Z",
            "facts": [
                {
                    "fact_type": "series_state_observed",
                    "series_key": "instr-1|1m",
                    "instrument_id": "instr-1",
                    "symbol": "BTCUSD",
                    "timeframe": "1m",
                },
                {
                    "fact_type": "decision_emitted",
                    "decision": {
                        "event_id": "runtime-signal-1",
                        "event_ts": "2026-02-01T00:00:00Z",
                        "event_name": "SIGNAL_EMITTED",
                        "root_id": "runtime-signal-1",
                        "correlation_id": "corr-1",
                        "context": {
                            "run_id": "run-1",
                            "bot_id": "bot-1",
                            "strategy_id": "strategy-1",
                            "symbol": "BTCUSD",
                            "timeframe": "1m",
                            "bar_ts": "2026-02-01T00:00:00Z",
                            "signal_id": "signal-1",
                            "decision_id": "decision-1",
                            "signal_type": "strategy_signal",
                            "direction": "long",
                            "signal_price": 100.0,
                        },
                    },
                },
                {
                    "fact_type": "decision_emitted",
                    "decision": {
                        "event_id": "runtime-decision-1",
                        "event_ts": "2026-02-01T00:00:01Z",
                        "event_name": "DECISION_ACCEPTED",
                        "root_id": "runtime-signal-1",
                        "parent_id": "runtime-signal-1",
                        "correlation_id": "corr-1",
                        "context": {
                            "run_id": "run-1",
                            "bot_id": "bot-1",
                            "strategy_id": "strategy-1",
                            "symbol": "BTCUSD",
                            "timeframe": "1m",
                            "bar_ts": "2026-02-01T00:00:00Z",
                            "signal_id": "signal-1",
                            "decision_id": "decision-1",
                            "direction": "long",
                            "signal_price": 100.0,
                        },
                    },
                },
            ],
        },
    )

    signal = next(event for event in events if event.event_name.value == "SIGNAL_EMITTED")
    decision = next(event for event in events if event.event_name.value == "DECISION_EMITTED")

    assert signal.event_id == "botlens:signal_emitted:runtime-signal-1"
    assert decision.root_id == signal.event_id
    assert decision.parent_id == signal.event_id


@pytest.mark.parametrize(
    ("payload", "error"),
    [
        ({"time": "2026-02-01T00:00:00Z", "high": 2.0, "low": 0.5, "close": 1.5}, "context.candle.open is required"),
        ({"time": "2026-02-01T00:00:00Z", "open": 1.0, "low": 0.5, "close": 1.5}, "context.candle.high is required"),
        ({"time": "2026-02-01T00:00:00Z", "open": 1.0, "high": 2.0, "close": 1.5}, "context.candle.low is required"),
        ({"time": "2026-02-01T00:00:00Z", "open": 1.0, "high": 2.0, "low": 0.5}, "context.candle.close is required"),
        (
            {"time": "2026-02-01T00:00:00Z", "open": "bad", "high": 2.0, "low": 0.5, "close": 1.5},
            "context.candle.open must be a finite number",
        ),
        (
            {"time": "2026-02-01T00:00:00Z", "open": 1.0, "high": [], "low": 0.5, "close": 1.5},
            "context.candle.high must be a finite number",
        ),
        (
            {"time": "2026-02-01T00:00:00Z", "open": float("nan"), "high": 2.0, "low": 0.5, "close": 1.5},
            "context.candle.open must be a finite number",
        ),
        (
            {"time": "2026-02-01T00:00:00Z", "open": 1.0, "high": float("inf"), "low": 0.5, "close": 1.5},
            "context.candle.high must be a finite number",
        ),
    ],
)
def test_botlens_candle_from_payload_rejects_missing_and_invalid_ohlc_fields(
    payload: dict[str, object],
    error: str,
) -> None:
    with pytest.raises(ValueError, match=error):
        BotLensCandle.from_payload(payload)


def test_decision_emitted_context_rejects_unknown_decision_state() -> None:
    with pytest.raises(ValueError, match="context.decision_state must be one of: accepted, rejected"):
        DecisionEmittedContext(
            run_id="run-1",
            bot_id="bot-1",
            series_key="instr-1|1m",
            decision_state="pending_review",
            decision_id="decision-1",
            bar_epoch=1769904000,
        )


def test_rejected_decision_requires_reason_code() -> None:
    with pytest.raises(ValueError, match="context.reason_code is required for rejected decisions"):
        DecisionEmittedContext(
            run_id="run-1",
            bot_id="bot-1",
            series_key="instr-1|1m",
            decision_state="rejected",
            decision_id="decision-1",
            bar_epoch=1769904000,
            message="rejected by rule",
        )


def test_rejected_decision_requires_message() -> None:
    with pytest.raises(ValueError, match="context.message is required for rejected decisions"):
        DecisionEmittedContext(
            run_id="run-1",
            bot_id="bot-1",
            series_key="instr-1|1m",
            decision_state="rejected",
            decision_id="decision-1",
            bar_epoch=1769904000,
            reason_code="rule_blocked",
        )


def test_signal_emitted_context_rejects_signal_id_aliasing_decision_id() -> None:
    with pytest.raises(ValueError, match="context.signal_id must not equal context.decision_id"):
        SignalEmittedContext(
            run_id="run-1",
            bot_id="bot-1",
            series_key="instr-1|1m",
            signal_id="decision-1",
            decision_id="decision-1",
            signal_type="strategy_signal",
            direction="long",
            signal_price=100.0,
            bar_epoch=1769904000,
        )


def test_decision_emitted_context_rejects_signal_id_aliasing_decision_id() -> None:
    with pytest.raises(ValueError, match="context.signal_id must not equal context.decision_id"):
        DecisionEmittedContext(
            run_id="run-1",
            bot_id="bot-1",
            series_key="instr-1|1m",
            decision_state="accepted",
            decision_id="decision-1",
            signal_id="decision-1",
            direction="long",
            signal_price=100.0,
            bar_epoch=1769904000,
        )


def test_trade_updated_fact_rejects_closed_status_without_closed_at() -> None:
    with pytest.raises(ValueError, match="trade_updated fact marks trade closed without closed_at"):
        build_botlens_domain_events_from_fact_batch(
            bot_id="bot-1",
            run_id="run-1",
            payload={
                "known_at": "2026-02-01T00:00:00Z",
                "facts": [
                    {
                        "fact_type": "series_state_observed",
                        "series_key": "instr-1|1m",
                        "instrument_id": "instr-1",
                        "symbol": "BTCUSD",
                        "timeframe": "1m",
                    },
                    {
                        "fact_type": "trade_updated",
                        "series_key": "instr-1|1m",
                        "trade": {
                            "trade_id": "trade-1",
                            "status": "closed",
                            "direction": "long",
                        },
                    },
                ],
            },
        )


@pytest.mark.parametrize(
    ("factory", "event_name"),
    [
        (
            lambda: CandleObservedContext(
                run_id="run-1",
                bot_id="bot-1",
                candle=BotLensCandle.from_payload(
                    {
                        "time": "2026-02-01T00:00:00Z",
                        "open": 1.0,
                        "high": 2.0,
                        "low": 0.5,
                        "close": 1.5,
                    }
                ),
            ),
            "CANDLE_OBSERVED",
        ),
        (
            lambda: SignalEmittedContext(
                run_id="run-1",
                bot_id="bot-1",
                signal_id="signal-1",
                signal_type="strategy_signal",
                direction="long",
                signal_price=100.0,
                bar_epoch=1769904000,
            ),
            "SIGNAL_EMITTED",
        ),
        (
            lambda: DecisionEmittedContext(
                run_id="run-1",
                bot_id="bot-1",
                decision_state="accepted",
                decision_id="decision-1",
                direction="long",
                signal_price=100.0,
                bar_epoch=1769904000,
            ),
            "DECISION_EMITTED",
        ),
        (
            lambda: TradeLifecycleContext(
                run_id="run-1",
                bot_id="bot-1",
                trade_id="trade-1",
                trade_state="open",
                direction="long",
            ),
            "TRADE",
        ),
    ],
)
def test_series_scoped_contexts_require_series_key(factory, event_name: str) -> None:
    with pytest.raises(ValueError, match=fr"context.series_key is required for {event_name}"):
        factory()


def test_run_scoped_diagnostic_context_may_omit_series_key() -> None:
    context = DiagnosticRecordedContext(
        run_id="run-1",
        bot_id="bot-1",
        diagnostic_id=None,
        level="warn",
        message="runtime degraded",
    )

    assert context.series_key is None


def test_runtime_state_fact_maps_health_trigger_event_field() -> None:
    events = build_botlens_domain_events_from_fact_batch(
        bot_id="bot-1",
        run_id="run-1",
        payload={
            "known_at": "2026-02-01T00:00:00Z",
            "facts": [
                {
                    "fact_type": "runtime_state_observed",
                    "event": "bootstrap_completed",
                    "runtime": {
                        "status": "running",
                        "worker_count": 2,
                        "active_workers": 1,
                        "warnings": ["soft limit"],
                    },
                }
            ],
        },
    )

    health = next(event for event in events if event.event_name.value == "HEALTH_STATUS_REPORTED")
    payload = health.serialize()["context"]
    assert payload["run_id"] == "run-1"
    assert payload["bot_id"] == "bot-1"
    assert payload["status"] == "running"
    assert payload["warning_count"] == 1
    assert payload["worker_count"] == 2
    assert payload["active_workers"] == 1
    assert payload["trigger_event"] == "bootstrap_completed"
    assert payload["warnings"] is None
    assert payload["runtime_state"] is None
    assert payload["progress_state"] is None


def test_runtime_state_health_event_id_is_stable_across_known_at_changes() -> None:
    payload = {
        "facts": [
            {
                "fact_type": "runtime_state_observed",
                "runtime": {
                    "status": "running",
                    "worker_count": 2,
                    "active_workers": 2,
                    "runtime_state": "live",
                    "progress_state": "progressing",
                    "last_useful_progress_at": "2026-02-01T00:00:00Z",
                },
            }
        ]
    }

    first = build_botlens_domain_events_from_fact_batch(
        bot_id="bot-1",
        run_id="run-1",
        payload={"known_at": "2026-02-01T00:00:00Z", **payload},
    )
    second = build_botlens_domain_events_from_fact_batch(
        bot_id="bot-1",
        run_id="run-1",
        payload={"known_at": "2026-02-01T00:00:05Z", **payload},
    )

    first_health = next(event for event in first if event.event_name.value == "HEALTH_STATUS_REPORTED")
    second_health = next(event for event in second if event.event_name.value == "HEALTH_STATUS_REPORTED")

    assert first_health.event_id == second_health.event_id


def test_trade_event_correlation_id_is_compacted_to_storage_limit() -> None:
    payload = {
        "known_at": "2026-04-19T07:31:41.779366Z",
        "series_key": "b56202d3-5107-42d1-be1a-bbcadc85bd4c|1h",
        "facts": [
            {
                "fact_type": "series_state_observed",
                "series_key": "b56202d3-5107-42d1-be1a-bbcadc85bd4c|1h",
                "instrument_id": "b56202d3-5107-42d1-be1a-bbcadc85bd4c",
                "symbol": "ETP-20DEC30-CDE",
                "timeframe": "1h",
            },
            {
                "fact_type": "trade_opened",
                "trade": {
                    "trade_id": "545d9ed4-cb57-4e9a-9ee5-6bf710e56193",
                    "status": "open",
                    "opened_at": "2026-03-27T11:00:00Z",
                    "direction": "long",
                },
            },
        ],
    }

    first = build_botlens_domain_events_from_fact_batch(
        bot_id="83bd32b2-79e7-4c05-ab3d-d7f3fbb7ca4d",
        run_id="7e22fc88-e0de-44b2-b7a1-b1e6eff5d4a2",
        payload=payload,
    )
    second = build_botlens_domain_events_from_fact_batch(
        bot_id="83bd32b2-79e7-4c05-ab3d-d7f3fbb7ca4d",
        run_id="7e22fc88-e0de-44b2-b7a1-b1e6eff5d4a2",
        payload=payload,
    )

    first_trade = next(event for event in first if event.event_name.value == "TRADE_OPENED")
    second_trade = next(event for event in second if event.event_name.value == "TRADE_OPENED")
    natural = (
        "7e22fc88-e0de-44b2-b7a1-b1e6eff5d4a2:"
        "b56202d3-5107-42d1-be1a-bbcadc85bd4c|1h:"
        "trade:545d9ed4-cb57-4e9a-9ee5-6bf710e56193:"
        "2026-03-27T11:00:00Z"
    )

    assert len(natural) > 128
    assert len(first_trade.correlation_id) <= 128
    assert first_trade.correlation_id.startswith(
        "7e22fc88-e0de-44b2-b7a1-b1e6eff5d4a2:b56202d3-5107-42d1-be1a-bbcadc85bd4c|1h:trade:"
    )
    assert first_trade.correlation_id != natural
    assert second_trade.correlation_id == first_trade.correlation_id


@pytest.mark.parametrize(
    ("fact_type", "trade", "event_name"),
    [
        (
            "trade_opened",
            {
                "trade_id": "trade-1",
                "status": "open",
                "opened_at": "2026-02-01T00:00:00Z",
                "direction": "long",
            },
            "TRADE_OPENED",
        ),
        (
            "trade_updated",
            {
                "trade_id": "trade-1",
                "status": "open",
                "opened_at": "2026-02-01T00:00:00Z",
                "direction": "long",
                "entry_price": 100.5,
            },
            "TRADE_UPDATED",
        ),
        (
            "trade_closed",
            {
                "trade_id": "trade-1",
                "status": "closed",
                "opened_at": "2026-02-01T00:00:00Z",
                "closed_at": "2026-02-01T00:05:00Z",
                "direction": "long",
                "entry_price": 100.5,
                "exit_price": 101.0,
            },
            "TRADE_CLOSED",
        ),
    ],
)
def test_trade_facts_map_to_explicit_trade_lifecycle_events(
    fact_type: str,
    trade: dict[str, object],
    event_name: str,
) -> None:
    events = build_botlens_domain_events_from_fact_batch(
        bot_id="bot-1",
        run_id="run-1",
        payload={
            "known_at": "2026-02-01T00:00:00Z",
            "facts": [
                {
                    "fact_type": "series_state_observed",
                    "series_key": "instrument-btc|1m",
                    "instrument_id": "instrument-btc",
                    "symbol": "BTC",
                    "timeframe": "1m",
                },
                {
                    "fact_type": fact_type,
                    "series_key": "instrument-btc|1m",
                    "trade": trade,
                },
            ],
        },
    )

    trade_event = next(event for event in events if event.event_name.value == event_name)

    assert trade_event.context.trade_id == "trade-1"
    assert trade_event.context.trade_state == ("closed" if event_name == "TRADE_CLOSED" else "open")


def test_trade_open_uses_simulated_entry_bar_time_not_batch_known_at() -> None:
    events = build_botlens_domain_events_from_fact_batch(
        bot_id="bot-1",
        run_id="run-1",
        payload={
            "known_at": "2026-04-25T07:36:35Z",
            "observed_at": "2026-04-25T07:36:36Z",
            "facts": [
                {
                    "fact_type": "series_state_observed",
                    "series_key": "instrument-btc|1m",
                    "instrument_id": "instrument-btc",
                    "symbol": "BTC",
                    "timeframe": "1m",
                },
                {
                    "fact_type": "trade_opened",
                    "series_key": "instrument-btc|1m",
                    "trade": {
                        "trade_id": "trade-1",
                        "status": "open",
                        "entry_time": "2026-02-01T00:05:00Z",
                        "direction": "long",
                        "strategy_id": "strategy-1",
                        "signal_id": "signal-1",
                        "decision_id": "decision-1",
                    },
                },
            ],
        },
    )

    trade_event = next(event for event in events if event.event_name.value == "TRADE_OPENED")
    payload = serialize_botlens_domain_event(trade_event)

    assert payload["event_ts"] == "2026-02-01T00:05:00Z"
    assert payload["context"]["bar_time"] == "2026-02-01T00:05:00Z"
    assert payload["context"]["event_time"] == "2026-02-01T00:05:00Z"
    assert payload["context"]["observed_at"] == "2026-04-25T07:36:36Z"
    assert payload["context"]["strategy_id"] == "strategy-1"
    assert payload["context"]["signal_id"] == "signal-1"
    assert payload["context"]["decision_id"] == "decision-1"


def test_trade_close_uses_simulated_exit_bar_time_not_batch_known_at() -> None:
    events = build_botlens_domain_events_from_fact_batch(
        bot_id="bot-1",
        run_id="run-1",
        payload={
            "known_at": "2026-04-25T07:43:04Z",
            "facts": [
                {
                    "fact_type": "series_state_observed",
                    "series_key": "instrument-btc|1m",
                    "instrument_id": "instrument-btc",
                    "symbol": "BTC",
                    "timeframe": "1m",
                },
                {
                    "fact_type": "trade_closed",
                    "series_key": "instrument-btc|1m",
                    "trade": {
                        "trade_id": "trade-1",
                        "status": "closed",
                        "entry_time": "2026-02-01T00:05:00Z",
                        "closed_at": "2026-02-01T02:00:00Z",
                        "direction": "long",
                    },
                },
            ],
        },
    )

    trade_event = next(event for event in events if event.event_name.value == "TRADE_CLOSED")
    payload = serialize_botlens_domain_event(trade_event)

    assert payload["event_ts"] == "2026-02-01T02:00:00Z"
    assert payload["context"]["bar_time"] == "2026-02-01T02:00:00Z"
    assert payload["context"]["event_time"] == "2026-02-01T02:00:00Z"


def test_trade_fact_requires_simulated_trade_bar_time() -> None:
    with pytest.raises(ValueError, match="missing simulated trade bar_time"):
        build_botlens_domain_events_from_fact_batch(
            bot_id="bot-1",
            run_id="run-1",
            payload={
                "known_at": "2026-04-25T07:36:35Z",
                "facts": [
                    {
                        "fact_type": "series_state_observed",
                        "series_key": "instrument-btc|1m",
                        "instrument_id": "instrument-btc",
                        "symbol": "BTC",
                        "timeframe": "1m",
                    },
                    {
                        "fact_type": "trade_opened",
                        "series_key": "instrument-btc|1m",
                        "trade": {
                            "trade_id": "trade-1",
                            "status": "open",
                            "direction": "long",
                        },
                    },
                ],
            },
        )


def test_trade_fact_rejects_event_time_that_disagrees_with_bar_time() -> None:
    with pytest.raises(ValueError, match="event_time must match simulated trade bar_time"):
        build_botlens_domain_events_from_fact_batch(
            bot_id="bot-1",
            run_id="run-1",
            payload={
                "known_at": "2026-04-25T07:36:35Z",
                "facts": [
                    {
                        "fact_type": "series_state_observed",
                        "series_key": "instrument-btc|1m",
                        "instrument_id": "instrument-btc",
                        "symbol": "BTC",
                        "timeframe": "1m",
                    },
                    {
                        "fact_type": "trade_opened",
                        "series_key": "instrument-btc|1m",
                        "trade": {
                            "trade_id": "trade-1",
                            "status": "open",
                            "entry_time": "2026-02-01T00:05:00Z",
                            "event_time": "2026-04-25T07:36:35Z",
                            "direction": "long",
                        },
                    },
                ],
            },
        )


def test_rejected_decision_uses_attempt_id_and_preserves_wallet_reason() -> None:
    events = build_botlens_domain_events_from_fact_batch(
        bot_id="bot-1",
        run_id="run-1",
        payload={
            "known_at": "2026-02-01T00:00:00Z",
            "facts": [
                {
                    "fact_type": "series_state_observed",
                    "series_key": "instrument-btc|1m",
                    "instrument_id": "instrument-btc",
                    "symbol": "BTC",
                    "timeframe": "1m",
                },
                {
                    "fact_type": "decision_emitted",
                    "decision": {
                        "event_id": "runtime-decision-1",
                        "event_ts": "2026-02-01T00:00:00Z",
                        "event_name": "DECISION_REJECTED",
                        "root_id": "runtime-signal-1",
                        "parent_id": "runtime-signal-1",
                        "correlation_id": "corr-1",
                        "context": {
                            "run_id": "run-1",
                            "bot_id": "bot-1",
                            "strategy_id": "strategy-1",
                            "symbol": "BTC",
                            "timeframe": "1m",
                            "bar_ts": "2026-02-01T00:00:00Z",
                            "signal_id": "signal-1",
                            "decision_id": "decision-1",
                            "trade_id": "pending-trade-1",
                            "reason_code": "WALLET_INSUFFICIENT_MARGIN",
                            "message": "insufficient margin",
                            "rejection_artifact": {
                                "context": {
                                    "settlement_attempt_id": "pending-trade-1",
                                    "order_request_id": "order-1",
                                }
                            },
                        },
                    },
                },
            ],
        },
    )

    decision_event = next(event for event in events if event.event_name.value == "DECISION_EMITTED")
    payload = serialize_botlens_domain_event(decision_event)

    assert payload["context"]["decision_state"] == "rejected"
    assert "trade_id" not in payload["context"] or payload["context"]["trade_id"] is None
    assert payload["context"]["attempt_id"] == "pending-trade-1"
    assert payload["context"]["settlement_attempt_id"] == "pending-trade-1"
    assert payload["context"]["order_request_id"] == "order-1"
    assert payload["context"]["reason_code"] == "WALLET_INSUFFICIENT_MARGIN"


def test_pre_order_rejected_decision_uses_entry_request_identity() -> None:
    events = build_botlens_domain_events_from_fact_batch(
        bot_id="bot-1",
        run_id="run-1",
        payload={
            "known_at": "2026-02-01T00:00:00Z",
            "facts": [
                {
                    "fact_type": "series_state_observed",
                    "series_key": "instrument-btc|1m",
                    "instrument_id": "instrument-btc",
                    "symbol": "BTC",
                    "timeframe": "1m",
                },
                {
                    "fact_type": "decision_emitted",
                    "decision": {
                        "event_id": "runtime-decision-1",
                        "event_ts": "2026-02-01T00:00:00Z",
                        "event_name": "DECISION_REJECTED",
                        "root_id": "runtime-signal-1",
                        "parent_id": "runtime-signal-1",
                        "correlation_id": "corr-1",
                        "context": {
                            "run_id": "run-1",
                            "bot_id": "bot-1",
                            "strategy_id": "strategy-1",
                            "symbol": "BTC",
                            "timeframe": "1m",
                            "bar_ts": "2026-02-01T00:00:00Z",
                            "signal_id": "signal-1",
                            "decision_id": "decision-1",
                            "reason_code": "WALLET_INSUFFICIENT_MARGIN",
                            "message": "insufficient margin",
                            "rejection_artifact": {
                                "context": {
                                    "entry_request_id": "entry_request:abc",
                                    "attempt_id": "entry_request:abc",
                                }
                            },
                        },
                    },
                },
            ],
        },
    )

    decision_event = next(event for event in events if event.event_name.value == "DECISION_EMITTED")
    payload = serialize_botlens_domain_event(decision_event)

    assert payload["context"]["decision_state"] == "rejected"
    assert "trade_id" not in payload["context"] or payload["context"]["trade_id"] is None
    assert payload["context"]["entry_request_id"] == "entry_request:abc"
    assert payload["context"]["attempt_id"] == "entry_request:abc"
    assert payload["context"]["reason_code"] == "WALLET_INSUFFICIENT_MARGIN"


def test_runtime_rows_use_domain_event_time_as_known_at_for_trade_events() -> None:
    events = build_botlens_domain_events_from_fact_batch(
        bot_id="bot-1",
        run_id="run-1",
        payload={
            "known_at": "2026-04-25T07:36:35Z",
            "observed_at": "2026-04-25T07:36:36Z",
            "facts": [
                {
                    "fact_type": "series_state_observed",
                    "series_key": "instrument-btc|1m",
                    "instrument_id": "instrument-btc",
                    "symbol": "BTC",
                    "timeframe": "1m",
                },
                {
                    "fact_type": "trade_opened",
                    "series_key": "instrument-btc|1m",
                    "trade": {
                        "trade_id": "trade-1",
                        "status": "open",
                        "entry_time": "2026-02-01T00:05:00Z",
                        "direction": "long",
                    },
                },
            ],
        },
    )
    batch = projection_batch_from_payload(
        batch_kind="botlens_runtime_facts",
        run_id="run-1",
        bot_id="bot-1",
        symbol_key="instrument-btc|1m",
        payload={"known_at": "2026-04-25T07:36:35Z"},
        events=events,
        seq=1,
    )

    rows = runtime_event_rows_from_batch(batch=batch)
    trade_row = next(row for row in rows if row["payload"]["event_name"] == "TRADE_OPENED")

    assert trade_row["event_time"] == "2026-02-01T00:05:00Z"
    assert trade_row["known_at"] == "2026-02-01T00:05:00Z"


def test_runtime_state_health_event_id_is_stable_when_only_warning_repeat_metadata_changes() -> None:
    base_warning = {
        "warning_id": "indicator_budget::typed_regime::instrument-btc|1m",
        "warning_type": "indicator_budget",
        "indicator_id": "typed_regime",
        "message": "Execution budget exceeded.",
        "count": 1,
        "first_seen_at": "2026-02-01T00:00:00Z",
        "last_seen_at": "2026-02-01T00:00:00Z",
    }
    first = build_botlens_domain_events_from_fact_batch(
        bot_id="bot-1",
        run_id="run-1",
        payload={
            "known_at": "2026-02-01T00:00:00Z",
            "facts": [
                {
                    "fact_type": "runtime_state_observed",
                    "runtime": {
                        "status": "running",
                        "warnings": [base_warning],
                    },
                }
            ],
        },
    )
    second = build_botlens_domain_events_from_fact_batch(
        bot_id="bot-1",
        run_id="run-1",
        payload={
            "known_at": "2026-02-01T00:00:10Z",
            "facts": [
                {
                    "fact_type": "runtime_state_observed",
                    "runtime": {
                        "status": "running",
                        "warnings": [
                            {
                                **base_warning,
                                "count": 7,
                                "last_seen_at": "2026-02-01T00:00:10Z",
                            }
                        ],
                    },
                }
            ],
        },
    )

    first_health = next(event for event in first if event.event_name.value == "HEALTH_STATUS_REPORTED")
    second_health = next(event for event in second if event.event_name.value == "HEALTH_STATUS_REPORTED")

    assert first_health.event_id == second_health.event_id


def test_runtime_state_health_event_id_changes_for_distinct_degraded_pressure_and_terminal_semantics() -> None:
    base_payload = {
        "facts": [
            {
                "fact_type": "runtime_state_observed",
                "runtime": {
                    "status": "running",
                    "worker_count": 2,
                    "active_workers": 2,
                    "runtime_state": "degraded",
                    "progress_state": "churning",
                    "last_useful_progress_at": "2026-02-01T00:00:00Z",
                    "degraded": {"active": True, "started_at": "2026-02-01T00:00:01Z", "reason_code": "subscriber_gap"},
                    "churn": {"active": True, "detected_at": "2026-02-01T00:00:02Z", "reason_code": "no_progress"},
                    "pressure": {
                        "captured_at": "2026-02-01T00:00:03Z",
                        "trigger": "telemetry_degraded",
                        "top_pressure": {"reason_code": "telemetry_backpressure", "value": 1, "unit": "flag"},
                    },
                    "recent_transitions": [
                        {
                            "from_state": "live",
                            "to_state": "degraded",
                            "transition_reason": "subscriber_gap",
                            "source_component": "runtime_worker",
                            "timestamp": "2026-02-01T00:00:01Z",
                        }
                    ],
                    "terminal": {"actor": "runtime_worker", "reason": "worker fault"},
                },
            }
        ]
    }

    degraded = build_botlens_domain_events_from_fact_batch(
        bot_id="bot-1",
        run_id="run-1",
        payload={"known_at": "2026-02-01T00:00:10Z", **base_payload},
    )
    pressure_changed = build_botlens_domain_events_from_fact_batch(
        bot_id="bot-1",
        run_id="run-1",
        payload={
            "known_at": "2026-02-01T00:00:11Z",
            "facts": [
                {
                    "fact_type": "runtime_state_observed",
                    "runtime": {
                        **base_payload["facts"][0]["runtime"],
                        "pressure": {
                            "captured_at": "2026-02-01T00:00:09Z",
                            "trigger": "telemetry_degraded",
                            "top_pressure": {"reason_code": "payload_bytes", "value": 262144, "unit": "bytes"},
                        },
                    },
                }
            ],
        },
    )
    terminal_cleared = build_botlens_domain_events_from_fact_batch(
        bot_id="bot-1",
        run_id="run-1",
        payload={
            "known_at": "2026-02-01T00:00:12Z",
            "facts": [
                {
                    "fact_type": "runtime_state_observed",
                    "runtime": {
                        **base_payload["facts"][0]["runtime"],
                        "terminal": {},
                    },
                }
            ],
        },
    )

    degraded_health = next(event for event in degraded if event.event_name.value == "HEALTH_STATUS_REPORTED")
    pressure_health = next(event for event in pressure_changed if event.event_name.value == "HEALTH_STATUS_REPORTED")
    terminal_health = next(event for event in terminal_cleared if event.event_name.value == "HEALTH_STATUS_REPORTED")

    assert degraded_health.event_id != pressure_health.event_id
    assert degraded_health.event_id != terminal_health.event_id


def test_runtime_state_health_event_id_changes_when_recent_transitions_change() -> None:
    base_runtime = {
        "status": "running",
        "runtime_state": "degraded",
        "progress_state": "churning",
        "recent_transitions": [
            {
                "from_state": "live",
                "to_state": "degraded",
                "transition_reason": "subscriber_gap",
                "source_component": "runtime_worker",
                "timestamp": "2026-02-01T00:00:01Z",
            }
        ],
    }

    first = build_botlens_domain_events_from_fact_batch(
        bot_id="bot-1",
        run_id="run-1",
        payload={"known_at": "2026-02-01T00:00:00Z", "facts": [{"fact_type": "runtime_state_observed", "runtime": base_runtime}]},
    )
    second = build_botlens_domain_events_from_fact_batch(
        bot_id="bot-1",
        run_id="run-1",
        payload={
            "known_at": "2026-02-01T00:00:10Z",
            "facts": [
                {
                    "fact_type": "runtime_state_observed",
                    "runtime": {
                        **base_runtime,
                        "recent_transitions": [
                            *base_runtime["recent_transitions"],
                            {
                                "from_state": "degraded",
                                "to_state": "live",
                                "transition_reason": "continuity_recovered",
                                "source_component": "runtime_worker",
                                "timestamp": "2026-02-01T00:00:09Z",
                            },
                        ],
                    },
                }
            ],
        },
    )

    first_health = next(event for event in first if event.event_name.value == "HEALTH_STATUS_REPORTED")
    second_health = next(event for event in second if event.event_name.value == "HEALTH_STATUS_REPORTED")

    assert first_health.event_id != second_health.event_id


def test_lifecycle_domain_event_carries_runtime_observability_metadata() -> None:
    events = build_botlens_domain_events_from_lifecycle(
        bot_id="bot-1",
        run_id="run-1",
        lifecycle={
            "phase": "degraded",
            "status": "degraded",
            "owner": "runtime",
            "message": "Runtime continuity degraded.",
            "checkpoint_at": "2026-02-01T00:00:00Z",
            "metadata": {
                "runtime_observability": {
                    "runtime_state": "degraded",
                    "progress_state": "churning",
                    "last_useful_progress_at": "2026-02-01T00:00:00Z",
                    "degraded": {
                        "active": True,
                        "started_at": "2026-02-01T00:00:00Z",
                        "reason_code": "subscriber_gap",
                    },
                }
            },
        },
    )

    lifecycle_event = next(event for event in events if event.event_name.value == "RUN_DEGRADED")
    context = lifecycle_event.serialize()["context"]

    assert context["metadata"]["runtime_observability"]["runtime_state"] == "degraded"
    assert context["metadata"]["runtime_observability"]["progress_state"] == "churning"
    assert context["metadata"]["runtime_observability"]["degraded"]["reason_code"] == "subscriber_gap"


def test_lifecycle_fault_event_preserves_runtime_transition_rejection_fields() -> None:
    events = build_botlens_domain_events_from_lifecycle(
        bot_id="bot-1",
        run_id="run-1",
        lifecycle={
            "phase": "live",
            "status": "running",
            "owner": "runtime",
            "message": "Rejected runtime state transition live -> awaiting_first_snapshot.",
            "checkpoint_at": "2026-02-01T00:00:00Z",
            "failure": {
                "phase": "live",
                "message": "Rejected runtime state transition live -> awaiting_first_snapshot.",
                "type": "runtime_state_transition_rejected",
                "reason_code": "runtime_state_transition_rejected",
                "from_state": "live",
                "attempted_to_state": "awaiting_first_snapshot",
                "transition_reason": "illegal_regression",
                "source_component": "container_runtime",
            },
        },
    )

    fault = next(event for event in events if event.event_name.value == "FAULT_RECORDED")
    context = fault.serialize()["context"]

    assert context["failure_type"] == "runtime_state_transition_rejected"
    assert context["from_state"] == "live"
    assert context["attempted_to_state"] == "awaiting_first_snapshot"
    assert context["transition_reason"] == "illegal_regression"


def test_deserialize_persisted_health_status_rejects_legacy_event_alias() -> None:
    with pytest.raises(ValueError, match="context.event is not allowed"):
        deserialize_botlens_domain_event(
            {
                "schema_version": 1,
                "event_id": "evt-health",
                "event_ts": "2026-02-01T00:00:00Z",
                "event_name": "HEALTH_STATUS_REPORTED",
                "root_id": "evt-health",
                "parent_id": None,
                "correlation_id": "corr-health",
                "context": {
                    "run_id": "run-1",
                    "bot_id": "bot-1",
                    "status": "running",
                    "warning_count": 1,
                    "event": "bootstrap_completed",
                },
            }
        )


def test_deserialize_persisted_diagnostic_rejects_unknown_context_fields() -> None:
    with pytest.raises(ValueError, match="DIAGNOSTIC_RECORDED context contains unsupported fields: traceback"):
        deserialize_botlens_domain_event(
            {
                "schema_version": 1,
                "event_id": "evt-diagnostic",
                "event_ts": "2026-02-01T00:00:00Z",
                "event_name": "DIAGNOSTIC_RECORDED",
                "root_id": "evt-diagnostic",
                "parent_id": None,
                "correlation_id": "corr-diagnostic",
                "context": {
                    "run_id": "run-1",
                    "bot_id": "bot-1",
                    "series_key": "instr-1|1m",
                    "level": "WARN",
                    "message": "bounded diagnostic",
                    "traceback": "must-not-persist",
                },
            }
        )


def test_deserialize_persisted_fault_rejects_unknown_context_fields() -> None:
    with pytest.raises(ValueError, match="FAULT_RECORDED context contains unsupported fields: stderr_tail"):
        deserialize_botlens_domain_event(
            {
                "schema_version": 1,
                "event_id": "evt-fault",
                "event_ts": "2026-02-01T00:00:00Z",
                "event_name": "FAULT_RECORDED",
                "root_id": "evt-fault",
                "parent_id": None,
                "correlation_id": "corr-fault",
                "context": {
                    "run_id": "run-1",
                    "bot_id": "bot-1",
                    "series_key": "instr-1|1m",
                    "fault_code": "runtime_exception",
                    "severity": "ERROR",
                    "message": "bounded fault",
                    "source": "runtime",
                    "stderr_tail": "must-not-persist",
                },
            }
        )


def test_deserialize_persisted_signal_emitted_requires_signal_id_not_decision_id_alias() -> None:
    with pytest.raises(ValueError, match="context.signal_id is required"):
        deserialize_botlens_domain_event(
            {
                "schema_version": 1,
                "event_id": "evt-signal",
                "event_ts": "2026-02-01T00:00:00Z",
                "event_name": "SIGNAL_EMITTED",
                "root_id": "evt-signal",
                "parent_id": None,
                "correlation_id": "corr-signal",
                "context": {
                    "run_id": "run-1",
                    "bot_id": "bot-1",
                    "series_key": "instr-1|1m",
                    "decision_id": "decision-1",
                    "signal_type": "strategy_signal",
                    "direction": "long",
                    "signal_price": 100.0,
                    "bar_epoch": 1769904000,
                },
            }
        )


def test_runtime_error_fact_builds_fault_recorded_domain_event() -> None:
    events = build_botlens_domain_events_from_fact_batch(
        bot_id="bot-1",
        run_id="run-1",
        payload={
            "known_at": "2026-02-01T00:00:00Z",
            "facts": [
                {
                    "fact_type": "series_state_observed",
                    "series_key": "instr-1|1m",
                    "instrument_id": "instr-1",
                    "symbol": "BTCUSD",
                    "timeframe": "1m",
                },
                {
                    "fact_type": "decision_emitted",
                    "decision": {
                        "event_id": "runtime-error-1",
                        "event_ts": "2026-02-01T00:00:05Z",
                        "event_name": "RUNTIME_ERROR",
                        "root_id": "runtime-signal-1",
                        "parent_id": "runtime-decision-1",
                        "correlation_id": "corr-1",
                        "context": {
                            "run_id": "run-1",
                            "bot_id": "bot-1",
                            "strategy_id": "strategy-1",
                            "symbol": "BTCUSD",
                            "timeframe": "1m",
                            "bar_ts": "2026-02-01T00:00:00Z",
                            "reason_code": "runtime_exception",
                            "message": "engine blew up",
                            "exception_type": "RuntimeError",
                            "location": "runtime.loop",
                        },
                    },
                },
            ],
        },
    )

    fault = next(event for event in events if event.event_name.value == "FAULT_RECORDED")
    payload = fault.serialize()

    assert payload["event_id"] == "botlens:fault_recorded:runtime-error-1"
    assert payload["root_id"] == "botlens:fault_recorded:runtime-signal-1"
    assert payload["parent_id"] == "botlens:fault_recorded:runtime-decision-1"
    assert payload["context"]["fault_code"] == "runtime_exception"
    assert payload["context"]["message"] == "engine blew up"
    assert payload["context"]["exception_type"] == "RuntimeError"
    assert payload["context"]["location"] == "runtime.loop"


def test_serialize_botlens_domain_event_persists_bounded_overlay_render_payload() -> None:
    events = build_botlens_domain_events_from_fact_batch(
        bot_id="bot-1",
        run_id="run-1",
        payload={
            "known_at": "2026-02-01T00:00:00Z",
            "facts": [
                {
                    "fact_type": "series_state_observed",
                    "series_key": "instr-1|1m",
                    "instrument_id": "instr-1",
                    "symbol": "BTCUSD",
                    "timeframe": "1m",
                },
                {
                    "fact_type": "overlay_ops_emitted",
                    "overlay_delta": {
                        "seq": 3,
                        "base_seq": 2,
                        "ops": [
                            {
                                "op": "upsert",
                                "key": "atr-short",
                                "overlay": {
                                    "overlay_id": "atr-short",
                                    "type": "candle_stats_atr_short",
                                    "strategy_id": "strategy-1",
                                    "source": "indicator_guard",
                                    "pane_key": "volatility",
                                    "pane_views": ["polyline"],
                                    "payload": {
                                        "polylines": [
                                            {
                                                "points": [
                                                    {"time": 1, "price": 100.0},
                                                    {"time": 2, "price": 101.0},
                                                    {"time": 3, "price": 102.0},
                                                ],
                                                "color": "#38bdf8",
                                            }
                                        ]
                                    },
                                },
                            }
                        ],
                    },
                },
            ],
        },
    )

    overlay_event = next(event for event in events if event.event_name.value == "OVERLAY_STATE_CHANGED")
    payload = serialize_botlens_domain_event(overlay_event)["context"]["overlay_delta"]

    assert payload["seq"] == 3
    assert payload["base_seq"] == 2
    assert payload["op_counts"] == {"upsert": 1}
    assert payload["point_count"] == 3
    assert len(payload["ops"]) == 1
    overlay = payload["ops"][0]["overlay"]
    assert overlay["overlay_id"] == "atr-short"
    assert overlay["type"] == "candle_stats_atr_short"
    assert overlay["strategy_id"] == "strategy-1"
    assert overlay["source"] == "indicator_guard"
    assert overlay["pane_key"] == "volatility"
    assert overlay["pane_views"] == ["polyline"]
    assert overlay["detail_level"] == "bounded_render"
    assert overlay["payload"]["polylines"][0]["points"][2]["price"] == 102.0
    assert overlay["payload_summary"] == {
        "geometry_keys": ["polylines"],
        "payload_counts": {"polylines": 1},
        "point_count": 3,
    }


def test_serialize_botlens_domain_event_preserves_polyline_history_when_payload_is_bounded(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(botlens_domain_events, "_durable_overlay_payload_point_limit", lambda: 2)

    events = build_botlens_domain_events_from_fact_batch(
        bot_id="bot-1",
        run_id="run-1",
        payload={
            "known_at": "2026-02-01T00:00:00Z",
            "facts": [
                {
                    "fact_type": "series_state_observed",
                    "series_key": "instr-1|1m",
                    "instrument_id": "instr-1",
                    "symbol": "BTCUSD",
                    "timeframe": "1m",
                },
                {
                    "fact_type": "overlay_ops_emitted",
                    "overlay_delta": {
                        "seq": 3,
                        "base_seq": 2,
                        "ops": [
                            {
                                "op": "upsert",
                                "key": "atr-short",
                                "overlay": {
                                    "overlay_id": "atr-short",
                                    "type": "candle_stats_atr_short",
                                    "pane_key": "volatility",
                                    "pane_views": ["polyline", "marker"],
                                    "payload": {
                                        "markers": [
                                            {"time": 1, "price": 100.0},
                                            {"time": 2, "price": 101.0},
                                            {"time": 3, "price": 102.0},
                                            {"time": 4, "price": 103.0},
                                        ],
                                        "polylines": [
                                            {
                                                "points": [
                                                    {"time": 1, "price": 100.0},
                                                    {"time": 2, "price": 101.0},
                                                    {"time": 3, "price": 102.0},
                                                    {"time": 4, "price": 103.0},
                                                ],
                                            }
                                        ],
                                    },
                                },
                            }
                        ],
                    },
                },
            ],
        },
    )

    overlay_event = next(event for event in events if event.event_name.value == "OVERLAY_STATE_CHANGED")
    overlay = serialize_botlens_domain_event(overlay_event)["context"]["overlay_delta"]["ops"][0]["overlay"]

    assert overlay["payload"]["markers"] == [
        {"time": 3, "price": 102.0},
        {"time": 4, "price": 103.0},
    ]
    assert overlay["payload"]["polylines"][0]["points"] == [
        {"time": 1, "price": 100.0},
        {"time": 2, "price": 101.0},
        {"time": 3, "price": 102.0},
        {"time": 4, "price": 103.0},
    ]
    assert overlay["payload_summary"]["point_count"] == 4


def test_serialize_botlens_domain_event_slims_health_status_to_compact_runtime_truth() -> None:
    events = build_botlens_domain_events_from_fact_batch(
        bot_id="bot-1",
        run_id="run-1",
        payload={
            "known_at": "2026-02-01T00:00:10Z",
            "facts": [
                {
                    "fact_type": "runtime_state_observed",
                    "event": "telemetry_degraded",
                    "runtime": {
                        "status": "degraded",
                        "worker_count": 4,
                        "active_workers": 3,
                        "runtime_state": "degraded",
                        "progress_state": "churning",
                        "last_useful_progress_at": "2026-02-01T00:00:02Z",
                        "warnings": [
                            {
                                "warning_id": "indicator_overlay_payload_exceeded::typed_regime",
                                "warning_type": "indicator_overlay_payload_exceeded",
                                "severity": "warning",
                                "indicator_id": "typed_regime",
                                "message": "payload budget exceeded",
                                "count": 3,
                                "context": {"bytes": 262144, "raw": "drop-me"},
                            },
                            {
                                "warning_id": "indicator_time_budget_exceeded::typed_regime",
                                "warning_type": "indicator_time_budget_exceeded",
                                "severity": "error",
                                "indicator_id": "typed_regime",
                                "message": "time budget exceeded",
                                "count": 2,
                                "context": {"elapsed_ms": 1800},
                            },
                        ],
                        "degraded": {
                            "active": True,
                            "started_at": "2026-02-01T00:00:01Z",
                            "reason_code": "subscriber_gap",
                            "raw": {"drop": "me"},
                        },
                        "churn": {
                            "active": True,
                            "detected_at": "2026-02-01T00:00:02Z",
                            "reason_code": "no_progress",
                            "activity_without_progress_count": 8,
                            "verbose": "drop-me",
                        },
                        "pressure": {
                            "captured_at": "2026-02-01T00:00:03Z",
                            "trigger": "telemetry_degraded",
                            "top_pressure": {
                                "reason_code": "payload_bytes",
                                "value": 262144,
                                "unit": "bytes",
                                "debug": "drop-me",
                            },
                            "all_pressures": [{"reason_code": "payload_bytes"}],
                        },
                        "recent_transitions": [
                            {"from_state": "booting", "to_state": "live", "timestamp": "2026-02-01T00:00:00Z"},
                            {"from_state": "live", "to_state": "degraded", "timestamp": "2026-02-01T00:00:01Z"},
                            {"from_state": "degraded", "to_state": "live", "timestamp": "2026-02-01T00:00:02Z"},
                            {"from_state": "live", "to_state": "degraded", "timestamp": "2026-02-01T00:00:03Z"},
                            {"from_state": "degraded", "to_state": "live", "timestamp": "2026-02-01T00:00:04Z"},
                        ],
                        "terminal": {
                            "status": "terminating",
                            "source": "runtime",
                            "actor": "runtime_worker",
                            "reason": "worker fault",
                            "expected_workers": 4,
                            "reported_workers": 2,
                            "worker_terminal_statuses": {
                                "worker-1": "stopped",
                                "worker-2": "stopped",
                            },
                        },
                    },
                }
            ],
        },
    )

    health_event = next(event for event in events if event.event_name.value == "HEALTH_STATUS_REPORTED")
    payload = serialize_botlens_domain_event(health_event)["context"]

    assert payload["status"] == "degraded"
    assert payload["warning_count"] == 2
    assert payload["warning_types"] == [
        "indicator_overlay_payload_exceeded",
        "indicator_time_budget_exceeded",
    ]
    assert payload["highest_warning_severity"] == "error"
    assert payload["runtime_state"] == "degraded"
    assert payload["progress_state"] == "churning"
    assert payload["last_useful_progress_at"] == "2026-02-01T00:00:02Z"
    assert payload["warnings"][0]["warning_id"] == "indicator_overlay_payload_exceeded::typed_regime"
    assert "context" not in payload["warnings"][0]
    assert payload["pressure"] == {
        "trigger": "telemetry_degraded",
        "top_pressure": {
            "reason_code": "payload_bytes",
            "value": 262144.0,
            "unit": "bytes",
        },
    }
    assert len(payload["recent_transitions"]) == 4
    assert payload["terminal"] == {
        "status": "terminating",
        "source": "runtime",
        "actor": "runtime_worker",
        "reason": "worker fault",
        "expected_workers": 4,
        "reported_workers": 2,
        "worker_terminal_status_count": 2,
    }


def test_runtime_state_health_event_id_is_stable_when_only_warning_context_changes() -> None:
    base_warning = {
        "warning_id": "indicator_overlay_payload_exceeded::typed_regime",
        "warning_type": "indicator_overlay_payload_exceeded",
        "severity": "warning",
        "indicator_id": "typed_regime",
        "message": "payload budget exceeded",
    }

    first = build_botlens_domain_events_from_fact_batch(
        bot_id="bot-1",
        run_id="run-1",
        payload={
            "known_at": "2026-02-01T00:00:00Z",
            "facts": [
                {
                    "fact_type": "runtime_state_observed",
                    "runtime": {
                        "status": "running",
                        "warnings": [{**base_warning, "context": {"payload_bytes": 262144}}],
                    },
                }
            ],
        },
    )
    second = build_botlens_domain_events_from_fact_batch(
        bot_id="bot-1",
        run_id="run-1",
        payload={
            "known_at": "2026-02-01T00:00:05Z",
            "facts": [
                {
                    "fact_type": "runtime_state_observed",
                    "runtime": {
                        "status": "running",
                        "warnings": [{**base_warning, "context": {"payload_bytes": 131072, "sample": "changed"}}],
                    },
                }
            ],
        },
    )

    first_health = next(event for event in first if event.event_name.value == "HEALTH_STATUS_REPORTED")
    second_health = next(event for event in second if event.event_name.value == "HEALTH_STATUS_REPORTED")

    assert first_health.event_id == second_health.event_id


def test_serialize_botlens_domain_event_slims_series_stats_to_compact_summary() -> None:
    events = build_botlens_domain_events_from_fact_batch(
        bot_id="bot-1",
        run_id="run-1",
        payload={
            "known_at": "2026-02-01T00:00:00Z",
            "facts": [
                {
                    "fact_type": "series_state_observed",
                    "series_key": "instr-1|1m",
                    "instrument_id": "instr-1",
                    "symbol": "BTCUSD",
                    "timeframe": "1m",
                },
                {
                    "fact_type": "series_stats_updated",
                    "stats": {
                        "total_trades": 10,
                        "wins": 6,
                        "losses": 4,
                        "win_rate": 0.6,
                        "net_pnl": 123.45678,
                        "max_drawdown": 12.34567,
                        "quote_currency": "usd",
                        "per_day": {"2026-02-01": 4},
                        "equity_curve": [1, 2, 3],
                    },
                },
            ],
        },
    )

    stats_event = next(event for event in events if event.event_name.value == "SERIES_STATS_REPORTED")
    payload = serialize_botlens_domain_event(stats_event)["context"]["stats"]

    assert payload == {
        "losses": 4,
        "max_drawdown": 12.3457,
        "net_pnl": 123.4568,
        "quote_currency": "USD",
        "total_trades": 10,
        "win_rate": 0.6,
        "wins": 6,
    }


def test_serialize_botlens_domain_event_slims_candle_to_ohlcv_truth() -> None:
    events = build_botlens_domain_events_from_fact_batch(
        bot_id="bot-1",
        run_id="run-1",
        payload={
            "known_at": "2026-02-01T00:00:00Z",
            "facts": [
                {
                    "fact_type": "series_state_observed",
                    "series_key": "instr-1|1m",
                    "instrument_id": "instr-1",
                    "symbol": "BTCUSD",
                    "timeframe": "1m",
                },
                {
                    "fact_type": "candle_upserted",
                    "candle": {
                        "time": "2026-02-01T00:00:00Z",
                        "end": "2026-02-01T00:01:00Z",
                        "open": 1.0,
                        "high": 2.0,
                        "low": 0.5,
                        "close": 1.5,
                        "atr": 0.7,
                        "volume": 42.0,
                        "range": 1.5,
                    },
                },
            ],
        },
    )

    candle_event = next(event for event in events if event.event_name.value == "CANDLE_OBSERVED")
    payload = serialize_botlens_domain_event(candle_event)["context"]["candle"]

    assert payload == {
        "time": "2026-02-01T00:00:00Z",
        "open": 1.0,
        "high": 2.0,
        "low": 0.5,
        "close": 1.5,
        "volume": 42.0,
    }


def test_serialize_botlens_domain_event_preserves_explicit_health_warning_summary_without_warning_bodies() -> None:
    event = deserialize_botlens_domain_event(
        {
            "schema_version": 1,
            "event_id": "evt-health",
            "event_ts": "2026-02-01T00:00:00Z",
            "event_name": "HEALTH_STATUS_REPORTED",
            "root_id": "evt-health",
            "parent_id": None,
            "correlation_id": "corr-health",
            "context": {
                "run_id": "run-1",
                "bot_id": "bot-1",
                "status": "degraded",
                "warning_count": 3,
                "warning_types": ["indicator_overlay_payload_exceeded"],
                "highest_warning_severity": "error",
            },
        }
    )

    payload = serialize_botlens_domain_event(event)["context"]

    assert payload["warning_count"] == 3
    assert payload["warning_types"] == ["indicator_overlay_payload_exceeded"]
    assert payload["highest_warning_severity"] == "error"
    assert "warnings" not in payload
