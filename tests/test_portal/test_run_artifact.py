from datetime import datetime, timezone

from engines.bot_runtime.core.runtime_events import (
    ReasonCode,
    RuntimeEventName,
    build_correlation_id,
    new_runtime_event,
)
from portal.backend.service.bots.bot_runtime.runtime.run_context import RunContext
from portal.backend.service.bots.bot_runtime.runtime import BotRuntime


def test_run_artifact_payload_contains_runtime_event_stream_and_derived_views():
    runtime = BotRuntime("bot-1", {"wallet_config": {"balances": {"USDC": 100}}})
    run_context = RunContext(bot_id="bot-1")
    bar_ts = datetime(2026, 1, 1, tzinfo=timezone.utc)
    deposit = new_runtime_event(
        run_id=run_context.run_id,
        bot_id="bot-1",
        strategy_id="__runtime__",
        symbol=None,
        timeframe=None,
        bar_ts=None,
        event_name=RuntimeEventName.WALLET_INITIALIZED,
        correlation_id=build_correlation_id(
            run_id=run_context.run_id,
            symbol=None,
            timeframe=None,
            bar_ts=None,
        ),
        payload={"balances": {"USDC": 100.0}, "source": "run_start"},
    )
    signal = new_runtime_event(
        run_id=run_context.run_id,
        bot_id="bot-1",
        strategy_id="strategy-1",
        symbol="BTCUSDT",
        timeframe="1m",
        bar_ts=bar_ts,
        event_name=RuntimeEventName.SIGNAL_EMITTED,
        correlation_id=build_correlation_id(
            run_id=run_context.run_id,
            symbol="BTCUSDT",
            timeframe="1m",
            bar_ts=bar_ts,
        ),
        payload={"signal_type": "strategy_signal", "direction": "long", "signal_price": 100.0},
        reason_code=ReasonCode.SIGNAL_STRATEGY_SIGNAL,
    )
    run_context.runtime_events.extend([deposit, signal])
    run_context.runtime_event_stream = [event.serialize() for event in run_context.runtime_events]
    run_context.decision_trace = [runtime._decision_trace_entry(event) for event in run_context.runtime_events]
    runtime._run_context = run_context
    artifact = runtime._run_artifact_payload("completed")

    assert artifact["bot_id"] == "bot-1"
    assert artifact["status"] == "completed"
    assert artifact["wallet_start"]["balances"]["USDC"] == 100
    assert "wallet_end" in artifact
    assert artifact["runtime_event_stream"][0]["event_name"] == RuntimeEventName.WALLET_INITIALIZED.value
    assert artifact["decision_trace"][1]["event_subtype"] == "strategy_signal"
