from datetime import datetime, timezone
from types import SimpleNamespace

from engines.bot_runtime.core.runtime_events import (
    ReasonCode,
    RuntimeEventName,
    RuntimeBar,
    SignalEmittedContext,
    WalletInitializedContext,
    build_correlation_id,
    new_runtime_event,
)
from engines.bot_runtime.deps import BotRuntimeDeps
from engines.bot_runtime.runtime.runtime import BotRuntime
from engines.bot_runtime.runtime.components.run_context import RunContext


class _ProxySeqCounter:
    def __init__(self, value: int = 0) -> None:
        self._value = int(value)

    def get(self) -> int:
        return int(self._value)

    def set(self, value: int) -> None:
        self._value = int(value)


class _ProxyLock:
    def acquire(self) -> None:
        return None

    def release(self) -> None:
        return None


def _runtime_deps() -> BotRuntimeDeps:
    def _no_op(*args, **kwargs):
        return None

    return BotRuntimeDeps(
        fetch_strategy=lambda _strategy_id: None,
        fetch_ohlcv=lambda *args, **kwargs: None,
        resolve_instrument=lambda _datasource, _exchange, _symbol: None,
        strategy_evaluate=lambda *args, **kwargs: {},
        strategy_run_preview=lambda *args, **kwargs: {},
        indicator_get_instance_meta=lambda *args, **kwargs: {},
        indicator_build_runtime_graph=lambda *args, **kwargs: ({}, []),
        indicator_build_runtime_instance=lambda *args, **kwargs: None,
        indicator_runtime_input_plan_for_instance=lambda *args, **kwargs: {},
        build_indicator_context=lambda bot_id, _overlay_cache: SimpleNamespace(
            cache_owner="test",
            cache_scope_id=bot_id,
        ),
        record_bot_runtime_event=lambda _payload: None,
        record_bot_runtime_events_batch=lambda _payloads: 0,
        record_bot_trade=lambda _payload: None,
        record_bot_trade_event=lambda _payload: None,
        record_bot_run_steps_batch=lambda _payloads: 0,
        update_bot_run_artifact=lambda _run_id, _payload: None,
        build_run_artifact_bundle=lambda _bot_id, _run_id, _config, _series: None,
    )


def test_emit_runtime_event_keeps_runtime_events_in_memory_only() -> None:
    persisted_rows = []

    def _capture_runtime_row(payload):
        persisted_rows.append(dict(payload))
        return None

    deps = _runtime_deps()
    deps = BotRuntimeDeps(
        **{
            **deps.__dict__,
            "record_bot_runtime_event": _capture_runtime_row,
        }
    )
    runtime = BotRuntime("bot-1", {"wallet_config": {"balances": {"USDC": 100}}}, deps=deps)
    runtime._run_context = RunContext(bot_id="bot-1")
    bar_ts = datetime(2026, 1, 1, tzinfo=timezone.utc)

    event = runtime._emit_runtime_event(
        event_name=RuntimeEventName.SIGNAL_EMITTED,
        correlation_id=build_correlation_id(
            run_id=runtime._run_context.run_id,
            symbol="BTCUSDT",
            timeframe="1m",
            bar_ts=bar_ts,
        ),
        context=SignalEmittedContext(
            run_id=runtime._run_context.run_id,
            bot_id="bot-1",
            strategy_id="strategy-1",
            symbol="BTCUSDT",
            timeframe="1m",
            bar_ts=bar_ts,
            signal_type="strategy_signal",
            direction="long",
            signal_price=100.0,
            signal_id="signal-1",
            bar=RuntimeBar(
                time=bar_ts,
                open=100.0,
                high=100.0,
                low=100.0,
                close=100.0,
            ),
            reason_code=ReasonCode.SIGNAL_STRATEGY_SIGNAL,
        ),
    )

    assert persisted_rows == []
    assert runtime._run_context.runtime_events == [event]
    assert runtime._run_context.runtime_event_stream[0]["event_id"] == event.event_id


def test_commit_botlens_fact_payload_allocates_seq_before_run_context_exists() -> None:
    appended_batches = []

    def _append_batch(**kwargs):
        appended_batches.append(dict(kwargs))
        return {"inserted_rows": 1}

    deps = _runtime_deps()
    deps = BotRuntimeDeps(
        **{
            **deps.__dict__,
            "append_botlens_canonical_fact_batch": _append_batch,
        }
    )
    runtime = BotRuntime(
        "bot-1",
        {
            "run_id": "run-1",
            "worker_id": "worker-1",
            "shared_wallet_proxy": {
                "runtime_event_seq": _ProxySeqCounter(0),
                "lock": _ProxyLock(),
            },
        },
        deps=deps,
    )

    outcome = runtime.commit_botlens_fact_payload(
        {
            "series_key": "instrument-btc|1m",
            "known_at": "2026-04-20T05:20:00Z",
            "facts": [
                {
                    "fact_type": "candle_upserted",
                    "series_key": "instrument-btc|1m",
                    "candle": {
                        "time": "2026-04-20T05:20:00Z",
                        "open": 1.0,
                        "high": 1.5,
                        "low": 0.9,
                        "close": 1.1,
                    },
                }
            ],
        },
        batch_kind="botlens_runtime_bootstrap_facts",
        dispatch=False,
    )

    assert outcome is not None
    assert outcome.batch.seq == 1
    assert runtime._start_context is not None
    assert runtime._start_context.run_id == "run-1"
    assert appended_batches == [
        {
            "bot_id": "bot-1",
            "run_id": "run-1",
            "seq": 1,
            "batch_kind": "botlens_runtime_bootstrap_facts",
            "payload": {
                "series_key": "instrument-btc|1m",
                "known_at": "2026-04-20T05:20:00Z",
                "facts": [
                    {
                        "fact_type": "candle_upserted",
                        "series_key": "instrument-btc|1m",
                        "candle": {
                            "time": "2026-04-20T05:20:00Z",
                            "open": 1.0,
                            "high": 1.5,
                            "low": 0.9,
                            "close": 1.1,
                        },
                    }
                ],
                "run_seq": 1,
                "seq": 1,
            },
            "context": {
                "worker_id": "worker-1",
                "source_emitter": "bot_runtime",
                "source_reason": "producer",
            },
        }
    ]


def test_build_run_context_reuses_start_context_run_id() -> None:
    runtime = BotRuntime(
        "bot-1",
        {
            "wallet_config": {"balances": {"USDC": 100}},
            "shared_wallet_proxy": {
                "runtime_event_seq": _ProxySeqCounter(0),
                "runtime_events": [],
                "reservations": {},
                "lock": _ProxyLock(),
            },
        },
        deps=_runtime_deps(),
    )

    start_context = runtime._ensure_start_context()
    run_context = runtime._build_run_context()

    assert run_context.run_id == start_context.run_id


def test_reset_regenerates_start_context_for_unconfigured_runtime() -> None:
    runtime = BotRuntime("bot-1", {"wallet_config": {"balances": {"USDC": 100}}}, deps=_runtime_deps())

    first_run_id = runtime._ensure_start_context().run_id
    runtime.reset()
    second_run_id = runtime._ensure_start_context().run_id

    assert second_run_id != first_run_id


def test_run_artifact_payload_contains_runtime_event_stream_and_derived_views(monkeypatch):
    runtime = BotRuntime("bot-1", {"wallet_config": {"balances": {"USDC": 100}}}, deps=_runtime_deps())
    run_context = RunContext(bot_id="bot-1")
    bar_ts = datetime(2026, 1, 1, tzinfo=timezone.utc)
    deposit = new_runtime_event(
        event_name=RuntimeEventName.WALLET_INITIALIZED,
        correlation_id=build_correlation_id(
            run_id=run_context.run_id,
            symbol=None,
            timeframe=None,
            bar_ts=None,
        ),
        context=WalletInitializedContext(
            run_id=run_context.run_id,
            bot_id="bot-1",
            strategy_id="__runtime__",
            symbol=None,
            timeframe=None,
            bar_ts=None,
            balances={"USDC": 100.0},
            source="run_start",
        ),
    )
    signal = new_runtime_event(
        event_name=RuntimeEventName.SIGNAL_EMITTED,
        correlation_id=build_correlation_id(
            run_id=run_context.run_id,
            symbol="BTCUSDT",
            timeframe="1m",
            bar_ts=bar_ts,
        ),
        context=SignalEmittedContext(
            run_id=run_context.run_id,
            bot_id="bot-1",
            strategy_id="strategy-1",
            symbol="BTCUSDT",
            timeframe="1m",
            bar_ts=bar_ts,
            signal_type="strategy_signal",
            direction="long",
            signal_price=100.0,
            signal_id="signal-1",
            bar=RuntimeBar(
                time=bar_ts,
                open=100.0,
                high=100.0,
                low=100.0,
                close=100.0,
            ),
            reason_code=ReasonCode.SIGNAL_STRATEGY_SIGNAL,
        ),
    )
    run_context.runtime_events.extend([deposit, signal])
    run_context.runtime_event_stream = [event.serialize() for event in run_context.runtime_events]
    runtime._run_context = run_context
    artifact = runtime._run_artifact_payload("completed")

    assert artifact["bot_id"] == "bot-1"
    assert artifact["status"] == "completed"
    assert artifact["wallet_start"]["balances"]["USDC"] == 100
    assert "wallet_end" in artifact
    assert artifact["runtime_event_stream"][0]["event_name"] == RuntimeEventName.WALLET_INITIALIZED.value
    assert artifact["decision_trace"][0]["event_subtype"] == "strategy_signal"
    assert artifact["decision_artifacts"] == []
    assert artifact["rejection_artifacts"] == []
