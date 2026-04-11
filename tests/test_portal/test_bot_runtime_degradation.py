from collections import deque
from datetime import datetime, timezone
from types import SimpleNamespace

from engines.bot_runtime.core.domain import Candle
from engines.bot_runtime.core.runtime_events import RuntimeEventName
from engines.bot_runtime.deps import BotRuntimeDeps
from engines.bot_runtime.runtime.components.overlay_delta import count_overlay_points
from engines.bot_runtime.runtime.components.run_context import RunContext
from engines.bot_runtime.runtime.runtime import BotRuntime
from engines.indicator_engine.contracts import EngineFrame, IndicatorGuardWarning


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


def test_overlay_point_counter_is_a_pure_helper():
    assert count_overlay_points(
        [
            {"payload": {"markers": [{}], "boxes": [{}, {}]}},
            {"payload": {"polylines": [{}]}},
        ]
    ) == 4


def test_series_step_degraded_marks_runtime_degraded_and_emits_event(monkeypatch):
    runtime = BotRuntime("bot-1", {"wallet_config": {"balances": {"USDC": 100}}}, deps=_runtime_deps())
    runtime._run_context = RunContext(bot_id="bot-1")
    emitted: list[dict] = []
    monkeypatch.setattr(runtime, "_emit_runtime_event", lambda **kwargs: emitted.append(kwargs))
    series = SimpleNamespace(strategy_id="strategy-1", symbol="BTCUSDT", timeframe="1h")
    state = SimpleNamespace(series=series)

    runtime._log_runner_error("series_step_degraded", state, {"error": "boom"})

    assert runtime.state["status"] == "degraded"
    assert runtime.state["degradation"]["message"] == "boom"
    assert runtime.warnings()[0]["type"] == "series_degraded"
    assert emitted[0]["event_name"] == RuntimeEventName.SYMBOL_DEGRADED


def test_execute_loop_preserves_degraded_status_instead_of_completed(monkeypatch):
    runtime = BotRuntime("bot-1", {"wallet_config": {"balances": {"USDC": 100}}}, deps=_runtime_deps())
    runtime._prepared = True
    runtime._live_mode = False
    monkeypatch.setattr(runtime, "_set_phase", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(runtime, "_log_event", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(runtime, "_start_overlay_aggregator", lambda: None)
    monkeypatch.setattr(runtime, "_stop_overlay_aggregator", lambda: None)
    monkeypatch.setattr(runtime, "_record_step_trace", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(runtime, "_flush_persistence_buffer", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(runtime, "_flush_step_trace_buffer", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(runtime, "_push_update", lambda *_args, **_kwargs: {})
    monkeypatch.setattr(runtime, "_persist_runtime_state", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(runtime, "_persist_run_artifact", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(runtime, "_aggregate_stats", lambda: {"completed_trades": 0, "gross_pnl": 0.0, "net_pnl": 0.0, "fees_paid": 0.0})
    monkeypatch.setattr(runtime, "_max_drawdown_from_trades", lambda: 0.0)

    class Runner:
        def run(self) -> None:
            with runtime._lock:
                runtime.state["status"] = "degraded"

    monkeypatch.setattr(runtime, "_build_series_runner", lambda: Runner())

    runtime._execute_loop()

    assert runtime.state["status"] == "degraded"


def test_runtime_warning_store_aggregates_repeated_indicator_warnings():
    runtime = BotRuntime("bot-1", {"wallet_config": {"balances": {"USDC": 100}}}, deps=_runtime_deps())

    runtime._record_runtime_warning(
        {
            "warning_type": "indicator_time_budget_exceeded",
            "indicator_id": "typed_regime",
            "symbol_key": "instrument-btc|1m",
            "symbol": "BTC",
            "timeframe": "1m",
            "message": "typed_regime exceeded the indicator execution budget repeatedly.",
            "source": "indicator_guard",
        }
    )
    runtime._record_runtime_warning(
        {
            "warning_type": "indicator_time_budget_exceeded",
            "indicator_id": "typed_regime",
            "symbol_key": "instrument-btc|1m",
            "symbol": "BTC",
            "timeframe": "1m",
            "message": "typed_regime exceeded the indicator execution budget repeatedly.",
            "source": "indicator_guard",
        }
    )

    warnings = runtime.warnings()

    assert len(warnings) == 1
    assert warnings[0]["warning_id"] == "indicator_time_budget_exceeded::typed_regime::instrument-btc|1m::btc::1m::indicator_guard"
    assert warnings[0]["count"] == 2
    assert warnings[0]["first_seen_at"] is not None
    assert warnings[0]["last_seen_at"] is not None


def test_runtime_signal_artifact_helper_delegates_without_name_error():
    runtime = BotRuntime("bot-1", {"wallet_config": {"balances": {"USDC": 100}}}, deps=_runtime_deps())

    out = runtime._build_signals_from_decision_artifacts(
        [
            {
                "evaluation_result": "matched_selected",
                "bar_epoch": 1700000000,
                "emitted_intent": "enter_long",
                "decision_id": "decision-1",
                "rule_id": "rule-1",
                "trigger": {"event_key": "evt-1"},
            }
        ]
    )

    assert len(out) == 1


def test_next_signal_for_records_guard_warning_without_bar_time_name_error(monkeypatch):
    runtime = BotRuntime("bot-1", {"wallet_config": {"balances": {"USDC": 100}}}, deps=_runtime_deps())
    warnings_recorded: list[dict] = []
    monkeypatch.setattr(runtime, "_record_runtime_warning", lambda payload: warnings_recorded.append(dict(payload)))
    monkeypatch.setattr(runtime, "_series_overlay_entries", lambda _state: [])
    monkeypatch.setattr(
        "engines.bot_runtime.runtime.mixins.execution_loop.evaluate_strategy_bar",
        lambda **_kwargs: SimpleNamespace(artifacts=[], selected_artifact=None),
    )

    candle = Candle(
        time=datetime(2026, 4, 10, 0, 40, tzinfo=timezone.utc),
        open=100.0,
        high=101.0,
        low=99.0,
        close=100.5,
        volume=42.0,
    )
    indicator_engine = SimpleNamespace(
        order=("typed-regime",),
        step=lambda **_kwargs: EngineFrame(
            outputs={},
            overlays={},
            guard_warnings=(
                IndicatorGuardWarning(
                    warning_type="indicator_overlay_payload_exceeded",
                    severity="warning",
                    indicator_id="typed_regime",
                    manifest_type="regime",
                    version="v1",
                    title="overlay payload exceeded budget",
                    message="typed_regime emitted an oversized overlay payload.",
                    context={"overlay_payload_bytes": 2048},
                ),
            ),
        ),
    )
    series = SimpleNamespace(
        instrument={"id": "instrument-btc"},
        timeframe="1h",
        symbol="BTCUSD",
        overlays=[],
        meta={"compiled_strategy": object()},
    )
    state = SimpleNamespace(
        last_evaluated_epoch=0,
        last_consumed_epoch=0,
        indicator_engine=indicator_engine,
        indicator_outputs={},
        indicator_overlays={},
        overlay_runtime_metrics={},
        pending_signals=deque(),
        decision_evaluation_state=SimpleNamespace(),
        indicator_output_types={},
        decision_artifacts=[],
    )

    runtime._next_signal_for(state, series, candle, int(candle.time.timestamp()))

    assert len(warnings_recorded) == 1
    assert warnings_recorded[0]["warning_type"] == "indicator_overlay_payload_exceeded"
    assert warnings_recorded[0]["bar_time"] == "2026-04-10T00:40:00Z"
    assert warnings_recorded[0]["context"]["bar_time"] == "2026-04-10T00:40:00Z"


def test_aggregate_stats_reuses_cached_trade_summary_until_trade_revision_changes():
    runtime = BotRuntime("bot-1", {"wallet_config": {"balances": {"USDC": 100}}}, deps=_runtime_deps())

    class FakeRiskEngine:
        def __init__(self):
            self.trade_revision = 1
            self.stats_calls = 0
            self.serialise_calls = 0

        def stats(self):
            self.stats_calls += 1
            return {
                "total_trades": 2,
                "completed_trades": 2,
                "legs_closed": 2,
                "wins": 1,
                "losses": 1,
                "breakeven_trades": 0,
                "win_rate": 0.5,
                "long_trades": 1,
                "short_trades": 1,
                "gross_pnl": 4.0,
                "fees_paid": 1.0,
                "net_pnl": 3.0,
                "quote_currency": "USDC",
            }

        def serialise_trades(self):
            self.serialise_calls += 1
            return [
                {
                    "trade_id": "trade-2",
                    "closed_at": "2026-04-10T00:02:00+00:00",
                    "net_pnl": -1.0,
                },
                {
                    "trade_id": "trade-1",
                    "closed_at": "2026-04-10T00:01:00+00:00",
                    "net_pnl": 4.0,
                },
            ]

    engine = FakeRiskEngine()
    runtime._series = [
        SimpleNamespace(
            strategy_id="strategy-1",
            symbol="BTCUSDT",
            timeframe="1h",
            risk_engine=engine,
        )
    ]

    first = runtime._aggregate_stats()
    second = runtime._aggregate_stats()

    assert first == second
    assert first["avg_win"] == 4.0
    assert first["avg_loss"] == -1.0
    assert first["largest_win"] == 4.0
    assert first["largest_loss"] == -1.0
    assert first["max_drawdown"] == 1.0
    assert first["total_fees"] == 1.0
    assert engine.stats_calls == 1
    assert engine.serialise_calls == 1

    engine.trade_revision = 2
    refreshed = runtime._aggregate_stats()

    assert refreshed == first
    assert engine.stats_calls == 2
    assert engine.serialise_calls == 2
