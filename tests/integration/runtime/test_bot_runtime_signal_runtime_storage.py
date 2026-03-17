from datetime import datetime, timedelta, timezone
from typing import Any, Mapping, MutableMapping

from engines.bot_runtime.core.domain import Candle
from engines.bot_runtime.runtime.core import SeriesExecutionState
from engines.bot_runtime.runtime.mixins.execution_loop import RuntimeExecutionLoopMixin
from engines.indicator_engine.contracts import IndicatorStateDelta, IndicatorStateSnapshot
from engines.bot_runtime.strategy.series_builder_parts.models import StrategySeries


class _SeriesBuilderStub:
    @staticmethod
    def _evaluate_rule_payload(rule_payload: Mapping[str, Any], indicator_payloads: Mapping[str, Any]):
        return None


class _RuntimeStub(RuntimeExecutionLoopMixin):
    def __init__(self) -> None:
        self.bot_id = "bot-1"
        self._series_builder = _SeriesBuilderStub()

    def _ensure_series_builder(self) -> _SeriesBuilderStub:
        return self._series_builder

    @staticmethod
    def _series_overlay_entries(state: SeriesExecutionState):
        return []

    @staticmethod
    def _overlay_change_metrics(previous_overlays, overlays):
        return 0.0, 0.0

    @staticmethod
    def _count_overlay_points(overlays):
        return 0


class _EngineStub:
    def apply_bar(self, state: MutableMapping[str, Any], candle: Candle) -> IndicatorStateDelta:
        revision = int(state.get("revision", 0)) + 1
        state["revision"] = revision
        state["known_at"] = candle.time
        return IndicatorStateDelta(changed=True, revision=revision, known_at=candle.time)

    def snapshot(self, state: Mapping[str, Any]) -> IndicatorStateSnapshot:
        known_at = state.get("known_at") or datetime(2024, 1, 1, tzinfo=timezone.utc)
        return IndicatorStateSnapshot(
            revision=int(state.get("revision", 0)),
            known_at=known_at,
            formed_at=known_at,
            source_timeframe="1m",
            payload={"_runtime_scope": "bot_runtime", "signals": []},
        )


class _PluginStub:
    signal_emitter = object()
    overlay_projector = None


def test_next_signal_for_persists_runtime_state_storage_across_bars(monkeypatch):
    observed_payloads = []
    storage_ids = []

    def _fake_emit_manifest_signals(*, manifest, snapshot_payload, candle, previous_candle):
        storage = snapshot_payload.get("_runtime_state_storage")
        assert isinstance(storage, dict)
        storage_ids.append(id(storage))
        storage["bars_seen"] = int(storage.get("bars_seen", 0)) + 1
        observed_payloads.append(
            {
                "indicator_id": snapshot_payload.get("_indicator_id"),
                "symbol": snapshot_payload.get("symbol"),
                "chart_timeframe": snapshot_payload.get("chart_timeframe"),
                "source_timeframe": snapshot_payload.get("source_timeframe"),
                "bars_seen": storage["bars_seen"],
            }
        )
        return {"signals": []}

    monkeypatch.setattr(
        "engines.bot_runtime.runtime.mixins.execution_loop.emit_manifest_signals",
        _fake_emit_manifest_signals,
    )

    start = datetime(2024, 1, 1, tzinfo=timezone.utc)
    candles = [
        Candle(time=start, open=100.0, high=101.0, low=99.0, close=100.5, volume=1.0),
        Candle(time=start + timedelta(minutes=1), open=100.5, high=102.0, low=100.0, close=101.5, volume=1.2),
    ]
    series = StrategySeries(
        strategy_id="strategy-1",
        name="Strategy",
        symbol="BTC-USD",
        timeframe="1m",
        datasource=None,
        exchange=None,
        candles=candles,
        meta={"rules": {}},
    )
    state = SeriesExecutionState(series=series, total_bars=len(candles))
    state.indicator_state_runtime["ind-1"] = {
        "indicator_type": "market_profile",
        "engine": _EngineStub(),
        "engine_state": {},
        "plugin": _PluginStub(),
        "indicator_meta": {},
    }

    runtime = _RuntimeStub()

    _, _, _, _, next_last_evaluated, next_last_consumed = runtime._next_signal_for(
        state,
        series,
        candles[0],
        int(candles[0].time.timestamp()),
    )
    state.last_evaluated_epoch = next_last_evaluated
    state.last_consumed_epoch = next_last_consumed
    state.bar_index = 1

    runtime._next_signal_for(
        state,
        series,
        candles[1],
        int(candles[1].time.timestamp()),
    )

    indicator_runtime = state.indicator_state_runtime["ind-1"]
    storage = indicator_runtime.get("signal_runtime_storage")
    assert isinstance(storage, dict)
    assert storage.get("bars_seen") == 2
    assert len(set(storage_ids)) == 1
    assert observed_payloads[0]["indicator_id"] == "ind-1"
    assert observed_payloads[0]["symbol"] == "BTC-USD"
    assert observed_payloads[0]["chart_timeframe"] == "1m"
    assert observed_payloads[1]["bars_seen"] == 2
