from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

pytest.importorskip("pandas")

import pandas as pd

from engines.bot_runtime.core.domain import Candle, StrategySignal, isoformat
from engines.bot_runtime.deps import BotRuntimeDeps
from engines.bot_runtime.strategy.models import Strategy, StrategyInstrumentLink
from engines.bot_runtime.strategy.series_builder import SeriesBuilder, StrategySeries


def _candle_at(ts: datetime, value: float = 100.0) -> Candle:
    return Candle(
        time=ts,
        open=value,
        high=value + 1.0,
        low=value - 1.0,
        close=value,
        volume=10.0,
    )


def _builder_deps(
    *,
    fetch_ohlcv=None,
    fetch_ohlcv_by_instrument=None,
    get_instrument_record=None,
    resolve_instrument=None,
) -> BotRuntimeDeps:
    return BotRuntimeDeps(
        fetch_strategy=lambda _strategy_id: None,
        fetch_ohlcv=fetch_ohlcv or (lambda *args, **kwargs: None),
        fetch_ohlcv_by_instrument=fetch_ohlcv_by_instrument,
        get_instrument_record=get_instrument_record,
        resolve_instrument=resolve_instrument or (lambda _datasource, _exchange, _symbol: None),
        strategy_evaluate=lambda *args, **kwargs: {},
        strategy_run_preview=lambda *args, **kwargs: {},
        indicator_get_instance_meta=lambda *args, **kwargs: {},
        indicator_build_runtime_graph=lambda *args, **kwargs: ({}, []),
        indicator_build_runtime_instance=lambda *args, **kwargs: None,
        indicator_runtime_input_plan_for_instance=lambda *args, **kwargs: {},
        build_indicator_context=lambda *_args, **_kwargs: None,
        record_bot_runtime_event=lambda _payload: None,
        record_bot_runtime_events_batch=lambda _payloads: 0,
        record_bot_trade=lambda _payload: None,
        record_bot_trade_event=lambda _payload: None,
        record_bot_run_steps_batch=lambda _payloads: 0,
        update_bot_run_artifact=lambda _run_id, _payload: None,
        build_run_artifact_bundle=lambda _bot_id, _run_id, _config, _series: None,
    )


def _ohlcv_frame() -> pd.DataFrame:
    index = pd.DatetimeIndex([datetime(2026, 1, 1, tzinfo=timezone.utc)])
    return pd.DataFrame(
        {
            "open": [100.0],
            "high": [101.0],
            "low": [99.0],
            "close": [100.5],
            "volume": [10.0],
        },
        index=index,
    )


def test_atm_template_strips_execution_profile_fields():
    builder = SeriesBuilder(
        bot_id="bot-1",
        config={},
        run_type="backtest",
        deps=_builder_deps(),
    )
    strategy = Strategy(
        id="strategy-1",
        name="strategy",
        timeframe="1h",
        datasource="CCXT",
        exchange="COINBASE",
        atm_template_id=None,
        atm_template={
            "tick_size": 0.01,
            "tick_value": 0.01,
            "contract_size": 1.0,
            "maker_fee_rate": 0.0,
            "taker_fee_rate": 0.0,
            "quote_currency": "USD",
            "proxy_derivative_margin_rates": {"overnight": {"long_margin_rate": 0.25}},
            "proxy_derivative_instrument_fields": {"tick_size": 0.5, "contract_size": 0.1, "tick_value": 0.05},
            "_meta": {
                "tick_size_override": True,
                "tick_value_override": True,
                "contract_size_override": True,
                "maker_fee_rate_override": True,
                "taker_fee_rate_override": True,
                "quote_currency_override": True,
            },
        },
        risk_config={},
        indicator_links=[],
        instrument_links=[],
    )

    template = builder._build_atm_template_with_instrument(
        strategy,
        {
            "symbol": "ETH/USD",
            "instrument_type": "spot",
            "tick_size": 0.01,
            "tick_value": 0.01,
            "contract_size": 1.0,
            "metadata": {
                "instrument_fields": {
                    "proxy_derivative_instrument_fields": {
                        "tick_size": 0.5,
                        "tick_value": 0.05,
                        "contract_size": 0.1,
                    }
                }
            },
        },
    )

    assert "tick_size" not in template
    assert "tick_value" not in template
    assert "contract_size" not in template
    assert "maker_fee_rate" not in template
    assert "taker_fee_rate" not in template
    assert "quote_currency" not in template
    assert "proxy_derivative_margin_rates" not in template
    assert "proxy_derivative_instrument_fields" not in template
    assert "tick_size_override" not in template.get("_meta", {})
    assert "tick_value_override" not in template.get("_meta", {})
    assert "contract_size_override" not in template.get("_meta", {})
    assert "maker_fee_rate_override" not in template.get("_meta", {})
    assert "taker_fee_rate_override" not in template.get("_meta", {})
    assert "quote_currency_override" not in template.get("_meta", {})


def test_incremental_eval_emits_only_current_epoch_and_newer_than_cursor():
    builder = SeriesBuilder(
        bot_id="bot-1",
        config={"incremental_signal_lookback_bars": 10},
        run_type="backtest",
        deps=_builder_deps(),
    )
    now = datetime(2026, 1, 1, 12, 0, tzinfo=timezone.utc)
    series = StrategySeries(
        strategy_id="s1",
        name="S1",
        symbol="BTC/USDT",
        timeframe="1m",
        datasource="demo",
        exchange="demo",
        candles=[_candle_at(now)],
        instrument={"id": "inst-1"},
        window_start=isoformat(now - timedelta(hours=1)),
    )

    def _fake_evaluate(*args, **kwargs):
        _ = args, kwargs
        return {
            "decision_artifacts": [
                {
                    "decision_id": "d-1",
                    "rule_id": "rule-1",
                    "bar_epoch": int((now - timedelta(minutes=1)).timestamp()),
                    "evaluation_result": "matched_selected",
                    "emitted_intent": "enter_long",
                    "trigger": {"event_key": "breakout_long"},
                },
                {
                    "decision_id": "d-2",
                    "rule_id": "rule-1",
                    "bar_epoch": int(now.timestamp()),
                    "evaluation_result": "matched_selected",
                    "emitted_intent": "enter_long",
                    "trigger": {"event_key": "breakout_long"},
                },
            ],
            "overlays": [],
            "perf": {"candle_fetch_ms": 3.5, "preview_replay_ms": 2.0},
        }

    builder._evaluate_strategy = _fake_evaluate  # type: ignore[assignment]

    signals, overlays, metrics = builder.evaluate_incremental_for_bar(
        series=series,
        candle=_candle_at(now),
        visible_candles=[_candle_at(now)],
        last_evaluated_epoch=int((now - timedelta(minutes=1)).timestamp()),
    )

    assert len(signals) == 1
    only = list(signals)[0]
    assert only.epoch == int(now.timestamp())
    assert only.direction == "long"
    assert overlays == []
    assert metrics["epochs_evaluated_this_tick"] == 1.0
    assert metrics["signals_emitted_count"] == 1.0
    assert metrics["candle_fetch_ms"] == 3.5
    assert metrics["preview_replay_ms"] == 2.0


def test_incremental_eval_uses_bounded_lookback_window():
    builder = SeriesBuilder(
        bot_id="bot-1",
        config={"incremental_signal_lookback_bars": 5},
        run_type="backtest",
        deps=_builder_deps(),
    )
    now = datetime(2026, 1, 1, 12, 0, tzinfo=timezone.utc)
    series = StrategySeries(
        strategy_id="s1",
        name="S1",
        symbol="BTC/USDT",
        timeframe="1m",
        datasource="demo",
        exchange="demo",
        candles=[_candle_at(now)],
        instrument={"id": "inst-1"},
        window_start=isoformat(now - timedelta(hours=2)),
    )
    observed: dict[str, str] = {}

    def _fake_evaluate(*, start_iso, end_iso, timeframe, instrument_id, strategy, include_walk_forward_markers=False):
        observed["start_iso"] = start_iso
        observed["end_iso"] = end_iso
        _ = timeframe, instrument_id, strategy, include_walk_forward_markers
        return {"decision_artifacts": [], "overlays": [], "perf": {}}

    builder._evaluate_strategy = _fake_evaluate  # type: ignore[assignment]

    builder.evaluate_incremental_for_bar(
        series=series,
        candle=_candle_at(now),
        visible_candles=[_candle_at(now)],
        last_evaluated_epoch=0,
    )

    expected_start = now - timedelta(minutes=5)
    assert observed["start_iso"] == isoformat(expected_start)
    assert observed["end_iso"] == isoformat(now)


def test_fetch_ohlcv_data_prefers_canonical_instrument_fetch() -> None:
    observed: dict[str, str] = {}

    def _fetch_by_instrument(instrument_id: str, start: str, end: str, interval: str) -> pd.DataFrame:
        observed.update(
            {
                "instrument_id": instrument_id,
                "start": start,
                "end": end,
                "interval": interval,
            }
        )
        return _ohlcv_frame()

    def _fetch_by_symbol(*_args, **_kwargs):
        raise AssertionError("symbol/provider fetch should not be used when instrument_id is available")

    builder = SeriesBuilder(
        bot_id="bot-1",
        config={},
        run_type="backtest",
        deps=_builder_deps(
            fetch_ohlcv=_fetch_by_symbol,
            fetch_ohlcv_by_instrument=_fetch_by_instrument,
        ),
    )

    frame = builder._fetch_ohlcv_data(
        "BTC/USD",
        "2026-01-01T00:00:00Z",
        "2026-01-01T01:00:00Z",
        "1h",
        "COINBASE",
        "coinbase",
        "strategy-1",
        instrument_id="inst-btc-spot",
    )

    assert len(frame) == 1
    assert observed == {
        "instrument_id": "inst-btc-spot",
        "start": "2026-01-01T00:00:00Z",
        "end": "2026-01-01T01:00:00Z",
        "interval": "1h",
    }


def test_fetch_ohlcv_data_fails_on_provider_ingestion_failure() -> None:
    frame = _ohlcv_frame()
    frame.attrs["gap_classification"] = [
        {
            "classification": "ingestion_failure",
            "reason_code": "provider_fetch_exception",
            "evidence": "provider_api_exception",
            "start": "2026-01-01T01:00:00+00:00",
            "end": "2026-01-01T02:00:00+00:00",
            "provider_evidence": {
                "exception_type": "RuntimeError",
                "exception_message": "429 Too Many Requests",
            },
        }
    ]

    builder = SeriesBuilder(
        bot_id="bot-1",
        config={},
        run_type="backtest",
        deps=_builder_deps(fetch_ohlcv_by_instrument=lambda *_args, **_kwargs: frame),
    )

    with pytest.raises(RuntimeError, match="provider_fetch_exception"):
        builder._fetch_ohlcv_data(
            "BTC/USD",
            "2026-01-01T00:00:00Z",
            "2026-01-01T02:00:00Z",
            "1h",
            "CCXT",
            "COINBASE",
            "strategy-1",
            instrument_id="inst-btc-spot",
        )


def test_instrument_link_resolution_prefers_canonical_instrument_record() -> None:
    strategy = Strategy(
        id="strategy-1",
        name="Strategy 1",
        timeframe="1h",
        datasource="COINBASE",
        exchange="coinbase_direct",
        atm_template_id=None,
        atm_template={},
        risk_config={},
        indicator_links=[],
        instrument_links=[],
        rules={},
    )
    link = StrategyInstrumentLink(
        id="link-1",
        strategy_id="strategy-1",
        instrument_id="inst-btc-spot",
        instrument_snapshot={"symbol": "BTC/USD", "datasource": "stale", "exchange": "stale"},
    )

    builder = SeriesBuilder(
        bot_id="bot-1",
        config={},
        run_type="backtest",
        deps=_builder_deps(
            get_instrument_record=lambda _instrument_id: {
                "id": "inst-btc-spot",
                "symbol": "BTC/USD",
                "datasource": "COINBASE",
                "exchange": "coinbase",
                "instrument_type": "spot",
            },
            resolve_instrument=lambda *_args, **_kwargs: None,
        ),
    )

    instrument = builder._instrument_for_link(strategy, link)

    assert instrument is not None
    assert instrument["id"] == "inst-btc-spot"
    assert instrument["datasource"] == "COINBASE"
    assert instrument["exchange"] == "coinbase"
    assert instrument["instrument_type"] == "spot"


def test_build_signals_from_decision_artifacts_preserves_signal_time_without_shift() -> None:
    ts = datetime(2026, 1, 1, 12, tzinfo=timezone.utc)
    artifacts = [
        {
            "decision_id": "d-1",
            "rule_id": "rule-1",
            "strategy_hash": "hash-1",
            "bar_epoch": int(ts.timestamp()),
            "evaluation_result": "matched_selected",
            "emitted_intent": "enter_long",
            "trigger": {"event_key": "breakout_long"},
        }
    ]
    out = SeriesBuilder._build_signals_from_decision_artifacts(artifacts)
    assert len(out) == 1
    assert out[0].epoch == int(ts.timestamp())
    assert out[0].direction == "long"
    assert out[0].signal_id == StrategySignal.build_signal_id(decision_id="d-1")
    assert out[0].strategy_hash == "hash-1"
    assert out[0].decision_id == "d-1"
    assert out[0].rule_id == "rule-1"

def test_build_signals_from_decision_artifacts_ignores_non_selected_entries() -> None:
    ts = datetime(2026, 1, 1, 12, tzinfo=timezone.utc)
    artifacts = [
        {
            "decision_id": "d-1",
            "rule_id": "rule-1",
            "bar_epoch": int(ts.timestamp()),
            "evaluation_result": "matched_suppressed",
            "emitted_intent": "enter_long",
            "trigger": {"event_key": "breakout_long"},
        }
    ]
    out = SeriesBuilder._build_signals_from_decision_artifacts(artifacts)
    assert len(out) == 0
