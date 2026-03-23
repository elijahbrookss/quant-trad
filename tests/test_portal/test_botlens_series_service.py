from __future__ import annotations

import pytest

pytest.importorskip("sqlalchemy")

from portal.backend.service.bots import botlens_series_service as svc


def _projection(
    *,
    series_key: str = "instrument-btc|1m",
    symbol: str = "BTC",
    candle_times: list[int] | None = None,
) -> dict:
    instrument_id, timeframe = str(series_key).split("|", 1)
    candles = [
        {
            "time": time_value,
            "open": float(time_value),
            "high": float(time_value),
            "low": float(time_value),
            "close": float(time_value),
        }
        for time_value in (candle_times or [1])
    ]
    return {
        "series": [
            {
                "instrument_id": instrument_id,
                "symbol": symbol,
                "timeframe": timeframe,
                "candles": candles,
                "overlays": [],
                "stats": {"total_trades": len(candles)},
            }
        ],
        "trades": [],
        "logs": [],
        "decisions": [],
        "warnings": [],
        "runtime": {"status": "running"},
    }


def _delta(
    *,
    series_key: str = "instrument-btc|1m",
    symbol: str = "BTC",
    candle_time: int,
    trade_id: str = "trade-1",
) -> dict:
    instrument_id, timeframe = str(series_key).split("|", 1)
    return {
        "event": "bar_closed",
        "runtime": {"status": "running", "warnings": ["runtime warning"]},
        "logs": [{"message": "delta log"}],
        "decisions": [{"event": "decision"}],
        "series": [
            {
                "series_key": series_key,
                "instrument_id": instrument_id,
                "symbol": symbol,
                "timeframe": timeframe,
                "candle": {
                    "time": candle_time,
                    "open": float(candle_time),
                    "high": float(candle_time),
                    "low": float(candle_time),
                    "close": float(candle_time),
                },
                "overlay_delta": {
                    "ops": [
                        {
                            "op": "upsert",
                            "key": "overlay:regime",
                            "overlay": {
                                "type": "regime_overlay",
                                "payload": {"state": "risk_on"},
                            },
                        }
                    ]
                },
                "stats": {"total_trades": 1},
                "trades": [{"trade_id": trade_id, "symbol": symbol}],
            }
        ],
    }


def test_get_series_window_reads_latest_per_series_view_row(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(svc, "get_bot_run", lambda run_id: {"run_id": run_id, "bot_id": "bot-1"})
    monkeypatch.setattr(
        svc,
        "get_latest_bot_run_view_state",
        lambda **kwargs: {
            "seq": 8,
            "event_time": "2026-01-01T00:03:00Z",
            "payload": _projection(candle_times=[1, 2, 3]),
        },
    )

    result = svc.get_series_window(run_id="run-1", series_key="instrument-btc|1m", to="now", limit=2)

    assert result["seq"] == 8
    assert [row["time"] for row in result["window"]["candles"]] == [2, 3]
    assert result["window"]["selected_series"]["series_key"] == "instrument-btc|1m"
    assert result["window"]["runtime"]["status"] == "running"


def test_get_series_window_raises_when_requested_series_is_missing(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(svc, "get_bot_run", lambda run_id: {"run_id": run_id, "bot_id": "bot-1"})
    monkeypatch.setattr(svc, "get_latest_bot_run_view_state", lambda **kwargs: None)
    monkeypatch.setattr(
        svc,
        "list_bot_run_view_states",
        lambda **kwargs: [
            {
                "series_key": "instrument-eth|5m",
                "seq": 4,
                "payload": _projection(series_key="instrument-eth|5m", symbol="ETH", candle_times=[10, 11]),
            }
        ],
    )

    with pytest.raises(ValueError, match="series 'instrument-btc\\|1m' was not found for run_id=run-1"):
        svc.get_series_window(run_id="run-1", series_key="instrument-btc|1m", to="now", limit=5)


def test_list_series_keys_uses_latest_per_series_rows(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(svc, "get_bot_run", lambda run_id: {"run_id": run_id, "bot_id": "bot-1"})
    monkeypatch.setattr(
        svc,
        "list_bot_run_view_states",
        lambda **kwargs: [
            {
                "series_key": "instrument-btc|1m",
                "seq": 8,
                "payload": _projection(series_key="instrument-btc|1m", candle_times=[1, 2]),
            },
            {
                "series_key": "instrument-eth|5m",
                "seq": 4,
                "payload": _projection(series_key="instrument-eth|5m", symbol="ETH", candle_times=[3, 4]),
            },
        ],
    )

    result = svc.list_series_keys(run_id="run-1")

    assert result == {"run_id": "run-1", "series": ["instrument-btc|1m", "instrument-eth|5m"]}


def test_list_series_keys_ignores_legacy_merged_bot_rows(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(svc, "get_bot_run", lambda run_id: {"run_id": run_id, "bot_id": "bot-1"})
    monkeypatch.setattr(
        svc,
        "list_bot_run_view_states",
        lambda **kwargs: [
            {
                "series_key": "bot",
                "seq": 8,
                "payload": _projection(series_key="instrument-btc|1m", candle_times=[1, 2]),
            }
        ],
    )

    result = svc.list_series_keys(run_id="run-1")

    assert result == {"run_id": "run-1", "series": []}


def test_get_series_history_reconstructs_from_bootstrap_and_delta_events(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(svc, "get_bot_run", lambda run_id: {"run_id": run_id, "bot_id": "bot-1"})
    monkeypatch.setattr(
        svc,
        "get_latest_bot_run_view_state",
        lambda **kwargs: {
            "seq": 3,
            "known_at": "2026-01-01T00:03:00Z",
            "payload": _projection(candle_times=[1, 2, 3]),
        },
    )
    monkeypatch.setattr(
        svc,
        "list_bot_runtime_events",
        lambda **kwargs: [
            {
                "payload": {
                    "series_key": "instrument-btc|1m",
                    "projection": _projection(series_key="instrument-btc|1m", candle_times=[1, 2]),
                }
            },
            {
                "payload": {
                    "series_key": "instrument-btc|1m",
                    "series_seq": 3,
                    "runtime_delta": _delta(candle_time=3),
                }
            },
            {
                "payload": {
                    "series_key": "instrument-eth|5m",
                    "projection": _projection(series_key="instrument-eth|5m", symbol="ETH", candle_times=[20]),
                }
            },
        ],
    )

    result = svc.get_series_history(
        run_id="run-1",
        series_key="instrument-btc|1m",
        before_ts="1970-01-01T00:00:04Z",
        limit=10,
    )

    assert [row["time"] for row in result["history"]["candles"]] == [1, 2, 3]
    assert result["has_more"] is False
    assert result["next_before_ts"] == 1


def test_get_series_history_uses_latest_projection_when_delta_log_is_empty(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(svc, "get_bot_run", lambda run_id: {"run_id": run_id, "bot_id": "bot-1"})
    monkeypatch.setattr(
        svc,
        "get_latest_bot_run_view_state",
        lambda **kwargs: {
            "seq": 9,
            "known_at": "2026-01-01T00:09:00Z",
            "payload": _projection(candle_times=[7, 8, 9]),
        },
    )
    monkeypatch.setattr(svc, "list_bot_runtime_events", lambda **kwargs: [])

    result = svc.get_series_history(
        run_id="run-1",
        series_key="instrument-btc|1m",
        before_ts="1970-01-01T00:00:10Z",
        limit=5,
    )

    assert [row["time"] for row in result["history"]["candles"]] == [7, 8, 9]
