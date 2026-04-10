from __future__ import annotations

import pytest

pytest.importorskip("sqlalchemy")

from portal.backend.service.bots import botlens_symbol_service as svc
from portal.backend.service.bots.botlens_contract import RUN_SCOPE_KEY


def _detail_payload(*, symbol_key: str = "instrument-btc|1m", symbol: str = "BTC", candle_times: list[int] | None = None) -> dict:
    instrument_id, timeframe = str(symbol_key).split("|", 1)
    candles = [
        {
            "time": value,
            "open": float(value),
            "high": float(value),
            "low": float(value),
            "close": float(value),
        }
        for value in (candle_times or [1])
    ]
    return {
        "detail": {
            "symbol_key": symbol_key,
            "instrument_id": instrument_id,
            "symbol": symbol,
            "timeframe": timeframe,
            "candles": candles,
            "overlays": [],
            "recent_trades": [],
            "logs": [],
            "decisions": [],
            "stats": {"total_trades": len(candles)},
            "runtime": {"status": "running"},
        }
    }


def _summary_payload() -> dict:
    return {
        "summary": {
            "symbol_index": {
                "instrument-btc|1m": {
                    "symbol_key": "instrument-btc|1m",
                    "symbol": "BTC",
                    "timeframe": "1m",
                    "display_label": "BTC · 1m",
                },
                "instrument-eth|5m": {
                    "symbol_key": "instrument-eth|5m",
                    "symbol": "ETH",
                    "timeframe": "5m",
                    "display_label": "ETH · 5m",
                },
            }
        }
    }


def test_get_symbol_detail_reads_symbol_row(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(svc, "get_bot_run", lambda run_id: {"run_id": run_id, "bot_id": "bot-1"})
    monkeypatch.setattr(
        svc,
        "get_latest_bot_run_view_state",
        lambda **kwargs: {"payload": _detail_payload(candle_times=[1, 2, 3])},
    )

    result = svc.get_symbol_detail(run_id="run-1", symbol_key="instrument-btc|1m", limit=2)

    assert result["symbol_key"] == "instrument-btc|1m"
    assert [row["time"] for row in result["detail"]["candles"]] == [2, 3]
    assert result["detail"]["runtime"]["status"] == "running"


def test_get_symbol_detail_uses_summary_catalog_when_detail_row_missing(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(svc, "get_bot_run", lambda run_id: {"run_id": run_id, "bot_id": "bot-1"})

    def _latest(**kwargs):
        if kwargs.get("series_key") == RUN_SCOPE_KEY:
            return {"payload": _summary_payload()}
        return None

    monkeypatch.setattr(svc, "get_latest_bot_run_view_state", _latest)

    result = svc.get_symbol_detail(run_id="run-1", symbol_key="instrument-btc|1m", limit=5)

    assert result["symbol_key"] == "instrument-btc|1m"
    assert result["detail"]["candles"] == []


def test_get_symbol_history_reconstructs_from_runtime_events(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(svc, "get_bot_run", lambda run_id: {"run_id": run_id, "bot_id": "bot-1"})
    monkeypatch.setattr(
        svc,
        "get_latest_bot_run_view_state",
        lambda **kwargs: {"payload": _detail_payload(candle_times=[1, 2, 3])},
    )
    monkeypatch.setattr(
        svc,
        "list_bot_runtime_events",
        lambda **kwargs: [
            {
                "payload": {
                    "series_key": "instrument-btc|1m",
                    "facts": [
                        {"fact_type": "candle_upserted", "candle": {"time": 1, "open": 1, "high": 1, "low": 1, "close": 1}},
                        {"fact_type": "candle_upserted", "candle": {"time": 2, "open": 2, "high": 2, "low": 2, "close": 2}},
                    ],
                }
            },
            {
                "payload": {
                    "series_key": "instrument-btc|1m",
                    "facts": [
                        {"fact_type": "candle_upserted", "candle": {"time": 3, "open": 3, "high": 3, "low": 3, "close": 3}},
                    ],
                }
            },
        ],
    )

    result = svc.get_symbol_history(
        run_id="run-1",
        symbol_key="instrument-btc|1m",
        before_ts="1970-01-01T00:00:04Z",
        limit=10,
    )

    assert [row["time"] for row in result["candles"]] == [1, 2, 3]
    assert result["has_more"] is False
    assert result["next_before_ts"] == 1


def test_list_run_symbols_reads_run_summary_row(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(svc, "get_bot_run", lambda run_id: {"run_id": run_id, "bot_id": "bot-1"})
    monkeypatch.setattr(
        svc,
        "get_latest_bot_run_view_state",
        lambda **kwargs: {"payload": _summary_payload()},
    )

    result = svc.list_run_symbols(run_id="run-1")

    assert result == {
        "schema_version": 4,
        "run_id": "run-1",
        "symbols": [
            {
                "symbol_key": "instrument-btc|1m",
                "symbol": "BTC",
                "timeframe": "1m",
                "display_label": "BTC · 1m",
            },
            {
                "symbol_key": "instrument-eth|5m",
                "symbol": "ETH",
                "timeframe": "5m",
                "display_label": "ETH · 5m",
            },
        ],
    }
