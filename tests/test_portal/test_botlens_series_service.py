from __future__ import annotations

import asyncio
from dataclasses import replace
from types import SimpleNamespace
from typing import Any

import pytest

from portal.backend.service.bots import botlens_chart_service as chart_svc
from portal.backend.service.bots import botlens_symbol_service as svc
from portal.backend.service.bots.botlens_state import SymbolReadinessState


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


def _run_summary_state() -> SimpleNamespace:
    entries = _summary_payload()["summary"]["symbol_index"]
    return SimpleNamespace(
        seq=11,
        symbol_catalog=SimpleNamespace(entries=entries),
        lifecycle=SimpleNamespace(live=True),
        health=SimpleNamespace(to_dict=lambda: {"status": "running"}),
        readiness=SimpleNamespace(catalog_discovered=True, run_live=True),
    )


class _FakeTelemetryHub:
    def __init__(self, *, symbol_snapshot=None, run_snapshot=None, cursor=None):
        self._symbol_snapshot = symbol_snapshot
        self._run_snapshot = run_snapshot
        self._cursor = cursor or {"base_seq": 0, "stream_session_id": None}

    def get_symbol_snapshot(self, **kwargs):
        return self._symbol_snapshot

    def get_run_snapshot(self, **kwargs):
        return self._run_snapshot

    async def ensure_run_snapshot(self, **kwargs):
        return self._run_snapshot

    async def ensure_symbol_snapshot(self, **kwargs):
        return self._symbol_snapshot

    async def current_cursor(self, **kwargs):
        return dict(self._cursor)

    async def current_symbol_cursor(self, **kwargs):
        return {
            **dict(self._cursor),
            "run_scope_seq": int(self._cursor.get("run_scope_seq") or 0),
            "symbol_scope_seq": int(self._cursor.get("symbol_scope_seq") or 0),
        }


def test_load_symbol_detail_state_trims_projection_candles(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(svc, "_telemetry_hub", lambda: _FakeTelemetryHub())
    monkeypatch.setattr(svc, "get_bot_run", lambda run_id: {"run_id": run_id, "bot_id": "bot-1"})
    monkeypatch.setattr(
        svc,
        "rebuild_run_projection_snapshot",
        lambda **kwargs: _run_summary_state(),
    )
    monkeypatch.setattr(
        svc,
        "rebuild_symbol_projection_snapshot",
        lambda **kwargs: replace(
            svc.empty_symbol_projection_snapshot(kwargs["symbol_key"]),
            candles=svc.SymbolCandlesState(
                candles=[
                    {"time": 1, "open": 1, "high": 1, "low": 1, "close": 1},
                    {"time": 2, "open": 2, "high": 2, "low": 2, "close": 2},
                    {"time": 3, "open": 3, "high": 3, "low": 3, "close": 3},
                ]
            ),
        ),
    )

    result = asyncio.run(svc.load_symbol_detail_state(run_id="run-1", symbol_key="instrument-btc|1m", limit=2))

    assert [row["time"] for row in result.candles.candles] == [2, 3]


def test_get_symbol_detail_uses_http_detail_transport_contract(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(svc, "_telemetry_hub", lambda: _FakeTelemetryHub())
    monkeypatch.setattr(svc, "get_bot_run", lambda run_id: {"run_id": run_id, "bot_id": "bot-1"})
    monkeypatch.setattr(
        svc,
        "load_symbol_detail_state",
        lambda **kwargs: asyncio.sleep(
            0,
            result=replace(
                svc.empty_symbol_projection_snapshot(kwargs["symbol_key"]),
                seq=7,
            ),
        ),
    )
    monkeypatch.setattr(svc, "rebuild_run_projection_snapshot", lambda **kwargs: _run_summary_state())
    monkeypatch.setattr(
        svc,
        "symbol_detail_response_contract",
        lambda **kwargs: {
            "owner": "http-detail",
            "detail_state": kwargs["symbol_state"],
        },
    )

    result = asyncio.run(svc.get_symbol_detail(run_id="run-1", symbol_key="instrument-btc|1m", limit=2))

    assert result["owner"] == "http-detail"
    assert result["detail_state"].symbol_key == "instrument-btc|1m"


def test_get_selected_symbol_bootstrap_reads_projected_snapshots_without_replay(monkeypatch: pytest.MonkeyPatch) -> None:
    projected_symbol = replace(
        svc.empty_symbol_projection_snapshot("instrument-btc|1m"),
        seq=17,
        readiness=SymbolReadinessState(snapshot_ready=True, symbol_live=True),
    )
    monkeypatch.setattr(
        svc,
        "_telemetry_hub",
        lambda: _FakeTelemetryHub(
            symbol_snapshot=projected_symbol,
            run_snapshot=SimpleNamespace(
                seq=11,
                symbol_catalog=SimpleNamespace(
                    entries={
                        "instrument-btc|1m": {
                            "symbol_key": "instrument-btc|1m",
                            "symbol": "BTC",
                            "timeframe": "1m",
                            "display_label": "BTC · 1m",
                        }
                    }
                ),
                lifecycle=SimpleNamespace(live=True),
                health=SimpleNamespace(to_dict=lambda: {"status": "running"}),
                readiness=SimpleNamespace(catalog_discovered=True, run_live=True),
            ),
            cursor={
                "base_seq": 23,
                "stream_session_id": "stream-1",
            }
        ),
    )
    monkeypatch.setattr(svc, "get_bot_run", lambda run_id: {"run_id": run_id, "bot_id": "bot-1"})
    monkeypatch.setattr(svc, "rebuild_run_projection_snapshot", lambda **kwargs: pytest.fail("run replay is not allowed"))
    monkeypatch.setattr(svc, "rebuild_symbol_projection_snapshot", lambda **kwargs: pytest.fail("symbol replay is not allowed"))
    monkeypatch.setattr(
        svc,
        "selected_symbol_snapshot_contract",
        lambda **kwargs: {
            "owner": "snapshot",
            "symbol_state": kwargs["symbol_state"],
            "run_bootstrap_seq": kwargs["run_bootstrap_seq"],
            "base_seq": kwargs["base_seq"],
            "stream_session_id": kwargs["stream_session_id"],
            "run_live": kwargs["run_live"],
            "transport_eligible": kwargs["transport_eligible"],
        },
    )

    result = asyncio.run(svc.get_selected_symbol_bootstrap(run_id="run-1", symbol_key="instrument-btc|1m", limit=2))

    assert result["owner"] == "snapshot"
    assert result["symbol_state"].symbol_key == "instrument-btc|1m"
    assert result["run_bootstrap_seq"] == 11
    assert result["base_seq"] == 23
    assert result["stream_session_id"] == "stream-1"
    assert result["run_live"] is True
    assert result["transport_eligible"] is True


def test_get_selected_symbol_bootstrap_returns_unavailable_when_projected_symbol_state_is_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        svc,
        "_telemetry_hub",
        lambda: _FakeTelemetryHub(
            symbol_snapshot=svc.empty_symbol_projection_snapshot("instrument-btc|1m"),
            run_snapshot=SimpleNamespace(
                seq=11,
                symbol_catalog=SimpleNamespace(
                    entries={
                        "instrument-btc|1m": {
                            "symbol_key": "instrument-btc|1m",
                            "symbol": "BTC",
                            "timeframe": "1m",
                            "display_label": "BTC · 1m",
                            "status": "running",
                        }
                    }
                ),
                lifecycle=SimpleNamespace(live=True),
                health=SimpleNamespace(to_dict=lambda: {"status": "running"}),
                readiness=SimpleNamespace(catalog_discovered=True, run_live=True),
            ),
            cursor={
                "base_seq": 23,
                "stream_session_id": "stream-1",
            },
        ),
    )
    monkeypatch.setattr(svc, "get_bot_run", lambda run_id: {"run_id": run_id, "bot_id": "bot-1"})
    monkeypatch.setattr(svc, "rebuild_run_projection_snapshot", lambda **kwargs: pytest.fail("run replay is not allowed"))
    monkeypatch.setattr(svc, "rebuild_symbol_projection_snapshot", lambda **kwargs: pytest.fail("symbol replay is not allowed"))

    result = asyncio.run(svc.get_selected_symbol_bootstrap(run_id="run-1", symbol_key="instrument-btc|1m", limit=2))

    assert result["contract"] == "botlens_selected_symbol_snapshot"
    assert result["state"] == "unavailable"
    assert result["contract_state"] == "snapshot_unavailable"
    assert result["unavailable_reason"] == "symbol_snapshot_unavailable"
    assert result["selection"]["selected_symbol_key"] == "instrument-btc|1m"
    assert result["readiness"] == {
        "catalog_discovered": True,
        "snapshot_ready": False,
        "symbol_live": False,
        "run_live": True,
    }
    assert result["selected_symbol"] is None


def test_get_symbol_detail_uses_summary_catalog_when_detail_row_missing(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(svc, "_telemetry_hub", lambda: _FakeTelemetryHub())
    monkeypatch.setattr(svc, "get_bot_run", lambda run_id: {"run_id": run_id, "bot_id": "bot-1"})
    monkeypatch.setattr(svc, "rebuild_run_projection_snapshot", lambda **kwargs: _run_summary_state())
    monkeypatch.setattr(svc, "rebuild_symbol_projection_snapshot", lambda **kwargs: None)

    result = asyncio.run(svc.load_symbol_detail_state(run_id="run-1", symbol_key="instrument-btc|1m", limit=5))

    assert result.symbol_key == "instrument-btc|1m"
    assert list(result.candles.candles) == []


def test_get_symbol_chart_history_reconstructs_from_domain_truth_without_projection_state(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(chart_svc, "get_bot_run", lambda run_id: {"run_id": run_id, "bot_id": "bot-1"})
    monkeypatch.setattr(
        chart_svc,
        "iter_all_run_domain_truth",
        lambda **kwargs: iter(
            [
                type(
                    "Event",
                    (),
                    {
                        "context": {
                            "candle": {"time": "1970-01-01T00:00:01Z", "open": 1, "high": 1, "low": 1, "close": 1},
                        }
                    },
                )(),
                type(
                    "Event",
                    (),
                    {
                        "context": {
                            "candle": {"time": "1970-01-01T00:00:02Z", "open": 2, "high": 2, "low": 2, "close": 2},
                        }
                    },
                )(),
                type(
                    "Event",
                    (),
                    {
                        "context": {
                            "candle": {"time": "1970-01-01T00:00:03Z", "open": 3, "high": 3, "low": 3, "close": 3},
                        }
                    },
                )(),
            ]
        ),
    )

    result = chart_svc.get_symbol_chart_history(
        run_id="run-1",
        symbol_key="instrument-btc|1m",
        start_time="1970-01-01T00:00:00Z",
        end_time="1970-01-01T00:00:04Z",
        limit=10,
    )

    assert [row["time"] for row in result["candles"]] == [1, 2, 3]
    assert result["contract"] == "botlens_chart_history"
    assert result["range"]["has_more_before"] is False
    assert result["range"]["has_more_after"] is False


def test_get_symbol_chart_history_forwards_typed_bar_time_window(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, Any] = {}
    monkeypatch.setattr(chart_svc, "get_bot_run", lambda run_id: {"run_id": run_id, "bot_id": "bot-1"})

    def _iter(**kwargs):
        captured.update(kwargs)
        return iter([])

    monkeypatch.setattr(chart_svc, "iter_all_run_domain_truth", _iter)

    result = chart_svc.get_symbol_chart_history(
        run_id="run-1",
        symbol_key="instrument-btc|1m",
        start_time="1970-01-01T00:00:00Z",
        end_time="1970-01-01T00:00:04Z",
        limit=10,
    )

    assert result["candles"] == []
    assert captured["bar_time_gte"] == "1970-01-01T00:00:00Z"
    assert captured["bar_time_lt"] == "1970-01-01T00:00:04Z"


def test_get_symbol_chart_history_rejects_invalid_truth_candle_payload(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(chart_svc, "get_bot_run", lambda run_id: {"run_id": run_id, "bot_id": "bot-1"})
    monkeypatch.setattr(
        chart_svc,
        "iter_all_run_domain_truth",
        lambda **kwargs: iter(
            [
                type(
                    "Event",
                    (),
                    {
                        "context": {
                            "candle": {
                                "time": "2026-02-01T00:00:00Z",
                                "open": "bad",
                                "high": 1.0,
                                "low": 1.0,
                                "close": 1.0,
                            },
                        }
                    },
                )()
            ]
        ),
    )

    with pytest.raises(ValueError, match="context.candle.open must be a finite number"):
        chart_svc.get_symbol_chart_history(
            run_id="run-1",
            symbol_key="instrument-btc|1m",
            start_time="2026-02-01T00:00:00Z",
            end_time="2026-02-01T00:01:00Z",
            limit=10,
        )


def test_list_run_symbols_reads_run_summary_row(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(svc, "_telemetry_hub", lambda: _FakeTelemetryHub(run_snapshot=_run_summary_state()))
    monkeypatch.setattr(svc, "get_bot_run", lambda run_id: {"run_id": run_id, "bot_id": "bot-1"})

    result = asyncio.run(svc.list_run_symbols(run_id="run-1"))

    assert result == {
        "contract": "botlens_symbol_catalog",
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


def test_list_run_symbols_uses_catalog_transport_contract(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(svc, "_telemetry_hub", lambda: _FakeTelemetryHub(run_snapshot=_run_summary_state()))
    monkeypatch.setattr(svc, "get_bot_run", lambda run_id: {"run_id": run_id, "bot_id": "bot-1"})
    monkeypatch.setattr(
        svc,
        "symbol_catalog_response_contract",
        lambda **kwargs: {"owner": "catalog", "symbol_catalog": kwargs["symbol_catalog"]},
    )

    result = asyncio.run(svc.list_run_symbols(run_id="run-1"))

    assert result["owner"] == "catalog"
    assert "instrument-btc|1m" in result["symbol_catalog"]
