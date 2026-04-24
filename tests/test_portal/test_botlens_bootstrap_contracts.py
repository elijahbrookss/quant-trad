from __future__ import annotations

from dataclasses import replace

from portal.backend.service.bots.botlens_state import (
    RunHealthState,
    SymbolReadinessState,
    SymbolCandlesState,
    SymbolDiagnosticsState,
    SymbolDecisionsState,
    SymbolOverlaysState,
    SymbolSignalsState,
    SymbolStatsState,
    SymbolTradesState,
    empty_symbol_projection_snapshot,
)
from portal.backend.service.bots.botlens_transport import (
    run_bootstrap_contract,
    selected_symbol_snapshot_contract,
    symbol_detail_response_contract,
)


def _symbol_state(symbol_key: str = "instrument-btc|1m"):
    return replace(
        empty_symbol_projection_snapshot(symbol_key),
        seq=7,
        readiness=SymbolReadinessState(snapshot_ready=True, symbol_live=True),
        candles=SymbolCandlesState(candles=({"time": 1, "open": 1, "high": 1, "low": 1, "close": 1},)),
        overlays=SymbolOverlaysState(overlays=({"type": "regime_overlay"},)),
        signals=SymbolSignalsState(signals=({"event_id": "signal-1"},)),
        decisions=SymbolDecisionsState(decisions=({"event_id": "decision-1"},)),
        trades=SymbolTradesState(trades=({"trade_id": "trade-1", "symbol_key": symbol_key},)),
        diagnostics=SymbolDiagnosticsState(diagnostics=({"id": "log-1", "message": "runtime log"},)),
        stats=SymbolStatsState(stats={"total_trades": 1}),
    )


def test_run_bootstrap_contract_is_run_scoped_and_excludes_selected_symbol_state() -> None:
    payload = run_bootstrap_contract(
        bot_id="bot-1",
        run_id="run-1",
        run_meta={"run_id": "run-1"},
        lifecycle={"phase": "live", "status": "running"},
        health={"status": "running", "warning_count": 0, "warnings": []},
        symbol_catalog={
            "instrument-btc|1m": {
                "symbol_key": "instrument-btc|1m",
                "instrument_id": "instrument-btc",
                "symbol": "BTC",
                "timeframe": "1m",
                "last_activity_at": "2026-04-16T12:00:00Z",
            }
        },
        open_trades={},
        selected_symbol_key="instrument-btc|1m",
        state="ready",
        run_live=True,
        transport_eligible=True,
        message="BotLens run bootstrap ready.",
        bootstrap_seq=11,
        base_seq=17,
        stream_session_id="stream-1",
    )

    assert payload["contract"] == "botlens_run_bootstrap"
    assert payload["contract_state"] == "bootstrap_ready"
    assert payload["readiness"] == {
        "catalog_discovered": True,
        "snapshot_ready": False,
        "symbol_live": False,
        "run_live": True,
    }
    assert payload["scope"] == {"bot_id": "bot-1", "run_id": "run-1"}
    assert payload["bootstrap"]["scope"] == "run"
    assert payload["bootstrap"]["base_seq"] == 17
    assert payload["navigation"]["selected_symbol_key"] == "instrument-btc|1m"
    assert payload["navigation"]["symbols"][0]["identity"]["symbol"] == "BTC"
    assert payload["navigation"]["symbols"][0]["activity"]["status"] == "running"
    assert payload["navigation"]["symbols"][0]["readiness"] == {
        "catalog_discovered": True,
        "snapshot_ready": False,
        "symbol_live": False,
    }
    assert payload["selected_symbol"] is None
    assert "detail" not in payload


def test_selected_symbol_snapshot_contract_is_symbol_scoped_and_not_detail_contract() -> None:
    symbol_state = _symbol_state()

    payload = selected_symbol_snapshot_contract(
        bot_id="bot-1",
        run_id="run-1",
        symbol_key="instrument-btc|1m",
        symbol_state=symbol_state,
        symbol_catalog_entry=None,
        run_health=RunHealthState(
            status="running",
            phase="live",
            warning_count=0,
            warnings=(),
            last_event_at="2026-04-16T12:00:00Z",
            worker_count=2,
            active_workers=2,
        ).to_dict(),
        run_bootstrap_seq=11,
        base_seq=17,
        stream_session_id="stream-1",
        run_live=True,
        transport_eligible=True,
        message="BotLens selected-symbol snapshot ready.",
    )

    assert payload["contract"] == "botlens_selected_symbol_snapshot"
    assert payload["contract_state"] == "snapshot_ready"
    assert payload["scope"] == {
        "bot_id": "bot-1",
        "run_id": "run-1",
        "symbol_key": "instrument-btc|1m",
    }
    assert payload["readiness"] == {
        "catalog_discovered": True,
        "snapshot_ready": True,
        "symbol_live": True,
        "run_live": True,
    }
    assert payload["bootstrap"]["scope"] == "selected_symbol_snapshot"
    assert payload["bootstrap"]["base_seq"] == 17
    assert payload["selected_symbol"]["metadata"]["symbol_key"] == "instrument-btc|1m"
    assert payload["selected_symbol"]["metadata"]["readiness"] == {
        "catalog_discovered": True,
        "snapshot_ready": True,
        "symbol_live": True,
        "run_live": True,
    }
    assert payload["selected_symbol"]["current"]["candles"][0]["time"] == 1
    assert payload["selected_symbol"]["current"]["continuity"]["candle_count"] == 1
    assert payload["selected_symbol"]["current"]["overlays"][0]["type"] == "regime_overlay"
    assert payload["selected_symbol"]["current"]["logs"][0]["message"] == "runtime log"
    assert payload["selected_symbol"]["current"]["runtime"]["status"] == "running"
    assert payload["live_transport"]["selected_symbol_key"] == "instrument-btc|1m"
    assert payload["live_transport"]["stream_session_id"] == "stream-1"
    assert "detail" not in payload


def test_symbol_detail_contract_remains_separate_from_selected_symbol_snapshot_contract() -> None:
    symbol_state = _symbol_state()

    detail_payload = symbol_detail_response_contract(
        run_id="run-1",
        symbol_state=symbol_state,
        run_health={"status": "running", "warning_count": 0, "warnings": []},
    )
    snapshot_payload = selected_symbol_snapshot_contract(
        bot_id="bot-1",
        run_id="run-1",
        symbol_key="instrument-btc|1m",
        symbol_state=symbol_state,
        symbol_catalog_entry=None,
        run_health={"status": "running", "warning_count": 0, "warnings": []},
        run_bootstrap_seq=11,
        base_seq=17,
        stream_session_id="stream-1",
        run_live=False,
        transport_eligible=False,
        message="BotLens selected-symbol snapshot ready.",
    )

    assert detail_payload["contract"] == "botlens_symbol_detail"
    assert snapshot_payload["contract"] == "botlens_selected_symbol_snapshot"
    assert "detail" in detail_payload
    assert "selected_symbol" in snapshot_payload
    assert detail_payload["scope"] == {"run_id": "run-1", "symbol_key": "instrument-btc|1m"}
    assert snapshot_payload["scope"]["symbol_key"] == "instrument-btc|1m"
    assert "logs" in detail_payload["detail"]
    assert "logs" in snapshot_payload["selected_symbol"]["current"]


def test_selected_symbol_snapshot_contract_can_report_unavailable_without_fabricating_state() -> None:
    payload = selected_symbol_snapshot_contract(
        bot_id="bot-1",
        run_id="run-1",
        symbol_key="instrument-btc|1m",
        symbol_state=None,
        symbol_catalog_entry={
            "symbol_key": "instrument-btc|1m",
            "symbol": "BTC",
            "timeframe": "1m",
            "display_label": "BTC · 1m",
            "status": "running",
        },
        run_health={"status": "running", "warning_count": 0, "warnings": []},
        run_bootstrap_seq=11,
        base_seq=17,
        stream_session_id="stream-1",
        run_live=True,
        transport_eligible=True,
        state="unavailable",
        unavailable_reason="symbol_snapshot_unavailable",
        message="BotLens selected-symbol snapshot is unavailable because projector state has not been built yet.",
    )

    assert payload["contract"] == "botlens_selected_symbol_snapshot"
    assert payload["state"] == "unavailable"
    assert payload["contract_state"] == "snapshot_unavailable"
    assert payload["bootstrap"]["ready"] is False
    assert payload["unavailable_reason"] == "symbol_snapshot_unavailable"
    assert payload["readiness"] == {
        "catalog_discovered": True,
        "snapshot_ready": False,
        "symbol_live": False,
        "run_live": True,
    }
    assert payload["selected_symbol"] is None
