from __future__ import annotations

import copy
from typing import Any

import pytest

from portal.backend.service.reports import report_data, run_research_dataset


class _ResearchDatasetStorage:
    def __init__(
        self,
        *,
        run: dict[str, Any],
        events: list[dict[str, Any]],
        trades: list[dict[str, Any]],
        steps: list[dict[str, Any]] | None = None,
        observability_events: list[dict[str, Any]] | None = None,
        candle_summaries: dict[tuple[str, str], dict[str, Any]] | None = None,
        candle_closures: dict[tuple[str, str], list[dict[str, Any]]] | None = None,
        candles: dict[tuple[str, str], list[dict[str, Any]]] | None = None,
    ) -> None:
        self._run = dict(run)
        self._events = sorted([dict(row) for row in events], key=lambda row: int(row.get("seq") or 0))
        self._trades = [dict(row) for row in trades]
        self._steps = [dict(row) for row in steps or []]
        self._observability_events = [dict(row) for row in observability_events or []]
        self._candle_summaries = {tuple(key): dict(value) for key, value in (candle_summaries or {}).items()}
        self._candle_closures = {
            tuple(key): [dict(row) for row in value]
            for key, value in (candle_closures or {}).items()
        }
        self._candles = {
            tuple(key): [dict(row) for row in value]
            for key, value in (candles or {}).items()
        }

    def get_bot_run(self, run_id: str):
        return dict(self._run) if run_id == self._run.get("run_id") else None

    def list_bot_trades_for_run(self, run_id: str):
        _ = run_id
        return [dict(row) for row in self._trades]

    def list_bot_run_steps_for_run(self, run_id: str):
        _ = run_id
        return [dict(row) for row in self._steps]

    def list_observability_events(self, run_id: str, limit: int = 2000):
        _ = run_id
        return [dict(row) for row in self._observability_events[:limit]]

    def get_candle_storage_summary(self, *, instrument_id: str, timeframe: str, start, end):
        _ = start, end
        summary = self._candle_summaries.get((instrument_id, timeframe))
        return dict(summary) if summary else None

    def list_candle_closure_evidence(self, *, instrument_id: str, timeframe: str, start, end):
        _ = start, end
        return [dict(row) for row in self._candle_closures.get((instrument_id, timeframe), [])]

    def list_candles_for_series(
        self,
        *,
        instrument_id: str,
        timeframe: str,
        start,
        end,
        limit: int,
        prefer_latest: bool = False,
    ):
        _ = start, end, prefer_latest
        return [dict(row) for row in self._candles.get((instrument_id, timeframe), [])[: int(limit or 2000)]]

    def list_bot_runtime_events(
        self,
        *,
        bot_id: str,
        run_id: str,
        after_seq: int,
        limit: int,
        event_types=None,
        event_type_prefixes=None,
        **_kwargs,
    ):
        _ = bot_id, run_id
        rows = [row for row in self._events if int(row.get("seq") or 0) > int(after_seq or 0)]
        if event_types:
            allowed = {str(entry) for entry in event_types}
            rows = [row for row in rows if str(row.get("event_type") or "") in allowed]
        if event_type_prefixes:
            prefixes = tuple(str(entry) for entry in event_type_prefixes)
            rows = [row for row in rows if str(row.get("event_type") or "").startswith(prefixes)]
        return [dict(row) for row in rows[: int(limit or 5000)]]


def _run() -> dict[str, Any]:
    return {
        "run_id": "run-1",
        "bot_id": "bot-1",
        "bot_name": "Research Bot",
        "strategy_id": "strategy-1",
        "strategy_name": "Research Strategy",
        "run_type": "backtest",
        "status": "completed",
        "timeframe": "1h",
        "datasource": "coinbase",
        "exchange": "CBI",
        "symbols": ["BTC", "ETH"],
        "backtest_start": "2026-03-01T00:00:00Z",
        "backtest_end": "2026-03-31T00:00:00Z",
        "started_at": "2026-04-01T00:00:00Z",
        "ended_at": "2026-04-01T00:10:00Z",
        "summary": {"net_pnl": 53.0, "total_trades": 3},
        "config_snapshot": {
            "execution_mode": "full",
            "playback_mode": "instant",
            "wallet_start": {"balances": {"USDC": 1000}},
            "date_range": {
                "start": "2026-03-01T00:00:00Z",
                "end": "2026-03-31T00:00:00Z",
            },
            "symbols": ["BTC", "ETH"],
            "timeframe": "1h",
            "material_config_hash": "material-1",
            "risk_settings": {"risk_per_trade": 0.01, "slippage_bps": 0.0},
            "atm_template": {"id": "atm-1", "targets": [1, 2, 3]},
            "indicators": [{"id": "ind-1", "type": "market_profile"}],
        },
    }


def _event(seq: int, event_name: str, context: dict[str, Any], *, event_type: str | None = None) -> dict[str, Any]:
    normalized_type = event_type or f"botlens_domain.{event_name.lower()}"
    return {
        "event_id": f"evt-{seq}",
        "seq": seq,
        "run_seq": context.get("run_seq", seq),
        "run_seq_status": context.get("run_seq_status", "runtime_assigned"),
        "event_type": normalized_type,
        "event_name": event_name,
        "symbol": context.get("symbol"),
        "timeframe": context.get("timeframe"),
        "trade_id": context.get("trade_id"),
        "decision_id": context.get("decision_id"),
        "signal_id": context.get("signal_id"),
        "reason_code": context.get("reason_code"),
        "bar_time": context.get("bar_time"),
        "payload": {
            "schema_version": 1,
            "event_id": f"evt-{seq}",
            "event_ts": context.get("event_time") or context.get("bar_time") or "2026-03-01T00:00:00Z",
            "event_name": event_name,
            "context": {
                "run_id": "run-1",
                "bot_id": "bot-1",
                "strategy_id": "strategy-1",
                "run_seq": seq,
                "run_seq_status": "runtime_assigned",
                "series_key": f"instrument-{context.get('symbol', 'BTC').lower()}|1h",
                "instrument_id": f"instrument-{context.get('symbol', 'BTC').lower()}",
                "symbol": context.get("symbol", "BTC"),
                "timeframe": context.get("timeframe", "1h"),
                **context,
            },
        },
    }


def _decision(seq: int, decision_id: str, state: str, *, trade_id: str | None = None, reason_code: str | None = None) -> dict[str, Any]:
    context = {
        "decision_id": decision_id,
        "decision_state": state,
        "signal_id": f"signal-{decision_id}",
        "rule_id": "rule-breakout",
        "rule_name": "Breakout",
        "direction": "long",
        "signal_price": 100.0,
        "trade_id": trade_id,
        "bar_time": f"2026-03-0{seq}T00:00:00Z",
        "wallet_snapshot": {
            "balances": {"USDC": 1000.0},
            "locked_margin": {"USDC": 0.0},
            "free_collateral": {"USDC": 1000.0},
            "margin_positions": {},
        },
    }
    if reason_code:
        context["reason_code"] = reason_code
        context["message"] = reason_code
    return _event(seq, "DECISION_EMITTED", context)


def _trade_event(
    seq: int,
    event_name: str,
    trade_id: str,
    symbol: str,
    *,
    close_reason: str | None = None,
    include_position_seq: bool = True,
    position_commit_seq: int | None = None,
) -> dict[str, Any]:
    context = {
        "trade_id": trade_id,
        "symbol": symbol,
        "bar_time": f"2026-03-{10 + seq:02d}T00:00:00Z",
        "entry_price": 100.0,
        "exit_price": 110.0,
        "close_reason": close_reason,
        "reason_code": close_reason,
        "legs": [{"status": "closed", "exit_time": f"2026-03-{10 + seq:02d}T00:00:00Z"}],
    }
    if include_position_seq:
        context["position_commit_seq"] = position_commit_seq or (1 if event_name == "TRADE_OPENED" else 2)
        context["position_commit_seq_status"] = "position_scoped"
    return _event(seq, event_name, context)


def _fallback(seq: int, symbol: str = "BTC", reason: str = "missing_1m_data") -> dict[str, Any]:
    return _event(
        seq,
        "execution_intrabar_fallback_pessimistic",
        {
            "symbol": symbol,
            "timeframe": "1h",
            "bar_time": "2026-03-20T00:00:00Z",
            "reason": reason,
            "raw_reason": reason,
            "execution_mode": "full",
        },
        event_type="runtime.execution",
    )


def _gap(seq: int) -> dict[str, Any]:
    return _event(
        seq,
        "candle_continuity_summary",
        {
            "symbol": "BTC",
            "instrument_id": "instrument-btc",
            "timeframe": "1h",
            "series_key": "instrument-btc|1h",
            "boundary_name": "run_final",
            "source_reason": "provider_missing_data",
            "detected_gap_count": 2,
            "gap_count_by_type": {"provider_missing_data": 1, "unknown_gap": 1},
        },
        event_type="observability",
    )


def _provider_gap(seq: int, *, include_identity: bool = True) -> dict[str, Any]:
    context = {
        "series_key": "instrument-btc|1h",
        "boundary_name": "run_final",
        "source_reason": "provider_missing_data",
        "detected_gap_count": 2,
        "gap_count_by_type": {"provider_missing_data": 2},
        "gaps": [
            {
                "previous_ts": "2026-03-06T21:00:00Z",
                "current_ts": "2026-03-06T23:00:00Z",
                "classification": "provider_missing_data",
                "reason_code": "provider_response_empty",
                "evidence": "provider_api_empty_response",
                "provider_evidence": {"provider_message": "exchange returned no candle"},
            }
        ],
    }
    if include_identity:
        context.update({"symbol": "BTC", "instrument_id": "instrument-btc", "timeframe": "1h"})
    else:
        context.update({"timeframe": "1h"})
    row = _event(seq, "candle_continuity_summary", context, event_type="observability")
    if not include_identity:
        row["symbol"] = None
        row["payload"]["context"]["symbol"] = None
        row["payload"]["context"]["instrument_id"] = None
    return row


def _unknown_gap(seq: int) -> dict[str, Any]:
    return _event(
        seq,
        "candle_continuity_summary",
        {
            "symbol": "BTC",
            "instrument_id": "instrument-btc",
            "series_key": "instrument-btc|1h",
            "boundary_name": "run_final",
            "detected_gap_count": 1,
            "gap_count_by_type": {"unknown_gap": 1},
            "gaps": [
                {
                    "previous_ts": "2026-03-06T21:00:00Z",
                    "current_ts": "2026-03-06T23:00:00Z",
                    "classification": "unknown_gap",
                    "expected_interval_seconds": 3600,
                    "actual_interval_seconds": 7200,
                    "missing_candle_estimate": 1,
                }
            ],
        },
        event_type="observability",
    )


def _observer_gap(
    *,
    boundary_name: str = "selected_symbol_snapshot",
    pipeline_stage: str = "botlens_selected_symbol_snapshot",
    message_kind: str = "ephemeral",
    detected_gap_count: int = 9,
) -> dict[str, Any]:
    return {
        "level": "INFO",
        "event_name": "candle_continuity_summary",
        "component": "botlens_symbol_service",
        "pipeline_stage": pipeline_stage,
        "message_kind": message_kind,
        "bot_id": "bot-1",
        "run_id": "run-1",
        "series_key": "instrument-btc|1h",
        "instrument_id": "instrument-btc",
        "symbol": "BTC",
        "timeframe": "1h",
        "observed_at": "2026-04-01T00:09:00Z",
        "details": {
            "boundary_name": boundary_name,
            "source_reason": "observer_snapshot",
            "series_key": "instrument-btc|1h",
            "instrument_id": "instrument-btc",
            "symbol": "BTC",
            "timeframe": "1h",
            "message_kind": message_kind,
            "pipeline_stage": pipeline_stage,
            "materiality": "diagnostic",
            "diagnostic_scope": "botlens_observer",
            "detected_gap_count": detected_gap_count,
            "gap_count_by_type": {"unknown_gap": detected_gap_count},
            "candle_count": 320,
            "missing_candle_estimate": detected_gap_count,
            "gaps": [
                {
                    "previous_ts": "2026-03-10T00:00:00Z",
                    "current_ts": "2026-03-10T10:00:00Z",
                    "classification": "unknown_gap",
                }
            ],
        },
    }


def _trades(extra: list[dict[str, Any]] | None = None) -> list[dict[str, Any]]:
    rows = [
        {
            "id": "trade-1",
            "run_id": "run-1",
            "bot_id": "bot-1",
            "strategy_id": "strategy-1",
            "symbol": "BTC",
            "direction": "long",
            "status": "closed",
            "contracts": 1,
            "entry_time": "2026-03-05T00:00:00Z",
            "entry_price": 100.0,
            "exit_time": "2026-03-05T02:00:00Z",
            "gross_pnl": 100.0,
            "fees_paid": 10.0,
            "net_pnl": 90.0,
            "metrics": {"close_reason": "TARGET", "fee_rate": 0.001, "fee_role": "taker", "fee_source": "runtime"},
        },
        {
            "id": "trade-2",
            "run_id": "run-1",
            "bot_id": "bot-1",
            "strategy_id": "strategy-1",
            "symbol": "BTC",
            "direction": "long",
            "status": "closed",
            "contracts": 1,
            "entry_time": "2026-03-23T00:00:00Z",
            "entry_price": 100.0,
            "exit_time": "2026-03-24T00:00:00Z",
            "gross_pnl": -50.0,
            "fees_paid": 5.0,
            "net_pnl": -55.0,
            "metrics": {"close_reason": "STOP", "fee_rate": 0.001, "fee_role": "taker", "fee_source": "runtime"},
        },
        {
            "id": "trade-3",
            "run_id": "run-1",
            "bot_id": "bot-1",
            "strategy_id": "strategy-1",
            "symbol": "ETH",
            "direction": "long",
            "status": "closed",
            "contracts": 1,
            "entry_time": "2026-03-28T00:00:00Z",
            "entry_price": 100.0,
            "exit_time": "2026-03-29T00:00:00Z",
            "gross_pnl": 20.0,
            "fees_paid": 2.0,
            "net_pnl": 18.0,
            "metrics": {"close_reason": "MIXED", "fee_rate": 0.001, "fee_role": "taker", "fee_source": "runtime"},
        },
    ]
    if extra:
        rows.extend(extra)
    return rows


def _events(*, omit_closed: str | None = None) -> list[dict[str, Any]]:
    rows = [
        _decision(1, "decision-1", "accepted", trade_id="trade-1"),
        _decision(2, "decision-2", "accepted", trade_id="trade-2"),
        _decision(3, "decision-3", "accepted", trade_id="trade-3"),
        _decision(4, "decision-4", "rejected", reason_code="WALLET_INSUFFICIENT_MARGIN"),
        _trade_event(5, "TRADE_OPENED", "trade-1", "BTC"),
        _trade_event(6, "TRADE_CLOSED", "trade-1", "BTC", close_reason="TARGET"),
        _trade_event(7, "TRADE_OPENED", "trade-2", "BTC"),
        _trade_event(8, "TRADE_CLOSED", "trade-2", "BTC", close_reason="STOP"),
        _trade_event(9, "TRADE_OPENED", "trade-3", "ETH"),
        _trade_event(10, "TRADE_CLOSED", "trade-3", "ETH", close_reason="MIXED"),
        _fallback(11, "BTC", "missing_1m_data"),
        _fallback(12, "ETH", "ambiguous_1m_candle"),
        _gap(13),
    ]
    if omit_closed:
        rows = [
            row
            for row in rows
            if not (_event_name(row) == "TRADE_CLOSED" and row.get("trade_id") == omit_closed)
        ]
    return rows


def _event_name(row: dict[str, Any]) -> str:
    return str(row.get("event_name") or row.get("payload", {}).get("event_name") or "")


def _steps() -> list[dict[str, Any]]:
    return [
        {"step_name": "prepare", "duration_ms": 10.0, "started_at": "2026-04-01T00:00:00Z", "ended_at": "2026-04-01T00:00:00.010000Z"},
        {"step_name": "runtime_loop", "duration_ms": 100.0, "started_at": "2026-04-01T00:00:01Z", "ended_at": "2026-04-01T00:00:01.100000Z"},
        {"step_name": "runtime_loop", "duration_ms": 200.0, "started_at": "2026-04-01T00:00:02Z", "ended_at": "2026-04-01T00:00:02.200000Z"},
    ]


def _install(monkeypatch: pytest.MonkeyPatch, storage: _ResearchDatasetStorage) -> None:
    monkeypatch.setattr(run_research_dataset, "storage", storage)
    monkeypatch.setattr(report_data, "storage", storage)


def _build(
    monkeypatch: pytest.MonkeyPatch,
    *,
    events=None,
    trades=None,
    observability_events=None,
    candle_summaries=None,
    candle_closures=None,
    candles=None,
):
    fake_storage = _ResearchDatasetStorage(
        run=_run(),
        events=_events() if events is None else events,
        trades=_trades() if trades is None else trades,
        steps=_steps(),
        observability_events=observability_events,
        candle_summaries=candle_summaries,
        candle_closures=candle_closures,
        candles=candles,
    )
    _install(monkeypatch, fake_storage)
    return run_research_dataset.build_run_research_dataset("run-1")


def test_dataset_builds_from_db_truth_without_artifact_directory(monkeypatch: pytest.MonkeyPatch) -> None:
    dataset = _build(monkeypatch)

    assert dataset["readiness"]["dataset_ready"] is True
    assert dataset["readiness"]["results_ready"] is True
    assert dataset["readiness"]["safe_to_compare"] is True
    assert dataset["readiness"]["dataset_status"] == "ready"
    assert dataset["readiness"]["results_status"] == "ready"
    assert dataset["readiness"]["comparison_status"] == "ready_with_caveats"
    assert dataset["readiness"]["execution_quality_status"] == "degraded"
    assert dataset["readiness"]["export_status"] == "available"
    assert dataset["sections"]["schema_version"] == "report_sections.v1"
    assert dataset["diagnostics"]["schema_version"] == "report_diagnostics.v1"
    assert dataset["timeseries"]["schema_version"] == "report_timeseries.v1"
    assert dataset["context"]["schema_version"] == "report_context.v1"
    assert dataset["candle_catalog"]["schema_version"] == "candle_catalog.v1"
    assert dataset["operational_health"]["schema_version"] == "operational_health.v1"


def test_dataset_summary_matches_trades_events_and_report_db_values(monkeypatch: pytest.MonkeyPatch) -> None:
    dataset = _build(monkeypatch)

    assert dataset["summary"]["total_decisions"] == 4
    assert dataset["summary"]["accepted_decisions"] == 3
    assert dataset["summary"]["rejected_decisions"] == 1
    assert dataset["summary"]["trades"] == 3
    assert dataset["summary"]["gross_pnl"] == pytest.approx(70.0)
    assert dataset["summary"]["fees"] == pytest.approx(17.0)
    assert dataset["summary"]["net_pnl"] == pytest.approx(53.0)


def test_dataset_includes_canonical_portfolio_metrics(monkeypatch: pytest.MonkeyPatch) -> None:
    dataset = _build(monkeypatch)

    metrics = dataset["portfolio_metrics"]
    assert metrics["schema_version"] == "portfolio_metrics.v1"
    assert metrics["annualization_periods"] == 252
    assert metrics["basis"]["return_series"] == "daily_closed_trade_net_pnl_over_starting_equity"
    assert metrics["sharpe"] is not None
    assert metrics["annualized_volatility"] is not None
    assert "sharpe_unavailable" not in metrics["caveats"]


def test_dataset_readiness_true_when_dataset_complete(monkeypatch: pytest.MonkeyPatch) -> None:
    dataset = _build(monkeypatch)

    assert dataset["readiness"]["conditions"]["dataset_ready"] is True
    assert dataset["readiness"]["conditions"]["accepted_trade_lifecycle_complete"] is True
    assert dataset["readiness"]["conditions"]["comparable_metrics_available"] is True


def test_safe_to_compare_false_when_accepted_trade_lifecycle_incomplete(monkeypatch: pytest.MonkeyPatch) -> None:
    dataset = _build(monkeypatch, events=_events(omit_closed="trade-2"))

    assert dataset["readiness"]["results_ready"] is False
    assert dataset["readiness"]["safe_to_compare"] is False
    assert dataset["readiness"]["reason"] == "trade_lifecycle_incomplete"


def test_safe_to_compare_false_when_terminal_open_trades_exist(monkeypatch: pytest.MonkeyPatch) -> None:
    open_trade = {
        "id": "trade-open",
        "run_id": "run-1",
        "bot_id": "bot-1",
        "strategy_id": "strategy-1",
        "symbol": "BTC",
        "direction": "long",
        "status": "open",
        "entry_time": "2026-03-30T00:00:00Z",
        "entry_price": 100.0,
        "exit_time": None,
        "gross_pnl": None,
        "fees_paid": None,
        "net_pnl": None,
        "metrics": {},
    }
    dataset = _build(monkeypatch, trades=_trades([open_trade]))

    assert dataset["summary"]["open_trades"] == 1
    assert dataset["readiness"]["safe_to_compare"] is False
    assert dataset["readiness"]["reason"] == "terminal_open_trades"


def test_position_ordering_missing_blocks_golden_candidate(monkeypatch: pytest.MonkeyPatch) -> None:
    events = _events()
    events[4] = _trade_event(5, "TRADE_OPENED", "trade-1", "BTC", include_position_seq=False)

    dataset = _build(monkeypatch, events=events)

    diagnostic_codes = {item["code"] for item in dataset["diagnostics"]["items"]}
    assert "position_ordering_missing" in dataset["readiness"]["caveats"]
    assert "position_ordering_missing" in dataset["readiness"]["golden_blocking_reasons"]
    assert "position_ordering_missing" in diagnostic_codes


def test_position_ordering_gap_is_informational_for_sparse_trade_events(monkeypatch: pytest.MonkeyPatch) -> None:
    events = _events()
    events[5] = _trade_event(6, "TRADE_CLOSED", "trade-1", "BTC", close_reason="TARGET", position_commit_seq=3)

    dataset = _build(monkeypatch, events=events)

    position_ordering = dataset["execution"]["position_ordering"]
    diagnostic_codes = {item["code"] for item in dataset["diagnostics"]["items"]}
    assert position_ordering["gap_count"] == 1
    assert position_ordering["gaps"][0]["missing_position_commit_seq"] == [2]
    assert position_ordering["replay_ordering_key"] == "trade_id,position_commit_seq"
    assert "position_ordering_gap" not in dataset["readiness"]["caveats"]
    assert "position_ordering_gap" not in dataset["readiness"]["golden_blocking_reasons"]
    assert "position_ordering_gap" not in diagnostic_codes


def test_trade_closed_context_uses_highest_position_commit_seq() -> None:
    closed = _trade_event(6, "TRADE_CLOSED", "trade-1", "BTC", close_reason="TARGET", position_commit_seq=2)
    stale = _trade_event(20, "TRADE_CLOSED", "trade-1", "BTC", close_reason="STALE", position_commit_seq=1)

    contexts = run_research_dataset._trade_closed_context_by_id([closed, stale])

    assert contexts["trade-1"]["close_reason"] == "TARGET"
    assert contexts["trade-1"]["position_commit_seq"] == 2


def test_safe_to_compare_true_when_dataset_complete_and_lifecycle_clean(monkeypatch: pytest.MonkeyPatch) -> None:
    dataset = _build(monkeypatch)

    assert dataset["readiness"]["results_ready"] is True
    assert dataset["readiness"]["safe_to_compare"] is True
    assert dataset["readiness"]["export_status"] == "available"


def test_dataset_includes_execution_mode_and_intrabar_fallback_summary(monkeypatch: pytest.MonkeyPatch) -> None:
    dataset = _build(monkeypatch)

    assert dataset["metadata"]["execution_mode"] == "full"
    assert dataset["metadata"]["configuration"]["risk"]["slippage_bps"] == 0.0
    assert dataset["metadata"]["configuration"]["atm"]["id"] == "atm-1"
    assert dataset["metadata"]["configuration"]["indicators"][0]["type"] == "market_profile"
    assert dataset["execution"]["execution_mode"] == "full"
    assert dataset["execution"]["slippage"]["total_slippage_cost"] == 0.0
    assert dataset["execution"]["intrabar_fallback_count"] == 2
    assert dataset["execution"]["fallback_reason_distribution"] == {
        "ambiguous_1m_candle": 1,
        "missing_1m_data": 1,
    }
    diagnostic_codes = {item["code"] for item in dataset["diagnostics"]["items"]}
    assert "intrabar_fallback_pessimistic" in diagnostic_codes


def test_dataset_enriches_trade_entry_risk_excursion_and_fallback_flags(monkeypatch: pytest.MonkeyPatch) -> None:
    trades = _trades()
    trades[0]["stop_price"] = 90.0
    trades[0]["tick_size"] = 1.0
    trades[0]["tick_value"] = 1.0
    trades[0]["metrics"] = {
        **trades[0]["metrics"],
        "r_ticks": 10.0,
        "r_value": 10.0,
        "legs": [
            {
                "id": "leg-1",
                "name": "tp-1",
                "status": "target",
                "exit_price": 110.0,
                "exit_time": "2026-03-05T01:00:00Z",
                "contracts": 1,
                "ticks": 10,
                "target_price": 110.0,
            }
        ],
    }
    events = _events() + [
        _event(
            20,
            "execution_intrabar_fallback_pessimistic",
            {
                "symbol": "BTC",
                "timeframe": "1h",
                "bar_time": "2026-03-05T01:00:00Z",
                "reason": "ambiguous_1m_candle",
                "raw_reason": "ambiguous_1m_candle",
                "execution_mode": "full",
            },
            event_type="runtime.execution",
        )
    ]

    dataset = _build(
        monkeypatch,
        events=events,
        trades=trades,
        candles={
            ("instrument-btc", "1m"): [
                {"time": "2026-03-05T00:00:00Z", "open": 100.0, "high": 105.0, "low": 98.0, "close": 103.0},
                {"time": "2026-03-05T01:00:00Z", "open": 103.0, "high": 112.0, "low": 96.0, "close": 111.0},
                {"time": "2026-03-05T02:00:00Z", "open": 111.0, "high": 111.0, "low": 99.0, "close": 110.0},
            ]
        },
    )

    trade = next(row for row in dataset["trades"] if row["trade_id"] == "trade-1")
    assert trade["entry_risk"]["stop_distance_price"] == pytest.approx(10.0)
    assert trade["entry_risk"]["r_ticks"] == pytest.approx(10.0)
    assert trade["excursion"]["mae_ticks"] == pytest.approx(-4.0)
    assert trade["excursion"]["mfe_ticks"] == pytest.approx(12.0)
    assert trade["excursion"]["mae_r"] == pytest.approx(-0.4)
    assert trade["intrabar_fallback_within_trade"] is True
    assert trade["intrabar_fallback_reasons"] == ["ambiguous_1m_candle"]
    assert trade["legs"][0]["excursion"]["mfe_ticks"] == pytest.approx(12.0)
    assert trade["legs"][0]["intrabar_fallback_within_leg"] is True


def test_dataset_includes_signals_and_trace_identity(monkeypatch: pytest.MonkeyPatch) -> None:
    dataset = _build(monkeypatch)

    assert len(dataset["signals"]) == 0
    assert dataset["decisions"][0]["instrument_id"] == "instrument-btc"
    assert dataset["trades"][0]["run_id"] == "run-1"
    assert dataset["trades"][0]["decision_id"] == "decision-1"
    assert dataset["decisions"][0]["run_id"] == "run-1"


def test_dataset_includes_timeseries_context_and_candle_catalog(monkeypatch: pytest.MonkeyPatch) -> None:
    dataset = _build(monkeypatch)

    assert dataset["timeseries"]["items"]["equity_curve"]["row_count"] == 4
    assert dataset["timeseries"]["items"]["returns_series"]["row_count"] == 3
    assert dataset["context"]["decision_context"]["row_count"] == 4
    assert dataset["context"]["trade_context"]["row_count"] == 3
    assert dataset["candle_catalog"]["items"]
    assert dataset["operational_health"]["event_volume_summary"]["total"] >= 1


def test_dataset_extracts_runtime_indicator_and_market_context_from_signal_artifact(monkeypatch: pytest.MonkeyPatch) -> None:
    events = [
        _event(
            1,
            "SIGNAL_EMITTED",
            {
                "signal_id": "signal-1",
                "decision_id": "decision-1",
                "rule_id": "rule-breakout",
                "signal_type": "strategy_signal",
                "direction": "long",
                "intent": "enter_long",
                "event_key": "breakout_long",
                "signal_price": 100.0,
                "bar_time": "2026-03-05T00:00:00Z",
                "decision_artifact": {
                    "decision_id": "decision-1",
                    "referenced_outputs": {
                        "ind-1.signal": {
                            "output_ref": "ind-1.signal",
                            "indicator_id": "ind-1",
                            "output_name": "signal",
                            "type": "signal",
                            "output_type": "signal",
                            "ready": True,
                            "bar_time": "2026-03-05T00:00:00Z",
                            "indicator_commit_seq": 12,
                            "indicator_commit_seq_status": "indicator_scoped",
                            "event_keys": ["breakout_long"],
                            "events": [
                                {
                                    "key": "breakout_long",
                                    "direction": "long",
                                    "known_at": 1772668800,
                                    "metadata": {
                                        "breakout_time": 1772665200,
                                        "confirmation_bars_required": 2,
                                        "reference": {"kind": "price_level", "label": "VAH", "price": 99.0},
                                        "distance_from_reference": 1.0,
                                    },
                                }
                            ],
                            "event_count": 1,
                        },
                        "ind-1.market_state": {
                            "output_ref": "ind-1.market_state",
                            "indicator_id": "ind-1",
                            "output_name": "market_state",
                            "type": "context",
                            "output_type": "context",
                            "ready": True,
                            "bar_time": "2026-03-05T00:00:00Z",
                            "indicator_commit_seq": 13,
                            "indicator_commit_seq_status": "indicator_scoped",
                            "state_key": "trend",
                            "fields": {"bias": "long", "state": "trend"},
                        },
                    },
                },
            },
        )
    ]

    dataset = _build(monkeypatch, events=events, trades=[])

    indicator_context = dataset["context"]["indicator_snapshots"]
    market_state = dataset["context"]["market_state"]
    assert indicator_context["row_count"] == 2
    signal_snapshot = next(row for row in indicator_context["items"] if row["output_name"] == "signal")
    assert signal_snapshot["values"]["events"][0]["metadata"]["distance_from_reference"] == 1.0
    assert dataset["signals"][0]["indicator_context"]["outputs"]["ind-1.signal"]["events"][0]["metadata"]["confirmation_bars_required"] == 2
    assert market_state["row_count"] == 1
    assert indicator_context["items"][1]["indicator_commit_seq"] == 13
    assert market_state["items"][0]["context_values"]["ind-1.market_state"] == {
        "state_key": "trend",
        "fields": {"bias": "long", "state": "trend"},
    }
    assert "indicator_snapshot_runtime_capture_unavailable" not in dataset["context"]["caveats"]
    assert "market_state_runtime_capture_unavailable" not in dataset["context"]["caveats"]


def test_dataset_extracts_market_context_from_observed_signal_artifact_outputs(monkeypatch: pytest.MonkeyPatch) -> None:
    events = [
        _event(
            1,
            "SIGNAL_EMITTED",
            {
                "signal_id": "signal-1",
                "decision_id": "decision-1",
                "rule_id": "rule-breakout",
                "signal_type": "strategy_signal",
                "direction": "long",
                "intent": "enter_long",
                "event_key": "breakout_long",
                "signal_price": 100.0,
                "bar_time": "2026-03-05T00:00:00Z",
                "decision_artifact": {
                    "decision_id": "decision-1",
                    "referenced_outputs": {
                        "ind-1.signal": {
                            "output_ref": "ind-1.signal",
                            "indicator_id": "ind-1",
                            "output_name": "signal",
                            "type": "signal",
                            "output_type": "signal",
                            "ready": True,
                            "bar_time": "2026-03-05T00:00:00Z",
                            "indicator_commit_seq": 12,
                            "indicator_commit_seq_status": "indicator_scoped",
                            "event_keys": ["breakout_long"],
                        },
                    },
                    "observed_outputs": {
                        "ind-1.market_regime": {
                            "output_ref": "ind-1.market_regime",
                            "indicator_id": "ind-1",
                            "output_name": "market_regime",
                            "type": "context",
                            "output_type": "context",
                            "ready": True,
                            "bar_time": "2026-03-05T00:00:00Z",
                            "indicator_commit_seq": 13,
                            "indicator_commit_seq_status": "indicator_scoped",
                            "state_key": "trend_up",
                            "fields": {"context_regime_state": "trend_up", "trend_direction": "long"},
                        },
                    },
                },
            },
        )
    ]

    dataset = _build(monkeypatch, events=events, trades=[])

    indicator_context = dataset["context"]["indicator_snapshots"]
    market_state = dataset["context"]["market_state"]
    assert indicator_context["row_count"] == 2
    assert market_state["row_count"] == 1
    assert market_state["items"][0]["context_values"]["ind-1.market_regime"] == {
        "state_key": "trend_up",
        "fields": {"context_regime_state": "trend_up", "trend_direction": "long"},
    }
    assert "market_state_runtime_capture_unavailable" not in dataset["context"]["caveats"]


def test_candle_catalog_uses_series_identity_without_symbol_instrument_cross_product(monkeypatch: pytest.MonkeyPatch) -> None:
    dataset = _build(
        monkeypatch,
        events=[row for row in _events() if _event_name(row) != "candle_continuity_summary"],
        candle_summaries={
            ("instrument-btc", "1h"): {
                "candle_count": 10,
                "gap_count": 0,
                "missing_count": 0,
                "available_resolutions": ["1m", "1h"],
            },
            ("instrument-eth", "1h"): {
                "candle_count": 8,
                "gap_count": 0,
                "missing_count": 0,
                "available_resolutions": ["1h"],
            },
        },
    )

    rows = dataset["candle_catalog"]["items"]
    pairs = {(row["instrument_id"], row["symbol"]) for row in rows}

    assert pairs == {("instrument-btc", "BTC"), ("instrument-eth", "ETH")}
    assert len(rows) == 2
    assert all(row["continuity_status"] == "clean" for row in rows)
    assert {row["candle_count"] for row in rows} == {8, 10}


def test_candle_catalog_prefers_storage_continuity_over_run_gap_diagnostics(monkeypatch: pytest.MonkeyPatch) -> None:
    dataset = _build(
        monkeypatch,
        candle_summaries={
            ("instrument-btc", "1h"): {
                "candle_count": 700,
                "gap_count": 0,
                "missing_count": 0,
                "available_resolutions": ["1m", "1h"],
            },
        },
    )

    btc = next(row for row in dataset["candle_catalog"]["items"] if row["instrument_id"] == "instrument-btc")

    assert btc["candle_count"] == 700
    assert btc["gap_count"] == 0
    assert btc["missing_count"] == 0
    assert btc["continuity_status"] == "clean"
    assert btc["storage_source"] == "market_candles_raw"


def test_readiness_data_quality_unknown_when_candle_continuity_is_unknown(monkeypatch: pytest.MonkeyPatch) -> None:
    events = [row for row in _events() if _event_name(row) != "candle_continuity_summary"]

    dataset = _build(monkeypatch, events=events)

    assert dataset["readiness"]["data_quality_status"] == "unknown"
    assert "candle_continuity_catalog_unavailable" in dataset["readiness"]["caveats"]


def test_dataset_includes_per_symbol_results(monkeypatch: pytest.MonkeyPatch) -> None:
    dataset = _build(monkeypatch)

    per_symbol = {row["symbol"]: row for row in dataset["strategy_insights"]["per_symbol_performance"]}
    assert per_symbol["BTC"]["trades"] == 2
    assert per_symbol["BTC"]["net_pnl"] == pytest.approx(35.0)
    assert per_symbol["ETH"]["net_pnl"] == pytest.approx(18.0)


def test_dataset_includes_close_reason_breakdown(monkeypatch: pytest.MonkeyPatch) -> None:
    dataset = _build(monkeypatch)

    by_reason = {row["close_reason"]: row for row in dataset["strategy_insights"]["close_reason_breakdown"]}
    assert by_reason["STOP"]["trades"] == 1
    assert by_reason["STOP"]["net_pnl"] == pytest.approx(-55.0)
    assert by_reason["TARGET"]["net_pnl"] == pytest.approx(90.0)


def test_dataset_includes_fee_and_pnl_accounting_checks(monkeypatch: pytest.MonkeyPatch) -> None:
    dataset = _build(monkeypatch)

    checks = dataset["fee_accounting"]["fee_sanity_checks"]
    assert checks["fees_non_negative"] is True
    assert checks["net_equals_gross_minus_fees"] is True
    assert checks["total_fees"] == pytest.approx(17.0)
    assert dataset["fee_accounting"]["suspicious_fee_outliers"] == []


def test_dataset_includes_botlens_rebuildable_snapshot_caveat(monkeypatch: pytest.MonkeyPatch) -> None:
    dataset = _build(monkeypatch)

    caveats = set(dataset["readiness"]["caveats"])
    assert "botlens_snapshots_rebuildable_from_material_event_ledger_and_compact_context" in caveats


def test_botlens_projection_failure_blocks_golden_candidate(monkeypatch: pytest.MonkeyPatch) -> None:
    dataset = _build(
        monkeypatch,
        observability_events=[
            {
                "level": "ERROR",
                "event_name": "run_projector_failed",
                "component": "botlens_run_projector",
                "message": "batch_apply_failed",
                "failure_mode": "batch_apply_failed",
                "bot_id": "bot-1",
                "details": {
                    "error": "botlens_run_projection_invalid: completed run retains open trades trade_ids=trade-stale",
                },
            }
        ],
    )

    diagnostic = next(item for item in dataset["diagnostics"]["items"] if item["code"] == "run_projector_failed")
    assert diagnostic["severity"] == "warning"
    assert diagnostic["readiness_impact"] == "blocks_golden"
    assert dataset["readiness"]["safe_to_compare"] is True
    assert dataset["readiness"]["golden_candidate_status"] == "blocked"
    assert "run_projector_failed" in dataset["diagnostics"]["summary"]["blocking_codes"]


def test_projection_truth_mismatch_detects_closed_canonical_trade(monkeypatch: pytest.MonkeyPatch) -> None:
    dataset = _build(
        monkeypatch,
        observability_events=[
            {
                "level": "ERROR",
                "event_name": "run_projector_failed",
                "component": "botlens_run_projector",
                "message": "batch_apply_failed",
                "failure_mode": "batch_apply_failed",
                "bot_id": "bot-1",
                "observed_at": "2026-04-01T00:10:00Z",
                "details": {
                    "error": "botlens_run_projection_invalid: completed run retains open trades trade_ids=trade-1",
                },
            }
        ],
    )

    mismatch = next(item for item in dataset["diagnostics"]["items"] if item["code"] == "projection_truth_mismatch")

    assert mismatch["readiness_impact"] == "blocks_golden"
    assert mismatch["affected_identity"]["trade_ids"] == ["trade-1"]
    assert "projection_truth_mismatch" in dataset["diagnostics"]["summary"]["blocking_codes"]


def test_projection_replay_resolution_downgrades_prior_projection_blockers(monkeypatch: pytest.MonkeyPatch) -> None:
    dataset = _build(
        monkeypatch,
        observability_events=[
            {
                "level": "WARN",
                "event_name": "run_notification_queue_overflow",
                "component": "botlens_symbol_projector",
                "message": "queue full",
                "observed_at": "2026-04-01T00:09:00Z",
                "details": {"replay_required": True},
            },
            {
                "level": "ERROR",
                "event_name": "run_projector_failed",
                "component": "botlens_run_projector",
                "message": "batch_apply_failed",
                "failure_mode": "batch_apply_failed",
                "bot_id": "bot-1",
                "observed_at": "2026-04-01T00:10:00Z",
                "details": {
                    "error": "botlens_run_projection_invalid: completed run retains open trades trade_ids=trade-1",
                },
            },
            {
                "level": "WARN",
                "event_name": "run_projector_reconciled",
                "component": "botlens_run_projector",
                "message": "replayed from canonical events",
                "observed_at": "2026-04-01T00:11:00Z",
                "details": {
                    "open_trade_count": 0,
                    "projection_state": "reconciled",
                    "replay_required": False,
                },
            },
        ],
    )

    blocking_codes = set(dataset["diagnostics"]["summary"]["blocking_codes"])

    assert "run_projector_failed" not in blocking_codes
    assert "run_notification_queue_overflow" not in blocking_codes
    assert "projection_truth_mismatch" not in blocking_codes
    assert "projection_replay_resolved" in dataset["readiness"]["caveats"]


def test_missing_runtime_ordering_blocks_golden_candidate(monkeypatch: pytest.MonkeyPatch) -> None:
    events = _events()
    for row in events:
        row.pop("run_seq", None)
        row.pop("run_seq_status", None)
        row["payload"]["context"].pop("run_seq", None)
        row["payload"]["context"].pop("run_seq_status", None)

    dataset = _build(monkeypatch, events=events)

    assert dataset["readiness"]["golden_candidate_status"] == "blocked"
    assert "runtime_ordering_unavailable" in dataset["diagnostics"]["summary"]["blocking_codes"]
    assert "runtime_ordering_unavailable" in dataset["readiness"]["golden_blocking_reasons"]


def test_duplicate_runtime_ordering_blocks_golden_candidate(monkeypatch: pytest.MonkeyPatch) -> None:
    events = _events()
    events[1]["run_seq"] = events[0]["run_seq"]
    events[1]["payload"]["context"]["run_seq"] = events[0]["payload"]["context"]["run_seq"]

    dataset = _build(monkeypatch, events=events)

    assert "runtime_ordering_inconsistent" in dataset["diagnostics"]["summary"]["blocking_codes"]
    assert "runtime_ordering_inconsistent" in dataset["readiness"]["golden_blocking_reasons"]


def test_backfilled_runtime_ordering_is_caveated_without_ordering_blocker(monkeypatch: pytest.MonkeyPatch) -> None:
    events = _events()
    for row in events:
        row["run_seq_status"] = "backfilled"
        row["payload"]["context"]["run_seq_status"] = "backfilled"

    dataset = _build(monkeypatch, events=events)

    assert "runtime_ordering_backfilled" in dataset["readiness"]["caveats"]
    assert "runtime_ordering_backfilled" not in dataset["diagnostics"]["summary"]["blocking_codes"]
    assert "runtime_ordering_backfilled" not in dataset["readiness"]["golden_blocking_reasons"]


def test_missing_wallet_decision_trace_blocks_golden_candidate(monkeypatch: pytest.MonkeyPatch) -> None:
    events = _events()
    for row in events:
        if _event_name(row) == "DECISION_EMITTED":
            row["payload"]["context"].pop("wallet_snapshot", None)

    dataset = _build(monkeypatch, events=events)

    assert "wallet_decision_trace_incomplete" in dataset["readiness"]["caveats"]
    assert "wallet_decision_trace_incomplete" in dataset["diagnostics"]["summary"]["blocking_codes"]
    assert "wallet_decision_trace_incomplete" in dataset["readiness"]["golden_blocking_reasons"]


def test_malformed_wallet_replay_blocks_golden_candidate(monkeypatch: pytest.MonkeyPatch) -> None:
    events = _events()
    events.append(
        _event(
            14,
            "WALLET_INITIALIZED",
            {
                "currency": "USD",
            },
        )
    )

    dataset = _build(monkeypatch, events=events)

    assert dataset["wallet_accounting"]["wallet_replay_status"] == "failed"
    assert "wallet_replay_failed" in dataset["readiness"]["caveats"]
    assert "wallet_replay_failed" in dataset["diagnostics"]["summary"]["blocking_codes"]
    assert "wallet_replay_failed" in dataset["readiness"]["golden_blocking_reasons"]


def test_incomplete_margin_rejection_trace_blocks_golden_candidate(monkeypatch: pytest.MonkeyPatch) -> None:
    events = _events()
    events.append(
        _event(
            14,
            "WALLET_INITIALIZED",
            {
                "currency": "USD",
                "wallet_commit_seq": 0,
                "wallet_event_order": 0,
                "balance_after": 1000.0,
                "wallet_after": {"balances": {"USD": 1000.0}},
            },
        )
    )
    events.append(
        _event(
            15,
            "MARGIN_REJECTED",
            {
                "decision_id": "decision-4",
                "reason": "WALLET_INSUFFICIENT_MARGIN",
                "currency": "USD",
                "wallet_commit_seq": 1,
                "wallet_event_order": 10,
                "margin_required": 0.0,
                "margin_available": None,
                "balance_before": None,
            },
        )
    )

    dataset = _build(monkeypatch, events=events)

    assert dataset["wallet_diagnostics"]["margin_rejection_trace_complete"] is False
    assert "wallet_margin_rejection_trace_incomplete" in dataset["readiness"]["caveats"]
    assert "margin_rejection_evidence_incomplete" in dataset["readiness"]["caveats"]
    assert "wallet_margin_rejection_trace_incomplete" in dataset["diagnostics"]["summary"]["blocking_codes"]
    assert "margin_rejection_evidence_incomplete" in dataset["diagnostics"]["summary"]["blocking_codes"]


def test_wallet_ledger_state_mismatch_blocks_golden_candidate(monkeypatch: pytest.MonkeyPatch) -> None:
    events = _events()
    events.append(
        _event(
            14,
            "WALLET_INITIALIZED",
            {
                "currency": "USD",
                "wallet_commit_seq": 0,
                "wallet_event_order": 0,
                "balance_after": 1000.0,
                "wallet_after": {"balances": {"USD": 1000.0}, "free_collateral": {"USD": 1000.0}},
            },
        )
    )
    events.append(
        _event(
            15,
            "FEE_APPLIED",
            {
                "currency": "USD",
                "wallet_commit_seq": 1,
                "wallet_event_order": 20,
                "balance_before": 0.0,
                "balance_after": 0.0,
                "fee": 1.0,
                "wallet_before": {"balances": {"USD": 1000.0}, "free_collateral": {"USD": 1000.0}},
                "wallet_after": {"balances": {"USD": 0.0}, "free_collateral": {"USD": 0.0}},
            },
        )
    )

    dataset = _build(monkeypatch, events=events)

    assert dataset["wallet_accounting"]["wallet_replay_status"] == "failed"
    assert "wallet_ledger_state_mismatch" in dataset["readiness"]["caveats"]
    assert "wallet_ledger_state_mismatch" in dataset["diagnostics"]["summary"]["blocking_codes"]


def test_operational_fingerprint_changes_when_runtime_event_order_changes(monkeypatch: pytest.MonkeyPatch) -> None:
    baseline = _build(monkeypatch, events=_events())
    reordered_events = _events()
    for row in reordered_events:
        if row.get("decision_id") == "decision-1":
            row["run_seq"] = 2
            row["payload"]["context"]["run_seq"] = 2
        elif row.get("decision_id") == "decision-2":
            row["run_seq"] = 1
            row["payload"]["context"]["run_seq"] = 1

    reordered = _build(monkeypatch, events=reordered_events)

    assert reordered["readiness"]["material_fingerprint"] == baseline["readiness"]["material_fingerprint"]
    assert reordered["readiness"]["semantic_fingerprint"] == baseline["readiness"]["semantic_fingerprint"]
    assert reordered["readiness"]["operational_fingerprint"] != baseline["readiness"]["operational_fingerprint"]


def test_material_fingerprint_changes_when_runtime_context_evidence_changes(monkeypatch: pytest.MonkeyPatch) -> None:
    def signal_event(*, bias: str) -> dict[str, Any]:
        return _event(
            1,
            "SIGNAL_EMITTED",
            {
                "signal_id": "signal-1",
                "decision_id": "decision-1",
                "rule_id": "rule-breakout",
                "signal_type": "strategy_signal",
                "direction": "long",
                "intent": "enter_long",
                "event_key": "breakout_long",
                "signal_price": 100.0,
                "bar_time": "2026-03-05T00:00:00Z",
                "decision_artifact": {
                    "decision_id": "decision-1",
                    "referenced_outputs": {
                        "ind-1.market_state": {
                            "output_ref": "ind-1.market_state",
                            "indicator_id": "ind-1",
                            "output_name": "market_state",
                            "type": "context",
                            "output_type": "context",
                            "ready": True,
                            "bar_time": "2026-03-05T00:00:00Z",
                            "indicator_commit_seq": 13,
                            "indicator_commit_seq_status": "indicator_scoped",
                            "state_key": "trend",
                            "fields": {"bias": bias, "state": "trend"},
                        },
                    },
                },
            },
        )

    baseline = _build(monkeypatch, events=[signal_event(bias="long")], trades=[])
    changed = _build(monkeypatch, events=[signal_event(bias="short")], trades=[])

    assert changed["readiness"]["material_fingerprint"] != baseline["readiness"]["material_fingerprint"]


def test_semantic_fingerprint_ignores_run_instance_identifiers(monkeypatch: pytest.MonkeyPatch) -> None:
    baseline = _build(monkeypatch, events=_events(), trades=_trades())
    changed_events = copy.deepcopy(_events())
    for row in changed_events:
        context = row.get("payload", {}).get("context", {})
        if context.get("signal_id"):
            context["signal_id"] = f"run-two-{context['signal_id']}"
            row["signal_id"] = context["signal_id"]
        if context.get("trade_id"):
            context["trade_id"] = f"run-two-{context['trade_id']}"
            row["trade_id"] = context["trade_id"]
    changed_trades = copy.deepcopy(_trades())
    for row in changed_trades:
        row["id"] = f"run-two-{row['id']}"

    changed = _build(monkeypatch, events=changed_events, trades=changed_trades)

    assert changed["readiness"]["semantic_fingerprint"] == baseline["readiness"]["semantic_fingerprint"]
    assert changed["readiness"]["material_fingerprint"] == baseline["readiness"]["material_fingerprint"]
    assert changed["metadata"]["report_semantic_fingerprint"] == baseline["metadata"]["report_semantic_fingerprint"]
    assert changed["readiness"]["operational_fingerprint"] != baseline["readiness"]["operational_fingerprint"]
    assert changed["metadata"]["report_operational_fingerprint"] != baseline["metadata"]["report_operational_fingerprint"]


def test_data_snapshot_hash_changes_when_candle_gap_window_changes(monkeypatch: pytest.MonkeyPatch) -> None:
    baseline_events = [row for row in _events() if _event_name(row) != "candle_continuity_summary"]
    changed_events = [row for row in _events() if _event_name(row) != "candle_continuity_summary"]
    baseline_events.append(_provider_gap(20))
    changed_gap = _provider_gap(20)
    changed_gap["payload"]["context"]["gaps"][0]["previous_ts"] = "2026-03-07T00:00:00Z"
    changed_gap["payload"]["context"]["gaps"][0]["current_ts"] = "2026-03-07T02:00:00Z"
    changed_events.append(changed_gap)

    baseline = _build(monkeypatch, events=baseline_events)
    changed = _build(monkeypatch, events=changed_events)

    assert changed["metadata"]["data_snapshot_hash"] != baseline["metadata"]["data_snapshot_hash"]


def test_observer_continuity_facts_do_not_change_material_identity(monkeypatch: pytest.MonkeyPatch) -> None:
    baseline = _build(monkeypatch, events=_events(), observability_events=[])
    observed = _build(
        monkeypatch,
        events=_events(),
        observability_events=[
            _observer_gap(boundary_name="selected_symbol_snapshot"),
            _observer_gap(
                boundary_name="run_bootstrap_selected_symbol",
                pipeline_stage="botlens_run_bootstrap_snapshot",
            ),
        ],
    )

    assert observed["metadata"]["data_snapshot_hash"] == baseline["metadata"]["data_snapshot_hash"]
    assert observed["readiness"]["semantic_fingerprint"] == baseline["readiness"]["semantic_fingerprint"]
    assert observed["readiness"]["material_fingerprint"] == baseline["readiness"]["material_fingerprint"]
    assert observed["readiness"]["golden_candidate_status"] == baseline["readiness"]["golden_candidate_status"]
    assert observed["readiness"]["golden_blocking_reasons"] == baseline["readiness"]["golden_blocking_reasons"]
    assert observed["candle_gaps"]["noncanonical_fact_count"] == 2
    assert all(row["evidence_scope"] == "diagnostic_observer" for row in observed["candle_gaps"]["diagnostic_facts"])
    assert all(row["boundary_name"] == "run_final" for row in observed["candle_gaps"]["facts"])


def test_missing_run_final_continuity_fails_without_certifying_from_observer_facts(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events = [row for row in _events() if _event_name(row) != "candle_continuity_summary"]

    dataset = _build(monkeypatch, events=events, observability_events=[_observer_gap()])

    assert dataset["candle_gaps"]["canonical_evidence_status"] == "missing"
    assert dataset["candle_gaps"]["facts"] == []
    assert dataset["candle_gaps"]["diagnostic_facts"]
    assert "missing_canonical_continuity_evidence" in dataset["readiness"]["caveats"]
    assert "missing_canonical_continuity_evidence" in dataset["readiness"]["golden_blocking_reasons"]
    assert "missing_canonical_continuity_evidence" in dataset["diagnostics"]["summary"]["blocking_codes"]
    assert dataset["readiness"]["golden_candidate_status"] == "blocked"


def test_lifecycle_failure_and_completion_blocks_golden_candidate(monkeypatch: pytest.MonkeyPatch) -> None:
    events = [
        _event(
            14,
            "RUN_FAILED",
            {
                "bar_time": None,
                "failure": {"reason_code": "stale_heartbeat"},
                "status": "crashed",
            },
        ),
        *_events(),
        _event(99, "RUN_COMPLETED", {"bar_time": None, "status": "completed"}),
    ]

    dataset = _build(monkeypatch, events=events)

    assert dataset["readiness"]["safe_to_compare"] is True
    assert dataset["readiness"]["golden_candidate_status"] == "blocked"
    assert "lifecycle_contradiction" in dataset["readiness"]["golden_blocking_reasons"]
    assert "lifecycle_contradiction" in dataset["diagnostics"]["summary"]["blocking_codes"]


def test_unclassified_fault_and_completion_blocks_golden_candidate(monkeypatch: pytest.MonkeyPatch) -> None:
    events = [
        _event(
            14,
            "FAULT_RECORDED",
            {
                "bar_time": None,
                "fault_code": "runtime_fault",
                "severity": "ERROR",
                "message": "Unclassified runtime fault.",
                "source": "runtime",
            },
        ),
        *_events(),
        _event(99, "RUN_COMPLETED", {"bar_time": None, "status": "completed"}),
    ]

    dataset = _build(monkeypatch, events=events)

    assert dataset["readiness"]["golden_candidate_status"] == "blocked"
    assert "lifecycle_contradiction" in dataset["readiness"]["golden_blocking_reasons"]
    assert "lifecycle_contradiction" in dataset["diagnostics"]["summary"]["blocking_codes"]


def test_recoverable_watchdog_stale_heartbeat_degrades_without_lifecycle_contradiction(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events = [
        _event(
            14,
            "FAULT_RECORDED",
            {
                "bar_time": None,
                "fault_code": "stale_heartbeat",
                "severity": "WARN",
                "message": "Recoverable watchdog stale heartbeat observed: stale_heartbeat:prev=backend.quanttrad",
                "source": "lifecycle",
                "component": "watchdog",
                "failure_type": "watchdog_stale_heartbeat",
                "reason_code": "stale_heartbeat",
                "reason": "stale_heartbeat:prev=backend.quanttrad",
                "recoverable": True,
            },
        ),
        *_events(),
        _event(99, "RUN_COMPLETED", {"bar_time": None, "status": "completed"}),
    ]

    dataset = _build(monkeypatch, events=events)

    codes = {item["code"]: item for item in dataset["diagnostics"]["items"]}
    assert "recoverable_watchdog_stale_heartbeat" in codes
    assert codes["recoverable_watchdog_stale_heartbeat"]["readiness_impact"] == "degrades_diagnostics"
    assert "lifecycle_contradiction" not in dataset["readiness"]["golden_blocking_reasons"]
    assert "lifecycle_contradiction" not in dataset["diagnostics"]["summary"]["blocking_codes"]


def test_recoverable_watchdog_startup_ambiguity_degrades_without_lifecycle_contradiction(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events = [
        _event(
            14,
            "FAULT_RECORDED",
            {
                "bar_time": None,
                "fault_code": "startup_container_ambiguous",
                "severity": "WARN",
                "message": "Recoverable watchdog startup ambiguity observed.",
                "source": "lifecycle",
                "component": "watchdog",
                "failure_type": "watchdog_startup_container_ambiguous",
                "reason_code": "startup_container_ambiguous",
                "reason": "startup_container_ambiguous:old_run_container",
                "recoverable": True,
            },
        ),
        *_events(),
        _event(99, "RUN_COMPLETED", {"bar_time": None, "status": "completed"}),
    ]

    dataset = _build(monkeypatch, events=events)

    codes = {item["code"]: item for item in dataset["diagnostics"]["items"]}
    assert "recoverable_watchdog_startup_ambiguity" in codes
    assert codes["recoverable_watchdog_startup_ambiguity"]["readiness_impact"] == "degrades_diagnostics"
    assert "lifecycle_contradiction" not in dataset["readiness"]["golden_blocking_reasons"]
    assert "lifecycle_contradiction" not in dataset["diagnostics"]["summary"]["blocking_codes"]


def test_candle_gap_diagnostics_skip_zero_gap_identity_rows(monkeypatch: pytest.MonkeyPatch) -> None:
    events = [
        *_events(),
        _event(
            20,
            "candle_continuity_summary",
            {
                "series_key": "68694ebd-dfa6-4757-99da-9b3a2b4f4aa8|1h",
                "detected_gap_count": 0,
                "gap_count_by_type": {"unknown_gap": 0},
            },
            event_type="observability",
        ),
    ]

    dataset = _build(monkeypatch, events=events)

    symbols = [row["symbol"] for row in dataset["candle_gaps"]["gap_counts_by_symbol"]]
    assert "68694ebd-dfa6-4757-99da-9b3a2b4f4aa8|1h" not in symbols


def test_provider_sparse_candle_gaps_degrade_without_golden_blocker(monkeypatch: pytest.MonkeyPatch) -> None:
    events = [row for row in _events() if _event_name(row) != "candle_continuity_summary"]
    events.append(_provider_gap(20))

    dataset = _build(monkeypatch, events=events)

    btc_catalog = next(row for row in dataset["candle_catalog"]["items"] if row.get("symbol") == "BTC")
    assert btc_catalog["continuity_status"] == "source_sparse"
    assert btc_catalog["readiness_impact"] == "degrades_metrics"
    assert btc_catalog["provider_gap_count"] == 2
    assert btc_catalog["first_gap_evidence"]["reason_code"] == "provider_response_empty"
    assert dataset["readiness"]["data_quality_status"] == "degraded"
    assert "candle_continuity_provider_sparse" in dataset["readiness"]["caveats"]
    assert "candle_continuity_degraded" not in dataset["readiness"]["golden_blocking_reasons"]
    diagnostics = {item["code"]: item for item in dataset["diagnostics"]["items"]}
    first_gap = diagnostics["candle_gaps_detected"]["affected_identity"]["first_gap_evidence"][0]["gap"]
    assert first_gap["provider_evidence"]["provider_message"] == "exchange returned no candle"


def test_unknown_candle_gaps_still_block_golden_candidate(monkeypatch: pytest.MonkeyPatch) -> None:
    events = [row for row in _events() if _event_name(row) != "candle_continuity_summary"]
    events.append(_unknown_gap(20))

    dataset = _build(monkeypatch, events=events)

    btc_catalog = next(row for row in dataset["candle_catalog"]["items"] if row.get("symbol") == "BTC")
    assert btc_catalog["continuity_status"] == "degraded"
    assert btc_catalog["readiness_impact"] == "blocks_golden"
    assert "candle_continuity_degraded" in dataset["readiness"]["golden_blocking_reasons"]


def test_unknown_candle_gaps_reclassify_from_closure_evidence(monkeypatch: pytest.MonkeyPatch) -> None:
    events = [row for row in _events() if _event_name(row) != "candle_continuity_summary"]
    events.append(_unknown_gap(20))

    dataset = _build(
        monkeypatch,
        events=events,
        candle_closures={
            ("instrument-btc", "1h"): [
                {
                    "start": "2026-03-06T22:00:00Z",
                    "end": "2026-03-06T23:00:00Z",
                    "metadata": {
                        "reason_code": "provider_response_empty",
                        "evidence": "provider_api_empty_response",
                        "provider_evidence": {"provider_message": "no candles returned"},
                    },
                }
            ]
        },
    )

    btc_catalog = next(row for row in dataset["candle_catalog"]["items"] if row.get("symbol") == "BTC")
    assert btc_catalog["continuity_status"] == "source_sparse"
    assert btc_catalog["provider_gap_count"] == 1
    assert btc_catalog["blocking_gap_count"] == 0
    assert btc_catalog["first_gap_evidence"]["classification"] == "provider_missing_data"
    assert "candle_continuity_degraded" not in dataset["readiness"]["golden_blocking_reasons"]
    assert "candle_continuity_provider_sparse" in dataset["readiness"]["caveats"]


def test_candle_gap_symbol_resolves_from_series_key_metadata(monkeypatch: pytest.MonkeyPatch) -> None:
    events = [row for row in _events() if _event_name(row) != "candle_continuity_summary"]
    events.append(_provider_gap(20, include_identity=False))

    dataset = _build(monkeypatch, events=events)

    symbols = [row["symbol"] for row in dataset["candle_gaps"]["gap_counts_by_symbol"]]
    assert "BTC" in symbols
    assert "UNKNOWN" not in symbols


def test_narrative_summary_contains_major_caveats_and_strategy_insights(monkeypatch: pytest.MonkeyPatch) -> None:
    dataset = _build(monkeypatch)

    context = dataset["narrative_summary"]
    assert "Caveats" in context
    assert "STOP" in context
    assert "FULL-mode intrabar fallbacks: 2" in context
    assert "Recommended Next Research Actions" in context
