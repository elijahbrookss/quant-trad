from __future__ import annotations

import copy
import logging
from typing import Any

import pytest

from engines.bot_runtime.core.execution_assumptions import resolve_execution_assumptions
from engines.bot_runtime.core.execution_context import (
    build_execution_context_bundle,
    execution_model_artifact_from_book_tape,
    resolve_execution_context,
)
from engines.bot_runtime.core.execution_profile import compile_series_execution_profile
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
        candle_provider_gaps: dict[tuple[str, str], list[dict[str, Any]]] | None = None,
        candles: dict[tuple[str, str], list[dict[str, Any]]] | None = None,
    ) -> None:
        self._run = dict(run)
        self._events = sorted([dict(row) for row in events], key=lambda row: int(row.get("seq") or 0))
        self._trades = [dict(row) for row in trades]
        self._steps = [dict(row) for row in steps or []]
        self._observability_events = [dict(row) for row in observability_events or []]
        self._candle_summaries = {tuple(key): dict(value) for key, value in (candle_summaries or {}).items()}
        self._candle_provider_gaps = {
            tuple(key): [dict(row) for row in value]
            for key, value in (candle_provider_gaps or {}).items()
        }
        self._candles = {
            tuple(key): [dict(row) for row in value]
            for key, value in (candles or {}).items()
        }
        self.candle_window_calls: list[dict[str, Any]] = []

    def get_bot_run(self, run_id: str):
        return dict(self._run) if run_id == self._run.get("run_id") else None

    def list_bot_trades_for_run(self, run_id: str):
        _ = run_id
        return [dict(row) for row in self._trades]

    def list_bot_run_steps_for_run(self, run_id: str):
        _ = run_id
        return [dict(row) for row in self._steps]

    def list_bot_run_lifecycle_events(self, run_id: str):
        _ = run_id
        return []

    def list_observability_events(self, run_id: str, limit: int = 2000):
        _ = run_id
        return [dict(row) for row in self._observability_events[:limit]]

    def get_candle_storage_summary(self, *, instrument_id: str, timeframe: str, start, end):
        _ = start, end
        summary = self._candle_summaries.get((instrument_id, timeframe))
        return dict(summary) if summary else None

    def list_candle_provider_gap_evidence(self, *, instrument_id: str, timeframe: str, start, end):
        _ = start, end
        return [dict(row) for row in self._candle_provider_gaps.get((instrument_id, timeframe), [])]

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

    def list_candles_for_series_windows(
        self,
        *,
        instrument_id: str,
        timeframe: str,
        windows,
    ):
        self.candle_window_calls.append(
            {
                "instrument_id": instrument_id,
                "timeframe": timeframe,
                "windows": [dict(window) for window in windows],
            }
        )
        source = self._candles.get((instrument_id, timeframe), [])
        return {
            str(window["window_id"]): [
                dict(row)
                for row in source[: int(window.get("limit") or 2000)]
            ]
            for window in windows
        }

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
            "dataset_binding": {
                "dataset_id": "mds_test",
                "dataset_hash": "a" * 64,
                "dataset_contract_version": "market_data_dataset.v1",
            },
            "backtest_warmup_bars": 100,
            "backtest_warmup_evidence": [
                {
                    "schema_version": "backtest_warmup_evidence.v1",
                    "strategy_id": "strategy-1",
                    "symbol": "BTC",
                    "timeframe": "1h",
                    "status": "ready",
                    "requested_bars": 100,
                    "required_bars": 100,
                    "loaded_bars": 100,
                    "missing_bars": 0,
                }
            ],
            "wallet_start": {"balances": {"USDC": 1000}},
            "date_range": {
                "start": "2026-03-01T00:00:00Z",
                "end": "2026-03-31T00:00:00Z",
            },
            "symbols": ["BTC", "ETH"],
            "timeframe": "1h",
            "material_config_hash": "material-1",
            "risk_settings": {"risk_per_trade": 0.01, "slippage_bps": 0.0},
            "strategies": [
                {
                    "id": "strategy-1",
                    "atm_template_id": "atm-1",
                    "atm_template": {
                        "schema_version": 2,
                        "name": "Research fixture ATM",
                        "take_profit_orders": [
                            {"id": "tp-1", "r_multiple": 1.0, "size_fraction": 1.0}
                        ],
                    },
                }
            ],
            "indicators": [{"id": "ind-1", "type": "market_profile"}],
            "indicator_source_diagnostics": [],
        },
    }


def _source_diagnostic(acceptability: str) -> dict[str, Any]:
    source_status = "ok" if acceptability == "accepted" else "warning"
    final_status = (
        "healthy"
        if acceptability == "accepted"
        else "source_sparse"
        if acceptability == "acceptable_with_caveat"
        else "degraded"
    )
    return {
        "strategy_id": "strategy-1",
        "instrument_id": "instrument-btc",
        "symbol": "BTC",
        "timeframe": "1h",
        "datasource": "coinbase",
        "exchange": "CBI",
        "indicator_id": "ind-1",
        "indicator_type": "market_profile",
        "source_candle_continuity": {
            "schema_version": "indicator_source_candle_continuity.v1",
            "timeframe": "5m",
            "row_count": 10,
            "status": source_status,
            "severity": source_status,
            "acceptability": acceptability,
            "message": f"source continuity is {acceptability}",
            "continuity": {
                "candle_count": 10,
                "final_status": final_status,
            },
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


def test_execution_section_projects_order_lifecycle_residuals_and_fill_parent() -> None:
    lifecycle_event = _event(
        80,
        "ORDER_LIFECYCLE_CHANGED",
        {
            "event_time": "2026-03-15T01:00:00Z",
            "bar_time": "2026-03-15T01:00:00Z",
            "known_at": "2026-03-15T01:00:00Z",
            "trade_id": "trade-1",
            "signal_id": "signal-1",
            "decision_id": "decision-1",
            "order_request_id": "order-1",
            "order_request_manifest_hash": "request-hash",
            "attempt_id": "attempt-1",
            "order_attempt_manifest_hash": "attempt-hash",
            "order_event_seq": 4,
            "previous_state": "open",
            "state": "partially_filled",
            "side": "buy",
            "requested_qty": 10.0,
            "attempt_requested_qty": 10.0,
            "attempt_cumulative_filled_qty": 4.0,
            "attempt_remaining_qty": 6.0,
            "order_cumulative_filled_qty": 4.0,
            "order_remaining_qty": 6.0,
            "execution_context_hash": "context-hash",
            "execution_policy_hash": "policy-hash",
            "order_lifecycle_replay_hash": "replay-prefix-hash",
            "fill_id": "fill-1",
            "fill_qty": 4.0,
            "fill_price": 100.0,
            "fill_fee": 0.04,
            "venue_event_name": "open",
        },
    )
    fill_event = _event(
        81,
        "ENTRY_FILLED",
        {
            "bar_time": "2026-03-15T01:00:00Z",
            "trade_id": "trade-1",
            "side": "buy",
            "direction": "long",
            "qty": 4.0,
            "price": 100.0,
            "notional": 400.0,
            "fee_paid": 0.04,
        },
    )
    fill_event["payload"]["parent_id"] = lifecycle_event["event_id"]

    execution = run_research_dataset._execution_section(
        run=_run(),
        events=[lifecycle_event, fill_event],
    )

    assert execution["order_lifecycle"]["event_count"] == 1
    assert execution["order_lifecycle"]["order_count"] == 1
    assert execution["order_lifecycle"]["open_order_count"] == 1
    assert execution["order_lifecycle"]["state_distribution"] == {"partially_filled": 1}
    assert execution["order_lifecycle"]["latest_orders"][0]["order_remaining_qty"] == 6.0
    assert execution["fills"][0]["order_lifecycle_event_id"] == lifecycle_event["event_id"]


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
    for name in (
        "get_bot_run",
        "list_bot_trades_for_run",
        "list_bot_run_steps_for_run",
        "list_bot_run_lifecycle_events",
        "list_observability_events",
        "get_candle_storage_summary",
        "list_candle_provider_gap_evidence",
        "list_candles_for_series_windows",
    ):
        monkeypatch.setattr(run_research_dataset, name, getattr(storage, name))
    monkeypatch.setattr(report_data, "get_bot_run", storage.get_bot_run)
    monkeypatch.setattr(
        report_data,
        "list_bot_runtime_events",
        storage.list_bot_runtime_events,
    )
    monkeypatch.setattr(
        report_data,
        "list_bot_trades_for_run",
        storage.list_bot_trades_for_run,
    )
    monkeypatch.setattr(
        report_data,
        "list_observability_event_rows",
        storage.list_observability_events,
    )


def _build(
    monkeypatch: pytest.MonkeyPatch,
    *,
    run=None,
    events=None,
    trades=None,
    steps=None,
    observability_events=None,
    candle_summaries=None,
    candle_provider_gaps=None,
    candles=None,
):
    fake_storage = _ResearchDatasetStorage(
        run=_run() if run is None else run,
        events=_events() if events is None else events,
        trades=_trades() if trades is None else trades,
        steps=_steps() if steps is None else steps,
        observability_events=observability_events,
        candle_summaries=candle_summaries,
        candle_provider_gaps=candle_provider_gaps,
        candles=candles,
    )
    _install(monkeypatch, fake_storage)
    return run_research_dataset.build_run_research_dataset("run-1")


def test_dataset_builds_from_db_truth_without_artifact_directory(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    with caplog.at_level(logging.INFO, logger=run_research_dataset.__name__):
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
    done_message = next(
        message
        for message in caplog.messages
        if "run_research_dataset_build_done" in message
    )
    for field in (
        "duration_ms=",
        "source_load_ms=",
        "trade_enrichment_ms=",
        "readiness_ms=",
        "wallet_accounting_ms=",
        "observability_ms=",
        "assembly_ms=",
        "serialization_ms=",
        "wallet_events=",
    ):
        assert field in done_message


def test_runtime_step_timings_use_weighted_average_and_merged_histogram(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    steps = [
        {
            "step_name": "runtime_loop",
            "sample_count": 2,
            "value_sum": 30.0,
            "value_max": 20.0,
            "p95_value": 20.0,
            "histogram_bounds": [10.0, 20.0, 50.0],
            "histogram_counts": [1, 1, 0],
        },
        {
            "step_name": "runtime_loop",
            "sample_count": 3,
            "value_sum": 90.0,
            "value_max": 40.0,
            "p95_value": 40.0,
            "histogram_bounds": [10.0, 20.0, 50.0],
            "histogram_counts": [0, 1, 2],
        },
    ]

    dataset = _build(monkeypatch, steps=steps)
    timing = dataset["performance"]["major_step_timings"][0]

    assert timing["step_name"] == "runtime_loop"
    assert timing["count"] == 5
    assert timing["total_ms"] == 120.0
    assert timing["avg_ms"] == 24.0
    assert timing["p95_ms"] == 40.0
    assert timing["p95_method"] == "merged_histogram_upper_bound"


@pytest.mark.parametrize(
    (
        "acceptability",
        "expected_status",
        "expected_caveat",
        "expected_code",
        "expected_impact",
    ),
    [
        (
            "acceptable_with_caveat",
            "acceptable_with_caveat",
            "indicator_source_continuity_caveat",
            "indicator_source_continuity_caveat",
            "degrades_metrics",
        ),
        (
            "investigate",
            "investigate",
            "indicator_source_continuity_investigate",
            "indicator_source_continuity_investigate",
            "blocks_golden",
        ),
    ],
)
def test_dataset_propagates_indicator_source_diagnostics_into_trust_sections(
    monkeypatch: pytest.MonkeyPatch,
    acceptability: str,
    expected_status: str,
    expected_caveat: str,
    expected_code: str,
    expected_impact: str,
) -> None:
    run = _run()
    diagnostic = _source_diagnostic(acceptability)
    run["config_snapshot"]["indicator_source_diagnostics"] = [diagnostic]

    dataset = _build(monkeypatch, run=run)

    source = dataset["context"]["indicator_source_diagnostics"]
    assert source["schema_version"] == "indicator_source_diagnostics.v1"
    assert source["available"] is True
    assert source["status"] == expected_status
    assert source["items"] == [diagnostic]
    assert expected_caveat in source["caveats"]
    assert dataset["readiness"]["data_quality_status"] == "degraded"
    assert expected_caveat in dataset["readiness"]["caveats"]
    report_diagnostic = next(
        item
        for item in dataset["diagnostics"]["items"]
        if item["code"] == expected_code
    )
    assert report_diagnostic["readiness_impact"] == expected_impact
    assert (
        report_diagnostic["affected_identity"]["source_candle_continuity"]
        == diagnostic["source_candle_continuity"]
    )
    if expected_impact == "blocks_golden":
        assert expected_code in dataset["readiness"]["golden_blocking_reasons"]


def test_dataset_marks_missing_indicator_source_diagnostics_unavailable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    run = _run()
    del run["config_snapshot"]["indicator_source_diagnostics"]

    dataset = _build(monkeypatch, run=run)

    source = dataset["context"]["indicator_source_diagnostics"]
    assert source["available"] is False
    assert source["status"] == "unavailable"
    assert (
        "indicator_source_diagnostics_unavailable"
        in dataset["readiness"]["caveats"]
    )
    assert (
        "indicator_source_diagnostics_unavailable"
        in dataset["readiness"]["golden_blocking_reasons"]
    )


@pytest.mark.parametrize(
    ("mutate", "message"),
    [
        (
            lambda diagnostic: diagnostic.pop("source_candle_continuity"),
            "source_candle_continuity is required",
        ),
        (
            lambda diagnostic: diagnostic["source_candle_continuity"].update(
                {"continuity": "not-a-mapping"}
            ),
            "source_candle_continuity continuity must be a mapping",
        ),
    ],
)
def test_dataset_rejects_malformed_indicator_source_diagnostics(
    monkeypatch: pytest.MonkeyPatch,
    mutate,
    message: str,
) -> None:
    run = _run()
    diagnostic = _source_diagnostic("accepted")
    mutate(diagnostic)
    run["config_snapshot"]["indicator_source_diagnostics"] = [diagnostic]

    with pytest.raises(ValueError, match=message):
        _build(monkeypatch, run=run)


def test_dataset_surfaces_insufficient_backtest_warmup(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    run = _run()
    run["config_snapshot"]["backtest_warmup_bars"] = 100
    run["config_snapshot"]["backtest_warmup_evidence"] = [
        {
            "schema_version": "backtest_warmup_evidence.v1",
            "strategy_id": "strategy-1",
            "symbol": "BTC",
            "timeframe": "1h",
            "status": "insufficient",
            "requested_bars": 100,
            "required_bars": 100,
            "loaded_bars": 72,
            "missing_bars": 28,
        }
    ]

    dataset = _build(monkeypatch, run=run)

    data_config = dataset["metadata"]["configuration"]["data"]
    assert data_config["backtest_warmup_bars"] == 100
    assert data_config["backtest_warmup_evidence"][0]["loaded_bars"] == 72
    assert dataset["readiness"]["data_quality_status"] == "degraded"
    assert "indicator_warmup" in dataset["readiness"]["degraded_sections"]
    assert "backtest_warmup_insufficient" in dataset["readiness"]["caveats"]
    assert (
        "backtest_warmup_insufficient"
        in dataset["readiness"]["golden_blocking_reasons"]
    )


def test_loaded_warmup_evidence_is_operational_not_material_configuration() -> None:
    left = {
        "backtest_warmup_bars": 100,
        "backtest_warmup_evidence": [
            {"symbol": "BTC", "requested_bars": 100, "loaded_bars": 100}
        ],
    }
    right = {
        "backtest_warmup_bars": 100,
        "backtest_warmup_evidence": [
            {"symbol": "BTC", "requested_bars": 100, "loaded_bars": 72}
        ],
    }

    assert run_research_dataset._config_hash(left) != (
        run_research_dataset._config_hash(right)
    )
    assert run_research_dataset._material_config_hash(left) == (
        run_research_dataset._material_config_hash(right)
    )


def test_dataset_marks_missing_backtest_warmup_evidence_unavailable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    run = _run()
    run["config_snapshot"].pop("backtest_warmup_evidence")

    dataset = _build(monkeypatch, run=run)

    assert dataset["readiness"]["data_quality_status"] != "clean"
    assert "indicator_warmup" in dataset["readiness"]["degraded_sections"]
    assert (
        "backtest_warmup_evidence_unavailable"
        in dataset["readiness"]["caveats"]
    )
    assert (
        "backtest_warmup_evidence_unavailable"
        in dataset["readiness"]["golden_blocking_reasons"]
    )


def test_dataset_exposes_candidate_lifecycle_from_report_indicator_artifacts(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        run_research_dataset.report_artifacts,
        "list_run_indicator_output_rows",
        lambda *_args, **_kwargs: {
            "schema_version": "indicator_output_artifact_rows.v1",
            "run_id": "run-1",
            "available": True,
            "source_files": [{"path": "series/symbol=BTC/timeframe=1h/indicators/ind-1.csv", "rows": 2}],
            "items": [
                {
                    "run_id": "run-1",
                    "bot_id": "bot-1",
                    "symbol": "BTC",
                    "timeframe": "1h",
                    "instrument_id": "instrument-btc",
                    "bar_time": "2026-03-01T00:00:00Z",
                    "known_at": "2026-03-01T00:00:00Z",
                    "indicator_id": "ind-1",
                    "indicator_type": "generic",
                    "indicator_version": "current",
                    "output_name": "candidate_lifecycle",
                    "output_type": "lifecycle",
                    "ready": True,
                    "indicator_commit_seq": 1,
                    "indicator_commit_seq_status": "indicator_scoped",
                    "value_json": '{"events":[{"candidate_id":"candidate-1","family":"retest","side":"long","stage":"formed","status":"active","group_key":"profile-1","known_at":1761955200,"reason":"source_confirmed"}]}',
                    "source_path": "series/symbol=BTC/timeframe=1h/indicators/ind-1.csv",
                },
                {
                    "run_id": "run-1",
                    "bot_id": "bot-1",
                    "symbol": "BTC",
                    "timeframe": "1h",
                    "instrument_id": "instrument-btc",
                    "bar_time": "2026-03-01T01:00:00Z",
                    "known_at": "2026-03-01T01:00:00Z",
                    "indicator_id": "ind-1",
                    "indicator_type": "generic",
                    "indicator_version": "current",
                    "output_name": "candidate_lifecycle",
                    "output_type": "lifecycle",
                    "ready": True,
                    "indicator_commit_seq": 2,
                    "indicator_commit_seq_status": "indicator_scoped",
                    "value_json": '{"events":[{"candidate_id":"candidate-1","family":"retest","side":"long","stage":"confirmed","status":"closed","group_key":"profile-1","known_at":1761958800,"reason":"signal_emitted","signal_output":"entry","signal_event_key":"entry_long"}]}',
                    "source_path": "series/symbol=BTC/timeframe=1h/indicators/ind-1.csv",
                },
            ],
        },
    )

    dataset = _build(monkeypatch)

    lifecycle = dataset["candidate_lifecycle"]
    assert lifecycle["schema_version"] == "candidate_lifecycle_dataset.v1"
    assert lifecycle["available"] is True
    assert lifecycle["row_count"] == 2
    assert lifecycle["items"][0]["candidate_id"] == "candidate-1"
    assert lifecycle["items"][1]["signal_event_key"] == "entry_long"
    assert lifecycle["summary"]["candidate_count"] == 1
    assert lifecycle["summary"]["terminal_counts"] == {"confirmed": 1}
    assert lifecycle["summary"]["funnel"]["formed"]["candidate_count"] == 1
    assert lifecycle["summary"]["funnel"]["confirmed"]["candidate_count"] == 1
    section = next(row for row in dataset["sections"]["items"] if row["name"] == "candidate_lifecycle")
    assert section["available"] is True
    assert section["row_count"] == 2


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


def test_position_ordering_non_monotonic_blocks_golden_candidate(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events = _events()
    events[4] = _trade_event(6, "TRADE_OPENED", "trade-1", "BTC")
    events[5] = _trade_event(
        5,
        "TRADE_CLOSED",
        "trade-1",
        "BTC",
        close_reason="TARGET",
    )

    dataset = _build(monkeypatch, events=events)

    position_ordering = dataset["execution"]["position_ordering"]
    diagnostic_codes = {item["code"] for item in dataset["diagnostics"]["items"]}
    assert position_ordering["non_monotonic_count"] == 1
    assert "position_ordering_non_monotonic" in dataset["readiness"]["caveats"]
    assert "position_ordering_non_monotonic" in dataset["readiness"]["golden_blocking_reasons"]
    assert "position_ordering_non_monotonic" in diagnostic_codes


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
    atm = dataset["metadata"]["configuration"]["atm"]
    assert atm["template_id"] == "atm-1"
    assert atm["template"]["take_profit_orders"][0]["id"] == "tp-1"
    assert dataset["metadata"]["configuration"]["indicators"][0]["type"] == "market_profile"
    assert dataset["execution"]["execution_mode"] == "full"
    assert dataset["execution"]["slippage"]["total_slippage_cost"] == 0.0
    assert "per_fill_slippage_facts_unavailable" in dataset["readiness"]["caveats"]


def test_dataset_certifies_x2_only_from_pinned_economic_evidence(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    run = copy.deepcopy(_run())
    assumptions = resolve_execution_assumptions(
        "selection",
        {
            "schema_version": "execution_assumptions.v1",
            "model_version": "conservative_bar.v1",
            "market_slippage_bps": 5.0,
            "stop_slippage_bps": 10.0,
            "passive_fill_policy": "strict_penetration",
            "fee_policy": "instrument_resolved",
            "full_fill_assumption": True,
            "cost_stress_scenarios": [
                {
                    "id": "moderate",
                    "additional_slippage_bps": 5.0,
                    "fee_multiplier": 1.25,
                }
            ],
        },
        source="run_start_request",
    ).to_dict()
    run["config_snapshot"]["economic_claim_intent"] = "selection"
    run["config_snapshot"]["execution_assumptions"] = assumptions
    events = _events()
    events.extend(
        [
            _event(
                14,
                "ENTRY_FILLED",
                {
                    "trade_id": "trade-1",
                    "symbol": "BTC",
                    "qty": 1.0,
                    "requested_price": 100.0,
                    "fill_price": 100.05,
                    "slippage_bps": 5.0,
                    "fee_paid": 0.10,
                    "fee_rate": 0.001,
                    "fee_type": "taker",
                    "fee_source": "instrument_contract",
                    "fee_version": "fee-v1",
                    "execution_model_version": assumptions["model_version"],
                    "execution_assumption_manifest_hash": assumptions["manifest_hash"],
                    "economic_claim_intent": "selection",
                    "full_fill_assumption": True,
                },
            ),
            _event(
                15,
                "EXIT_FILLED",
                {
                    "trade_id": "trade-1",
                    "symbol": "BTC",
                    "qty": 1.0,
                    "requested_price": 110.0,
                    "fill_price": 110.0,
                    "slippage_bps": 0.0,
                    "fee_paid": 0.11,
                    "fee_rate": 0.001,
                    "fee_type": "maker",
                    "fee_source": "instrument_contract",
                    "fee_version": "fee-v1",
                    "execution_model_version": assumptions["model_version"],
                    "execution_assumption_manifest_hash": assumptions["manifest_hash"],
                    "economic_claim_intent": "selection",
                    "full_fill_assumption": True,
                },
            ),
        ]
    )

    dataset = _build(monkeypatch, run=run, events=events)

    assert dataset["execution"]["quality"]["execution_quality_class"] == "X2"
    assert dataset["execution"]["quality"]["blocking_reasons"] == []
    assert dataset["execution"]["quality"]["assumption_manifest_hash"] == assumptions["manifest_hash"]
    assert dataset["execution"]["cost_stress"]["status"] == "available"
    assert dataset["execution"]["cost_stress"]["scenario_count"] == 1
    assert len(dataset["execution"]["cost_stress"]["evidence_hash"]) == 64
    assert dataset["readiness"]["execution_quality_class"] == "X2"
    assert dataset["readiness"]["scientific_quality_class"] == "S0"
    assert dataset["readiness"]["promotion_eligibility"] == "ineligible"
    assert "scientific_quality_below_S3" in dataset["readiness"]["promotion_blocking_reasons"]

    events[-1]["payload"]["context"].pop("execution_assumption_manifest_hash")
    downgraded = _build(monkeypatch, run=run, events=events)
    assert downgraded["execution"]["quality"]["execution_quality_class"] == "X0"
    assert "per_fill_execution_evidence_incomplete" in downgraded["execution"]["quality"]["blocking_reasons"]
    assert dataset["execution"]["intrabar_fallback_count"] == 2
    assert dataset["execution"]["fallback_reason_distribution"] == {
        "ambiguous_1m_candle": 1,
        "missing_1m_data": 1,
    }
    diagnostic_codes = {item["code"] for item in dataset["diagnostics"]["items"]}
    assert "intrabar_fallback_pessimistic" in diagnostic_codes


def test_dataset_validates_phase_2a_context_bundle_and_per_fill_component_hashes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    run = copy.deepcopy(_run())
    assumptions = resolve_execution_assumptions(
        "selection",
        {
            "model_version": "conservative_bar.v1",
            "market_slippage_bps": 5.0,
            "stop_slippage_bps": 10.0,
            "passive_fill_policy": "strict_penetration",
            "fee_policy": "instrument_resolved",
            "full_fill_assumption": True,
            "cost_stress_scenarios": [
                {"id": "moderate", "additional_slippage_bps": 5.0, "fee_multiplier": 1.25}
            ],
        },
        source="run_start_request",
    )
    instrument = {
        "id": "btc",
        "symbol": "BTC",
        "instrument_type": "spot",
        "datasource": "fixture",
        "exchange": "fixture-venue",
        "tick_size": 0.01,
        "contract_size": 1.0,
        "tick_value": 0.01,
        "base_currency": "BTC",
        "quote_currency": "USD",
        "min_order_size": 0.001,
        "qty_step": 0.001,
        "min_notional": 1.0,
        "maker_fee_rate": 0.001,
        "taker_fee_rate": 0.001,
        "fee_source": "instrument_contract",
        "fee_schedule_version": "fee-v1",
    }
    profile = compile_series_execution_profile(instrument)
    context = resolve_execution_context(
        profile,
        assumptions,
        instrument_payload=instrument,
        source="backend_startup_resolution",
    )
    bundle = build_execution_context_bundle([context])
    run["config_snapshot"]["economic_claim_intent"] = "selection"
    run["config_snapshot"]["execution_assumptions"] = assumptions.to_dict()
    run["config_snapshot"]["resolved_execution_context_bundle"] = bundle.to_dict()
    evidence = {
        **context.evidence_metadata(),
        "execution_model_version": assumptions.model_version,
        "execution_assumption_manifest_hash": assumptions.manifest_hash,
        "economic_claim_intent": "selection",
        "full_fill_assumption": True,
        "time_in_force": "gtc",
    }
    events = _events()
    events.extend(
        [
            _event(
                14,
                "ENTRY_FILLED",
                {
                    **evidence,
                    "trade_id": "trade-1",
                    "symbol": "BTC",
                    "qty": 1.0,
                    "requested_price": 100.0,
                    "fill_price": 100.05,
                    "slippage_bps": 5.0,
                    "fee_paid": 0.10,
                    "fee_rate": 0.001,
                    "fee_type": "taker",
                    "fee_source": "instrument_contract",
                    "fee_version": "fee-v1",
                    "post_only": False,
                },
            ),
            _event(
                15,
                "EXIT_FILLED",
                {
                    **evidence,
                    "trade_id": "trade-1",
                    "symbol": "BTC",
                    "qty": 1.0,
                    "requested_price": 110.0,
                    "fill_price": 110.0,
                    "slippage_bps": 0.0,
                    "fee_paid": 0.11,
                    "fee_rate": 0.001,
                    "fee_type": "maker",
                    "fee_source": "instrument_contract",
                    "fee_version": "fee-v1",
                    "post_only": False,
                },
            ),
        ]
    )

    dataset = _build(monkeypatch, run=run, events=events)

    quality = dataset["execution"]["quality"]
    assert quality["execution_quality_class"] == "X2"
    assert quality["resolved_execution_context_status"] == "available"
    assert quality["resolved_execution_context_bundle_hash"] == bundle.bundle_hash
    assert quality["resolved_execution_context_evidence"]["contexts"][0]["context_hash"] == context.context_hash

    events[-1]["payload"]["context"]["fee_schedule_hash"] = "0" * 64
    downgraded = _build(monkeypatch, run=run, events=events)
    assert downgraded["execution"]["quality"]["execution_quality_class"] == "X0"
    assert "per_fill_execution_context_component_mismatch" in downgraded["execution"]["quality"]["blocking_reasons"]


def test_dataset_certifies_x4_and_downgrades_deterministically_on_book_evidence(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    run = copy.deepcopy(_run())
    assumptions = resolve_execution_assumptions(
        "selection",
        {
            "model_version": "conservative_bar.v1",
            "market_slippage_bps": 5.0,
            "stop_slippage_bps": 10.0,
            "passive_fill_policy": "strict_penetration",
            "fee_policy": "instrument_resolved",
            "full_fill_assumption": True,
            "cost_stress_scenarios": [
                {"id": "moderate", "additional_slippage_bps": 5.0, "fee_multiplier": 1.25}
            ],
        },
        source="run_start_request",
    )
    instrument = {
        "id": "btc",
        "symbol": "BTC",
        "instrument_type": "spot",
        "datasource": "fixture",
        "exchange": "fixture-venue",
        "tick_size": 0.01,
        "contract_size": 1.0,
        "tick_value": 0.01,
        "base_currency": "BTC",
        "quote_currency": "USD",
        "min_order_size": 0.001,
        "qty_step": 0.001,
        "min_notional": 1.0,
        "maker_fee_rate": 0.001,
        "taker_fee_rate": 0.001,
        "fee_source": "instrument_contract",
        "fee_schedule_version": "fee-v1",
        "venue_execution_profile": {
            "profile_id": "fixture-l2",
            "version": "fixture-l2.v1",
            "venue_id": "fixture-venue",
            "supported_order_types": ["market", "limit_aggressive", "stop_market"],
            "supported_time_in_force": ["gtc", "ioc", "fok"],
            "post_only_supported": False,
            "post_only_behavior": "reject_would_cross",
            "liquidity_role_by_order_type": {
                "market": "taker",
                "limit_aggressive": "taker",
                "stop_market": "taker",
            },
            "price_increment_policy": "reject",
            "quantity_increment_policy": "reject",
            "book_data_capability": "l2",
            "lifecycle_event_mapping": {
                state: state
                for state in (
                    "requested",
                    "validated",
                    "accepted",
                    "open",
                    "partially_filled",
                    "filled",
                    "canceled",
                    "rejected",
                    "expired",
                    "replaced",
                )
            },
            "external_order_submission_enabled": False,
            "source": "fixture",
        },
    }
    profile = compile_series_execution_profile(instrument)
    context = resolve_execution_context(
        profile,
        assumptions,
        instrument_payload=instrument,
        execution_model_artifact=execution_model_artifact_from_book_tape(
            assumptions,
            source_capability="l2",
        ),
        source="backend_startup_resolution",
    )
    bundle = build_execution_context_bundle([context])
    run["config_snapshot"]["economic_claim_intent"] = "selection"
    run["config_snapshot"]["execution_assumptions"] = assumptions.to_dict()
    run["config_snapshot"]["resolved_execution_context_bundle"] = bundle.to_dict()
    book_evidence = {
        "schema_version": "book_execution_evidence.v1",
        "execution_model_version": context.model.version,
        "execution_model_artifact_hash": context.model.artifact_hash,
        "execution_quality_ceiling": "X4",
        "execution_book_tape_id": "ebt_test",
        "execution_book_tape_hash": "1" * 64,
        "execution_book_replay_fingerprint": "2" * 64,
        "execution_book_replay_certified": True,
        "execution_book_source_capability": "l2",
        "execution_book_snapshot_hash": "3" * 64,
        "execution_book_state_hash": "4" * 64,
        "execution_book_validity_interval_id": "validity-1",
        "execution_book_source_reference": {
            "definition_id": "definition-1",
            "session_id": "session-1",
            "connection_epoch": 0,
            "source_product_id": "BTC-USD",
            "source_sequence": 7,
            "receive_ordinal": 7,
            "event_ordinal": 0,
        },
        "execution_book_product_definition_version_id": "product.v1",
        "execution_book_quantity_unit": "base",
        "execution_book_effective_at": "2026-03-01T00:00:00Z",
        "execution_book_known_at": "2026-03-01T00:00:00.001000Z",
        "order_arrival_at": "2026-03-01T00:00:00.002000Z",
        "arrival_latency_ms": 0,
        "order_type": "market",
        "side": "buy",
        "time_in_force": "gtc",
        "requested_qty": 1.0,
        "reference_price": 101.0,
        "best_bid": 100.0,
        "best_ask": 101.0,
        "spread": 1.0,
        "eligible_visible_depth": 2.0,
        "eligible_level_count": 1,
        "fill_id": "book-fill-1",
        "book_level_index": 1,
        "book_side": "ask",
        "visible_level_qty": 2.0,
        "consumed_level_qty": 1.0,
        "price_improvement": 0.0,
        "limitations": ["aggregated_depth_only", "exact_queue_position_unavailable"],
    }
    context_evidence = {
        **context.evidence_metadata(),
        "execution_model_version": context.model.version,
        "execution_assumption_manifest_hash": assumptions.manifest_hash,
        "passive_fill_policy": assumptions.passive_fill_policy,
        "economic_claim_intent": "selection",
        "fee_policy": assumptions.fee_policy,
        "full_fill_assumption": assumptions.full_fill_assumption,
        "market_slippage_bps": assumptions.market_slippage_bps,
        "stop_slippage_bps": assumptions.stop_slippage_bps,
        "time_in_force": "gtc",
        "post_only": False,
    }
    lifecycle_context = {
        "known_at": book_evidence["order_arrival_at"],
        "symbol": "BTC",
        "instrument_id": "btc",
        "order_request_id": "order-1",
        "order_request_manifest_hash": "5" * 64,
        "attempt_id": "attempt-1",
        "order_attempt_manifest_hash": "6" * 64,
        "order_event_seq": 4,
        "previous_state": "accepted",
        "state": "filled",
        "side": "buy",
        "requested_qty": 1.0,
        "attempt_requested_qty": 1.0,
        "attempt_cumulative_filled_qty": 1.0,
        "attempt_remaining_qty": 0.0,
        "order_cumulative_filled_qty": 1.0,
        "order_remaining_qty": 0.0,
        "execution_context_hash": context.context_hash,
        "execution_policy_hash": "7" * 64,
        "order_lifecycle_replay_hash": "8" * 64,
        "source_sequence": 1,
        "fill_id": "book-fill-1",
        "fill_qty": 1.0,
        "fill_price": 101.0,
        "fill_fee": 0.101,
        "book_execution_evidence": book_evidence,
    }
    fill_context = {
        **context_evidence,
        "trade_id": "trade-1",
        "symbol": "BTC",
        "instrument_id": "btc",
        "qty": 1.0,
        "requested_price": 101.0,
        "fill_price": 101.0,
        "slippage_bps": 0.0,
        "fee_paid": 0.101,
        "fee_rate": 0.001,
        "fee_type": "taker",
        "fee_source": "instrument_contract",
        "fee_version": "fee-v1",
        "book_execution_evidence": book_evidence,
    }
    events = [
        *_events(),
        _event(14, "ORDER_LIFECYCLE_CHANGED", lifecycle_context),
        _event(15, "ENTRY_FILLED", fill_context),
    ]

    dataset = _build(monkeypatch, run=run, events=events)
    quality = dataset["execution"]["quality"]
    assert quality["execution_quality_class"] == "X4"
    assert quality["blocking_reasons"] == []
    assert quality["l2_execution_evidence"]["status"] == "available"
    assert quality["l2_execution_evidence"]["tape_hashes"] == ["1" * 64]
    assert dataset["execution"]["order_lifecycle"]["events"][0][
        "book_execution_evidence"
    ]["execution_book_snapshot_hash"] == "3" * 64

    depth_breach = copy.deepcopy(events)
    depth_breach[-2]["payload"]["context"]["book_execution_evidence"][
        "visible_level_qty"
    ] = 0.5
    downgraded_x3 = _build(monkeypatch, run=run, events=depth_breach)
    assert downgraded_x3["execution"]["quality"]["execution_quality_class"] == "X3"
    assert "per_level_visible_depth_bound_invalid" in downgraded_x3["execution"]["quality"]["blocking_reasons"]

    uncertified = copy.deepcopy(events)
    uncertified[-2]["payload"]["context"]["book_execution_evidence"][
        "execution_book_replay_certified"
    ] = False
    downgraded_x2 = _build(monkeypatch, run=run, events=uncertified)
    assert downgraded_x2["execution"]["quality"]["execution_quality_class"] == "X2"
    assert "execution_book_replay_uncertified" in downgraded_x2["execution"]["quality"]["blocking_reasons"]


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


def test_excursion_candle_reads_scale_with_unique_series_not_trade_count(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[dict[str, Any]] = []

    def list_windows(*, instrument_id: str, timeframe: str, windows):
        calls.append(
            {
                "instrument_id": instrument_id,
                "timeframe": timeframe,
                "windows": [dict(window) for window in windows],
            }
        )
        return {str(window["window_id"]): [] for window in windows}

    monkeypatch.setattr(
        run_research_dataset,
        "list_candles_for_series_windows",
        list_windows,
    )
    trade = _trades()[0]
    trades = [
        {
            **trade,
            "trade_id": f"trade-{index}",
            "instrument_id": "instrument-btc",
            "timeframe": "1h",
        }
        for index in range(25)
    ]

    results = run_research_dataset._prefetch_excursion_candles(trades)

    assert len(results) == 25
    assert {(call["instrument_id"], call["timeframe"]) for call in calls} == {
        ("instrument-btc", "1m"),
        ("instrument-btc", "1h"),
    }
    assert all(len(call["windows"]) == 25 for call in calls)


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


def test_dataset_reports_observability_event_truncation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observability_events = [
        {
            "event_name": "diagnostic_event",
            "level": "INFO",
            "observed_at": "2026-04-01T00:00:00Z",
            "details": {"index": index},
        }
        for index in range(2001)
    ]

    dataset = _build(
        monkeypatch,
        observability_events=observability_events,
    )

    coverage = dataset["operational_health"]["observability_event_coverage"]
    assert coverage == {
        "schema_version": "observability_event_coverage.v1",
        "status": "truncated",
        "retained_count": 2000,
        "probe_count": 2001,
        "limit": 2000,
        "has_more": True,
        "ordering": "observed_at_desc",
    }
    assert "observability_events_truncated" in dataset["readiness"]["caveats"]
    assert (
        "observability_events_truncated"
        in dataset["readiness"]["golden_blocking_reasons"]
    )
    diagnostics = {
        row["code"]: row for row in dataset["diagnostics"]["items"]
    }
    assert (
        diagnostics["observability_events_truncated"]["readiness_impact"]
        == "blocks_golden"
    )


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
    assert btc["storage_source"] == "market.candle_versions"


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


def test_missing_fee_facts_are_visible_in_report_trust(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    trades = _trades()
    for trade in trades:
        metrics = dict(trade.get("metrics") or {})
        metrics.pop("fee_rate", None)
        metrics.pop("fee_role", None)
        metrics.pop("fee_source", None)
        trade["metrics"] = metrics

    dataset = _build(monkeypatch, trades=trades)

    caveats = set(dataset["readiness"]["caveats"])
    assert "fee_role_facts_unavailable" in caveats
    assert "fee_rate_facts_unavailable" in caveats
    assert "fee_accounting" in dataset["readiness"]["degraded_sections"]


def test_unconfigured_default_zero_fees_are_visible_in_report_trust(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    trades = _trades()
    for trade in trades:
        metrics = dict(trade.get("metrics") or {})
        metrics["fee_rate"] = 0.0
        metrics["fee_source"] = "default_zero"
        trade["metrics"] = metrics

    dataset = _build(monkeypatch, trades=trades)

    caveats = set(dataset["readiness"]["caveats"])
    assert "unconfigured_zero_fee_model" in caveats
    assert "fee_accounting" in dataset["readiness"]["degraded_sections"]


def test_dataset_includes_botlens_rebuildable_snapshot_caveat(monkeypatch: pytest.MonkeyPatch) -> None:
    dataset = _build(monkeypatch)

    caveats = set(dataset["readiness"]["caveats"])
    assert "botlens_snapshots_rebuildable_from_material_event_ledger_and_compact_context" in caveats


def test_operational_health_exposes_opt_in_runtime_profile_artifact(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    dataset = _build(
        monkeypatch,
        observability_events=[
            {
                "event_name": "runtime_profile_completed",
                "worker_id": "worker-1",
                "observed_at": "2026-03-31T00:00:00Z",
                "details": {
                    "schema_version": "python_profile.v1",
                    "status": "completed",
                    "wall_seconds": 12.5,
                    "cpu_seconds": 11.0,
                    "peak_memory_bytes": 2048,
                },
            }
        ],
    )

    assert dataset["operational_health"]["profile_artifacts"] == [
        {
            "schema_version": "python_profile.v1",
            "status": "completed",
            "wall_seconds": 12.5,
            "cpu_seconds": 11.0,
            "peak_memory_bytes": 2048,
            "worker_id": "worker-1",
            "observed_at": "2026-03-31T00:00:00Z",
        }
    ]


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


def test_spot_fills_drive_wallet_replay_and_execution_trace(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    run = _run()
    run["config_snapshot"]["strategies"][0]["instruments"] = [
        {
            "instrument_id": "instrument-btc",
            "instrument_snapshot": {
                "id": "instrument-btc",
                "symbol": "BTC",
                "instrument_type": "spot",
            },
        }
    ]
    events = _events()
    events.extend(
        [
            _event(
                14,
                "WALLET_INITIALIZED",
                {
                    "currency": "USD",
                    "wallet_commit_seq": 0,
                    "wallet_event_order": 0,
                    "balance_before": 0.0,
                    "balance_after": 1000.0,
                    "wallet_after": {
                        "balances": {"USD": 1000.0},
                        "locked_margin": {},
                        "free_collateral": {"USD": 1000.0},
                        "margin_positions": {},
                    },
                },
            ),
            _event(
                15,
                "ENTRY_FILLED",
                {
                    "trade_id": "trade-spot-1",
                    "instrument_id": "instrument-btc",
                    "symbol": "BTC",
                    "timeframe": "1h",
                    "bar_time": "2026-03-15T00:00:00Z",
                    "wallet_commit_seq": 1,
                    "side": "buy",
                    "direction": "long",
                    "qty": 1.0,
                    "price": 100.0,
                    "notional": 100.0,
                    "fee_paid": 1.0,
                    "fee_rate": 0.01,
                    "fee_type": "taker",
                    "fee_source": "test",
                    "base_currency": "BTC",
                    "quote_currency": "USD",
                    "accounting_mode": "spot",
                    "wallet_delta": {
                        "collateral_reserved": 0.0,
                        "collateral_released": 0.0,
                        "fee_paid": 1.0,
                    },
                    "wallet_before": {
                        "balances": {"USD": 1000.0},
                        "locked_margin": {},
                        "free_collateral": {"USD": 1000.0},
                    },
                },
            ),
            _event(
                16,
                "EXIT_FILLED",
                {
                    "trade_id": "trade-spot-1",
                    "instrument_id": "instrument-btc",
                    "symbol": "BTC",
                    "timeframe": "1h",
                    "bar_time": "2026-03-16T00:00:00Z",
                    "wallet_commit_seq": 2,
                    "side": "sell",
                    "direction": "long",
                    "qty": 1.0,
                    "price": 155.0,
                    "notional": 155.0,
                    "fee_paid": 1.0,
                    "fee_rate": 0.01,
                    "fee_type": "taker",
                    "fee_source": "test",
                    "realized_pnl": 55.0,
                    "exit_kind": "target",
                    "base_currency": "BTC",
                    "quote_currency": "USD",
                    "accounting_mode": "spot",
                    "wallet_delta": {
                        "collateral_reserved": 0.0,
                        "collateral_released": 0.0,
                        "fee_paid": 1.0,
                    },
                    "wallet_before": {
                        "balances": {"BTC": 1.0, "USD": 899.0},
                        "locked_margin": {},
                        "free_collateral": {"BTC": 1.0, "USD": 899.0},
                    },
                },
            ),
        ]
    )

    dataset = _build(monkeypatch, run=run, events=events)

    assert dataset["wallet_accounting"]["wallet_replay_status"] == "passed"
    assert dataset["wallet_accounting"]["locked_margin_final"] == {}
    assert dataset["wallet_accounting"]["wallet_diagnostics"]["replay_projection"]["balances"] == {
        "BTC": 0.0,
        "USD": 1053.0,
    }
    assert dataset["execution"]["fill_count"] == 2
    assert [row["event_name"] for row in dataset["execution"]["fills"]] == [
        "entry_filled",
        "exit_filled",
    ]
    assert dataset["metadata"]["instrument_semantics"] == [
        {
            "instrument_id": "instrument-btc",
            "symbol": "BTC",
            "instrument_type": "spot",
            "source_instrument_type": "spot",
            "execution_semantics": "spot",
            "research_market_role": None,
            "accounting_mode": "spot",
            "margin_calc_type": None,
        }
    ]
    assert "instrument-btc" in dataset["metadata"]["instrument_ids"]


def test_report_rejects_conflicting_fill_and_configured_instrument_semantics(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    run = _run()
    run["config_snapshot"]["runtime_readiness"] = {
        "profiles": [
            {
                "instrument_id": "instrument-btc",
                "symbol": "BTC",
                "instrument_type": "future",
                "source_instrument_type": "future",
                "execution_semantics": "derivative",
                "accounting_mode": "margin",
            }
        ]
    }
    events = [
        *_events(),
        _event(
            14,
            "ENTRY_FILLED",
            {
                "trade_id": "trade-conflict",
                "instrument_id": "instrument-btc",
                "symbol": "BTC",
                "timeframe": "1h",
                "bar_time": "2026-03-15T00:00:00Z",
                "accounting_mode": "spot",
            },
        ),
    ]

    with pytest.raises(
        ValueError,
        match=r"conflicting report (execution_semantics|accounting_mode)",
    ):
        _build(monkeypatch, run=run, events=events)


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

    assert reordered["readiness"]["semantic_fingerprint"] == baseline["readiness"]["semantic_fingerprint"]
    assert reordered["readiness"]["operational_fingerprint"] != baseline["readiness"]["operational_fingerprint"]


def test_semantic_fingerprint_changes_when_runtime_context_evidence_changes(monkeypatch: pytest.MonkeyPatch) -> None:
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

    assert changed["readiness"]["semantic_fingerprint"] != baseline["readiness"]["semantic_fingerprint"]


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
    assert changed["metadata"]["report_semantic_fingerprint"] == baseline["metadata"]["report_semantic_fingerprint"]
    assert changed["readiness"]["operational_fingerprint"] != baseline["readiness"]["operational_fingerprint"]
    assert changed["metadata"]["report_operational_fingerprint"] != baseline["metadata"]["report_operational_fingerprint"]


def test_data_snapshot_hash_uses_runtime_candle_values_not_gap_metadata() -> None:
    snapshot = {
        "schema_version": "candle_series_snapshot.v1",
        "strategy_id": "strategy-1",
        "instrument_id": "instrument-btc",
        "symbol": "BTC",
        "timeframe": "1h",
        "datasource": "coinbase",
        "exchange": "cbi",
        "candle_value_hash": "a" * 64,
        "candle_count": 10,
        "warmup_candle_count": 2,
        "replay_candle_count": 8,
        "first_ts": "2026-03-01T00:00:00Z",
        "last_ts": "2026-03-01T09:00:00Z",
        "fields": ["time", "open", "high", "low", "close", "atr", "volume"],
    }
    baseline = {
        "items": [
            {
                "instrument_id": "instrument-btc",
                "timeframe": "1h",
                "gap_count": 0,
                "candle_snapshot": snapshot,
            }
        ]
    }
    changed_gap = copy.deepcopy(baseline)
    changed_gap["items"][0]["gap_count"] = 3

    assert run_research_dataset._data_snapshot_hash(
        changed_gap
    ) == run_research_dataset._data_snapshot_hash(baseline)

    changed_values = copy.deepcopy(baseline)
    changed_values["items"][0]["candle_snapshot"]["candle_value_hash"] = "b" * 64
    assert run_research_dataset._data_snapshot_hash(
        changed_values
    ) != run_research_dataset._data_snapshot_hash(baseline)


def test_data_snapshot_hash_is_unavailable_when_any_series_lacks_runtime_evidence() -> None:
    catalog = {
        "items": [
            {
                "instrument_id": "instrument-btc",
                "timeframe": "1h",
                "candle_snapshot": {
                    "schema_version": "candle_series_snapshot.v1",
                    "strategy_id": "strategy-1",
                    "instrument_id": "instrument-btc",
                    "symbol": "BTC",
                    "timeframe": "1h",
                    "candle_value_hash": "a" * 64,
                    "candle_count": 10,
                    "warmup_candle_count": 2,
                    "replay_candle_count": 8,
                },
            },
            {
                "instrument_id": "instrument-eth",
                "timeframe": "1h",
                "candle_snapshot": {},
            },
        ]
    }

    assert run_research_dataset._data_snapshot_hash(catalog) is None


def test_data_snapshot_hash_requires_exact_configured_terminal_snapshot_set() -> None:
    run = _run()
    run["config_snapshot"]["expected_candle_series"] = [
        {
            "strategy_id": "strategy-1",
            "instrument_id": "instrument-btc",
            "symbol": "BTC",
            "timeframe": "1h",
        },
        {
            "strategy_id": "strategy-1",
            "instrument_id": "instrument-eth",
            "symbol": "ETH",
            "timeframe": "1h",
        },
    ]
    expected = run_research_dataset._expected_candle_series(run)
    btc_snapshot = {
        "schema_version": "candle_series_snapshot.v1",
        "strategy_id": "strategy-1",
        "instrument_id": "instrument-btc",
        "symbol": "BTC",
        "timeframe": "1h",
        "datasource": "coinbase",
        "exchange": "cbi",
        "candle_value_hash": "a" * 64,
        "candle_count": 10,
        "warmup_candle_count": 2,
        "replay_candle_count": 8,
        "first_ts": "2026-03-01T00:00:00Z",
        "last_ts": "2026-03-01T09:00:00Z",
        "fields": ["time", "open", "high", "low", "close", "atr", "volume"],
    }
    catalog = {
        "items": [
            {
                "instrument_id": "instrument-btc",
                "symbol": "BTC",
                "timeframe": "1h",
                "candle_snapshot": btc_snapshot,
            }
        ]
    }

    assert run_research_dataset._data_snapshot_hash(
        catalog,
        candle_gaps={"facts": [{"candle_snapshot": btc_snapshot}]},
        expected_series=expected,
    ) is None

    eth_snapshot = {
        **btc_snapshot,
        "instrument_id": "instrument-eth",
        "symbol": "ETH",
        "candle_value_hash": "b" * 64,
    }
    assert run_research_dataset._data_snapshot_hash(
        catalog,
        candle_gaps={
            "facts": [
                {"candle_snapshot": btc_snapshot},
                {"candle_snapshot": eth_snapshot},
            ]
        },
        expected_series=expected,
    )


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
                "failure": {"reason_code": "stale_run_lease"},
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


def test_recoverable_watchdog_stale_run_lease_degrades_without_lifecycle_contradiction(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events = [
        _event(
            14,
            "FAULT_RECORDED",
            {
                "bar_time": None,
                "fault_code": "stale_run_lease",
                "severity": "WARN",
                "message": "Recoverable watchdog stale run lease observed: stale_run_lease:prev=backend.quanttrad",
                "source": "lifecycle",
                "component": "watchdog",
                "failure_type": "watchdog_stale_run_lease",
                "reason_code": "stale_run_lease",
                "reason": "stale_run_lease:prev=backend.quanttrad",
                "recoverable": True,
            },
        ),
        *_events(),
        _event(99, "RUN_COMPLETED", {"bar_time": None, "status": "completed"}),
    ]

    dataset = _build(monkeypatch, events=events)

    codes = {item["code"]: item for item in dataset["diagnostics"]["items"]}
    assert "recoverable_watchdog_stale_run_lease" in codes
    assert codes["recoverable_watchdog_stale_run_lease"]["readiness_impact"] == "degrades_diagnostics"
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


def test_producer_terminal_continuity_details_are_canonical_and_transport_is_diagnostic(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events = [row for row in _events() if _event_name(row) != "candle_continuity_summary"]
    events.append(
        _event(
            20,
            "DIAGNOSTIC_RECORDED",
            {
                "series_key": "instrument-btc|1h",
                "instrument_id": "instrument-btc",
                "symbol": "BTC",
                "timeframe": "1h",
                "diagnostic_code": "candle_continuity_summary",
                "details": {
                    "series_key": "instrument-btc|1h",
                    "instrument_id": "instrument-btc",
                    "symbol": "BTC",
                    "timeframe": "1h",
                    "boundary_name": "run_final",
                    "materiality": "canonical",
                    "evidence_scope": "canonical_terminal",
                    "candle_count": 169,
                    "detected_gap_count": 0,
                    "missing_candle_estimate": 0,
                    "gap_count_by_type": {"unknown_gap": 0},
                },
            },
        )
    )
    transport = _observer_gap(
        boundary_name="transport_run_final",
        pipeline_stage="botlens_run_final",
        message_kind="lifecycle",
        detected_gap_count=35,
    )
    transport["details"]["diagnostic_scope"] = "transport_continuity"

    dataset = _build(
        monkeypatch,
        events=events,
        observability_events=[transport],
        candle_summaries={
            ("instrument-btc", "1h"): {
                "candle_count": 169,
                "gap_count": 0,
                "missing_count": 0,
                "available_resolutions": ["1h"],
            }
        },
    )

    assert dataset["candle_gaps"]["canonical_evidence_status"] == "present"
    assert dataset["candle_gaps"]["blocking_gap_count"] == 0
    assert dataset["candle_gaps"]["facts"][0]["candle_count"] == 169
    assert dataset["candle_gaps"]["diagnostic_facts"][0]["detected_gap_count"] == 35
    catalog = next(row for row in dataset["candle_catalog"]["items"] if row["instrument_id"] == "instrument-btc")
    assert catalog["continuity_status"] == "clean"
    assert catalog["first_gap_evidence"] is None


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


def test_unknown_candle_gaps_reclassify_from_provider_gap_evidence(monkeypatch: pytest.MonkeyPatch) -> None:
    events = [row for row in _events() if _event_name(row) != "candle_continuity_summary"]
    events.append(_unknown_gap(20))

    dataset = _build(
        monkeypatch,
        events=events,
        candle_provider_gaps={
            ("instrument-btc", "1h"): [
                {
                    "classification": "provider_missing_data",
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


def test_metadata_uses_canonical_instrument_id_not_strategy_link_id() -> None:
    run = _run()
    run["config_snapshot"]["strategies"][0]["instruments"] = [
        {
            "id": "strategy-instrument-link",
            "instrument_id": "instrument-btc",
            "symbol": "BTC",
        }
    ]

    assert run_research_dataset._metadata_instrument_ids(run) == [
        "instrument-btc"
    ]


def test_backtest_without_frozen_dataset_identity_cannot_be_golden(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    run = _run()
    run["config_snapshot"].pop("dataset_binding")

    dataset = _build(monkeypatch, run=run)

    assert "missing_dataset_id" in dataset["readiness"]["golden_blocking_reasons"]
    assert "missing_dataset_hash" in dataset["readiness"]["golden_blocking_reasons"]
