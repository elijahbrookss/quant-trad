from __future__ import annotations

from datetime import datetime, timezone
from queue import Empty
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from engines.bot_runtime.core.domain import Candle, StrategySignal
from engines.bot_runtime.core.runtime_events import (
    EntryFilledContext,
    ExitKind,
    ReasonCode,
    RuntimeEventName,
    WalletDelta,
    WalletInitializedContext,
    build_correlation_id,
    new_runtime_event,
    runtime_event_from_dict,
)
from engines.bot_runtime.runtime.mixins.runtime_events import RuntimeEventsMixin
from engines.bot_runtime.runtime.mixins.runtime_push_stream import RuntimePushStreamMixin
from engines.bot_runtime.runtime.components.canonical_facts import CanonicalFactAppender, LiveFactsBroadcastConsumer
from portal.backend.service.bots.botlens_domain_events import (
    build_botlens_domain_events_from_fact_batch,
    serialize_botlens_domain_event,
)


class _FakeRuntime(RuntimePushStreamMixin):
    def __init__(self) -> None:
        self._lock = SimpleNamespace()
        self._subscribers = {}

    def _runtime_log_context(self, **kwargs):
        return dict(kwargs)


class _SimpleLock:
    def __enter__(self):
        return None

    def __exit__(self, exc_type, exc, tb):
        return False


def _runtime() -> _FakeRuntime:
    runtime = _FakeRuntime()
    runtime._lock = _SimpleLock()
    return runtime


def _trade_series(trade_payload: dict, *, revision: int = 1) -> SimpleNamespace:
    return SimpleNamespace(
        instrument={"id": "instrument-bip"},
        timeframe="1h",
        strategy_id="strategy-1",
        symbol="BIP-20DEC30-CDE",
        datasource="COINBASE",
        exchange="coinbase_direct",
        risk_engine=SimpleNamespace(
            trade_revision=revision,
            serialise_trades=lambda: [dict(trade_payload)],
            stats=lambda: {},
        ),
    )


class _EventRuntime(RuntimeEventsMixin):
    def __init__(self) -> None:
        self._lock = _SimpleLock()
        self._run_context = SimpleNamespace(
            run_id="run-1",
            runtime_event_seq=0,
            runtime_events=[],
            runtime_event_stream=[],
        )
        self.bot_id = "bot-1"
        self.config = {}
        self._event_sinks = []
        self._report_artifact_bundle = None


class _PushRuntime(_FakeRuntime):
    def __init__(self) -> None:
        super().__init__()
        self._lock = _SimpleLock()
        self._subscribers = {"sub-1": {"queue": object(), "overflow_policy": "fail", "overflowed": False}}
        self._push_series_cache = {}
        self._push_log_marker = None
        self._push_decision_marker = None
        self._push_payload_bytes_sample_every = 10
        self._botlens_fact_stream_overlay_point_limit = 160
        self._obs_enabled = False
        self._warning_revision = 0
        self._push_runtime_health_fingerprint = None
        self._push_runtime_health_emitted_monotonic = 0.0
        self._push_runtime_health_warning_revision = 0
        self._push_runtime_health_status = None
        self.state = {"status": "running"}
        self.broadcast_payloads = []
        self._logs = []
        self._decision_events = []
        self._canonical_seq = 0
        self._canonical_fact_appender = CanonicalFactAppender(
            allocate_seq=self._allocate_test_canonical_seq,
            append_batch=lambda **_kwargs: {"inserted_rows": 1},
            consumers=(LiveFactsBroadcastConsumer(self._broadcast),),
        )
        self._run_context = SimpleNamespace(run_id="run-1")
        self.bot_id = "bot-1"
        self.config = {}

    def _allocate_test_canonical_seq(self) -> int:
        self._canonical_seq += 1
        return self._canonical_seq

    def snapshot(self):
        return {
            "status": "running",
            "known_at": "2026-04-09T14:00:00Z",
            "last_snapshot_at": "2026-04-09T14:00:00Z",
            "stats": {"bars_processed": 12},
        }

    def logs(self):
        return []

    def decision_events(self):
        return []

    def _aggregate_stats(self):
        return {}

    def _series_state_for(self, series):
        return SimpleNamespace(bar_index=1)

    def _series_visible_overlays(self, series, *, status, refresh=True):
        _ = status, refresh
        return list(series.overlays or [])

    def _series_overlay_revision(self, series, *, status):
        _ = status
        return (
            "running",
            tuple(
                (
                    str(entry.get("overlay_id") or entry.get("type") or ""),
                    str(entry.get("type") or ""),
                )
                for entry in (series.overlays or [])
            ),
        )

    def _overlay_summary(self, overlays):
        return {
            "total_overlays": len(overlays),
            "type_counts": {},
            "payload_counts": {},
            "profile_params_samples": {},
        }

    def _series_log_context(self, series, **fields):
        _ = series
        return dict(fields)

    def _record_step_trace(self, *args, **kwargs):
        _ = args, kwargs
        return None

    def _broadcast(self, event, payload=None):
        self.broadcast_payloads.append({"event": event, **dict(payload or {})})
        return (1, 0)


def test_subscribe_drop_and_signal_replaces_backpressure_with_gap_event() -> None:
    runtime = _runtime()
    token, queue_ref = runtime.subscribe(overflow_policy="drop_and_signal")

    for index in range(queue_ref.maxsize):
        queue_ref.put_nowait({"type": f"seed-{index}"})

    subscribers, dropped = runtime._broadcast("facts", {"payload": "next"})

    assert subscribers == 1
    assert dropped == 0

    gap = queue_ref.get_nowait()
    assert gap == {
        "type": "gap",
        "reason": "subscriber_backpressure",
        "event": "facts",
    }

    with runtime._lock:
        assert runtime._subscribers[token]["overflowed"] is True

    runtime.unsubscribe(token)
    with runtime._lock:
        assert token not in runtime._subscribers
    try:
        queue_ref.get_nowait()
        raise AssertionError("queue should be drained after unsubscribe")
    except Empty:
        pass


def test_botlens_fact_stream_surface_metrics_budget_viewer_blind_fact_attribution() -> None:
    metrics = RuntimePushStreamMixin._botlens_fact_stream_surface_metrics(
        {
            "facts": [
                {"fact_type": "candle_upserted", "candle": {"time": "2026-01-01T00:00:00Z"}},
                {"fact_type": "overlay_ops_emitted", "overlay_delta": {"ops": [{"op": "upsert"}]}},
                {"fact_type": "wallet_ledger_event", "wallet_event": {"event_name": "MARGIN_RESERVED"}},
                {"fact_type": "decision_emitted", "decision": {"event_name": "DECISION_ACCEPTED"}},
                {"fact_type": "series_stats_updated", "stats": {"net_pnl": 1.25}},
            ]
        },
        include_bytes=True,
    )

    assert metrics["botlens_fact_stream_fact_count"] == 5
    assert metrics["botlens_fact_stream_candles_fact_count"] == 1
    assert metrics["botlens_fact_stream_overlays_fact_count"] == 1
    assert metrics["botlens_fact_stream_wallet_fact_count"] == 1
    assert metrics["botlens_fact_stream_decisions_fact_count"] == 1
    assert metrics["botlens_fact_stream_symbol_summary_fact_count"] == 1
    assert metrics["botlens_fact_stream_candles_payload_bytes"] > 0
    assert all(not key.startswith(("botlens_live_", "live_transport_")) for key in metrics)


def test_botlens_fact_stream_runtime_state_omits_heavy_snapshot_fields() -> None:
    runtime = _runtime()

    fact = runtime._runtime_state_fact(
        event="bar",
        runtime_snapshot={
            "status": "running",
            "runtime_state": "live",
            "progress_state": "progressing",
            "stats": {"equity_curve": list(range(1000))},
            "warnings": [
                {
                    "warning_id": "indicator::budget::instrument-bip|1h",
                    "warning_type": "indicator_budget",
                    "severity": "warning",
                    "message": "budget exceeded",
                    "context": {"raw_snapshot": list(range(1000))},
                }
            ],
            "pressure": {
                "captured_at": "2026-04-09T14:00:00Z",
                "trigger": "bar",
                "top_pressure": {
                    "reason_code": "payload_bytes",
                    "value": 445000,
                    "unit": "bytes",
                    "raw": list(range(1000)),
                },
                "all_pressures": [{"reason_code": "db"} for _ in range(100)],
            },
            "recent_transitions": [
                {"from_state": "s0", "to_state": "s1", "timestamp": "2026-04-09T14:00:00Z"},
                {"from_state": "s1", "to_state": "s2", "timestamp": "2026-04-09T14:01:00Z"},
                {"from_state": "s2", "to_state": "s3", "timestamp": "2026-04-09T14:02:00Z"},
                {"from_state": "s3", "to_state": "s4", "timestamp": "2026-04-09T14:03:00Z"},
                {"from_state": "s4", "to_state": "s5", "timestamp": "2026-04-09T14:04:00Z"},
            ],
        },
    )

    runtime_payload = fact["runtime"]
    assert runtime_payload["status"] == "running"
    assert runtime_payload["runtime_state"] == "live"
    assert runtime_payload["warning_count"] == 1
    assert "stats" not in runtime_payload
    assert "context" not in runtime_payload["warnings"][0]
    assert runtime_payload["pressure"] == {
        "trigger": "bar",
        "top_pressure": {"reason_code": "payload_bytes", "value": 445000.0, "unit": "bytes"},
    }
    assert len(runtime_payload["recent_transitions"]) == 4
    assert runtime_payload["recent_transitions"][0]["from_state"] == "s1"


def test_botlens_fact_stream_series_identity_keeps_only_routing_identity() -> None:
    runtime = _runtime()
    series = SimpleNamespace(
        instrument={"id": "instrument-bip", "metadata": {"large": list(range(1000))}},
        timeframe="1h",
        strategy_id="strategy-1",
        symbol="BIP-20DEC30-CDE",
        datasource={"provider": "COINBASE", "raw": list(range(1000))},
        exchange={"name": "coinbase_direct", "raw": list(range(1000))},
    )

    identity = runtime._series_identity(series)

    assert identity == {
        "series_key": "instrument-bip|1h",
        "strategy_id": "strategy-1",
        "instrument_id": "instrument-bip",
        "symbol": "BIP-20DEC30-CDE",
        "timeframe": "1h",
    }


def test_botlens_fact_stream_series_stats_keeps_compact_summary() -> None:
    stats = RuntimePushStreamMixin._compact_series_stats(
        {
            "total_trades": 3,
            "wins": 2,
            "losses": 1,
            "net_pnl": 12.34567,
            "fees_paid": 0.45678,
            "total_fees": 0.45678,
            "quote_currency": "usd",
            "equity_curve": list(range(1000)),
            "per_day": {"2026-04-09": 3},
        }
    )

    assert stats == {
        "fees_paid": 0.4568,
        "losses": 1,
        "net_pnl": 12.3457,
        "quote_currency": "USD",
        "total_fees": 0.4568,
        "total_trades": 3,
        "wins": 2,
    }


def test_botlens_fact_stream_overlay_delta_uses_bounded_render_payload() -> None:
    runtime = _runtime()
    runtime._botlens_fact_stream_overlay_point_limit = 2
    delta = runtime._build_overlay_delta(
        {},
        [
            {
                "overlay_id": "overlay-1",
                "type": "candle_stats_atr_short",
                "strategy_id": "strategy-1",
                "source": "indicator_guard",
                "pane_key": "volatility",
                "pane_views": ["polyline", "marker"],
                "color": "#38bdf8",
                "ui": {"label": "ATR", "color": "#38bdf8", "debug": list(range(1000))},
                "payload": {
                    "markers": [
                        {"time": 1, "price": 100.0},
                        {"time": 2, "price": 101.0},
                        {"time": 3, "price": 102.0},
                    ],
                    "polylines": [
                        {
                            "points": [
                                {"time": index, "price": float(index)}
                                for index in range(1, 10)
                            ]
                        }
                    ],
                },
            }
        ],
    )

    overlay = delta["ops"][0]["overlay"]
    assert overlay["detail_level"] == "bounded_render"
    assert overlay["payload"]["markers"] == [
        {"time": 2, "price": 101.0},
        {"time": 3, "price": 102.0},
    ]
    assert overlay["payload"]["polylines"][0]["points"] == [
        {"time": 8, "price": 8.0},
        {"time": 9, "price": 9.0},
    ]
    assert overlay["payload_summary"] == {
        "geometry_keys": ["markers", "polylines"],
        "payload_counts": {"markers": 2, "polylines": 1},
        "point_count": 2,
    }
    assert overlay["ui"] == {"label": "ATR", "color": "#38bdf8"}


def test_botlens_bootstrap_payload_emits_fact_batch_for_selected_series() -> None:
    runtime = _runtime()
    runtime.state = {"status": "running"}
    runtime._intrabar_manager = None
    runtime._botlens_fact_stream_log_fact_limit = 32
    runtime._botlens_fact_stream_decision_fact_limit = 64
    runtime._botlens_bootstrap_closed_trade_limit = 1
    runtime._push_wallet_marker = None
    runtime._run_context = SimpleNamespace(runtime_event_stream=[])
    runtime._series_state_for = lambda _series: SimpleNamespace(bar_index=1)
    runtime._series_visible_overlays = lambda selected, *, status: list(selected.overlays or [])

    def _visible_candles(selected, status, bar_index, intrabar_manager):
        _ = status, intrabar_manager
        return list(selected.candles[: bar_index + 1])

    def _serialise_trade_window(*, max_closed):
        assert max_closed == 1
        return [
            {
                "trade_id": "closed-new",
                "status": "closed",
                "entry_time": "2026-04-09T13:00:00Z",
                "closed_at": "2026-04-09T13:30:00Z",
                "direction": "short",
                "position_commit_seq": 2,
            },
            {
                "trade_id": "open-1",
                "status": "open",
                "entry_time": "2026-04-09T14:00:00Z",
                "direction": "long",
                "position_commit_seq": 1,
            },
        ]

    runtime._chart_state_builder = SimpleNamespace(visible_candles=_visible_candles)
    series = SimpleNamespace(
        instrument={"id": "instrument-bip"},
        timeframe="1h",
        strategy_id="strategy-1",
        symbol="BIP-20DEC30-CDE",
        datasource="COINBASE",
        exchange="coinbase_direct",
        candles=[
            {"time": 1, "open": 1.0, "high": 2.0, "low": 0.5, "close": 1.5},
            {"time": 2, "open": 1.5, "high": 2.5, "low": 1.0, "close": 2.0},
            {"time": 3, "open": 2.0, "high": 3.0, "low": 1.5, "close": 2.5},
        ],
        overlays=[{"type": "line", "value": 1.5}],
        risk_engine=SimpleNamespace(
            serialise_trade_window=_serialise_trade_window,
            serialise_trades=lambda: (_ for _ in ()).throw(AssertionError("full trade list was serialized")),
            stats=lambda: {"open_trades": 1, "fees_paid": 0.25},
        ),
    )
    other_series = SimpleNamespace(
        instrument={"id": "instrument-ignored"},
        timeframe="1h",
        strategy_id="strategy-2",
        symbol="IGNORED",
        datasource="COINBASE",
        exchange="coinbase_direct",
        candles=[],
        overlays=[],
        risk_engine=SimpleNamespace(
            serialise_trades=lambda: (_ for _ in ()).throw(AssertionError("other series trades were serialized")),
            stats=lambda: {},
        ),
    )
    runtime._series = [series, other_series]
    runtime.snapshot = lambda: {
        "status": "running",
        "known_at": "2026-04-09T14:00:00Z",
        "last_snapshot_at": "2026-04-09T14:00:00Z",
        "stats": {"bars_processed": 12},
    }
    runtime.chart_payload = lambda: (_ for _ in ()).throw(AssertionError("bootstrap should not build chart_payload"))
    runtime.logs = lambda limit=200: [{"id": "log-1", "message": "bootstrap"}]
    runtime.decision_events = lambda limit=200: [{"event_id": "decision-1", "action": "hold"}]

    payload = runtime.botlens_bootstrap_payload()

    assert payload["type"] == "facts"
    assert payload["event"] == "bootstrap"
    assert payload["series_key"] == "instrument-bip|1h"
    assert "projection" not in payload
    assert "runtime_delta" not in payload

    fact_types = [fact["fact_type"] for fact in payload["facts"]]
    assert "runtime_state_observed" in fact_types
    assert "series_state_observed" in fact_types
    assert fact_types.count("candle_upserted") == 2
    assert "overlay_ops_emitted" in fact_types
    assert "series_stats_updated" in fact_types
    assert "trade_opened" in fact_types
    assert "trade_closed" in fact_types
    assert "log_emitted" in fact_types
    assert "decision_emitted" in fact_types
    trade_ids = [
        fact["trade"]["trade_id"]
        for fact in payload["facts"]
        if fact["fact_type"] in {"trade_opened", "trade_closed"}
    ]
    assert "closed-old" not in trade_ids
    assert "closed-new" in trade_ids
    assert "open-1" in trade_ids


def test_wallet_facts_emit_full_entry_ledger_trace_in_logical_order() -> None:
    entry = {
        "event_id": "entry-event-1",
        "event_name": "ENTRY_FILLED",
        "seq": 12,
        "event_ts": "2026-02-01T00:00:00Z",
        "correlation_id": "trade:trade-1",
        "context": {
            "run_id": "run-1",
            "bot_id": "bot-1",
            "strategy_id": "strategy-1",
            "series_key": "instrument-btc|1h",
            "instrument_id": "instrument-btc",
            "symbol": "BTC-FUT",
            "timeframe": "1h",
            "bar_ts": "2026-02-01T00:00:00Z",
            "trade_id": "trade-1",
            "decision_id": "decision-1",
            "wallet_commit_seq": 7,
            "wallet_eval_seq": 6,
            "position_commit_seq": 1,
            "side": "buy",
            "direction": "long",
            "qty": 2.0,
            "price": 100.0,
            "notional": 200.0,
            "fee_paid": 0.4,
            "quote_currency": "USD",
            "required_delta": {
                "currency": "USD",
                "collateral_reserved": 100.0,
                "total_required_collateral": 100.4,
            },
            "wallet_delta": {
                "collateral_reserved": 100.0,
                "collateral_released": 0.0,
                "fee_paid": 0.4,
                "balance_delta": -0.4,
            },
            "wallet_before": {
                "balances": {"USD": 1000.0},
                "locked_margin": {"USD": 0.0},
                "free_collateral": {"USD": 1000.0},
            },
        },
    }

    facts = RuntimePushStreamMixin._wallet_facts_from_runtime_event(entry)
    wallet_events = [fact["wallet_event"] for fact in facts]

    assert [event["event_name"] for event in wallet_events] == [
        "MARGIN_RESERVED",
        "FEE_APPLIED",
        "POSITION_OPENED",
        "EQUITY_UPDATED",
    ]
    assert [event["event_id"] for event in wallet_events] == sorted(event["event_id"] for event in wallet_events)
    assert all(event["run_seq"] == 12 for event in wallet_events)
    assert all(event["run_seq_status"] == "runtime_assigned" for event in wallet_events)
    assert all(event["source_run_seq"] == 12 for event in wallet_events)
    assert all(event["wallet_commit_seq"] == 7 for event in wallet_events)
    assert all(event["wallet_eval_seq"] == 6 for event in wallet_events)
    assert all(event["position_commit_seq"] == 1 for event in wallet_events)
    assert [event["wallet_event_order"] for event in wallet_events] == [10, 20, 40, 50]
    assert all(event["decision_id"] == "decision-1" for event in wallet_events)
    assert wallet_events[0]["margin_required"] == 100.0
    assert wallet_events[0]["margin_available"] == 1000.0
    assert wallet_events[0]["balance_after"] == 1000.0
    assert wallet_events[0]["locked_margin_after"] == 100.0
    assert wallet_events[0]["free_collateral_after"] == 900.0
    assert wallet_events[0]["wallet_after"]["locked_margin"]["USD"] == 100.0
    assert wallet_events[1]["balance_before"] == 1000.0
    assert wallet_events[1]["balance_after"] == 999.6
    assert wallet_events[1]["fee"] == 0.4
    assert wallet_events[1]["free_collateral_before"] == 900.0
    assert wallet_events[1]["free_collateral_after"] == 899.6
    assert wallet_events[-1]["equity_after"] == 999.6


def test_wallet_initialized_round_trip_preserves_wallet_commit_clock() -> None:
    event = new_runtime_event(
        event_name=RuntimeEventName.WALLET_INITIALIZED,
        correlation_id="wallet:init",
        context=WalletInitializedContext(
            run_id="run-1",
            bot_id="bot-1",
            strategy_id="__runtime__",
            symbol=None,
            timeframe=None,
            bar_ts=datetime(2026, 1, 1, tzinfo=timezone.utc),
            balances={"USD": 1000.0},
            source="run_start",
            wallet_commit_seq=0,
            wallet_commit_seq_status="runtime_assigned",
            wallet_eval_seq=0,
        ),
        allow_missing_parent=True,
    )

    restored = runtime_event_from_dict(event.serialize())
    facts = RuntimePushStreamMixin._wallet_facts_from_runtime_event(restored.serialize())

    assert restored.context.wallet_commit_seq == 0
    assert restored.context.wallet_commit_seq_status == "runtime_assigned"
    assert restored.context.wallet_eval_seq == 0
    assert facts[0]["wallet_event"]["wallet_commit_seq"] == 0
    assert facts[0]["wallet_event"]["wallet_commit_seq_status"] == "runtime_assigned"


def test_wallet_facts_emit_exit_ledger_with_absolute_release_state() -> None:
    entry = {
        "event_id": "exit-event-1",
        "event_name": "EXIT_FILLED",
        "seq": 13,
        "event_ts": "2026-02-01T02:00:00Z",
        "correlation_id": "trade:trade-1",
        "context": {
            "run_id": "run-1",
            "bot_id": "bot-1",
            "strategy_id": "strategy-1",
            "series_key": "instrument-btc|1h",
            "instrument_id": "instrument-btc",
            "symbol": "BTC-FUT",
            "timeframe": "1h",
            "bar_ts": "2026-02-01T02:00:00Z",
            "trade_id": "trade-1",
            "decision_id": "decision-1",
            "wallet_commit_seq": 8,
            "wallet_eval_seq": 7,
            "position_commit_seq": 2,
            "side": "sell",
            "direction": "long",
            "qty": 2.0,
            "price": 105.0,
            "notional": 210.0,
            "fee_paid": 0.42,
            "realized_pnl": 10.0,
            "quote_currency": "USD",
            "wallet_delta": {
                "collateral_reserved": 0.0,
                "collateral_released": 100.0,
                "fee_paid": 0.42,
                "balance_delta": 9.58,
            },
            "wallet_before": {
                "balances": {"USD": 999.6},
                "locked_margin": {"USD": 100.0},
                "free_collateral": {"USD": 899.6},
                "margin_positions": {
                    "trade-1": {"currency": "USD", "open_qty": 2.0, "locked_margin": 100.0}
                },
            },
        },
    }

    facts = RuntimePushStreamMixin._wallet_facts_from_runtime_event(entry)
    wallet_events = [fact["wallet_event"] for fact in facts]

    assert [event["event_name"] for event in wallet_events] == [
        "MARGIN_RELEASED",
        "FEE_APPLIED",
        "REALIZED_PNL_APPLIED",
        "POSITION_CLOSED",
        "EQUITY_UPDATED",
    ]
    assert all(event["source_run_seq"] == 13 for event in wallet_events)
    assert all(event["wallet_commit_seq"] == 8 for event in wallet_events)
    assert all(event["wallet_eval_seq"] == 7 for event in wallet_events)
    assert all(event["position_commit_seq"] == 2 for event in wallet_events)
    assert [event["wallet_event_order"] for event in wallet_events] == [10, 20, 30, 40, 50]
    assert wallet_events[0]["balance_before"] == 999.6
    assert wallet_events[0]["balance_after"] == 999.6
    assert wallet_events[0]["margin_released"] == 100.0
    assert wallet_events[0]["locked_margin_after"] == 0.0
    assert wallet_events[0]["free_collateral_after"] == 999.6
    assert wallet_events[1]["balance_after"] == pytest.approx(999.18)
    assert wallet_events[2]["balance_after"] == pytest.approx(1009.18)
    assert wallet_events[-1]["wallet_after"]["balances"]["USD"] == pytest.approx(1009.18)


def test_wallet_facts_rebase_same_trade_partial_exit_before_state() -> None:
    runtime = _runtime()
    runtime._push_wallet_marker = None
    runtime._run_context = SimpleNamespace(runtime_event_stream=[])
    entry_before = {
        "balances": {"USD": 1000.0},
        "locked_margin": {"USD": 0.0},
        "free_collateral": {"USD": 1000.0},
        "margin_positions": {},
    }
    stale_exit_before = {
        "balances": {"USD": 999.0},
        "locked_margin": {"USD": 100.0},
        "free_collateral": {"USD": 899.0},
        "margin_positions": {
            "trade-1": {"currency": "USD", "open_qty": 2.0, "locked_margin": 100.0}
        },
    }

    def _exit_event(seq: int, event_id: str, *, realized_pnl: float) -> dict:
        return {
            "event_id": event_id,
            "event_name": "EXIT_FILLED",
            "seq": seq,
            "event_ts": "2026-02-01T02:00:00Z",
            "correlation_id": "trade:trade-1",
            "context": {
                "run_id": "run-1",
                "bot_id": "bot-1",
                "strategy_id": "strategy-1",
                "series_key": "instrument-btc|1h",
                "instrument_id": "instrument-btc",
                "symbol": "BTC-FUT",
                "timeframe": "1h",
                "bar_ts": "2026-02-01T02:00:00Z",
                "trade_id": "trade-1",
                "decision_id": "decision-1",
                "wallet_commit_seq": seq - 5,
                "wallet_eval_seq": seq - 6,
                "position_commit_seq": seq - 11,
                "side": "sell",
                "direction": "long",
                "qty": 1.0,
                "price": 105.0,
                "notional": 105.0,
                "fee_paid": 0.5,
                "realized_pnl": realized_pnl,
                "quote_currency": "USD",
                "wallet_delta": {
                    "collateral_reserved": 0.0,
                    "collateral_released": 50.0,
                    "fee_paid": 0.5,
                    "balance_delta": realized_pnl - 0.5,
                },
                "wallet_before": stale_exit_before,
            },
        }

    runtime._run_context.runtime_event_stream = [
        {
            "event_id": "entry-event-1",
            "event_name": "ENTRY_FILLED",
            "seq": 12,
            "event_ts": "2026-02-01T00:00:00Z",
            "correlation_id": "trade:trade-1",
            "context": {
                "run_id": "run-1",
                "bot_id": "bot-1",
                "strategy_id": "strategy-1",
                "series_key": "instrument-btc|1h",
                "instrument_id": "instrument-btc",
                "symbol": "BTC-FUT",
                "timeframe": "1h",
                "bar_ts": "2026-02-01T00:00:00Z",
                "trade_id": "trade-1",
                "decision_id": "decision-1",
                "wallet_commit_seq": 7,
                "wallet_eval_seq": 6,
                "position_commit_seq": 1,
                "side": "buy",
                "direction": "long",
                "qty": 2.0,
                "price": 100.0,
                "notional": 200.0,
                "fee_paid": 1.0,
                "quote_currency": "USD",
                "wallet_delta": {
                    "collateral_reserved": 100.0,
                    "collateral_released": 0.0,
                    "fee_paid": 1.0,
                    "balance_delta": -1.0,
                },
                "wallet_before": entry_before,
            },
        },
        _exit_event(13, "exit-event-1", realized_pnl=10.0),
        _exit_event(14, "exit-event-2", realized_pnl=11.0),
    ]

    wallet_events = [
        fact["wallet_event"]
        for fact in runtime._wallet_facts()
        if fact["fact_type"] == "wallet_ledger_event"
    ]

    second_release = next(
        event
        for event in wallet_events
        if event["source_event_id"] == "exit-event-2" and event["event_name"] == "MARGIN_RELEASED"
    )
    assert second_release["balance_before"] == pytest.approx(1008.5)
    assert second_release["locked_margin_before"] == pytest.approx(50.0)
    assert second_release["free_collateral_before"] == pytest.approx(958.5)
    assert second_release["wallet_before"]["margin_positions"]["trade-1"]["open_qty"] == pytest.approx(1.0)


def test_wallet_facts_emit_margin_rejection_with_full_evidence() -> None:
    entry = {
        "event_id": "decision-event-1",
        "event_name": "DECISION_REJECTED",
        "seq": 22,
        "event_ts": "2026-02-01T00:00:00Z",
        "correlation_id": "decision:decision-1",
        "context": {
            "run_id": "run-1",
            "bot_id": "bot-1",
            "strategy_id": "strategy-1",
            "series_key": "instrument-btc|1h",
            "instrument_id": "instrument-btc",
            "symbol": "BTC-FUT",
            "timeframe": "1h",
            "bar_ts": "2026-02-01T00:00:00Z",
            "decision_id": "decision-1",
            "wallet_commit_seq": 9,
            "wallet_eval_seq": 8,
            "reason_code": "WALLET_INSUFFICIENT_MARGIN",
            "message": "WALLET_INSUFFICIENT_MARGIN",
            "direction": "long",
            "signal_price": 100.0,
            "wallet_snapshot": {
                "balances": {"USD": 100.0},
                "locked_margin": {"USD": 0.0},
                "free_collateral": {"USD": 100.0},
            },
            "margin_requirement": {
                "currency": "USD",
                "total_required_collateral": 120.0,
            },
        },
    }

    facts = RuntimePushStreamMixin._wallet_facts_from_runtime_event(entry)
    wallet_event = facts[0]["wallet_event"]

    assert wallet_event["event_name"] == "MARGIN_REJECTED"
    assert wallet_event["decision_id"] == "decision-1"
    assert wallet_event["balance_before"] == 100.0
    assert wallet_event["margin_available"] == 100.0
    assert wallet_event["margin_required"] == 120.0
    assert wallet_event["wallet_before"]["balances"]["USD"] == 100.0
    assert wallet_event["wallet_after"]["balances"]["USD"] == 100.0


def test_commit_botlens_fact_payload_uses_configured_run_id_before_run_context_exists() -> None:
    runtime = _runtime()
    appended: list[dict[str, object]] = []
    runtime.bot_id = "bot-1"
    runtime.config = {"run_id": "run-1", "worker_id": "worker-1"}
    runtime._run_context = None
    runtime._canonical_fact_appender = SimpleNamespace(
        append_fact_batch=lambda **kwargs: appended.append(dict(kwargs)) or {"ok": True},
    )

    result = runtime.commit_botlens_fact_payload(
        {
            "series_key": "instrument-bip|1h",
            "known_at": "2026-04-09T14:00:00Z",
            "facts": [
                {
                    "fact_type": "candle_upserted",
                    "series_key": "instrument-bip|1h",
                    "candle": {"time": "2026-04-09T14:00:00Z"},
                }
            ],
        },
        batch_kind="botlens_runtime_bootstrap_facts",
        dispatch=False,
    )

    assert result == {"ok": True}
    assert appended == [
        {
            "bot_id": "bot-1",
            "run_id": "run-1",
            "batch_kind": "botlens_runtime_bootstrap_facts",
            "payload": {
                "series_key": "instrument-bip|1h",
                "known_at": "2026-04-09T14:00:00Z",
                "facts": [
                    {
                        "fact_type": "candle_upserted",
                        "series_key": "instrument-bip|1h",
                        "candle": {"time": "2026-04-09T14:00:00Z"},
                    }
                ],
            },
            "context": {
                "worker_id": "worker-1",
                "source_emitter": "bot_runtime",
                "source_reason": "producer",
            },
            "dispatch": False,
        }
    ]


def test_trade_payload_is_open_rejects_closed_status_without_closed_at() -> None:
    runtime = _runtime()

    with pytest.raises(RuntimeError, match="closed trade snapshot missing closed_at"):
        runtime._trade_payload_is_open({"trade_id": "trade-1", "status": "closed"})


def test_rejection_metadata_uses_attempt_id_instead_of_trade_id() -> None:
    trade_id, metadata = RuntimeEventsMixin._normalise_rejection_metadata(
        {"trade_id": "pending-trade-1", "order_request_id": "order-1"},
        blocking_trade_id=None,
    )

    assert trade_id is None
    assert metadata["attempt_id"] == "pending-trade-1"
    assert metadata["settlement_attempt_id"] == "pending-trade-1"
    assert metadata["order_request_id"] == "order-1"
    assert "trade_id" not in metadata


def test_rejection_metadata_uses_entry_request_id_as_attempt_identity() -> None:
    trade_id, metadata = RuntimeEventsMixin._normalise_rejection_metadata(
        {
            "entry_request_id": "entry_request:abc",
            "reason": "WALLET_INSUFFICIENT_MARGIN",
        },
        blocking_trade_id=None,
    )

    assert trade_id is None
    assert metadata["entry_request_id"] == "entry_request:abc"
    assert metadata["attempt_id"] == "entry_request:abc"
    assert "trade_id" not in metadata


def test_rejected_attempt_identity_failsafe_derives_stable_entry_request_id() -> None:
    context = {
        "run_id": "run-1",
        "strategy_id": "strategy-1",
        "instrument_id": "instrument-btc",
        "symbol": "BTC",
        "timeframe": "1h",
        "bar_time": "2026-02-01T00:00:00Z",
        "decision_id": "decision-1",
        "signal_id": "signal-1",
        "direction": "long",
        "event_key": "breakout-long",
        "attempt_kind": "entry_request",
    }

    first = RuntimeEventsMixin._ensure_rejected_attempt_identity(context)
    second = RuntimeEventsMixin._ensure_rejected_attempt_identity(context)
    changed = RuntimeEventsMixin._ensure_rejected_attempt_identity({**context, "decision_id": "decision-2"})

    assert first["entry_request_id"].startswith("entry_request:")
    assert first["attempt_id"] == first["entry_request_id"]
    assert first["entry_request_id"] == second["entry_request_id"]
    assert first["entry_request_id"] != changed["entry_request_id"]


def test_rejected_attempt_identity_failsafe_preserves_source_identity() -> None:
    context = RuntimeEventsMixin._ensure_rejected_attempt_identity(
        {
            "entry_request_id": "entry_request:source",
            "attempt_id": None,
            "decision_id": "decision-1",
        }
    )

    assert context["entry_request_id"] == "entry_request:source"
    assert context["attempt_id"] == "entry_request:source"


def test_emit_rejected_decision_adds_attempt_identity_when_source_lost() -> None:
    runtime = _EventRuntime()
    candle = Candle(
        time=datetime(2026, 2, 1, tzinfo=timezone.utc),
        open=100.0,
        high=101.0,
        low=99.0,
        close=100.0,
        atr=1.0,
    )
    series = SimpleNamespace(
        strategy_id="strategy-1",
        symbol="BTC",
        timeframe="1h",
        instrument={"id": "instrument-btc"},
    )
    signal = StrategySignal(
        epoch=int(candle.time.timestamp()),
        direction="long",
        signal_id="signal-1",
        decision_id="decision-1",
        rule_id="rule-1",
        intent="enter_long",
        event_key="breakout-long",
    )

    event = runtime._emit_decision_event(
        series=series,
        candle=candle,
        signal=signal,
        decision="rejected",
        decision_artifact={"decision_id": "decision-1"},
        rejection_artifact={
            "context": {
                "wallet_snapshot": {
                    "balances": {"USD": 10.0},
                    "locked_margin": {"USD": 0.0},
                    "free_collateral": {"USD": 10.0},
                    "margin_positions": {},
                },
                "margin_requirement": {"currency": "USD", "total_required_collateral": 20.0},
            }
        },
        signal_price=100.0,
        reason_code="WALLET_INSUFFICIENT_MARGIN",
        message="WALLET_INSUFFICIENT_MARGIN",
        trade_id=None,
    )

    assert event.context.trade_id is None
    assert event.context.entry_request_id is not None
    assert event.context.entry_request_id.startswith("entry_request:")
    assert event.context.attempt_id == event.context.entry_request_id
    assert event.context.reason_code.value == "WALLET_INSUFFICIENT_MARGIN"
    assert event.context.rejection_artifact["context"]["entry_request_id"] == event.context.entry_request_id


def test_emit_rejected_decision_preserves_wallet_evidence_when_artifact_is_sanitized() -> None:
    runtime = _EventRuntime()
    candle = Candle(
        time=datetime(2026, 2, 1, tzinfo=timezone.utc),
        open=100.0,
        high=101.0,
        low=99.0,
        close=100.0,
        atr=1.0,
    )
    series = SimpleNamespace(
        strategy_id="strategy-1",
        symbol="BTC",
        timeframe="1h",
        instrument={"id": "instrument-btc"},
    )
    signal = StrategySignal(
        epoch=int(candle.time.timestamp()),
        direction="long",
        signal_id="signal-1",
        decision_id="decision-1",
        rule_id="rule-1",
        intent="enter_long",
        event_key="breakout-long",
    )

    event = runtime._emit_decision_event(
        series=series,
        candle=candle,
        signal=signal,
        decision="rejected",
        decision_artifact={"decision_id": "decision-1"},
        rejection_artifact={"context": {"entry_request_id": "entry_request:source"}},
        signal_price=100.0,
        reason_code="WALLET_INSUFFICIENT_MARGIN",
        message="WALLET_INSUFFICIENT_MARGIN",
        trade_id=None,
        wallet_evidence={
            "wallet_snapshot": {
                "balances": {"USD": 100.0},
                "locked_margin": {"USD": 0.0},
                "free_collateral": {"USD": 100.0},
                "margin_positions": {},
            },
            "margin_requirement": {
                "currency": "USD",
                "total_required_collateral": 120.0,
            },
        },
    )

    assert event.context.wallet_snapshot["balances"]["USD"] == 100.0
    assert event.context.margin_requirement["total_required_collateral"] == 120.0
    assert event.context.wallet_snapshot["free_collateral"]["USD"] == 100.0
    assert runtime._run_context.runtime_event_stream[-1]["context"]["attempt_id"] == event.context.entry_request_id


def test_emit_terminal_exit_fill_uses_backtest_end_reason() -> None:
    runtime = _EventRuntime()
    candle = Candle(
        time=datetime(2026, 2, 1, 2, tzinfo=timezone.utc),
        open=104.0,
        high=106.0,
        low=103.0,
        close=105.0,
        atr=1.0,
    )
    series = SimpleNamespace(
        strategy_id="strategy-1",
        symbol="BTC",
        timeframe="1h",
        instrument={"id": "instrument-btc"},
        risk_engine=SimpleNamespace(
            contract_size=2.0,
            base_currency="BTC",
            quote_currency="USD",
        ),
        execution_profile=SimpleNamespace(accounting_mode="margin"),
    )
    entry_time = datetime(2026, 2, 1, tzinfo=timezone.utc)
    runtime._run_context.runtime_events.append(
        new_runtime_event(
            event_name=RuntimeEventName.WALLET_INITIALIZED,
            correlation_id="wallet:init",
            context=WalletInitializedContext(
                run_id="run-1",
                bot_id="bot-1",
                strategy_id="strategy-1",
                series_key="instrument-btc|1h",
                instrument_id="instrument-btc",
                symbol="BTC",
                timeframe="1h",
                bar_ts=entry_time,
                balances={"USD": 1000.0},
                source="test",
            ),
            allow_missing_parent=True,
        )
    )
    entry = new_runtime_event(
        event_name=RuntimeEventName.ENTRY_FILLED,
        correlation_id=build_correlation_id(
            run_id="run-1",
            symbol="BTC",
            timeframe="1h",
            bar_ts=entry_time,
        ),
        context=EntryFilledContext(
            run_id="run-1",
            bot_id="bot-1",
            strategy_id="strategy-1",
            series_key="instrument-btc|1h",
            instrument_id="instrument-btc",
            symbol="BTC",
            timeframe="1h",
            bar_ts=entry_time,
            trade_id="trade-1",
            wallet_correlation_id="trade:trade-1",
            side="buy",
            direction="long",
            qty=2.0,
            price=100.0,
            notional=400.0,
            fee_paid=0.4,
            base_currency="BTC",
            quote_currency="USD",
            accounting_mode="margin",
            wallet_delta=WalletDelta(
                collateral_reserved=100.0,
                collateral_released=0.0,
                fee_paid=0.4,
                balance_delta=-0.4,
            ),
        ),
        allow_missing_parent=True,
    )
    runtime._run_context.runtime_events.append(entry)

    event = runtime._emit_exit_filled_event(
        series=series,
        candle=candle,
        event={
            "type": "backtest_end",
            "trade_id": "trade-1",
            "contracts": 2.0,
            "price": 105.0,
            "time": "2026-02-01T02:00:00Z",
            "direction": "long",
            "currency": "USD",
            "pnl": 10.0,
            "fee_paid": 0.42,
            "reason_code": "BACKTEST_END",
            "wallet_fill_metadata": {
                "correlation_id": "trade:trade-1",
                "wallet_before": {
                    "balances": {"USD": 999.6},
                    "locked_margin": {"USD": 100.0},
                    "free_collateral": {"USD": 899.6},
                    "margin_positions": {
                        "trade-1": {"currency": "USD", "open_qty": 2.0, "locked_margin": 100.0}
                    },
                },
                "wallet_delta": {
                    "collateral_reserved": 0.0,
                    "collateral_released": 100.0,
                    "fee_paid": 0.42,
                    "balance_delta": 9.58,
                },
            },
        },
    )

    assert event.context.exit_kind == ExitKind.CLOSE
    assert event.context.event_subtype == "backtest_end"
    assert event.context.reason_code == ReasonCode.BACKTEST_END
    assert event.context.wallet_delta.collateral_released == 100.0
    assert event.context.wallet_before["balances"]["USD"] == pytest.approx(999.6)


def test_trade_facts_emit_open_before_close_when_first_observed_already_closed() -> None:
    runtime = _runtime()
    trade_payload = {
        "trade_id": "trade-1",
        "status": "closed",
        "entry_time": "2026-04-09T13:55:00Z",
        "closed_at": "2026-04-09T14:00:00Z",
        "direction": "long",
        "reason_code": "BACKTEST_END",
        "position_commit_seq": 2,
    }
    series = _trade_series(trade_payload, revision=1)

    trade_facts, _stats, _count, refresh_required = runtime._trade_facts(series=series, cache={})

    assert [fact["fact_type"] for fact in trade_facts] == ["trade_opened", "trade_closed"]
    assert trade_facts[0]["trade"]["bar_time"] == "2026-04-09T13:55:00Z"
    assert trade_facts[0]["trade"]["event_time"] == "2026-04-09T13:55:00Z"
    assert trade_facts[0]["trade"]["status"] == "open"
    assert trade_facts[0]["trade"]["position_commit_seq"] == 1
    assert "closed_at" not in trade_facts[0]["trade"]
    assert "reason_code" not in trade_facts[0]["trade"]
    assert trade_facts[1]["trade"]["bar_time"] == "2026-04-09T14:00:00Z"
    assert trade_facts[1]["trade"]["event_time"] == "2026-04-09T14:00:00Z"
    assert trade_facts[1]["trade"]["position_commit_seq"] == 2
    assert trade_facts[1]["trade"]["reason_code"] == "BACKTEST_END"
    assert refresh_required is True


def test_trade_facts_same_bar_open_close_emits_deterministic_lifecycle_order() -> None:
    runtime = _runtime()
    trade_payload = {
        "trade_id": "trade-1",
        "status": "closed",
        "entry_time": "2026-04-09T14:00:00Z",
        "closed_at": "2026-04-09T14:00:00Z",
        "direction": "long",
        "position_commit_seq": 2,
    }
    series = _trade_series(trade_payload, revision=1)

    trade_facts, _stats, _count, _refresh = runtime._trade_facts(series=series, cache={})

    assert [fact["fact_type"] for fact in trade_facts] == ["trade_opened", "trade_closed"]
    assert [fact["trade"]["bar_time"] for fact in trade_facts] == [
        "2026-04-09T14:00:00Z",
        "2026-04-09T14:00:00Z",
    ]
    assert [fact["trade"]["position_commit_seq"] for fact in trade_facts] == [1, 2]


def test_trade_facts_do_not_duplicate_open_for_previously_opened_trade() -> None:
    runtime = _runtime()
    trade_payload = {
        "trade_id": "trade-1",
        "status": "closed",
        "entry_time": "2026-04-09T13:00:00Z",
        "closed_at": "2026-04-09T14:00:00Z",
        "direction": "long",
        "position_commit_seq": 2,
    }
    series = _trade_series(trade_payload, revision=2)
    cache = {
        "trades_revision": 1,
        "trade_fingerprints": {},
        "emitted_trade_ids": ("trade-1",),
        "emitted_open_trade_ids": ("trade-1",),
        "emitted_closed_trade_ids": (),
        "open_trade_ids": ("trade-1",),
    }

    trade_facts, _stats, _count, _refresh = runtime._trade_facts(series=series, cache=cache)

    assert [fact["fact_type"] for fact in trade_facts] == ["trade_closed"]
    assert trade_facts[0]["trade"]["bar_time"] == "2026-04-09T14:00:00Z"


def test_trade_facts_reject_missing_position_commit_seq() -> None:
    runtime = _runtime()
    trade_payload = {
        "trade_id": "trade-1",
        "status": "open",
        "entry_time": "2026-04-09T13:00:00Z",
        "direction": "long",
    }
    series = _trade_series(trade_payload, revision=1)

    with pytest.raises(RuntimeError, match="position_commit_seq is required"):
        runtime._trade_facts(series=series, cache={})


def test_trade_facts_build_domain_events_with_required_lifecycle_and_simulated_times() -> None:
    runtime = _runtime()
    trade_payload = {
        "trade_id": "trade-1",
        "status": "closed",
        "entry_time": "2026-04-09T13:55:00Z",
        "closed_at": "2026-04-09T14:00:00Z",
        "direction": "long",
        "strategy_id": "strategy-1",
        "signal_id": "signal-1",
        "decision_id": "decision-1",
        "position_commit_seq": 2,
    }
    series = _trade_series(trade_payload, revision=1)

    trade_facts, _stats, _count, _refresh = runtime._trade_facts(series=series, cache={})
    events = build_botlens_domain_events_from_fact_batch(
        bot_id="bot-1",
        run_id="run-1",
        payload={
            "known_at": "2026-04-09T14:00:00Z",
            "observed_at": "2026-04-09T14:00:01Z",
            "facts": [
                {
                    "fact_type": "series_state_observed",
                    "series_key": "instrument-bip|1h",
                    "instrument_id": "instrument-bip",
                    "symbol": "BIP-20DEC30-CDE",
                    "timeframe": "1h",
                },
                *trade_facts,
            ],
        },
    )
    trade_events = [serialize_botlens_domain_event(event) for event in events if event.event_name.value.startswith("TRADE_")]

    assert [event["event_name"] for event in trade_events] == ["TRADE_OPENED", "TRADE_CLOSED"]
    assert [event["context"]["position_commit_seq"] for event in trade_events] == [1, 2]
    assert trade_events[0]["context"]["bar_time"] == "2026-04-09T13:55:00Z"
    assert trade_events[0]["context"]["event_time"] == "2026-04-09T13:55:00Z"
    assert trade_events[1]["context"]["bar_time"] == "2026-04-09T14:00:00Z"
    assert trade_events[1]["context"]["event_time"] == "2026-04-09T14:00:00Z"


def test_trade_facts_enrich_trade_bar_time_and_decision_lineage() -> None:
    runtime = _runtime()
    runtime._run_context = SimpleNamespace(
        runtime_events=[
            SimpleNamespace(
                event_name=SimpleNamespace(value="DECISION_ACCEPTED"),
                context=SimpleNamespace(
                    trade_id="trade-1",
                    strategy_id="strategy-1",
                    signal_id="signal-1",
                    decision_id="decision-1",
                ),
            )
        ]
    )
    trade_payload = {
        "trade_id": "trade-1",
        "status": "open",
        "entry_time": "2026-02-01T00:05:00Z",
        "direction": "long",
        "position_commit_seq": 1,
    }
    series = SimpleNamespace(
        instrument={"id": "instrument-bip"},
        timeframe="1h",
        strategy_id="strategy-1",
        symbol="BIP-20DEC30-CDE",
        datasource="COINBASE",
        exchange="coinbase_direct",
        risk_engine=SimpleNamespace(
            trade_revision=1,
            serialise_trades=lambda: [dict(trade_payload)],
            stats=lambda: {},
        ),
    )

    trade_facts, _stats, _count, _refresh = runtime._trade_facts(series=series, cache={})

    trade = trade_facts[0]["trade"]
    assert trade["bar_time"] == "2026-02-01T00:05:00Z"
    assert trade["event_time"] == "2026-02-01T00:05:00Z"
    assert trade["strategy_id"] == "strategy-1"
    assert trade["signal_id"] == "signal-1"
    assert trade["decision_id"] == "decision-1"
    assert trade["position_commit_seq"] == 1


def test_trade_facts_skip_noop_active_bar_when_trade_revision_unchanged() -> None:
    runtime = _runtime()
    trade_payload = {
        "trade_id": "trade-1",
        "status": "open",
        "entry_time": "2026-02-01T00:05:00Z",
        "direction": "long",
        "bars_held": 1,
        "position_commit_seq": 1,
        "metrics": {"bars_held": 1, "mfe_ticks": 1.0},
    }
    serialise_calls = 0

    def _serialise_trades():
        nonlocal serialise_calls
        serialise_calls += 1
        return [dict(trade_payload)]

    series = SimpleNamespace(
        instrument={"id": "instrument-bip"},
        timeframe="1h",
        strategy_id="strategy-1",
        symbol="BIP-20DEC30-CDE",
        datasource="COINBASE",
        exchange="coinbase_direct",
        risk_engine=SimpleNamespace(
            trade_revision=1,
            serialise_trades=_serialise_trades,
            stats=lambda: {},
        ),
    )
    cache = {}

    first_facts, _stats, _count, _refresh = runtime._trade_facts(series=series, cache=cache)
    trade_payload["bars_held"] = 2
    trade_payload["metrics"] = {"bars_held": 2, "mfe_ticks": 3.0}
    second_facts, _stats, _count, _refresh = runtime._trade_facts(series=series, cache=cache)

    assert [fact["fact_type"] for fact in first_facts] == ["trade_opened"]
    assert second_facts == []
    assert serialise_calls == 1


def test_trade_facts_use_engine_cursor_changes_without_full_trade_serialization() -> None:
    runtime = _runtime()
    calls: list[object] = []
    cursor_batches = [
        {
            "from_revision": 0,
            "to_revision": 1,
            "total_trades": 10,
            "cursor_expired": False,
            "trades": [
                {
                    "trade_id": "trade-1",
                    "status": "open",
                    "entry_time": "2026-02-01T00:05:00Z",
                    "direction": "long",
                    "position_commit_seq": 1,
                }
            ],
        },
        {
            "from_revision": 1,
            "to_revision": 2,
            "total_trades": 10,
            "cursor_expired": False,
            "trades": [
                {
                    "trade_id": "trade-1",
                    "status": "closed",
                    "entry_time": "2026-02-01T00:05:00Z",
                    "closed_at": "2026-02-01T01:00:00Z",
                    "direction": "long",
                    "position_commit_seq": 2,
                }
            ],
        },
    ]

    def _serialise_trade_changes_since(cursor_revision):
        calls.append(cursor_revision)
        return cursor_batches.pop(0)

    series = SimpleNamespace(
        instrument={"id": "instrument-bip"},
        timeframe="1h",
        strategy_id="strategy-1",
        symbol="BIP-20DEC30-CDE",
        datasource="COINBASE",
        exchange="coinbase_direct",
        risk_engine=SimpleNamespace(
            trade_revision=1,
            serialise_trade_changes_since=_serialise_trade_changes_since,
            serialise_trades=lambda: (_ for _ in ()).throw(AssertionError("full trade list was serialized")),
            stats=lambda: {},
        ),
    )
    cache = {}

    first_facts, _stats, first_count, _refresh = runtime._trade_facts(series=series, cache=cache)
    series.risk_engine.trade_revision = 2
    second_facts, _stats, second_count, _refresh = runtime._trade_facts(series=series, cache=cache)

    assert calls == [None, 1]
    assert first_count == 10
    assert second_count == 10
    assert [fact["fact_type"] for fact in first_facts] == ["trade_opened"]
    assert [fact["fact_type"] for fact in second_facts] == ["trade_closed"]
    assert cache["trade_cursor_revision"] == 2


def test_push_update_keeps_overlay_facts_off_the_first_live_bar_and_emits_after_visual_refresh_interval() -> None:
    runtime = _PushRuntime()
    series = SimpleNamespace(
        instrument={"id": "instrument-bip"},
        timeframe="1h",
        strategy_id="strategy-1",
        symbol="BIP-20DEC30-CDE",
        datasource="COINBASE",
        exchange="coinbase_direct",
        overlays=[{"overlay_id": "overlay-1", "type": "regime_overlay", "payload": {"blocks": []}}],
        trade_overlay=None,
        candles=[{"time": 1}, {"time": 2}],
        risk_engine=SimpleNamespace(trade_revision=0, serialise_trades=lambda: [], stats=lambda: {}),
    )
    runtime._series = [series]

    candle = SimpleNamespace(
        to_dict=lambda: {"time": 2, "open": 1.5, "high": 2.5, "low": 1.0, "close": 2.0},
    )

    with patch("engines.bot_runtime.runtime.mixins.runtime_push_stream.time.monotonic", return_value=10.0):
        runtime._push_update("bar", series=series, candle=candle)

    first_fact_types = [fact["fact_type"] for fact in runtime.broadcast_payloads[0]["facts"]]
    assert "overlay_ops_emitted" not in first_fact_types

    with patch("engines.bot_runtime.runtime.mixins.runtime_push_stream.time.monotonic", return_value=15.0):
        runtime._push_update("bar", series=series, candle=candle)

    second_fact_types = [fact["fact_type"] for fact in runtime.broadcast_payloads[1]["facts"]]
    assert "overlay_ops_emitted" in second_fact_types


def test_push_update_defers_visual_overlay_refresh_until_refresh_interval() -> None:
    runtime = _PushRuntime()
    refresh_calls = 0

    def _refresh_indicator_overlays_for_state(_state, **_kwargs):
        nonlocal refresh_calls
        refresh_calls += 1

    runtime._refresh_indicator_overlays_for_state = _refresh_indicator_overlays_for_state
    series = SimpleNamespace(
        instrument={"id": "instrument-bip"},
        timeframe="1h",
        strategy_id="strategy-1",
        symbol="BIP-20DEC30-CDE",
        datasource="COINBASE",
        exchange="coinbase_direct",
        overlays=[{"overlay_id": "overlay-1", "type": "regime_overlay", "payload": {"blocks": []}}],
        trade_overlay=None,
        candles=[{"time": 1}, {"time": 2}, {"time": 3}],
        risk_engine=SimpleNamespace(trade_revision=0, serialise_trades=lambda: [], stats=lambda: {}),
    )
    runtime._series = [series]
    candle = SimpleNamespace(
        to_dict=lambda: {"time": 2, "open": 1.5, "high": 2.5, "low": 1.0, "close": 2.0},
    )

    with patch("engines.bot_runtime.runtime.mixins.runtime_push_stream.time.monotonic", return_value=10.0):
        runtime._push_update("bar", series=series, candle=candle)
    with patch("engines.bot_runtime.runtime.mixins.runtime_push_stream.time.monotonic", return_value=12.0):
        runtime._push_update("bar", series=series, candle=candle)

    assert refresh_calls == 0

    with patch("engines.bot_runtime.runtime.mixins.runtime_push_stream.time.monotonic", return_value=15.0):
        runtime._push_update("bar", series=series, candle=candle)

    assert refresh_calls == 1


def test_push_update_coalesces_unchanged_series_stats() -> None:
    runtime = _PushRuntime()
    series = SimpleNamespace(
        instrument={"id": "instrument-bip"},
        timeframe="1h",
        strategy_id="strategy-1",
        symbol="BIP-20DEC30-CDE",
        datasource="COINBASE",
        exchange="coinbase_direct",
        overlays=[],
        trade_overlay=None,
        candles=[{"time": 1}, {"time": 2}],
        risk_engine=SimpleNamespace(
            trade_revision=0,
            serialise_trades=lambda: [],
            stats=lambda: {"total_trades": 0, "net_pnl": 0.0},
        ),
    )
    runtime._series = [series]
    candle = SimpleNamespace(
        to_dict=lambda: {"time": 2, "open": 1.5, "high": 2.5, "low": 1.0, "close": 2.0},
    )

    with patch("engines.bot_runtime.runtime.mixins.runtime_push_stream.time.monotonic", return_value=10.0):
        runtime._push_update("bar", series=series, candle=candle)
    with patch("engines.bot_runtime.runtime.mixins.runtime_push_stream.time.monotonic", return_value=11.0):
        runtime._push_update("bar", series=series, candle=candle)

    first_fact_types = [fact["fact_type"] for fact in runtime.broadcast_payloads[0]["facts"]]
    second_fact_types = [fact["fact_type"] for fact in runtime.broadcast_payloads[1]["facts"]]

    assert "series_stats_updated" in first_fact_types
    assert "series_stats_updated" not in second_fact_types


def test_push_update_bounds_live_log_and_decision_fact_batches() -> None:
    runtime = _PushRuntime()
    runtime._botlens_fact_stream_log_fact_limit = 2
    runtime._botlens_fact_stream_decision_fact_limit = 3
    runtime.logs = lambda: [{"id": f"log-{index}"} for index in range(5)]
    runtime.decision_events = lambda: [{"event_id": f"decision-{index}"} for index in range(6)]
    series = SimpleNamespace(
        instrument={"id": "instrument-bip"},
        timeframe="1h",
        strategy_id="strategy-1",
        symbol="BIP-20DEC30-CDE",
        datasource="COINBASE",
        exchange="coinbase_direct",
        overlays=[],
        trade_overlay=None,
        candles=[{"time": 1}, {"time": 2}],
        risk_engine=SimpleNamespace(trade_revision=0, serialise_trades=lambda: [], stats=lambda: {}),
    )
    runtime._series = [series]
    candle = SimpleNamespace(
        to_dict=lambda: {"time": 2, "open": 1.5, "high": 2.5, "low": 1.0, "close": 2.0},
    )

    runtime._push_update("bar", series=series, candle=candle)

    facts = runtime.broadcast_payloads[0]["facts"]
    logs = [fact["log"] for fact in facts if fact["fact_type"] == "log_emitted"]
    decisions = [fact["decision"] for fact in facts if fact["fact_type"] == "decision_emitted"]

    assert [entry["id"] for entry in logs] == ["log-3", "log-4"]
    assert [entry["event_id"] for entry in decisions] == ["decision-3", "decision-4", "decision-5"]


def test_visual_overlay_refresh_trigger_allows_immediate_emit_on_trade_entry() -> None:
    runtime = _runtime()
    cache = {}
    overlay_revision = ("running", ("overlay-1",))

    with patch("engines.bot_runtime.runtime.mixins.runtime_push_stream.time.monotonic", return_value=10.0):
        assert runtime._should_emit_visual_overlay_facts(
            cache,
            event="bar",
            overlay_revision=overlay_revision,
            trade_entry_refresh_required=False,
        ) is False

    with patch("engines.bot_runtime.runtime.mixins.runtime_push_stream.time.monotonic", return_value=10.1):
        assert runtime._should_emit_visual_overlay_facts(
            cache,
            event="bar",
            overlay_revision=("running", ("overlay-2",)),
            trade_entry_refresh_required=True,
        ) is True


def test_push_update_coalesces_repeated_runtime_warning_counts_until_health_heartbeat() -> None:
    runtime = _PushRuntime()
    runtime._runtime_health_emit_interval_ms = 5_000
    warning = {
        "warning_id": "indicator::budget::instrument-bip|1h",
        "warning_type": "execution_budget_exceeded",
        "severity": "warning",
        "source": "runtime",
        "symbol_key": "instrument-bip|1h",
        "symbol": "BIP-20DEC30-CDE",
        "timeframe": "1h",
        "message": "Indicator execution budget exceeded",
        "count": 1,
        "last_seen_at": "2026-04-09T14:00:00Z",
        "context": {"indicator_id": "indicator-1", "budget_ms": 35.0, "observed_ms": 76.0},
    }
    runtime_snapshot = {
        "status": "running",
        "runtime_state": "live",
        "progress_state": "progressing",
        "known_at": "2026-04-09T14:00:00Z",
        "last_snapshot_at": "2026-04-09T14:00:00Z",
        "warnings": [dict(warning)],
    }
    snapshot_calls = 0

    def _snapshot():
        nonlocal snapshot_calls
        snapshot_calls += 1
        return {
            key: ([dict(entry) for entry in value] if isinstance(value, list) else value)
            for key, value in runtime_snapshot.items()
        }

    runtime.snapshot = _snapshot
    series = SimpleNamespace(
        instrument={"id": "instrument-bip"},
        timeframe="1h",
        strategy_id="strategy-1",
        symbol="BIP-20DEC30-CDE",
        datasource="COINBASE",
        exchange="coinbase_direct",
        overlays=[],
        trade_overlay=None,
        candles=[{"time": 1}, {"time": 2}],
        risk_engine=SimpleNamespace(trade_revision=0, serialise_trades=lambda: [], stats=lambda: {}),
    )
    runtime._series = [series]
    candle = SimpleNamespace(
        to_dict=lambda: {"time": 2, "open": 1.5, "high": 2.5, "low": 1.0, "close": 2.0},
    )

    with patch("engines.bot_runtime.runtime.mixins.runtime_push_stream.time.monotonic", return_value=10.0):
        runtime._push_update("bar", series=series, candle=candle)

    runtime_snapshot["warnings"] = [{**warning, "count": 2, "last_seen_at": "2026-04-09T14:00:01Z"}]
    with patch("engines.bot_runtime.runtime.mixins.runtime_push_stream.time.monotonic", return_value=11.0):
        runtime._push_update("bar", series=series, candle=candle)

    runtime_snapshot["warnings"] = [{**warning, "count": 3, "last_seen_at": "2026-04-09T14:00:07Z"}]
    with patch("engines.bot_runtime.runtime.mixins.runtime_push_stream.time.monotonic", return_value=16.0):
        runtime._push_update("bar", series=series, candle=candle)

    first_fact_types = [fact["fact_type"] for fact in runtime.broadcast_payloads[0]["facts"]]
    second_fact_types = [fact["fact_type"] for fact in runtime.broadcast_payloads[1]["facts"]]
    third_fact_types = [fact["fact_type"] for fact in runtime.broadcast_payloads[2]["facts"]]

    assert "runtime_state_observed" in first_fact_types
    assert "runtime_state_observed" not in second_fact_types
    assert "runtime_state_observed" in third_fact_types
    assert snapshot_calls == 2
