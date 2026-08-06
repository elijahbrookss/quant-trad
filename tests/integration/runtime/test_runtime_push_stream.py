from __future__ import annotations

import threading
from datetime import datetime, timezone
from queue import Empty
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from engines.bot_runtime.core.domain import Candle, StrategySignal
from engines.bot_runtime.core.runtime_events import (
    EntryFilledContext,
    ExitKind,
    OrderLifecycleChangedContext,
    ReasonCode,
    RuntimeEventName,
    WalletDelta,
    WalletInitializedContext,
    build_correlation_id,
    new_runtime_event,
    runtime_event_from_dict,
)
from engines.bot_runtime.runtime.mixins.runtime_events import (
    RuntimeEventsMixin,
    _fill_accounting_mode,
    _fill_currency_pair,
)
from engines.bot_runtime.runtime.mixins.runtime_projection import RuntimeProjectionMixin
from engines.bot_runtime.runtime.mixins.runtime_push_stream import RuntimePushStreamMixin
from engines.bot_runtime.runtime.components.canonical_facts import (
    CanonicalFactAppender,
    CanonicalFactProjectionDispatcher,
    LiveFactsBroadcastConsumer,
)
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


def test_fill_accounting_mode_resolves_explicit_spot_semantics_and_rejects_ambiguity() -> None:
    spot_series = SimpleNamespace(
        execution_profile=SimpleNamespace(
            accounting_mode=None,
            instrument=SimpleNamespace(execution_semantics="spot"),
        ),
        instrument={},
    )
    margin_series = SimpleNamespace(
        execution_profile=SimpleNamespace(
            accounting_mode="margin",
            instrument=SimpleNamespace(execution_semantics="derivative"),
        ),
        instrument={},
    )
    ambiguous_series = SimpleNamespace(
        execution_profile=SimpleNamespace(
            accounting_mode=None,
            instrument=SimpleNamespace(execution_semantics="derivative"),
        ),
        instrument={},
    )

    assert _fill_accounting_mode(spot_series) == "spot"
    assert _fill_accounting_mode(margin_series) == "margin"
    with pytest.raises(ValueError, match="fill accounting_mode is required"):
        _fill_accounting_mode(ambiguous_series)


def test_fill_currency_pair_uses_execution_profile_and_rejects_conflicts() -> None:
    series = SimpleNamespace(
        execution_profile=SimpleNamespace(
            instrument=SimpleNamespace(base_currency="BTC", quote_currency="USD"),
        ),
        instrument={},
    )

    assert _fill_currency_pair(series, observed_quote="usd") == ("BTC", "USD")
    with pytest.raises(ValueError, match="fill base_currency conflicts"):
        _fill_currency_pair(series, observed_base="ETH")


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
        self._overlay_projection_cache = {}
        self._push_log_marker = None
        self._push_decision_marker = None
        self._push_payload_bytes_sample_every = 10
        self._botlens_fact_stream_overlay_point_limit = 160
        self._botlens_overlay_window_bars = 640
        self._botlens_overlay_emit_every_bars = 25
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
        self._chart_state_builder = SimpleNamespace(
            visible_overlays=lambda overlays, _status, _epoch: list(overlays or []),
        )

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

    def _current_epoch_for(self, series):
        _ = series
        return None

    def _refresh_chart_overlay_cache_from_projection(self):
        return None

    def _projected_overlays_for_series(self, series):
        cache = self._overlay_projection_cache.get(self._series_identity(series)["series_key"], {})
        return list(cache.get("visible_overlays") or [])

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


class _IncrementalPushRuntime(RuntimeProjectionMixin, _PushRuntime):
    pass


class _CountedDecision:
    def __init__(self, event_id: str) -> None:
        self.event_id = event_id
        self.serialize_calls = 0

    def serialize(self):
        self.serialize_calls += 1
        return {"event_id": self.event_id}


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
    payload_summary = dict(overlay["payload_summary"])
    polyline_fingerprint = payload_summary.pop("polyline_fingerprint")
    assert len(polyline_fingerprint) == 64
    assert payload_summary == {
        "geometry_keys": ["markers", "polylines"],
        "payload_counts": {"markers": 2, "polylines": 1},
        "point_count": 2,
        "source_payload_counts": {"markers": 3, "polylines": 1},
        "source_point_count": 9,
        "truncated": True,
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
                "exit_price": 100.0,
                "close_reason": "STOP",
                "reason_code": "EXEC_EXIT_STOP",
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

    runtime._chart_state_builder = SimpleNamespace(
        visible_candles=_visible_candles,
        visible_overlays=lambda overlays, _status, _epoch: list(overlays or []),
    )
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
    runtime._overlay_projection_cache = {
        "instrument-bip|1h": {"visible_overlays": [{"overlay_id": "overlay-1", "type": "line", "value": 1.5}]}
    }
    runtime._projected_overlays_for_series = lambda selected: list(
        runtime._overlay_projection_cache.get(runtime._series_identity(selected)["series_key"], {}).get("visible_overlays") or []
    )
    runtime._current_epoch_for = lambda _series: None
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


def test_spot_fill_does_not_emit_derived_margin_ledger_events() -> None:
    entry = {
        "event_id": "entry-event-spot-1",
        "event_name": "ENTRY_FILLED",
        "seq": 12,
        "event_ts": "2026-02-01T00:00:00Z",
        "correlation_id": "trade:trade-spot-1",
        "context": {
            "run_id": "run-1",
            "bot_id": "bot-1",
            "strategy_id": "strategy-1",
            "series_key": "instrument-btc|1h",
            "instrument_id": "instrument-btc",
            "symbol": "BTC/USD",
            "timeframe": "1h",
            "bar_ts": "2026-02-01T00:00:00Z",
            "trade_id": "trade-spot-1",
            "wallet_correlation_id": "trade:trade-spot-1",
            "wallet_commit_seq": 1,
            "side": "buy",
            "direction": "long",
            "qty": 1.0,
            "price": 100.0,
            "notional": 100.0,
            "fee_paid": 1.0,
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
    }

    assert RuntimePushStreamMixin._wallet_facts_from_runtime_event(entry) == []


def test_live_transport_payload_slims_wallet_snapshots_and_log_context() -> None:
    runtime = _runtime()
    payload = {
        "facts": [
            {
                "fact_type": "wallet_ledger_event",
                "wallet_event": {
                    "event_name": "MARGIN_RESERVED",
                    "wallet_commit_seq": 1,
                    "balance_before": 1000.0,
                    "balance_after": 1000.0,
                    "wallet_before": {
                        "balances": {"USD": 1000.0},
                        "margin_positions": {"trade-1": {"locked_margin": 10.0}},
                    },
                    "wallet_after": {
                        "balances": {"USD": 1000.0},
                        "margin_positions": {"trade-1": {"locked_margin": 20.0}},
                    },
                    "wallet_delta": {"collateral_reserved": 10.0},
                    "margin_requirement": {"total_required_collateral": 10.0},
                },
            },
            {
                "fact_type": "log_emitted",
                "log": {
                    "id": "diag-1",
                    "event": "overlay_debug",
                    "message": "large debug",
                    "context": {
                        "component": "indicator_guard",
                        "operation": "overlay_snapshot",
                        "raw": {"payload": "x" * 1024},
                        "traceback": "nope",
                    },
                },
            },
        ]
    }

    live_payload = runtime._botlens_live_transport_payload(payload)
    wallet_event = live_payload["facts"][0]["wallet_event"]
    log = live_payload["facts"][1]["log"]

    assert "wallet_before" not in wallet_event
    assert "wallet_after" not in wallet_event
    assert "wallet_delta" not in wallet_event
    assert "margin_requirement" not in wallet_event
    assert wallet_event["wallet_snapshot_summary"]["before_positions"] == 1
    assert "raw" not in log["context"]
    assert "traceback" not in log["context"]


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


def test_order_lifecycle_runtime_event_round_trip_preserves_residual_and_pins() -> None:
    event = new_runtime_event(
        event_name=RuntimeEventName.ORDER_LIFECYCLE_CHANGED,
        event_id="order-event-4",
        correlation_id="run-1:BTC-USD:1h:2026-02-01T00:00:00.000Z",
        root_id="signal-1",
        parent_id="decision-1",
        event_ts=datetime(2026, 2, 1, 1, tzinfo=timezone.utc),
        context=OrderLifecycleChangedContext(
            run_id="run-1",
            bot_id="bot-1",
            strategy_id="strategy-1",
            series_key="instrument-btc|1h",
            instrument_id="instrument-btc",
            symbol="BTC-USD",
            timeframe="1h",
            bar_ts=datetime(2026, 2, 1, 1, tzinfo=timezone.utc),
            order_request_id="order-1",
            order_request_manifest_hash="request-hash",
            attempt_id="attempt-1",
            order_attempt_manifest_hash="attempt-hash",
            order_event_seq=4,
            previous_state="open",
            state="partially_filled",
            known_at=datetime(2026, 2, 1, 1, tzinfo=timezone.utc),
            side="buy",
            requested_qty=10.0,
            attempt_requested_qty=10.0,
            attempt_cumulative_filled_qty=4.0,
            attempt_remaining_qty=6.0,
            order_cumulative_filled_qty=4.0,
            order_remaining_qty=6.0,
            execution_context_hash="context-hash",
            execution_policy_hash="policy-hash",
            order_lifecycle_replay_hash="replay-prefix-hash",
            trade_id="trade-1",
            signal_id="signal-domain-1",
            decision_id="decision-domain-1",
            fill_id="fill-1",
            fill_qty=4.0,
            fill_price=100.0,
            fill_fee=0.04,
            venue_event_name="open",
        ),
    )

    restored = runtime_event_from_dict(event.serialize())

    assert restored.serialize() == event.serialize()
    assert restored.context.state == "partially_filled"
    assert restored.context.order_remaining_qty == 6.0
    assert restored.context.execution_context_hash == "context-hash"
    assert restored.context.execution_policy_hash == "policy-hash"


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
        "exit_price": 101.0,
        "close_reason": "BACKTEST_END",
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
        "exit_price": 105.0,
        "close_reason": "TARGET",
        "reason_code": "EXEC_EXIT_TARGET",
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
        "exit_price": 105.0,
        "close_reason": "TARGET",
        "reason_code": "EXEC_EXIT_TARGET",
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


def test_trade_facts_reject_closed_snapshot_missing_terminal_evidence() -> None:
    runtime = _runtime()
    trade_payload = {
        "trade_id": "trade-1",
        "status": "closed",
        "entry_time": "2026-04-09T13:00:00Z",
        "closed_at": "2026-04-09T14:00:00Z",
        "direction": "long",
        "position_commit_seq": 2,
    }
    series = _trade_series(trade_payload, revision=1)

    with pytest.raises(
        RuntimeError,
        match="domain snapshot missing terminal fields.*exit_price,close_reason,reason_code",
    ):
        runtime._trade_facts(series=series, cache={})


def test_trade_facts_build_domain_events_with_required_lifecycle_and_simulated_times() -> None:
    runtime = _runtime()
    trade_payload = {
        "trade_id": "trade-1",
        "status": "closed",
        "entry_time": "2026-04-09T13:55:00Z",
        "closed_at": "2026-04-09T14:00:00Z",
        "exit_price": 105.0,
        "close_reason": "TARGET",
        "reason_code": "EXEC_EXIT_TARGET",
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
                    "exit_price": 105.0,
                    "close_reason": "TARGET",
                    "reason_code": "EXEC_EXIT_TARGET",
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


def test_push_update_never_builds_or_emits_overlay_projection_facts() -> None:
    runtime = _PushRuntime()
    series = SimpleNamespace(
        instrument={"id": "instrument-bip"},
        timeframe="1h",
        strategy_id="strategy-1",
        symbol="BIP-20DEC30-CDE",
        datasource="COINBASE",
        exchange="coinbase_direct",
        candles=[{"time": 1}, {"time": 2}],
        risk_engine=SimpleNamespace(trade_revision=0, serialise_trades=lambda: [], stats=lambda: {}),
    )
    runtime._series = [series]

    candle = SimpleNamespace(
        to_dict=lambda: {"time": 2, "open": 1.5, "high": 2.5, "low": 1.0, "close": 2.0},
    )

    runtime._build_indicator_overlay_projection_for_state = lambda *_args, **_kwargs: (_ for _ in ()).throw(
        AssertionError("push_update must not build overlay projections")
    )

    runtime._push_update("bar", series=series, candle=candle)
    runtime._push_update("bar", series=series, candle=candle)

    for payload in runtime.broadcast_payloads:
        fact_types = [fact["fact_type"] for fact in payload["facts"]]
        assert "overlay_ops_emitted" not in fact_types


def test_push_update_persists_canonical_decisions_without_subscribers() -> None:
    runtime = _PushRuntime()
    runtime._subscribers = {}
    captured = []

    def _append(**kwargs):
        captured.append(dict(kwargs))
        return {"inserted_rows": 1}

    runtime._canonical_fact_appender = CanonicalFactAppender(
        allocate_seq=runtime._allocate_test_canonical_seq,
        append_batch=_append,
        consumers=(),
    )
    runtime.decision_events = lambda: [
        {
            "event_id": "decision-event-1",
            "event_name": "DECISION_ACCEPTED",
            "event_ts": "2026-04-09T14:00:00Z",
            "correlation_id": "signal:signal-1",
            "context": {
                "run_id": "run-1",
                "bot_id": "bot-1",
                "strategy_id": "strategy-1",
                "series_key": "instrument-bip|1h",
                "instrument_id": "instrument-bip",
                "symbol": "BIP-20DEC30-CDE",
                "timeframe": "1h",
                "bar_ts": "2026-04-09T14:00:00Z",
                "decision_id": "decision-1",
                "signal_id": "signal-1",
                "direction": "long",
                "signal_price": 100.0,
            },
        }
    ]
    series = SimpleNamespace(
        instrument={"id": "instrument-bip"},
        timeframe="1h",
        strategy_id="strategy-1",
        symbol="BIP-20DEC30-CDE",
        datasource="COINBASE",
        exchange="coinbase_direct",
        candles=[{"time": 1}, {"time": 2}],
        risk_engine=SimpleNamespace(trade_revision=0, serialise_trades=lambda: [], stats=lambda: {}),
    )
    runtime._series = [series]
    candle = SimpleNamespace(
        time=datetime(2026, 4, 9, 14, tzinfo=timezone.utc),
        to_dict=lambda: {"time": "2026-04-09T14:00:00Z", "open": 1.5, "high": 2.5, "low": 1.0, "close": 2.0},
    )

    result = runtime._push_update("bar", series=series, candle=candle)

    assert result["subscriber_count"] == 0.0
    assert len(captured) == 1
    assert [fact["decision"]["event_name"] for fact in captured[0]["payload"]["facts"]] == [
        "DECISION_ACCEPTED"
    ]


def test_terminal_continuity_fact_uses_complete_producer_summary() -> None:
    runtime = _PushRuntime()
    runtime._subscribers = {}
    captured = []

    def _append(**kwargs):
        captured.append(dict(kwargs))
        return {"inserted_rows": 1}

    runtime._canonical_fact_appender = CanonicalFactAppender(
        allocate_seq=runtime._allocate_test_canonical_seq,
        append_batch=_append,
        consumers=(),
    )
    summary = {
        "candle_count": 169,
        "first_ts": "2024-01-01T00:00:00Z",
        "last_ts": "2024-01-08T00:00:00Z",
        "expected_interval_seconds": 3600,
        "detected_gap_count": 0,
        "defect_gap_count": 0,
        "missing_candle_estimate": 0,
        "gap_count_by_type": {"unknown_gap": 0},
        "final_status": "healthy",
    }
    candle_snapshot = {
        "schema_version": "candle_series_snapshot.v1",
        "strategy_id": "strategy-1",
        "instrument_id": "instrument-btc",
        "symbol": "BTC/USD",
        "timeframe": "1h",
        "candle_value_hash": "a" * 64,
        "candle_count": 169,
        "warmup_candle_count": 100,
        "replay_candle_count": 69,
    }
    runtime._series = [
        SimpleNamespace(
            instrument={"id": "instrument-btc"},
            timeframe="1h",
            strategy_id="strategy-1",
            symbol="BTC/USD",
            datasource="CCXT",
            exchange="coinbase",
            candles=[SimpleNamespace(time=datetime(2024, 1, 8, tzinfo=timezone.utc))],
            meta={
                "candle_continuity": summary,
                "candle_snapshot": candle_snapshot,
            },
        )
    ]

    emitted = runtime._emit_terminal_candle_continuity_facts(status="completed")

    assert emitted == 1
    fact = captured[0]["payload"]["facts"][0]
    assert fact["fact_type"] == "candle_continuity_summary"
    assert fact["summary"] == {
        **summary,
        "candle_snapshot": candle_snapshot,
        "boundary_name": "run_final",
        "evidence_scope": "canonical_terminal",
        "materiality": "canonical",
        "source_reason": "completed",
    }


def test_push_update_queues_botlens_projection_dispatch_off_bar_path() -> None:
    runtime = _PushRuntime()
    broadcast_started = threading.Event()
    broadcast_release = threading.Event()

    def _blocking_broadcast(event, payload=None):
        broadcast_started.set()
        if not broadcast_release.wait(timeout=2.0):
            raise RuntimeError("test broadcast was not released")
        runtime.broadcast_payloads.append({"transport_event": event, **dict(payload or {})})
        return (1, 0)

    runtime._canonical_fact_appender = CanonicalFactAppender(
        allocate_seq=runtime._allocate_test_canonical_seq,
        append_batch=lambda **_kwargs: {"inserted_rows": 1},
        projection_dispatcher=CanonicalFactProjectionDispatcher(
            consumers=(LiveFactsBroadcastConsumer(_blocking_broadcast),),
            queue_max=4,
            flush_interval_s=0.001,
            drain_timeout_s=2.0,
        ),
    )
    series = SimpleNamespace(
        instrument={"id": "instrument-bip"},
        timeframe="1h",
        strategy_id="strategy-1",
        symbol="BIP-20DEC30-CDE",
        datasource="COINBASE",
        exchange="coinbase_direct",
        candles=[{"time": 1}, {"time": 2}],
        risk_engine=SimpleNamespace(trade_revision=0, serialise_trades=lambda: [], stats=lambda: {}),
    )
    runtime._series = [series]
    candle = SimpleNamespace(
        to_dict=lambda: {"time": 2, "open": 1.5, "high": 2.5, "low": 1.0, "close": 2.0},
    )

    result = runtime._push_update("bar", series=series, candle=candle)

    assert result["subscriber_count"] == 1.0
    assert result["dropped_messages"] == 0.0
    assert broadcast_started.wait(timeout=1.0)
    assert runtime.broadcast_payloads == []

    broadcast_release.set()
    runtime._canonical_fact_appender.flush(reason="test", shutdown=True, timeout_s=2.0)

    assert runtime.broadcast_payloads[0]["transport_event"] == "facts"


def test_overlay_projection_uses_bar_cadence_and_emits_bounded_delta() -> None:
    runtime = _PushRuntime()
    projection_calls = 0

    def _build_indicator_overlay_projection_for_state(_state, **_kwargs):
        nonlocal projection_calls
        projection_calls += 1
        return [{"overlay_id": f"overlay-{projection_calls}", "type": "regime_overlay", "payload": {"blocks": []}}]

    runtime._build_indicator_overlay_projection_for_state = _build_indicator_overlay_projection_for_state
    runtime._botlens_overlay_emit_every_bars = 3
    series = SimpleNamespace(
        instrument={"id": "instrument-bip"},
        timeframe="1h",
        strategy_id="strategy-1",
        symbol="BIP-20DEC30-CDE",
        datasource="COINBASE",
        exchange="coinbase_direct",
        candles=[{"time": 1}, {"time": 2}, {"time": 3}],
        risk_engine=SimpleNamespace(trade_revision=0, serialise_trades=lambda: [], stats=lambda: {}),
    )
    runtime._series = [series]
    candle = SimpleNamespace(
        time=datetime(2026, 4, 9, 14, tzinfo=timezone.utc),
        to_dict=lambda: {"time": 2, "open": 1.5, "high": 2.5, "low": 1.0, "close": 2.0},
    )
    state = SimpleNamespace(series=series, bar_index=1)

    first = runtime._emit_overlay_projection_for_state(state, candle, reason="test")
    state.bar_index = 2
    second = runtime._emit_overlay_projection_for_state(state, candle, reason="test")
    state.bar_index = 4
    third = runtime._emit_overlay_projection_for_state(state, candle, reason="test")

    assert projection_calls == 2
    assert first["overlay_projection_projected_count"] == 1.0
    assert second["overlay_projection_skipped_count"] == 1.0
    assert third["overlay_projection_projected_count"] == 1.0
    overlay_payloads = [
        payload
        for payload in runtime.broadcast_payloads
        if any(fact["fact_type"] == "overlay_ops_emitted" for fact in payload.get("facts", []))
    ]
    assert len(overlay_payloads) == 2
    first_delta = overlay_payloads[0]["facts"][0]["overlay_delta"]
    assert first_delta["projection"] == {
        "mode": "bounded",
        "window_bars": 640,
        "emit_every_bars": 3,
        "bar_index": 1,
        "reason": "test",
        "terminal": False,
    }
    assert first_delta["ops"][0]["overlay"]["detail_level"] == "bounded_render"


def _runtime_with_existing_overlay_projection_cache() -> tuple[_PushRuntime, SimpleNamespace, SimpleNamespace, dict]:
    runtime = _PushRuntime()
    runtime._build_indicator_overlay_projection_for_state = lambda *_args, **_kwargs: [
        {"overlay_id": "next-overlay", "type": "regime_overlay", "payload": {"blocks": []}}
    ]
    runtime._botlens_overlay_emit_every_bars = 1
    series = SimpleNamespace(
        instrument={"id": "instrument-bip"},
        timeframe="1h",
        strategy_id="strategy-1",
        symbol="BIP-20DEC30-CDE",
        datasource="COINBASE",
        exchange="coinbase_direct",
        candles=[{"time": 1}],
        risk_engine=SimpleNamespace(trade_revision=0, serialise_trades=lambda: [], stats=lambda: {}),
    )
    runtime._series = [series]
    existing_cache = {
        "overlay_entries": {
            "old-overlay": {
                "overlay_id": "old-overlay",
                "type": "regime_overlay",
                "detail_level": "bounded_render",
                "payload": {"blocks": []},
            }
        },
        "overlay_fingerprints": {"old-overlay": "old-fingerprint"},
        "overlay_order": ["old-overlay"],
        "overlay_commit_seq": 7,
        "raw_overlays": [{"overlay_id": "old-overlay", "type": "regime_overlay", "payload": {"blocks": []}}],
        "visible_overlays": [{"overlay_id": "old-overlay", "type": "regime_overlay", "payload": {"blocks": []}}],
        "last_projected_bar_index": 4,
        "last_projected_epoch": 1_765_000_000,
        "projection_mode": "bounded",
    }
    runtime._overlay_projection_cache["instrument-bip|1h"] = dict(existing_cache)
    candle = SimpleNamespace(
        time=datetime(2026, 4, 9, 14, tzinfo=timezone.utc),
        to_dict=lambda: {"time": 2, "open": 1.5, "high": 2.5, "low": 1.0, "close": 2.0},
    )
    state = SimpleNamespace(series=series, bar_index=10)
    return runtime, candle, state, existing_cache


def test_overlay_projection_reraises_canonical_append_failure_without_advancing_cache() -> None:
    runtime, candle, state, existing_cache = _runtime_with_existing_overlay_projection_cache()
    runtime.commit_botlens_fact_payload = lambda *_args, **_kwargs: (_ for _ in ()).throw(
        RuntimeError("projection append failed")
    )

    with pytest.raises(RuntimeError, match="projection append failed"):
        runtime._emit_overlay_projection_for_state(state, candle, reason="test")

    assert runtime._overlay_projection_cache["instrument-bip|1h"] == existing_cache
    assert runtime.broadcast_payloads == []


def test_overlay_projection_reraises_canonical_dispatch_failure_without_advancing_cache() -> None:
    runtime, candle, state, existing_cache = _runtime_with_existing_overlay_projection_cache()
    runtime._canonical_fact_appender = SimpleNamespace(
        append_fact_batch=lambda **_kwargs: SimpleNamespace(batch=SimpleNamespace()),
        dispatch=lambda _batch: (_ for _ in ()).throw(RuntimeError("projection dispatch failed")),
    )

    with pytest.raises(RuntimeError, match="projection dispatch failed"):
        runtime._emit_overlay_projection_for_state(state, candle, reason="test")

    assert runtime._overlay_projection_cache["instrument-bip|1h"] == existing_cache
    assert runtime.broadcast_payloads == []


def test_push_update_coalesces_unchanged_series_stats() -> None:
    runtime = _PushRuntime()
    series = SimpleNamespace(
        instrument={"id": "instrument-bip"},
        timeframe="1h",
        strategy_id="strategy-1",
        symbol="BIP-20DEC30-CDE",
        datasource="COINBASE",
        exchange="coinbase_direct",
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


def test_push_update_coalesces_unchanged_series_metadata_until_identity_revision() -> None:
    runtime = _PushRuntime()
    series = SimpleNamespace(
        instrument={"id": "instrument-bip"},
        timeframe="1h",
        strategy_id="strategy-1",
        symbol="BIP-20DEC30-CDE",
        datasource="COINBASE",
        exchange="coinbase_direct",
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
    series.symbol = "BIP-REVISED"
    with patch("engines.bot_runtime.runtime.mixins.runtime_push_stream.time.monotonic", return_value=12.0):
        runtime._push_update("bar", series=series, candle=candle)

    emitted = [
        [fact["fact_type"] for fact in payload["facts"]]
        for payload in runtime.broadcast_payloads
    ]
    assert "series_state_observed" in emitted[0]
    assert "series_state_observed" not in emitted[1]
    assert "series_state_observed" in emitted[2]


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


def test_decision_facts_apply_marker_and_limit_before_serialization() -> None:
    runtime = _IncrementalPushRuntime()
    runtime._botlens_fact_stream_decision_fact_limit = 3
    decisions = [_CountedDecision(f"decision-{index}") for index in range(4)]
    runtime._decision_events = decisions

    facts, dropped = runtime._decision_facts()

    assert [fact["decision"]["event_id"] for fact in facts] == [
        "decision-1",
        "decision-2",
        "decision-3",
    ]
    assert dropped == 1
    assert [entry.serialize_calls for entry in decisions] == [0, 1, 1, 1]

    repeated, repeated_dropped = runtime._decision_facts()

    assert repeated == []
    assert repeated_dropped == 0
    assert [entry.serialize_calls for entry in decisions] == [0, 1, 1, 1]

    appended = _CountedDecision("decision-4")
    runtime._decision_events.append(appended)
    incremental, incremental_dropped = runtime._decision_facts()

    assert [fact["decision"]["event_id"] for fact in incremental] == ["decision-4"]
    assert incremental_dropped == 0
    assert appended.serialize_calls == 1


def test_overlay_projection_due_uses_bar_distance_not_wall_clock() -> None:
    runtime = _runtime()
    runtime._botlens_overlay_emit_every_bars = 5
    state = SimpleNamespace(bar_index=10)
    cache = {"last_projected_bar_index": 6}

    assert runtime._overlay_projection_due_for_state(state, cache) is False
    state.bar_index = 11
    assert runtime._overlay_projection_due_for_state(state, cache) is True


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


def test_log_facts_skip_retained_log_scan_when_revision_is_unchanged() -> None:
    runtime = _PushRuntime()
    runtime._logs = [{"id": "log-1", "message": "first"}]
    runtime._log_revision = 1
    runtime._push_log_revision_seen = -1

    first, first_dropped = runtime._log_facts()
    runtime._entries_after_marker = lambda *_args, **_kwargs: (_ for _ in ()).throw(
        AssertionError("unchanged logs must not be rescanned")
    )
    second, second_dropped = runtime._log_facts()

    assert [fact["log"]["id"] for fact in first] == ["log-1"]
    assert first_dropped == 0
    assert second == []
    assert second_dropped == 0


def test_wallet_facts_read_only_new_runtime_events_after_first_cursor() -> None:
    runtime = _PushRuntime()
    runtime._run_context.runtime_event_stream = [
        {"event_id": "event-1", "event_name": "IGNORED", "context": {}},
    ]
    runtime._push_wallet_stream_length = 0

    assert runtime._wallet_facts() == []
    runtime._entries_after_marker = lambda *_args, **_kwargs: (_ for _ in ()).throw(
        AssertionError("append-only wallet stream must not be rescanned")
    )
    runtime._run_context.runtime_event_stream.append(
        {"event_id": "event-2", "event_name": "IGNORED", "context": {}}
    )

    assert runtime._wallet_facts() == []
    assert runtime._push_wallet_stream_length == 2
