from __future__ import annotations

from datetime import datetime, timezone
from queue import Empty
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from engines.bot_runtime.core.domain import Candle, StrategySignal
from engines.bot_runtime.runtime.mixins.runtime_events import RuntimeEventsMixin
from engines.bot_runtime.runtime.mixins.runtime_push_stream import RuntimePushStreamMixin
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
        self._obs_enabled = False
        self.state = {"status": "running"}
        self.broadcast_payloads = []
        self._logs = []
        self._decision_events = []
        self._canonical_fact_appender = SimpleNamespace(
            append_fact_batch=lambda **kwargs: None,
            dispatch=lambda _batch: (),
        )
        self._run_context = SimpleNamespace(run_id="run-1")
        self.bot_id = "bot-1"
        self.config = {}

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

    def _series_visible_overlays(self, series, *, status):
        _ = status
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


def test_botlens_bootstrap_payload_emits_fact_batch_for_selected_series() -> None:
    runtime = _runtime()
    series = SimpleNamespace(
        instrument={"id": "instrument-bip"},
        timeframe="1h",
        strategy_id="strategy-1",
        symbol="BIP-20DEC30-CDE",
        datasource="COINBASE",
        exchange="coinbase_direct",
    )
    runtime._series = [series]
    runtime.snapshot = lambda: {
        "status": "running",
        "known_at": "2026-04-09T14:00:00Z",
        "last_snapshot_at": "2026-04-09T14:00:00Z",
        "stats": {"bars_processed": 12},
    }
    runtime.chart_payload = lambda: {
        "series": [
            {
                "instrument_id": "instrument-bip",
                "symbol": "BIP-20DEC30-CDE",
                "timeframe": "1h",
                "bar_index": 1,
                "candles": [
                    {"time": 1, "open": 1.0, "high": 2.0, "low": 0.5, "close": 1.5},
                    {"time": 2, "open": 1.5, "high": 2.5, "low": 1.0, "close": 2.0},
                ],
                "overlays": [{"type": "line", "value": 1.5}],
                "stats": {"open_trades": 0},
            }
        ],
        "trades": [{"trade_id": "trade-1", "status": "open"}],
        "logs": [{"id": "log-1", "message": "bootstrap"}],
        "decisions": [{"event_id": "decision-1", "action": "hold"}],
    }

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
    assert "log_emitted" in fact_types
    assert "decision_emitted" in fact_types


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
        rejection_artifact={"context": {}},
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
    assert runtime._run_context.runtime_event_stream[-1]["context"]["attempt_id"] == event.context.entry_request_id


def test_trade_facts_emit_open_before_close_when_first_observed_already_closed() -> None:
    runtime = _runtime()
    trade_payload = {
        "trade_id": "trade-1",
        "status": "closed",
        "entry_time": "2026-04-09T13:55:00Z",
        "closed_at": "2026-04-09T14:00:00Z",
        "direction": "long",
    }
    series = _trade_series(trade_payload, revision=1)

    trade_facts, _stats, _count, refresh_required = runtime._trade_facts(series=series, cache={})

    assert [fact["fact_type"] for fact in trade_facts] == ["trade_opened", "trade_closed"]
    assert trade_facts[0]["trade"]["bar_time"] == "2026-04-09T13:55:00Z"
    assert trade_facts[0]["trade"]["event_time"] == "2026-04-09T13:55:00Z"
    assert trade_facts[0]["trade"]["status"] == "open"
    assert "closed_at" not in trade_facts[0]["trade"]
    assert trade_facts[1]["trade"]["bar_time"] == "2026-04-09T14:00:00Z"
    assert trade_facts[1]["trade"]["event_time"] == "2026-04-09T14:00:00Z"
    assert refresh_required is True


def test_trade_facts_same_bar_open_close_emits_deterministic_lifecycle_order() -> None:
    runtime = _runtime()
    trade_payload = {
        "trade_id": "trade-1",
        "status": "closed",
        "entry_time": "2026-04-09T14:00:00Z",
        "closed_at": "2026-04-09T14:00:00Z",
        "direction": "long",
    }
    series = _trade_series(trade_payload, revision=1)

    trade_facts, _stats, _count, _refresh = runtime._trade_facts(series=series, cache={})

    assert [fact["fact_type"] for fact in trade_facts] == ["trade_opened", "trade_closed"]
    assert [fact["trade"]["bar_time"] for fact in trade_facts] == [
        "2026-04-09T14:00:00Z",
        "2026-04-09T14:00:00Z",
    ]


def test_trade_facts_do_not_duplicate_open_for_previously_opened_trade() -> None:
    runtime = _runtime()
    trade_payload = {
        "trade_id": "trade-1",
        "status": "closed",
        "entry_time": "2026-04-09T13:00:00Z",
        "closed_at": "2026-04-09T14:00:00Z",
        "direction": "long",
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
    runtime.snapshot = lambda: {
        key: ([dict(entry) for entry in value] if isinstance(value, list) else value)
        for key, value in runtime_snapshot.items()
    }
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
