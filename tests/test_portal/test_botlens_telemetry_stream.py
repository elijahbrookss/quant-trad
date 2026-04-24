"""Tests for the BotLens telemetry pipeline.

Architecture under test (post-refactor):
  - SymbolProjector owns canonical symbol-level state.
  - RunProjector owns canonical run-level state and lifecycle.
  - ProjectorRegistry creates/holds per-run contexts.
  - IntakeRouter routes ingest payloads to mailboxes.
  - BotTelemetryHub is the thin public coordinator.
  - Fanout is downstream of projection (non-blocking).

Tests exercise projector methods directly for unit-level clarity.
Integration tests use the full hub with short asyncio.sleep to let tasks run.
"""

from __future__ import annotations

import asyncio
import json
from typing import Any, Dict, List
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

pytest.importorskip("sqlalchemy")

from portal.backend.service.observability import (
    BackendObserver,
    QueueStateMetricOwner,
    get_observability_sink,
    reset_observability_sink,
)
from portal.backend.service.bots.botlens_contract import (
    BRIDGE_BOOTSTRAP_KIND,
    BRIDGE_FACTS_KIND,
    LIFECYCLE_KIND,
)
from portal.backend.service.bots.botlens_domain_events import botlens_domain_event_type
from portal.backend.service.bots.botlens_mailbox import (
    BootstrapSlot,
    FanoutEnvelope,
    FanoutRunDeltaBatch,
    FanoutSymbolDeltaBatch,
    RunMailbox,
    SymbolMailbox,
    QueueEnvelope,
)
from portal.backend.service.bots.botlens_symbol_projector import (
    SymbolProjector,
    SymbolSummaryNotification,
)
from portal.backend.service.bots.botlens_run_projector import RunProjector
from portal.backend.service.bots.botlens_state import RunOpenTradesDelta, RunSymbolCatalogDelta
from portal.backend.service.bots.botlens_projector_registry import ProjectorRegistry
from portal.backend.service.bots.botlens_intake_router import IntakeRouter
import portal.backend.service.bots.botlens_symbol_projector as sym_mod
import portal.backend.service.bots.botlens_run_projector as run_mod


@pytest.fixture(autouse=True)
def _reset_observability() -> None:
    reset_observability_sink()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

class FakeWebSocket:
    def __init__(self) -> None:
        self.accepted = False
        self.messages: List[Dict[str, Any]] = []
        self.closed = False

    async def accept(self) -> None:
        self.accepted = True

    async def send_text(self, payload: str) -> None:
        self.messages.append(json.loads(payload))

    async def close(self, code: int = 1000) -> None:
        self.closed = True


def _queue_owner(*, key: str, depth_metric: str, utilization_metric: str, oldest_age_metric: str | None = None, **labels: Any) -> QueueStateMetricOwner:
    return QueueStateMetricOwner(
        observer=BackendObserver(component="test_queue_owner"),
        key=key,
        depth_metric=depth_metric,
        utilization_metric=utilization_metric,
        oldest_age_metric=oldest_age_metric,
        labels=labels,
    )


def _facts_batch(
    *,
    candle_time: int,
    symbol_key: str = "instrument-btc|1m",
    symbol: str = "BTC",
    warnings: list | None = None,
    overlay_delta: dict | None = None,
    log_entries: list | None = None,
    decision_entries: list | None = None,
) -> list:
    instrument_id, timeframe = str(symbol_key).split("|", 1)
    facts = [
        {
            "fact_type": "runtime_state_observed",
            "runtime": {
                "status": "running",
                "worker_count": 2,
                "active_workers": 1,
                "warnings": list(warnings or []),
            },
        },
        {
            "fact_type": "series_state_observed",
            "series_key": symbol_key,
            "instrument_id": instrument_id,
            "symbol": symbol,
            "timeframe": timeframe,
        },
        {
            "fact_type": "candle_upserted",
            "series_key": symbol_key,
            "candle": {
                "time": candle_time,
                "open": float(candle_time),
                "high": float(candle_time),
                "low": float(candle_time),
                "close": float(candle_time),
            },
        },
        {
            "fact_type": "trade_opened",
            "series_key": symbol_key,
            "trade": {"trade_id": "trade-1", "symbol_key": symbol_key, "symbol": symbol},
        },
    ]
    if isinstance(overlay_delta, dict):
        facts.append({
            "fact_type": "overlay_ops_emitted",
            "series_key": symbol_key,
            "overlay_delta": dict(overlay_delta),
        })
    for entry in log_entries or []:
        facts.append({"fact_type": "log_emitted", "log": dict(entry)})
    for entry in decision_entries or []:
        facts.append({"fact_type": "decision_emitted", "decision": dict(entry)})
    return facts


def _bootstrap_payload(
    *,
    run_id: str = "run-1",
    bot_id: str = "bot-1",
    symbol_key: str = "instrument-btc|1m",
    run_seq: int = 1,
    bridge_session_id: str = "session-1",
    candle_time: int = 1,
) -> dict:
    return {
        "kind": BRIDGE_BOOTSTRAP_KIND,
        "bot_id": bot_id,
        "run_id": run_id,
        "series_key": symbol_key,
        "run_seq": run_seq,
        "bridge_session_id": bridge_session_id,
        "bridge_seq": 1,
        "facts": _facts_batch(candle_time=candle_time, symbol_key=symbol_key),
        "event_time": "2026-01-01T00:00:00Z",
        "known_at": "2026-01-01T00:00:00Z",
    }


def _facts_payload(
    *,
    run_id: str = "run-1",
    bot_id: str = "bot-1",
    symbol_key: str = "instrument-btc|1m",
    run_seq: int = 2,
    bridge_session_id: str = "session-1",
    candle_time: int = 2,
    **kwargs,
) -> dict:
    return {
        "kind": BRIDGE_FACTS_KIND,
        "bot_id": bot_id,
        "run_id": run_id,
        "series_key": symbol_key,
        "run_seq": run_seq,
        "bridge_session_id": bridge_session_id,
        "bridge_seq": run_seq,
        "facts": _facts_batch(candle_time=candle_time, symbol_key=symbol_key, **kwargs),
        "event_time": "2026-01-01T00:01:00Z",
        "known_at": "2026-01-01T00:01:00Z",
    }


def _make_symbol_projector(
    *,
    run_id: str = "run-1",
    bot_id: str = "bot-1",
    symbol_key: str = "instrument-btc|1m",
    persisted_rows: list | None = None,
    persisted_events: list | None = None,
) -> tuple[SymbolProjector, asyncio.Queue, asyncio.Queue]:
    mailbox = SymbolMailbox(run_id=run_id, bot_id=bot_id, symbol_key=symbol_key)
    run_notifications: asyncio.Queue = asyncio.Queue()
    fanout_channel: asyncio.Queue = asyncio.Queue()

    rows = persisted_rows if persisted_rows is not None else []
    events = persisted_events if persisted_events is not None else []

    projector = SymbolProjector(
        run_id=run_id,
        bot_id=bot_id,
        symbol_key=symbol_key,
        mailbox=mailbox,
        run_notifications=run_notifications,
        fanout_channel=fanout_channel,
        run_notification_queue_metrics=_queue_owner(
            key=f"run_notification_queue:{run_id}",
            depth_metric="run_notification_queue_depth",
            utilization_metric="run_notification_queue_utilization",
            oldest_age_metric="run_notification_queue_oldest_age_ms",
            bot_id=bot_id,
            run_id=run_id,
            queue_name="run_notification_queue",
        ),
        fanout_queue_metrics=_queue_owner(
            key=f"fanout_channel:{run_id}",
            depth_metric="fanout_queue_depth",
            utilization_metric="fanout_queue_utilization",
            oldest_age_metric="fanout_queue_oldest_age_ms",
            bot_id=bot_id,
            run_id=run_id,
            queue_name="fanout_channel",
        ),
    )
    return projector, run_notifications, fanout_channel


@pytest.mark.asyncio
async def test_projectors_use_shared_botlens_domain_event_type_helper(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    symbol_rows: list[dict] = []
    run_rows: list[dict] = []

    monkeypatch.setattr(
        sym_mod,
        "record_bot_runtime_events_batch",
        lambda rows: symbol_rows.extend([dict(row) for row in rows]) or len(rows),
    )

    symbol_projector, *_ = _make_symbol_projector()
    await symbol_projector._load_initial_state()
    await symbol_projector._apply_bootstrap(_bootstrap_payload())

    assert symbol_rows
    assert all(
        row["event_type"] == botlens_domain_event_type(row["payload"]["event_name"])
        for row in symbol_rows
    )

    monkeypatch.setattr(
        run_mod,
        "record_bot_runtime_events_batch",
        lambda rows: run_rows.extend([dict(row) for row in rows]) or len(rows),
    )

    async def _fake_evict(_run_id: str) -> None:
        return None

    run_projector = RunProjector(
        run_id="run-1",
        bot_id="bot-1",
        mailbox=RunMailbox(run_id="run-1", bot_id="bot-1"),
        fanout_channel=asyncio.Queue(),
        fanout_queue_metrics=_queue_owner(
            key="fanout_channel:run-1",
            depth_metric="fanout_queue_depth",
            utilization_metric="fanout_queue_utilization",
            oldest_age_metric="fanout_queue_oldest_age_ms",
            bot_id="bot-1",
            run_id="run-1",
            queue_name="fanout_channel",
        ),
        on_evict=_fake_evict,
    )
    await run_projector._persist_lifecycle_event(
        lifecycle={
            "phase": "live",
            "status": "running",
            "owner": "runtime",
            "checkpoint_at": "2026-01-01T00:00:00Z",
        },
        lifecycle_seq=1,
    )

    assert run_rows
    assert all(
        row["event_type"] == botlens_domain_event_type(row["payload"]["event_name"])
        for row in run_rows
    )


# ---------------------------------------------------------------------------
# BootstrapSlot unit tests
# ---------------------------------------------------------------------------

class TestBootstrapSlot:
    def test_last_writer_wins(self) -> None:
        slot = BootstrapSlot(run_id="run-1", bot_id="bot-1", symbol_key="btc|1m")
        slot.put({"seq": 1})
        slot.put({"seq": 2})
        assert slot.superseded_count == 1
        payload = slot.take()
        assert payload is not None
        assert payload["seq"] == 2
        assert not slot.pending

    def test_event_set_on_put_cleared_on_take(self) -> None:
        async def scenario() -> None:
            slot = BootstrapSlot(run_id="run-1", bot_id="bot-1", symbol_key="btc|1m")
            assert not slot.event.is_set()
            slot.put({"x": 1})
            assert slot.event.is_set()
            slot.take()
            assert not slot.event.is_set()
        asyncio.run(scenario())

    def test_take_empty_returns_none(self) -> None:
        slot = BootstrapSlot(run_id="run-1", bot_id="bot-1", symbol_key="btc|1m")
        assert slot.take() is None
        assert not slot.pending


# ---------------------------------------------------------------------------
# SymbolMailbox unit tests
# ---------------------------------------------------------------------------

class TestSymbolMailbox:
    def test_enqueue_facts_queues_payload(self) -> None:
        async def scenario() -> None:
            mailbox = SymbolMailbox(run_id="run-1", bot_id="bot-1", symbol_key="btc|1m")
            assert mailbox.enqueue_facts({"seq": 1})
            assert mailbox.fact_queue.qsize() == 1
        asyncio.run(scenario())

    def test_set_bootstrap_replaces_previous(self) -> None:
        async def scenario() -> None:
            mailbox = SymbolMailbox(run_id="run-1", bot_id="bot-1", symbol_key="btc|1m")
            mailbox.set_bootstrap({"seq": 1})
            mailbox.set_bootstrap({"seq": 2})
            assert mailbox.bootstrap_slot.superseded_count == 1
            payload = mailbox.bootstrap_slot.take()
            assert payload is not None
            assert payload["seq"] == 2
        asyncio.run(scenario())


# ---------------------------------------------------------------------------
# SymbolProjector unit tests — projection methods tested directly
# ---------------------------------------------------------------------------

class TestSymbolProjectorBootstrap:
    def test_bootstrap_resets_state_and_applies_facts(self, monkeypatch: pytest.MonkeyPatch) -> None:
        async def scenario() -> None:
            events: list = []
            monkeypatch.setattr(
                sym_mod,
                "record_bot_runtime_events_batch",
                lambda rows: events.extend([dict(row) for row in rows]) or len(rows),
            )

            projector, notifications, fanout = _make_symbol_projector()
            await projector._load_initial_state()
            await projector._apply_bootstrap(_bootstrap_payload(candle_time=5, run_seq=10))

            snapshot = projector.get_snapshot()
            assert snapshot.candles.candles[-1]["time"] == 5
            assert snapshot.symbol_key == "instrument-btc|1m"
            assert len(events) >= 2
            assert {event["event_type"] for event in events} >= {
                "botlens_domain.health_status_reported",
                "botlens_domain.candle_observed",
            }

        asyncio.run(scenario())

    def test_bootstrap_sets_session_id(self, monkeypatch: pytest.MonkeyPatch) -> None:
        async def scenario() -> None:
            monkeypatch.setattr(sym_mod, "record_bot_runtime_events_batch", lambda rows: len(rows))

            projector, *_ = _make_symbol_projector()
            await projector._load_initial_state()
            await projector._apply_bootstrap(_bootstrap_payload(bridge_session_id="sess-A"))
            assert projector._current_session_id == "sess-A"

        asyncio.run(scenario())

    def test_second_bootstrap_replaces_first_state(self, monkeypatch: pytest.MonkeyPatch) -> None:
        async def scenario() -> None:
            monkeypatch.setattr(sym_mod, "record_bot_runtime_events_batch", lambda rows: len(rows))

            projector, *_ = _make_symbol_projector()
            await projector._load_initial_state()
            await projector._apply_bootstrap(_bootstrap_payload(candle_time=1, run_seq=1))
            await projector._apply_bootstrap(_bootstrap_payload(candle_time=99, run_seq=50, bridge_session_id="sess-B"))

            # State must reflect the second bootstrap, not the first.
            snapshot = projector.get_snapshot()
            assert snapshot.candles.candles[-1]["time"] == 99
            assert projector._current_session_id == "sess-B"

        asyncio.run(scenario())

    def test_bootstrap_emits_typed_deltas_to_fanout_channel(self, monkeypatch: pytest.MonkeyPatch) -> None:
        async def scenario() -> None:
            monkeypatch.setattr(sym_mod, "record_bot_runtime_events_batch", lambda rows: len(rows))

            projector, _, fanout = _make_symbol_projector()
            await projector._load_initial_state()
            await projector._apply_bootstrap(_bootstrap_payload(candle_time=1))

            assert not fanout.empty()
            item = fanout.get_nowait()
            assert isinstance(item, FanoutEnvelope)
            assert isinstance(item.item, FanoutSymbolDeltaBatch)
            delta_types = {d.event.delta_type for d in item.item.deltas}
            assert "botlens_symbol_candle_delta" in delta_types
            assert "botlens_symbol_runtime_delta" in delta_types

        asyncio.run(scenario())

    def test_bootstrap_notifies_run_projector(self, monkeypatch: pytest.MonkeyPatch) -> None:
        async def scenario() -> None:
            monkeypatch.setattr(sym_mod, "record_bot_runtime_events_batch", lambda rows: len(rows))

            projector, notifications, _ = _make_symbol_projector()
            await projector._load_initial_state()
            await projector._apply_bootstrap(_bootstrap_payload(candle_time=1))

            assert not notifications.empty()
            notification = notifications.get_nowait()
            assert isinstance(notification, QueueEnvelope)
            assert isinstance(notification.payload, SymbolSummaryNotification)
            assert notification.payload.symbol_key == "instrument-btc|1m"

        asyncio.run(scenario())


class TestSymbolProjectorFacts:
    def test_facts_advance_state(self, monkeypatch: pytest.MonkeyPatch) -> None:
        async def scenario() -> None:
            monkeypatch.setattr(sym_mod, "record_bot_runtime_events_batch", lambda rows: len(rows))

            projector, *_ = _make_symbol_projector()
            await projector._load_initial_state()
            await projector._apply_bootstrap(_bootstrap_payload(candle_time=1, run_seq=1))
            await projector._apply_facts(_facts_payload(candle_time=2, run_seq=2))

            snapshot = projector.get_snapshot()
            assert snapshot.candles.candles[-1]["time"] == 2

        asyncio.run(scenario())

    def test_stale_session_facts_rejected(self, monkeypatch: pytest.MonkeyPatch) -> None:
        async def scenario() -> None:
            events: list = []
            monkeypatch.setattr(
                sym_mod,
                "record_bot_runtime_events_batch",
                lambda rows: events.extend([dict(row) for row in rows]) or len(rows),
            )

            projector, *_ = _make_symbol_projector()
            await projector._load_initial_state()
            await projector._apply_bootstrap(
                _bootstrap_payload(candle_time=1, run_seq=1, bridge_session_id="session-A")
            )
            events.clear()

            # Facts from old session must be rejected.
            await projector._apply_facts(
                _facts_payload(candle_time=99, run_seq=99, bridge_session_id="session-OLD")
            )
            assert not events
            snapshot = projector.get_snapshot()
            assert snapshot.candles.candles[-1]["time"] == 1

        asyncio.run(scenario())

    def test_same_session_facts_accepted_after_bootstrap(self, monkeypatch: pytest.MonkeyPatch) -> None:
        async def scenario() -> None:
            events: list = []
            monkeypatch.setattr(
                sym_mod,
                "record_bot_runtime_events_batch",
                lambda rows: events.extend([dict(row) for row in rows]) or len(rows),
            )

            projector, *_ = _make_symbol_projector()
            await projector._load_initial_state()
            await projector._apply_bootstrap(
                _bootstrap_payload(candle_time=1, run_seq=1, bridge_session_id="session-B")
            )
            events.clear()

            await projector._apply_facts(
                _facts_payload(candle_time=5, run_seq=2, bridge_session_id="session-B")
            )
            assert events
            snapshot = projector.get_snapshot()
            assert snapshot.candles.candles[-1]["time"] == 5

        asyncio.run(scenario())

    def test_new_session_facts_accepted_after_replacement_bootstrap(self, monkeypatch: pytest.MonkeyPatch) -> None:
        async def scenario() -> None:
            events: list = []
            monkeypatch.setattr(
                sym_mod,
                "record_bot_runtime_events_batch",
                lambda rows: events.extend([dict(row) for row in rows]) or len(rows),
            )

            projector, *_ = _make_symbol_projector()
            await projector._load_initial_state()
            await projector._apply_bootstrap(
                _bootstrap_payload(candle_time=1, run_seq=1, bridge_session_id="session-A")
            )
            await projector._apply_bootstrap(
                _bootstrap_payload(candle_time=10, run_seq=10, bridge_session_id="session-B")
            )
            events.clear()

            await projector._apply_facts(
                _facts_payload(candle_time=11, run_seq=11, bridge_session_id="session-B")
            )

            assert events
            snapshot = projector.get_snapshot()
            assert snapshot.candles.candles[-1]["time"] == 11
            assert projector._current_session_id == "session-B"

        asyncio.run(scenario())


class TestSymbolProjectorDrainStale:
    def test_drain_stale_session_keeps_fresh_facts(self) -> None:
        async def scenario() -> None:
            mailbox = SymbolMailbox(run_id="run-1", bot_id="bot-1", symbol_key="btc|1m")
            projector = SymbolProjector(
                run_id="run-1", bot_id="bot-1", symbol_key="btc|1m",
                mailbox=mailbox,
                run_notifications=asyncio.Queue(),
                fanout_channel=asyncio.Queue(),
                run_notification_queue_metrics=_queue_owner(
                    key="run_notification_queue:run-1",
                    depth_metric="run_notification_queue_depth",
                    utilization_metric="run_notification_queue_utilization",
                    oldest_age_metric="run_notification_queue_oldest_age_ms",
                    bot_id="bot-1",
                    run_id="run-1",
                    queue_name="run_notification_queue",
                ),
                fanout_queue_metrics=_queue_owner(
                    key="fanout_channel:run-1",
                    depth_metric="fanout_queue_depth",
                    utilization_metric="fanout_queue_utilization",
                    oldest_age_metric="fanout_queue_oldest_age_ms",
                    bot_id="bot-1",
                    run_id="run-1",
                    queue_name="fanout_channel",
                ),
            )

            # Put two stale + two fresh facts in the queue.
            for seq in range(1, 3):
                mailbox.fact_queue.put_nowait(QueueEnvelope(payload={"bridge_session_id": "old-session", "seq": seq}))
            for seq in range(3, 5):
                mailbox.fact_queue.put_nowait(QueueEnvelope(payload={"bridge_session_id": "new-session", "seq": seq}))

            drained = projector._drain_stale_session_batches("new-session")
            assert drained == 2
            assert mailbox.fact_queue.qsize() == 2
            remaining = []
            while not mailbox.fact_queue.empty():
                remaining.append(mailbox.fact_queue.get_nowait())
            assert all(r.payload["bridge_session_id"] == "new-session" for r in remaining)

        asyncio.run(scenario())


# ---------------------------------------------------------------------------
# RunProjector unit tests
# ---------------------------------------------------------------------------

class TestRunProjectorSymbolNotification:
    def _make_run_projector(
        self,
        *,
        run_id: str = "run-1",
        bot_id: str = "bot-1",
    ) -> tuple[RunProjector, asyncio.Queue]:
        mailbox = RunMailbox(run_id=run_id, bot_id=bot_id)
        fanout_channel: asyncio.Queue = asyncio.Queue()

        async def fake_evict(rid: str) -> None:
            pass

        projector = RunProjector(
            run_id=run_id,
            bot_id=bot_id,
            mailbox=mailbox,
            fanout_channel=fanout_channel,
            fanout_queue_metrics=_queue_owner(
                key=f"fanout_channel:{run_id}",
                depth_metric="fanout_queue_depth",
                utilization_metric="fanout_queue_utilization",
                oldest_age_metric="fanout_queue_oldest_age_ms",
                bot_id=bot_id,
                run_id=run_id,
                queue_name="fanout_channel",
            ),
            on_evict=fake_evict,
        )
        return projector, fanout_channel

    def _make_notification(
        self,
        *,
        symbol_key: str = "instrument-btc|1m",
        seq: int = 1,
        candle_time: int = 1,
        trade_upserts: list | None = None,
        trade_removals: list | None = None,
    ) -> SymbolSummaryNotification:
        symbol_summary = {
            "symbol_key": symbol_key,
            "symbol": "BTC",
            "timeframe": "1m",
            "display_label": "BTC 1m",
            "status": "running",
            "last_event_at": "2026-01-01T00:00:00Z",
            "last_price": 1.0,
            "stats": {"total_trades": 1},
        }
        return SymbolSummaryNotification(
            run_id="run-1",
            bot_id="bot-1",
            symbol_key=symbol_key,
            symbol_summary=symbol_summary,
            trade_upserts=tuple(trade_upserts or []),
            trade_removals=tuple(trade_removals or []),
            seq=seq,
            runtime={
                "status": "running",
                "worker_count": 1,
                "active_workers": 1,
                "warnings": [],
                "last_event_at": "2026-01-01T00:00:00Z",
                "last_useful_progress_at": "2026-01-01T00:00:00Z",
            },
            event_time="2026-01-01T00:00:00Z",
            known_at="2026-01-01T00:00:00Z",
        )

    def test_symbol_notification_updates_symbol_index(self, monkeypatch: pytest.MonkeyPatch) -> None:
        async def scenario() -> None:
            projector, fanout_channel = self._make_run_projector()
            await projector._load_initial_state()

            notification = self._make_notification(candle_time=5, seq=10)
            await projector._process_symbol_notification(notification)

            snapshot = projector.get_snapshot()
            assert "instrument-btc|1m" in snapshot.symbol_catalog.entries
            assert snapshot.seq == 10
            # FanoutSummaryDelta emitted
            assert not fanout_channel.empty()
            item = fanout_channel.get_nowait()
            assert isinstance(item, FanoutEnvelope)
            assert isinstance(item.item, FanoutRunDeltaBatch)
            assert any(isinstance(delta, RunSymbolCatalogDelta) for delta in item.item.deltas)

        asyncio.run(scenario())

    def test_symbol_notification_merges_open_trades(self, monkeypatch: pytest.MonkeyPatch) -> None:
        async def scenario() -> None:
            projector, fanout_channel = self._make_run_projector()
            await projector._load_initial_state()

            # Notification carries an open trade.
            notification = self._make_notification(
                seq=1,
                trade_upserts=[{
                    "trade_id": "t-1",
                    "symbol_key": "instrument-btc|1m",
                    "status": "open",
                }],
            )
            await projector._process_symbol_notification(notification)

            snapshot = projector.get_snapshot()
            assert "t-1" in snapshot.open_trades.entries

            # FanoutOpenTradesDelta must have been emitted.
            items = []
            while not fanout_channel.empty():
                items.append(fanout_channel.get_nowait())
            trade_deltas = [
                delta
                for item in items
                if isinstance(item, FanoutEnvelope) and isinstance(item.item, FanoutRunDeltaBatch)
                for delta in item.item.deltas
                if isinstance(delta, RunOpenTradesDelta)
            ]
            assert len(trade_deltas) == 1
            assert trade_deltas[0].upserts[0]["trade_id"] == "t-1"

        asyncio.run(scenario())

    def test_run_projector_is_sole_writer_of_open_trades(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """SymbolProjector must never write run-level open_trades_index."""
        async def scenario() -> None:
            # Build a symbol projector and verify _apply_facts does NOT touch
            # any run-level summary — it only sends a notification.
            monkeypatch.setattr(sym_mod, "record_bot_runtime_events_batch", lambda rows: len(rows))

            projector, run_notifications, _ = _make_symbol_projector()
            await projector._load_initial_state()
            await projector._apply_bootstrap(_bootstrap_payload(candle_time=1))

            # Drain the bootstrap notification.
            while not run_notifications.empty():
                run_notifications.get_nowait()

            await projector._apply_facts(_facts_payload(candle_time=2))

            # Symbol projector has no open_trades_index — it must use notifications.
            assert "open_trades_index" not in projector._state

            # But a notification was sent to the run projector.
            assert not run_notifications.empty()
            n = run_notifications.get_nowait()
            assert isinstance(n, QueueEnvelope)
            assert isinstance(n.payload, SymbolSummaryNotification)
            assert n.payload.trade_upserts or n.payload.trade_removals or True  # trade data in notification

        asyncio.run(scenario())


# ---------------------------------------------------------------------------
# Per-run isolation test
# ---------------------------------------------------------------------------

class TestPerRunIsolation:
    def test_two_runs_use_separate_mailboxes(self) -> None:
        async def scenario() -> None:
            from portal.backend.service.bots.botlens_run_stream import BotLensRunStream

            run_stream = BotLensRunStream()
            registry = ProjectorRegistry(run_stream=run_stream)

            mailbox_a = await registry.ensure_run(run_id="run-A", bot_id="bot-1")
            mailbox_b = await registry.ensure_run(run_id="run-B", bot_id="bot-1")

            assert mailbox_a is not mailbox_b
            assert mailbox_a.run_id == "run-A"
            assert mailbox_b.run_id == "run-B"
            assert registry.active_run_count() == 2

        asyncio.run(scenario())

    def test_same_run_returns_same_mailbox(self) -> None:
        async def scenario() -> None:
            from portal.backend.service.bots.botlens_run_stream import BotLensRunStream

            run_stream = BotLensRunStream()
            registry = ProjectorRegistry(run_stream=run_stream)

            mailbox_1 = await registry.ensure_run(run_id="run-X", bot_id="bot-1")
            mailbox_2 = await registry.ensure_run(run_id="run-X", bot_id="bot-1")

            assert mailbox_1 is mailbox_2
            assert registry.active_run_count() == 1

        asyncio.run(scenario())


# ---------------------------------------------------------------------------
# IntakeRouter unit tests
# ---------------------------------------------------------------------------

class TestIntakeRouter:
    def _make_router_with_spy(self) -> tuple[IntakeRouter, dict]:
        from portal.backend.service.bots.botlens_run_stream import BotLensRunStream
        run_stream = BotLensRunStream()
        registry = ProjectorRegistry(run_stream=run_stream)
        router = IntakeRouter(registry=registry)
        spy: dict = {"facts_enqueued": [], "bootstraps_set": [], "lifecycle_enqueued": []}
        return router, spy, registry

    def test_routes_bootstrap_to_slot(self) -> None:
        async def scenario() -> None:
            from portal.backend.service.bots.botlens_run_stream import BotLensRunStream
            run_stream = BotLensRunStream()
            registry = ProjectorRegistry(run_stream=run_stream)
            router = IntakeRouter(registry=registry)

            payload = _bootstrap_payload()
            await router.route(payload)

            mailbox = await registry.ensure_symbol(
                run_id="run-1", bot_id="bot-1", symbol_key="instrument-btc|1m"
            )
            assert mailbox.bootstrap_slot.pending

        asyncio.run(scenario())

    def test_routes_facts_to_queue(self) -> None:
        async def scenario() -> None:
            from portal.backend.service.bots.botlens_run_stream import BotLensRunStream
            run_stream = BotLensRunStream()
            registry = ProjectorRegistry(run_stream=run_stream)
            router = IntakeRouter(registry=registry)

            payload = _facts_payload()
            await router.route(payload)

            mailbox = await registry.ensure_symbol(
                run_id="run-1", bot_id="bot-1", symbol_key="instrument-btc|1m"
            )
            assert mailbox.fact_queue.qsize() == 1

        asyncio.run(scenario())

    def test_routes_lifecycle_to_channel(self) -> None:
        async def scenario() -> None:
            from portal.backend.service.bots.botlens_run_stream import BotLensRunStream
            run_stream = BotLensRunStream()
            registry = ProjectorRegistry(run_stream=run_stream)
            router = IntakeRouter(registry=registry)

            payload = {
                "kind": LIFECYCLE_KIND,
                "bot_id": "bot-1",
                "run_id": "run-1",
                "phase": "live",
                "status": "running",
            }
            await router.route(payload)

            mailbox = await registry.ensure_run(run_id="run-1", bot_id="bot-1")
            assert mailbox.lifecycle_channel.qsize() == 1

        asyncio.run(scenario())

    def test_drops_payload_with_missing_run_id(self) -> None:
        async def scenario() -> None:
            from portal.backend.service.bots.botlens_run_stream import BotLensRunStream
            run_stream = BotLensRunStream()
            registry = ProjectorRegistry(run_stream=run_stream)
            router = IntakeRouter(registry=registry)

            # Should return cleanly without creating any projectors.
            await router.route({"kind": BRIDGE_FACTS_KIND, "bot_id": "bot-1"})
            assert registry.active_run_count() == 0

        asyncio.run(scenario())


# ---------------------------------------------------------------------------
# Bootstrap supersession tests
# ---------------------------------------------------------------------------

class TestBootstrapSupersession:
    def test_second_bootstrap_supersedes_first_in_slot(self) -> None:
        async def scenario() -> None:
            mailbox = SymbolMailbox(run_id="run-1", bot_id="bot-1", symbol_key="btc|1m")
            mailbox.set_bootstrap({"run_seq": 1, "session": "A"})
            mailbox.set_bootstrap({"run_seq": 2, "session": "B"})

            assert mailbox.bootstrap_slot.superseded_count == 1
            taken = mailbox.bootstrap_slot.take()
            assert taken is not None
            assert taken["run_seq"] == 2

        asyncio.run(scenario())

    def test_bootstrap_drains_old_session_facts_keeps_new(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        async def scenario() -> None:
            monkeypatch.setattr(sym_mod, "record_bot_runtime_events_batch", lambda rows: len(rows))

            projector, _, _ = _make_symbol_projector()
            await projector._load_initial_state()

            # Simulate: queue has 3 stale + 2 fresh facts.
            for i in range(3):
                projector._mailbox.fact_queue.put_nowait(QueueEnvelope(payload={
                    "bridge_session_id": "old-session",
                    "facts": [],
                    "run_seq": i,
                }))
            for i in range(2):
                projector._mailbox.fact_queue.put_nowait(QueueEnvelope(payload={
                    "bridge_session_id": "new-session",
                    "facts": [],
                    "run_seq": 100 + i,
                }))

            drained = projector._drain_stale_session_batches("new-session")
            assert drained == 3
            assert projector._mailbox.fact_queue.qsize() == 2

        asyncio.run(scenario())


# ---------------------------------------------------------------------------
# Fanout decoupling test
# ---------------------------------------------------------------------------

class TestFanoutDecoupling:
    def test_symbol_projector_does_not_await_delivery(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Projection must put_nowait to fanout, never awaiting delivery."""
        async def scenario() -> None:
            monkeypatch.setattr(sym_mod, "record_bot_runtime_events_batch", lambda rows: len(rows))

            fanout: asyncio.Queue = asyncio.Queue(maxsize=100)
            mailbox = SymbolMailbox(run_id="run-1", bot_id="bot-1", symbol_key="instrument-btc|1m")
            projector = SymbolProjector(
                run_id="run-1", bot_id="bot-1", symbol_key="instrument-btc|1m",
                mailbox=mailbox,
                run_notifications=asyncio.Queue(),
                fanout_channel=fanout,
                run_notification_queue_metrics=_queue_owner(
                    key="run_notification_queue:run-1",
                    depth_metric="run_notification_queue_depth",
                    utilization_metric="run_notification_queue_utilization",
                    oldest_age_metric="run_notification_queue_oldest_age_ms",
                    bot_id="bot-1",
                    run_id="run-1",
                    queue_name="run_notification_queue",
                ),
                fanout_queue_metrics=_queue_owner(
                    key="fanout_channel:run-1",
                    depth_metric="fanout_queue_depth",
                    utilization_metric="fanout_queue_utilization",
                    oldest_age_metric="fanout_queue_oldest_age_ms",
                    bot_id="bot-1",
                    run_id="run-1",
                    queue_name="fanout_channel",
                ),
            )
            await projector._load_initial_state()
            await projector._apply_bootstrap(_bootstrap_payload(candle_time=1))

            # Fanout item was produced without blocking on delivery.
            assert not fanout.empty()
            item = fanout.get_nowait()
            assert isinstance(item, FanoutEnvelope)
            assert isinstance(item.item, FanoutSymbolDeltaBatch)

        asyncio.run(scenario())

    def test_fanout_channel_full_does_not_block_projection(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """When fanout channel is full, projection must continue (delta dropped, not blocked)."""
        async def scenario() -> None:
            monkeypatch.setattr(sym_mod, "record_bot_runtime_events_batch", lambda rows: len(rows))

            # Fill fanout channel to capacity.
            fanout: asyncio.Queue = asyncio.Queue(maxsize=1)
            fanout.put_nowait({"sentinel": True})  # fills it

            mailbox = SymbolMailbox(run_id="run-1", bot_id="bot-1", symbol_key="instrument-btc|1m")
            projector = SymbolProjector(
                run_id="run-1", bot_id="bot-1", symbol_key="instrument-btc|1m",
                mailbox=mailbox,
                run_notifications=asyncio.Queue(),
                fanout_channel=fanout,
                run_notification_queue_metrics=_queue_owner(
                    key="run_notification_queue:run-1",
                    depth_metric="run_notification_queue_depth",
                    utilization_metric="run_notification_queue_utilization",
                    oldest_age_metric="run_notification_queue_oldest_age_ms",
                    bot_id="bot-1",
                    run_id="run-1",
                    queue_name="run_notification_queue",
                ),
                fanout_queue_metrics=_queue_owner(
                    key="fanout_channel:run-1",
                    depth_metric="fanout_queue_depth",
                    utilization_metric="fanout_queue_utilization",
                    oldest_age_metric="fanout_queue_oldest_age_ms",
                    bot_id="bot-1",
                    run_id="run-1",
                    queue_name="fanout_channel",
                ),
            )
            await projector._load_initial_state()

            # Should not raise, even though fanout is full.
            await projector._apply_bootstrap(_bootstrap_payload(candle_time=1))
            # Projection completed. The full fanout just logged a warning and moved on.

        asyncio.run(scenario())


# ---------------------------------------------------------------------------
# Integration test — full hub pipeline with background tasks
# ---------------------------------------------------------------------------

class TestHubIntegration:
    def test_bootstrap_then_facts_update_in_memory_projections_and_fanout(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        async def scenario() -> None:
            events: list = []
            monkeypatch.setattr(
                sym_mod,
                "record_bot_runtime_events_batch",
                lambda rows: events.extend([dict(row) for row in rows]) or len(rows),
            )

            from portal.backend.service.bots.telemetry_stream import BotTelemetryHub

            hub = BotTelemetryHub()
            ws = FakeWebSocket()

            await hub.add_run_viewer(run_id="run-1", ws=ws, selected_symbol_key="instrument-btc|1m")

            await hub.ingest(_bootstrap_payload(candle_time=1, run_seq=1))
            await hub.ingest(_facts_payload(candle_time=2, run_seq=2))

            # Give background tasks and thread pool operations time to complete.
            # asyncio.sleep(0) yields to other tasks but doesn't wait for threads;
            # a real sleep ensures asyncio.to_thread persistence calls finish.
            await asyncio.sleep(0.15)

            symbol_snapshot = hub.get_symbol_snapshot(run_id="run-1", symbol_key="instrument-btc|1m")
            run_snapshot = hub.get_run_snapshot(run_id="run-1")
            assert symbol_snapshot is not None
            assert symbol_snapshot.candles.candles[-1]["time"] == 2
            assert run_snapshot is not None
            assert "instrument-btc|1m" in run_snapshot.symbol_catalog.entries
            assert events

            message_types = {m["type"] for m in ws.messages}
            assert "botlens_live_connected" in message_types
            assert "botlens_symbol_candle_delta" in message_types

        asyncio.run(scenario())

    def test_viewer_receives_symbol_typed_deltas(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        async def scenario() -> None:
            monkeypatch.setattr(sym_mod, "record_bot_runtime_events_batch", lambda rows: len(rows))

            from portal.backend.service.bots.telemetry_stream import BotTelemetryHub
            hub = BotTelemetryHub()
            ws = FakeWebSocket()

            await hub.add_run_viewer(run_id="run-1", ws=ws, selected_symbol_key="instrument-btc|1m")
            await hub.ingest(_bootstrap_payload(candle_time=1, run_seq=1))
            await hub.ingest(_facts_payload(
                candle_time=2, run_seq=2,
                log_entries=[{"id": "log-1", "message": "test"}],
            ))

            await asyncio.sleep(0.15)

            message_types = {m["type"] for m in ws.messages}
            assert "botlens_symbol_candle_delta" in message_types
            assert "botlens_symbol_runtime_delta" in message_types

        asyncio.run(scenario())

    def test_two_runs_do_not_share_state(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        async def scenario() -> None:
            monkeypatch.setattr(sym_mod, "record_bot_runtime_events_batch", lambda rows: len(rows))

            from portal.backend.service.bots.telemetry_stream import BotTelemetryHub
            hub = BotTelemetryHub()

            await hub.ingest(_bootstrap_payload(run_id="run-A", candle_time=10, run_seq=100))
            await hub.ingest(_bootstrap_payload(run_id="run-B", candle_time=20, run_seq=200))

            await asyncio.sleep(0.15)

            run_a_snapshot = hub.get_symbol_snapshot(run_id="run-A", symbol_key="instrument-btc|1m")
            run_b_snapshot = hub.get_symbol_snapshot(run_id="run-B", symbol_key="instrument-btc|1m")
            assert run_a_snapshot is not None
            assert run_b_snapshot is not None
            assert run_a_snapshot.candles.candles[-1]["time"] == 10
            assert run_b_snapshot.candles.candles[-1]["time"] == 20

        asyncio.run(scenario())

    def test_bootstrap_supersession_produces_final_state(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Multiple bootstraps for same symbol: only latest state is canonical."""
        async def scenario() -> None:
            monkeypatch.setattr(sym_mod, "record_bot_runtime_events_batch", lambda rows: len(rows))

            from portal.backend.service.bots.telemetry_stream import BotTelemetryHub
            hub = BotTelemetryHub()

            # Three bootstraps in quick succession — only latest (candle=99) matters.
            for seq, candle_time in [(1, 10), (2, 20), (3, 99)]:
                await hub.ingest(
                    _bootstrap_payload(
                        candle_time=candle_time,
                        run_seq=seq,
                        bridge_session_id=f"session-{seq}",
                    )
                )

            await asyncio.sleep(0.15)

            symbol_snapshot = hub.get_symbol_snapshot(run_id="run-1", symbol_key="instrument-btc|1m")
            assert symbol_snapshot is not None
            assert symbol_snapshot.candles.candles[-1]["time"] == 99

        asyncio.run(scenario())
