from __future__ import annotations

import asyncio
from typing import Any

import pytest

from portal.backend.service.bots.botlens_contract import BRIDGE_BOOTSTRAP_KIND, BRIDGE_FACTS_KIND, LIFECYCLE_KIND
from portal.backend.service.bots.botlens_intake_router import IntakeRouter
from portal.backend.service.bots.botlens_mailbox import RunMailbox, SymbolMailbox
import portal.backend.service.bots.botlens_intake_router as intake_mod


class _FakeRegistry:
    def __init__(self) -> None:
        self.run_mailboxes: dict[str, RunMailbox] = {}
        self.symbol_mailboxes: dict[tuple[str, str], SymbolMailbox] = {}

    async def ensure_symbol(self, *, run_id: str, bot_id: str, symbol_key: str) -> SymbolMailbox:
        key = (str(run_id), str(symbol_key))
        mailbox = self.symbol_mailboxes.get(key)
        if mailbox is None:
            mailbox = SymbolMailbox(run_id=str(run_id), bot_id=str(bot_id), symbol_key=str(symbol_key))
            self.symbol_mailboxes[key] = mailbox
        return mailbox

    async def ensure_run(self, *, run_id: str, bot_id: str) -> RunMailbox:
        key = str(run_id)
        mailbox = self.run_mailboxes.get(key)
        if mailbox is None:
            mailbox = RunMailbox(run_id=str(run_id), bot_id=str(bot_id))
            self.run_mailboxes[key] = mailbox
        return mailbox


def _facts_payload(*, run_seq: int, candle_time: int) -> dict[str, Any]:
    return {
        "kind": BRIDGE_FACTS_KIND,
        "bot_id": "bot-1",
        "run_id": "run-1",
        "series_key": "instrument-btc|1m",
        "run_seq": run_seq,
        "bridge_session_id": "session-1",
        "bridge_seq": run_seq,
        "event_time": "2026-01-01T00:01:00Z",
        "known_at": "2026-01-01T00:01:00Z",
        "facts": [
            {
                "fact_type": "runtime_state_observed",
                "runtime": {
                    "status": "running",
                    "worker_count": 2,
                    "active_workers": 1,
                    "warnings": [
                        {
                            "warning_id": "warn-1",
                            "warning_type": "runtime",
                            "severity": "warning",
                            "message": "queue healthy",
                        }
                    ],
                },
            },
            {
                "fact_type": "series_state_observed",
                "series_key": "instrument-btc|1m",
                "instrument_id": "instrument-btc",
                "symbol": "BTC",
                "timeframe": "1m",
            },
            {
                "fact_type": "candle_upserted",
                "series_key": "instrument-btc|1m",
                "candle": {
                    "time": f"2026-01-01T00:0{candle_time}:00Z",
                    "open": float(candle_time),
                    "high": float(candle_time) + 1.0,
                    "low": float(candle_time) - 1.0,
                    "close": float(candle_time) + 0.5,
                },
            },
            {
                "fact_type": "trade_opened",
                "series_key": "instrument-btc|1m",
                "trade": {
                    "trade_id": f"trade-{run_seq}",
                    "status": "open",
                    "direction": "long",
                    "opened_at": f"2026-01-01T00:0{candle_time}:00Z",
                    "bar_time": f"2026-01-01T00:0{candle_time}:00Z",
                },
            },
        ],
    }


def test_intake_router_persists_only_budgeted_transport_rows_and_skips_canonical_trade_truth(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def scenario() -> None:
        persisted_batches: list[dict[str, Any]] = []

        async def _persist_rows(*, rows, context):
            persisted_batches.append(
                {
                    "row_count": len(rows),
                    "event_ids": [str(row.get("event_id") or "") for row in rows],
                    "event_names": [
                        str((row.get("payload") or {}).get("event_name") or "")
                        for row in rows
                    ],
                    "context": dict(context or {}),
                }
            )
            return len(rows)

        registry = _FakeRegistry()
        router = IntakeRouter(registry=registry)
        monkeypatch.setattr(router, "_persist_rows", _persist_rows)

        await router.route(_facts_payload(run_seq=2, candle_time=2))

        mailbox = await registry.ensure_symbol(
            run_id="run-1",
            bot_id="bot-1",
            symbol_key="instrument-btc|1m",
        )

        assert mailbox.fact_queue.qsize() == 1
        assert len(persisted_batches) == 1
        assert persisted_batches[0]["row_count"] == 1
        assert set(persisted_batches[0]["event_names"]) == {"SERIES_METADATA_REPORTED"}

    asyncio.run(scenario())


def test_intake_router_treats_source_persisted_wallet_facts_as_canonical(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def scenario() -> None:
        persisted_batches: list[dict[str, Any]] = []

        async def _persist_rows(*, rows, context):
            persisted_batches.append(
                {
                    "event_names": [
                        str((row.get("payload") or {}).get("event_name") or "")
                        for row in rows
                    ],
                    "context": dict(context or {}),
                }
            )
            return len(rows)

        registry = _FakeRegistry()
        router = IntakeRouter(registry=registry)
        monkeypatch.setattr(router, "_persist_rows", _persist_rows)

        payload = _facts_payload(run_seq=2, candle_time=2)
        payload["facts"].append(
            {
                "fact_type": "wallet_ledger_event",
                "series_key": "instrument-btc|1m",
                "wallet_event": {
                    "event_name": "MARGIN_RESERVED",
                    "event_id": "wallet-margin-reserved-1",
                    "event_ts": "2026-01-01T00:02:00Z",
                    "known_at": "2026-01-01T00:02:00Z",
                    "source_run_seq": 2,
                    "source_run_seq_status": "runtime_assigned",
                    "wallet_commit_seq": 1,
                    "wallet_commit_seq_status": "runtime_assigned",
                    "wallet_eval_seq": 0,
                    "wallet_event_order": 10,
                    "run_id": "run-1",
                    "bot_id": "bot-1",
                    "series_key": "instrument-btc|1m",
                    "instrument_id": "instrument-btc",
                    "symbol": "BTC",
                    "timeframe": "1m",
                    "currency": "USD",
                    "balance_before": 1000.0,
                    "balance_after": 1000.0,
                    "equity_before": 1000.0,
                    "equity_after": 1000.0,
                    "free_collateral_before": 1000.0,
                    "free_collateral_after": 900.0,
                    "locked_margin_before": 0.0,
                    "locked_margin_after": 100.0,
                    "margin_required": 100.0,
                    "margin_reserved": 100.0,
                    "margin_available": 1000.0,
                    "reason": "entry_fill",
                    "wallet_before": {
                        "balances": {"USD": 1000.0},
                        "locked_margin": {},
                        "free_collateral": {"USD": 1000.0},
                        "margin_positions": {},
                    },
                    "wallet_after": {
                        "balances": {"USD": 1000.0},
                        "locked_margin": {"USD": 100.0},
                        "free_collateral": {"USD": 900.0},
                        "margin_positions": {},
                    },
                },
            }
        )

        await router.route(payload)

        assert len(persisted_batches) == 1
        assert set(persisted_batches[0]["event_names"]) == {
            "SERIES_METADATA_REPORTED",
        }
        assert "MARGIN_RESERVED" not in persisted_batches[0]["event_names"]

    asyncio.run(scenario())


def test_intake_router_filters_repeated_derived_event_ids_before_db_write(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def scenario() -> None:
        persisted_batches: list[list[str]] = []

        async def _persist_rows(*, rows, context):
            _ = context
            persisted_batches.append([str(row.get("event_id") or "") for row in rows])
            return len(rows)

        registry = _FakeRegistry()
        router = IntakeRouter(registry=registry)
        monkeypatch.setattr(router, "_persist_rows", _persist_rows)

        payload = _facts_payload(run_seq=2, candle_time=2)
        await router.route(payload)
        await router.route(payload)

        assert len(persisted_batches) == 1
        assert len(persisted_batches[0]) == 1

    asyncio.run(scenario())


def test_intake_router_routes_continuity_instrumented_bootstrap_and_facts_without_crashing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def scenario() -> None:
        persisted_batches: list[dict[str, Any]] = []

        async def _persist_rows(*, rows, context):
            persisted_batches.append(
                {
                    "row_count": len(rows),
                    "context": dict(context or {}),
                }
            )
            return len(rows)

        async def _startup_bootstrap_allowed(*, run_id: str, bot_id: str):
            _ = run_id, bot_id
            return True, "initializing"

        registry = _FakeRegistry()
        router = IntakeRouter(registry=registry)
        monkeypatch.setattr(router, "_persist_rows", _persist_rows)
        monkeypatch.setattr(router, "_startup_bootstrap_allowed", _startup_bootstrap_allowed)

        bootstrap_payload = {
            **_facts_payload(run_seq=1, candle_time=1),
            "kind": BRIDGE_BOOTSTRAP_KIND,
        }
        facts_payload = _facts_payload(run_seq=2, candle_time=2)
        facts_payload["facts"].append(
            {
                "fact_type": "candle_upserted",
                "series_key": "instrument-btc|1m",
                "candle": {
                    "time": "2026-01-01T00:04:00Z",
                    "open": 4.0,
                    "high": 5.0,
                    "low": 3.0,
                    "close": 4.5,
                },
            }
        )

        await router.route(bootstrap_payload)
        await router.route(facts_payload)

        mailbox = await registry.ensure_symbol(
            run_id="run-1",
            bot_id="bot-1",
            symbol_key="instrument-btc|1m",
        )

        assert mailbox.bootstrap_slot.pending
        assert mailbox.fact_queue.qsize() == 1
        assert [batch["context"]["message_kind"] for batch in persisted_batches] == [
            BRIDGE_BOOTSTRAP_KIND,
            BRIDGE_FACTS_KIND,
        ]

    asyncio.run(scenario())


def test_intake_router_lifecycle_ingest_skips_persistence_and_only_enqueues_projector_batch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def scenario() -> None:
        persisted_called = False

        def _record(rows, *, context=None):
            nonlocal persisted_called
            persisted_called = True
            _ = rows, context
            return 0

        monkeypatch.setattr(intake_mod, "record_bot_runtime_events_batch", _record)

        registry = _FakeRegistry()
        router = IntakeRouter(registry=registry)

        await router.route(
            {
                "kind": LIFECYCLE_KIND,
                "bot_id": "bot-1",
                "run_id": "run-1",
                "seq": 9,
                "phase": "live",
                "status": "running",
                "owner": "runtime",
                "message": "Bot is live.",
                "checkpoint_at": "2026-01-01T00:02:00Z",
                "metadata": {
                    "runtime_observability": {
                        "runtime_state": "live",
                        "progress_state": "progressing",
                    }
                },
            }
        )

        mailbox = await registry.ensure_run(run_id="run-1", bot_id="bot-1")
        envelope = mailbox.lifecycle_queue.get_nowait()
        batch = envelope.payload

        assert persisted_called is False
        assert batch.seq == 9
        assert [event.event_name.value for event in batch.events] == ["RUN_READY"]

    asyncio.run(scenario())
