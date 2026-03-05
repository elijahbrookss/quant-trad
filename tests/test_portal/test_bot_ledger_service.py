from __future__ import annotations

import sys
import types

sys.modules.pop("portal.backend.service.storage", None)
sys.modules.pop("portal.backend.service.storage.storage", None)
storage_pkg = types.ModuleType("portal.backend.service.storage")
storage_pkg.__path__ = []
storage_mod = types.ModuleType("portal.backend.service.storage.storage")
storage_mod.list_bot_runtime_events = lambda *args, **kwargs: []
sys.modules["portal.backend.service.storage"] = storage_pkg
sys.modules["portal.backend.service.storage.storage"] = storage_mod

from portal.backend.service.bots import ledger_service


def test_list_run_ledger_events_projects_runtime_payload_shape(monkeypatch):
    def _fake_list_bot_runtime_events(*, bot_id, run_id, after_seq, limit, event_types):
        assert bot_id == "bot-1"
        assert run_id == "run-1"
        assert after_seq == 0
        assert limit == 500
        assert event_types == ["DECISION_ACCEPTED"]
        return [
            {
                "seq": 11,
                "bot_id": "bot-1",
                "run_id": "run-1",
                "critical": False,
                "event_time": "2026-02-28T01:00:00Z",
                "known_at": "2026-02-28T01:00:00Z",
                "created_at": "2026-02-28T01:00:00Z",
                "payload": {
                    "schema_version": 1,
                    "event_id": "evt-1",
                    "event_ts": "2026-02-28T01:00:00Z",
                    "event_name": "DECISION_ACCEPTED",
                    "category": "DECISION",
                    "root_id": "root-1",
                    "parent_id": "parent-1",
                    "correlation_id": "corr-1",
                    "strategy_id": "strategy-1",
                    "symbol": "BTC-USD",
                    "timeframe": "1h",
                    "reason_code": "DECISION_ACCEPTED",
                    "payload": {
                        "event_subtype": "signal_accepted",
                        "trade_id": "trade-1",
                        "direction": "long",
                        "qty": 1.25,
                        "price": 42000.5,
                        "wallet_delta": {"fee_paid": 1.75},
                        "event_impact_pnl": 12.3,
                        "trade_net_pnl": 77.7,
                        "context": {"rule_id": "r1"},
                    },
                },
            }
        ]

    monkeypatch.setattr(ledger_service, "list_bot_runtime_events", _fake_list_bot_runtime_events)

    payload = ledger_service.list_run_ledger_events(
        bot_id="bot-1",
        run_id="run-1",
        after_seq=0,
        limit=500,
        event_names=["decision_accepted"],
    )

    assert payload["count"] == 1
    assert payload["next_after_seq"] == 11
    event = payload["events"][0]
    assert event["event_id"] == "evt-1"
    assert event["event_type"] == "decision"
    assert event["event_subtype"] == "signal_accepted"
    assert event["trade_id"] == "trade-1"
    assert event["side"] == "long"
    assert event["qty"] == 1.25
    assert event["price"] == 42000.5
    assert event["fee_paid"] == 1.75
    assert event["context"] == {"rule_id": "r1"}


def test_list_run_ledger_events_derives_exit_subtype_and_reason_fields(monkeypatch):
    def _fake_list_bot_runtime_events(*, bot_id, run_id, after_seq, limit, event_types):
        assert event_types is None
        return [
            {
                "seq": 21,
                "bot_id": bot_id,
                "run_id": run_id,
                "critical": True,
                "event_time": "2026-02-28T02:00:00Z",
                "known_at": "2026-02-28T02:00:00Z",
                "created_at": "2026-02-28T02:00:00Z",
                "payload": {
                    "schema_version": 1,
                    "event_id": "evt-exit-1",
                    "event_ts": "2026-02-28T02:00:00Z",
                    "event_name": "EXIT_FILLED",
                    "root_id": "root-2",
                    "parent_id": "parent-2",
                    "strategy_id": "strategy-1",
                    "symbol": "SOL-USD",
                    "timeframe": "15m",
                    "payload": {
                        "trade_id": "trade-9",
                        "side": "sell",
                        "qty": 3.0,
                        "price": 188.1,
                        "exit_kind": "TARGET",
                        "reason_detail": "target reached",
                    },
                },
            }
        ]

    monkeypatch.setattr(ledger_service, "list_bot_runtime_events", _fake_list_bot_runtime_events)

    payload = ledger_service.list_run_ledger_events(bot_id="bot-1", run_id="run-1")

    assert payload["count"] == 1
    event = payload["events"][0]
    assert event["event_name"] == "EXIT_FILLED"
    assert event["event_type"] == "outcome"
    assert event["event_subtype"] == "target"
    assert event["reason_detail"] == "target reached"
    assert event["critical"] is True
