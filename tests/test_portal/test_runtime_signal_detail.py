from __future__ import annotations

import pytest

pytest.importorskip("sqlalchemy")

from portal.backend.service.bots import ledger_service


def test_get_run_signal_detail_resolves_runtime_signal_and_audit(monkeypatch) -> None:
    rows = [
        {
            "event_id": "evt-signal-1",
            "seq": 1,
            "bot_id": "bot-1",
            "run_id": "run-1",
            "event_type": "runtime.signal",
            "created_at": "2026-02-01T00:00:00Z",
            "known_at": "2026-02-01T00:00:00Z",
            "payload": {
                "event_id": "evt-signal-1",
                "event_name": "SIGNAL_EMITTED",
                "category": "SIGNAL",
                "root_id": "evt-signal-1",
                "correlation_id": "corr-1",
                "strategy_id": "strategy-1",
                "symbol": "ES",
                "timeframe": "1h",
                "payload": {
                    "signal_id": "decision-1",
                    "source_type": "runtime",
                    "source_id": "run-1",
                    "signal_type": "strategy_signal",
                    "direction": "long",
                    "signal_price": 100.5,
                    "strategy_hash": "hash-1",
                    "decision_id": "decision-1",
                    "rule_id": "rule-1",
                    "intent": "enter_long",
                    "event_key": "breakout_long",
                    "decision_artifact": {
                        "bar_epoch": 1769904000,
                    },
                    "bar": {
                        "time": "2026-02-01T00:00:00Z",
                        "open": 100.0,
                        "high": 101.0,
                        "low": 99.0,
                        "close": 100.5,
                    },
                },
            },
        },
        {
            "event_id": "evt-decision-1",
            "seq": 2,
            "bot_id": "bot-1",
            "run_id": "run-1",
            "event_type": "runtime.decision",
            "created_at": "2026-02-01T00:00:01Z",
            "known_at": "2026-02-01T00:00:01Z",
            "payload": {
                "event_id": "evt-decision-1",
                "event_name": "DECISION_ACCEPTED",
                "category": "DECISION",
                "root_id": "evt-signal-1",
                "parent_id": "evt-signal-1",
                "correlation_id": "corr-1",
                "strategy_id": "strategy-1",
                "symbol": "ES",
                "timeframe": "1h",
                "payload": {
                    "signal_id": "decision-1",
                    "source_type": "runtime",
                    "source_id": "run-1",
                    "decision": "accepted",
                    "direction": "long",
                    "signal_price": 100.5,
                    "strategy_hash": "hash-1",
                    "decision_id": "decision-1",
                    "rule_id": "rule-1",
                    "intent": "enter_long",
                    "event_key": "breakout_long",
                    "event_subtype": "signal_accepted",
                },
            },
        },
    ]

    monkeypatch.setattr(
        ledger_service,
        "list_bot_runtime_events",
        lambda **kwargs: rows,
    )

    detail = ledger_service.get_run_signal_detail(
        bot_id="bot-1",
        run_id="run-1",
        signal_id="decision-1",
    )

    assert detail["signal"] == {
        "epoch": 1769904000,
        "direction": "long",
        "signal_id": "decision-1",
        "source_type": "runtime",
        "source_id": "run-1",
        "strategy_hash": "hash-1",
        "decision_id": "decision-1",
        "rule_id": "rule-1",
        "intent": "enter_long",
        "event_key": "breakout_long",
    }
    assert detail["audit"]["signal_event"]["event_name"] == "SIGNAL_EMITTED"
    assert detail["audit"]["decision_artifact"] == {"bar_epoch": 1769904000}
    assert [event["event_name"] for event in detail["audit"]["related_events"]] == [
        "SIGNAL_EMITTED",
        "DECISION_ACCEPTED",
    ]
