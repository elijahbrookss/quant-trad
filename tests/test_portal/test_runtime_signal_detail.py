from __future__ import annotations

import pytest

from portal.backend.service.bots import botlens_forensics_service
from portal.backend.service.bots.botlens_retrieval_queries import DomainTruthEvent


def _truth_event(
    *,
    row_id: int,
    seq: int,
    event_name: str,
    event_id: str,
    root_event_id: str | None = None,
    parent_event_id: str | None = None,
    correlation_id: str | None = None,
    context: dict | None = None,
) -> DomainTruthEvent:
    payload = dict(context or {})
    return DomainTruthEvent(
        row_id=row_id,
        seq=seq,
        bot_id="bot-1",
        run_id="run-1",
        event_id=event_id,
        event_name=event_name,
        event_type=f"botlens_domain.{event_name.lower()}",
        event_ts=payload.get("bar_time") or "2026-02-01T00:00:00Z",
        created_at="2026-02-01T00:00:00Z",
        known_at="2026-02-01T00:00:00Z",
        root_event_id=root_event_id,
        parent_event_id=parent_event_id,
        correlation_id=correlation_id,
        series_key=payload.get("series_key"),
        context=payload,
    )


def test_list_run_forensic_events_applies_filters_before_page_slicing(monkeypatch) -> None:
    calls = []
    rows = [
        _truth_event(row_id=11, seq=1, event_name="SIGNAL_EMITTED", event_id="evt-1", correlation_id="corr-1"),
        _truth_event(row_id=12, seq=2, event_name="DECISION_EMITTED", event_id="evt-2", correlation_id="corr-2"),
        _truth_event(row_id=13, seq=3, event_name="FAULT_RECORDED", event_id="evt-3", correlation_id="corr-1"),
    ]

    def _fake_page(**kwargs):
        calls.append((kwargs["after_seq"], kwargs["after_row_id"], kwargs["limit"]))
        eligible = [
            row for row in rows
            if int(row.seq) > int(kwargs["after_seq"]) or (
                int(row.seq) == int(kwargs["after_seq"]) and int(row.row_id) > int(kwargs["after_row_id"])
            )
        ]
        eligible.sort(key=lambda row: (int(row.seq), int(row.row_id)))
        return eligible[:1]

    monkeypatch.setattr(botlens_forensics_service, "list_run_domain_truth_page", _fake_page)

    page = botlens_forensics_service.list_run_forensic_events(
        bot_id="bot-1",
        run_id="run-1",
        correlation_id="corr-1",
        limit=2,
    )

    assert [doc["truth"]["event_id"] for doc in page["documents"]] == ["evt-1", "evt-3"]
    assert page["next_cursor"] == {"after_seq": 3, "after_row_id": 13}
    assert calls[:3] == [(0, 0, 200), (1, 11, 200), (2, 12, 200)]


def test_get_run_signal_forensics_returns_causal_chain_documents(monkeypatch) -> None:
    rows = [
        _truth_event(
            row_id=21,
            seq=1,
            event_name="SIGNAL_EMITTED",
            event_id="evt-signal-1",
            root_event_id="evt-signal-1",
            correlation_id="corr-1",
            context={
                "series_key": "instrument-es|1h",
                "strategy_id": "strategy-1",
                "symbol": "ES",
                "timeframe": "1h",
                "bar_time": "2026-02-01T00:00:00Z",
                "bar_epoch": 1769904000,
                "signal_id": "signal-1",
                "decision_id": "decision-1",
                "signal_type": "strategy_signal",
                "direction": "long",
                "signal_price": 100.5,
                "intent": "enter_long",
                "rule_id": "rule-1",
                "event_key": "breakout_long",
            },
        ),
        _truth_event(
            row_id=22,
            seq=2,
            event_name="DECISION_EMITTED",
            event_id="evt-decision-1",
            root_event_id="evt-signal-1",
            parent_event_id="evt-signal-1",
            correlation_id="corr-1",
            context={
                "series_key": "instrument-es|1h",
                "signal_id": "signal-1",
                "decision_id": "decision-1",
                "decision_state": "accepted",
                "direction": "long",
                "signal_price": 100.5,
            },
        ),
        _truth_event(
            row_id=23,
            seq=3,
            event_name="TRADE_OPENED",
            event_id="evt-trade-1",
            root_event_id="evt-signal-1",
            parent_event_id="evt-decision-1",
            correlation_id="corr-1",
            context={
                "series_key": "instrument-es|1h",
                "trade_id": "trade-1",
                "signal_id": "signal-1",
                "decision_id": "decision-1",
                "trade_state": "open",
                "entry_price": 100.75,
            },
        ),
        _truth_event(
            row_id=24,
            seq=4,
            event_name="DECISION_EMITTED",
            event_id="evt-decision-2",
            root_event_id="evt-other",
            parent_event_id="evt-other",
            correlation_id="corr-2",
            context={
                "signal_id": "other-signal",
                "decision_id": "other-decision",
                "decision_state": "accepted",
            },
        ),
    ]

    monkeypatch.setattr(botlens_forensics_service, "list_all_run_domain_truth", lambda **kwargs: rows)

    detail = botlens_forensics_service.get_run_signal_forensics(
        bot_id="bot-1",
        run_id="run-1",
        signal_id="signal-1",
    )

    assert detail["contract"] == "botlens_signal_forensics"
    assert detail["signal"] == {
        "signal_id": "signal-1",
        "decision_id": "decision-1",
        "strategy_id": "strategy-1",
        "symbol_key": "instrument-es|1h",
        "symbol": "ES",
        "timeframe": "1h",
        "bar_time": "2026-02-01T00:00:00Z",
        "bar_epoch": 1769904000,
        "signal_type": "strategy_signal",
        "direction": "long",
        "signal_price": 100.5,
        "intent": "enter_long",
        "rule_id": "rule-1",
        "event_key": "breakout_long",
    }
    assert [doc["truth"]["event_id"] for doc in detail["causal_chain"]["documents"]] == [
        "evt-signal-1",
        "evt-decision-1",
        "evt-trade-1",
    ]
    assert detail["causal_chain"]["documents"][0]["truth"]["context"]["signal_id"] == "signal-1"


def test_get_run_signal_forensics_rejects_unknown_signal(monkeypatch) -> None:
    monkeypatch.setattr(botlens_forensics_service, "list_all_run_domain_truth", lambda **kwargs: [])

    with pytest.raises(KeyError, match="BotLens signal not found"):
        botlens_forensics_service.get_run_signal_forensics(
            bot_id="bot-1",
            run_id="run-1",
            signal_id="signal-1",
        )
