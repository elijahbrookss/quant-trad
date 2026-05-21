from __future__ import annotations

from portal.backend.service.bots import botlens_forensics_service as svc
from portal.backend.service.bots.botlens_retrieval_queries import DomainTruthEvent


def _domain_event(
    *,
    row_id: int,
    seq: int,
    event_id: str,
    event_name: str,
    root_event_id: str,
    correlation_id: str,
    context: dict,
) -> DomainTruthEvent:
    return DomainTruthEvent(
        row_id=row_id,
        seq=seq,
        bot_id="bot-1",
        run_id="run-1",
        event_id=event_id,
        event_name=event_name,
        event_type=f"botlens_domain.{event_name.lower()}",
        event_ts="2026-02-01T00:00:00Z",
        created_at="2026-02-01T00:00:00Z",
        known_at="2026-02-01T00:00:00Z",
        root_event_id=root_event_id,
        parent_event_id=None,
        correlation_id=correlation_id,
        series_key="instrument-btc|1m",
        context=dict(context),
    )


def test_list_run_forensic_events_forwards_typed_filters_to_domain_query(monkeypatch) -> None:
    calls: list[dict] = []

    def _fake_page(**kwargs):
        calls.append(dict(kwargs))
        return []

    monkeypatch.setattr(svc, "list_run_domain_truth_page", _fake_page)
    monkeypatch.setattr(svc, "forensic_event_page_contract", lambda **kwargs: kwargs)

    result = svc.list_run_forensic_events(
        bot_id="bot-1",
        run_id="run-1",
        root_event_id="root-1",
        correlation_id="corr-1",
        limit=25,
    )

    assert calls == [
        {
            "bot_id": "bot-1",
            "run_id": "run-1",
            "after_seq": 0,
            "after_row_id": 0,
            "limit": 200,
            "event_names": None,
            "series_key": None,
            "root_event_id": "root-1",
            "correlation_id": "corr-1",
        }
    ]
    assert result["filters"]["root_event_id"] == "root-1"
    assert result["filters"]["correlation_id"] == "corr-1"


def test_get_run_signal_forensics_queries_signal_and_causal_filters(monkeypatch) -> None:
    signal_event = _domain_event(
        row_id=101,
        seq=11,
        event_id="evt-signal",
        event_name="SIGNAL_EMITTED",
        root_event_id="root-1",
        correlation_id="corr-1",
        context={
            "signal_id": "signal-1",
            "decision_id": "decision-1",
            "symbol": "BTC",
            "timeframe": "1m",
            "bar_time": "2026-02-01T00:00:00Z",
        },
    )
    decision_event = _domain_event(
        row_id=102,
        seq=12,
        event_id="evt-decision",
        event_name="DECISION_EMITTED",
        root_event_id="root-1",
        correlation_id="corr-1",
        context={
            "signal_id": "signal-1",
            "decision_id": "decision-1",
            "reason_code": "rule_blocked",
            "message": "rule blocked",
        },
    )
    calls: list[dict] = []

    def _fake_list_all_run_domain_truth(**kwargs):
        calls.append(dict(kwargs))
        if kwargs.get("signal_id") == "signal-1":
            return [signal_event]
        if kwargs.get("root_event_id") == "root-1":
            return [signal_event, decision_event]
        if kwargs.get("correlation_id") == "corr-1":
            return [signal_event, decision_event]
        return []

    monkeypatch.setattr(svc, "list_all_run_domain_truth", _fake_list_all_run_domain_truth)
    monkeypatch.setattr(svc, "signal_forensic_contract", lambda **kwargs: kwargs)

    result = svc.get_run_signal_forensics(bot_id="bot-1", run_id="run-1", signal_id="signal-1")

    assert calls == [
        {
            "bot_id": "bot-1",
            "run_id": "run-1",
            "event_names": ["SIGNAL_EMITTED"],
            "signal_id": "signal-1",
            "page_size": 200,
        },
        {
            "bot_id": "bot-1",
            "run_id": "run-1",
            "root_event_id": "root-1",
        },
        {
            "bot_id": "bot-1",
            "run_id": "run-1",
            "correlation_id": "corr-1",
        },
    ]
    assert result["signal"]["signal_id"] == "signal-1"
    assert [document["truth"]["event_id"] for document in result["documents"]] == ["evt-signal", "evt-decision"]
