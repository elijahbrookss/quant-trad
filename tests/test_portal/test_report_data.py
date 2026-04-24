from __future__ import annotations

from typing import Any

from portal.backend.service.reports import report_data


class _FakeReportStorage:
    def __init__(self, *, run: dict[str, Any], events: list[dict[str, Any]]) -> None:
        self._run = dict(run)
        self._events = sorted([dict(event) for event in events], key=lambda row: int(row.get("seq") or 0))

    def get_bot_run(self, run_id: str):
        if run_id == self._run.get("run_id"):
            return dict(self._run)
        return None

    def list_bot_runtime_events(
        self,
        *,
        bot_id: str,
        run_id: str,
        after_seq: int,
        limit: int,
        event_types=None,
        event_type_prefixes=None,
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


def _run(**overrides):
    data = {
        "run_id": "run-1",
        "bot_id": "bot-1",
        "status": "completed",
        "decision_ledger": [],
    }
    data.update(overrides)
    return data


def _event_row(seq: int, event_name: str, context: dict[str, Any]) -> dict[str, Any]:
    event_type = f"botlens_domain.{event_name.lower()}"
    return {
        "event_id": f"evt-{seq}",
        "seq": seq,
        "event_type": event_type,
        "event_name": event_name,
        "series_key": context.get("series_key"),
        "instrument_id": context.get("instrument_id"),
        "symbol": context.get("symbol"),
        "timeframe": context.get("timeframe"),
        "signal_id": context.get("signal_id"),
        "decision_id": context.get("decision_id"),
        "trade_id": context.get("trade_id"),
        "reason_code": context.get("reason_code"),
        "bar_time": context.get("bar_time"),
        "payload": {
            "schema_version": 1,
            "event_id": f"evt-{seq}",
            "event_ts": context.get("event_time") or context.get("bar_time") or "2026-02-01T00:00:00Z",
            "event_name": event_name,
            "root_id": f"root-{seq}",
            "parent_id": None,
            "correlation_id": f"corr-{seq}",
            "context": {
                "run_id": "run-1",
                "bot_id": "bot-1",
                "series_key": "instrument-bip|1h",
                "instrument_id": "instrument-bip",
                "symbol": "BIP-20DEC30-CDE",
                "timeframe": "1h",
                "strategy_id": "strategy-1",
                **context,
            },
        },
    }


def _decision_row(
    seq: int,
    *,
    decision_state: str,
    decision_id: str,
    trade_id: str | None = None,
    reason_code: str | None = None,
) -> dict[str, Any]:
    context = {
        "decision_state": decision_state,
        "decision_id": decision_id,
        "signal_id": f"signal-{decision_id}",
        "direction": "long",
        "signal_price": 100.0,
        "trade_id": trade_id,
        "bar_time": "2026-02-01T00:00:00Z",
    }
    if reason_code:
        context["reason_code"] = reason_code
        context["message"] = reason_code
    return _event_row(seq, "DECISION_EMITTED", context)


def _trade_row(seq: int, event_name: str, trade_id: str) -> dict[str, Any]:
    return _event_row(
        seq,
        event_name,
        {
            "trade_id": trade_id,
            "bar_time": "2026-02-01T00:05:00Z",
        },
    )


def test_report_decision_ledger_reads_botlens_domain_decision_rows(monkeypatch) -> None:
    storage = _FakeReportStorage(
        run=_run(),
        events=[
            _decision_row(1, decision_state="accepted", decision_id="decision-1", trade_id="trade-1"),
            _decision_row(
                2,
                decision_state="rejected",
                decision_id="decision-2",
                reason_code="WALLET_INSUFFICIENT_MARGIN",
            ),
        ],
    )
    monkeypatch.setattr(report_data, "storage", storage)

    ledger = report_data.list_decision_ledger("run-1")

    assert [entry["decision_state"] for entry in ledger] == ["accepted", "rejected"]
    assert [entry["event_subtype"] for entry in ledger] == ["signal_accepted", "signal_rejected"]
    assert ledger[1]["reason_code"] == "WALLET_INSUFFICIENT_MARGIN"


def test_report_rejection_count_matches_rejected_domain_decision_rows(monkeypatch) -> None:
    storage = _FakeReportStorage(
        run=_run(),
        events=[
            _decision_row(1, decision_state="accepted", decision_id="decision-1", trade_id="trade-1"),
            _decision_row(2, decision_state="rejected", decision_id="decision-2", reason_code="RISK"),
            _decision_row(3, decision_state="rejected", decision_id="decision-3", reason_code="MARGIN"),
        ],
    )
    monkeypatch.setattr(report_data, "storage", storage)

    summary = report_data.summarize_decision_ledger(report_data.list_decision_ledger("run-1"))

    assert summary["rejected"] == 2
    assert summary["total"] == 3


def test_report_accepted_count_matches_accepted_domain_decision_rows(monkeypatch) -> None:
    storage = _FakeReportStorage(
        run=_run(),
        events=[
            _decision_row(1, decision_state="accepted", decision_id="decision-1", trade_id="trade-1"),
            _decision_row(2, decision_state="accepted", decision_id="decision-2", trade_id="trade-2"),
            _decision_row(3, decision_state="rejected", decision_id="decision-3", reason_code="MARGIN"),
        ],
    )
    monkeypatch.setattr(report_data, "storage", storage)

    summary = report_data.summarize_decision_ledger(report_data.list_decision_ledger("run-1"))

    assert summary["accepted"] == 2
    assert summary["total"] == 3


def test_result_readiness_is_false_when_accepted_trade_lifecycle_is_incomplete(monkeypatch) -> None:
    storage = _FakeReportStorage(
        run=_run(),
        events=[
            _decision_row(1, decision_state="accepted", decision_id="decision-1", trade_id="trade-1"),
            _trade_row(2, "TRADE_CLOSED", "trade-1"),
        ],
    )
    monkeypatch.setattr(report_data, "storage", storage)

    readiness = report_data.get_result_readiness("run-1")

    assert readiness["safe_to_compare"] is False
    assert readiness["reason"] == "trade_lifecycle_incomplete"
    assert readiness["missing_trade_opened"] == ["trade-1"]
