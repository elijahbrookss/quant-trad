from __future__ import annotations

from datetime import datetime, timedelta

import pytest

from core.candle_snapshot import aggregate_candle_series_snapshots
from engines.bot_runtime.core.runtime_events import RuntimeEventName
from tests.helpers.builders.persisted_runtime_harness import (
    EVALUATION_START,
    STARTING_CASH,
    run_persisted_reference,
    semantic_fingerprint,
    semantic_trace,
    semantic_trace_through,
)


def _as_datetime(value: object) -> datetime:
    if isinstance(value, datetime):
        return value
    return datetime.fromisoformat(str(value).replace("Z", "+00:00"))


def test_persisted_runtime_reference_is_repeatable_causal_and_adapter_consistent(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    first = run_persisted_reference(
        monkeypatch=monkeypatch,
        root=tmp_path / "first",
        include_future_suffix=True,
    )
    repeated = run_persisted_reference(
        monkeypatch=monkeypatch,
        root=tmp_path / "repeated",
        include_future_suffix=True,
    )
    paper_replay = run_persisted_reference(
        monkeypatch=monkeypatch,
        root=tmp_path / "paper",
        include_future_suffix=True,
        adapter_kind="paper",
    )
    prefix = run_persisted_reference(
        monkeypatch=monkeypatch,
        root=tmp_path / "prefix",
        include_future_suffix=False,
    )

    assert semantic_trace(repeated) == semantic_trace(first)
    assert semantic_fingerprint(repeated) == semantic_fingerprint(first)
    assert semantic_trace(paper_replay) == semantic_trace(first)
    assert semantic_trace_through(
        first,
        known_at=EVALUATION_START + timedelta(minutes=3),
    ) == semantic_trace_through(
        prefix,
        known_at=EVALUATION_START + timedelta(minutes=3),
    )

    assert first.manifest["status"] == "completed"
    manifest_kinds = {entry["kind"] for entry in first.manifest["files"]}
    assert {
        "candles",
        "decision_trace",
        "indicator_outputs",
        "runtime_artifact",
        "runtime_events",
        "summary",
    }.issubset(manifest_kinds)

    series = first.series_snapshot["series"][0]
    assert series["backtest_warmup"]["status"] == "ready"
    assert series["backtest_warmup"]["loaded_bars"] == 100
    candle_snapshot = series["candle_snapshot"]
    assert len(candle_snapshot["candle_value_hash"]) == 64
    assert candle_snapshot["candle_count"] == 107
    assert candle_snapshot["warmup_candle_count"] == 100
    assert candle_snapshot["replay_candle_count"] == 7
    data_snapshot = aggregate_candle_series_snapshots([candle_snapshot])
    assert len(data_snapshot["data_snapshot_hash"]) == 64
    assert series["candle_continuity"]["detected_gap_count"] == 0
    assert series["candle_gap_classification"] is None

    runtime_events = first.artifact["runtime_event_stream"]
    event_names = [event["event_name"] for event in runtime_events]
    assert RuntimeEventName.WALLET_INITIALIZED.value in event_names
    assert RuntimeEventName.DECISION_ACCEPTED.value in event_names
    assert RuntimeEventName.ORDER_LIFECYCLE_CHANGED.value in event_names
    assert RuntimeEventName.ENTRY_FILLED.value in event_names
    assert RuntimeEventName.EXIT_FILLED.value in event_names

    order_events = [
        event
        for event in runtime_events
        if event["event_name"] == RuntimeEventName.ORDER_LIFECYCLE_CHANGED.value
    ]
    lifecycle_event_by_id = {event["event_id"]: event for event in order_events}
    order_sequences: dict[str, list[int]] = {}
    for event in order_events:
        context = event["context"]
        order_sequences.setdefault(context["order_request_id"], []).append(
            int(context["order_event_seq"])
        )
        assert context["execution_context_hash"]
        assert context["execution_policy_hash"]
        assert context["order_request_manifest_hash"]
        assert context["order_attempt_manifest_hash"]
        assert context["order_lifecycle_replay_hash"]
    for sequences in order_sequences.values():
        assert sequences == list(range(1, len(sequences) + 1))

    traces = first.artifact["order_lifecycle_traces"]
    assert set(traces) == set(order_sequences)
    for request_id, trace in traces.items():
        assert trace["request"]["request_id"] == request_id
        assert trace["replay_hash"] == trace["snapshot"]["replay_hash"]
        assert len(trace["events"]) == len(order_sequences[request_id])

    entry_fills = [
        event
        for event in runtime_events
        if event["event_name"] == RuntimeEventName.ENTRY_FILLED.value
    ]
    exit_fills = [
        event
        for event in runtime_events
        if event["event_name"] == RuntimeEventName.EXIT_FILLED.value
    ]
    for fill in entry_fills + exit_fills:
        lifecycle_parent = lifecycle_event_by_id[fill["parent_id"]]
        assert lifecycle_parent["context"]["state"] == "filled"
        assert lifecycle_parent["root_id"] == fill["root_id"]
    assert entry_fills[0]["context"]["bar_ts"] == "2026-01-01T00:02:00Z"
    assert exit_fills[0]["context"]["bar_ts"] == "2026-01-01T00:03:00Z"
    assert exit_fills[0]["context"]["reason_code"] == "EXEC_EXIT_TARGET"
    assert entry_fills[1]["context"]["bar_ts"] == "2026-01-01T00:06:00Z"
    assert exit_fills[1]["context"]["bar_ts"] == "2026-01-01T00:06:00Z"
    assert exit_fills[1]["context"]["reason_code"] == "BACKTEST_END"

    closed = [trade for trade in first.trades if trade.get("status") == "closed"]
    assert len(closed) == 2
    target_trade, terminal_trade = closed
    assert _as_datetime(target_trade["entry_time"]) == (
        EVALUATION_START + timedelta(minutes=2)
    )
    assert _as_datetime(target_trade["exit_time"]) == (
        EVALUATION_START + timedelta(minutes=3)
    )
    assert target_trade["gross_pnl"] == pytest.approx(10.0)
    assert target_trade["fees_paid"] == pytest.approx(0.313)
    assert target_trade["net_pnl"] == pytest.approx(9.687)
    assert _as_datetime(terminal_trade["entry_time"]) == (
        EVALUATION_START + timedelta(minutes=6)
    )
    assert _as_datetime(terminal_trade["exit_time"]) == (
        EVALUATION_START + timedelta(minutes=6)
    )
    assert terminal_trade["gross_pnl"] == pytest.approx(0.0)
    assert terminal_trade["fees_paid"] == pytest.approx(4.0)
    assert terminal_trade["net_pnl"] == pytest.approx(-4.0)

    gross_pnl = sum(float(trade["gross_pnl"]) for trade in closed)
    fees = sum(float(trade["fees_paid"]) for trade in closed)
    net_pnl = sum(float(trade["net_pnl"]) for trade in closed)
    assert gross_pnl == pytest.approx(10.0)
    assert fees == pytest.approx(4.313)
    assert net_pnl == pytest.approx(gross_pnl - fees)

    summary = first.summary["summary"]
    assert summary["total_trades"] == len(closed)
    assert summary["fees"] == pytest.approx(fees)
    assert summary["net_pnl"] == pytest.approx(net_pnl)
    assert summary["equity_end"] == pytest.approx(STARTING_CASH + net_pnl)
    wallet_state = first.artifact["wallet_state"]
    assert wallet_state["balances"]["USD"] == pytest.approx(summary["equity_end"])
    assert wallet_state["free_collateral"]["USD"] == pytest.approx(
        summary["equity_end"]
    )
    assert wallet_state["locked_margin"] == {}
    assert wallet_state["margin_positions"] == {}
    assert first.run_upsert["summary"]["net_pnl"] == pytest.approx(
        summary["net_pnl"]
    )
    assert first.run_upsert["summary"]["equity_end"] == pytest.approx(
        summary["equity_end"]
    )

    step_names = {row["step_name"] for row in first.step_rollups}
    assert {
        "run_loop",
        "step_signal_eval",
        "step_decision_flow",
        "step_execution_prime",
        "settlement_apply",
        "step_series_state",
    }.issubset(step_names)
