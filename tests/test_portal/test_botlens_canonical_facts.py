from __future__ import annotations

from typing import Any

import portal.backend.service.bots.botlens_canonical_facts as canonical_mod


def _fact_payload() -> dict[str, Any]:
    return {
        "series_key": "instrument-btc|1m",
        "known_at": "2026-04-19T12:00:00Z",
        "event_time": "2026-04-19T12:00:00Z",
        "facts": [
            {
                "fact_type": "runtime_state_observed",
                "runtime": {
                    "status": "running",
                    "warnings": [
                        {
                            "warning_id": "warn-1",
                            "warning_type": "runtime_warning",
                            "severity": "warning",
                            "message": "transport lag",
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
                    "time": "2026-04-19T12:00:00Z",
                    "open": 100.0,
                    "high": 101.0,
                    "low": 99.0,
                    "close": 100.5,
                },
            },
            {
                "fact_type": "overlay_ops_emitted",
                "series_key": "instrument-btc|1m",
                "overlay_delta": {
                    "overlay_commit_seq": 1,
                    "base_overlay_commit_seq": 0,
                    "overlay_commit_seq_status": "overlay_scoped",
                    "ops": [
                        {
                            "op": "upsert",
                            "key": "overlay-1",
                            "overlay": {
                                "overlay_id": "overlay-1",
                                "type": "regime_overlay",
                                "pane_key": "price",
                                "pane_views": ["polyline"],
                                "payload": {
                                    "polylines": [
                                        {
                                            "points": [
                                                {"time": 1, "price": 100.0},
                                                {"time": 2, "price": 101.0},
                                            ]
                                        }
                                    ]
                                },
                            },
                        }
                    ],
                },
            },
            {
                "fact_type": "decision_emitted",
                "decision": {
                    "event_id": "runtime-signal-1",
                    "event_name": "SIGNAL_EMITTED",
                    "event_ts": "2026-04-19T12:00:00Z",
                    "correlation_id": "corr-1",
                    "root_id": "runtime-signal-1",
                    "parent_id": None,
                    "context": {
                        "run_id": "run-1",
                        "bot_id": "bot-1",
                        "strategy_id": "strategy-1",
                        "symbol": "BTC",
                        "timeframe": "1m",
                        "bar_ts": "2026-04-19T12:00:00Z",
                        "signal_id": "signal-1",
                        "signal_type": "strategy_signal",
                        "direction": "long",
                        "signal_price": 100.5,
                        "reason_code": "SIGNAL_STRATEGY_SIGNAL",
                    },
                },
            },
            {
                "fact_type": "decision_emitted",
                "decision": {
                    "event_id": "runtime-decision-1",
                    "event_name": "DECISION_ACCEPTED",
                    "event_ts": "2026-04-19T12:00:00Z",
                    "correlation_id": "corr-1",
                    "root_id": "runtime-signal-1",
                    "parent_id": "runtime-signal-1",
                    "context": {
                        "run_id": "run-1",
                        "bot_id": "bot-1",
                        "strategy_id": "strategy-1",
                        "symbol": "BTC",
                        "timeframe": "1m",
                        "bar_ts": "2026-04-19T12:00:00Z",
                        "decision_id": "decision-1",
                        "signal_id": "signal-1",
                        "decision": "accepted",
                        "direction": "long",
                        "signal_price": 100.5,
                        "reason_code": "DECISION_ACCEPTED",
                    },
                },
            },
            {
                "fact_type": "trade_opened",
                "series_key": "instrument-btc|1m",
                "trade": {
                    "trade_id": "trade-1",
                    "status": "open",
                    "direction": "long",
                    "side": "buy",
                    "qty": 1.0,
                    "entry_price": 100.5,
                    "opened_at": "2026-04-19T12:00:00Z",
                    "position_commit_seq": 1,
                    "position_commit_seq_status": "position_scoped",
                },
            },
            {
                "fact_type": "log_emitted",
                "log": {
                    "id": "log-1",
                    "message": "debug only",
                    "level": "INFO",
                },
            },
        ],
    }


def test_append_botlens_canonical_fact_batch_persists_only_budgeted_runtime_truth(monkeypatch) -> None:
    captured: dict[str, Any] = {}

    def _record(rows, *, context=None):
        captured["rows"] = [dict(row) for row in rows]
        captured["context"] = dict(context or {})
        return len(rows)

    monkeypatch.setattr(canonical_mod, "record_bot_runtime_events_batch", _record)

    result = canonical_mod.append_botlens_canonical_fact_batch(
        bot_id="bot-1",
        run_id="run-1",
        seq=11,
        batch_kind="botlens_runtime_facts",
        payload=_fact_payload(),
        context={"worker_id": "worker-1"},
    )

    event_names = {row["payload"]["event_name"] for row in captured["rows"]}

    assert result["seq"] == 11
    assert result["event_count"] == 8
    assert result["row_count"] == 4
    assert result["inserted_rows"] == 4
    assert result["retention_summary"]["dropped_or_summarized_count"] == 4
    assert event_names == {
        "SERIES_METADATA_REPORTED",
        "SIGNAL_EMITTED",
        "DECISION_EMITTED",
        "TRADE_OPENED",
    }
    assert captured["context"]["source_reason"] == "producer"
    assert captured["context"]["pipeline_stage"] == "botlens_canonical_append"


def test_append_botlens_canonical_fact_batches_persists_multiple_payloads_in_one_write(monkeypatch) -> None:
    captured: dict[str, Any] = {"calls": []}

    def _record(rows, *, context=None):
        captured["calls"].append({"rows": [dict(row) for row in rows], "context": dict(context or {})})
        return len(rows)

    monkeypatch.setattr(canonical_mod, "record_bot_runtime_events_batch", _record)

    second_payload = _fact_payload()
    second_payload["known_at"] = "2026-04-19T13:00:00Z"
    second_payload["event_time"] = "2026-04-19T13:00:00Z"
    for fact in second_payload["facts"]:
        if fact.get("fact_type") == "candle_upserted":
            fact["candle"] = {**fact["candle"], "time": "2026-04-19T13:00:00Z", "close": 102.5}

    result = canonical_mod.append_botlens_canonical_fact_batches(
        [
            {
                "bot_id": "bot-1",
                "run_id": "run-1",
                "seq": 11,
                "batch_kind": "botlens_runtime_facts",
                "payload": _fact_payload(),
            },
            {
                "bot_id": "bot-1",
                "run_id": "run-1",
                "seq": 12,
                "batch_kind": "botlens_runtime_facts",
                "payload": second_payload,
            },
        ]
    )

    assert len(captured["calls"]) == 1
    assert result["batch_count"] == 2
    assert result["event_count"] == 16
    assert result["row_count"] == 8
    assert result["inserted_rows"] == 8
    assert result["retention_summary"]["dropped_or_summarized_count"] == 8
    assert result["seq_min"] == 11
    assert result["seq_max"] == 12
    assert {row["seq"] for row in captured["calls"][0]["rows"]} == {11, 12}
    assert captured["calls"][0]["context"]["pipeline_stage"] == "botlens_canonical_append_batch"
    assert captured["calls"][0]["context"]["batch_count"] == 2
