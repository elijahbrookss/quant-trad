from __future__ import annotations

from datetime import datetime, timezone

import pytest

from engines.bot_runtime.runtime.components.overlay_delta import build_overlay_delta
from portal.backend.service.bots import botlens_chart_service as chart_service
from portal.backend.service.bots.botlens_overlay_history import build_chart_overlay_history
from portal.backend.service.bots.botlens_retrieval_queries import DomainTruthEvent


def _iso(epoch: int) -> str:
    return datetime.fromtimestamp(epoch, tz=timezone.utc).isoformat().replace("+00:00", "Z")


def _event(
    *,
    overlay_seq: int,
    base_overlay_seq: int,
    bar_epoch: int,
    ops: list[dict],
    terminal: bool = False,
) -> DomainTruthEvent:
    run_seq = 100 + overlay_seq
    timestamp = _iso(bar_epoch)
    return DomainTruthEvent(
        row_id=overlay_seq,
        seq=run_seq,
        bot_id="bot-1",
        run_id="run-1",
        event_id=f"overlay-{overlay_seq}",
        event_name="OVERLAY_STATE_CHANGED",
        event_type="botlens_domain.overlay_state_changed",
        event_ts=timestamp,
        created_at=timestamp,
        known_at=timestamp,
        root_event_id=None,
        parent_event_id=None,
        correlation_id=None,
        series_key="instrument-btc|1m",
        context={
            "run_seq": run_seq,
            "run_seq_status": "runtime_assigned",
            "bar_time": timestamp,
            "overlay_delta": {
                "overlay_commit_seq": overlay_seq,
                "base_overlay_commit_seq": base_overlay_seq,
                "overlay_commit_seq_status": "overlay_scoped",
                "projection": {
                    "mode": "bounded",
                    "window_bars": 4,
                    "emit_every_bars": 2,
                    "bar_index": overlay_seq,
                    "reason": "bar_finalize",
                    "terminal": terminal,
                },
                "ops": ops,
            },
        },
    )


def _first_overlay_event() -> DomainTruthEvent:
    return _event(
        overlay_seq=1,
        base_overlay_seq=0,
        bar_epoch=60,
        ops=[
            {
                "op": "upsert",
                "key": "indicator.main",
                "overlay": {
                    "overlay_id": "indicator.main",
                    "type": "candle_stats_atr_short",
                    "pane_key": "price",
                    "pane_views": ["marker", "price_line"],
                    "payload": {
                        "markers": [{"time": 60, "price": 100.0}],
                        "price_lines": [
                            {
                                "price": 100.0,
                                "originTime": 60,
                                "color": "#38bdf8",
                                "lineWidth": 1,
                            }
                        ],
                    },
                },
            }
        ],
    )


def _terminal_overlay_event() -> DomainTruthEvent:
    return _event(
        overlay_seq=2,
        base_overlay_seq=1,
        bar_epoch=180,
        terminal=True,
        ops=[
            {
                "op": "patch",
                "key": "indicator.main",
                "payload_patch": {
                    "replace": {
                        "markers": [
                            {"time": 60, "price": 100.0},
                            {"time": 180, "price": 102.0},
                        ]
                    }
                },
            }
        ],
    )


def test_overlay_history_replays_clocks_and_clips_geometry_deterministically() -> None:
    events = [_first_overlay_event(), _terminal_overlay_event()]

    first = build_chart_overlay_history(
        events=events,
        symbol_key="instrument-btc|1m",
        run_status="completed",
        range_start_epoch=60,
        range_end_epoch=240,
        timeframe_seconds=60,
        has_more_after=False,
    )
    second = build_chart_overlay_history(
        events=events,
        symbol_key="instrument-btc|1m",
        run_status="completed",
        range_start_epoch=60,
        range_end_epoch=240,
        timeframe_seconds=60,
        has_more_after=False,
    )

    overlays, evidence = first
    assert first == second
    assert evidence["coverage"] == "complete"
    assert evidence["complete_for_returned_candles"] is True
    assert evidence["ordering_assured"] is True
    assert evidence["event_count"] == 2
    assert evidence["applied_event_count"] == 2
    assert evidence["last_overlay_commit_seq"] == 2
    assert evidence["terminal_checkpoint_present"] is True
    assert len(evidence["fingerprint"]) == 64
    assert len(overlays) == 1
    assert overlays[0]["source_overlay_id"] == "indicator.main"
    assert overlays[0]["overlay_id"].startswith("history:60:240:")
    assert overlays[0]["payload"]["markers"] == [
        {"time": 60, "price": 100.0},
        {"time": 180, "price": 102.0},
    ]
    assert overlays[0]["payload"]["segments"] == [
        {
            "x1": 60,
            "x2": 240,
            "y1": 100.0,
            "y2": 100.0,
            "color": "#38bdf8",
            "lineWidth": 1,
            "role": "historical_price_line",
        }
    ]


def test_overlay_history_stops_at_clock_gap_and_marks_page_incomplete() -> None:
    gap = _event(
        overlay_seq=3,
        base_overlay_seq=2,
        bar_epoch=180,
        terminal=True,
        ops=[],
    )

    _overlays, evidence = build_chart_overlay_history(
        events=[_first_overlay_event(), gap],
        symbol_key="instrument-btc|1m",
        run_status="completed",
        range_start_epoch=60,
        range_end_epoch=240,
        timeframe_seconds=60,
        has_more_after=False,
    )

    assert evidence["coverage"] == "bounded"
    assert evidence["complete_for_returned_candles"] is False
    assert evidence["ordering_assured"] is False
    assert evidence["applied_event_count"] == 1
    assert "overlay_timeline_gap_or_order_violation" in evidence["reason_codes"]


def test_overlay_history_uses_overlay_clock_when_persistence_arrival_is_reordered() -> None:
    events = [_terminal_overlay_event(), _first_overlay_event()]

    overlays, evidence = build_chart_overlay_history(
        events=events,
        symbol_key="instrument-btc|1m",
        run_status="completed",
        range_start_epoch=60,
        range_end_epoch=240,
        timeframe_seconds=60,
        has_more_after=False,
    )

    assert evidence["complete_for_returned_candles"] is True
    assert evidence["ordering_assured"] is True
    assert evidence["applied_event_count"] == 2
    assert len(overlays) == 1


def test_overlay_history_requires_terminal_checkpoint_for_latest_terminal_page() -> None:
    event = _event(
        overlay_seq=1,
        base_overlay_seq=0,
        bar_epoch=180,
        terminal=False,
        ops=[],
    )

    _overlays, evidence = build_chart_overlay_history(
        events=[event],
        symbol_key="instrument-btc|1m",
        run_status="completed",
        range_start_epoch=60,
        range_end_epoch=240,
        timeframe_seconds=60,
        has_more_after=False,
    )

    assert evidence["complete_for_returned_candles"] is False
    assert evidence["terminal_checkpoint_required"] is True
    assert evidence["terminal_checkpoint_present"] is False
    assert "terminal_overlay_checkpoint_missing" in evidence["reason_codes"]


def test_overlay_history_never_claims_complete_geometry_after_payload_truncation() -> None:
    event = _event(
        overlay_seq=1,
        base_overlay_seq=0,
        bar_epoch=180,
        terminal=True,
        ops=[
            {
                "op": "upsert",
                "key": "indicator.main",
                "overlay": {
                    "overlay_id": "indicator.main",
                    "type": "strategy_signal",
                    "payload": {"markers": [{"time": 180, "price": 102.0}]},
                    "payload_summary": {
                        "payload_counts": {"markers": 1},
                        "source_payload_counts": {"markers": 641},
                        "truncated": True,
                    },
                },
            }
        ],
    )

    _overlays, evidence = build_chart_overlay_history(
        events=[event],
        symbol_key="instrument-btc|1m",
        run_status="completed",
        range_start_epoch=60,
        range_end_epoch=240,
        timeframe_seconds=60,
        has_more_after=False,
    )

    assert evidence["coverage"] == "bounded"
    assert evidence["complete_for_returned_candles"] is False
    assert evidence["payload_truncated"] is True
    assert "overlay_payload_truncated" in evidence["reason_codes"]


def test_overlay_history_does_not_invent_evidence_for_old_runs() -> None:
    overlays, evidence = build_chart_overlay_history(
        events=[],
        symbol_key="instrument-btc|1m",
        run_status="completed",
        range_start_epoch=60,
        range_end_epoch=240,
        timeframe_seconds=60,
        has_more_after=False,
    )

    assert overlays == []
    assert evidence["coverage"] == "unavailable"
    assert evidence["reason_codes"] == ["overlay_timeline_not_retained"]


def test_forced_overlay_delta_emits_noop_terminal_checkpoint() -> None:
    cache: dict[str, object] = {}
    overlay = {
        "overlay_id": "indicator.main",
        "type": "strategy_signal",
        "payload": {"markers": [{"time": 1, "price": 100.0}]},
    }

    first = build_overlay_delta(cache, [overlay])
    terminal = build_overlay_delta(cache, [overlay], force=True)

    assert first is not None
    assert terminal == {
        "overlay_commit_seq": 2,
        "base_overlay_commit_seq": 1,
        "overlay_commit_seq_status": "overlay_scoped",
        "ops": [],
    }


def test_terminal_overlay_timeline_is_loaded_once_and_sliced_without_lookahead(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events = (
        _first_overlay_event(),
        _terminal_overlay_event(),
    )
    query_count = 0

    def _iter(**_kwargs):
        nonlocal query_count
        query_count += 1
        return iter(events)

    chart_service.clear_terminal_overlay_timeline_cache()
    monkeypatch.setattr(chart_service, "iter_all_run_domain_truth", _iter)
    first = chart_service._terminal_overlay_timeline(
        bot_id="bot-1",
        run_id="run-1",
        symbol_key="instrument-btc|1m",
    )
    second = chart_service._terminal_overlay_timeline(
        bot_id="bot-1",
        run_id="run-1",
        symbol_key="instrument-btc|1m",
    )

    assert first == second == events
    assert query_count == 1
    assert chart_service._overlay_events_before(
        second,
        range_end=datetime.fromtimestamp(120, tz=timezone.utc),
    ) == [events[0]]
    chart_service.clear_terminal_overlay_timeline_cache()
