from __future__ import annotations

import portal.backend.service.bots.botlens_event_replay as replay
from portal.backend.service.bots.botlens_domain_events import (
    build_botlens_domain_events_from_lifecycle,
    serialize_botlens_domain_event,
)


def _lifecycle_payload(*, phase: str, status: str, checkpoint_at: str) -> dict:
    event = build_botlens_domain_events_from_lifecycle(
        bot_id="bot-1",
        run_id="run-1",
        lifecycle={
            "phase": phase,
            "status": status,
            "component": "runtime",
            "checkpoint_at": checkpoint_at,
        },
    )[0]
    return serialize_botlens_domain_event(event)


def test_domain_projection_replay_orders_by_run_seq_before_source_seq(monkeypatch) -> None:
    ready_payload = _lifecycle_payload(
        phase="live",
        status="ready",
        checkpoint_at="2026-02-01T00:00:00Z",
    )
    completed_payload = _lifecycle_payload(
        phase="completed",
        status="completed",
        checkpoint_at="2026-02-01T00:01:00Z",
    )
    rows = [
        {
            "id": 20,
            "seq": 7,
            "run_seq": 2,
            "event_name": "RUN_COMPLETED",
            "payload": completed_payload,
            "event_time": "2026-02-01T00:01:00Z",
            "known_at": "2026-02-01T00:01:00Z",
        },
        {
            "id": 10,
            "seq": 7,
            "run_seq": 1,
            "event_name": "RUN_READY",
            "payload": ready_payload,
            "event_time": "2026-02-01T00:00:00Z",
            "known_at": "2026-02-01T00:00:00Z",
        },
    ]
    monkeypatch.setattr(
        replay,
        "_list_bot_runtime_events",
        lambda **kwargs: rows if int(kwargs.get("after_seq") or 0) == 0 else [],
    )

    batches = replay.load_domain_projection_batches(bot_id="bot-1", run_id="run-1")

    assert [batch.seq for batch in batches] == [1, 2]
    assert [batch.events[0].event_name.value for batch in batches] == ["RUN_READY", "RUN_COMPLETED"]
