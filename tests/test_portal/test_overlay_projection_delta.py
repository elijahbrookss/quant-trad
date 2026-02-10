from __future__ import annotations

from datetime import datetime, timezone

from engines.bot_runtime.core.indicator_state.overlay_projection import project_overlay_delta
from engines.bot_runtime.core.indicator_state.contracts import IndicatorStateSnapshot, OverlayProjectionInput


def _snapshot(revision: int, value: float) -> IndicatorStateSnapshot:
    now = datetime(2024, 1, 1, tzinfo=timezone.utc)
    return IndicatorStateSnapshot(
        revision=revision,
        known_at=now,
        formed_at=now,
        source_timeframe="30m",
        payload={"items": [{"id": "a", "value": value}]},
    )


def _projector(projection_input: OverlayProjectionInput):
    items = list((projection_input.snapshot.payload or {}).get("items") or [])
    return {
        f"entry:{idx}:{item.get('id')}": {
            "type": "generic_overlay",
            "payload": dict(item),
        }
        for idx, item in enumerate(items)
    }


def test_overlay_projection_sequence_and_delta_behavior() -> None:
    state = {}

    first = project_overlay_delta(
        projection_input=OverlayProjectionInput(snapshot=_snapshot(1, 100.0), previous_projection_state=state),
        entry_projector=_projector,
    )
    assert first.seq == 1
    assert first.base_seq == 0
    assert first.authoritative_snapshot is True
    assert first.ops[0]["op"] == "reset"

    state = {"seq": first.seq, "revision": 1, "entries": {}}
    second = project_overlay_delta(
        projection_input=OverlayProjectionInput(snapshot=_snapshot(1, 100.0), previous_projection_state=state),
        entry_projector=_projector,
    )
    assert second.seq == first.seq
    assert second.ops == []

    state = {"seq": second.seq, "revision": 1, "entries": {"old": {"type": "generic_overlay", "payload": {}}}}
    third = project_overlay_delta(
        projection_input=OverlayProjectionInput(snapshot=_snapshot(2, 101.0), previous_projection_state=state),
        entry_projector=_projector,
    )
    assert third.seq == second.seq + 1
    assert any(op["op"] == "remove" for op in third.ops)
    assert any(op["op"] == "upsert" for op in third.ops)
