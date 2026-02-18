"""Indicator-agnostic overlay projection helpers for indicator state snapshots."""

from __future__ import annotations

import json
from typing import Any, Callable, Mapping

from .contracts import OverlayProjectionInput, ProjectionDelta

OverlayEntryProjector = Callable[[OverlayProjectionInput], Mapping[str, Mapping[str, Any]]]


def project_overlay_delta(
    *,
    projection_input: OverlayProjectionInput,
    entry_projector: OverlayEntryProjector,
) -> ProjectionDelta:
    previous = dict(projection_input.previous_projection_state or {})
    previous_seq = int(previous.get("seq") or 0)
    previous_revision = int(previous.get("revision") or -1)

    if projection_input.snapshot.revision == previous_revision:
        return ProjectionDelta(seq=previous_seq, base_seq=previous_seq, ops=[])

    next_entries = dict(entry_projector(projection_input))
    previous_entries = dict(previous.get("entries") or {})

    ops = []
    for key in previous_entries:
        if key not in next_entries:
            ops.append({"op": "remove", "key": key})
    for key, entry in next_entries.items():
        if _fingerprint(entry) != _fingerprint(previous_entries.get(key)):
            ops.append({"op": "upsert", "key": key, "overlay": entry})

    next_seq = previous_seq + 1
    authoritative_snapshot = previous_seq == 0
    if authoritative_snapshot:
        ops = [{"op": "reset", "entries": list(next_entries.values())}]

    return ProjectionDelta(
        seq=next_seq,
        base_seq=previous_seq,
        ops=ops,
        authoritative_snapshot=authoritative_snapshot,
    )


def _fingerprint(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)
