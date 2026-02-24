"""Indicator-agnostic overlay projection helpers for indicator state snapshots."""

from __future__ import annotations

import hashlib
import math
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
    return fingerprint_overlay_entry(value)


def fingerprint_overlay_entry(value: Any) -> str:
    digest = hashlib.blake2b(digest_size=16)
    _fingerprint_update(digest, value)
    return digest.hexdigest()


def _fingerprint_update(digest: "hashlib._Hash", value: Any) -> None:
    if value is None:
        digest.update(b"n:")
        return
    if isinstance(value, bool):
        digest.update(b"b:1" if value else b"b:0")
        return
    if isinstance(value, int):
        digest.update(f"i:{value};".encode("utf-8"))
        return
    if isinstance(value, float):
        if not math.isfinite(value):
            digest.update(b"f:nonfinite;")
            return
        digest.update(f"f:{value:.17g};".encode("utf-8"))
        return
    if isinstance(value, str):
        encoded = value.encode("utf-8", errors="replace")
        digest.update(b"s:")
        digest.update(str(len(encoded)).encode("utf-8"))
        digest.update(b":")
        digest.update(encoded)
        digest.update(b";")
        return
    if isinstance(value, Mapping):
        digest.update(b"m{")
        for key in sorted(value.keys(), key=lambda item: str(item)):
            _fingerprint_update(digest, str(key))
            digest.update(b"=")
            _fingerprint_update(digest, value.get(key))
            digest.update(b",")
        digest.update(b"}")
        return
    if isinstance(value, (list, tuple)):
        digest.update(b"l[")
        for item in value:
            _fingerprint_update(digest, item)
            digest.update(b",")
        digest.update(b"]")
        return
    digest.update(b"o:")
    digest.update(str(value).encode("utf-8", errors="replace"))
    digest.update(b";")
