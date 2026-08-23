"""Closed, versioned classifications for continuous-stream quality evidence."""

from __future__ import annotations

from typing import Final


STREAM_QUALITY_CLASSIFICATIONS_V1: Final[frozenset[str]] = frozenset(
    {
        "archive_loss",
        "backpressure_stop",
        "book_invalid",
        "canonicalization_lag",
        "collector_restart_gap",
        "decode_error",
        "disconnect",
        "divergent_duplicate",
        "duplicate",
        "heartbeat_gap",
        "out_of_order",
        "provider_trade_conflict",
        "provider_trade_side_unknown",
        "resync_snapshot_accepted",
        "resync_started",
        "sequence_gap",
        "unknown_zero_delete",
        "update_before_snapshot",
    }
)


def normalize_stream_quality_classification(value: object) -> str:
    """Return one canonical v1 classification or fail with its rejected value."""

    normalized = str(value or "").strip().lower()
    if normalized not in STREAM_QUALITY_CLASSIFICATIONS_V1:
        raise ValueError(
            "market_stream_quality_invalid: unsupported classification="
            f"{normalized!r}"
        )
    return normalized


__all__ = [
    "STREAM_QUALITY_CLASSIFICATIONS_V1",
    "normalize_stream_quality_classification",
]
