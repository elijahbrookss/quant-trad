"""Canonical public series identity helpers."""

from __future__ import annotations

from typing import Any


def canonical_series_key(instrument_id: Any, timeframe: Any) -> str:
    normalized_instrument_id = str(instrument_id or "").strip()
    normalized_timeframe = str(timeframe or "").strip().lower()
    if not normalized_instrument_id or not normalized_timeframe:
        return ""
    return f"{normalized_instrument_id}|{normalized_timeframe}"


def normalize_series_key(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    instrument_id, separator, timeframe = text.partition("|")
    normalized_instrument_id = instrument_id.strip()
    normalized_timeframe = timeframe.strip().lower()
    if not separator or "|" in timeframe or not normalized_instrument_id or not normalized_timeframe:
        return ""
    return canonical_series_key(normalized_instrument_id, normalized_timeframe)


__all__ = ["canonical_series_key", "normalize_series_key"]
