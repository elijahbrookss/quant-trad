"""Shared helpers for signal rule normalization."""

from __future__ import annotations

import math
from typing import Any, Mapping, Optional, Tuple

import pandas as pd


def as_timestamp(value: Any, tz: Optional[str]) -> Optional[pd.Timestamp]:
    """Convert ``value`` to a pandas timestamp, applying timezone when provided."""

    if value is None:
        return None
    try:
        ts = pd.Timestamp(value)
    except Exception:
        return None
    if tz is not None:
        if ts.tzinfo is None:
            try:
                ts = ts.tz_localize(tz)  # type: ignore[arg-type]
            except Exception:
                return None
        else:
            try:
                ts = ts.tz_convert(tz)  # type: ignore[arg-type]
            except Exception:
                return None
    return ts


def normalise_meta_timestamp(value: Any, tz: Optional[str]) -> Optional[pd.Timestamp]:
    """Convert metadata timestamps into timezone-aware pandas timestamps."""

    if value is None:
        return None

    try:
        ts = pd.Timestamp(value)
    except Exception:
        return None

    if tz is not None:
        if ts.tzinfo is None:
            try:
                ts = ts.tz_localize(tz)  # type: ignore[arg-type]
            except Exception:
                return None
        else:
            try:
                ts = ts.tz_convert(tz)  # type: ignore[arg-type]
            except Exception:
                return None
    return ts


def value_area_identifier(value_area: Mapping[str, Any]) -> Optional[str]:
    start = value_area.get("start") or value_area.get("start_date")
    if start is None:
        return None
    try:
        return pd.Timestamp(start).isoformat()
    except Exception:
        return None


def resolve_index_position(index: pd.Index, ts: Optional[pd.Timestamp]) -> Optional[int]:
    """Return the integer position of ``ts`` within ``index`` when possible."""

    if ts is None:
        return None

    try:
        positions = index.get_indexer([ts], method="nearest")
    except Exception:
        return None

    if positions.size and positions[0] >= 0:
        return int(positions[0])
    return None


def clean_numeric(value: Any, default: Optional[float] = None) -> Optional[float]:
    """Return a float if the value is finite, otherwise ``default``."""

    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return default

    if math.isnan(numeric) or math.isinf(numeric):
        return default

    return numeric


def finite_float(value: Any, default: Optional[float] = None) -> Optional[float]:
    """Alias for :func:`clean_numeric` to emphasise finite-only semantics."""

    return clean_numeric(value, default=default)


def format_duration(seconds: float) -> str:
    """Return a compact, human readable duration string."""

    if seconds >= 1:
        return f"{seconds:.2f}s"
    return f"{seconds * 1000:.1f}ms"


def to_epoch_seconds(value: Any) -> Optional[int]:
    """Best-effort conversion of timestamps into epoch seconds."""

    if value is None:
        return None

    if isinstance(value, int):
        return value

    if isinstance(value, float):
        numeric = float(value)
        return int(numeric) if math.isfinite(numeric) else None

    if isinstance(value, pd.Timestamp):
        ts = value
        try:
            ts = value.tz_convert("UTC") if value.tzinfo else value.tz_localize("UTC")
        except (TypeError, ValueError):
            ts = value.tz_localize("UTC", nonexistent="NaT", ambiguous="NaT") if value.tzinfo is None else value
        if pd.isna(ts):
            return None
        return int(ts.value // 10**9)

    try:
        candidate = pd.Timestamp(value)
    except Exception:
        return None

    if pd.isna(candidate):
        return None

    if candidate.tzinfo is None:
        candidate = candidate.tz_localize("UTC")
    else:
        candidate = candidate.tz_convert("UTC")

    return int(candidate.value // 10**9)


def bias_label_from_direction(
    direction: Optional[str], fallback: Optional[str] = None
) -> Optional[str]:
    """Translate raw direction hints into a Long/Short bias label."""

    hint = direction or fallback
    if not hint:
        return None

    text = str(hint).strip().lower()
    if text in {"above", "up", "long", "buy", "support"}:
        return "Long"
    if text in {"below", "down", "short", "sell", "resistance"}:
        return "Short"
    return None


def hex_to_rgb(color: str) -> Optional[Tuple[int, int, int]]:
    if not isinstance(color, str):
        return None

    value = color.strip().lstrip("#")
    if len(value) != 6:
        return None

    try:
        r = int(value[0:2], 16)
        g = int(value[2:4], 16)
        b = int(value[4:6], 16)
    except ValueError:
        return None

    return r, g, b


def rgba_from_hex(color: str, alpha: float) -> Optional[str]:
    rgb = hex_to_rgb(color)
    if rgb is None:
        return None

    r, g, b = rgb
    a = min(max(alpha, 0.0), 1.0)
    return f"rgba({r},{g},{b},{a:.2f})"


def readable_text_color(color: str) -> str:
    """Pick a contrasting text color for the provided background color."""

    rgb = hex_to_rgb(color)
    if rgb is None:
        return "#0f172a"

    r, g, b = rgb
    luminance = (0.299 * r + 0.587 * g + 0.114 * b) / 255
    return "#0f172a" if luminance > 0.55 else "#f8fafc"
