"""Deterministic fingerprints for the exact candles consumed by a runtime series."""

from __future__ import annotations

import hashlib
import json
import math
import operator
from collections.abc import Iterable, Mapping
from datetime import datetime, timezone
from typing import Any, Dict, Optional


CANDLE_SERIES_SNAPSHOT_SCHEMA_VERSION = "candle_series_snapshot.v1"
CANDLE_DATA_SNAPSHOT_SCHEMA_VERSION = "candle_data_snapshot.v1"
_HASH_FIELDS = ("time", "open", "high", "low", "close", "atr", "volume")


def _value(entry: Any, field: str) -> Any:
    if isinstance(entry, Mapping):
        return entry.get(field)
    return getattr(entry, field, None)


def _canonical_time(value: Any) -> str:
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, (int, float)) and not isinstance(value, bool):
        numeric = float(value)
        if not math.isfinite(numeric):
            raise ValueError(f"candle_snapshot_invalid: non-finite time {value!r}")
        if abs(numeric) > 2e10:
            numeric /= 1000.0
        parsed = datetime.fromtimestamp(numeric, tz=timezone.utc)
    else:
        text = str(value or "").strip()
        if not text:
            raise ValueError("candle_snapshot_invalid: candle time is required")
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        try:
            parsed = datetime.fromisoformat(text)
        except ValueError as exc:
            raise ValueError(
                f"candle_snapshot_invalid: invalid candle time {value!r}"
            ) from exc
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return (
        parsed.astimezone(timezone.utc)
        .isoformat(timespec="microseconds")
        .replace("+00:00", "Z")
    )


def _canonical_number(
    value: Any,
    *,
    field: str,
    required: bool,
    nonnegative: bool = False,
) -> Optional[str]:
    if value is None and not required:
        return None
    if isinstance(value, bool):
        raise ValueError(
            f"candle_snapshot_invalid: field={field} boolean values are not numeric"
        )
    try:
        numeric = float(value)
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError(
            f"candle_snapshot_invalid: field={field} value={value!r} is not numeric"
        ) from exc
    if not math.isfinite(numeric):
        raise ValueError(
            f"candle_snapshot_invalid: field={field} value={value!r} is not finite"
        )
    if nonnegative and numeric < 0:
        raise ValueError(
            f"candle_snapshot_invalid: field={field} value={value!r} is negative"
        )
    if numeric == 0.0:
        numeric = 0.0
    return numeric.hex()


def _canonical_candle(entry: Any) -> Dict[str, Optional[str]]:
    raw_time = _value(entry, "time")
    if raw_time in (None, ""):
        raw_time = _value(entry, "timestamp")
    timestamp = _canonical_time(raw_time)
    prices = {
        field: _canonical_number(_value(entry, field), field=field, required=True)
        for field in ("open", "high", "low", "close")
    }
    numeric_prices = {field: float.fromhex(str(value)) for field, value in prices.items()}
    if numeric_prices["high"] < numeric_prices["low"]:
        raise ValueError(
            "candle_snapshot_invalid: "
            f"timestamp={timestamp} high={numeric_prices['high']} below low={numeric_prices['low']}"
        )
    if numeric_prices["high"] < max(numeric_prices["open"], numeric_prices["close"]):
        raise ValueError(
            "candle_snapshot_invalid: "
            f"timestamp={timestamp} high below open/close"
        )
    if numeric_prices["low"] > min(numeric_prices["open"], numeric_prices["close"]):
        raise ValueError(
            "candle_snapshot_invalid: "
            f"timestamp={timestamp} low above open/close"
        )
    return {
        "time": timestamp,
        **prices,
        "atr": _canonical_number(
            _value(entry, "atr"),
            field="atr",
            required=False,
            nonnegative=True,
        ),
        "volume": _canonical_number(
            _value(entry, "volume"),
            field="volume",
            required=False,
            nonnegative=True,
        ),
    }


def _stable_hash(payload: Mapping[str, Any]) -> str:
    encoded = json.dumps(
        dict(payload),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    )
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def build_candle_series_snapshot(
    candles: Iterable[Any],
    *,
    instrument_id: Any,
    symbol: Any,
    timeframe: Any,
    datasource: Any = None,
    exchange: Any = None,
    strategy_id: Any = None,
    replay_start_index: Any = 0,
) -> Dict[str, Any]:
    """Return exact, order-independent value identity for one runtime series."""

    instrument = str(instrument_id or "").strip()
    interval = str(timeframe or "").strip().lower()
    if not instrument:
        raise ValueError("candle_snapshot_invalid: instrument_id is required")
    if not interval:
        raise ValueError("candle_snapshot_invalid: timeframe is required")
    strategy = str(strategy_id or "").strip()
    if not strategy:
        raise ValueError("candle_snapshot_invalid: strategy_id is required")
    if isinstance(replay_start_index, bool):
        raise ValueError(
            "candle_snapshot_invalid: replay_start_index must be an integer"
        )
    try:
        replay_index = operator.index(replay_start_index)
    except TypeError as exc:
        raise ValueError(
            "candle_snapshot_invalid: replay_start_index must be an integer"
        ) from exc

    rows = [_canonical_candle(candle) for candle in candles]
    if not rows:
        raise ValueError("candle_snapshot_invalid: at least one candle is required")
    rows.sort(key=lambda row: str(row["time"]))
    duplicate_times = [
        current["time"]
        for previous, current in zip(rows, rows[1:])
        if previous["time"] == current["time"]
    ]
    if duplicate_times:
        raise ValueError(
            "candle_snapshot_invalid: "
            f"duplicate candle time {duplicate_times[0]}"
        )
    if replay_index < 0 or replay_index > len(rows):
        raise ValueError(
            "candle_snapshot_invalid: "
            f"replay_start_index={replay_index} outside candle_count={len(rows)}"
        )

    identity = {
        "strategy_id": strategy,
        "instrument_id": instrument,
        "symbol": str(symbol or "").strip().upper() or None,
        "timeframe": interval,
        "datasource": str(datasource or "").strip().lower() or None,
        "exchange": str(exchange or "").strip().lower() or None,
    }
    material = {
        "schema_version": CANDLE_SERIES_SNAPSHOT_SCHEMA_VERSION,
        "identity": identity,
        "replay_start_index": replay_index,
        "fields": list(_HASH_FIELDS),
        "candles": rows,
    }
    return {
        "schema_version": CANDLE_SERIES_SNAPSHOT_SCHEMA_VERSION,
        **identity,
        "series_key": f"{instrument}|{interval}",
        "hash_algorithm": "sha256",
        "candle_value_hash": _stable_hash(material),
        "candle_count": len(rows),
        "warmup_candle_count": replay_index,
        "replay_candle_count": len(rows) - replay_index,
        "first_ts": rows[0]["time"],
        "last_ts": rows[-1]["time"],
        "fields": list(_HASH_FIELDS),
    }


def aggregate_candle_series_snapshots(
    snapshots: Iterable[Mapping[str, Any]],
) -> Dict[str, Any]:
    """Aggregate complete per-series evidence into one run data snapshot."""

    by_identity: Dict[tuple[str, str, str], Dict[str, Any]] = {}
    for raw in snapshots:
        snapshot = dict(raw)
        if snapshot.get("schema_version") != CANDLE_SERIES_SNAPSHOT_SCHEMA_VERSION:
            raise ValueError(
                "candle_snapshot_invalid: unsupported series snapshot schema"
            )
        strategy_id = str(snapshot.get("strategy_id") or "").strip()
        instrument_id = str(snapshot.get("instrument_id") or "").strip()
        timeframe = str(snapshot.get("timeframe") or "").strip().lower()
        value_hash = str(snapshot.get("candle_value_hash") or "").strip().lower()
        if not strategy_id or not instrument_id or not timeframe:
            raise ValueError(
                "candle_snapshot_invalid: series snapshot identity is incomplete"
            )
        if len(value_hash) != 64 or any(char not in "0123456789abcdef" for char in value_hash):
            raise ValueError(
                "candle_snapshot_invalid: candle_value_hash must be a SHA-256 hex digest"
            )
        fields = tuple(snapshot.get("fields") or ())
        if fields != _HASH_FIELDS:
            raise ValueError(
                "candle_snapshot_invalid: series snapshot fields are not canonical"
            )
        counts: Dict[str, int] = {}
        for field in (
            "candle_count",
            "warmup_candle_count",
            "replay_candle_count",
        ):
            value = snapshot.get(field)
            if isinstance(value, bool):
                raise ValueError(
                    f"candle_snapshot_invalid: {field} must be an integer"
                )
            try:
                counts[field] = operator.index(value)
            except TypeError as exc:
                raise ValueError(
                    f"candle_snapshot_invalid: {field} must be an integer"
                ) from exc
        if counts["candle_count"] <= 0:
            raise ValueError(
                "candle_snapshot_invalid: candle_count must be positive"
            )
        if counts["warmup_candle_count"] < 0 or counts["replay_candle_count"] < 0:
            raise ValueError(
                "candle_snapshot_invalid: warmup/replay counts must be nonnegative"
            )
        if (
            counts["warmup_candle_count"] + counts["replay_candle_count"]
            != counts["candle_count"]
        ):
            raise ValueError(
                "candle_snapshot_invalid: warmup/replay counts do not sum to candle_count"
            )
        first_ts = _canonical_time(snapshot.get("first_ts"))
        last_ts = _canonical_time(snapshot.get("last_ts"))
        if first_ts > last_ts:
            raise ValueError(
                "candle_snapshot_invalid: first_ts is after last_ts"
            )
        normalized = {
            "strategy_id": strategy_id,
            "instrument_id": instrument_id,
            "symbol": str(snapshot.get("symbol") or "").strip().upper() or None,
            "timeframe": timeframe,
            "datasource": str(snapshot.get("datasource") or "").strip().lower() or None,
            "exchange": str(snapshot.get("exchange") or "").strip().lower() or None,
            "candle_value_hash": value_hash,
            **counts,
            "first_ts": first_ts,
            "last_ts": last_ts,
            "fields": list(_HASH_FIELDS),
        }
        key = (strategy_id, instrument_id, timeframe)
        existing = by_identity.get(key)
        if existing is not None:
            if str(existing.get("candle_value_hash")) != value_hash:
                raise ValueError(
                    "candle_snapshot_invalid: conflicting hashes for "
                    f"strategy_id={strategy_id} instrument_id={instrument_id} "
                    f"timeframe={timeframe}"
                )
            if existing != normalized:
                raise ValueError(
                    "candle_snapshot_invalid: conflicting metadata for "
                    f"strategy_id={strategy_id} instrument_id={instrument_id} "
                    f"timeframe={timeframe}"
                )
            continue
        by_identity[key] = normalized
    if not by_identity:
        raise ValueError("candle_snapshot_invalid: no series snapshots supplied")
    series = [by_identity[key] for key in sorted(by_identity)]
    material = {
        "schema_version": CANDLE_DATA_SNAPSHOT_SCHEMA_VERSION,
        "series": series,
    }
    return {
        "schema_version": CANDLE_DATA_SNAPSHOT_SCHEMA_VERSION,
        "hash_algorithm": "sha256",
        "data_snapshot_hash": _stable_hash(material),
        "series_count": len(series),
        "candle_count": sum(int(row["candle_count"]) for row in series),
        "series": series,
    }


__all__ = [
    "CANDLE_DATA_SNAPSHOT_SCHEMA_VERSION",
    "CANDLE_SERIES_SNAPSHOT_SCHEMA_VERSION",
    "aggregate_candle_series_snapshots",
    "build_candle_series_snapshot",
]
