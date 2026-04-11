from __future__ import annotations

import json
import logging
import time
from dataclasses import dataclass
from typing import Any, Dict, Iterable, Mapping, Tuple

from utils.log_context import with_log_context

from .botlens_contract import (
    SCHEMA_VERSION,
    STREAM_SYMBOL_CANDLE_DELTA_TYPE,
    STREAM_SYMBOL_DECISION_DELTA_TYPE,
    STREAM_SYMBOL_DELTA_TYPES,
    STREAM_SYMBOL_LOG_DELTA_TYPE,
    STREAM_SYMBOL_OVERLAY_DELTA_TYPE,
    STREAM_SYMBOL_RUNTIME_DELTA_TYPE,
    STREAM_SYMBOL_TRADE_DELTA_TYPE,
    normalize_series_key,
)


def _mapping(value: Any) -> Dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _json_size_bytes(value: Any) -> int:
    return len(json.dumps(value, separators=(",", ":"), default=str).encode("utf-8"))


@dataclass(frozen=True)
class TypedDeltaEvent:
    delta_type: str
    run_id: str
    symbol_key: str
    seq: int
    event_time: Any
    payload: Dict[str, Any]

    def to_message(self, *, stream_session_id: str) -> Dict[str, Any]:
        return {
            "type": self.delta_type,
            "schema_version": SCHEMA_VERSION,
            "run_id": self.run_id,
            "symbol_key": self.symbol_key,
            "seq": self.seq,
            "event_time": self.event_time,
            "stream_session_id": str(stream_session_id),
            "payload": dict(self.payload),
        }


@dataclass(frozen=True)
class PreparedTypedDelta:
    event: TypedDeltaEvent
    payload_bytes: int
    build_ms: float


@dataclass(frozen=True)
class TypedDeltaDeliveryStats:
    emit_ms: float
    viewer_count: int
    filtered_viewer_count: int
    stale_viewer_count: int


class SymbolTypedDeltaBuilder:
    @staticmethod
    def _build_event(
        *,
        delta_type: str,
        run_id: str,
        symbol_key: str,
        seq: int,
        event_time: Any,
        payload: Mapping[str, Any],
    ) -> PreparedTypedDelta:
        started = time.perf_counter()
        event = TypedDeltaEvent(
            delta_type=str(delta_type),
            run_id=str(run_id),
            symbol_key=normalize_series_key(symbol_key),
            seq=int(seq),
            event_time=event_time,
            payload=dict(payload or {}),
        )
        payload_bytes = _json_size_bytes(event.payload)
        build_ms = max((time.perf_counter() - started) * 1000.0, 0.0)
        return PreparedTypedDelta(event=event, payload_bytes=payload_bytes, build_ms=build_ms)

    @classmethod
    def build(
        cls,
        *,
        run_id: str,
        symbol_key: str,
        seq: int,
        event_time: Any,
        delta: Mapping[str, Any],
    ) -> Tuple[PreparedTypedDelta, ...]:
        prepared: list[PreparedTypedDelta] = []
        if isinstance(delta.get("candle"), Mapping):
            prepared.append(
                cls._build_event(
                    delta_type=STREAM_SYMBOL_CANDLE_DELTA_TYPE,
                    run_id=run_id,
                    symbol_key=symbol_key,
                    seq=seq,
                    event_time=event_time,
                    payload={"candle": _mapping(delta.get("candle"))},
                )
            )
        if isinstance(delta.get("overlay_delta"), Mapping):
            prepared.append(
                cls._build_event(
                    delta_type=STREAM_SYMBOL_OVERLAY_DELTA_TYPE,
                    run_id=run_id,
                    symbol_key=symbol_key,
                    seq=seq,
                    event_time=event_time,
                    payload={"overlay_delta": _mapping(delta.get("overlay_delta"))},
                )
            )
        trade_upserts = [dict(entry) for entry in (delta.get("trade_upserts") or []) if isinstance(entry, Mapping)]
        trade_removals = [str(entry) for entry in (delta.get("trade_removals") or []) if str(entry).strip()]
        if trade_upserts or trade_removals:
            prepared.append(
                cls._build_event(
                    delta_type=STREAM_SYMBOL_TRADE_DELTA_TYPE,
                    run_id=run_id,
                    symbol_key=symbol_key,
                    seq=seq,
                    event_time=event_time,
                    payload={"upserts": trade_upserts, "removals": trade_removals},
                )
            )
        log_append = [dict(entry) for entry in (delta.get("log_append") or []) if isinstance(entry, Mapping)]
        if log_append:
            prepared.append(
                cls._build_event(
                    delta_type=STREAM_SYMBOL_LOG_DELTA_TYPE,
                    run_id=run_id,
                    symbol_key=symbol_key,
                    seq=seq,
                    event_time=event_time,
                    payload={"append": log_append},
                )
            )
        decision_append = [dict(entry) for entry in (delta.get("decision_append") or []) if isinstance(entry, Mapping)]
        if decision_append:
            prepared.append(
                cls._build_event(
                    delta_type=STREAM_SYMBOL_DECISION_DELTA_TYPE,
                    run_id=run_id,
                    symbol_key=symbol_key,
                    seq=seq,
                    event_time=event_time,
                    payload={"append": decision_append},
                )
            )
        if isinstance(delta.get("runtime"), Mapping):
            prepared.append(
                cls._build_event(
                    delta_type=STREAM_SYMBOL_RUNTIME_DELTA_TYPE,
                    run_id=run_id,
                    symbol_key=symbol_key,
                    seq=seq,
                    event_time=event_time,
                    payload={"runtime": _mapping(delta.get("runtime"))},
                )
            )
        return tuple(prepared)


class TypedDeltaInstrumentation:
    @staticmethod
    def emission_summary(
        prepared_deltas: Iterable[PreparedTypedDelta],
        deliveries: Iterable[TypedDeltaDeliveryStats] | None = None,
    ) -> Dict[str, Any]:
        delivery_entries = list(deliveries or [])
        summary_events = []
        counts_by_type: Dict[str, int] = {}
        total_payload_bytes = 0
        total_build_ms = 0.0
        total_emit_ms = 0.0
        total_filtered = 0
        total_stale = 0
        max_viewers = 0
        for index, prepared in enumerate(prepared_deltas):
            delivery = delivery_entries[index] if index < len(delivery_entries) else None
            event = prepared.event
            counts_by_type[event.delta_type] = counts_by_type.get(event.delta_type, 0) + 1
            total_payload_bytes += int(prepared.payload_bytes)
            total_build_ms += float(prepared.build_ms)
            emit_ms = float(delivery.emit_ms) if delivery is not None else 0.0
            viewer_count = int(delivery.viewer_count) if delivery is not None else 0
            filtered_count = int(delivery.filtered_viewer_count) if delivery is not None else 0
            stale_count = int(delivery.stale_viewer_count) if delivery is not None else 0
            total_emit_ms += emit_ms
            total_filtered += filtered_count
            total_stale += stale_count
            max_viewers = max(max_viewers, viewer_count)
            summary_events.append(
                {
                    "type": event.delta_type,
                    "symbol_key": event.symbol_key,
                    "seq": int(event.seq),
                    "payload_bytes": int(prepared.payload_bytes),
                    "build_ms": round(float(prepared.build_ms), 6),
                    "emit_ms": round(emit_ms, 6),
                    "viewer_count": viewer_count,
                    "filtered_viewer_count": filtered_count,
                    "stale_viewer_count": stale_count,
                }
            )
        return {
            "event_count": len(summary_events),
            "counts_by_type": counts_by_type,
            "events": summary_events,
            "total_payload_bytes": total_payload_bytes,
            "total_build_ms": round(total_build_ms, 6),
            "total_emit_ms": round(total_emit_ms, 6),
            "filtered_viewer_count": total_filtered,
            "stale_viewer_count": total_stale,
            "max_viewer_count": max_viewers,
        }

    @staticmethod
    def log_emission(
        *,
        logger: logging.Logger,
        prepared_delta: PreparedTypedDelta,
        delivery: TypedDeltaDeliveryStats,
    ) -> None:
        event = prepared_delta.event
        logger.info(
            with_log_context(
                "botlens_typed_delta_emitted",
                {
                    "run_id": event.run_id,
                    "symbol_key": event.symbol_key,
                    "delta_type": event.delta_type,
                    "seq": int(event.seq),
                    "payload_bytes": int(prepared_delta.payload_bytes),
                    "build_ms": round(float(prepared_delta.build_ms), 6),
                    "emit_ms": round(float(delivery.emit_ms), 6),
                    "viewer_count": int(delivery.viewer_count),
                    "filtered_viewer_count": int(delivery.filtered_viewer_count),
                    "stale_viewer_count": int(delivery.stale_viewer_count),
                },
            )
        )

__all__ = [
    "PreparedTypedDelta",
    "STREAM_SYMBOL_DELTA_TYPES",
    "SymbolTypedDeltaBuilder",
    "TypedDeltaDeliveryStats",
    "TypedDeltaEvent",
    "TypedDeltaInstrumentation",
]
