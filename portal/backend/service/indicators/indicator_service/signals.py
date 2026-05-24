from __future__ import annotations

import hashlib
import logging
import json
from datetime import datetime, timezone
from time import perf_counter
from typing import Any, Dict, List, Mapping, Optional, Sequence, Set

from engines.bot_runtime.core.series_identity import canonical_series_key
from engines.bot_runtime.core.domain import timeframe_to_seconds
from engines.bot_runtime.core.domain import Candle
from engines.indicator_engine.signal_output import (
    assert_signal_output_event,
    assert_signal_output_has_no_execution_fields,
    signal_output_reference,
)
from engines.indicator_engine.runtime_engine import IndicatorExecutionEngine
from indicators.config import DataContext, IndicatorExecutionContext
from overlays.schema import build_overlay

from ...market import candle_service, instrument_service
from .context import IndicatorServiceContext, _context
from .runtime_graph import build_runtime_indicator_graph
from .runtime_contract import SIGNAL_RUNTIME_PATH_ENGINE_SNAPSHOT
from .utils import ensure_color

logger = logging.getLogger(__name__)


def _iso_utc(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _humanize_signal_label(value: str) -> str:
    text = str(value or "").strip().replace("_", " ")
    if not text:
        return "Signal"
    return " ".join(token.capitalize() for token in text.split())


def _signal_bubble_direction(event_direction: Any) -> str:
    normalized = str(event_direction or "").strip().lower()
    if normalized in {"long", "buy", "bull", "bullish", "up"}:
        return "below"
    return "above"


def _finite_float(value: Any) -> Optional[float]:
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return None
    return numeric if numeric == numeric and numeric not in {float("inf"), float("-inf")} else None


def _format_price(value: Any, *, precision: Optional[int] = None) -> Optional[str]:
    numeric = _finite_float(value)
    if numeric is None:
        return None
    digits = int(precision) if precision is not None and int(precision) >= 0 else 2
    return f"{numeric:.{digits}f}"


def _signal_reference_meta(event: Mapping[str, Any]) -> Optional[str]:
    reference = signal_output_reference(event)
    if not reference:
        return None
    label = str(
        reference.get("label")
        or reference.get("name")
        or reference.get("family")
        or reference.get("kind")
        or ""
    ).strip()
    precision = reference.get("precision")
    try:
        resolved_precision = int(precision) if precision is not None else None
    except (TypeError, ValueError):
        resolved_precision = None
    price_text = _format_price(reference.get("price"), precision=resolved_precision)
    if label and price_text:
        return f"{label} {price_text}"
    if label:
        return label
    return price_text


def _signal_reference_detail(event: Mapping[str, Any]) -> Optional[str]:
    metadata = event.get("metadata")
    if not isinstance(metadata, Mapping):
        return None
    reference = signal_output_reference(event)
    precision = reference.get("precision") if reference else None
    try:
        resolved_precision = int(precision) if precision is not None else None
    except (TypeError, ValueError):
        resolved_precision = None
    trigger_price_text = _format_price(metadata.get("trigger_price"), precision=resolved_precision)
    if trigger_price_text:
        return f"Trigger {trigger_price_text}"
    return None


def _signal_reference_payload(event: Mapping[str, Any]) -> Optional[Dict[str, Any]]:
    reference = signal_output_reference(event)
    if not reference:
        return None
    return dict(reference)


def _build_signal_id(
    *,
    series_key: str,
    indicator_id: str,
    output_name: str,
    event_key: str,
    event_time: str,
    known_at: Any,
    direction: Any,
    pattern_id: Any,
    confidence: Any,
    metadata: Mapping[str, Any],
) -> str:
    identity_payload = {
        "series_key": series_key,
        "indicator_id": indicator_id,
        "output_name": output_name,
        "event_key": event_key,
        "event_time": event_time,
        "known_at": known_at,
        "direction": direction,
        "pattern_id": pattern_id,
        "confidence": confidence,
        "metadata": metadata,
    }
    encoded = json.dumps(identity_payload, sort_keys=True, default=str, separators=(",", ":")).encode("utf-8")
    digest = hashlib.blake2b(encoded, digest_size=10).hexdigest()
    return f"sig_{digest}"


def _build_candles(df: pd.DataFrame) -> List[Candle]:
    import pandas as pd

    if df is None or getattr(df, "empty", False):
        return []
    candles: List[Candle] = []
    timestamps = pd.to_datetime(df.index, utc=True)
    for timestamp, (_, row) in zip(timestamps, df.iterrows()):
        candles.append(
            Candle(
                time=timestamp.to_pydatetime(),
                open=float(row["open"]),
                high=float(row["high"]),
                low=float(row["low"]),
                close=float(row["close"]),
                volume=float(row["volume"]) if row.get("volume") is not None else None,
            )
        )
    return candles


class IndicatorSignalExecutor:
    """Generate indicator signal previews from typed runtime outputs only."""

    def __init__(self, ctx: IndicatorServiceContext = _context) -> None:
        self._ctx = ctx

    def execute(
        self,
        inst_id: str,
        start: str,
        end: str,
        interval: str,
        *,
        symbol: Optional[str] = None,
        datasource: Optional[str] = None,
        exchange: Optional[str] = None,
        instrument_id: str,
        config: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        t0 = perf_counter()
        meta = dict(self._load_meta(inst_id))
        if not bool(meta.get("runtime_supported")):
            raise RuntimeError(f"Indicator is not runtime-supported: {inst_id}")

        (
            resolved_symbol,
            resolved_datasource,
            resolved_exchange,
            resolved_instrument_id,
        ) = self._resolve_market_selection(
            meta,
            symbol=symbol,
            datasource=datasource,
            exchange=exchange,
            instrument_id=instrument_id,
        )
        execution_context = self._build_execution_context(
            symbol=resolved_symbol,
            start=start,
            end=end,
            interval=interval,
            datasource=resolved_datasource,
            exchange=resolved_exchange,
            instrument_id=resolved_instrument_id,
        )
        enabled_event_keys = self._normalise_enabled_event_keys(dict(config or {}))
        requested_output_names = self._normalise_enabled_output_names(dict(config or {}))
        _, indicators = build_runtime_indicator_graph(
            [inst_id],
            execution_context=execution_context,
            ctx=self._ctx,
            preloaded_metas={inst_id: meta},
        )

        df = self._load_candles(
            execution_context=execution_context,
            inst_id=inst_id,
            symbol=resolved_symbol,
            interval=str(execution_context.interval or ""),
        )
        candles = _build_candles(df)
        engine = IndicatorExecutionEngine(indicators)
        enabled_output_names = {
            str(output.get("name") or "")
            for output in (meta.get("typed_outputs") or [])
            if isinstance(output, Mapping)
            and output.get("type") == "signal"
            and str(output.get("name") or "").strip()
            and output.get("enabled", True) is not False
        }
        if requested_output_names is not None:
            enabled_output_names = {
                output_name
                for output_name in enabled_output_names
                if output_name in requested_output_names
            }

        signals: List[Dict[str, Any]] = []
        signal_bubbles_by_output: Dict[str, List[Dict[str, Any]]] = {}
        for candle in candles:
            frame = engine.step(bar=candle, bar_time=candle.time, include_overlays=False)
            frame_signals = self._collect_frame_signals(
                indicator_id=inst_id,
                outputs=frame.outputs,
                output_types=engine.output_types,
                candle=candle,
                symbol=resolved_symbol,
                interval=str(execution_context.interval or ""),
                datasource=execution_context.datasource,
                exchange=execution_context.exchange,
                instrument_id=execution_context.instrument_id,
                enabled_output_names=enabled_output_names,
                enabled_event_keys=enabled_event_keys,
            )
            signals.extend(frame_signals)
            self._append_signal_bubbles(
                signal_bubbles_by_output=signal_bubbles_by_output,
                frame_signals=frame_signals,
                candle=candle,
                output_name_labels={
                    str(output.get("name") or ""): str(output.get("label") or output.get("name") or "")
                    for output in (meta.get("typed_outputs") or [])
                    if isinstance(output, Mapping)
                    and output.get("type") == "signal"
                    and str(output.get("name") or "").strip()
                },
            )
        payload = ensure_color(dict(meta), ctx=self._ctx)
        signal_overlays = self._build_signal_overlays(
            indicator_id=inst_id,
            indicator_name=str(meta.get("name") or meta.get("type") or "Indicator"),
            indicator_color=str(payload.get("color") or "").strip() or None,
            signal_bubbles_by_output=signal_bubbles_by_output,
        )
        runtime_invariants = {
            "source_timeframe": str(execution_context.interval or ""),
            "bars_used": len(candles),
            "signals_count": len(signals),
            "signal_overlay_count": len(signal_overlays),
        }
        payload["runtime_path"] = SIGNAL_RUNTIME_PATH_ENGINE_SNAPSHOT
        payload["runtime_invariants"] = runtime_invariants
        payload["machine"] = {
            "signals": signals,
        }
        payload["ui"] = {
            "overlays": signal_overlays,
        }
        logger.info(
            "event=indicator_signal_execute_complete indicator_id=%s indicator_type=%s symbol=%s timeframe=%s source_timeframe=%s bars=%s signals=%s signal_overlays=%s duration_total_ms=%.3f",
            inst_id,
            meta.get("type"),
            resolved_symbol,
            interval,
            execution_context.interval,
            len(candles),
            len(signals),
            len(signal_overlays),
            (perf_counter() - t0) * 1000.0,
        )
        return payload

    def _load_meta(
        self,
        inst_id: str,
    ) -> Mapping[str, Any]:
        from .api import get_instance_meta

        return get_instance_meta(inst_id, ctx=self._ctx)

    @staticmethod
    def _resolve_market_selection(
        meta: Mapping[str, Any],
        *,
        symbol: Optional[str],
        datasource: Optional[str],
        exchange: Optional[str],
        instrument_id: str,
    ) -> Tuple[str, Optional[str], Optional[str], str]:
        resolved_symbol = str(symbol or "").strip()
        resolved_datasource = str(datasource or meta.get("datasource") or "").strip() or None
        resolved_exchange = exchange or meta.get("exchange")
        resolved_instrument_id = str(instrument_id or "").strip()

        if resolved_instrument_id:
            instrument = instrument_service.get_instrument_record(resolved_instrument_id)
            if not resolved_symbol:
                resolved_symbol = str(instrument.get("symbol") or "").strip()
            if not resolved_datasource:
                resolved_datasource = str(instrument.get("datasource") or "").strip() or None
            if resolved_exchange is None:
                instrument_exchange = str(instrument.get("exchange") or "").strip()
                resolved_exchange = instrument_exchange or None

        if not resolved_symbol:
            raise ValueError("Indicator signal preview requires symbol.")
        if not resolved_instrument_id:
            raise ValueError("Indicator signal preview requires instrument_id.")
        return resolved_symbol, resolved_datasource, resolved_exchange, resolved_instrument_id

    def _build_execution_context(
        self,
        *,
        symbol: str,
        start: str,
        end: str,
        interval: str,
        datasource: Optional[str] = None,
        exchange: Optional[str] = None,
        instrument_id: Optional[str] = None,
    ) -> IndicatorExecutionContext:
        return IndicatorExecutionContext(
            symbol=symbol,
            start=start,
            end=end,
            interval=interval,
            datasource=str(datasource or "").strip() or None,
            exchange=exchange,
            instrument_id=instrument_id,
        )

    def _load_candles(
        self,
        *,
        execution_context: IndicatorExecutionContext,
        inst_id: str,
        symbol: str,
        interval: str,
    ):
        data_ctx: DataContext = execution_context.data_context()
        logger.info(
            "event=indicator_signal_prepare indicator=%s symbol=%s interval=%s start=%s end=%s datasource=%s exchange=%s instrument_id=%s",
            inst_id,
            symbol,
            interval,
            data_ctx.start,
            data_ctx.end,
            execution_context.datasource,
            execution_context.exchange,
            execution_context.instrument_id,
        )
        df = candle_service.fetch_ohlcv_for_context(
            data_ctx,
            datasource=execution_context.datasource,
            exchange=execution_context.exchange,
        )
        if df is None or df.empty:
            raise LookupError("No candles available for given window")
        return df

    @staticmethod
    def _resolve_timeframe_seconds(interval: str) -> int:
        timeframe_seconds = timeframe_to_seconds(interval)
        if timeframe_seconds is None or int(timeframe_seconds) <= 0:
            raise ValueError(f"Indicator signal preview requires a valid interval: {interval}")
        return int(timeframe_seconds)

    @staticmethod
    def _build_series_key(*, instrument_id: str, interval: str) -> str:
        series_key = canonical_series_key(instrument_id, interval)
        if not series_key:
            raise ValueError("Indicator signal preview requires instrument_id and interval for series_key.")
        return series_key

    def _collect_frame_signals(
        self,
        *,
        indicator_id: str,
        outputs: Mapping[str, Any],
        output_types: Mapping[str, Any],
        candle: Candle,
        symbol: str,
        interval: str,
        datasource: Optional[str],
        exchange: Optional[str],
        instrument_id: Optional[str],
        enabled_output_names: Optional[Set[str]],
        enabled_event_keys: Set[str],
    ) -> List[Dict[str, Any]]:
        collected: List[Dict[str, Any]] = []
        event_time = _iso_utc(candle.time)
        timeframe_seconds = self._resolve_timeframe_seconds(interval)
        if not instrument_id:
            raise ValueError("Indicator signal preview requires instrument_id.")
        series_key = self._build_series_key(instrument_id=instrument_id, interval=interval)
        for output_ref, runtime_output in outputs.items():
            if not str(output_ref).startswith(f"{indicator_id}."):
                continue
            if output_types.get(output_ref) != "signal":
                continue
            if runtime_output is None or not getattr(runtime_output, "ready", False):
                continue
            indicator_key, _, output_name = str(output_ref).partition(".")
            if enabled_output_names is not None and output_name not in enabled_output_names:
                continue
            value = getattr(runtime_output, "value", {})
            events = value.get("events") if isinstance(value, Mapping) else None
            if not isinstance(events, list):
                continue
            for event in events:
                if not isinstance(event, Mapping):
                    continue
                assert_signal_output_event(event)
                assert_signal_output_has_no_execution_fields(event)
                event_key = str(event.get("key") or "").strip()
                if enabled_event_keys and event_key.lower() not in enabled_event_keys:
                    continue
                known_at = event.get("known_at")
                metadata = dict(event.get("metadata") or {})
                if datasource:
                    metadata["datasource"] = datasource
                if exchange:
                    metadata["exchange"] = exchange
                resolved_known_at = known_at if known_at is not None else event_time
                signal_id = _build_signal_id(
                    series_key=series_key,
                    indicator_id=indicator_key,
                    output_name=output_name,
                    event_key=event_key,
                    event_time=event_time,
                    known_at=resolved_known_at,
                    direction=event.get("direction"),
                    pattern_id=event.get("pattern_id"),
                    confidence=event.get("confidence"),
                    metadata=metadata,
                )
                event_payload: Dict[str, Any] = {
                    "signal_id": signal_id,
                    "event_key": event_key,
                    "instrument_id": instrument_id,
                    "series_key": series_key,
                    "indicator_id": indicator_key,
                    "output_name": output_name,
                    "symbol": symbol,
                    "event_time": event_time,
                    "known_at": resolved_known_at,
                    "timeframe_seconds": timeframe_seconds,
                    "metadata": metadata,
                }
                pattern_id = event.get("pattern_id")
                if pattern_id is not None:
                    event_payload["pattern_id"] = pattern_id
                if event.get("direction") is not None:
                    event_payload["direction"] = event.get("direction")
                if event.get("confidence") is not None:
                    event_payload["confidence"] = event.get("confidence")
                collected.append(event_payload)
        return collected

    @staticmethod
    def _append_signal_bubbles(
        *,
        signal_bubbles_by_output: Dict[str, List[Dict[str, Any]]],
        frame_signals: Sequence[Mapping[str, Any]],
        candle: Candle,
        output_name_labels: Mapping[str, str],
    ) -> None:
        bubble_time = int(candle.time.timestamp())
        bubble_price = float(candle.close)
        for event in frame_signals:
            output_name = str(event.get("output_name") or "").strip()
            if not output_name:
                raise RuntimeError("indicator_signal_overlay_invalid: missing output_name")
            event_key = str(event.get("event_key") or "").strip()
            if not event_key:
                raise RuntimeError("indicator_signal_overlay_invalid: missing event_key")
            bucket = signal_bubbles_by_output.setdefault(output_name, [])
            bubble: Dict[str, Any] = {
                "signal_id": event.get("signal_id"),
                "time": bubble_time,
                "price": bubble_price,
                "label": _humanize_signal_label(event_key),
                "meta": _signal_reference_meta(event) or str(output_name_labels.get(output_name) or output_name),
                "direction": _signal_bubble_direction(event.get("direction")),
                "subtype": "bubble",
            }
            detail = _signal_reference_detail(event)
            if detail:
                bubble["detail"] = detail
            known_at = event.get("known_at")
            if known_at is not None:
                bubble["known_at"] = known_at
            event_direction = event.get("direction")
            if event_direction is not None:
                bubble["bias"] = str(event_direction)
            reference = _signal_reference_payload(event)
            if reference:
                bubble["reference"] = reference
            trigger_price = _finite_float((event.get("metadata") or {}).get("trigger_price") if isinstance(event.get("metadata"), Mapping) else None)
            if trigger_price is not None:
                bubble["trigger_price"] = trigger_price
            bucket.append(bubble)

    @staticmethod
    def _build_signal_overlays(
        *,
        indicator_id: str,
        indicator_name: str,
        indicator_color: Optional[str],
        signal_bubbles_by_output: Mapping[str, Sequence[Mapping[str, Any]]],
    ) -> List[Dict[str, Any]]:
        overlays: List[Dict[str, Any]] = []
        for output_name, bubbles in sorted(signal_bubbles_by_output.items()):
            if not bubbles:
                continue
            overlay = dict(build_overlay("indicator_signal", {"bubbles": [dict(bubble) for bubble in bubbles]}))
            overlay["indicator_id"] = indicator_id
            overlay["overlay_id"] = f"{indicator_id}.{output_name}.signals"
            overlay["overlay_name"] = output_name
            overlay["source"] = "signal"
            overlay["ui"] = {
                **dict(overlay.get("ui") or {}),
                "label": f"{indicator_name} {output_name}",
                "color": indicator_color,
            }
            if indicator_color:
                overlay["color"] = indicator_color
            overlays.append(overlay)
        return overlays

    @staticmethod
    def _normalise_enabled_event_keys(config: Mapping[str, Any]) -> Set[str]:
        enabled = config.get("enabled_event_keys")
        if enabled is None:
            return set()
        if isinstance(enabled, (str, bytes)):
            candidates = [enabled]
        elif isinstance(enabled, Sequence):
            candidates = list(enabled)
        else:
            candidates = []
        return {
            str(item).strip().lower()
            for item in candidates
            if str(item).strip()
        }

    @staticmethod
    def _normalise_enabled_output_names(config: Mapping[str, Any]) -> Set[str] | None:
        enabled = config.get("enabled_signal_outputs")
        if enabled is None:
            return None
        if isinstance(enabled, (str, bytes)):
            candidates: Sequence[Any] = [enabled]
        elif isinstance(enabled, Sequence):
            candidates = list(enabled)
        else:
            candidates = []
        return {
            str(item).strip()
            for item in candidates
            if str(item).strip()
        }


__all__ = ["IndicatorSignalExecutor"]
