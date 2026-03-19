from __future__ import annotations

import logging
from copy import deepcopy
from datetime import datetime, timezone
from time import perf_counter
from typing import Any, Dict, List, Mapping, Optional, Sequence, Set, Tuple

import pandas as pd

from engines.bot_runtime.core.domain import Candle
from engines.indicator_engine.runtime_engine import IndicatorExecutionEngine
from indicators.config import DataContext
from signals.contract import assert_no_execution_fields, assert_signal_contract

from .context import IndicatorServiceContext, _context
from .runtime_contract import SIGNAL_RUNTIME_PATH_ENGINE_SNAPSHOT
from .utils import ensure_color, get_indicator_entry, resolve_data_provider
from ..indicator_factory import (
    INDICATOR_MAP,
    resolve_indicator_params,
    runtime_indicator_builder_for_type,
)
from ...market import instrument_service

logger = logging.getLogger(__name__)
_SIGNAL_EXEC_CACHE: Dict[Tuple[Any, ...], Dict[str, Any]] = {}


def _iso_utc(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _build_candles(df: pd.DataFrame) -> List[Candle]:
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
        config: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        t0 = perf_counter()
        entry = self._load_entry(inst_id, start, end, interval, symbol, datasource, exchange)
        meta = dict(entry.meta)
        if not bool(meta.get("runtime_supported")):
            raise RuntimeError(f"Indicator is not runtime-supported: {inst_id}")

        resolved_symbol = self._resolve_symbol(entry, symbol)
        runtime_plan = self._ctx.factory.build_runtime_input_plan(
            meta,
            strategy_interval=interval,
            start=start,
            end=end,
        )
        plan_start = str(runtime_plan.get("start") or start)
        plan_end = str(runtime_plan.get("end") or end)
        plan_interval = str(runtime_plan.get("source_timeframe") or interval)
        enabled_event_keys = self._normalise_enabled_event_keys(dict(config or {}))

        cache_key = self._build_cache_key(
            inst_id=inst_id,
            meta=meta,
            symbol=resolved_symbol,
            datasource=datasource,
            exchange=exchange,
            plan_start=plan_start,
            plan_end=plan_end,
            plan_interval=plan_interval,
            enabled_event_keys=enabled_event_keys,
        )
        cached_payload = _SIGNAL_EXEC_CACHE.get(cache_key)
        if cached_payload is not None:
            return deepcopy(cached_payload)

        provider, data_ctx = self._prepare_provider(
            meta=meta,
            symbol=resolved_symbol,
            start=plan_start,
            end=plan_end,
            interval=plan_interval,
            datasource=datasource,
            exchange=exchange,
        )
        df = self._load_candles(provider, data_ctx, inst_id, resolved_symbol, plan_interval)
        candles = _build_candles(df)
        indicator = self._build_runtime_indicator(inst_id=inst_id, meta=meta)
        engine = IndicatorExecutionEngine([indicator])

        signals: List[Dict[str, Any]] = []
        for candle in candles:
            frame = engine.step(bar=candle, bar_time=candle.time)
            signals.extend(
                self._collect_frame_signals(
                    indicator_id=inst_id,
                    outputs=frame.outputs,
                    output_types=engine.output_types,
                    candle=candle,
                    symbol=resolved_symbol,
                    enabled_event_keys=enabled_event_keys,
                )
            )

        payload = ensure_color(dict(meta), ctx=self._ctx)
        payload["signals"] = signals
        payload["runtime_path"] = SIGNAL_RUNTIME_PATH_ENGINE_SNAPSHOT
        payload["runtime_invariants"] = {
            "source_timeframe": plan_interval,
            "bars_used": len(candles),
            "signals_count": len(signals),
        }
        _SIGNAL_EXEC_CACHE[cache_key] = deepcopy(payload)
        logger.info(
            "event=indicator_signal_execute_complete indicator_id=%s indicator_type=%s symbol=%s timeframe=%s source_timeframe=%s bars=%s signals=%s duration_total_ms=%.3f",
            inst_id,
            meta.get("type"),
            resolved_symbol,
            interval,
            plan_interval,
            len(candles),
            len(signals),
            (perf_counter() - t0) * 1000.0,
        )
        return payload

    def _build_cache_key(
        self,
        *,
        inst_id: str,
        meta: Mapping[str, Any],
        symbol: str,
        datasource: Optional[str],
        exchange: Optional[str],
        plan_start: str,
        plan_end: str,
        plan_interval: str,
        enabled_event_keys: Set[str],
    ) -> Tuple[Any, ...]:
        params = meta.get("params") if isinstance(meta, Mapping) else {}
        if isinstance(params, Mapping):
            params_items = tuple(sorted((str(key), repr(value)) for key, value in params.items()))
        else:
            params_items = tuple()
        return (
            str(inst_id),
            str(meta.get("type") if isinstance(meta, Mapping) else ""),
            str(symbol),
            str(datasource or ""),
            str(exchange or ""),
            str(plan_start),
            str(plan_end),
            str(plan_interval),
            params_items,
            tuple(sorted(enabled_event_keys)),
            SIGNAL_RUNTIME_PATH_ENGINE_SNAPSHOT,
        )

    def _load_entry(
        self,
        inst_id: str,
        start: str,
        end: str,
        interval: str,
        symbol: Optional[str],
        datasource: Optional[str] = None,
        exchange: Optional[str] = None,
    ):
        _ = start, end, interval, symbol, datasource, exchange
        return get_indicator_entry(inst_id, datasource=datasource, exchange=exchange, build_instance=False, ctx=self._ctx)

    @staticmethod
    def _resolve_symbol(entry: Any, symbol: Optional[str]) -> str:
        value = str(symbol or entry.meta.get("params", {}).get("symbol") or "").strip()
        if not value:
            raise ValueError("Indicator signal preview requires symbol.")
        return value

    def _prepare_provider(
        self,
        *,
        meta: Mapping[str, Any],
        symbol: str,
        start: str,
        end: str,
        interval: str,
        datasource: Optional[str] = None,
        exchange: Optional[str] = None,
    ):
        effective_datasource = datasource or meta.get("datasource") or meta.get("params", {}).get("datasource")
        effective_exchange = exchange or meta.get("exchange") or meta.get("params", {}).get("exchange")
        provider = resolve_data_provider(
            effective_datasource,
            exchange=effective_exchange,
            ctx=self._ctx,
        )
        instrument_id = instrument_service.require_instrument_id(
            effective_datasource,
            effective_exchange,
            symbol,
        )
        data_ctx = DataContext(
            symbol=symbol,
            start=start,
            end=end,
            interval=interval,
            instrument_id=instrument_id,
        )
        return provider, data_ctx

    def _load_candles(self, provider, data_ctx: DataContext, inst_id: str, symbol: str, interval: str):
        logger.info(
            "event=indicator_signal_prepare indicator=%s symbol=%s interval=%s start=%s end=%s",
            inst_id,
            symbol,
            interval,
            data_ctx.start,
            data_ctx.end,
        )
        df = provider.get_ohlcv(data_ctx)
        if df is None or df.empty:
            raise LookupError("No candles available for given window")
        return df

    def _build_runtime_indicator(self, *, inst_id: str, meta: Mapping[str, Any]) -> Any:
        indicator_type = str(meta.get("type") or "").strip()
        indicator_cls = INDICATOR_MAP.get(indicator_type)
        if indicator_cls is None:
            raise KeyError(f"Unknown indicator type: {indicator_type}")
        builder = runtime_indicator_builder_for_type(indicator_type)
        resolved_params = resolve_indicator_params(indicator_cls, meta.get("params"))
        return builder(
            indicator_id=inst_id,
            meta=meta,
            resolved_params=resolved_params,
            strategy_indicator_metas={inst_id: dict(meta)},
        )

    def _collect_frame_signals(
        self,
        *,
        indicator_id: str,
        outputs: Mapping[str, Any],
        output_types: Mapping[str, Any],
        candle: Candle,
        symbol: str,
        enabled_event_keys: Set[str],
    ) -> List[Dict[str, Any]]:
        collected: List[Dict[str, Any]] = []
        event_time = _iso_utc(candle.time)
        for output_ref, runtime_output in outputs.items():
            if not str(output_ref).startswith(f"{indicator_id}."):
                continue
            if output_types.get(output_ref) != "signal":
                continue
            if runtime_output is None or not getattr(runtime_output, "ready", False):
                continue
            indicator_key, _, output_name = str(output_ref).partition(".")
            value = getattr(runtime_output, "value", {})
            events = value.get("events") if isinstance(value, Mapping) else None
            if not isinstance(events, list):
                continue
            for event in events:
                if not isinstance(event, Mapping):
                    continue
                event_key = str(event.get("key") or "").strip()
                if not event_key:
                    continue
                if enabled_event_keys and event_key.lower() not in enabled_event_keys:
                    continue
                metadata = {
                    "bar_time": event_time,
                    "indicator_id": indicator_key,
                    "output_name": output_name,
                }
                for key, value in event.items():
                    if key in {"key", "direction", "confidence"}:
                        continue
                    metadata[str(key)] = value
                signal_payload: Dict[str, Any] = {
                    "type": event_key,
                    "signal_type": event_key,
                    "event_key": event_key,
                    "rule_id": event_key,
                    "indicator_id": indicator_key,
                    "output_name": output_name,
                    "symbol": symbol,
                    "time": event_time,
                    "signal_time": event_time,
                    "metadata": metadata,
                }
                if event.get("direction") is not None:
                    signal_payload["direction"] = event.get("direction")
                if event.get("confidence") is not None:
                    signal_payload["confidence"] = event.get("confidence")
                assert_signal_contract(signal_payload)
                assert_no_execution_fields(signal_payload)
                collected.append(signal_payload)
        return collected

    @staticmethod
    def _normalise_enabled_event_keys(config: Mapping[str, Any]) -> Set[str]:
        enabled = config.get("enabled_rules")
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


__all__ = ["IndicatorSignalExecutor"]
