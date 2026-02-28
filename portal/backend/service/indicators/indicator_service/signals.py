from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from time import perf_counter
from copy import deepcopy
from typing import Any, Dict, Iterable, List, Mapping, MutableMapping, Optional, Sequence, Set, Tuple

from engines.bot_runtime.core.domain import Candle
from engines.bot_runtime.core.domain import timeframe_to_seconds
from engines.indicator_engine import ensure_builtin_indicator_plugins_registered
from engines.indicator_engine.plugins import plugin_registry
from indicators.config import DataContext
from signals.base import BaseSignal
from signals.contract import assert_no_execution_fields, assert_signal_contract
from signals.runtime import emit_manifest_signals

from .context import IndicatorServiceContext, _context
from .overlay_pipeline import OverlayProjectionContext, project_indicator_overlays
from .runtime_contract import SIGNAL_RUNTIME_PATH_ENGINE_SNAPSHOT
from ...market import instrument_service
from .utils import (
    ensure_color,
    get_indicator_entry,
    normalize_datasource,
    normalize_exchange,
    resolve_data_provider,
)

logger = logging.getLogger(__name__)
_SIGNAL_EXEC_CACHE: Dict[Tuple[Any, ...], Dict[str, Any]] = {}


def _derive_bias_from_metadata(metadata: Mapping[str, Any]) -> Optional[str]:
    direction = str(metadata.get("direction") or "").strip().lower()
    breakout_direction = str(metadata.get("breakout_direction") or "").strip().lower()
    pointer_direction = str(metadata.get("pointer_direction") or "").strip().lower()
    variant = str(metadata.get("variant") or "").strip().lower()
    candidate = direction or breakout_direction or pointer_direction
    if not candidate:
        if "up" in variant or "above" in variant:
            candidate = "up"
        elif "down" in variant or "below" in variant:
            candidate = "down"
    if candidate in {"long", "up", "above", "bull", "bullish", "buy"}:
        return "bullish"
    if candidate in {"short", "down", "below", "bear", "bearish", "sell"}:
        return "bearish"
    return None


def _derive_direction_from_metadata(metadata: Mapping[str, Any]) -> Optional[str]:
    direction = str(metadata.get("direction") or "").strip().lower()
    if direction in {"long", "short"}:
        return direction
    breakout_direction = str(metadata.get("breakout_direction") or "").strip().lower()
    pointer_direction = str(metadata.get("pointer_direction") or "").strip().lower()
    bias = str(metadata.get("bias") or "").strip().lower()
    if breakout_direction in {"above", "up"} or pointer_direction == "up" or bias == "bullish":
        return "long"
    if breakout_direction in {"below", "down"} or pointer_direction == "down" or bias == "bearish":
        return "short"
    return None


@dataclass
class BreakoutCacheContext:
    cache_spec: Optional[Any]
    cache_key: Optional[Tuple[Any, ...]]
    requested_rule_ids: Optional[Set[str]]
    requested_rule_identities: Optional[List["RuleIdentity"]] = None
    using_cached_breakouts: bool = False
    drop_breakout_from_response: bool = False


@dataclass(frozen=True)
class RuleIdentity:
    raw_id: str
    family: str
    version: Optional[int]


class IndicatorSignalExecutor:
    """Execute indicator signals via runtime state-engine semantics."""

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
        t_prepare_start = perf_counter()
        entry = self._load_entry(inst_id, start, end, interval, symbol, datasource, exchange)
        t_prepare_ms = (perf_counter() - t_prepare_start) * 1000.0
        sym = self._resolve_symbol(entry, symbol)
        t_plan_start = perf_counter()
        runtime_plan = self._ctx.factory.build_runtime_input_plan(
            entry.meta,
            strategy_interval=interval,
            start=start,
            end=end,
        )
        t_plan_ms = (perf_counter() - t_plan_start) * 1000.0
        plan_start = str(runtime_plan.get("start") or start)
        plan_end = str(runtime_plan.get("end") or end)
        plan_interval = str(runtime_plan.get("source_timeframe") or interval)
        signal_start = str(start)
        signal_end = str(end)
        signal_interval = str(interval)
        requested_rule_ids = self._normalise_enabled_rules(dict(config or {}))
        requested_rule_identities = (
            [self._rule_identity_from_id(rule_id) for rule_id in sorted(requested_rule_ids)]
            if requested_rule_ids
            else None
        )
        provider, data_ctx = self._prepare_provider(
            entry.meta, sym, signal_start, signal_end, signal_interval, datasource, exchange
        )
        include_overlays = bool((config or {}).get("include_overlays", False))
        cache_key = self._build_cache_key(
            inst_id=inst_id,
            meta=entry.meta,
            symbol=sym,
            datasource=datasource,
            exchange=exchange,
            plan_start=signal_start,
            plan_end=signal_end,
            plan_interval=signal_interval,
            config=config or {},
            include_overlays=include_overlays,
        )
        cached_payload = _SIGNAL_EXEC_CACHE.get(cache_key)
        if cached_payload is not None:
            cached_runtime_path = str(cached_payload.get("runtime_path") or SIGNAL_RUNTIME_PATH_ENGINE_SNAPSHOT)
            runtime_invariants = cached_payload.get("runtime_invariants")
            if isinstance(runtime_invariants, Mapping):
                self._log_signal_response_invariant(
                    indicator_id=inst_id,
                    indicator_type=entry.meta.get("type"),
                    symbol=sym,
                    timeframe=signal_interval,
                    source_timeframe=str(runtime_invariants.get("source_timeframe") or plan_interval),
                    bars_used=int(runtime_invariants.get("bars_used") or 0),
                    profiles_count=int(runtime_invariants.get("profiles_count") or 0),
                    signals_count=int(runtime_invariants.get("signals_count") or 0),
                    runtime_path=cached_runtime_path,
                    include_overlays=include_overlays,
                    cache_hit=True,
                )
            logger.info(
                "event=indicator_signal_cache_hit indicator_id=%s indicator_type=%s symbol=%s timeframe=%s include_overlays=%s runtime_path=%s",
                inst_id,
                entry.meta.get("type"),
                sym,
                signal_interval,
                include_overlays,
                cached_runtime_path,
            )
            return deepcopy(cached_payload)
        t_fetch_start = perf_counter()
        df = self._load_candles(provider, data_ctx, inst_id, sym, signal_interval)
        t_fetch_ms = (perf_counter() - t_fetch_start) * 1000.0
        logger.info(
            "event=indicator_signal_mode indicator_id=%s indicator_type=%s mode=runtime_state signal_timeframe=%s profile_source_timeframe=%s requested_rules=%s",
            inst_id,
            entry.meta.get("type"),
            signal_interval,
            plan_interval,
            sorted(requested_rule_ids) if requested_rule_ids else None,
        )
        logger.info(
            "event=indicator_runtime_engine_prepare_start indicator_id=%s indicator_type=%s symbol=%s",
            inst_id,
            entry.meta.get("type"),
            sym,
        )
        t_artifacts_ms = 0.0
        logger.info(
            "event=indicator_runtime_engine_prepare_complete indicator_id=%s indicator_type=%s symbol=%s ready=%s duration_ms=%.3f",
            inst_id,
            entry.meta.get("type"),
            sym,
            True,
            t_artifacts_ms,
        )
        t_runtime_start = perf_counter()
        signals_all = self._run_runtime_state_signals(
            inst_id=inst_id,
            meta=entry.meta,
            df=df,
            symbol=sym,
            timeframe=signal_interval,
            runtime_scope=f"{signal_start}|{signal_end}|{signal_interval}",
        )
        t_runtime_ms = (perf_counter() - t_runtime_start) * 1000.0
        cache_ctx = BreakoutCacheContext(
            cache_spec=None,
            cache_key=None,
            requested_rule_ids=requested_rule_ids,
            requested_rule_identities=requested_rule_identities,
        )
        t_filter_start = perf_counter()
        filtered = self._filter_signals(signals_all, cache_ctx)
        t_filter_ms = (perf_counter() - t_filter_start) * 1000.0
        t_overlay_start = perf_counter()
        overlays: List[Any] = []
        if include_overlays:
            overlays = project_indicator_overlays(
                OverlayProjectionContext(
                    indicator_id=inst_id,
                    meta=entry.meta,
                    df=df,
                    symbol=sym,
                    timeframe=signal_interval,
                    signals=filtered,
                )
            )
        t_overlay_ms = (perf_counter() - t_overlay_start) * 1000.0
        payload = ensure_color(dict(entry.meta), ctx=self._ctx)
        # Convert BaseSignal objects to dicts for JSON serialization and strategy evaluation
        payload["signals"] = [sig.to_dict() if hasattr(sig, "to_dict") else sig for sig in filtered]
        payload["overlays"] = overlays
        profiles_count = self._infer_profiles_count(overlays)
        payload["runtime_path"] = SIGNAL_RUNTIME_PATH_ENGINE_SNAPSHOT
        payload["runtime_invariants"] = {
            "source_timeframe": plan_interval,
            "bars_used": len(df) if df is not None else 0,
            "profiles_count": profiles_count,
            "signals_count": len(filtered),
        }
        self._log_signal_response_invariant(
            indicator_id=inst_id,
            indicator_type=entry.meta.get("type"),
            symbol=sym,
            timeframe=signal_interval,
            source_timeframe=plan_interval,
            bars_used=len(df) if df is not None else 0,
            profiles_count=profiles_count,
            signals_count=len(filtered),
            runtime_path=SIGNAL_RUNTIME_PATH_ENGINE_SNAPSHOT,
            include_overlays=include_overlays,
            cache_hit=False,
        )
        _SIGNAL_EXEC_CACHE[cache_key] = deepcopy(payload)
        t_total_ms = (perf_counter() - t0) * 1000.0
        logger.info(
            "event=indicator_signal_execute_complete indicator_id=%s indicator_type=%s symbol=%s timeframe=%s bars=%s raw_signals=%s filtered_signals=%s overlays=%s include_overlays=%s duration_total_ms=%.3f duration_prepare_ms=%.3f duration_plan_ms=%.3f duration_fetch_ms=%.3f duration_artifacts_ms=%.3f duration_runtime_ms=%.3f duration_filter_ms=%.3f duration_overlay_ms=%.3f",
            inst_id,
            entry.meta.get("type"),
            sym,
            signal_interval,
            len(df) if df is not None else 0,
            len(signals_all),
            len(filtered),
            len(overlays) if isinstance(overlays, list) else 0,
            include_overlays,
            t_total_ms,
            t_prepare_ms,
            t_plan_ms,
            t_fetch_ms,
            t_artifacts_ms,
            t_runtime_ms,
            t_filter_ms,
            t_overlay_ms,
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
        config: Mapping[str, Any],
        include_overlays: bool,
    ) -> Tuple[Any, ...]:
        params = meta.get("params") if isinstance(meta, Mapping) else {}
        params_items: Tuple[Tuple[str, str], ...]
        if isinstance(params, Mapping):
            params_items = tuple(sorted((str(k), repr(v)) for k, v in params.items()))
        else:
            params_items = tuple()
        enabled_rules = config.get("enabled_rules") if isinstance(config, Mapping) else None
        enabled_rules_key = (
            tuple(sorted(str(r).strip().lower() for r in enabled_rules if r is not None))
            if isinstance(enabled_rules, Sequence) and not isinstance(enabled_rules, (str, bytes))
            else tuple()
        )
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
            SIGNAL_RUNTIME_PATH_ENGINE_SNAPSHOT,
            enabled_rules_key,
            bool(include_overlays),
        )

    @staticmethod
    def _infer_profiles_count(overlays: Sequence[Any]) -> int:
        if not isinstance(overlays, Sequence):
            return 0
        for overlay in overlays:
            if not isinstance(overlay, Mapping):
                continue
            payload = overlay.get("payload")
            if not isinstance(payload, Mapping):
                continue
            profiles = payload.get("profiles")
            if isinstance(profiles, Sequence) and not isinstance(profiles, (str, bytes)):
                return len(profiles)
            boxes = payload.get("boxes")
            if isinstance(boxes, Sequence) and not isinstance(boxes, (str, bytes)):
                return len(boxes)
            value_areas = payload.get("value_areas")
            if isinstance(value_areas, Sequence) and not isinstance(value_areas, (str, bytes)):
                return len(value_areas)
        return 0

    @staticmethod
    def _log_signal_response_invariant(
        *,
        indicator_id: str,
        indicator_type: Any,
        symbol: str,
        timeframe: str,
        source_timeframe: str,
        bars_used: int,
        profiles_count: int,
        signals_count: int,
        runtime_path: str,
        include_overlays: bool,
        cache_hit: bool,
    ) -> None:
        logger.info(
            "event=indicator_signal_response_invariant indicator_id=%s indicator_type=%s symbol=%s timeframe=%s source_timeframe=%s bars_used=%s profiles_count=%s signals_count=%s runtime_path=%s include_overlays=%s cache_hit=%s",
            indicator_id,
            indicator_type,
            symbol,
            timeframe,
            source_timeframe,
            bars_used,
            profiles_count,
            signals_count,
            runtime_path,
            include_overlays,
            cache_hit,
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
        return get_indicator_entry(
            inst_id,
            datasource=datasource,
            exchange=exchange,
            build_instance=False,
            ctx=self._ctx,
        )


    def _resolve_symbol(self, entry, symbol: Optional[str]) -> str:
        base_params = entry.meta.get("params", {})
        sym = symbol or base_params.get("symbol")
        if not sym:
            raise ValueError("Stored indicator has no symbol and none was provided")
        return sym

    def _prepare_provider(
        self,
        meta: Mapping[str, Any],
        symbol: str,
        start: str,
        end: str,
        interval: str,
        datasource: Optional[str],
        exchange: Optional[str],
    ):
        stored_params = meta.get("params", {})
        stored_datasource = normalize_datasource(
            meta.get("datasource") or stored_params.get("datasource"), ctx=self._ctx
        )
        stored_exchange = normalize_exchange(
            meta.get("exchange") or stored_params.get("exchange"), ctx=self._ctx
        )

        req_datasource = normalize_datasource(datasource, ctx=self._ctx)
        req_exchange = normalize_exchange(exchange, ctx=self._ctx)

        effective_datasource = req_datasource or stored_datasource
        effective_exchange = req_exchange or stored_exchange

        logger.info(
            "event=signal_executor_prepare_provider indicator_id=%s symbol=%s "
            "req_datasource=%s req_exchange=%s stored_datasource=%s stored_exchange=%s "
            "effective_datasource=%s effective_exchange=%s",
            meta.get("id"),
            symbol,
            req_datasource,
            req_exchange,
            stored_datasource,
            stored_exchange,
            effective_datasource,
            effective_exchange,
        )

        # resolve_data_provider will raise ValueError if effective_datasource is None
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

    def _run_runtime_state_signals(
        self,
        *,
        inst_id: str,
        meta: Mapping[str, Any],
        df: Any,
        symbol: str,
        timeframe: str,
        runtime_scope: str,
    ) -> List[BaseSignal]:
        indicator_type = str(meta.get("type") or "").strip().lower()
        if not indicator_type:
            raise RuntimeError(f"indicator_signal_runtime_invalid: indicator_id={inst_id} missing type")
        ensure_builtin_indicator_plugins_registered()
        try:
            plugin = plugin_registry().resolve(indicator_type)
        except Exception as exc:
            raise RuntimeError(
                f"indicator_signal_runtime_plugin_missing: indicator_id={inst_id} indicator_type={indicator_type}"
            ) from exc
        if getattr(plugin, "signal_emitter", None) is None:
            raise RuntimeError(
                f"indicator_signal_runtime_emitter_missing: indicator_id={inst_id} indicator_type={indicator_type}"
            )
        if getattr(plugin, "engine_factory", None) is None:
            raise RuntimeError(
                f"indicator_signal_runtime_engine_missing: indicator_id={inst_id} indicator_type={indicator_type}"
            )

        engine = plugin.engine_factory(meta)
        window_context = {
            "symbol": symbol,
            "timeframe": timeframe,
            "indicator_id": inst_id,
            "strategy_id": str(meta.get("strategy_id") or ""),
        }
        state = engine.initialize(window_context)

        logger.info(
            "event=indicator_signal_runtime_input_source indicator_id=%s indicator_type=%s source=engine_snapshot",
            inst_id,
            indicator_type,
        )
        emitted: List[BaseSignal] = []
        previous_candle: Optional[Candle] = None
        t_loop_start = perf_counter()
        apply_ms = 0.0
        snapshot_ms = 0.0
        emitter_ms = 0.0
        convert_ms = 0.0
        emitted_raw = 0
        bar_count = 0
        diagnostics_sum: Dict[str, float] = {}
        diagnostics_max: Dict[str, float] = {}
        runtime_state_storage: Dict[str, Any] = {}
        for timestamp, row in df.iterrows():
            bar_count += 1
            candle_time = timestamp.to_pydatetime() if hasattr(timestamp, "to_pydatetime") else timestamp
            if getattr(candle_time, "tzinfo", None) is None:
                candle_time = candle_time.replace(tzinfo=timezone.utc)
            else:
                candle_time = candle_time.astimezone(timezone.utc)
            candle = Candle(
                time=candle_time,
                open=float(row.get("open")),
                high=float(row.get("high")),
                low=float(row.get("low")),
                close=float(row.get("close")),
                volume=float(row.get("volume")) if row.get("volume") is not None else None,
            )
            t_apply = perf_counter()
            engine.apply_bar(state, candle)
            apply_ms += (perf_counter() - t_apply) * 1000.0
            t_snapshot = perf_counter()
            snapshot = engine.snapshot(state)
            snapshot_ms += (perf_counter() - t_snapshot) * 1000.0
            raw_payload = snapshot.payload
            if not isinstance(raw_payload, Mapping):
                raise RuntimeError(
                    f"indicator_signal_runtime_snapshot_invalid: indicator_id={inst_id} indicator_type={indicator_type} reason=payload_non_mapping"
                )
            payload: MutableMapping[str, Any] = dict(raw_payload)
            payload.setdefault("_indicator_id", inst_id)
            scope_value = str(payload.get("_runtime_scope") or "").strip()
            if not scope_value:
                raise RuntimeError(
                    "indicator_signal_runtime_scope_missing: "
                    f"indicator_id={inst_id} indicator_type={indicator_type} "
                    f"symbol={symbol} timeframe={timeframe}"
                )
            payload.setdefault("symbol", symbol)
            payload.setdefault("chart_timeframe", timeframe)
            payload.setdefault("source_timeframe", str(snapshot.source_timeframe or ""))
            payload["_runtime_state_storage"] = runtime_state_storage
            t_emit = perf_counter()
            result = emit_manifest_signals(
                manifest=plugin,
                snapshot_payload=payload,
                candle=candle,
                previous_candle=previous_candle,
            )
            emitter_ms += (perf_counter() - t_emit) * 1000.0
            if isinstance(result, Mapping):
                raw_diagnostics = result.get("diagnostics")
                if isinstance(raw_diagnostics, Mapping):
                    for key, value in raw_diagnostics.items():
                        metric_name = str(key).strip()
                        if not metric_name:
                            continue
                        numeric: Optional[float] = None
                        if isinstance(value, bool):
                            numeric = 1.0 if value else 0.0
                        elif isinstance(value, (int, float)):
                            numeric = float(value)
                        if numeric is None:
                            continue
                        diagnostics_sum[metric_name] = diagnostics_sum.get(metric_name, 0.0) + numeric
                        current_max = diagnostics_max.get(metric_name)
                        if current_max is None or numeric > current_max:
                            diagnostics_max[metric_name] = numeric
            signals = result.get("signals") if isinstance(result, Mapping) else []
            if isinstance(signals, Sequence):
                emitted_raw += len(signals)
                for signal in signals:
                    if not isinstance(signal, Mapping):
                        continue
                    t_convert = perf_counter()
                    converted = self._signal_from_runtime_payload(
                        signal,
                        default_symbol=symbol,
                        indicator_id=inst_id,
                        timeframe_seconds=timeframe_to_seconds(timeframe) or 0,
                        runtime_scope=scope_value,
                    )
                    convert_ms += (perf_counter() - t_convert) * 1000.0
                    if converted is not None:
                        emitted.append(converted)
            previous_candle = candle
            if bar_count % 1000 == 0:
                logger.debug(
                    "event=indicator_signal_runtime_progress indicator_id=%s indicator_type=%s bars_processed=%s emitted_raw=%s emitted_converted=%s apply_ms=%.3f snapshot_ms=%.3f emitter_ms=%.3f convert_ms=%.3f diagnostics_sum=%s diagnostics_max=%s",
                    inst_id,
                    indicator_type,
                    bar_count,
                    emitted_raw,
                    len(emitted),
                    apply_ms,
                    snapshot_ms,
                    emitter_ms,
                    convert_ms,
                    diagnostics_sum,
                    diagnostics_max,
                )
        loop_ms = (perf_counter() - t_loop_start) * 1000.0
        logger.info(
            "event=indicator_signal_runtime_complete indicator_id=%s indicator_type=%s signals=%s bars=%s emitted_raw=%s duration_loop_ms=%.3f duration_apply_ms=%.3f duration_snapshot_ms=%.3f duration_emitter_ms=%.3f duration_convert_ms=%.3f",
            inst_id,
            indicator_type,
            len(emitted),
            len(df),
            emitted_raw,
            loop_ms,
            apply_ms,
            snapshot_ms,
            emitter_ms,
            convert_ms,
        )
        logger.info(
            "event=indicator_signal_runtime_diagnostics indicator_id=%s indicator_type=%s diagnostics_sum=%s diagnostics_max=%s",
            inst_id,
            indicator_type,
            diagnostics_sum,
            diagnostics_max,
        )
        if emitted_raw == 0 and bar_count > 0:
            logger.info(
                "event=indicator_signal_runtime_zero_emission indicator_id=%s indicator_type=%s bars=%s candidate_count=%s candidate_chosen=%s candidate_lockout_blocked=%s candidate_already_pending=%s pending_confirm_invalid=%s pending_confirm_progress=%s retest_waiting_min_bars=%s retest_condition_rejected=%s",
                inst_id,
                indicator_type,
                bar_count,
                int(diagnostics_sum.get("candidate_count", 0.0) or 0),
                int(diagnostics_sum.get("candidate_chosen", 0.0) or 0),
                int(diagnostics_sum.get("candidate_lockout_blocked", 0.0) or 0),
                int(diagnostics_sum.get("candidate_already_pending", 0.0) or 0),
                int(diagnostics_sum.get("pending_confirm_invalid", 0.0) or 0),
                int(diagnostics_sum.get("pending_confirm_progress", 0.0) or 0),
                int(diagnostics_sum.get("retest_waiting_min_bars", 0.0) or 0),
                int(diagnostics_sum.get("retest_condition_rejected", 0.0) or 0),
            )
        return emitted

    def _signal_from_runtime_payload(
        self,
        signal: Mapping[str, Any],
        *,
        default_symbol: str,
        indicator_id: str,
        timeframe_seconds: int,
        runtime_scope: str,
    ) -> Optional[BaseSignal]:
        signal_type = str(signal.get("type") or "").strip()
        if not signal_type:
            return None
        signal_time_raw = signal.get("signal_time", signal.get("time"))
        ts = self._coerce_signal_time(signal_time_raw)
        if ts is None:
            return None
        symbol = str(signal.get("symbol") or default_symbol or "").strip()
        if not symbol:
            return None
        confidence = signal.get("confidence")
        try:
            confidence_value = float(confidence) if confidence is not None else 1.0
        except (TypeError, ValueError):
            confidence_value = 1.0
        metadata = {
            key: value
            for key, value in signal.items()
            if key not in {"type", "symbol", "time", "confidence", "metadata"}
        }
        nested_metadata = signal.get("metadata")
        if isinstance(nested_metadata, Mapping):
            for key, value in nested_metadata.items():
                metadata.setdefault(str(key), value)
        inferred_direction = _derive_direction_from_metadata(metadata)
        if inferred_direction and not metadata.get("direction"):
            metadata["direction"] = inferred_direction
        inferred_bias = _derive_bias_from_metadata(metadata)
        if inferred_bias and not metadata.get("bias"):
            metadata["bias"] = inferred_bias
        resolved_runtime_scope = str(metadata.get("runtime_scope") or runtime_scope or "").strip()
        if not resolved_runtime_scope:
            raise RuntimeError(
                "indicator_signal_contract_scope_missing: "
                f"indicator_id={indicator_id} signal_type={signal_type}"
            )
        metadata["runtime_scope"] = resolved_runtime_scope
        metadata.setdefault("signal_type", signal_type)
        metadata.setdefault("signal_time", int(ts.timestamp()))
        metadata.setdefault("indicator_id", indicator_id)
        metadata.setdefault("timeframe_seconds", int(timeframe_seconds))
        metadata.setdefault("symbol", symbol)
        metadata.setdefault("rule_id", signal_type)
        metadata.setdefault("pattern_id", metadata.get("rule_id") or signal_type)
        identity = self._rule_identity_from_id(str(metadata.get("rule_id") or signal_type))
        metadata.setdefault("rule_family", identity.family)
        metadata.setdefault("rule_version", identity.version)
        contract_payload: Dict[str, Any] = {
            "signal_type": signal_type,
            "signal_time": metadata.get("signal_time"),
            "symbol": symbol,
            "timeframe_seconds": metadata.get("timeframe_seconds"),
            "indicator_id": metadata.get("indicator_id"),
            "rule_id": metadata.get("rule_id"),
            "pattern_id": metadata.get("pattern_id"),
            "runtime_scope": metadata.get("runtime_scope"),
            "known_at": metadata.get("known_at"),
            "metadata": metadata,
        }
        assert_signal_contract(contract_payload)
        assert_no_execution_fields(contract_payload)
        return BaseSignal(
            type=signal_type,
            symbol=symbol,
            time=ts,
            confidence=confidence_value,
            metadata=metadata,
        )

    @staticmethod
    def _coerce_signal_time(value: Any) -> Optional[datetime]:
        if isinstance(value, datetime):
            return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
        if isinstance(value, (int, float)):
            return datetime.fromtimestamp(float(value), tz=timezone.utc)
        if isinstance(value, str):
            text = value.strip()
            if not text:
                return None
            if text.endswith("Z"):
                text = text[:-1] + "+00:00"
            try:
                parsed = datetime.fromisoformat(text)
            except ValueError:
                return None
            return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
        return None

    def _normalise_enabled_rules(
        self, rule_config: Dict[str, Any]
    ) -> Optional[Set[str]]:
        enabled_rules_config = rule_config.get("enabled_rules")
        if enabled_rules_config is None:
            return None
        normalised_rules: List[str] = []
        seen: Set[str] = set()
        requested: Set[str] = set()
        for rule_id in enabled_rules_config:
            if rule_id is None:
                continue
            rule_str = str(rule_id).strip()
            if not rule_str:
                continue
            norm = rule_str.lower()
            if norm not in seen:
                normalised_rules.append(norm)
                seen.add(norm)
                requested.update(self._expand_rule_identifier(norm))
        if normalised_rules:
            rule_config["enabled_rules"] = normalised_rules
            return requested
        rule_config.pop("enabled_rules", None)
        return None

    def _filter_signals(
        self, signals_all: Sequence[BaseSignal], cache_ctx: BreakoutCacheContext
    ) -> Sequence[BaseSignal]:
        filtered_signals = signals_all
        if cache_ctx.requested_rule_ids is not None:
            logger.info(
                "signal_filtering | raw_signals=%d | requested_rule_ids=%s | drop_breakout=%s",
                len(signals_all),
                sorted(cache_ctx.requested_rule_ids) if cache_ctx.requested_rule_ids else None,
                cache_ctx.drop_breakout_from_response,
            )
            filtered_signals = []
            matched_count = 0
            compatible_family_matches = 0
            for idx, sig in enumerate(signals_all):
                identifiers = self._collect_signal_identifiers(sig)
                intersection = identifiers.intersection(cache_ctx.requested_rule_ids)
                matched = bool(intersection)
                compatibility_reason: Optional[str] = None
                if (
                    not matched
                    and cache_ctx.requested_rule_identities
                    and self._signal_matches_requested_identity(sig, cache_ctx.requested_rule_identities)
                ):
                    matched = True
                    compatible_family_matches += 1
                    compatibility_reason = "family_version_compat"
                if matched:
                    filtered_signals.append(sig)
                    matched_count += 1
                # Log first 3 signals for debugging
                if idx < 3:
                    # Extract metadata for debugging
                    sig_metadata = getattr(sig, "metadata", None)
                    metadata_keys = list(sig_metadata.keys()) if isinstance(sig_metadata, dict) else None
                    metadata_rule_id = sig_metadata.get("rule_id") if isinstance(sig_metadata, dict) else None
                    metadata_aliases = sig_metadata.get("aliases") if isinstance(sig_metadata, dict) else None

                    logger.info(
                        "signal_filtering_debug | signal_idx=%d | signal_type=%s | has_metadata=%s | metadata_keys=%s | metadata_rule_id=%s | metadata_aliases=%s | collected_identifiers=%s | requested_ids=%s | matched=%s | intersection=%s",
                        idx,
                        getattr(sig, "type", None),
                        sig_metadata is not None,
                        metadata_keys,
                        metadata_rule_id,
                        metadata_aliases,
                        sorted(identifiers) if identifiers else [],
                        sorted(cache_ctx.requested_rule_ids),
                        matched,
                        sorted(intersection) if intersection else [],
                    )
                    if compatibility_reason:
                        logger.info(
                            "signal_filtering_compat_match | reason=%s | requested=%s | signal_rule_id=%s | signal_family=%s | signal_version=%s",
                            compatibility_reason,
                            sorted(cache_ctx.requested_rule_ids),
                            metadata_rule_id,
                            sig_metadata.get("rule_family") if isinstance(sig_metadata, dict) else None,
                            sig_metadata.get("rule_version") if isinstance(sig_metadata, dict) else None,
                        )
            logger.info(
                "signal_filtering_after_rules | filtered_signals=%d | matched=%d | dropped=%d | compatible_family_matches=%d",
                len(filtered_signals),
                matched_count,
                len(signals_all) - matched_count,
                compatible_family_matches,
            )
        if len(filtered_signals) != len(signals_all):
            logger.debug(
                "event=indicator_signal_filtered indicator=%s total=%d returned=%d requested_rules=%s",
                cache_ctx.cache_key,
                len(signals_all),
                len(filtered_signals),
                sorted(cache_ctx.requested_rule_ids) if cache_ctx.requested_rule_ids else None,
            )
        return filtered_signals

    def _collect_signal_identifiers(self, signal: BaseSignal) -> Set[str]:
        identifiers: Set[str] = set()

        def _append(value: Any) -> None:
            if isinstance(value, str):
                normalised = value.strip().lower()
                if normalised:
                    identifiers.add(normalised)
            elif isinstance(value, Iterable) and not isinstance(
                value, (str, bytes, Mapping)
            ):
                for item in value:
                    _append(item)

        base_fields: Dict[str, Any] = {}
        if getattr(signal, "type", None):
            base_fields["type"] = signal.type

        sources: List[Mapping[str, Any]] = [base_fields]
        metadata = getattr(signal, "metadata", None)
        if isinstance(metadata, Mapping):
            sources.append(metadata)

        keys = ("rule_id", "pattern_id", "signal_id", "pattern", "id", "type")
        alias_keys = ("aliases", "rule_aliases", "pattern_aliases", "signal_aliases")

        for source in sources:
            for key in keys:
                _append(source.get(key))
            for alias_key in alias_keys:
                _append(source.get(alias_key))

        expanded: Set[str] = set(identifiers)
        for identifier in identifiers:
            expanded.update(self._expand_rule_identifier(identifier))

        return expanded

    @staticmethod
    def _expand_rule_identifier(identifier: str) -> Set[str]:
        variants = {identifier}
        if identifier.endswith("_rule"):
            variants.add(identifier[: -len("_rule")])
        else:
            variants.add(f"{identifier}_rule")
        return variants

    @staticmethod
    def _rule_identity_from_id(rule_id: str) -> RuleIdentity:
        raw = str(rule_id or "").strip().lower()
        if not raw:
            return RuleIdentity(raw_id="", family="", version=None)
        base = raw[:-5] if raw.endswith("_rule") else raw
        match = re.match(r"^(?P<family>.+)_v(?P<version>\d+)$", base)
        if match:
            family = str(match.group("family") or "").strip().lower()
            version = int(match.group("version"))
            return RuleIdentity(raw_id=raw, family=family, version=version)
        return RuleIdentity(raw_id=raw, family=base, version=None)

    def _signal_matches_requested_identity(
        self,
        signal: BaseSignal,
        requested: Sequence[RuleIdentity],
    ) -> bool:
        metadata = signal.metadata if isinstance(signal.metadata, Mapping) else {}
        rule_id = str(metadata.get("rule_id") or "").strip().lower()
        family = str(metadata.get("rule_family") or "").strip().lower()
        version_raw = metadata.get("rule_version")
        version: Optional[int]
        try:
            version = int(version_raw) if version_raw is not None else None
        except (TypeError, ValueError):
            version = None
        if not family and rule_id:
            derived = self._rule_identity_from_id(rule_id)
            family = derived.family
            version = derived.version if version is None else version
        if not family:
            return False
        for selector in requested:
            if selector.family != family:
                continue
            if selector.version is None:
                return True
            if version is None:
                # Allow unversioned runtime emissions to satisfy explicit version
                # selectors within the same family while version rollout is in
                # progress.
                return True
            if selector.version == version:
                return True
        return False


__all__ = ["IndicatorSignalExecutor", "BreakoutCacheContext"]
