"""SeriesBuilder mixin."""

from __future__ import annotations

import logging
from collections import deque
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from typing import Any, Deque, Dict, List, Mapping, Optional, Sequence, Tuple

from engines.bot_runtime.core.domain import (
    Candle,
    StrategySignal,
    isoformat,
    normalize_epoch,
    timeframe_duration,
)
from portal.backend.service.market.stats_contract import REGIME_VERSION
from signals.overlays.schema import build_overlay
from utils.log_context import build_log_context, with_log_context
from utils.perf_log import perf_log

from ..regime_overlay import build_regime_overlays

logger = logging.getLogger(__name__)

DEFAULT_SIM_LOOKBACK_DAYS = 7


def _regime_version_or_raise() -> str:
    version = str(REGIME_VERSION or "").strip()
    if not version:
        raise RuntimeError("REGIME_VERSION is empty; fix portal.backend.service.market.stats_contract")
    return version

class SeriesBuilderOverlaysRegimeMixin:
    def _instrument_for(
        self,
        datasource: Optional[str],
        exchange: Optional[str],
        symbol: Optional[str],
    ) -> Optional[Dict[str, Any]]:
        # Look up canonical instrument record in storage by datasource/exchange/symbol.
        if not symbol:
            return None
        from .....market import instrument_service

        try:
            record = instrument_service.resolve_instrument(datasource, exchange, symbol)
        except Exception:
            record = None
        return record

    def _resolve_live_window(self) -> Tuple[str, str]:
        lookback_days = int(self.config.get("sim_lookback_days") or DEFAULT_SIM_LOOKBACK_DAYS)
        lookback_days = max(lookback_days, 1)
        end_dt = datetime.now(timezone.utc)
        start_dt = end_dt - timedelta(days=lookback_days)
        return isoformat(start_dt), isoformat(end_dt)

    def _indicator_overlay_entries(
        self,
        strategy: Mapping[str, Any],
        start_iso: str,
        end_iso: str,
        timeframe: Optional[str],
        symbol: Optional[str],
        datasource: Optional[str],
        exchange: Optional[str],
    ) -> List[Dict[str, Any]]:
        from .....indicators import indicator_service

        if not self._include_indicator_overlays:
            return []

        overlays: List[Dict[str, Any]] = []
        strategy_meta = strategy or {}
        links = list(strategy_meta.get("indicator_links") or [])
        if not links and strategy_meta.get("indicator_ids"):
            links = [
                {"indicator_id": indicator_id}
                for indicator_id in strategy_meta.get("indicator_ids")
                if indicator_id
            ]
        seen: set[str] = set()
        for link in links:
            indicator_id = str(link.get("indicator_id") or link.get("id") or "").strip()
            if not indicator_id or indicator_id in seen:
                continue
            seen.add(indicator_id)

            # Load fresh indicator metadata from DB (no snapshots)
            try:
                if self._indicator_ctx is None:
                    indicator_meta = indicator_service.get_instance_meta(indicator_id)
                else:
                    indicator_meta = indicator_service.get_instance_meta(
                        indicator_id, ctx=self._indicator_ctx
                    )
            except KeyError:
                context = self._runtime_log_context(indicator_id=indicator_id)
                logger.warning(with_log_context("bot_overlay_indicator_not_found", context))
                continue

            params = dict(indicator_meta.get("params") or {})
            indicator_type = indicator_meta.get("type") or "indicator"
            color = indicator_meta.get("color")

            # Debug log to verify params loaded from fresh indicator metadata
            params_context = self._runtime_log_context(
                indicator_id=indicator_id,
                indicator_type=indicator_type,
                params_keys=list(params.keys()),
            )
            logger.debug(with_log_context("bot_overlay_params_loaded", params_context))

            # For multi-instrument strategies, always use the series symbol (not indicator's stored symbol)
            # Fallback to indicator params symbol only if series symbol is truly missing
            window_symbol = symbol if symbol else params.get("symbol")
            if not window_symbol:
                context = self._runtime_log_context(indicator_id=indicator_id, indicator_type=indicator_type)
                logger.warning(with_log_context("bot_overlay_missing_symbol", context))
                continue

            interval = params.get("interval") or timeframe
            if not interval:
                raise RuntimeError("Indicator overlay requires an interval; set strategy 'timeframe' or indicator params")

            # Prefer explicit link-level datasource, then params from fresh indicator metadata,
            # then strategy-level datasource/exchange
            ds = (
                link.get("datasource")
                or params.get("datasource")
                or datasource
                or indicator_meta.get("datasource")
            )
            ex = (
                link.get("exchange")
                or params.get("exchange")
                or exchange
                or indicator_meta.get("exchange")
            )
            # IMPORTANT: Don't pass overlay_options for botlens - use indicator's stored params
            # The indicator instance is loaded with the correct params from the database.
            # Passing overlay_options would override the stored params with the same values,
            # which is circular and unnecessary. Overlay options are for UI-level temporary
            # overrides, not for reading stored configuration.
            overlay_options = None
            if overlay_options:
                context = self._runtime_log_context(
                    indicator_id=indicator_id,
                    indicator_type=indicator_type,
                    options=overlay_options,
                )
                logger.info(with_log_context("bot_overlay_options", context))
            request_context = self._runtime_log_context(
                indicator_id=indicator_id,
                indicator_type=indicator_type,
                start=start_iso,
                end=end_iso,
                timeframe=interval,
                symbol=window_symbol,
                datasource=ds,
                exchange=ex,
            )
            logger.info(with_log_context("bot_overlay_request", request_context))

            # Resolve indicator runtime plan so overlay recompute cadence follows the indicator's source timeframe.
            try:
                if self._indicator_ctx is None:
                    runtime_input_plan = indicator_service.runtime_input_plan_for_instance(
                        indicator_id,
                        strategy_interval=str(interval),
                        start=start_iso,
                        end=end_iso,
                    )
                else:
                    runtime_input_plan = indicator_service.runtime_input_plan_for_instance(
                        indicator_id,
                        strategy_interval=str(interval),
                        start=start_iso,
                        end=end_iso,
                        ctx=self._indicator_ctx,
                    )
            except Exception as exc:
                logger.warning(
                    with_log_context(
                        "bot_overlay_runtime_input_plan_failed",
                        self._runtime_log_context(
                            indicator_id=indicator_id,
                            indicator_type=indicator_type,
                            start=start_iso,
                            end=end_iso,
                            timeframe=interval,
                            error=str(exc),
                        ),
                    )
                )
                runtime_input_plan = {}

            plan_start = str(runtime_input_plan.get("start") or start_iso)
            plan_end = str(runtime_input_plan.get("end") or end_iso)
            plan_interval = str(runtime_input_plan.get("source_timeframe") or interval)
            try:
                source_timeframe_seconds = int(timeframe_duration(plan_interval).total_seconds())
            except Exception:
                source_timeframe_seconds = 0
            source_bucket = None
            if source_timeframe_seconds > 0:
                try:
                    plan_end_text = str(plan_end).strip()
                    if plan_end_text.endswith("Z"):
                        plan_end_text = f"{plan_end_text[:-1]}+00:00"
                    plan_end_dt = datetime.fromisoformat(plan_end_text)
                    if plan_end_dt.tzinfo is None:
                        plan_end_dt = plan_end_dt.replace(tzinfo=timezone.utc)
                    else:
                        plan_end_dt = plan_end_dt.astimezone(timezone.utc)
                    source_bucket = int(plan_end_dt.timestamp()) // source_timeframe_seconds
                except Exception:
                    source_bucket = None

            overlay_runtime_key = ":".join(
                [
                    indicator_id,
                    str(window_symbol or "").upper(),
                    str(ds or "").lower(),
                    str(ex or "").lower(),
                    str(plan_interval),
                ]
            )
            incremental_cache_fingerprint = None
            if source_bucket is not None:
                with self._overlay_runtime_cache_lock:
                    cached = self._indicator_overlay_runtime_cache.get(overlay_runtime_key)
                if cached and cached.get("source_bucket") == source_bucket:
                    cached_overlay = cached.get("overlay")
                    if isinstance(cached_overlay, Mapping):
                        overlays.append(deepcopy(dict(cached_overlay)))
                        logger.debug(
                            with_log_context(
                                "bot_overlay_runtime_cache_hit",
                                self._runtime_log_context(
                                    indicator_id=indicator_id,
                                    indicator_type=indicator_type,
                                    source_timeframe=plan_interval,
                                    source_bucket=source_bucket,
                                ),
                            )
                        )
                        continue
                if cached and self._indicator_ctx is not None:
                    cache = getattr(self._indicator_ctx, "incremental_cache", None)
                    if cache is not None and hasattr(cache, "fingerprint_for"):
                        try:
                            incremental_cache_fingerprint = cache.fingerprint_for(indicator_id, str(window_symbol))
                        except Exception:
                            incremental_cache_fingerprint = None
                if cached and incremental_cache_fingerprint is not None:
                    if cached.get("cache_fingerprint") == incremental_cache_fingerprint:
                        cached_overlay = cached.get("overlay")
                        if isinstance(cached_overlay, Mapping):
                            overlays.append(deepcopy(dict(cached_overlay)))
                            logger.debug(
                                with_log_context(
                                    "bot_overlay_runtime_cache_hit",
                                    self._runtime_log_context(
                                        indicator_id=indicator_id,
                                        indicator_type=indicator_type,
                                        source_timeframe=plan_interval,
                                        source_bucket=source_bucket,
                                        cache_hit_reason="incremental_cache_fingerprint",
                                    ),
                                )
                            )
                            continue
            try:
                if self._indicator_ctx is None:
                    payload = indicator_service.overlays_for_instance(
                        indicator_id,
                        start=plan_start,
                        end=plan_end,
                        interval=str(plan_interval),
                        symbol=window_symbol,
                        datasource=ds,
                        exchange=ex,
                        overlay_options=overlay_options or None,
                    )
                else:
                    payload = indicator_service.overlays_for_instance(
                        indicator_id,
                        start=plan_start,
                        end=plan_end,
                        interval=str(plan_interval),
                        symbol=window_symbol,
                        datasource=ds,
                        exchange=ex,
                        overlay_options=overlay_options or None,
                        ctx=self._indicator_ctx,
                    )
                received_context = self._runtime_log_context(
                    indicator_id=indicator_id,
                    boxes=len(payload.get("boxes", [])),
                    markers=len(payload.get("markers", [])),
                    price_lines=len(payload.get("price_lines", [])),
                )
                logger.info(with_log_context("bot_overlay_received", received_context))
            except Exception as exc:  # pragma: no cover - defensive logging
                error_context = self._runtime_log_context(
                    strategy_id=strategy_meta.get("id"),
                    indicator_id=indicator_id,
                    error=str(exc),
                )
                logger.error(with_log_context("bot_indicator_overlay_failed", error_context))
                continue
            if isinstance(payload, Mapping) and "type" in payload and "payload" in payload:
                # Treat typed payloads as already-constructed overlays.
                overlay = dict(payload)
            else:
                overlay = build_overlay(indicator_type, payload)
            overlay.update(
                {
                    "ind_id": indicator_id,
                    "color": color,
                    "source": "indicator",
                    "bot_id": self.bot_id,
                    "strategy_id": strategy_meta.get("id"),
                    "symbol": window_symbol,
                }
            )
            overlays.append(overlay)
            if source_bucket is not None:
                with self._overlay_runtime_cache_lock:
                    self._indicator_overlay_runtime_cache[overlay_runtime_key] = {
                        "source_bucket": source_bucket,
                        "overlay": deepcopy(overlay),
                        "cache_fingerprint": incremental_cache_fingerprint,
                    }
            appended_context = self._runtime_log_context(
                indicator_id=indicator_id,
                total_overlays=len(overlays),
            )
            logger.info(with_log_context("bot_overlay_appended", appended_context))
        return overlays

    def _build_regime_overlays(
        self,
        *,
        instrument_id: str,
        candles: Sequence[Candle],
        timeframe: str,
        strategy_id: Optional[str],
        symbol: Optional[str],
    ) -> List[Dict[str, Any]]:
        regime_version = _regime_version_or_raise()
        if not instrument_id:
            self._emit_warning(
                "regime_overlay_missing_instrument",
                "Regime overlay skipped: instrument metadata missing",
                strategy_id=strategy_id,
                symbol=symbol,
                timeframe=timeframe,
            )
            logger.warning(
                with_log_context(
                    "bot_regime_overlay_missing_instrument",
                    self._runtime_log_context(
                        strategy_id=strategy_id,
                        symbol=symbol,
                        instrument_id=instrument_id,
                        timeframe=timeframe,
                    ),
                )
            )
            return []
        if not candles:
            self._emit_warning(
                "regime_overlay_no_candles",
                "Regime overlay skipped: no candles available for window",
                strategy_id=strategy_id,
                symbol=symbol,
                timeframe=timeframe,
            )
            logger.warning(
                with_log_context(
                    "bot_regime_overlay_no_candles",
                    self._runtime_log_context(
                        strategy_id=strategy_id,
                        symbol=symbol,
                        instrument_id=instrument_id,
                        timeframe=timeframe,
                    ),
                )
            )
            return []
        try:
            timeframe_seconds = int(timeframe_duration(timeframe).total_seconds())
        except Exception as exc:
            logger.warning(
                with_log_context(
                    "bot_regime_overlay_timeframe_invalid",
                    self._runtime_log_context(
                        strategy_id=strategy_id,
                        symbol=symbol,
                        instrument_id=instrument_id,
                        timeframe=timeframe,
                        error=str(exc),
                    ),
                )
            )
            return []
        if timeframe_seconds <= 0:
            logger.warning(
                with_log_context(
                    "bot_regime_overlay_timeframe_nonpositive",
                    self._runtime_log_context(
                        strategy_id=strategy_id,
                        symbol=symbol,
                        instrument_id=instrument_id,
                        timeframe=timeframe,
                        timeframe_seconds=timeframe_seconds,
                    ),
                )
            )
            return []
        start_dt = candles[0].time
        end_dt = candles[-1].time
        regime_rows = self._regime_rows_for_window(
            instrument_id=instrument_id,
            timeframe=timeframe,
            timeframe_seconds=timeframe_seconds,
            start_dt=start_dt,
            end_dt=end_dt,
            strategy_id=strategy_id,
            symbol=symbol,
        )
        logger.debug(
            with_log_context(
                "bot_regime_rows_collected",
                self._runtime_log_context(
                    strategy_id=strategy_id,
                    symbol=symbol,
                    instrument_id=instrument_id,
                    timeframe=timeframe,
                    timeframe_seconds=timeframe_seconds,
                    regime_version=regime_version,
                    regime_rows=len(regime_rows),
                ),
            )
        )
        overlays = build_regime_overlays(
            candles=candles,
            regime_rows=regime_rows,
            timeframe_seconds=timeframe_seconds,
            regime_version=regime_version,
        )
        if not overlays:
            self._emit_warning(
                "regime_overlay_empty",
                "Regime overlay unavailable: no regime stats found for window",
                strategy_id=strategy_id,
                symbol=symbol,
                timeframe=timeframe,
            )
            logger.warning(
                with_log_context(
                    "bot_regime_overlay_empty",
                    self._runtime_log_context(
                        strategy_id=strategy_id,
                        symbol=symbol,
                        instrument_id=instrument_id,
                        timeframe=timeframe,
                    ),
                )
            )
            return []
        overlay_counts = {
            "overlays": len(overlays),
            "boxes": sum(len(o.get("payload", {}).get("boxes", []) or []) for o in overlays),
            "segments": sum(len(o.get("payload", {}).get("segments", []) or []) for o in overlays),
            "markers": sum(len(o.get("payload", {}).get("markers", []) or []) for o in overlays),
        }
        logger.debug(
            with_log_context(
                "bot_regime_overlay_built",
                self._runtime_log_context(
                    strategy_id=strategy_id,
                    symbol=symbol,
                    instrument_id=instrument_id,
                    timeframe=timeframe,
                    **overlay_counts,
                ),
            )
        )
        # Emit a clear trace when overlays are produced so operators can correlate with frontend counts.
        logger.info(
            with_log_context(
                "bot_regime_overlay_emitted",
                self._runtime_log_context(
                    strategy_id=strategy_id,
                    symbol=symbol,
                    instrument_id=instrument_id,
                    timeframe=timeframe,
                    timeframe_seconds=timeframe_seconds,
                    regime_version=regime_version,
                    **overlay_counts,
                ),
            )
        )
        for overlay in overlays:
            overlay.update(
                {
                    "source": "regime_stats",
                    "bot_id": self.bot_id,
                    "strategy_id": strategy_id,
                    "symbol": symbol,
                    "instrument_id": instrument_id,
                }
            )
        return overlays

    def _regime_rows_for_window(
        self,
        *,
        instrument_id: str,
        timeframe: str,
        timeframe_seconds: int,
        start_dt: datetime,
        end_dt: datetime,
        strategy_id: Optional[str],
        symbol: Optional[str],
    ) -> Dict[datetime, Mapping[str, Any]]:
        start_norm = self._to_utc_naive(start_dt)
        end_norm = self._to_utc_naive(end_dt)
        regime_version = _regime_version_or_raise()
        cache_key = f"{instrument_id}:{timeframe_seconds}:{regime_version}"
        with self._regime_cache_lock:
            cached = self._regime_snapshot_cache.get(cache_key)
            if cached is None:
                rows = self._normalize_regime_rows(
                    self._fetch_regime_rows(
                        instrument_id=instrument_id,
                        timeframe_seconds=timeframe_seconds,
                        start_dt=start_dt,
                        end_dt=end_dt,
                        strategy_id=strategy_id,
                        symbol=symbol,
                        timeframe=timeframe,
                        cache_state="miss",
                    )
                )
                self._regime_snapshot_cache[cache_key] = {
                    "start": start_norm,
                    "end": end_norm,
                    "rows": dict(rows),
                }
                return rows

            cached_start = self._to_utc_naive_optional(cached.get("start"))
            cached_end = self._to_utc_naive_optional(cached.get("end"))
            cached_rows_raw = cached.get("rows") if isinstance(cached.get("rows"), Mapping) else {}
            cached_rows = self._normalize_regime_rows(cached_rows_raw)
            if not isinstance(cached_start, datetime) or not isinstance(cached_end, datetime):
                rows = self._normalize_regime_rows(
                    self._fetch_regime_rows(
                        instrument_id=instrument_id,
                        timeframe_seconds=timeframe_seconds,
                        start_dt=start_dt,
                        end_dt=end_dt,
                        strategy_id=strategy_id,
                        symbol=symbol,
                        timeframe=timeframe,
                        cache_state="rebuild",
                    )
                )
                self._regime_snapshot_cache[cache_key] = {
                    "start": start_norm,
                    "end": end_norm,
                    "rows": dict(rows),
                }
                return rows

            needs_older = start_norm < cached_start
            needs_newer = end_norm > cached_end
            rows = dict(cached_rows)
            if needs_older:
                older_rows = self._normalize_regime_rows(
                    self._fetch_regime_rows(
                        instrument_id=instrument_id,
                        timeframe_seconds=timeframe_seconds,
                        start_dt=start_norm,
                        end_dt=cached_start,
                        strategy_id=strategy_id,
                        symbol=symbol,
                        timeframe=timeframe,
                        cache_state="partial_older",
                    )
                )
                rows.update(older_rows)
            if needs_newer:
                newer_rows = self._normalize_regime_rows(
                    self._fetch_regime_rows(
                        instrument_id=instrument_id,
                        timeframe_seconds=timeframe_seconds,
                        start_dt=cached_end,
                        end_dt=end_norm,
                        strategy_id=strategy_id,
                        symbol=symbol,
                        timeframe=timeframe,
                        cache_state="partial_newer",
                    )
                )
                rows.update(newer_rows)

            # Bound cache memory to the active window.
            window_rows = {
                candle_time: payload
                for candle_time, payload in rows.items()
                if start_norm <= self._to_utc_naive(candle_time) <= end_norm
            }
            self._regime_snapshot_cache[cache_key] = {
                "start": start_norm,
                "end": end_norm,
                "rows": dict(window_rows),
            }
            return window_rows

    def _normalize_regime_rows(
        self,
        rows: Mapping[Any, Mapping[str, Any]],
    ) -> Dict[datetime, Mapping[str, Any]]:
        normalized: Dict[datetime, Mapping[str, Any]] = {}
        for candle_time, payload in (rows or {}).items():
            if not isinstance(candle_time, datetime):
                continue
            normalized[self._to_utc_naive(candle_time)] = payload
        return normalized

    def _fetch_regime_rows(
        self,
        *,
        instrument_id: str,
        timeframe_seconds: int,
        start_dt: datetime,
        end_dt: datetime,
        strategy_id: Optional[str],
        symbol: Optional[str],
        timeframe: str,
        cache_state: str,
    ) -> Dict[datetime, Mapping[str, Any]]:
        from portal.backend.service.market.stats_repository import build_stats_snapshot

        regime_version = _regime_version_or_raise()
        if end_dt < start_dt:
            return {}
        stats_cache_key = f"{instrument_id}:{timeframe_seconds}:{start_dt.isoformat()}->{end_dt.isoformat()}"
        with perf_log(
            "cache.lookup",
            logger=logger,
            base_context=self._runtime_log_context(
                instrument_id=instrument_id,
                timeframe_seconds=timeframe_seconds,
                strategy_id=strategy_id,
                symbol=symbol,
                timeframe=timeframe,
                cache_state=cache_state,
            ),
            enabled=self._obs_enabled,
            slow_ms=self._obs_slow_ms,
            operation_name="build_stats_snapshot",
            cache_scope="series_builder_regime",
            cache_key_summary=stats_cache_key,
        ):
            stats_snapshot = build_stats_snapshot(
                instrument_ids=[instrument_id],
                timeframe_seconds=timeframe_seconds,
                start=start_dt,
                end=end_dt,
                regime_versions=[regime_version],
                include_latest_regime=True,
            )
        logger.debug(
            with_log_context(
                "bot_regime_overlay_snapshot_built",
                self._runtime_log_context(
                    strategy_id=strategy_id,
                    symbol=symbol,
                    instrument_id=instrument_id,
                    timeframe=timeframe,
                    timeframe_seconds=timeframe_seconds,
                    regime_rows=len(stats_snapshot.regime_stats_by_version),
                    start=start_dt,
                    end=end_dt,
                    cache_state=cache_state,
                ),
            )
        )
        regime_rows: Dict[datetime, Mapping[str, Any]] = {}
        discovered_versions: set[str] = set()
        for (inst_id, candle_time, version), regime in stats_snapshot.regime_stats_by_version.items():
            if inst_id != instrument_id:
                continue
            discovered_versions.add(str(version))
            if str(version) != regime_version:
                continue
            if candle_time:
                regime_rows[self._to_utc_naive(candle_time)] = regime
        if not regime_rows and discovered_versions and regime_version not in discovered_versions:
            available_versions = sorted(discovered_versions)
            warning_context = self._runtime_log_context(
                strategy_id=strategy_id,
                symbol=symbol,
                instrument_id=instrument_id,
                timeframe=timeframe,
                requested_regime_version=regime_version,
                available_regime_versions=",".join(available_versions),
            )
            logger.warning(with_log_context("bot_regime_overlay_version_mismatch", warning_context))
            self._emit_warning(
                "regime_overlay_version_mismatch",
                "Regime overlay version mismatch: runtime requested unavailable regime version.",
                strategy_id=strategy_id,
                symbol=symbol,
                instrument_id=instrument_id,
                timeframe=timeframe,
                requested_regime_version=regime_version,
                available_regime_versions=available_versions,
            )
        return regime_rows

    @staticmethod
    def _to_utc_naive(value: Any) -> datetime:
        if not isinstance(value, datetime):
            raise TypeError(f"Expected datetime value, received {type(value).__name__}")
        if value.tzinfo is None:
            return value
        return value.astimezone(timezone.utc).replace(tzinfo=None)

    @staticmethod
    def _to_utc_naive_optional(value: Any) -> Optional[datetime]:
        if not isinstance(value, datetime):
            return None
        if value.tzinfo is None:
            return value
        return value.astimezone(timezone.utc).replace(tzinfo=None)

    @staticmethod
    def _indicator_overlay_cache_key(
        indicator_id: str,
        start_iso: Optional[str],
        end_iso: Optional[str],
        interval: Optional[str],
        symbol: Optional[str],
        datasource: Optional[str],
        exchange: Optional[str],
        overlay_signature: Optional[str] = None,
    ) -> str:
        parts = [
            indicator_id or "",
            start_iso or "",
            end_iso or "",
            str(interval or ""),
            (symbol or "").upper(),
            (datasource or "").lower(),
            (exchange or "").lower(),
            overlay_signature or "",
        ]
        return ":".join(parts)

    @staticmethod
    def _overlay_signature(options: Mapping[str, Any]) -> str:
        if not options:
            return ""
        parts = [f"{key}={options[key]}" for key in sorted(options.keys())]
        return "|".join(parts)

    @staticmethod
    def _overlay_options_for_indicator(
        indicator_type: str, params: Mapping[str, Any]
    ) -> Dict[str, Any]:
        """Extract optional per-indicator overlay options from metadata."""
        raw = params.get("overlay_options") if isinstance(params, Mapping) else None
        options = dict(raw) if isinstance(raw, Mapping) else {}
        context = build_log_context(
            indicator_type=indicator_type,
            params_keys=list(params.keys()) if isinstance(params, Mapping) else [],
            options_keys=sorted(options.keys()),
        )
        logger.debug(with_log_context("overlay_options_extracted", context))
        return options

    @staticmethod
    def _extract_indicator_overlays(result: Mapping[str, Any]) -> List[Dict[str, Any]]:
        # Indicator results include overlays that visualize raw signal markers.
        # The bot lens should only render the strategy's configured indicator
        # overlays, so skip signal-driven visuals entirely.
        return []

    @staticmethod
    def _build_signals_from_markers(markers: Mapping[str, Any]) -> Deque[StrategySignal]:
        queued: List[StrategySignal] = []
        buy_markers = markers.get("buy", []) or []
        sell_markers = markers.get("sell", []) or []
        context = build_log_context(
            buy_count=len(buy_markers),
            sell_count=len(sell_markers),
        )
        logger.debug(with_log_context("build_signals_from_markers", context))
        for entry in buy_markers:
            epoch = SeriesBuilderOverlaysRegimeMixin._normalise_epoch(entry.get("time"))
            known_at = SeriesBuilderOverlaysRegimeMixin._normalise_epoch(entry.get("known_at"))
            if epoch is not None and known_at is not None and known_at > epoch:
                raise RuntimeError(
                    f"signal_contract_invalid: known_at({known_at}) must be <= signal_time({epoch}) for long marker"
                )
            if epoch is not None:
                queued.append(StrategySignal(epoch=epoch, direction="long"))
        for entry in sell_markers:
            epoch = SeriesBuilderOverlaysRegimeMixin._normalise_epoch(entry.get("time"))
            known_at = SeriesBuilderOverlaysRegimeMixin._normalise_epoch(entry.get("known_at"))
            if epoch is not None and known_at is not None and known_at > epoch:
                raise RuntimeError(
                    f"signal_contract_invalid: known_at({known_at}) must be <= signal_time({epoch}) for short marker"
                )
            if epoch is not None:
                queued.append(StrategySignal(epoch=epoch, direction="short"))
        queued.sort(key=lambda signal: signal.epoch)
        total_context = build_log_context(total_signals=len(queued))
        logger.debug(with_log_context("build_signals_from_markers", total_context))
        return deque(queued)

    @staticmethod
    def _normalise_epoch(value: Any) -> Optional[int]:
        if value in (None, ""):
            return None
        if isinstance(value, (int, float)):
            try:
                return int(float(value))
            except (TypeError, ValueError):
                return None
        text = str(value).strip()
        if not text:
            return None
        if text.endswith("Z"):
            text = f"{text[:-1]}+00:00"
        try:
            parsed = datetime.fromisoformat(text)
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
            else:
                parsed = parsed.astimezone(timezone.utc)
            return int(parsed.timestamp())
        except ValueError:
            return normalize_epoch(value)

    @staticmethod
    def _build_candles(df: pd.DataFrame, timeframe: Optional[str] = None) -> List[Candle]:
        import pandas as pd

        frame = df.copy()
        frame.index = pd.to_datetime(frame.index, utc=True)
        if not frame.index.is_monotonic_increasing:
            frame = frame.sort_index()
        range_series = frame.get("high", frame.get("High")) - frame.get("low", frame.get("Low"))
        frame["__range__"] = range_series
        atr_col = None
        for candidate in ("ATR_Wilder", "atr", "atr_wilder"):
            if candidate in frame.columns:
                atr_col = candidate
                break
        volume_col = None
        for candidate in ("volume", "Volume"):
            if candidate in frame.columns:
                volume_col = candidate
                break
        if atr_col:
            frame["__avg_atr_15"] = frame[atr_col].rolling(window=15).mean().shift(1)
        frame["__avg_range_15"] = range_series.rolling(window=15).mean().shift(1)
        if volume_col:
            frame["__avg_volume_15"] = frame[volume_col].rolling(window=15).mean().shift(1)
        candles: List[Candle] = []
        duration = timeframe_duration(timeframe)
        for ts, row in frame.iterrows():
            try:
                open_price = float(row.get("open", row.get("Open")))
                high_price = float(row.get("high", row.get("High")))
                low_price = float(row.get("low", row.get("Low")))
                close_price = float(row.get("close", row.get("Close")))
            except (TypeError, ValueError):
                continue
            start_dt = ts.to_pydatetime()
            end_dt = start_dt + duration if duration else None
            atr_value = None
            if atr_col and row.get(atr_col) is not None:
                try:
                    atr_value = float(row.get(atr_col))
                except (TypeError, ValueError):
                    atr_value = None
            volume_value = None
            if volume_col and row.get(volume_col) is not None:
                try:
                    volume_value = float(row.get(volume_col))
                except (TypeError, ValueError):
                    volume_value = None
            lookback = {
                "avg_range_15": row.get("__avg_range_15"),
                "avg_atr_15": row.get("__avg_atr_15"),
                "avg_volume_15": row.get("__avg_volume_15"),
            }
            candles.append(
                Candle(
                    time=start_dt,
                    open=open_price,
                    high=high_price,
                    low=low_price,
                    close=close_price,
                    end=end_dt,
                    atr=atr_value,
                    volume=volume_value,
                    range=float(high_price - low_price),
                    lookback_15={k: float(v) if v is not None and not pd.isna(v) else None for k, v in lookback.items()},
                )
            )
        return candles

    def _resolve_risk_template(self, strategy: Mapping[str, Any]) -> Dict[str, Any]:
        from portal.backend.service.risk.atm import merge_templates

        return merge_templates(strategy.get("atm_template"))
