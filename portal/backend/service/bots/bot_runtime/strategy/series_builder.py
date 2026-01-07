"""Series preparation utilities for bot runtime orchestration."""

from __future__ import annotations

import logging
from collections import deque
from copy import deepcopy
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Deque, Dict, List, Mapping, Optional, Sequence, Tuple

from ....risk.atm import merge_templates
from engines.bot_runtime.adapters import BacktestAdapter, PerpExecutionAdapter
from engines.bot_runtime.core.domain import (
    Candle,
    LadderRiskEngine,
    StrategySignal,
    isoformat,
    normalize_epoch,
    timeframe_duration,
)
from utils.log_context import build_log_context, merge_log_context, series_log_context, strategy_log_context, with_log_context
from .models import Strategy
from ..reporting.reporting import instrument_key
from .strategy_loader import StrategyLoader

logger = logging.getLogger(__name__)

DEFAULT_SIM_LOOKBACK_DAYS = 7


@dataclass
class StrategySeries:
    """Runtime payload describing a single strategy stream."""

    strategy_id: str
    name: str
    symbol: str
    timeframe: str
    datasource: Optional[str]
    exchange: Optional[str]
    candles: List[Candle]
    signals: Deque[StrategySignal] = field(default_factory=deque)
    overlays: List[Dict[str, Any]] = field(default_factory=list)
    risk_engine: LadderRiskEngine = field(default_factory=LadderRiskEngine)
    window_start: Optional[str] = None
    window_end: Optional[str] = None
    meta: Dict[str, Any] = field(default_factory=dict)
    last_consumed_epoch: int = 0
    instrument: Optional[Dict[str, Any]] = None
    atm_template: Dict[str, Any] = field(default_factory=dict)
    trade_overlay: Optional[Dict[str, Any]] = None


class SeriesBuilder:
    """Prepare strategy series and overlays for the runtime."""

    def __init__(
        self,
        bot_id: str,
        config: Mapping[str, Any],
        run_type: str,
        log_candle_sequence: Optional[callable] = None,
    ):
        self.bot_id = bot_id
        self.config = config
        self.run_type = run_type
        self._log_candle_sequence = log_candle_sequence
        self._indicator_overlay_cache: Dict[str, Dict[str, Any]] = {}

    def _runtime_log_context(self, **fields: object) -> Dict[str, object]:
        return build_log_context(bot_id=self.bot_id, bot_mode=self.run_type, **fields)

    def _series_log_context(self, series: StrategySeries, **fields: object) -> Dict[str, object]:
        return merge_log_context(self._runtime_log_context(), series_log_context(series), **fields)

    def _strategy_log_context(self, strategy: Strategy, **fields: object) -> Dict[str, object]:
        return merge_log_context(self._runtime_log_context(), strategy_log_context(strategy), **fields)

    def reset(self) -> None:
        self._indicator_overlay_cache.clear()

    def build_series_by_ids(self, strategy_ids: List[str]) -> List[StrategySeries]:
        """Build series from strategy IDs (clean DB-based approach).

        Loads strategies fresh from the database with proper typing,
        avoiding config drift and confusion.

        Args:
            strategy_ids: List of strategy IDs to build series for

        Returns:
            List of StrategySeries ready for runtime execution (one per enabled instrument per strategy)

        Raises:
            ValueError: If any strategy not found
        """
        series_list: List[StrategySeries] = []
        for strategy_id in strategy_ids:
            # Load strategy fresh from DB with proper typing
            strategy = StrategyLoader.fetch_strategy(strategy_id)
            # Build one series per enabled instrument
            series_per_strategy = self._build_series_for_strategy(strategy)
            series_list.extend(series_per_strategy)
        return series_list

    def append_series_updates(self, series: StrategySeries, start_iso: str, end_iso: str) -> bool:
        from ....market.candle_service import fetch_ohlcv
        from ....strategies import strategy_service

        df = fetch_ohlcv(
            series.symbol,
            start_iso,
            end_iso,
            series.timeframe,
            datasource=series.datasource,
            exchange=series.exchange,
        )
        if df is None or getattr(df, "empty", False):
            return False
        new_candles = [
            c
            for c in self._build_candles(df, series.timeframe)
            if not series.candles or c.time > series.candles[-1].time
        ]
        if not new_candles:
            return False
        series.candles.extend(new_candles)
        try:
            config = {"mode": self.run_type}
            context = self._series_log_context(series, has_meta=series.meta is not None)
            logger.info(with_log_context("append_series_updates", context))
            if series.meta:
                rules = series.meta.get("rules", [])
                rules_context = self._series_log_context(series, rules_in_meta=len(rules))
                logger.info(with_log_context("append_series_updates_rules", rules_context))

            call_context = self._series_log_context(
                series,
                start=series.window_start or start_iso,
                end=end_iso,
                mode=self.run_type,
            )
            logger.info(with_log_context("append_series_updates_call", call_context))

            evaluation = strategy_service.evaluate(
                strategy_id=series.strategy_id,
                start=series.window_start or start_iso,
                end=end_iso,
                interval=series.timeframe,
                symbol=series.symbol,
                datasource=series.datasource,
                exchange=series.exchange,
                config=config,
            )
            overlays = self._extract_indicator_overlays(evaluation)
            overlays.extend(
                self._indicator_overlay_entries(
                    series.meta or {},
                    series.window_start or start_iso,
                    end_iso,
                    series.timeframe,
                    series.symbol,
                    series.datasource,
                    series.exchange,
                )
            )
            series.overlays = overlays
            chart_markers = evaluation.get("chart_markers") or {}
            marker_context = self._series_log_context(
                series,
                chart_markers_keys=sorted(chart_markers.keys()),
                buy_markers=len(chart_markers.get("buy", [])),
                sell_markers=len(chart_markers.get("sell", [])),
            )
            logger.debug(with_log_context("append_series_updates_markers", marker_context))
            signals = self._build_signals_from_markers(chart_markers)
            raw_context = self._series_log_context(
                series,
                raw_signals=len(signals),
                last_consumed_epoch=series.last_consumed_epoch,
            )
            logger.debug(with_log_context("append_series_updates_signals_raw", raw_context))
            while signals and signals[0].epoch <= series.last_consumed_epoch:
                signals.popleft()
            filtered_context = self._series_log_context(series, filtered_signals=len(signals))
            logger.debug(with_log_context("append_series_updates_signals_filtered", filtered_context))
            series.signals = signals
            series.window_end = end_iso
        except Exception as exc:  # pragma: no cover - defensive logging
            error_context = self._series_log_context(series, error=str(exc))
            logger.exception(with_log_context("bot_runtime_refresh_failed", error_context))
        return True

    def _fetch_ohlcv_data(
        self,
        symbol: str,
        start_iso: str,
        end_iso: str,
        timeframe: str,
        datasource: Optional[str],
        exchange: Optional[str],
        strategy_id: Optional[str] = None,
    ):
        """Fetch and validate OHLCV dataframe for strategy."""
        from ....market.candle_service import fetch_ohlcv

        try:
            df = fetch_ohlcv(
                symbol,
                start_iso,
                end_iso,
                timeframe,
                datasource=datasource,
                exchange=exchange,
            )
        except Exception as exc:
            message = f"Failed to fetch OHLCV data: {exc}"
            raise RuntimeError(message) from exc

        if df is None or getattr(df, "empty", False):
            message = "No OHLCV data returned for strategy"
            context = self._runtime_log_context(
                strategy_id=strategy_id,
                symbol=symbol,
                timeframe=timeframe,
                datasource=datasource,
                exchange=exchange,
            )
            logger.error(with_log_context("bot_runtime_no_candles", context))
            raise RuntimeError(message)

        # Warn if dataframe is not sorted
        if not df.index.is_monotonic_increasing:
            first_idx = df.index[0] if len(df.index) else None
            second_idx = df.index[1] if len(df.index) > 1 else None
            context = self._runtime_log_context(
                strategy_id=strategy_id,
                symbol=symbol,
                timeframe=timeframe,
                datasource=datasource,
                exchange=exchange,
                first=first_idx,
                second=second_idx,
                rows=len(df.index),
            )
            logger.warning(with_log_context("bot_runtime_unsorted_dataframe", context))

        return df

    def _evaluate_strategy(
        self,
        start_iso: str,
        end_iso: str,
        timeframe: str,
        instrument_id: str,
        strategy: Any,
    ) -> Dict[str, Any]:
        """Evaluate strategy and generate signals."""
        from ....strategies import strategy_service

        try:
            config = {"mode": self.run_type}
            rules = strategy.rules if hasattr(strategy, "rules") else {}
            context = self._strategy_log_context(
                strategy,
                has_rules=hasattr(strategy, "rules"),
                rule_count=len(rules),
            )
            logger.info(with_log_context("evaluate_strategy", context))
            # Intentionally avoid enabled_rules filtering here to keep bot runtime
            # aligned with strategy preview outputs.

            call_context = self._strategy_log_context(
                strategy,
                start=start_iso,
                end=end_iso,
                timeframe=timeframe,
                instrument_id=instrument_id,
                mode=self.run_type,
            )
            logger.info(with_log_context("evaluate_strategy_call", call_context))

            evaluation = strategy_service.generate_strategy_signals(
                strategy_id=strategy.id,
                start=start_iso,
                end=end_iso,
                interval=timeframe,
                instrument_ids=[instrument_id],
                config=config,
            )

            instrument_payload = None
            if isinstance(evaluation, dict) and "instruments" in evaluation:
                instruments = evaluation.get("instruments") or {}
                instrument_payload = instruments.get(instrument_id)
            if not isinstance(instrument_payload, dict):
                instrument_payload = evaluation
            # Log what we got back
            chart_markers = instrument_payload.get("chart_markers", {}) if isinstance(instrument_payload, dict) else {}
            result_context = self._strategy_log_context(
                strategy,
                buy_markers=len(chart_markers.get("buy", [])),
                sell_markers=len(chart_markers.get("sell", [])),
            )
            logger.info(with_log_context("evaluate_strategy_result", result_context))
        except Exception as exc:  # pragma: no cover - defensive logging
            message = f"Strategy evaluation failed: {exc}"
            error_context = self._strategy_log_context(strategy, error=str(exc))
            logger.exception(with_log_context("bot_runtime_strategy_eval_failed", error_context))
            raise RuntimeError(message) from exc

        return instrument_payload if isinstance(instrument_payload, dict) else evaluation

    def _build_atm_template_with_instrument(
        self,
        strategy: Strategy,
        instrument: Optional[Dict[str, Any]],
    ) -> Dict[str, Any]:
        """Merge ATM template with instrument fields (if not overridden)."""
        atm_template = merge_templates(strategy.atm_template)
        template_meta = atm_template.get("_meta") if isinstance(atm_template.get("_meta"), dict) else {}

        def _apply_instrument_field(field: str) -> None:
            """Apply instrument field to template if not overridden."""
            if template_meta.get(f"{field}_override"):
                return
            if not instrument:
                return
            value = instrument.get(field)
            if value is None:
                return
            atm_template[field] = value

        # Apply instrument fields to template
        for field_name in (
            "tick_size",
            "tick_value",
            "contract_size",
            "maker_fee_rate",
            "taker_fee_rate",
            "quote_currency",
        ):
            _apply_instrument_field(field_name)

        return atm_template

    @staticmethod
    def _apply_risk_multiplier(atm_template: Dict[str, Any], multiplier: float) -> Dict[str, Any]:
        """Apply risk multiplier to ATM template.

        Multiplies the 'risk_per_trade' field (if present) by the given multiplier.
        This allows per-instrument risk scaling within a strategy.

        Args:
            atm_template: Original ATM template
            multiplier: Risk multiplier (e.g., 1.5 = 150% of base risk)

        Returns:
            Modified ATM template with adjusted risk
        """
        template_copy = deepcopy(atm_template)
        if "risk_per_trade" in template_copy:
            original_risk = template_copy["risk_per_trade"]
            template_copy["risk_per_trade"] = original_risk * multiplier
        return template_copy

    def _build_series_for_strategy(self, strategy: Strategy) -> List[StrategySeries]:
        """Build series for all instruments in a strategy.

        This method coordinates multi-instrument support by:
        1. Iterating through all instrument_links
        2. Building a separate StrategySeries for each instrument
        3. Applying per-instrument risk multipliers

        Args:
            strategy: Strategy domain model loaded from database

        Returns:
            List of StrategySeries (one per instrument)

        Raises:
            RuntimeError: If strategy has no instruments or cannot build series
        """
        if not strategy.instrument_links:
            raise RuntimeError(f"Strategy {strategy.id} has no instruments configured")

        series_list: List[StrategySeries] = []

        for instrument_link in strategy.instrument_links:
            enabled = getattr(instrument_link, "enabled", True)
            if enabled is False:
                context = self._strategy_log_context(
                    strategy,
                    symbol=instrument_link.symbol,
                )
                logger.info(with_log_context("series_instrument_skipped", context))
                continue

            try:
                series = self._build_single_series(strategy, instrument_link)
                series_list.append(series)

                # Log series build success with signal count
                signal_count = len(series.signals) if series.signals else 0
                if signal_count == 0:
                    context = self._series_log_context(
                        series,
                        candles=len(series.candles),
                        signals=signal_count,
                    )
                    logger.warning(with_log_context("series_built_no_signals", context))
                else:
                    context = self._series_log_context(
                        series,
                        candles=len(series.candles),
                        signals=signal_count,
                    )
                    logger.info(with_log_context("series_built", context))
            except Exception as exc:
                context = self._strategy_log_context(
                    strategy,
                    symbol=instrument_link.symbol,
                    error=str(exc),
                )
                logger.exception(with_log_context("series_build_failed", context))
                # Continue with other instruments rather than failing entirely
                continue

        if not series_list:
            raise RuntimeError(
                f"Strategy {strategy.id} has {len(strategy.instrument_links)} instrument(s) "
                f"but no series could be built (check if all instruments are disabled or errored)"
            )

        # Summary: Check for series with zero signals
        zero_signal_series = [s for s in series_list if not s.signals or len(s.signals) == 0]
        if zero_signal_series:
            symbols = [s.symbol for s in zero_signal_series]
            context = self._strategy_log_context(
                strategy,
                zero_signal_series=len(zero_signal_series),
                symbols=symbols,
            )
            logger.warning(with_log_context("strategy_zero_signal_series", context))

        return series_list

    def _build_single_series(
        self,
        strategy: Strategy,
        instrument_link: Any,  # StrategyInstrumentLink type
    ) -> StrategySeries:
        """Build complete series for a single instrument (orchestrator method).

        This method coordinates:
        1. Metadata resolution (symbol, timeframe, window)
        2. Data fetching (OHLCV candles)
        3. Strategy evaluation (signals, overlays)
        4. Instrument resolution and ATM template merging
        5. Risk engine creation with per-instrument risk multiplier

        Args:
            strategy: Strategy domain model loaded from database
            instrument_link: StrategyInstrumentLink for this specific instrument

        Returns:
            StrategySeries ready for runtime execution
        """
        # Step 1: Resolve strategy metadata from instrument link
        symbol = instrument_link.symbol
        if not symbol:
            raise RuntimeError(f"Instrument link for strategy {strategy.id} missing symbol")
        instrument_id = instrument_link.instrument_id
        if not instrument_id:
            raise RuntimeError(f"Instrument link for strategy {strategy.id} missing instrument_id")

        timeframe = strategy.timeframe
        datasource = strategy.datasource
        exchange = strategy.exchange

        # Extract risk multiplier for this instrument
        risk_multiplier = instrument_link.risk_multiplier or 1.0

        # Determine time window
        if self.run_type == "backtest":
            start_iso = self.config.get("backtest_start")
            end_iso = self.config.get("backtest_end")
            if not start_iso or not end_iso:
                start_iso, end_iso = self._resolve_live_window()
        else:
            start_iso, end_iso = self._resolve_live_window()

        # Step 2: Fetch and build candles
        df = self._fetch_ohlcv_data(
            symbol, start_iso, end_iso, timeframe, datasource, exchange, strategy.id
        )
        candles = self._build_candles(df, timeframe)
        if not candles:
            raise RuntimeError(f"No valid candles could be built for strategy {strategy.id}")
        if self._log_candle_sequence:
            self._log_candle_sequence("build_series", strategy.id, candles)

        # Step 3: Evaluate strategy for signals and overlays
        evaluation = self._evaluate_strategy(
            start_iso, end_iso, timeframe, instrument_id, strategy
        )
        overlays = self._extract_indicator_overlays(evaluation)
        signals = self._build_signals_from_markers(evaluation.get("chart_markers") or {})

        # Step 4: Resolve instrument and build ATM template
        instrument = self._instrument_for(datasource, exchange, symbol)
        if instrument and instrument.get("instrument_snapshot"):
            instrument = instrument.get("instrument_snapshot")

        instrument_context = self._strategy_log_context(
            strategy,
            symbol=symbol,
            instrument_id=instrument.get("id") if isinstance(instrument, dict) else None,
        )
        logger.debug(with_log_context("series_instrument_resolved", instrument_context))

        atm_template = self._build_atm_template_with_instrument(strategy, instrument)

        # Apply per-instrument risk multiplier to ATM template
        if risk_multiplier != 1.0:
            atm_template = self._apply_risk_multiplier(atm_template, risk_multiplier)
            context = self._strategy_log_context(
                strategy,
                symbol=symbol,
                multiplier=risk_multiplier,
            )
            logger.info(with_log_context("risk_multiplier_applied", context))

        # Step 5: Create risk engine and assemble series
        risk_engine = LadderRiskEngine(atm_template, instrument=instrument)
        if risk_engine.instrument_type == "spot":
            risk_engine.attach_execution_adapter(
                BacktestAdapter(
                    tick_size=risk_engine.tick_size,
                    qty_step=risk_engine.qty_step,
                    min_qty=risk_engine.min_qty,
                    min_notional=risk_engine.min_notional,
                )
            )
        elif risk_engine.instrument_type:
            risk_engine.attach_execution_adapter(
                PerpExecutionAdapter(
                    tick_size=risk_engine.tick_size,
                    qty_step=risk_engine.qty_step,
                    min_qty=risk_engine.min_qty,
                    min_notional=risk_engine.min_notional,
                )
            )

        # Convert strategy to dict for backward compatibility with meta field
        series_meta = strategy.to_dict()
        if instrument:
            series_meta["instrument"] = instrument
        series_meta["atm_template"] = atm_template

        ready_context = self._strategy_log_context(
            strategy,
            symbol=symbol,
            contracts=atm_template.get("contracts"),
            targets=",".join(str(order.get("ticks")) for order in atm_template.get("take_profit_orders", [])),
        )
        logger.info(with_log_context("bot_runtime_series_ready", ready_context))

        return StrategySeries(
            strategy_id=strategy.id,
            name=f"{strategy.name} ({symbol})",  # Include symbol for multi-instrument clarity
            symbol=symbol,
            timeframe=timeframe,
            datasource=datasource,
            exchange=exchange,
            candles=candles,
            signals=signals,
            overlays=overlays
            + self._indicator_overlay_entries(
                series_meta,  # Use dict for backward compatibility
                start_iso,
                end_iso,
                timeframe,
                symbol,
                datasource,
                exchange,
            ),
            risk_engine=risk_engine,
            window_start=start_iso,
            window_end=end_iso,
            meta=series_meta,
            instrument=instrument,
            atm_template=atm_template,
        )

    def _instrument_for(
        self,
        datasource: Optional[str],
        exchange: Optional[str],
        symbol: Optional[str],
    ) -> Optional[Dict[str, Any]]:
        # Look up canonical instrument record in storage by datasource/exchange/symbol.
        if not symbol:
            return None
        from ....market import instrument_service

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
        from ....indicators import indicator_service

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
                indicator_meta = indicator_service.get_instance_meta(indicator_id)
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
            overlay_signature = ""
            cache_key = self._indicator_overlay_cache_key(
                indicator_id,
                start_iso,
                end_iso,
                interval,
                window_symbol,
                ds,
                ex,
                overlay_signature,
            )
            cached = self._indicator_overlay_cache.get(cache_key)
            if cached:
                context = self._runtime_log_context(
                    indicator_id=indicator_id,
                    indicator_type=indicator_type,
                    symbol=window_symbol,
                    timeframe=interval,
                )
                logger.info(with_log_context("bot_overlay_cache_hit", context))
                overlays.append(deepcopy(cached))
                continue
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
            try:
                payload = indicator_service.overlays_for_instance(
                    indicator_id,
                    start=start_iso,
                    end=end_iso,
                    interval=str(interval),
                    symbol=window_symbol,
                    datasource=ds,
                    exchange=ex,
                    overlay_options=overlay_options or None,
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
            overlays.append(
                {
                    "ind_id": indicator_id,
                    "type": indicator_type,
                    "payload": payload,
                    "color": color,
                    "source": "indicator",
                }
            )
            appended_context = self._runtime_log_context(
                indicator_id=indicator_id,
                total_overlays=len(overlays),
            )
            logger.info(with_log_context("bot_overlay_appended", appended_context))
            self._indicator_overlay_cache[cache_key] = deepcopy(overlays[-1])
        return overlays

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
        """Extract overlay options using single-key lookup (no dual-key fallbacks)."""
        if indicator_type != "market_profile":
            return {}

        options: Dict[str, Any] = {}

        # Direct key access (no dual-key fallback to market_profile_ prefix)
        for key in ("use_merged_value_areas", "merge_threshold", "min_merge_sessions",
                    "extend_value_area_to_chart_end"):
            if key in params:
                options[key] = params[key]

        # Debug log to verify extraction
        context = build_log_context(
            indicator_type=indicator_type,
            params_keys=list(params.keys()),
            options=options,
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
            epoch = SeriesBuilder._normalise_epoch(entry.get("time"))
            if epoch is not None:
                queued.append(StrategySignal(epoch=epoch, direction="long"))
        for entry in sell_markers:
            epoch = SeriesBuilder._normalise_epoch(entry.get("time"))
            if epoch is not None:
                queued.append(StrategySignal(epoch=epoch, direction="short"))
        queued.sort(key=lambda signal: signal.epoch)
        total_context = build_log_context(total_signals=len(queued))
        logger.debug(with_log_context("build_signals_from_markers", total_context))
        return deque(queued)

    @staticmethod
    def _normalise_epoch(value: Any) -> Optional[int]:
        """Deprecated: Use normalize_epoch from domain module instead."""
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
        return merge_templates(strategy.get("atm_template"))
