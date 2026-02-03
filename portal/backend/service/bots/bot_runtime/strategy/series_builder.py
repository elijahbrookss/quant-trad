"""Series preparation utilities for bot runtime orchestration."""

from __future__ import annotations

import logging
from collections import deque
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
from copy import deepcopy
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from typing import Any, Callable, Deque, Dict, List, Mapping, Optional, Sequence, Tuple

import pandas as pd

from ....risk.atm import merge_templates
from engines.bot_runtime.adapters import BacktestAdapter, LiveAdapter, PaperAdapter
from engines.bot_runtime.core.domain import (
    Candle,
    LadderRiskEngine,
    StrategySignal,
    isoformat,
    normalize_epoch,
    timeframe_duration,
)
from portal.backend.service.market.stats_repository import build_stats_snapshot
from portal.backend.service.market.stats_queue import REGIME_VERSION
from utils.log_context import build_log_context, merge_log_context, series_log_context, strategy_log_context, with_log_context
from signals.overlays.schema import build_overlay
from .regime_overlay import build_regime_overlays
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
        log_candle_sequence: Optional[Callable[..., None]] = None,
        indicator_ctx: Optional[Any] = None,
        warning_sink: Optional[Callable[[Dict[str, object]], None]] = None,
    ):
        self.bot_id = bot_id
        self.config = config
        self.run_type = run_type
        self._log_candle_sequence = log_candle_sequence
        self._indicator_ctx = indicator_ctx
        self._warning_sink = warning_sink
        self._indicator_overlay_cache: Dict[str, Dict[str, Any]] = {}
        self._indicator_overlay_lock = threading.RLock()

    def _runtime_log_context(self, **fields: object) -> Dict[str, object]:
        return build_log_context(bot_id=self.bot_id, bot_mode=self.run_type, **fields)

    def _series_log_context(self, series: StrategySeries, **fields: object) -> Dict[str, object]:
        return merge_log_context(self._runtime_log_context(), series_log_context(series), **fields)

    def _strategy_log_context(self, strategy: Strategy, **fields: object) -> Dict[str, object]:
        return merge_log_context(self._runtime_log_context(), strategy_log_context(strategy), **fields)

    def _emit_warning(self, warning_type: str, message: str, **context: object) -> None:
        """Forward builder warnings to the runtime when configured."""

        if not self._warning_sink:
            return
        payload_context = {key: value for key, value in context.items() if value is not None and value != ""}
        self._warning_sink(
            {
                "type": warning_type,
                "message": message,
                "context": payload_context,
            }
        )

    def reset(self) -> None:
        with self._indicator_overlay_lock:
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
        self._maybe_emit_data_limit_warning(
            df,
            start_iso,
            series.symbol,
            series.timeframe,
            series.datasource,
            series.exchange,
            series.strategy_id,
        )
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
            instrument_id = None
            if isinstance(series.instrument, Mapping):
                instrument_id = series.instrument.get("id")
            regime_overlays = self._build_regime_overlays(
                instrument_id=instrument_id or "",
                candles=series.candles,
                timeframe=series.timeframe,
                strategy_id=series.strategy_id,
                symbol=series.symbol,
            )
            overlays.extend(regime_overlays)
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

        self._maybe_emit_data_limit_warning(
            df,
            start_iso,
            symbol,
            timeframe,
            datasource,
            exchange,
            strategy_id,
        )

        return df

    def _maybe_emit_data_limit_warning(
        self,
        df: Any,
        start_iso: Optional[str],
        symbol: Optional[str],
        timeframe: Optional[str],
        datasource: Optional[str],
        exchange: Optional[str],
        strategy_id: Optional[str],
    ) -> None:
        if not self._warning_sink or df is None or getattr(df, "empty", False):
            return
        if not start_iso:
            return
        requested = pd.to_datetime(start_iso, utc=True, errors="coerce")
        if requested is pd.NaT:
            return
        actual = pd.to_datetime(df.index.min(), utc=True, errors="coerce")
        if actual is pd.NaT:
            return
        delta_seconds = (actual - requested).total_seconds()
        if delta_seconds <= 60:
            return
        message = (
            "Historical data limited by provider. "
            f"Requested start {requested.strftime('%Y-%m-%d')}, "
            f"oldest available {actual.strftime('%Y-%m-%d')}."
        )
        self._emit_warning(
            "historical_data_limited",
            message,
            strategy_id=strategy_id,
            symbol=symbol,
            timeframe=timeframe,
            datasource=datasource,
            exchange=exchange,
            requested_start=requested.isoformat(),
            earliest_available=actual.isoformat(),
        )

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
            if isinstance(instrument_payload, dict):
                walk_forward_markers = self._build_walk_forward_markers(
                    strategy,
                    instrument_payload.get("indicator_results") or {},
                )
                if walk_forward_markers is not None:
                    chart_markers = walk_forward_markers
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

    def _build_walk_forward_markers(
        self,
        strategy: Any,
        indicator_payloads: Mapping[str, Any],
    ) -> Optional[Dict[str, List[Dict[str, Any]]]]:
        if not isinstance(indicator_payloads, Mapping):
            return None
        from strategies import evaluator, markers

        signals_by_indicator: Dict[str, List[Dict[str, Any]]] = {}
        all_epochs: Set[int] = set()
        for indicator_id, payload in indicator_payloads.items():
            if not isinstance(payload, Mapping):
                continue
            signals = payload.get("signals")
            if not isinstance(signals, Sequence):
                continue
            cleaned: List[Dict[str, Any]] = []
            for signal in signals:
                if not isinstance(signal, Mapping):
                    continue
                epoch = evaluator._extract_signal_epoch(signal)
                if epoch is None:
                    continue
                cleaned.append(dict(signal))
                all_epochs.add(epoch)
            if cleaned:
                cleaned.sort(key=lambda entry: evaluator._extract_signal_epoch(entry) or 0)
                signals_by_indicator[str(indicator_id)] = cleaned

        if not all_epochs:
            return {"buy": [], "sell": []}

        sorted_epochs = sorted(all_epochs)
        buy_results: List[Dict[str, Any]] = []
        sell_results: List[Dict[str, Any]] = []
        emitted: Set[Tuple[str, int, Optional[str]]] = set()

        for epoch in sorted_epochs:
            filtered_payloads: Dict[str, Dict[str, Any]] = {}
            for indicator_id, signals in signals_by_indicator.items():
                filtered = [
                    signal
                    for signal in signals
                    if (evaluator._extract_signal_epoch(signal) or 0) <= epoch
                    and self._signal_known_at_epoch(signal) <= epoch
                ]
                payload = indicator_payloads.get(indicator_id) or {}
                if isinstance(payload, Mapping):
                    payload_copy = dict(payload)
                else:
                    payload_copy = {}
                payload_copy["signals"] = filtered
                filtered_payloads[indicator_id] = payload_copy

            for rule in getattr(strategy, "rules", {}).values():
                result = self._evaluate_rule_payload(rule, filtered_payloads)
                if not result:
                    continue
                if not result.get("matched"):
                    continue
                terminal_signal = result.get("signal")
                if not isinstance(terminal_signal, Mapping):
                    continue
                terminal_epoch = evaluator._extract_signal_epoch(terminal_signal)
                if terminal_epoch is None or terminal_epoch != epoch:
                    continue
                dedupe_key = (result.get("rule_id") or "", terminal_epoch, result.get("direction"))
                if dedupe_key in emitted:
                    continue
                emitted.add(dedupe_key)
                payload = dict(result)
                payload["signals"] = [terminal_signal]
                if result.get("action") == "buy":
                    buy_results.append(payload)
                elif result.get("action") == "sell":
                    sell_results.append(payload)

        return markers.build_chart_markers(buy_results, sell_results)

    @staticmethod
    def _evaluate_rule_payload(
        rule_payload: Any,
        indicator_payloads: Mapping[str, Dict[str, Any]],
    ) -> Optional[Dict[str, Any]]:
        from strategies import evaluator

        if hasattr(rule_payload, "evaluate"):
            return rule_payload.evaluate(indicator_payloads)
        if not isinstance(rule_payload, Mapping):
            return None

        matched = False
        reason: Optional[str] = None
        condition_results: List[Dict[str, Any]] = []
        trigger_signals: List[Dict[str, Any]] = []

        enabled = rule_payload.get("enabled", True)
        conditions = rule_payload.get("conditions") or []
        if not enabled:
            reason = "Rule disabled"
        elif not conditions:
            reason = "Rule has no conditions"
        else:
            match_results: List[bool] = []
            for condition in conditions:
                if not isinstance(condition, Mapping):
                    continue
                condition_obj = SimpleNamespace(
                    indicator_id=condition.get("indicator_id"),
                    signal_type=condition.get("signal_type"),
                    rule_id=condition.get("rule_id"),
                    direction=condition.get("direction"),
                )
                result = evaluator._evaluate_condition(condition_obj, indicator_payloads)
                condition_results.append(result)
                match_results.append(result.get("matched"))
                if result.get("matched"):
                    signals = result.get("signals") or []
                    if signals:
                        trigger_signals.extend(signals)
                    elif result.get("signal"):
                        trigger_signals.append(result.get("signal"))

            match_mode = str(rule_payload.get("match") or "all").lower()
            if match_mode == "any":
                matched = any(match_results)
            else:
                matched = bool(match_results) and all(match_results)

            if not matched and not reason:
                reason = "No matching signals"

        direction = None
        if trigger_signals:
            direction = evaluator._infer_signal_direction(trigger_signals[-1])

        return {
            "rule_id": rule_payload.get("id"),
            "rule_name": rule_payload.get("name"),
            "action": rule_payload.get("action"),
            "matched": matched,
            "conditions": condition_results,
            "signals": trigger_signals if matched else [],
            "direction": direction,
            "reason": reason,
        }

    @staticmethod
    def _signal_known_at_epoch(signal: Mapping[str, Any]) -> int:
        from strategies import evaluator

        metadata = signal.get("metadata") if isinstance(signal, Mapping) else None
        candidates = []
        if isinstance(metadata, Mapping):
            for key in (
                "known_at",
                "formed_at",
                "session_end",
                "value_area_end",
                "profile_end",
                "va_end",
            ):
                if key in metadata:
                    candidates.append(metadata.get(key))
        if "known_at" in signal:
            candidates.append(signal.get("known_at"))
        if "formed_at" in signal:
            candidates.append(signal.get("formed_at"))
        for value in candidates:
            epoch = evaluator._iso_to_epoch_seconds(value)
            if epoch is not None:
                return epoch
        return 0

    def _build_atm_template_with_instrument(
        self,
        strategy: Strategy,
        instrument: Optional[Dict[str, Any]],
    ) -> Dict[str, Any]:
        """Merge ATM template with instrument fields (if not overridden)."""
        atm_template = merge_templates(strategy.atm_template)
        template_meta = atm_template.get("_meta") if isinstance(atm_template.get("_meta"), dict) else {}

        def _apply_instrument_field(field: str) -> None:
            """Always apply instrument field to template to avoid stale overrides."""
            if not instrument:
                return
            value = instrument.get(field)
            if value is None:
                atm_template.pop(field, None)
                template_meta.pop(f"{field}_override", None)
                return
            atm_template[field] = value
            template_meta.pop(f"{field}_override", None)

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

        risk = atm_template.get("risk") if isinstance(atm_template.get("risk"), dict) else {}
        if strategy.base_risk_per_trade is not None:
            risk["base_risk_per_trade"] = strategy.base_risk_per_trade
        if strategy.global_risk_multiplier is not None:
            risk["global_risk_multiplier"] = strategy.global_risk_multiplier
        if risk:
            atm_template["risk"] = risk

        if template_meta:
            atm_template["_meta"] = template_meta

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
        enabled_links: List[Any] = []

        for instrument_link in strategy.instrument_links:
            enabled = getattr(instrument_link, "enabled", True)
            if enabled is False:
                context = self._strategy_log_context(
                    strategy,
                    symbol=instrument_link.symbol,
                )
                logger.info(with_log_context("series_instrument_skipped", context))
                continue
            enabled_links.append(instrument_link)

        if enabled_links:
            with ThreadPoolExecutor(max_workers=len(enabled_links)) as executor:
                future_map = {
                    executor.submit(self._build_single_series, strategy, link): link
                    for link in enabled_links
                }
                for future in as_completed(future_map):
                    instrument_link = future_map[future]
                    try:
                        series = future.result()
                    except Exception as exc:
                        context = self._strategy_log_context(
                            strategy,
                            symbol=instrument_link.symbol,
                            error=str(exc),
                        )
                        logger.exception(with_log_context("series_build_failed", context))
                        continue
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
        self._attach_execution_adapter(risk_engine, instrument)

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

        overlay_entries = self._indicator_overlay_entries(
            series_meta,  # Use dict for backward compatibility
            start_iso,
            end_iso,
            timeframe,
            symbol,
            datasource,
            exchange,
        )
        overlays = overlays + overlay_entries
        regime_overlays = self._build_regime_overlays(
            instrument_id=instrument_id,
            candles=candles,
            timeframe=timeframe,
            strategy_id=strategy.id,
            symbol=symbol,
        )
        if regime_overlays:
            overlays.extend(regime_overlays)
        return StrategySeries(
            strategy_id=strategy.id,
            name=f"{strategy.name} ({symbol})",  # Include symbol for multi-instrument clarity
            symbol=symbol,
            timeframe=timeframe,
            datasource=datasource,
            exchange=exchange,
            candles=candles,
            signals=signals,
            overlays=overlays,
            risk_engine=risk_engine,
            window_start=start_iso,
            window_end=end_iso,
            meta=series_meta,
            instrument=instrument,
            atm_template=atm_template,
        )

    @staticmethod
    def _overlay_summary(overlays: Sequence[Mapping[str, Any]]) -> Dict[str, Any]:
        type_counts: Dict[str, int] = {}
        payload_counts = {
            "boxes": 0,
            "markers": 0,
            "price_lines": 0,
            "polylines": 0,
            "segments": 0,
            "bubbles": 0,
        }
        profile_counts: Dict[str, int] = {}
        profile_params_present: Dict[str, int] = {}
        for overlay in overlays or []:
            if not isinstance(overlay, Mapping):
                continue
            overlay_type = str(overlay.get("type") or "unknown")
            type_counts[overlay_type] = type_counts.get(overlay_type, 0) + 1
            payload = overlay.get("payload")
            if not isinstance(payload, Mapping):
                continue
            profiles = payload.get("profiles")
            if isinstance(profiles, list):
                profile_counts[overlay_type] = profile_counts.get(overlay_type, 0) + len(profiles)
            if "profile_params" in payload:
                profile_params_present[overlay_type] = profile_params_present.get(overlay_type, 0) + 1
            for key in payload_counts.keys():
                entries = payload.get(key)
                if isinstance(entries, list):
                    payload_counts[key] += len(entries)
        return {
            "total_overlays": len(overlays or []),
            "type_counts": type_counts,
            "payload_counts": payload_counts,
            "profile_counts": profile_counts,
            "profile_params_present": profile_params_present,
        }

    def _attach_execution_adapter(self, risk_engine: LadderRiskEngine, instrument: Optional[Dict[str, Any]]) -> None:
        if not instrument:
            raise ValueError("Instrument metadata is required to attach execution adapter.")
        short_requires_borrow = instrument.get("short_requires_borrow")
        if short_requires_borrow is None:
            raise ValueError("Instrument metadata missing short_requires_borrow for execution adapter selection.")

        adapter = self._adapter_for_run_type(
            short_requires_borrow=bool(short_requires_borrow),
            tick_size=risk_engine.tick_size,
            qty_step=risk_engine.qty_step,
            min_qty=risk_engine.min_qty,
            min_notional=risk_engine.min_notional,
            contract_size=risk_engine.contract_size,
        )
        risk_engine.attach_execution_adapter(adapter)

    def _adapter_for_run_type(
        self,
        *,
        short_requires_borrow: bool,
        tick_size: float,
        qty_step: Optional[float],
        min_qty: Optional[float],
        min_notional: Optional[float],
        contract_size: float,
    ):
        if self.run_type == "backtest":
            return BacktestAdapter(
                tick_size=tick_size,
                qty_step=qty_step,
                min_qty=min_qty,
                min_notional=min_notional,
                contract_size=contract_size,
                short_requires_borrow=short_requires_borrow,
            )
        if self.run_type == "paper":
            return PaperAdapter(
                tick_size=tick_size,
                qty_step=qty_step,
                min_qty=min_qty,
                min_notional=min_notional,
                contract_size=contract_size,
                short_requires_borrow=short_requires_borrow,
            )
        if self.run_type == "live":
            spot_adapter = self.config.get("spot_execution_adapter")
            derivatives_adapter = self.config.get("derivatives_execution_adapter")
            if not spot_adapter and not derivatives_adapter:
                raise ValueError("Live execution requires spot_execution_adapter or derivatives_execution_adapter.")
            return LiveAdapter(
                short_requires_borrow=short_requires_borrow,
                spot_adapter=spot_adapter,
                derivatives_adapter=derivatives_adapter,
            )
        raise ValueError(f"Unsupported run_type '{self.run_type}' for execution adapter selection.")

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
            with self._indicator_overlay_lock:
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
                if self._indicator_ctx is None:
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
                else:
                    payload = indicator_service.overlays_for_instance(
                        indicator_id,
                        start=start_iso,
                        end=end_iso,
                        interval=str(interval),
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
                if payload.get("pane_views"):
                    overlay = dict(payload)
                else:
                    overlay = build_overlay(str(payload.get("type") or indicator_type), payload.get("payload"))
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
            appended_context = self._runtime_log_context(
                indicator_id=indicator_id,
                total_overlays=len(overlays),
            )
            logger.info(with_log_context("bot_overlay_appended", appended_context))
            with self._indicator_overlay_lock:
                self._indicator_overlay_cache[cache_key] = deepcopy(overlays[-1])
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
        if not instrument_id:
            logger.warning(
                with_log_context(
                    "bot_regime_overlay_missing_instrument",
                    self._runtime_log_context(
                        bot_id=self.bot_id,
                        strategy_id=strategy_id,
                        symbol=symbol,
                        instrument_id=instrument_id,
                        timeframe=timeframe,
                    ),
                )
            )
            return []
        if not candles:
            logger.warning(
                with_log_context(
                    "bot_regime_overlay_no_candles",
                    self._runtime_log_context(
                        bot_id=self.bot_id,
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
                        bot_id=self.bot_id,
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
                        bot_id=self.bot_id,
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
        stats_snapshot = build_stats_snapshot(
            instrument_ids=[instrument_id],
            timeframe_seconds=timeframe_seconds,
            start=start_dt,
            end=end_dt,
            regime_versions=[REGIME_VERSION],
            include_latest_regime=True,
        )
        logger.debug(
            with_log_context(
                "bot_regime_overlay_snapshot_built",
                self._runtime_log_context(
                    bot_id=self.bot_id,
                    strategy_id=strategy_id,
                    symbol=symbol,
                    instrument_id=instrument_id,
                    timeframe=timeframe,
                    timeframe_seconds=timeframe_seconds,
                    regime_rows=len(stats_snapshot.regime_stats_by_version),
                    start=start_dt,
                    end=end_dt,
                ),
            )
        )
        regime_rows: Dict[datetime, Mapping[str, Any]] = {}
        for (inst_id, candle_time, version), regime in stats_snapshot.regime_stats_by_version.items():
            if inst_id != instrument_id or str(version) != str(REGIME_VERSION):
                continue
            if candle_time:
                regime_rows[candle_time] = regime
        overlays = build_regime_overlays(
            candles=candles,
            regime_rows=regime_rows,
            timeframe_seconds=timeframe_seconds,
            regime_version=str(REGIME_VERSION),
        )
        if not overlays:
            logger.warning(
                with_log_context(
                    "bot_regime_overlay_empty",
                    self._runtime_log_context(
                        bot_id=self.bot_id,
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
                    bot_id=self.bot_id,
                    strategy_id=strategy_id,
                    symbol=symbol,
                    instrument_id=instrument_id,
                    timeframe=timeframe,
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
            known_at = SeriesBuilder._normalise_epoch(entry.get("known_at"))
            if epoch is not None and known_at is not None and known_at > epoch:
                logger.debug(
                    with_log_context(
                        "signal_known_at_delay",
                        build_log_context(
                            original_epoch=epoch,
                            known_at=known_at,
                            direction="long",
                        ),
                    )
                )
                epoch = known_at
            if epoch is not None:
                queued.append(StrategySignal(epoch=epoch, direction="long"))
        for entry in sell_markers:
            epoch = SeriesBuilder._normalise_epoch(entry.get("time"))
            known_at = SeriesBuilder._normalise_epoch(entry.get("known_at"))
            if epoch is not None and known_at is not None and known_at > epoch:
                logger.debug(
                    with_log_context(
                        "signal_known_at_delay",
                        build_log_context(
                            original_epoch=epoch,
                            known_at=known_at,
                            direction="short",
                        ),
                    )
                )
                epoch = known_at
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
