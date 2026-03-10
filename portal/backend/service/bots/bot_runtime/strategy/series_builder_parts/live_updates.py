"""SeriesBuilder mixin."""

from __future__ import annotations

import logging
from types import SimpleNamespace
from typing import Any, Dict, List, Mapping, Optional, Sequence, Set, Tuple

from signals.contract import assert_no_execution_fields, assert_signal_contract
from utils.log_context import with_log_context
from utils.perf_log import perf_log

from .models import StrategySeries

logger = logging.getLogger(__name__)

class SeriesBuilderLiveUpdatesMixin:
    def append_series_updates(self, series: StrategySeries, start_iso: str, end_iso: str) -> bool:
        from .....market.candle_service import fetch_ohlcv
        from .....strategies import strategy_service

        series_context = self._series_log_context(series)
        with perf_log(
            "bot_runtime_fetch_ohlcv",
            logger=logger,
            base_context=series_context,
            enabled=self._obs_enabled,
            slow_ms=self._obs_slow_ms,
            start_iso=start_iso,
            end_iso=end_iso,
        ) as perf:
            df = fetch_ohlcv(
                series.symbol,
                start_iso,
                end_iso,
                series.timeframe,
                datasource=series.datasource,
                exchange=series.exchange,
            )
            perf.add_fields(rows_returned=len(df) if df is not None else 0)
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

            # NOTE: NO CACHE – strategy evaluation re-computes per call.
            cache_key_summary = f"{series.symbol}:{series.timeframe}:{series.window_start or start_iso}->{end_iso}"
            with perf_log(
                "cache.absent",
                logger=logger,
                base_context=series_context,
                enabled=self._obs_enabled,
                slow_ms=self._obs_slow_ms,
                operation_name="strategy_service.evaluate",
                cache_scope="none",
                cache_key_summary=cache_key_summary,
            ):
                with perf_log(
                    "bot_runtime_strategy_evaluate",
                    logger=logger,
                    base_context=series_context,
                    enabled=self._obs_enabled,
                    slow_ms=self._obs_slow_ms,
                    strategy_id=series.strategy_id,
                ) as perf:
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
                    chart_markers = evaluation.get("chart_markers") or {}
                    perf.add_fields(
                        buy_markers_count=len(chart_markers.get("buy", [])),
                        sell_markers_count=len(chart_markers.get("sell", [])),
                    )
            overlays = self._extract_indicator_overlays(evaluation)
            if self._include_indicator_overlays:
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
        from .....market.candle_service import fetch_ohlcv

        try:
            runtime_context = self._runtime_log_context(
                strategy_id=strategy_id,
                symbol=symbol,
                timeframe=timeframe,
                datasource=datasource,
                exchange=exchange,
            )
            with perf_log(
                "bot_runtime_fetch_ohlcv",
                logger=logger,
                base_context=runtime_context,
                enabled=self._obs_enabled,
                slow_ms=self._obs_slow_ms,
                start_iso=start_iso,
                end_iso=end_iso,
            ) as perf:
                df = fetch_ohlcv(
                    symbol,
                    start_iso,
                    end_iso,
                    timeframe,
                    datasource=datasource,
                    exchange=exchange,
                )
                perf.add_fields(rows_returned=len(df) if df is not None else 0)
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
        import pandas as pd

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
        *,
        include_walk_forward_markers: bool = False,
        evaluation_config: Optional[Mapping[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Evaluate strategy and generate signals."""
        from .....strategies import strategy_service

        try:
            config = {"mode": self.run_type}
            if isinstance(evaluation_config, Mapping):
                for key, value in evaluation_config.items():
                    if value is None:
                        continue
                    if isinstance(value, Mapping):
                        config[key] = dict(value)
                    else:
                        config[key] = value
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
            if include_walk_forward_markers and isinstance(instrument_payload, dict):
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
            payload_timeframe_seconds = None
            for timeframe_key in ("chart_timeframe_seconds", "source_timeframe_seconds", "timeframe_seconds"):
                try:
                    candidate = int(payload.get(timeframe_key))  # type: ignore[arg-type]
                except (TypeError, ValueError, AttributeError):
                    candidate = 0
                if candidate > 0:
                    payload_timeframe_seconds = candidate
                    break
            payload_runtime_scope = str(payload.get("_runtime_scope") or "") if isinstance(payload, Mapping) else ""
            for signal in signals:
                if not isinstance(signal, Mapping):
                    continue
                signal_copy = dict(signal)
                signal_copy.setdefault("indicator_id", str(indicator_id))
                if payload_timeframe_seconds is not None:
                    signal_copy.setdefault("timeframe_seconds", payload_timeframe_seconds)
                if payload_runtime_scope:
                    signal_copy.setdefault("runtime_scope", payload_runtime_scope)
                signal_copy.setdefault("rule_id", signal_copy.get("type"))
                signal_copy.setdefault("pattern_id", signal_copy.get("rule_id") or signal_copy.get("type"))
                metadata = signal_copy.get("metadata")
                if isinstance(metadata, Mapping):
                    metadata_copy = dict(metadata)
                else:
                    metadata_copy = {}
                metadata_copy.setdefault("rule_id", signal_copy.get("rule_id"))
                metadata_copy.setdefault("pattern_id", signal_copy.get("pattern_id"))
                metadata_copy.setdefault("indicator_id", signal_copy.get("indicator_id"))
                metadata_copy.setdefault("runtime_scope", signal_copy.get("runtime_scope"))
                metadata_copy.setdefault("timeframe_seconds", signal_copy.get("timeframe_seconds"))
                if "signal_time" in signal_copy:
                    metadata_copy.setdefault("signal_time", signal_copy.get("signal_time"))
                elif "time" in signal_copy:
                    metadata_copy.setdefault("signal_time", signal_copy.get("time"))
                signal_copy["metadata"] = metadata_copy
                assert_signal_contract(signal_copy)
                assert_no_execution_fields(signal_copy)
                epoch = evaluator._extract_signal_epoch(signal_copy)
                if epoch is None:
                    continue
                cleaned.append(signal_copy)
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
        terminal_signal: Optional[Mapping[str, Any]] = None
        if trigger_signals:
            terminal_candidates = [
                signal for signal in trigger_signals if isinstance(signal, Mapping)
            ]
            if terminal_candidates:
                terminal_candidates.sort(
                    key=lambda signal: evaluator._extract_signal_epoch(signal) or 0
                )
                terminal_signal = terminal_candidates[-1]
                direction = evaluator._infer_signal_direction(terminal_signal)

        return {
            "rule_id": rule_payload.get("id"),
            "rule_name": rule_payload.get("name"),
            "action": rule_payload.get("action"),
            "matched": matched,
            "conditions": condition_results,
            "signal": terminal_signal if matched else None,
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
