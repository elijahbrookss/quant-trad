"""SeriesBuilder mixin."""

from __future__ import annotations

import logging
from types import SimpleNamespace
from typing import Any, Dict, List, Mapping, Optional

from utils.log_context import with_log_context
from utils.perf_log import perf_log

from .models import StrategySeries

logger = logging.getLogger(__name__)

class SeriesBuilderLiveUpdatesMixin:
    def append_series_updates(self, series: StrategySeries, start_iso: str, end_iso: str) -> bool:
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
            df = self._deps.fetch_ohlcv(
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

            instrument_id = None
            if isinstance(series.instrument, Mapping):
                instrument_id = series.instrument.get("id")
            if not instrument_id:
                raise RuntimeError(
                    f"Series {series.strategy_id} is missing instrument id for live updates"
                )
            strategy_obj = SimpleNamespace(
                id=series.strategy_id,
                rules=(series.meta or {}).get("rules") or {},
            )

            # NOTE: NO CACHE – strategy evaluation re-computes per call.
            cache_key_summary = f"{series.symbol}:{series.timeframe}:{series.window_start or start_iso}->{end_iso}"
            with perf_log(
                "cache.absent",
                logger=logger,
                base_context=series_context,
                enabled=self._obs_enabled,
                slow_ms=self._obs_slow_ms,
                operation_name="strategy_service.generate_signals",
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
                    evaluation = self._evaluate_strategy(
                        start_iso=series.window_start or start_iso,
                        end_iso=end_iso,
                        timeframe=series.timeframe,
                        instrument_id=str(instrument_id),
                        strategy=strategy_obj,
                    )
                    decision_artifacts = evaluation.get("decision_artifacts") or []
                    perf.add_fields(
                        decision_artifacts_count=len(decision_artifacts),
                    )
            series.overlays = [dict(entry) for entry in evaluation.get("overlays") or [] if isinstance(entry, Mapping)]
            marker_context = self._series_log_context(
                series,
                decision_artifacts=len(decision_artifacts),
                overlays=len(series.overlays),
            )
            logger.debug(with_log_context("append_series_updates_preview_result", marker_context))
            signals = self._build_signals_from_decision_artifacts(decision_artifacts)
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
                df = self._deps.fetch_ohlcv(
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
        evaluation_config: Optional[Mapping[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Evaluate strategy and generate signals."""
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

            evaluation = self._deps.strategy_run_preview(
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
            decision_artifacts = instrument_payload.get("decision_artifacts", []) if isinstance(instrument_payload, dict) else []
            result_context = self._strategy_log_context(
                strategy,
                decision_artifacts=len(decision_artifacts) if isinstance(decision_artifacts, list) else 0,
                overlays=len(instrument_payload.get("overlays", [])) if isinstance(instrument_payload, dict) and isinstance(instrument_payload.get("overlays"), list) else 0,
            )
            logger.info(with_log_context("evaluate_strategy_result", result_context))
        except Exception as exc:  # pragma: no cover - defensive logging
            message = f"Strategy evaluation failed: {exc}"
            error_context = self._strategy_log_context(strategy, error=str(exc))
            logger.exception(with_log_context("bot_runtime_strategy_eval_failed", error_context))
            raise RuntimeError(message) from exc

        return instrument_payload if isinstance(instrument_payload, dict) else evaluation
