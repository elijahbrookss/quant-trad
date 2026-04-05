"""SeriesBuilder mixin."""

from __future__ import annotations

import logging
import time
from collections import deque
from concurrent.futures import ThreadPoolExecutor, as_completed
from copy import deepcopy
from datetime import datetime, timezone
from types import SimpleNamespace
from typing import Any, Deque, Dict, List, Mapping, Optional, Sequence, Tuple

from engines.bot_runtime.core.domain import (
    Candle,
    LadderRiskEngine,
    StrategySignal,
    isoformat,
    normalize_epoch,
    timeframe_duration,
)
from engines.bot_runtime.adapters import BacktestAdapter, LiveAdapter, PaperAdapter
from engines.bot_runtime.core.execution_profile import compile_series_execution_profile, SeriesExecutionProfile
from atm import merge_templates
from strategies.compiler import compile_strategy
from strategies.evaluator import build_signal_candidate
from utils.log_context import build_log_context, with_log_context

from ..models import Strategy
from .models import StrategySeries

logger = logging.getLogger(__name__)

class SeriesBuilderConstructionMixin:
    @staticmethod
    def _build_signals_from_decision_artifacts(artifacts: Sequence[Mapping[str, Any]]) -> Deque[StrategySignal]:
        queued: List[StrategySignal] = []
        for artifact in artifacts:
            if not isinstance(artifact, Mapping):
                continue
            if str(artifact.get("evaluation_result") or "") != "matched_selected":
                continue
            candidate = build_signal_candidate(artifact)
            queued.append(
                StrategySignal(
                    epoch=int(candidate["epoch"]),
                    direction=str(candidate["direction"]),
                    decision_id=str(candidate.get("decision_id") or ""),
                    rule_id=str(candidate.get("rule_id") or ""),
                    intent=str(candidate.get("intent") or ""),
                    event_key=str(candidate.get("event_key") or ""),
                )
            )
        queued.sort(key=lambda signal: signal.epoch)
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
    def _build_candles(df: Any, timeframe: Optional[str] = None) -> List[Candle]:
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

        Multiplies the canonical nested base risk field by the given multiplier.
        This allows per-instrument risk scaling within a strategy.

        Args:
            atm_template: Original ATM template
            multiplier: Risk multiplier (e.g., 1.5 = 150% of base risk)

        Returns:
            Modified ATM template with adjusted risk
        """
        template_copy = deepcopy(atm_template)
        risk = template_copy.get("risk") if isinstance(template_copy.get("risk"), dict) else {}
        if "base_risk_per_trade" in risk:
            risk["base_risk_per_trade"] = float(risk["base_risk_per_trade"]) * float(multiplier)
            template_copy["risk"] = risk
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
        eligible_links: List[Any] = []

        for instrument_link in strategy.instrument_links:
            symbol = str(getattr(instrument_link, "symbol", "") or "").strip()
            if self._runtime_symbols is not None and symbol.upper() not in self._runtime_symbols:
                context = self._strategy_log_context(
                    strategy,
                    symbol=symbol,
                    allowed_symbols=sorted(self._runtime_symbols),
                )
                logger.info(with_log_context("series_instrument_filtered", context))
                continue
            eligible_links.append(instrument_link)

        if eligible_links:
            with ThreadPoolExecutor(max_workers=len(eligible_links)) as executor:
                future_map = {
                    executor.submit(self._build_single_series, strategy, link): link
                    for link in eligible_links
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

        # Determine time window. Backtests now seed with bounded warmup and then execute walk-forward event-by-event.
        replay_start_index = 0
        window_start_iso: Optional[str] = None
        if self.run_type == "backtest":
            configured_start = self.config.get("backtest_start")
            configured_end = self.config.get("backtest_end")
            if not configured_start or not configured_end:
                raise RuntimeError("Backtest runtime requires both backtest_start and backtest_end")
            start_iso = str(configured_start)
            end_iso = str(configured_end)
            warmup_bars = self._resolve_backtest_warmup_bars(strategy, timeframe)
            candles, replay_start_index, window_start_iso = self._build_backtest_candles_with_warmup(
                symbol=symbol,
                timeframe=timeframe,
                datasource=datasource,
                exchange=exchange,
                strategy_id=strategy.id,
                backtest_start_iso=start_iso,
                backtest_end_iso=end_iso,
                warmup_bars=warmup_bars,
            )
        else:
            start_iso, end_iso = self._resolve_live_window()
            window_start_iso = start_iso
            # Paper/live placeholders still use the same event-driven runtime semantics.
            df = self._fetch_ohlcv_data(
                symbol, start_iso, end_iso, timeframe, datasource, exchange, strategy.id
            )
            candles = self._build_candles(df, timeframe)
        if not candles:
            raise RuntimeError(f"No valid candles could be built for strategy {strategy.id}")
        if self._log_candle_sequence:
            self._log_candle_sequence("build_series", strategy.id, candles)

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
        execution_profile = compile_series_execution_profile(
            instrument or {},
            template=atm_template,
            runtime_requires_derivatives=False,
        )
        profile_context = self._strategy_log_context(
            strategy,
            symbol=symbol,
            instrument_type=execution_profile.instrument.instrument_type,
            accounting_mode=execution_profile.accounting_mode,
            supports_margin=execution_profile.capabilities.supports_margin,
            supports_short=execution_profile.capabilities.supports_short,
            short_requires_borrow=execution_profile.capabilities.short_requires_borrow,
            qty_step=execution_profile.constraints.qty_step,
            max_qty=execution_profile.constraints.max_qty,
            min_notional=execution_profile.constraints.min_notional,
        )
        logger.info(with_log_context("series_execution_profile_compiled", profile_context))
        risk_engine = LadderRiskEngine(
            atm_template,
            instrument=instrument,
            execution_profile=execution_profile,
        )
        risk_engine.set_runtime_context(
            strategy_id=strategy.id,
            strategy_name=strategy.name,
            timeframe=timeframe,
            datasource=datasource,
            exchange=exchange,
            symbol=symbol,
            instrument_id=instrument.get("id") if isinstance(instrument, dict) else None,
        )
        self._attach_execution_adapter(risk_engine, execution_profile)
        strategy_rules, strategy_params = strategy.compilation_inputs()
        compiled_strategy = compile_strategy(
            strategy_id=strategy.id,
            timeframe=timeframe,
            rules=list(strategy_rules.values()),
            attached_indicator_ids=strategy.indicator_ids,
            indicator_meta_getter=self._deps.indicator_get_instance_meta,
            params=strategy_params,
        )

        # Convert strategy to dict for backward compatibility with meta field
        series_meta = strategy.to_dict()
        series_meta.setdefault("rules", deepcopy(getattr(strategy, "rules", {}) or {}))
        series_meta["compiled_strategy"] = compiled_strategy
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

        # No precomputed signals/overlays in runtime path. These are evaluated incrementally per bar.
        overlays: List[Dict[str, Any]] = []
        signals = deque()
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
            window_start=window_start_iso,
            window_end=end_iso,
            meta=series_meta,
            instrument=instrument,
            atm_template=atm_template,
            replay_start_index=replay_start_index,
            execution_profile=execution_profile,
        )

    def _build_backtest_candles_with_warmup(
        self,
        *,
        symbol: str,
        timeframe: str,
        datasource: Optional[str],
        exchange: Optional[str],
        strategy_id: str,
        backtest_start_iso: str,
        backtest_end_iso: str,
        warmup_bars: int = 100,
    ) -> Tuple[List[Candle], int, str]:
        import pandas as pd

        start_ts = pd.to_datetime(backtest_start_iso, utc=True)
        end_ts = pd.to_datetime(backtest_end_iso, utc=True)
        if pd.isna(start_ts) or pd.isna(end_ts):
            raise RuntimeError("Invalid backtest_start or backtest_end timestamp")
        if start_ts >= end_ts:
            raise RuntimeError("backtest_start must be before backtest_end")

        # Bounded seed window before walk-forward start for indicator-state priming.
        tf_delta = timeframe_duration(timeframe)
        if tf_delta is None or tf_delta.total_seconds() <= 0:
            raise RuntimeError(f"Unsupported timeframe '{timeframe}' for warmup fetch")
        safe_warmup_bars = max(int(warmup_bars or 100), 1)
        warmup_start_ts = start_ts - (tf_delta * safe_warmup_bars)

        warmup_candles: List[Candle] = []
        try:
            warmup_df = self._fetch_ohlcv_data(
                symbol=symbol,
                start_iso=isoformat(warmup_start_ts.to_pydatetime()),
                end_iso=backtest_start_iso,
                timeframe=timeframe,
                datasource=datasource,
                exchange=exchange,
                strategy_id=strategy_id,
            )
            warmup_candles = [
                candle for candle in self._build_candles(warmup_df, timeframe)
                if candle.time <= start_ts.to_pydatetime()
            ]
        except RuntimeError:
            # Warmup is bounded and best-effort; walk-forward candles remain mandatory.
            warmup_candles = []
        if len(warmup_candles) > safe_warmup_bars:
            warmup_candles = warmup_candles[-safe_warmup_bars:]

        replay_df = self._fetch_ohlcv_data(
            symbol=symbol,
            start_iso=backtest_start_iso,
            end_iso=backtest_end_iso,
            timeframe=timeframe,
            datasource=datasource,
            exchange=exchange,
            strategy_id=strategy_id,
        )
        replay_candles = [
            candle for candle in self._build_candles(replay_df, timeframe)
            if candle.time >= start_ts.to_pydatetime() and candle.time <= end_ts.to_pydatetime()
        ]
        if not replay_candles:
            raise RuntimeError(f"No walk-forward candles found between {backtest_start_iso} and {backtest_end_iso}")

        combined = warmup_candles + replay_candles
        deduped: Dict[int, Candle] = {}
        for candle in combined:
            deduped[int(candle.time.timestamp())] = candle
        ordered = [deduped[key] for key in sorted(deduped.keys())]
        replay_start_index = 0
        for idx, candle in enumerate(ordered):
            if candle.time >= start_ts.to_pydatetime():
                replay_start_index = idx
                break
        return ordered, replay_start_index, isoformat(warmup_start_ts.to_pydatetime())

    def _resolve_backtest_warmup_bars(self, strategy: Strategy, timeframe: str) -> int:
        # Strategy/runtime warmup is intentionally separate from indicator-
        # specific fetch windows (e.g. indicator days_back settings).
        default_bars = 100
        configured = self.config.get("backtest_warmup_bars")
        if configured is not None:
            try:
                parsed = int(configured)
                if parsed > 0:
                    return parsed
            except (TypeError, ValueError):
                pass
        return default_bars

    def evaluate_incremental_for_bar(
        self,
        *,
        series: StrategySeries,
        candle: Candle,
        visible_candles: Sequence[Candle],
        last_evaluated_epoch: int = 0,
    ) -> Tuple[Deque[StrategySignal], List[Dict[str, Any]], Dict[str, Optional[float]]]:
        """Evaluate signals/overlays only up to the current bar (no lookahead)."""
        stage_started = time.perf_counter()
        end_iso = isoformat(candle.time)
        start_iso = str(series.window_start or end_iso)
        timeframe_delta = timeframe_duration(series.timeframe)
        if timeframe_delta and timeframe_delta.total_seconds() > 0:
            bounded_start = candle.time - (timeframe_delta * self._incremental_signal_lookback_bars)
            bounded_start_iso = isoformat(bounded_start)
            if series.window_start:
                start_iso = max(str(series.window_start), bounded_start_iso)
            else:
                start_iso = bounded_start_iso
        instrument_id = None
        if isinstance(series.instrument, Mapping):
            instrument_id = series.instrument.get("id")
        if not instrument_id:
            raise RuntimeError(f"Series {series.strategy_id} is missing instrument id for incremental evaluation")

        strategy_obj = SimpleNamespace(
            id=series.strategy_id,
            rules=(series.meta or {}).get("rules") or {},
        )
        evaluation_config = None
        if self._indicator_incremental_eval:
            evaluation_config = self._indicator_runtime_eval_config(
                series=series,
                start_iso=start_iso,
                end_iso=end_iso,
            )
        strategy_eval_started = time.perf_counter()
        evaluate_kwargs: Dict[str, Any] = {
            "start_iso": start_iso,
            "end_iso": end_iso,
            "timeframe": series.timeframe,
            "instrument_id": str(instrument_id),
            "strategy": strategy_obj,
        }
        if evaluation_config is not None:
            evaluate_kwargs["evaluation_config"] = evaluation_config
        evaluation = self._evaluate_strategy(
            **evaluate_kwargs,
        )
        strategy_eval_ms = max((time.perf_counter() - strategy_eval_started) * 1000.0, 0.0)

        decision_artifacts = evaluation.get("decision_artifacts") or []
        current_epoch = int(candle.time.timestamp())
        signals = deque(
            signal
            for signal in self._build_signals_from_decision_artifacts(decision_artifacts)
            if signal.epoch == current_epoch and signal.epoch > last_evaluated_epoch
        )

        overlay_started = time.perf_counter()
        strategy_meta = series.meta or {}
        indicator_links = list(strategy_meta.get("indicator_links") or [])
        indicator_ids = strategy_meta.get("indicator_ids")
        if not indicator_links and isinstance(indicator_ids, list):
            indicator_links = [{"indicator_id": indicator_id} for indicator_id in indicator_ids if indicator_id]
        indicators_count = float(len(indicator_links))
        overlays = [dict(entry) for entry in evaluation.get("overlays") or [] if isinstance(entry, Mapping)]
        overlays_update_ms = max((time.perf_counter() - overlay_started) * 1000.0, 0.0)
        perf_payload = evaluation.get("perf") if isinstance(evaluation, Mapping) else None
        candle_fetch_ms: Optional[float] = None
        preview_replay_ms: Optional[float] = None
        if isinstance(perf_payload, Mapping):
            raw_fetch = perf_payload.get("candle_fetch_ms")
            raw_replay = perf_payload.get("preview_replay_ms")
            try:
                candle_fetch_ms = float(raw_fetch) if raw_fetch is not None else None
            except (TypeError, ValueError):
                candle_fetch_ms = None
            try:
                preview_replay_ms = float(raw_replay) if raw_replay is not None else None
            except (TypeError, ValueError):
                preview_replay_ms = None
        total_eval_ms = max((time.perf_counter() - stage_started) * 1000.0, 0.0)
        return signals, overlays, {
            "epochs_evaluated_this_tick": 1.0,
            "strategy_eval_ms": strategy_eval_ms,
            "candle_fetch_ms": candle_fetch_ms,
            "preview_replay_ms": preview_replay_ms,
            "signals_emitted_count": float(len(signals)),
            "overlays_update_ms": overlays_update_ms,
            "indicators_count": indicators_count,
            "total_eval_ms": total_eval_ms,
        }

    @staticmethod
    def _series_runtime_key(series: StrategySeries) -> str:
        return ":".join(
            [
                str(series.strategy_id or ""),
                str(series.symbol or "").upper(),
                str(series.timeframe or ""),
                str(series.datasource or "").lower(),
                str(series.exchange or "").lower(),
            ]
        )

    def _indicator_runtime_eval_config(
        self,
        *,
        series: StrategySeries,
        start_iso: str,
        end_iso: str,
    ) -> Dict[str, Any]:
        import pandas as pd

        if not self._indicator_incremental_eval:
            return {}

        strategy_meta = series.meta or {}
        links = list(strategy_meta.get("indicator_links") or [])
        if not links and strategy_meta.get("indicator_ids"):
            links = [{"indicator_id": indicator_id} for indicator_id in strategy_meta.get("indicator_ids") if indicator_id]
        if not links:
            return {}

        overrides: Dict[str, Dict[str, Any]] = {}
        series_key = self._series_runtime_key(series)
        for link in links:
            indicator_id = str(link.get("indicator_id") or link.get("id") or "").strip()
            if not indicator_id:
                continue
            try:
                indicator_meta = self._deps.indicator_get_instance_meta(indicator_id, ctx=self._indicator_ctx)
                runtime_plan = self._deps.indicator_runtime_input_plan_for_instance(
                    indicator_id,
                    strategy_interval=str(series.timeframe),
                    start=start_iso,
                    end=end_iso,
                    ctx=self._indicator_ctx,
                )
            except Exception as exc:
                logger.warning(
                    with_log_context(
                        "indicator_runtime_input_plan_skipped",
                        self._runtime_log_context(
                            strategy_id=series.strategy_id,
                            symbol=series.symbol,
                            timeframe=series.timeframe,
                            indicator_id=indicator_id,
                            error=str(exc),
                        ),
                    )
                )
                continue
            if not bool(runtime_plan.get("incremental_eval", False)):
                continue
            source_timeframe = str(runtime_plan.get("source_timeframe") or series.timeframe)
            override_start = str(runtime_plan.get("start") or start_iso)
            try:
                source_delta = timeframe_duration(source_timeframe)
                source_seconds = int(source_delta.total_seconds()) if source_delta else 0
            except Exception:
                source_seconds = 0
            if source_seconds > 0:
                end_ts = pd.Timestamp(end_iso)
                if end_ts.tzinfo is None:
                    end_ts = end_ts.tz_localize(timezone.utc)
                else:
                    end_ts = end_ts.tz_convert(timezone.utc)
                source_bucket = int(end_ts.timestamp()) // source_seconds
                state_key = f"{series_key}:{indicator_id}"
                with self._overlay_runtime_cache_lock:
                    prior_state = self._indicator_runtime_state.get(state_key) or {}
                prior_bucket = prior_state.get("last_source_bucket")
                if isinstance(prior_bucket, int):
                    start_bucket = max(prior_bucket - (self._indicator_source_lookback_bars - 1), 0)
                else:
                    start_bucket = max(source_bucket - (self._indicator_source_lookback_bars - 1), 0)
                override_start = isoformat(datetime.fromtimestamp(start_bucket * source_seconds, tz=timezone.utc))
                with self._overlay_runtime_cache_lock:
                    self._indicator_runtime_state[state_key] = {
                        "last_source_bucket": source_bucket,
                        "source_timeframe": source_timeframe,
                        "last_end": end_iso,
                    }
            overrides[indicator_id] = {
                "start": override_start,
                "end": end_iso,
                "source_timeframe": source_timeframe,
            }
        if not overrides:
            return {}
        return {"runtime_input_plan_overrides": overrides}

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

    def _attach_execution_adapter(
        self,
        risk_engine: LadderRiskEngine,
        execution_profile: SeriesExecutionProfile,
    ) -> None:
        short_requires_borrow = execution_profile.capabilities.short_requires_borrow

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
