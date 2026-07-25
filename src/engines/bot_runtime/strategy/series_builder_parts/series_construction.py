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

from core.candle_continuity import expected_interval_seconds, summarize_candle_continuity
from core.candle_snapshot import build_candle_series_snapshot
from engines.bot_runtime.core.domain import (
    Candle,
    LadderRiskEngine,
    StrategySignal,
    isoformat,
    timeframe_duration,
)
from engines.bot_runtime.core.domain.candle_factory import build_candles_from_dataframe
from engines.bot_runtime.adapters import BacktestAdapter, LiveAdapter, PaperAdapter
from engines.bot_runtime.core.execution_profile import (
    SeriesExecutionProfile,
    compile_series_execution_profile,
    normalize_execution_semantics,
    normalize_runtime_instrument_type,
)
from atm import normalise_template
from risk import normalise_risk_config
from strategies.compiler import compile_strategy
from utils.log_context import build_log_context, with_log_context

from ..models import Strategy
from .models import StrategySeries

logger = logging.getLogger(__name__)

class SeriesBuilderConstructionMixin:
    @staticmethod
    def _runtime_series_candle_continuity(
        candles: Sequence[Candle],
        *,
        timeframe: str,
        gap_classification: Any = None,
    ):
        return summarize_candle_continuity(
            (candle.to_dict() for candle in candles),
            expected_interval_seconds_value=expected_interval_seconds(timeframe=timeframe),
            gap_classification=gap_classification,
        )

    @staticmethod
    def _build_signals_from_decision_artifacts(artifacts: Sequence[Mapping[str, Any]]) -> Deque[StrategySignal]:
        queued: List[StrategySignal] = []
        for artifact in artifacts:
            if not isinstance(artifact, Mapping):
                continue
            if str(artifact.get("evaluation_result") or "") != "matched_selected":
                continue
            queued.append(StrategySignal.from_decision_artifact(artifact))
        queued.sort(key=lambda signal: signal.epoch)
        return deque(queued)

    @staticmethod
    def _build_candles(df: Any, timeframe: Optional[str] = None) -> List[Candle]:
        return build_candles_from_dataframe(df, timeframe=timeframe)

    def _build_atm_template(self, strategy: Strategy) -> Dict[str, Any]:
        """Return the canonical strategy-owned execution-policy template."""

        return normalise_template(strategy.atm_template)

    @staticmethod
    def _has_proxy_derivative_reference(instrument: Mapping[str, Any]) -> bool:
        metadata = instrument.get("metadata") if isinstance(instrument.get("metadata"), Mapping) else {}
        fields = metadata.get("instrument_fields") if isinstance(metadata.get("instrument_fields"), Mapping) else {}
        execution_fields = fields.get("proxy_derivative_instrument_fields")
        margin_rates = fields.get("proxy_derivative_margin_rates")
        return isinstance(execution_fields, Mapping) and bool(execution_fields) and isinstance(margin_rates, Mapping) and bool(margin_rates)

    @staticmethod
    def _build_risk_config_for_instrument(strategy: Strategy, symbol: str, multiplier: float) -> Dict[str, Any]:
        """Resolve concrete risk config for one instrument series."""

        risk_config = normalise_risk_config(getattr(strategy, "risk_config", {}) or {})
        if multiplier != 1.0:
            risk_config["instrument_risk_multiplier"] = float(multiplier)
        else:
            risk_config.pop("instrument_risk_multiplier", None)
        risk_config["instrument_symbol"] = symbol
        return risk_config

    def _execution_semantics_for_instrument(self, instrument: Mapping[str, Any]) -> Optional[str]:
        """Resolve the runtime execution model for a source instrument."""

        instrument_type = normalize_runtime_instrument_type(instrument.get("instrument_type"))
        configured = self.config.get("execution_semantics")
        if configured:
            execution_semantics = normalize_execution_semantics(configured, instrument_type=instrument_type)
            if execution_semantics == "proxy_derivative" and self.run_type != "backtest":
                raise RuntimeError("proxy_derivative execution is currently supported for backtest runs only")
            return execution_semantics

        risk = self.config.get("risk")
        if isinstance(risk, Mapping) and risk.get("execution_semantics"):
            execution_semantics = normalize_execution_semantics(risk.get("execution_semantics"), instrument_type=instrument_type)
            if execution_semantics == "proxy_derivative" and self.run_type != "backtest":
                raise RuntimeError("proxy_derivative execution is currently supported for backtest runs only")
            return execution_semantics

        if self.run_type == "backtest" and instrument_type == "spot" and self._has_proxy_derivative_reference(instrument):
            return "proxy_derivative"
        return normalize_execution_semantics(None, instrument_type=instrument_type)

    def _instrument_for_link(self, strategy: Strategy, instrument_link: Any) -> Optional[Dict[str, Any]]:
        """Resolve the canonical instrument for a strategy link."""

        snapshot = dict(getattr(instrument_link, "instrument_snapshot", {}) or {})
        instrument_id = str(getattr(instrument_link, "instrument_id", "") or snapshot.get("id") or "").strip()
        if instrument_id:
            get_instrument_record = getattr(self._deps, "get_instrument_record", None)
            if get_instrument_record is not None:
                try:
                    record = get_instrument_record(instrument_id)
                    if record:
                        return {**snapshot, **dict(record)}
                except Exception:
                    logger.warning(
                        with_log_context(
                            "series_instrument_lookup_failed",
                            self._strategy_log_context(
                                strategy,
                                instrument_id=instrument_id,
                                symbol=snapshot.get("symbol"),
                            ),
                        )
                    )
            if snapshot:
                snapshot.setdefault("id", instrument_id)
                return snapshot

        symbol = str(snapshot.get("symbol") or getattr(instrument_link, "symbol", "") or "").strip()
        if not symbol:
            return None
        return self._instrument_for(strategy.datasource, strategy.exchange, symbol)

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
        failures: List[Tuple[str, str, Exception]] = []

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
                        failures.append(
                            (
                                str(getattr(instrument_link, "instrument_id", "") or ""),
                                str(getattr(instrument_link, "symbol", "") or ""),
                                exc,
                            )
                        )
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

        if failures:
            failures.sort(key=lambda row: (row[0], row[1], type(row[2]).__name__, str(row[2])))
            details = "; ".join(
                f"instrument_id={instrument_id or '<missing>'} "
                f"symbol={symbol or '<missing>'} "
                f"error={type(exc).__name__}: {exc}"
                for instrument_id, symbol, exc in failures
            )
            raise RuntimeError(
                f"Strategy {strategy.id} failed to build {len(failures)} of "
                f"{len(eligible_links)} eligible series: {details}"
            ) from failures[0][2]

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
        # Step 1: Resolve strategy metadata from the canonical instrument link.
        instrument_id = instrument_link.instrument_id
        if not instrument_id:
            raise RuntimeError(f"Instrument link for strategy {strategy.id} missing instrument_id")
        instrument = self._instrument_for_link(strategy, instrument_link)
        if instrument and instrument.get("instrument_snapshot"):
            instrument = instrument.get("instrument_snapshot")
        symbol = str((instrument or {}).get("symbol") or instrument_link.symbol or "").strip()
        if not symbol:
            raise RuntimeError(f"Instrument link for strategy {strategy.id} missing symbol")

        timeframe = strategy.timeframe
        datasource = (instrument or {}).get("datasource") or strategy.datasource
        exchange = (instrument or {}).get("exchange") or strategy.exchange

        # Extract risk multiplier for this instrument
        risk_multiplier = instrument_link.risk_multiplier or 1.0

        # Determine time window. Backtests now seed with bounded warmup and then execute walk-forward event-by-event.
        replay_start_index = 0
        window_start_iso: Optional[str] = None
        backtest_warmup_evidence: Optional[Dict[str, Any]] = None
        if self.run_type == "backtest":
            configured_start = self.config.get("backtest_start")
            configured_end = self.config.get("backtest_end")
            if not configured_start or not configured_end:
                raise RuntimeError("Backtest runtime requires both backtest_start and backtest_end")
            start_iso = str(configured_start)
            end_iso = str(configured_end)
            indicator_warmup_requirements = (
                self._indicator_warmup_requirements(strategy)
            )
            warmup_bars = self._resolve_backtest_warmup_bars(
                strategy,
                timeframe,
                indicator_requirements=indicator_warmup_requirements,
            )
            required_warmup_bars = max(
                [
                    100,
                    *[
                        int(row["required_bars"])
                        for row in indicator_warmup_requirements
                    ],
                ]
            )
            (
                candles,
                replay_start_index,
                window_start_iso,
                candle_gap_classification,
                backtest_warmup_evidence,
            ) = self._build_backtest_candles_with_warmup(
                symbol=symbol,
                timeframe=timeframe,
                datasource=datasource,
                exchange=exchange,
                strategy_id=strategy.id,
                instrument_id=instrument_id,
                backtest_start_iso=start_iso,
                backtest_end_iso=end_iso,
                warmup_bars=warmup_bars,
                required_warmup_bars=required_warmup_bars,
                indicator_warmup_requirements=indicator_warmup_requirements,
            )
        else:
            start_iso, end_iso = self._resolve_live_window()
            window_start_iso = start_iso
            # Paper/live placeholders still use the same event-driven runtime semantics.
            df = self._fetch_ohlcv_data(
                symbol,
                start_iso,
                end_iso,
                timeframe,
                datasource,
                exchange,
                strategy.id,
                instrument_id=instrument_id,
            )
            candles = self._build_candles(df, timeframe)
            candle_gap_classification = df.attrs.get("gap_classification") if hasattr(df, "attrs") else None
            if self.run_type == "paper" and bool(self.config.get("paper_live_market_data")):
                # Streaming paper uses historical candles only to warm indicator state.
                # Entries/exits start with candles closed after the run begins.
                replay_start_index = len(candles)
        if not candles:
            raise RuntimeError(f"No valid candles could be built for strategy {strategy.id}")
        if self._log_candle_sequence:
            self._log_candle_sequence("build_series", strategy.id, candles)

        instrument_context = self._strategy_log_context(
            strategy,
            symbol=symbol,
            instrument_id=instrument.get("id") if isinstance(instrument, dict) else None,
        )
        logger.debug(with_log_context("series_instrument_resolved", instrument_context))

        atm_template = self._build_atm_template(strategy)
        risk_config = self._build_risk_config_for_instrument(strategy, symbol, risk_multiplier)

        if risk_multiplier != 1.0:
            context = self._strategy_log_context(
                strategy,
                symbol=symbol,
                multiplier=risk_multiplier,
            )
            logger.info(with_log_context("risk_multiplier_applied", context))

        # Step 5: Create risk engine and assemble series
        execution_semantics = self._execution_semantics_for_instrument(instrument or {})
        execution_profile = compile_series_execution_profile(
            instrument or {},
            risk_config=risk_config,
            require_margin_accounting=execution_semantics in {"derivative", "proxy_derivative"},
            execution_semantics=execution_semantics,
        )
        profile_context = self._strategy_log_context(
            strategy,
            symbol=symbol,
            instrument_type=execution_profile.instrument.instrument_type,
            source_instrument_type=execution_profile.instrument.source_instrument_type,
            execution_semantics=execution_profile.instrument.execution_semantics,
            research_market_role=execution_profile.instrument.research_market_role,
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
            risk_config=risk_config,
        )
        risk_engine.set_runtime_context(
            strategy_id=strategy.id,
            strategy_name=strategy.name,
            timeframe=timeframe,
            datasource=datasource,
            exchange=exchange,
            symbol=symbol,
            instrument_id=(instrument.get("id") if isinstance(instrument, dict) else None) or instrument_id,
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
            series_meta["execution_semantics"] = execution_profile.instrument.execution_semantics
            series_meta["source_instrument_type"] = execution_profile.instrument.source_instrument_type
            series_meta["research_market_role"] = execution_profile.instrument.research_market_role
        series_meta["atm_template"] = atm_template
        series_meta["risk_config"] = deepcopy(risk_config)
        if backtest_warmup_evidence is not None:
            series_meta["backtest_warmup"] = backtest_warmup_evidence
        if candle_gap_classification:
            series_meta["candle_gap_classification"] = candle_gap_classification
        continuity_summary = self._runtime_series_candle_continuity(
            candles,
            timeframe=timeframe,
            gap_classification=candle_gap_classification,
        )
        series_meta["candle_continuity"] = continuity_summary.to_dict()
        series_meta["candle_snapshot"] = build_candle_series_snapshot(
            candles,
            instrument_id=(instrument.get("id") if isinstance(instrument, dict) else None)
            or instrument_id,
            symbol=symbol,
            timeframe=timeframe,
            datasource=datasource,
            exchange=exchange,
            strategy_id=strategy.id,
            replay_start_index=replay_start_index,
        )
        if continuity_summary.detected_gap_count:
            logger.warning(
                with_log_context(
                    "runtime_series_candle_gap_detected",
                    self._strategy_log_context(
                        strategy,
                        symbol=symbol,
                        timeframe=timeframe,
                        gaps=continuity_summary.detected_gap_count,
                        missing_candles=continuity_summary.missing_candle_estimate,
                        gap_count_by_type=continuity_summary.gap_count_by_type,
                    ),
                )
            )

        ready_context = self._strategy_log_context(
            strategy,
            symbol=symbol,
            target_count=len(atm_template.get("take_profit_orders", [])),
            target_ids=",".join(str(order.get("id")) for order in atm_template.get("take_profit_orders", [])),
        )
        logger.info(with_log_context("bot_runtime_series_ready", ready_context))

        # No precomputed signals or visual projections in runtime path.
        # Signals are evaluated incrementally per bar; BotLens overlays are
        # projected from indicator snapshots on the bounded projection cadence.
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
        required_warmup_bars: Optional[int] = None,
        indicator_warmup_requirements: Sequence[Mapping[str, Any]] = (),
        instrument_id: Optional[str] = None,
    ) -> Tuple[List[Candle], int, str, Optional[Any], Dict[str, Any]]:
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
        if isinstance(warmup_bars, bool):
            raise RuntimeError("backtest warmup bars must be a positive integer")
        try:
            safe_warmup_bars = int(warmup_bars)
        except (TypeError, ValueError) as exc:
            raise RuntimeError("backtest warmup bars must be a positive integer") from exc
        if safe_warmup_bars <= 0:
            raise RuntimeError("backtest warmup bars must be a positive integer")
        required_bars = int(required_warmup_bars or safe_warmup_bars)
        if required_bars <= 0:
            raise RuntimeError("required backtest warmup bars must be positive")
        warmup_start_ts = start_ts - (tf_delta * safe_warmup_bars)

        gap_classification: List[Any] = []
        warmup_df = self._fetch_ohlcv_data(
            symbol=symbol,
            start_iso=isoformat(warmup_start_ts.to_pydatetime()),
            end_iso=backtest_start_iso,
            timeframe=timeframe,
            datasource=datasource,
            exchange=exchange,
            strategy_id=strategy_id,
            instrument_id=instrument_id,
        )
        warmup_candles = [
            candle
            for candle in self._build_candles(warmup_df, timeframe)
            if candle.time < start_ts.to_pydatetime()
        ]
        if getattr(warmup_df, "attrs", {}).get("gap_classification"):
            gap_classification.extend(warmup_df.attrs["gap_classification"])
        if len(warmup_candles) > safe_warmup_bars:
            warmup_candles = warmup_candles[-safe_warmup_bars:]
        loaded_warmup_bars = len(warmup_candles)
        warmup_evidence = {
            "schema_version": "backtest_warmup_evidence.v1",
            "status": (
                "ready"
                if loaded_warmup_bars >= required_bars
                else "insufficient"
            ),
            "requested_bars": safe_warmup_bars,
            "required_bars": required_bars,
            "loaded_bars": loaded_warmup_bars,
            "missing_bars": max(required_bars - loaded_warmup_bars, 0),
            "request_satisfies_requirements": safe_warmup_bars >= required_bars,
            "indicator_requirements": [
                dict(row) for row in indicator_warmup_requirements
            ],
            "requested_range": {
                "start": isoformat(warmup_start_ts.to_pydatetime()),
                "end_exclusive": backtest_start_iso,
            },
            "loaded_range": {
                "start": (
                    isoformat(warmup_candles[0].time)
                    if warmup_candles
                    else None
                ),
                "end": (
                    isoformat(warmup_candles[-1].time)
                    if warmup_candles
                    else None
                ),
            },
        }

        replay_df = self._fetch_ohlcv_data(
            symbol=symbol,
            start_iso=backtest_start_iso,
            end_iso=backtest_end_iso,
            timeframe=timeframe,
            datasource=datasource,
            exchange=exchange,
            strategy_id=strategy_id,
            instrument_id=instrument_id,
        )
        replay_candles = [
            candle for candle in self._build_candles(replay_df, timeframe)
            if candle.time >= start_ts.to_pydatetime() and candle.time <= end_ts.to_pydatetime()
        ]
        if getattr(replay_df, "attrs", {}).get("gap_classification"):
            gap_classification.extend(replay_df.attrs["gap_classification"])
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
        warmup_evidence["replay_start_index"] = replay_start_index
        return (
            ordered,
            replay_start_index,
            isoformat(warmup_start_ts.to_pydatetime()),
            gap_classification or None,
            warmup_evidence,
        )

    def _indicator_warmup_requirements(
        self,
        strategy: Strategy,
    ) -> List[Dict[str, Any]]:
        requirements: List[Dict[str, Any]] = []
        seen: set[str] = set()
        for indicator_id in strategy.indicator_ids:
            normalized_id = str(indicator_id or "").strip()
            if not normalized_id or normalized_id in seen:
                continue
            seen.add(normalized_id)
            try:
                meta = self._deps.indicator_get_instance_meta(
                    normalized_id,
                    ctx=self._indicator_ctx,
                )
            except Exception as exc:
                raise RuntimeError(
                    "Indicator warmup metadata failed for "
                    f"strategy={strategy.id} indicator={normalized_id}: {exc}"
                ) from exc
            if not isinstance(meta, Mapping):
                raise RuntimeError(
                    "Indicator warmup metadata must be a mapping for "
                    f"strategy={strategy.id} indicator={normalized_id}"
                )
            params = meta.get("params")
            if not isinstance(params, Mapping) or "warmup_bars" not in params:
                continue
            raw_bars = params.get("warmup_bars")
            if isinstance(raw_bars, bool):
                raise RuntimeError(
                    "Indicator warmup_bars must be a positive integer for "
                    f"strategy={strategy.id} indicator={normalized_id}"
                )
            try:
                required_bars = int(raw_bars)
            except (TypeError, ValueError) as exc:
                raise RuntimeError(
                    "Indicator warmup_bars must be a positive integer for "
                    f"strategy={strategy.id} indicator={normalized_id}"
                ) from exc
            if required_bars <= 0:
                raise RuntimeError(
                    "Indicator warmup_bars must be a positive integer for "
                    f"strategy={strategy.id} indicator={normalized_id}"
                )
            requirements.append(
                {
                    "indicator_id": normalized_id,
                    "indicator_type": meta.get("type"),
                    "required_bars": required_bars,
                }
            )
        return requirements

    def _resolve_backtest_warmup_bars(
        self,
        strategy: Strategy,
        timeframe: str,
        *,
        indicator_requirements: Sequence[Mapping[str, Any]] = (),
    ) -> int:
        # Strategy/runtime warmup is intentionally separate from indicator-
        # specific fetch windows (e.g. indicator days_back settings).
        _ = strategy, timeframe
        default_bars = max(
            [
                100,
                *[
                    int(row.get("required_bars") or 0)
                    for row in indicator_requirements
                ],
            ]
        )
        configured = self.config.get("backtest_warmup_bars")
        if configured is None:
            return default_bars
        if isinstance(configured, bool):
            raise ValueError("backtest_warmup_bars must be a positive integer")
        try:
            parsed = int(configured)
        except (TypeError, ValueError) as exc:
            raise ValueError(
                "backtest_warmup_bars must be a positive integer"
            ) from exc
        if parsed <= 0:
            raise ValueError("backtest_warmup_bars must be a positive integer")
        return parsed

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
                raise RuntimeError(
                    "Indicator runtime link is missing indicator_id for "
                    f"strategy={series.strategy_id} symbol={series.symbol} "
                    f"timeframe={series.timeframe}"
                )
            try:
                self._deps.indicator_get_instance_meta(
                    indicator_id,
                    ctx=self._indicator_ctx,
                )
                runtime_plan = self._deps.indicator_runtime_input_plan_for_instance(
                    indicator_id,
                    strategy_interval=str(series.timeframe),
                    start=start_iso,
                    end=end_iso,
                    ctx=self._indicator_ctx,
                )
            except Exception as exc:
                raise RuntimeError(
                    "Indicator runtime input plan failed for "
                    f"strategy={series.strategy_id} indicator={indicator_id} "
                    f"symbol={series.symbol} timeframe={series.timeframe}: {exc}"
                ) from exc
            if not isinstance(runtime_plan, Mapping):
                raise RuntimeError(
                    "Indicator runtime input plan must be a mapping for "
                    f"strategy={series.strategy_id} indicator={indicator_id} "
                    f"symbol={series.symbol} timeframe={series.timeframe}"
                )
            if not bool(runtime_plan.get("incremental_eval", False)):
                continue
            source_timeframe = str(runtime_plan.get("source_timeframe") or series.timeframe)
            override_start = str(runtime_plan.get("start") or start_iso)
            source_delta = timeframe_duration(source_timeframe)
            source_seconds = int(source_delta.total_seconds()) if source_delta else 0
            if source_seconds <= 0:
                raise RuntimeError(
                    "Indicator runtime input plan has unsupported source_timeframe "
                    f"'{source_timeframe}' for strategy={series.strategy_id} "
                    f"indicator={indicator_id} symbol={series.symbol}"
                )
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
        constraints = execution_profile.constraints

        adapter = self._adapter_for_run_type(
            short_requires_borrow=bool(short_requires_borrow),
            tick_size=constraints.tick_size,
            qty_step=constraints.qty_step,
            min_qty=constraints.min_order_size,
            min_notional=constraints.min_notional,
            contract_size=constraints.contract_size,
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
