"""Series preparation utilities for bot runtime orchestration."""

from __future__ import annotations

import logging
from collections import deque
from copy import deepcopy
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Deque, Dict, List, Mapping, Optional, Sequence, Tuple

from ..atm import DEFAULT_ATM_TEMPLATE, merge_templates
from .domain import (
    DEFAULT_RISK,
    Candle,
    LadderRiskEngine,
    StrategySignal,
    isoformat,
    timeframe_duration,
)
from .reporting import instrument_key

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

    def reset(self) -> None:
        self._indicator_overlay_cache.clear()

    def build_series(self, strategies: Sequence[Mapping[str, Any]]) -> List[StrategySeries]:
        series_list: List[StrategySeries] = []
        for strategy in strategies:
            stream = self._build_series_for_strategy(strategy)
            series_list.append(stream)
        return series_list

    def append_series_updates(self, series: StrategySeries, start_iso: str, end_iso: str) -> bool:
        from ..candle_service import fetch_ohlcv
        from .. import strategy_service

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
            evaluation = strategy_service.evaluate(
                strategy_id=series.strategy_id,
                start=series.window_start or start_iso,
                end=end_iso,
                interval=series.timeframe,
                symbol=series.symbol,
                datasource=series.datasource,
                exchange=series.exchange,
                config={"mode": self.run_type},
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
            signals = self._build_signals_from_markers(evaluation.get("chart_markers") or {})
            while signals and signals[0].epoch <= series.last_consumed_epoch:
                signals.popleft()
            series.signals = signals
            series.window_end = end_iso
        except Exception as exc:  # pragma: no cover - defensive logging
            logger.exception(
                "bot_runtime_refresh_failed | bot=%s | strategy=%s | error=%s",
                self.bot_id,
                series.strategy_id,
                exc,
            )
        return True

    def _build_series_for_strategy(self, strategy: Mapping[str, Any]) -> StrategySeries:
        from .. import strategy_service
        from ..candle_service import fetch_ohlcv

        symbol = self._resolve_symbol(strategy)
        if not symbol:
            message = "Strategy missing symbol"
            raise RuntimeError(message)
        timeframe = self._resolve_timeframe(strategy)
        datasource = self._resolve_datasource(strategy)
        exchange = self._resolve_exchange(strategy)
        if self.run_type == "backtest":
            start_iso = self.config.get("backtest_start")
            end_iso = self.config.get("backtest_end")
            if not start_iso or not end_iso:
                start_iso, end_iso = self._resolve_live_window()
        else:
            start_iso, end_iso = self._resolve_live_window()

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
            logger.error(
                "bot_runtime_no_candles | bot=%s | strategy=%s | symbol=%s | timeframe=%s",
                self.bot_id,
                strategy.get("id"),
                symbol,
                timeframe,
            )
            raise RuntimeError(message)
        if not df.index.is_monotonic_increasing:
            first_idx = df.index[0] if len(df.index) else None
            second_idx = df.index[1] if len(df.index) > 1 else None
            logger.warning(
                "bot_runtime_unsorted_dataframe | bot=%s | strategy=%s | symbol=%s | timeframe=%s | first=%s | second=%s | rows=%s",
                self.bot_id,
                strategy.get("id"),
                symbol,
                timeframe,
                first_idx,
                second_idx,
                len(df.index),
            )

        candles = self._build_candles(df, timeframe)
        if not candles:
            raise RuntimeError("No valid candles could be built for strategy")
        if self._log_candle_sequence:
            self._log_candle_sequence("build_series", strategy.get("id"), candles)

        try:
            evaluation = strategy_service.generate_strategy_signals(
                strategy_id=strategy.get("id"),
                start=start_iso,
                end=end_iso,
                interval=timeframe,
                symbol=symbol,
                datasource=datasource,
                exchange=exchange,
                config={"mode": self.run_type},
            )
        except Exception as exc:  # pragma: no cover - defensive logging
            message = f"Strategy evaluation failed: {exc}"
            logger.exception(
                "bot_runtime_strategy_eval_failed | bot=%s | strategy=%s | error=%s",
                self.bot_id,
                strategy.get("id"),
                exc,
            )
            raise RuntimeError(message)

        overlays = self._extract_indicator_overlays(evaluation)
        signals = self._build_signals_from_markers(evaluation.get("chart_markers") or {})
        instrument = self._instrument_for(datasource, exchange, symbol)
        bot_override = self.config.get("risk") or {}
        override_payload = bot_override if bot_override and bot_override != DEFAULT_ATM_TEMPLATE else None
        atm_template = merge_templates(
            strategy.get("atm_template"),
            override_payload,
        )
        template_meta = atm_template.get("_meta") if isinstance(atm_template.get("_meta"), dict) else {}

        def _apply_instrument_field(field: str) -> None:
            if template_meta.get(f"{field}_override"):
                return
            if not instrument:
                return
            value = instrument.get(field)
            if value is None:
                return
            atm_template[field] = value

        for field_name in (
            "tick_size",
            "tick_value",
            "contract_size",
            "maker_fee_rate",
            "taker_fee_rate",
            "quote_currency",
        ):
            _apply_instrument_field(field_name)
        risk_engine = LadderRiskEngine(atm_template, instrument=instrument)
        series_meta = dict(strategy)
        if instrument:
            series_meta["instrument"] = instrument
        series_meta["atm_template"] = atm_template
        logger.info(
            "bot_runtime_series_ready | bot=%s | strategy=%s | contracts=%s | targets=%s",
            self.bot_id,
            strategy.get("id"),
            atm_template.get("contracts"),
            ",".join(str(order.get("ticks")) for order in atm_template.get("take_profit_orders", [])),
        )

        return StrategySeries(
            strategy_id=str(strategy.get("id")),
            name=strategy.get("name") or str(strategy.get("id")) or "strategy",
            symbol=symbol,
            timeframe=timeframe,
            datasource=datasource,
            exchange=exchange,
            candles=candles,
            signals=signals,
            overlays=overlays
            + self._indicator_overlay_entries(
                strategy,
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

    @staticmethod
    def _resolve_symbol(strategy: Mapping[str, Any]) -> Optional[str]:
        symbols = strategy.get("symbols") or []
        if symbols:
            return str(symbols[0])
        return strategy.get("symbol") or None

    def _resolve_timeframe(self, strategy: Mapping[str, Any]) -> str:
        return str(strategy.get("timeframe") or self.config.get("timeframe") or "15m")

    def _resolve_datasource(self, strategy: Mapping[str, Any]) -> Optional[str]:
        return self.config.get("datasource") or strategy.get("datasource")

    def _resolve_exchange(self, strategy: Mapping[str, Any]) -> Optional[str]:
        return self.config.get("exchange") or strategy.get("exchange")

    def _instrument_for(
        self,
        datasource: Optional[str],
        exchange: Optional[str],
        symbol: Optional[str],
    ) -> Optional[Dict[str, Any]]:
        index = self.config.get("instrument_index") or {}
        if not symbol:
            return None
        keys = [
            instrument_key(datasource, exchange, symbol),
            instrument_key(datasource, None, symbol),
            instrument_key(None, exchange, symbol),
            instrument_key(None, None, symbol),
        ]
        for key in keys:
            if key in index:
                return index[key]
        return None

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
        from .. import indicator_service

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
            snapshot = dict(link.get("indicator_snapshot") or {})
            params = dict(snapshot.get("params") or {})
            indicator_type = snapshot.get("type") or link.get("indicator_type") or "indicator"
            color = snapshot.get("color") or link.get("color")
            window_symbol = symbol or params.get("symbol")
            interval = params.get("interval") or timeframe or self.config.get("timeframe") or "15m"
            ds = link.get("datasource") or snapshot.get("datasource") or params.get("datasource") or datasource
            ex = link.get("exchange") or snapshot.get("exchange") or params.get("exchange") or exchange
            cache_key = self._indicator_overlay_cache_key(indicator_id, start_iso, end_iso, interval, window_symbol, ds, ex)
            cached = self._indicator_overlay_cache.get(cache_key)
            if cached:
                overlays.append(deepcopy(cached))
                continue
            try:
                payload = indicator_service.overlays_for_instance(
                    indicator_id,
                    start=start_iso,
                    end=end_iso,
                    interval=str(interval),
                    symbol=window_symbol,
                    datasource=ds,
                    exchange=ex,
                )
            except Exception as exc:  # pragma: no cover - defensive logging
                logger.debug(
                    "bot_indicator_overlay_failed | bot=%s | strategy=%s | indicator=%s | error=%s",
                    self.bot_id,
                    strategy_meta.get("id"),
                    indicator_id,
                    exc,
                )
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
    ) -> str:
        parts = [
            indicator_id or "",
            start_iso or "",
            end_iso or "",
            str(interval or ""),
            (symbol or "").upper(),
            (datasource or "").lower(),
            (exchange or "").lower(),
        ]
        return ":".join(parts)

    @staticmethod
    def _extract_indicator_overlays(result: Mapping[str, Any]) -> List[Dict[str, Any]]:
        # Indicator results include overlays that visualize raw signal markers.
        # The bot lens should only render the strategy's configured indicator
        # overlays, so skip signal-driven visuals entirely.
        return []

    @staticmethod
    def _build_signals_from_markers(markers: Mapping[str, Any]) -> Deque[StrategySignal]:
        queued: List[StrategySignal] = []
        for entry in markers.get("buy", []) or []:
            epoch = SeriesBuilder._normalise_epoch(entry.get("time"))
            if epoch is not None:
                queued.append(StrategySignal(epoch=epoch, direction="long"))
        for entry in markers.get("sell", []) or []:
            epoch = SeriesBuilder._normalise_epoch(entry.get("time"))
            if epoch is not None:
                queued.append(StrategySignal(epoch=epoch, direction="short"))
        queued.sort(key=lambda signal: signal.epoch)
        return deque(queued)

    @staticmethod
    def _normalise_epoch(value: Any) -> Optional[int]:
        if value in (None, ""):
            return None
        if isinstance(value, (int, float)):
            return int(value)
        text = str(value).strip()
        if not text:
            return None
        if text.isdigit():
            return int(text)
        try:
            return int(float(text))
        except (TypeError, ValueError):
            pass
        try:
            if text.endswith("Z"):
                text = text[:-1]
            parsed = datetime.fromisoformat(text)
            return int(parsed.timestamp())
        except ValueError:
            return None

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
        bot_risk = self.config.get("risk") or DEFAULT_RISK
        strategy_risk = strategy.get("risk") or {}
        return merge_templates(DEFAULT_ATM_TEMPLATE, bot_risk, strategy_risk)
