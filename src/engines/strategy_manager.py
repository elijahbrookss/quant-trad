"""Session manager for orchestrating multi-indicator strategies."""

from __future__ import annotations

import inspect
import json
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, Iterable, List, Mapping, MutableMapping, Optional, Sequence, Set, Tuple

import pandas as pd

from core.logger import logger
from data_providers.base_provider import BaseDataProvider
from data_providers.factory import get_provider
from engines.strategy_engine import StrategyContext, StrategyEngine
from indicators.base import BaseIndicator
from indicators.config import DataContext


IndicatorFactory = Callable[..., Optional[BaseIndicator]]
ChartHook = Callable[..., Optional[Any]]


@dataclass(frozen=True)
class TimeframeSpec:
    """Configuration describing a single OHLCV timeframe request."""

    start: str
    end: str
    interval: str
    datasource: Optional[str] = None
    exchange: Optional[str] = None

    def build_context(self, symbol: str) -> DataContext:
        """Create a :class:`DataContext` for the supplied symbol."""

        return DataContext(symbol=symbol, start=self.start, end=self.end, interval=self.interval)


@dataclass(frozen=True)
class StrategyConfig:
    """High-level configuration for a single trading strategy."""

    strategy_id: str
    symbols: Sequence[str]
    primary_timeframe: str
    timeframes: Mapping[str, TimeframeSpec]
    indicator_factories: Sequence[IndicatorFactory]
    chart_hooks: Sequence[ChartHook]
    datasource: Optional[str] = None
    exchange: Optional[str] = None
    engine_kwargs: Mapping[str, Any] = field(default_factory=dict)


def _normalise_for_hash(value: Any) -> Any:
    """Normalise arbitrary payloads so they can be hashed deterministically."""

    if isinstance(value, Mapping):
        return {key: _normalise_for_hash(value[key]) for key in sorted(value)}
    if isinstance(value, (list, tuple)):
        return [_normalise_for_hash(v) for v in value]
    if isinstance(value, set):
        return sorted(_normalise_for_hash(v) for v in value)
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    return value


def _hash_payload(value: Any) -> str:
    """Return a stable hash for dictionaries/lists used in overlay emission."""

    normalised = _normalise_for_hash(value)
    return json.dumps(normalised, sort_keys=True, default=str)


class StrategySession:
    """Runtime state for a single strategy + symbol pair."""

    def __init__(self, config: StrategyConfig, symbol: str) -> None:
        self.config = config
        self.symbol = symbol
        self.timeframe_data: Dict[str, pd.DataFrame] = {}
        self.contexts: Dict[str, DataContext] = {}
        self.providers: Dict[str, BaseDataProvider] = {}
        self.primary_frame: pd.DataFrame = pd.DataFrame()
        self.engine_frame: pd.DataFrame = pd.DataFrame()
        self.bar_index: int = 0
        self.total_bars: int = 0
        self.finished: bool = False
        self._overlay_hashes: Set[str] = set()
        self._marker_hashes: Set[str] = set()
        self._hook_state: MutableMapping[int, Dict[str, Any]] = {}

        self._initialise_timeframes()

    # ------------------------------------------------------------------ #
    def _initialise_timeframes(self) -> None:
        """Load OHLCV windows for every configured timeframe."""

        if self.config.primary_timeframe not in self.config.timeframes:
            raise ValueError(
                f"Primary timeframe '{self.config.primary_timeframe}' missing for strategy "
                f"{self.config.strategy_id}"
            )

        for name, spec in self.config.timeframes.items():
            datasource = spec.datasource or self.config.datasource
            exchange = spec.exchange or self.config.exchange
            provider = get_provider(datasource, exchange=exchange)
            ctx = spec.build_context(self.symbol)
            df = provider.get_ohlcv(ctx)
            if df is None or df.empty:
                logger.warning(
                    "No OHLCV data fetched | strategy=%s symbol=%s timeframe=%s",
                    self.config.strategy_id,
                    self.symbol,
                    name,
                )
                df = pd.DataFrame()
            else:
                df = df.sort_index()

            self.timeframe_data[name] = df
            self.contexts[name] = ctx
            self.providers[name] = provider

        self.primary_frame = self.timeframe_data.get(self.config.primary_timeframe, pd.DataFrame())
        self.engine_frame = self._prepare_engine_frame(self.primary_frame)
        self.total_bars = len(self.engine_frame)
        self.finished = self.total_bars == 0

        logger.info(
            "Initialised strategy session | strategy=%s symbol=%s bars=%d",
            self.config.strategy_id,
            self.symbol,
            self.total_bars,
        )

    # ------------------------------------------------------------------ #
    @staticmethod
    def _prepare_engine_frame(df: pd.DataFrame) -> pd.DataFrame:
        """Return a DataFrame with OHLC column names formatted for the engine."""

        if df is None or df.empty:
            return pd.DataFrame()

        rename_map = {
            "open": "Open",
            "high": "High",
            "low": "Low",
            "close": "Close",
            "volume": "Volume",
        }
        cols = {col: rename_map[col] for col in rename_map if col in df.columns}
        return df.rename(columns=cols)

    # ------------------------------------------------------------------ #
    def get_timeframe(self, name: str) -> pd.DataFrame:
        """Return the cached OHLCV data for the requested timeframe."""

        return self.timeframe_data.get(name, pd.DataFrame())

    # ------------------------------------------------------------------ #
    def get_context(self, name: str) -> Optional[DataContext]:
        """Return the :class:`DataContext` for the given timeframe alias."""

        return self.contexts.get(name)

    # ------------------------------------------------------------------ #
    def get_provider(self, name: str) -> Optional[BaseDataProvider]:
        """Return the provider servicing the requested timeframe."""

        return self.providers.get(name)

    # ------------------------------------------------------------------ #
    def _call_with_kwargs(
        self,
        func: Callable[..., Any],
        kwargs: Mapping[str, Any],
        fallback: Optional[Any] = None,
    ) -> Any:
        """Invoke *func* with only the keyword arguments it accepts."""

        try:
            signature = inspect.signature(func)
        except (TypeError, ValueError):
            signature = None

        if signature is None:
            if fallback is not None:
                return func(fallback)
            return func(**kwargs)  # type: ignore[arg-type]

        accepts_kwargs = any(param.kind == inspect.Parameter.VAR_KEYWORD for param in signature.parameters.values())

        call_kwargs: Dict[str, Any] = {}
        for name, param in signature.parameters.items():
            if name == "self" or param.kind in {inspect.Parameter.VAR_POSITIONAL}:
                continue
            if name in kwargs:
                call_kwargs[name] = kwargs[name]

        try:
            if accepts_kwargs:
                return func(**{**kwargs, **call_kwargs})
            return func(**call_kwargs)
        except TypeError as exc:
            if fallback is None:
                raise
            logger.debug(
                "Callable %s rejected kwargs (%s). Falling back to positional call.",
                getattr(func, "__name__", repr(func)),
                exc,
            )
            return func(fallback)

    # ------------------------------------------------------------------ #
    def _build_indicators(self, window_data: Mapping[str, pd.DataFrame]) -> List[BaseIndicator]:
        """Initialise indicators using the configured factories."""

        indicators: List[BaseIndicator] = []
        shared_kwargs = {
            "symbol": self.symbol,
            "strategy_id": self.config.strategy_id,
            "timeframes": window_data,
            "market_data": window_data,
            "data": window_data,
            "contexts": self.contexts,
            "providers": self.providers,
            "primary_timeframe": self.config.primary_timeframe,
        }

        for factory in self.config.indicator_factories:
            try:
                indicator = self._call_with_kwargs(factory, shared_kwargs, fallback=window_data)
            except Exception:
                logger.exception(
                    "Indicator factory failed | strategy=%s symbol=%s factory=%s",
                    self.config.strategy_id,
                    self.symbol,
                    getattr(factory, "__name__", repr(factory)),
                )
                continue

            if not isinstance(indicator, BaseIndicator):
                continue

            if not getattr(indicator, "symbol", None):
                setattr(indicator, "symbol", self.symbol)

            indicators.append(indicator)

        return indicators

    # ------------------------------------------------------------------ #
    def _invoke_chart_hooks(
        self,
        indicators: Sequence[BaseIndicator],
        engine: StrategyEngine,
        engine_output: pd.DataFrame,
        window_data: Mapping[str, pd.DataFrame],
        bar_timestamp: Optional[pd.Timestamp],
        strategy_context: StrategyContext,
    ) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], Set[Tuple[str, str]]]:
        """Execute registered chart hooks and collect overlay payloads."""

        overlays: List[Dict[str, Any]] = []
        markers: List[Dict[str, Any]] = []
        legend_entries: Set[Tuple[str, str]] = set()

        hook_kwargs = {
            "symbol": self.symbol,
            "strategy_id": self.config.strategy_id,
            "timeframes": window_data,
            "market_data": window_data,
            "data": window_data,
            "contexts": self.contexts,
            "providers": self.providers,
            "indicators": indicators,
            "engine": engine,
            "engine_output": engine_output,
            "bar_index": max(self.bar_index - 1, 0),
            "bar_timestamp": bar_timestamp,
            "primary_timeframe": self.config.primary_timeframe,
            "strategy_context": strategy_context,
        }

        for idx, hook in enumerate(self.config.chart_hooks):
            state = self._hook_state.setdefault(idx, {})
            hook_kwargs["state"] = state
            try:
                result = self._call_with_kwargs(hook, hook_kwargs)
            except Exception:
                logger.exception(
                    "Chart hook failed | strategy=%s symbol=%s hook=%s",
                    self.config.strategy_id,
                    self.symbol,
                    getattr(hook, "__name__", repr(hook)),
                )
                continue

            if result is None:
                continue

            if isinstance(result, Mapping):
                overlays.extend(result.get("overlays", []) or [])
                markers.extend(result.get("markers", []) or [])
                legend = result.get("legend_entries") or result.get("legend") or []
                legend_entries.update({tuple(entry) for entry in legend})
            elif isinstance(result, Iterable) and not isinstance(result, (str, bytes)):
                overlays.extend(result)  # type: ignore[arg-type]

        return overlays, markers, legend_entries

    # ------------------------------------------------------------------ #
    def step(self) -> Optional[Dict[str, Any]]:
        """Advance the session by one bar and return new overlay payloads."""

        if self.finished:
            return None

        self.bar_index += 1
        window_size = min(self.bar_index, self.total_bars)
        engine_slice = self.engine_frame.iloc[:window_size]

        window_data = {name: df.iloc[:window_size].copy() for name, df in self.timeframe_data.items() if not df.empty}
        indicators = self._build_indicators(window_data)

        engine = StrategyEngine(indicators, **self.config.engine_kwargs)
        strategy_context = StrategyContext(
            strategy_id=self.config.strategy_id,
            symbol=self.symbol,
            timeframe=self.config.primary_timeframe,
        )
        if not engine_slice.empty:
            engine_output, engine_markers = engine.run(
                engine_slice,
                context=strategy_context,
                additional_frames=window_data,
            )
        else:
            engine_output, engine_markers = pd.DataFrame(), []
        bar_timestamp = engine_slice.index[-1] if not engine_slice.empty else None

        overlays, markers, legend_entries = self._invoke_chart_hooks(
            indicators,
            engine,
            engine_output,
            window_data,
            bar_timestamp,
            strategy_context,
        )

        combined_markers = engine_markers + markers
        enriched_markers = []
        for marker in combined_markers:
            payload = dict(marker)
            payload.setdefault("strategy_id", self.config.strategy_id)
            payload.setdefault("symbol", self.symbol)
            payload.setdefault("timeframe", self.config.primary_timeframe)
            enriched_markers.append(payload)

        unique_overlays = self._dedupe(overlays, self._overlay_hashes)
        unique_markers = self._dedupe(enriched_markers, self._marker_hashes)

        if self.bar_index >= self.total_bars:
            self.finished = True

        return {
            "strategy_id": self.config.strategy_id,
            "symbol": self.symbol,
            "bar_timestamp": bar_timestamp,
            "overlays": unique_overlays,
            "markers": unique_markers,
            "legend_entries": legend_entries,
            "engine_output": engine_output,
        }

    # ------------------------------------------------------------------ #
    @staticmethod
    def _dedupe(items: Iterable[Mapping[str, Any]], seen: Set[str]) -> List[Dict[str, Any]]:
        """Filter out payloads that were previously emitted."""

        fresh: List[Dict[str, Any]] = []
        for item in items or []:
            key = _hash_payload(item)
            if key in seen:
                continue
            seen.add(key)
            fresh.append(dict(item))
        return fresh


class StrategySessionManager:
    """Coordinate multiple strategy sessions and aggregate overlays."""

    def __init__(self, configs: Sequence[StrategyConfig]) -> None:
        self.sessions: List[StrategySession] = []
        for config in configs:
            for symbol in config.symbols:
                try:
                    session = StrategySession(config, symbol)
                except Exception:
                    logger.exception(
                        "Failed to initialise session | strategy=%s symbol=%s",
                        config.strategy_id,
                        symbol,
                    )
                    continue
                self.sessions.append(session)

    # ------------------------------------------------------------------ #
    def get_session(self, strategy_id: str, symbol: str) -> Optional[StrategySession]:
        """Return the session matching the provided identifiers."""

        for session in self.sessions:
            if session.config.strategy_id == strategy_id and session.symbol == symbol:
                return session
        return None

    # ------------------------------------------------------------------ #
    def is_complete(self) -> bool:
        """Return ``True`` when every managed session has finished processing."""

        return all(session.finished for session in self.sessions)

    # ------------------------------------------------------------------ #
    def step_all(self) -> Dict[str, Any]:
        """Advance all sessions by one bar and merge their overlay payloads."""

        aggregated_overlays: List[Dict[str, Any]] = []
        aggregated_markers: List[Dict[str, Any]] = []
        legend_entries: Set[Tuple[str, str]] = set()

        for session in self.sessions:
            result = session.step()
            if not result:
                continue
            aggregated_overlays.extend(result.get("overlays", []))
            aggregated_markers.extend(result.get("markers", []))
            legend_entries.update(result.get("legend_entries", set()))

        return {
            "overlays": aggregated_overlays,
            "markers": aggregated_markers,
            "legend_entries": legend_entries,
        }

    # ------------------------------------------------------------------ #
    def run(self) -> Dict[str, Any]:
        """Run all sessions to completion and return aggregated artefacts."""

        overlays: List[Dict[str, Any]] = []
        markers: List[Dict[str, Any]] = []
        legend_entries: Set[Tuple[str, str]] = set()

        while not self.is_complete():
            step_payload = self.step_all()
            overlays.extend(step_payload.get("overlays", []))
            markers.extend(step_payload.get("markers", []))
            legend_entries.update(step_payload.get("legend_entries", set()))

        return {
            "overlays": overlays,
            "markers": markers,
            "legend_entries": legend_entries,
        }


__all__ = [
    "StrategyConfig",
    "StrategySession",
    "StrategySessionManager",
    "TimeframeSpec",
]

