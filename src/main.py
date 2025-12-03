"""Command-line entry point wiring the strategy session manager."""

from __future__ import annotations

import os
from typing import Any, Dict, Mapping, Optional, Sequence, Set, Tuple

import pandas as pd
from dotenv import load_dotenv

from core.logger import logger
from engines.strategy_manager import (
    StrategyConfig,
    StrategyInstrument,
    StrategySession,
    StrategySessionManager,
    TimeframeSpec,
)
from indicators.base import BaseIndicator
from indicators.market_profile import MarketProfileIndicator
from signals.engine.market_profile_generator import MarketProfileSignalGenerator
from indicators.config import DataContext


SYMBOL = "CL"
END_DATE = "2025-07-15"
LOWER_START = "2025-07-01"
HIGHER_START = "2025-05-10"


if not os.getenv("GITHUB_ACTIONS"):
    load_dotenv("secrets.env")
    load_dotenv(".env")


def market_profile_indicator_factory(
    *,
    symbol: str,
    timeframes: Mapping[str, pd.DataFrame],
    contexts: Mapping[str, DataContext],
    **_: Any,
) -> Optional[MarketProfileIndicator]:
    """Return a MarketProfile indicator initialised with the higher timeframe data."""

    higher_df = timeframes.get("higher")
    if higher_df is None or higher_df.empty:
        logger.warning("market_profile_indicator_factory skipped | reason=empty-higher-frame symbol=%s", symbol)
        return None

    interval = getattr(contexts.get("higher"), "interval", "30m")
    indicator = MarketProfileIndicator(
        df=higher_df.copy(),
        bin_size=0.5,
        mode="tpo",
        interval=interval,
    )
    indicator.symbol = symbol
    return indicator


def market_profile_chart_hook(
    *,
    symbol: str,
    timeframes: Mapping[str, pd.DataFrame],
    indicators: Sequence[BaseIndicator],
    state: Dict[str, Any],
    **_: Any,
) -> Optional[Dict[str, Any]]:
    """Generate overlays and legend entries for the market profile strategy."""

    lower_df = timeframes.get("lower")
    if lower_df is None or lower_df.empty:
        return None

    indicator = next((ind for ind in indicators if isinstance(ind, MarketProfileIndicator)), None)
    if indicator is None:
        return None

    merged_value_areas = indicator.merge_value_areas()
    indicator_overlays, legend_entries = indicator.to_overlays(lower_df, use_merged=True)

    signal_generator = state.get("signal_generator")
    if not isinstance(signal_generator, MarketProfileSignalGenerator) or signal_generator.indicator is not indicator:
        signal_generator = MarketProfileSignalGenerator(indicator=indicator, symbol=symbol)
        state["signal_generator"] = signal_generator
        state["emitted_ids"] = set()

    emitted_ids: Set[Tuple[str, str, Any]] = state.setdefault("emitted_ids", set())
    signals = signal_generator.generate_signals(lower_df, merged_value_areas)
    new_signals = []
    for sig in signals:
        key = (sig.type, sig.symbol, getattr(sig, "time", None))
        if key in emitted_ids:
            continue
        emitted_ids.add(key)
        new_signals.append(sig)

    signal_overlays = (
        MarketProfileSignalGenerator.to_overlays(plot_df=lower_df, signals=new_signals)
        if new_signals
        else []
    )

    combined_overlays = indicator_overlays + signal_overlays
    legends = set(legend_entries)
    if signal_overlays:
        legends.add(("MarketProfile breakout", "red"))

    return {
        "overlays": combined_overlays,
        "legend_entries": legends,
    }


def build_manager(symbol: str) -> Tuple[StrategySessionManager, StrategySession]:
    """Initialise the strategy manager and return it alongside the target session."""

    config = StrategyConfig(
        strategy_id="market_profile",
        provider_id=None,
        venue_id=None,
        instruments=[StrategyInstrument(symbol=symbol)],
        primary_timeframe="lower",
        timeframes={
            "lower": TimeframeSpec(start=LOWER_START, end=END_DATE, interval="15m"),
            "higher": TimeframeSpec(start=HIGHER_START, end=END_DATE, interval="30m"),
        },
        indicator_factories=[market_profile_indicator_factory],
        chart_hooks=[market_profile_chart_hook],
    )

    manager = StrategySessionManager([config])
    session = manager.get_session(config.strategy_id, symbol)
    if session is None:
        raise RuntimeError(f"Failed to initialise strategy session for symbol={symbol}")
    return manager, session


def main() -> None:
    """Run the configured strategies and render chart overlays for the primary session."""

    manager, session = build_manager(SYMBOL)
    results = manager.run()

    primary_timeframe = session.config.primary_timeframe
    provider = session.get_provider(primary_timeframe)
    context = session.get_context(primary_timeframe)

    if provider is None or context is None:
        logger.error(
            "Missing provider/context for plotting | strategy=%s symbol=%s timeframe=%s",
            session.config.strategy_id,
            session.symbol,
            primary_timeframe,
        )
        return

    overlays = results.get("overlays", [])
    legend_entries = results.get("legend_entries", set())
    logger.info(
        "Plotting overlays | strategy=%s symbol=%s overlays=%d markers=%d",
        session.config.strategy_id,
        session.symbol,
        len(overlays),
        len(results.get("markers", [])),
    )

    chart_title = f"{session.symbol} – {session.config.strategy_id} – {context.interval}"
    provider.plot_ohlcv(
        plot_ctx=context,
        title=chart_title,
        overlays=overlays,
        legend_entries=legend_entries,
        show_volume=True,
    )

    logger.info(
        "Strategy execution complete | strategy=%s symbol=%s overlays=%d markers=%d",
        session.config.strategy_id,
        session.symbol,
        len(overlays),
        len(results.get("markers", [])),
    )


if __name__ == "__main__":
    main()

