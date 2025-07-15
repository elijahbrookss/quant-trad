import pandas as pd
import time
from classes.Logger import logger

from classes.indicators.MarketProfileIndicator import MarketProfileIndicator
from data_providers.alpaca_provider import AlpacaProvider
from classes.indicators.config import DataContext
from classes.signals.market_profile_signal import MarketProfileSignalGenerator

symbol = "CL"
end = "2025-07-15"
DELAY_SECONDS = 0
provider = AlpacaProvider()

lower_timespan_ctx = DataContext(
    symbol=symbol,
    start="2025-07-01",
    end=end,
    interval="15m"
)

higher_timespan_ctx = DataContext(
    symbol=symbol,
    start="2025-05-10",
    end=end,
    interval="30m"
)

def get_value_areas(plot_df: pd.DataFrame):
    try:
        logger.info("Creating MarketProfileIndicator with higher timeframe context")
        mpi = MarketProfileIndicator.from_context(
            provider=provider,
            ctx=higher_timespan_ctx,
            bin_size=0.5,
            mode="tpo",
            interval="30m"
        )

        logger.info("Merging value areas for MarketProfileIndicator")
        merged = mpi.merge_value_areas()
        logger.debug("Merged value areas count: %d", len(merged))
        return merged

    except Exception as e:
        logger.exception("Error creating value areas: %s", str(e))
        return []

def simulate_signal_generation(delay_seconds: int = 5):
    try:
        logger.info("Fetching OHLCV data for simulation: %s %s-%s (%s)",
                    lower_timespan_ctx.symbol, lower_timespan_ctx.start, lower_timespan_ctx.end, lower_timespan_ctx.interval)
        full_df = provider.get_ohlcv(lower_timespan_ctx)
        if full_df is None or full_df.empty:
            logger.warning("No data returned for lower timeframe context")
            return

        logger.info("Fetched data shape: %s", full_df.shape)

        logger.info("Generating value areas and overlays from MarketProfileIndicator.")
        mpi = MarketProfileIndicator.from_context(
            provider=provider,
            ctx=higher_timespan_ctx,
            bin_size=0.5,
            mode="tpo",
            interval="30m"
        )
        merged_value_areas = mpi.merge_value_areas()
        indicator_overlays, legend_entries = mpi.to_overlays(full_df, use_merged=True)

        if not merged_value_areas:
            logger.warning("No value areas available for signal generation")
            return

        signal_generator = MarketProfileSignalGenerator(symbol=symbol)
        all_signals = []

        for i in range(30, len(full_df)):
            current_df = full_df.iloc[:i+1]
            signals = signal_generator.generate_signals(current_df, merged_value_areas)
            new_signals = signals[len(all_signals):]
            all_signals.extend(new_signals)

            if new_signals:
                logger.info("[%d] %d new signal(s) at %s", i, len(new_signals), current_df.index[-1])
                for sig in new_signals:
                    print(sig.to_dict())

            time.sleep(delay_seconds)

        logger.info("Simulation complete. Total signals generated: %d", len(all_signals))

        signal_overlays = MarketProfileSignalGenerator.to_overlays(plot_df=full_df, signals=all_signals) if all_signals else []
        combined_overlays = indicator_overlays + signal_overlays
        legend_entries.add(("MarketProfile breakout", "red")) 
        chart_title = f"{symbol} – MarketProfileSignal + MarketProfileIndicator – {lower_timespan_ctx.interval}"
        logger.info("Plotting chart with %d overlays.", len(combined_overlays))

        provider.plot_ohlcv(
            plot_ctx=lower_timespan_ctx,
            title=chart_title,
            overlays=combined_overlays,
            legend_entries=legend_entries,
            show_volume=True
        )

        if all_signals:
            logger.info("All signals (to_dict):")
            for sig in all_signals:
                print(sig.to_dict())

    except Exception as e:
        logger.exception("Simulation failed: %s", str(e))


if __name__ == "__main__":
    simulate_signal_generation(DELAY_SECONDS)
