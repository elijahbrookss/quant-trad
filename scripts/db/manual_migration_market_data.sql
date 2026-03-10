-- Manual migration helpers (run manually; adjust source table/column names as needed).

-- MIGRATION A: legacy OHLC table -> market_candles_raw
-- Expected legacy columns: symbol, interval, datasource, timestamp, open, high, low, close, volume
-- instrument_id is resolved externally (via portal instruments table or manual mapping).
INSERT INTO market_candles_raw (
    instrument_id,
    timeframe_seconds,
    candle_time,
    close_time,
    open,
    high,
    low,
    close,
    volume,
    trade_count,
    is_closed,
    source_time,
    inserted_at
)
SELECT
    map.instrument_id,
    map.timeframe_seconds,
    legacy.timestamp AS candle_time,
    legacy.timestamp + make_interval(secs => map.timeframe_seconds) AS close_time,
    legacy.open,
    legacy.high,
    legacy.low,
    legacy.close,
    legacy.volume,
    NULL AS trade_count,
    TRUE AS is_closed,
    NULL AS source_time,
    COALESCE(legacy.data_ingested_ts, now()) AS inserted_at
FROM legacy_ohlc_raw AS legacy
JOIN instrument_timeframe_map AS map
  ON map.symbol = legacy.symbol
 AND map.datasource = legacy.datasource
 AND map.interval = legacy.interval;

-- MIGRATION B: legacy derived candle stats -> candle_stats
INSERT INTO candle_stats (
    instrument_id,
    timeframe_seconds,
    candle_time,
    stats_version,
    computed_at,
    stats
)
SELECT
    map.instrument_id,
    map.timeframe_seconds,
    legacy.timestamp AS candle_time,
    legacy.stats_version,
    COALESCE(legacy.computed_at, now()) AS computed_at,
    legacy.stats_json AS stats
FROM legacy_candle_stats AS legacy
JOIN instrument_timeframe_map AS map
  ON map.symbol = legacy.symbol
 AND map.datasource = legacy.datasource
 AND map.interval = legacy.interval;

-- MIGRATION C: legacy derivatives state -> derivatives_market_state
INSERT INTO derivatives_market_state (
    instrument_id,
    observed_at,
    source_time,
    open_interest,
    open_interest_value,
    funding_rate,
    funding_time,
    mark_price,
    index_price,
    premium_rate,
    premium_index,
    next_funding_time,
    inserted_at
)
SELECT
    map.instrument_id,
    legacy.observed_at,
    legacy.source_time,
    legacy.open_interest,
    legacy.open_interest_value,
    legacy.funding_rate,
    legacy.funding_time,
    legacy.mark_price,
    legacy.index_price,
    legacy.premium_rate,
    legacy.premium_index,
    legacy.next_funding_time,
    COALESCE(legacy.inserted_at, now()) AS inserted_at
FROM legacy_derivatives_state AS legacy
JOIN instrument_map AS map
  ON map.symbol = legacy.symbol
 AND map.datasource = legacy.datasource
 AND map.exchange = legacy.exchange;
