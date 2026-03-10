-- Core market data schema (fresh baseline)

CREATE TABLE IF NOT EXISTS market_candles_raw (
    instrument_id TEXT NOT NULL,
    timeframe_seconds INTEGER NOT NULL,
    candle_time TIMESTAMPTZ NOT NULL,
    close_time TIMESTAMPTZ NOT NULL,
    open DOUBLE PRECISION NOT NULL,
    high DOUBLE PRECISION NOT NULL,
    low DOUBLE PRECISION NOT NULL,
    close DOUBLE PRECISION NOT NULL,
    volume DOUBLE PRECISION,
    trade_count BIGINT,
    is_closed BOOLEAN NOT NULL DEFAULT TRUE,
    source_time TIMESTAMPTZ,
    inserted_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (instrument_id, timeframe_seconds, candle_time),
    CHECK (timeframe_seconds > 0),
    CHECK (close_time > candle_time),
    CHECK (high >= low),
    CHECK (low <= open AND open <= high),
    CHECK (low <= close AND close <= high),
    CHECK (volume IS NULL OR volume >= 0),
    CHECK (trade_count IS NULL OR trade_count >= 0)
);

CREATE TABLE IF NOT EXISTS candle_stats (
    instrument_id TEXT NOT NULL,
    timeframe_seconds INTEGER NOT NULL,
    candle_time TIMESTAMPTZ NOT NULL,
    stats_version TEXT NOT NULL,
    computed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    stats JSONB NOT NULL,
    PRIMARY KEY (instrument_id, timeframe_seconds, candle_time, stats_version),
    FOREIGN KEY (instrument_id, timeframe_seconds, candle_time)
        REFERENCES market_candles_raw (instrument_id, timeframe_seconds, candle_time)
        ON DELETE CASCADE,
    CHECK (jsonb_typeof(stats) = 'object')
);

CREATE TABLE IF NOT EXISTS derivatives_market_state (
    instrument_id TEXT NOT NULL,
    observed_at TIMESTAMPTZ NOT NULL,
    source_time TIMESTAMPTZ,
    open_interest DOUBLE PRECISION,
    open_interest_value DOUBLE PRECISION,
    funding_rate DOUBLE PRECISION,
    funding_time TIMESTAMPTZ,
    mark_price DOUBLE PRECISION,
    index_price DOUBLE PRECISION,
    premium_rate DOUBLE PRECISION,
    premium_index DOUBLE PRECISION,
    next_funding_time TIMESTAMPTZ,
    inserted_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (instrument_id, observed_at),
    CHECK (open_interest IS NULL OR open_interest >= 0),
    CHECK (open_interest_value IS NULL OR open_interest_value >= 0)
);

CREATE TABLE IF NOT EXISTS portal_candle_closures (
    instrument_id TEXT NOT NULL,
    timeframe_seconds INTEGER NOT NULL,
    start_ts TIMESTAMPTZ NOT NULL,
    end_ts TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ DEFAULT now(),
    PRIMARY KEY (instrument_id, timeframe_seconds, start_ts, end_ts),
    CHECK (timeframe_seconds > 0),
    CHECK (end_ts > start_ts)
);

CREATE INDEX IF NOT EXISTS idx_candles_raw_instrument_tf_time
    ON market_candles_raw (instrument_id, timeframe_seconds, candle_time DESC);

CREATE INDEX IF NOT EXISTS idx_candle_stats_instrument_tf_time
    ON candle_stats (instrument_id, timeframe_seconds, candle_time DESC);

CREATE INDEX IF NOT EXISTS idx_derivatives_state_instrument_time
    ON derivatives_market_state (instrument_id, observed_at DESC);

CREATE INDEX IF NOT EXISTS idx_derivatives_state_time
    ON derivatives_market_state (observed_at DESC);

CREATE INDEX IF NOT EXISTS idx_candle_closures_lookup
    ON portal_candle_closures (instrument_id, timeframe_seconds, start_ts);
