"""Schema helpers for report export outputs.

These keep column ordering in one place while allowing dynamic columns
to be added without editing the exporter every time a field changes.
"""

from __future__ import annotations

from typing import Dict, Iterable, List, Sequence


# Stable base column orders for each CSV we emit. Dynamic fields that
# appear in the rows will be appended after these in alphabetical order.
RUN_COLUMNS: Sequence[str] = (
    "run_id",
    "bot_id",
    "bot_name",
    "strategy_id",
    "strategy_name",
    "run_type",
    "status",
    "timeframe",
    "datasource",
    "exchange",
    "symbols",
    "backtest_start",
    "backtest_end",
    "started_at",
    "ended_at",
    "duration_seconds",
    "summary_json",
    "config_snapshot_json",
    "created_at",
    "updated_at",
)

INSTRUMENT_COLUMNS: Sequence[str] = (
    "instrument_id",
    "symbol",
    "datasource",
    "exchange",
    "flags",
    "fees",
    "metadata_json",
)

TRADE_COLUMNS_BASE: Sequence[str] = (
    "trade_id",
    "run_id",
    "bot_id",
    "strategy_id",
    "instrument_id",
    "symbol",
    "timeframe",
    "direction",
    "status",
    "contracts",
    "entry_time",
    "entry_price",
    "stop_price",
    "exit_time",
    "gross_pnl",
    "fees_paid",
    "net_pnl",
    "duration_seconds",
    "was_win",
    "metrics_json",
    "created_at",
    "updated_at",
    "entry_candle_time",
    "entry_stats_warmup_flags",
    "entry_fallback_used",
    "entry_regime_missing",
    "entry_volatility_state",
    "entry_structure_state",
    "entry_expansion_state",
    "entry_liquidity_state",
    "entry_regime_confidence",
    "entry_tr_pct",
    "entry_atr_ratio",
    "entry_atr_slope",
    "entry_atr_zscore",
    "entry_overlap_pct",
    "entry_directional_efficiency",
    "entry_range_position",
)

TRADE_EVENT_COLUMNS: Sequence[str] = (
    "event_id",
    "trade_id",
    "bot_id",
    "strategy_id",
    "instrument_id",
    "symbol",
    "event_type",
    "reason_code",
    "leg",
    "contracts",
    "price",
    "pnl",
    "event_time",
    "created_at",
)

LEDGER_EVENT_COLUMNS: Sequence[str] = (
    "event_id",
    "parent_event_id",
    "reason_code",
    "event_type",
    "event_subtype",
    "event_ts",
    "created_at",
    "symbol",
    "instrument_id",
    "trade_id",
    "strategy_id",
    "strategy_name",
    "timeframe",
    "side",
    "qty",
    "price",
    "event_impact_pnl",
    "trade_net_pnl",
    "reason_detail",
    "evidence_refs_json",
    "alternatives_rejected_json",
    "context_json",
)

RAW_CANDLES_COLUMNS: Sequence[str] = (
    "instrument_id",
    "symbol",
    "timeframe",
    "timeframe_seconds",
    "candle_time",
    "close_time",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "trade_count",
    "is_closed",
    "source_time",
    "inserted_at",
)

DERIVATIVES_COLUMNS: Sequence[str] = (
    "instrument_id",
    "symbol",
    "observed_at",
    "source_time",
    "open_interest",
    "open_interest_value",
    "funding_rate",
    "funding_time",
    "mark_price",
    "index_price",
    "premium_rate",
    "premium_index",
    "next_funding_time",
    "inserted_at",
)

CANDLE_STATS_BASE_COLUMNS: Sequence[str] = (
    "instrument_id",
    "symbol",
    "timeframe",
    "timeframe_seconds",
    "candle_time",
    "stats_version",
    "computed_at",
    "stats_json",
    "tr",
    "tr_pct",
    "atr_short",
    "atr_long",
    "atr_ratio",
    "atr_slope",
    "atr_zscore",
    "slope",
    "directional_efficiency",
    "slope_stability",
    "slope_stability_warmup",
    "overlap_pct",
    "range_contraction",
    "range_position",
    "volume_zscore",
    "volume_vs_median",
)

REGIME_STATS_BASE_COLUMNS: Sequence[str] = (
    "instrument_id",
    "symbol",
    "timeframe",
    "timeframe_seconds",
    "candle_time",
    "regime_version",
    "computed_at",
    "regime_json",
    "volatility_state",
    "structure_state",
    "expansion_state",
    "liquidity_state",
    "confidence",
)

DECISION_LEDGER_BASE_COLUMNS: Sequence[str] = (
    "ts",
    "decision_id",
    "trade_id",
    "instrument_id",
    "symbol",
    "timeframe_seconds",
    "decision_type",
    "action",
    "outcome",
    "reason_code",
    "context_json",
    "side",
    "qty",
    "price",
    "event_impact_pnl",
    "trade_net_pnl",
    "reason_detail",
    "evidence_refs_json",
    "alternatives_rejected_json",
    "created_at",
)


def derive_fieldnames(rows: Iterable[Dict[str, object]], base_order: Sequence[str]) -> List[str]:
    """Return deterministic fieldnames given rows, honoring base_order first.

    Any keys not in base_order are appended alphabetically so new fields
    appear automatically without code changes.
    """
    materialized_rows = list(rows)
    if not materialized_rows:
        return list(base_order)
    seen = set()
    ordered: List[str] = []
    for name in base_order:
        if any(name in row for row in materialized_rows):
            ordered.append(name)
            seen.add(name)
    dynamic_keys = set()
    for row in materialized_rows:
        dynamic_keys.update(row.keys())
    extras = sorted(k for k in dynamic_keys if k not in seen)
    ordered.extend(extras)
    return ordered
