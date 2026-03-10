/**
 * Application-wide constants and default values.
 */

/**
 * Default ATM template structure.
 */
export const DEFAULT_ATM_TEMPLATE = {
  name: 'Untitled Template',
  schema_version: 2,
  contracts: 1,
  stop_r_multiple: null,
  stop_ticks: null,
  stop_price: null,
  take_profit_orders: [],
  stop_adjustments: [],
  trailing_stop: {
    enabled: false,
  },
  initial_stop: {
    mode: 'atr',
    atr_period: 14,
    atr_multiplier: 1.0,
  },
  risk: {
    base_risk_per_trade: null,
    global_risk_multiplier: 1.0,
  },
  _meta: {},
}

/**
 * Default data source for market data.
 */
export const DEFAULT_DATASOURCE = 'ALPACA'

/**
 * Available data sources.
 */
export const DATA_SOURCES = [
  { value: 'ALPACA', label: 'Alpaca Markets' },
  { value: 'IBKR', label: 'Interactive Brokers' },
  { value: 'CCXT', label: 'Crypto Exchanges (CCXT)' },
]

/**
 * Common timeframes.
 */
export const TIMEFRAMES = [
  { value: '1m', label: '1 Minute' },
  { value: '5m', label: '5 Minutes' },
  { value: '15m', label: '15 Minutes' },
  { value: '30m', label: '30 Minutes' },
  { value: '1h', label: '1 Hour' },
  { value: '4h', label: '4 Hours' },
  { value: '1d', label: '1 Day' },
  { value: '1w', label: '1 Week' },
]

/**
 * Rule actions.
 */
export const RULE_ACTIONS = [
  { value: 'BUY', label: 'Buy (Long Entry)' },
  { value: 'SELL', label: 'Sell (Short Entry)' },
  { value: 'CLOSE_LONG', label: 'Close Long' },
  { value: 'CLOSE_SHORT', label: 'Close Short' },
  { value: 'CLOSE_ALL', label: 'Close All Positions' },
]

/**
 * Rule match modes.
 */
export const RULE_MATCH_MODES = [
  { value: 'all', label: 'All conditions must match (AND)' },
  { value: 'any', label: 'Any condition can match (OR)' },
]

/**
 * Stop adjustment actions.
 */
export const STOP_ADJUSTMENT_ACTIONS = [
  { value: 'move_to_breakeven', label: 'Move to Breakeven' },
  { value: 'move_to_entry', label: 'Move to Entry' },
  { value: 'trail_by_r', label: 'Trail by R' },
  { value: 'trail_with_atr', label: 'Trail with ATR' },
]

/**
 * Instrument types.
 */
export const INSTRUMENT_TYPES = [
  { value: 'stock', label: 'Stock' },
  { value: 'future', label: 'Future' },
  { value: 'option', label: 'Option' },
  { value: 'forex', label: 'Forex' },
  { value: 'crypto', label: 'Cryptocurrency' },
]

/**
 * API endpoints.
 */
export const API_ENDPOINTS = {
  STRATEGIES: '/api/strategies',
  INDICATORS: '/api/indicators',
  INSTRUMENTS: '/api/instruments',
  ATM_TEMPLATES: '/api/strategies/atm-templates',
  SYMBOL_PRESETS: '/api/strategies/presets/symbols',
}

/**
 * CSS class names for common components.
 */
export const STYLES = {
  input: 'w-full rounded-lg border border-white/10 bg-black/40 px-3 py-2 text-sm text-white placeholder-slate-500 focus:border-[color:var(--accent-alpha-40)] focus:outline-none',
  select: 'w-full rounded-lg border border-white/10 bg-black/40 px-3 py-2 text-sm text-white focus:border-[color:var(--accent-alpha-40)] focus:outline-none',
  button: 'rounded-lg bg-[color:var(--accent-base)] px-4 py-2 text-sm font-medium text-white transition hover:bg-[color:var(--accent-alpha-80)] focus:outline-none focus:ring-2 focus:ring-[color:var(--accent-ring)]',
  buttonGhost: 'rounded-lg border border-white/10 bg-transparent px-4 py-2 text-sm font-medium text-slate-300 transition hover:bg-white/5 focus:outline-none',
  buttonDanger: 'rounded-lg bg-rose-600 px-4 py-2 text-sm font-medium text-white transition hover:bg-rose-700 focus:outline-none',
  card: 'rounded-2xl border border-white/10 bg-black/20 p-4',
  badge: 'rounded-full border border-white/10 bg-white/5 px-3 py-1 text-xs text-slate-100',
  label: 'text-xs font-semibold uppercase tracking-[0.3em] text-slate-400',
  hint: 'text-[11px] text-slate-500',
  error: 'text-[11px] text-rose-400',
}
