import { symbolsFromInstrumentSlots } from '../../utils/instrumentSymbols'

/**
 * Get color class for timeframe - provides visual differentiation at a glance
 */
const getTimeframeColor = (timeframe) => {
  const tf = (timeframe || '').toLowerCase()
  if (tf.includes('15m') || tf.includes('15min')) return 'bg-violet-500'
  if (tf.includes('1h') || tf === 'h1' || tf === '60m') return 'bg-sky-500'
  if (tf.includes('4h') || tf === 'h4' || tf === '240m') return 'bg-emerald-500'
  if (tf.includes('1d') || tf === 'd1' || tf === 'daily') return 'bg-amber-500'
  if (tf.includes('1w') || tf === 'w1' || tf === 'weekly') return 'bg-rose-500'
  return 'bg-slate-500'
}

/**
 * Build a map of symbol -> instrument for quick lookup
 */
const buildInstrumentMap = (instruments) => {
  const map = new Map()
  if (!Array.isArray(instruments)) return map
  for (const inst of instruments) {
    const key = (inst?.symbol || '').toUpperCase()
    if (key) map.set(key, inst)
  }
  return map
}

/**
 * Get display name for a symbol - prefers base_currency from instrument metadata
 */
const getSymbolDisplay = (symbol, instrumentsMap) => {
  if (!symbol) return ''
  const str = String(symbol).toUpperCase()

  // Try to get base_currency from instrument metadata
  const instrument = instrumentsMap?.get(str)
  const baseCurrency = instrument?.metadata?.instrument_fields?.base_currency || instrument?.base_currency
  if (baseCurrency) return baseCurrency

  // Fallback: for pairs like BTCUSDT, show as-is if short enough
  if (str.length <= 8) return str
  // Truncate long symbols
  return str.slice(0, 6) + '…'
}

/**
 * Grid display of strategy cards.
 */
export const StrategyGrid = ({ strategies, selectedId, onSelect, onEdit, onDelete, layout = 'grid' }) => {
  if (!strategies.length) {
    return (
      <div className="rounded-2xl border border-dashed border-white/10 bg-black/20 p-12 text-center">
        <svg
          className="mx-auto h-12 w-12 text-slate-600"
          fill="none"
          viewBox="0 0 24 24"
          stroke="currentColor"
        >
          <path
            strokeLinecap="round"
            strokeLinejoin="round"
            strokeWidth={1.5}
            d="M9 5H7a2 2 0 00-2 2v12a2 2 0 002 2h10a2 2 0 002-2V7a2 2 0 00-2-2h-2M9 5a2 2 0 002 2h2a2 2 0 002-2M9 5a2 2 0 012-2h2a2 2 0 012 2m-6 9l2 2 4-4"
          />
        </svg>
        <p className="mt-4 text-sm font-medium text-slate-400">No strategies yet</p>
        <p className="mt-1 text-xs text-slate-500">Create your first strategy to get started</p>
      </div>
    )
  }

  const gridClasses =
    layout === 'stacked'
      ? 'grid gap-2'
      : 'grid gap-4 sm:grid-cols-2 lg:grid-cols-3'

  return (
    <div className={gridClasses}>
      {strategies.map((strategy) => {
        const isActive = strategy.id === selectedId
        const ruleCount = Array.isArray(strategy.rules) ? strategy.rules.length : 0
        const indicatorCount = Array.isArray(strategy.indicator_ids) ? strategy.indicator_ids.length : 0
        const hasMissingIndicators = Array.isArray(strategy.missing_indicators) && strategy.missing_indicators.length > 0
        const symbols = symbolsFromInstrumentSlots(strategy.instrument_slots)
        const symbolCount = symbols.length
        const timeframeColor = getTimeframeColor(strategy.timeframe)
        const instrumentsMap = buildInstrumentMap(strategy.instruments)

        return (
          <div
            key={strategy.id}
            onClick={() => onSelect(strategy.id)}
            role="button"
            tabIndex={0}
            onKeyDown={(e) => { if (e.key === 'Enter' || e.key === ' ') onSelect(strategy.id) }}
            className={`group relative flex cursor-pointer overflow-hidden rounded-xl border transition-all duration-200 ${
              isActive
                ? 'border-[color:var(--accent-alpha-60)] bg-[color:var(--accent-alpha-10)]'
                : 'border-white/[0.06] bg-black/40 hover:border-white/[0.12] hover:bg-black/50'
            }`}
            aria-label={`Select ${strategy.name}`}
          >
            {/* Timeframe color stripe */}
            <div className={`w-1 flex-shrink-0 ${timeframeColor}`} />

            {/* Main content */}
            <div className="flex flex-1 items-start gap-3 p-3">
              <div className="min-w-0 flex-1">
                {/* Name row with status dot */}
                <div className="flex items-center gap-2">
                  <h3 className="truncate text-sm font-medium text-white">{strategy.name}</h3>
                  {hasMissingIndicators && (
                    <span
                      className="h-2 w-2 flex-shrink-0 rounded-full bg-amber-500"
                      title="Missing indicators"
                    />
                  )}
                </div>

                {/* Info line */}
                <p className="mt-0.5 text-xs text-slate-500">
                  <span className="text-slate-400">{strategy.timeframe}</span>
                  <span className="mx-1.5">·</span>
                  <span>{symbolCount} symbol{symbolCount !== 1 ? 's' : ''}</span>
                  <span className="mx-1.5">·</span>
                  <span>{ruleCount}R / {indicatorCount}I</span>
                </p>

                {/* Symbol preview - shown subtly, uses base_currency when available */}
                <p className="mt-1 truncate text-[11px] text-slate-600">
                  {symbols.slice(0, 3).map((s) => getSymbolDisplay(s, instrumentsMap)).join(', ')}
                  {symbols.length > 3 && ` +${symbols.length - 3}`}
                </p>
              </div>

              {/* Hover actions */}
              <div className="flex flex-shrink-0 items-center gap-1.5 opacity-0 transition-opacity duration-150 group-hover:opacity-100">
                <button
                  onClick={(e) => { e.stopPropagation(); onEdit(strategy); }}
                  className="rounded-md bg-white/5 p-1.5 text-slate-400 transition hover:bg-white/10 hover:text-white"
                  title="Edit strategy"
                >
                  <svg className="h-3.5 w-3.5" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M11 5H6a2 2 0 00-2 2v11a2 2 0 002 2h11a2 2 0 002-2v-5m-1.414-9.414a2 2 0 112.828 2.828L11.828 15H9v-2.828l8.586-8.586z" />
                  </svg>
                </button>
                <button
                  onClick={(e) => { e.stopPropagation(); onDelete(strategy); }}
                  className="rounded-md bg-white/5 p-1.5 text-slate-400 transition hover:bg-rose-500/20 hover:text-rose-400"
                  title="Delete strategy"
                >
                  <svg className="h-3.5 w-3.5" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M19 7l-.867 12.142A2 2 0 0116.138 21H7.862a2 2 0 01-1.995-1.858L5 7m5 4v6m4-6v6m1-10V4a1 1 0 00-1-1h-4a1 1 0 00-1 1v3M4 7h16" />
                  </svg>
                </button>
              </div>
            </div>
          </div>
        )
      })}
    </div>
  )
}
