import { useCallback, useMemo, useState } from 'react'
import { symbolsFromInstrumentSlots } from '../../utils/instrumentSymbols'

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
 * Flat strategy registry grouped by exchange.
 */
export const StrategyGrid = ({ strategies, selectedId, onSelect }) => {
  const [search, setSearch] = useState('')
  const [collapsedGroups, setCollapsedGroups] = useState({})

  const filteredStrategies = useMemo(() => {
    const query = search.trim().toLowerCase()
    if (!query) return strategies

    return strategies.filter((strategy) => {
      const symbols = symbolsFromInstrumentSlots(strategy?.instrument_slots)
      const haystack = [
        strategy?.name,
        strategy?.exchange,
        strategy?.venue_id,
        strategy?.datasource,
        strategy?.timeframe,
        ...symbols,
      ]
        .filter(Boolean)
        .join(' ')
        .toLowerCase()
      return haystack.includes(query)
    })
  }, [search, strategies])

  const groupedStrategies = useMemo(() => {
    const groups = new Map()
    for (const strategy of filteredStrategies) {
      const key = (
        strategy?.exchange
        || strategy?.venue_id
        || strategy?.datasource
        || 'UNSPECIFIED'
      ).toString().trim().toUpperCase()
      if (!groups.has(key)) groups.set(key, [])
      groups.get(key).push(strategy)
    }

    return [...groups.entries()]
      .sort((a, b) => a[0].localeCompare(b[0]))
      .map(([exchange, items]) => ({
        exchange,
        items: [...items].sort((a, b) => (a?.name || '').localeCompare(b?.name || '')),
      }))
  }, [filteredStrategies])

  const toggleGroup = useCallback((exchange) => {
    setCollapsedGroups((prev) => ({
      ...prev,
      [exchange]: !prev[exchange],
    }))
  }, [])

  if (!strategies.length) {
    return (
      <div className="border border-dashed border-white/10 bg-black/20 p-12 text-center">
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

  return (
    <div className="space-y-3">
      <div className="relative">
        <input
          type="text"
          value={search}
          onChange={(event) => setSearch(event.target.value)}
          placeholder="Search strategies..."
          className="w-full border border-white/[0.08] bg-black/20 px-3 py-2 text-xs text-slate-200 outline-none placeholder:text-slate-500 focus:border-white/[0.18]"
        />
      </div>

      {!filteredStrategies.length ? (
        <div className="border border-dashed border-white/[0.08] bg-black/10 px-3 py-6 text-center text-xs text-slate-500">
          No strategies match your search.
        </div>
      ) : null}

      {groupedStrategies.map((group) => (
        <section key={group.exchange} className="space-y-1">
          <button
            type="button"
            onClick={() => toggleGroup(group.exchange)}
            className="flex w-full items-center justify-between px-1 py-1 text-left"
            aria-expanded={!collapsedGroups[group.exchange]}
            aria-label={`${collapsedGroups[group.exchange] ? 'Expand' : 'Collapse'} ${group.exchange}`}
          >
            <span className="text-[10px] font-semibold uppercase tracking-[0.26em] text-slate-500">
              {group.exchange}
            </span>
            <span className="text-[10px] text-slate-500">
              {collapsedGroups[group.exchange] ? '▸' : '▾'} {group.items.length}
            </span>
          </button>
          <div className={`border-y border-white/[0.05] ${collapsedGroups[group.exchange] ? 'hidden' : ''}`}>
            {group.items.map((strategy) => {
              const isActive = strategy.id === selectedId
              const symbols = symbolsFromInstrumentSlots(strategy.instrument_slots)
              const instrumentsMap = buildInstrumentMap(strategy.instruments)
              const primarySymbol = symbols[0] ? getSymbolDisplay(symbols[0], instrumentsMap) : 'No symbol'
              const symbolMeta = symbols.length > 1
                ? `${primarySymbol} +${symbols.length - 1}`
                : primarySymbol
              const timeframeMeta = strategy?.timeframe || '—'

              return (
                <button
                  key={strategy.id}
                  type="button"
                  onClick={() => onSelect(strategy.id)}
                  className={`group relative flex w-full items-center justify-between border-l-[3px] px-3 py-3 text-left transition ${
                    isActive
                      ? 'border-l-[color:var(--accent-base)]'
                      : 'border-l-transparent'
                  } hover:bg-white/[0.03]`}
                  aria-label={`Select ${strategy.name}`}
                >
                  <span className="min-w-0 truncate pr-3 text-sm font-semibold text-slate-100">
                    {strategy.name}
                  </span>
                  <span className="shrink-0 text-[11px] text-slate-500">
                    {timeframeMeta} · {symbolMeta}
                  </span>
                </button>
              )
            })}
          </div>
        </section>
      ))}
    </div>
  )
}
