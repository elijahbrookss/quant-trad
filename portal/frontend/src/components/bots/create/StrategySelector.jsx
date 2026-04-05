import { useMemo, useState } from 'react'
import { Search, Check } from 'lucide-react'
import { symbolsFromInstrumentSlots } from '../../../utils/instrumentSymbols.js'

/**
 * Get base currency from instrument metadata or truncate symbol
 */
const getSymbolDisplay = (symbol, strategy) => {
  if (!symbol) return symbol
  const instruments = strategy?.instruments || []
  for (const inst of instruments) {
    if (inst?.symbol?.toUpperCase() === symbol.toUpperCase()) {
      const baseCurrency = inst?.metadata?.instrument_fields?.base_currency || inst?.base_currency
      if (baseCurrency) return baseCurrency
    }
  }
  if (symbol.length <= 6) return symbol
  return symbol.slice(0, 4) + '…'
}

export function StrategySelector({
  strategies,
  selectedIds,
  onToggle,
  onSelect,
  loading,
  error,
  compact = false,
}) {
  const [query, setQuery] = useState('')
  const handleSelect = onSelect || onToggle || (() => {})

  const filteredStrategies = useMemo(() => {
    const needle = query.trim().toLowerCase()
    if (!needle) return strategies
    return strategies.filter((strategy) => {
      const symbols = symbolsFromInstrumentSlots(strategy.instrument_slots).join(', ')
      const haystack = [strategy.name, strategy.timeframe, strategy.exchange, strategy.datasource, symbols]
        .filter(Boolean)
        .join(' ')
        .toLowerCase()
      return haystack.includes(needle)
    })
  }, [query, strategies])

  // Compact mode: show as clickable cards instead of checkboxes
  if (compact) {
    return (
      <div className="space-y-2">
        {loading && <p className="text-xs text-slate-500">Loading strategies…</p>}
        {error && (
          <div className="rounded-md border border-rose-900/50 bg-rose-950/20 px-2 py-1.5 text-xs text-rose-300">
            {error}
          </div>
        )}
        {strategies.length > 6 && (
          <label className="flex items-center gap-2 rounded-md border border-slate-800 bg-slate-950/50 px-2.5 py-1.5 text-sm text-slate-200 transition-colors focus-within:border-slate-700">
            <Search className="size-3 shrink-0 text-slate-600" />
            <input
              type="search"
              value={query}
              onChange={(event) => setQuery(event.target.value)}
              placeholder="Search strategies…"
              className="w-full bg-transparent text-xs text-slate-200 placeholder:text-slate-600 focus:outline-none"
            />
          </label>
        )}
        <div className="max-h-48 space-y-1.5 overflow-y-auto">
          {strategies.length === 0 ? (
            <p className="py-2 text-center text-xs text-slate-500">No strategies available</p>
          ) : filteredStrategies.length === 0 ? (
            <p className="py-2 text-center text-xs text-slate-500">No matches</p>
          ) : (
            filteredStrategies.map((strategy) => {
              const checked = selectedIds.includes(strategy.id)
              const rawSymbols = symbolsFromInstrumentSlots(strategy.instrument_slots)
              const displaySymbols = rawSymbols.slice(0, 3).map(s => getSymbolDisplay(s, strategy)).join(', ')
              const extraCount = rawSymbols.length - 3
              return (
                <button
                  key={strategy.id}
                  type="button"
                  onClick={() => handleSelect(strategy.id)}
                  className={`flex w-full items-center gap-2 rounded-md border px-2.5 py-2 text-left transition-all ${
                    checked
                      ? 'border-emerald-800/50 bg-emerald-950/30 text-slate-100'
                      : 'border-slate-800 bg-slate-950/50 text-slate-400 hover:border-slate-700 hover:bg-slate-900/50 hover:text-slate-300'
                  }`}
                >
                  <div className={`flex size-4 shrink-0 items-center justify-center rounded ${
                    checked ? 'bg-emerald-500/20 text-emerald-400' : 'bg-slate-800 text-transparent'
                  }`}>
                    <Check className="size-3" />
                  </div>
                  <div className="min-w-0 flex-1">
                    <p className="truncate text-xs font-medium">{strategy.name}</p>
                    <p className="truncate text-[10px] text-slate-500">
                      {strategy.timeframe?.toUpperCase()} · {displaySymbols}{extraCount > 0 ? ` +${extraCount}` : ''}
                    </p>
                  </div>
                </button>
              )
            })
          )}
        </div>
      </div>
    )
  }

  // Default mode: checkbox list
  return (
    <div className="space-y-3">
      <div className="flex items-center justify-between">
        <label className="text-xs font-medium text-slate-400">Strategies</label>
        {loading ? <span className="text-xs text-slate-500">Loading…</span> : null}
      </div>
      <label className="flex items-center gap-2 rounded-lg border border-slate-800 bg-slate-950/50 px-3 py-2 text-sm text-slate-200 transition-colors focus-within:border-slate-700 focus-within:bg-slate-950">
        <Search className="size-3.5 shrink-0 text-slate-600" />
        <input
          type="search"
          value={query}
          onChange={(event) => setQuery(event.target.value)}
          placeholder="Search by name, timeframe, or symbol"
          className="w-full bg-transparent text-sm text-slate-200 placeholder:text-slate-600 focus:outline-none"
        />
      </label>
      {error ? (
        <div className="rounded-lg border border-rose-900/50 bg-rose-950/20 px-3 py-2 text-xs text-rose-300">
          {error}
        </div>
      ) : null}
      <div className="max-h-72 space-y-2 overflow-y-auto rounded-lg border border-slate-800 bg-slate-950/50 p-2">
        {strategies.length === 0 ? (
          <p className="px-2 py-2 text-sm text-slate-500">No strategies configured</p>
        ) : filteredStrategies.length === 0 ? (
          <p className="px-2 py-2 text-sm text-slate-500">No strategies match your search</p>
        ) : (
          filteredStrategies.map((strategy) => {
            const checked = selectedIds.includes(strategy.id)
            const rawSymbols = symbolsFromInstrumentSlots(strategy.instrument_slots)
            const displaySymbols = rawSymbols.slice(0, 3).map(s => getSymbolDisplay(s, strategy)).join(', ')
            const extraCount = rawSymbols.length - 3
            return (
              <label
                key={strategy.id}
                className="flex cursor-pointer items-start gap-3 rounded-md border border-transparent px-2 py-2 transition-colors hover:border-slate-800 hover:bg-slate-900/50"
              >
                <input
                  type="checkbox"
                  className="mt-0.5 size-4 shrink-0 rounded border border-slate-700 bg-slate-900 accent-slate-600"
                  checked={checked}
                  onChange={() => handleSelect(strategy.id)}
                />
                <div className="min-w-0 flex-1 space-y-0.5">
                  <p className="truncate text-sm font-medium text-slate-200">{strategy.name}</p>
                  <p className="text-xs text-slate-500">
                    {strategy.timeframe?.toUpperCase()} · {displaySymbols}{extraCount > 0 ? ` +${extraCount}` : ''} · {strategy.exchange || strategy.datasource || '—'}
                  </p>
                </div>
              </label>
            )
          })
        )}
      </div>
    </div>
  )
}
