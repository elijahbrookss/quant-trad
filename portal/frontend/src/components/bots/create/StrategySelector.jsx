import { useMemo, useState } from 'react'
import { CheckSquare, Search } from 'lucide-react'
import { symbolsFromInstrumentSlots } from '../../../utils/instrumentSymbols.js'

export function StrategySelector({ strategies, selectedIds, onToggle, loading, error }) {
  const [query, setQuery] = useState('')

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

  return (
    <div className="space-y-3">
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-2 text-[11px] uppercase tracking-[0.3em] text-slate-400">
          <CheckSquare className="size-4" /> Strategies
        </div>
        {loading ? <span className="text-[11px] text-slate-500">Loading…</span> : null}
      </div>
      <label className="flex items-center gap-2 rounded-xl border border-white/10 bg-black/30 px-3 py-2 text-sm text-slate-200">
        <Search className="size-4 text-slate-500" />
        <input
          type="search"
          value={query}
          onChange={(event) => setQuery(event.target.value)}
          placeholder="Search strategies by name, timeframe, or venue"
          className="w-full bg-transparent text-sm text-white placeholder:text-slate-500 focus:outline-none"
        />
      </label>
      {error ? <p className="text-xs text-rose-300">{error}</p> : null}
      <div className="max-h-72 space-y-2 overflow-y-auto rounded-xl border border-white/10 bg-black/30 p-2">
        {strategies.length === 0 ? (
          <p className="px-2 py-1 text-xs text-slate-400">Create a strategy to start a bot.</p>
        ) : filteredStrategies.length === 0 ? (
          <p className="px-2 py-1 text-xs text-slate-400">No strategies match your search.</p>
        ) : (
          filteredStrategies.map((strategy) => {
            const checked = selectedIds.includes(strategy.id)
            const symbols = symbolsFromInstrumentSlots(strategy.instrument_slots).join(', ')
            return (
              <label
                key={strategy.id}
                className="flex cursor-pointer items-start gap-3 rounded-lg px-2 py-2 transition hover:bg-white/5"
              >
                <input
                  type="checkbox"
                  className="mt-0.5 size-4 rounded border border-white/30 bg-transparent"
                  checked={checked}
                  onChange={() => onToggle(strategy.id)}
                />
                <div className="flex flex-col gap-0.5">
                  <span className="text-sm font-semibold text-white">{strategy.name}</span>
                  <span className="text-[11px] uppercase tracking-[0.3em] text-slate-500">
                    {strategy.timeframe} • {symbols} • {strategy.exchange || strategy.datasource || '—'}
                  </span>
                </div>
              </label>
            )
          })
        )}
      </div>
    </div>
  )
}
