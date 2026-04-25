function symbolButtonClass({ isSelected, isLoading }) {
  if (isSelected && isLoading) {
    return 'border-amber-400/40 bg-amber-400/10 text-amber-100'
  }
  if (isSelected) {
    return 'border-[color:var(--accent-alpha-40)] bg-[color:var(--accent-alpha-12)] text-[color:var(--accent-text-strong)] shadow-[0_0_0_1px_var(--accent-alpha-12)]'
  }
  return 'border-white/8 bg-black/20 text-slate-300 hover:border-white/16 hover:bg-black/30 hover:text-slate-100'
}

function summaryTone(status) {
  const normalized = String(status || '').trim().toLowerCase()
  if (normalized.includes('run') || normalized.includes('live')) return 'text-emerald-300'
  if (normalized.includes('load') || normalized.includes('boot')) return 'text-amber-300'
  if (normalized.includes('error') || normalized.includes('fail')) return 'text-rose-300'
  return 'text-slate-300'
}

export function SymbolSelectorPanel({ model, onSelectSymbol }) {
  const items = Array.isArray(model?.items) ? model.items : []

  return (
    <section className="space-y-3">
      <div className="flex flex-wrap items-center justify-between gap-3">
        <div>
          <p className="qt-ops-kicker">Symbol Scope</p>
          <p className={`mt-1 text-sm ${summaryTone(model?.bootstrapStatus)}`}>
            Switch inspection scope without leaving the current run.
          </p>
        </div>
        <span className="qt-mono text-[11px] uppercase tracking-[0.14em] text-slate-500">
          {items.length} tracked
        </span>
      </div>

      {items.length ? (
        <div className="grid max-h-[15rem] gap-2 overflow-auto pr-1 sm:grid-cols-2 2xl:grid-cols-3">
          {items.map((item) => (
            <button
              key={item.key}
              type="button"
              onClick={() => onSelectSymbol(item.key)}
              className={`rounded-[4px] border px-3 py-3 text-left transition ${symbolButtonClass(item)}`}
            >
              <div className="flex items-start justify-between gap-3">
                <div className="min-w-0">
                  <p className="qt-mono text-sm font-semibold uppercase tracking-[0.16em]">{item.symbol}</p>
                  <p className="mt-1 text-xs text-slate-400">{item.label}</p>
                </div>
                <span className="qt-ops-chip shrink-0 px-2 py-1 text-[10px]">
                  {item.isLoading ? 'Loading' : item.isReady ? 'Ready' : item.status}
                </span>
              </div>
              <div className="qt-mono mt-3 flex flex-wrap gap-x-3 gap-y-1 text-[11px] uppercase tracking-[0.12em] text-slate-500">
                <span>{item.timeframe}</span>
                <span>trades {item.trades}</span>
                <span>open {item.openTrades}</span>
                <span>net {Number.isFinite(item.netPnl) ? item.netPnl.toFixed(2) : '—'}</span>
              </div>
            </button>
          ))}
        </div>
      ) : (
        <div className="rounded-[4px] border border-dashed border-white/10 px-4 py-5 text-sm text-slate-400">
          No symbol navigation is available for this run.
        </div>
      )}
    </section>
  )
}
