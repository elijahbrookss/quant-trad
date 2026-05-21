function symbolButtonClass({ isSelected, isLoading }) {
  if (isSelected && isLoading) {
    return 'border-amber-400/40 bg-amber-400/10 text-amber-100'
  }
  if (isSelected) {
    return 'border-[color:var(--accent-alpha-40)] bg-[color:var(--accent-alpha-12)] text-[color:var(--accent-text-strong)] shadow-[inset_0_0_0_1px_var(--accent-alpha-12)]'
  }
  return 'border-white/8 bg-black/10 text-slate-300 hover:border-white/16 hover:bg-black/25 hover:text-slate-100'
}

function humanizeStatus(value) {
  return String(value || '')
    .trim()
    .toLowerCase()
    .split('_')
    .filter(Boolean)
    .map((segment) => segment.charAt(0).toUpperCase() + segment.slice(1))
    .join(' ')
}

function statusPill(item) {
  if (item?.isLoading) return { label: 'Loading', tone: 'amber' }
  const normalized = String(item?.status || '').trim().toLowerCase()
  if (!normalized || ['ready', 'waiting', 'idle', 'snapshot_ready'].includes(normalized)) return null
  if (normalized.includes('run') || normalized.includes('live')) return { label: 'Live', tone: 'emerald' }
  if (normalized.includes('complete')) return { label: 'Completed', tone: 'slate' }
  if (normalized.includes('load') || normalized.includes('boot')) return { label: humanizeStatus(normalized), tone: 'amber' }
  if (normalized.includes('error') || normalized.includes('fail') || normalized.includes('unavailable')) {
    return { label: humanizeStatus(normalized), tone: 'rose' }
  }
  return { label: humanizeStatus(normalized), tone: 'slate' }
}

function statusPillClass(tone) {
  return {
    amber: 'border-amber-400/30 bg-amber-400/10 text-amber-200',
    emerald: 'border-emerald-400/30 bg-emerald-400/10 text-emerald-200',
    rose: 'border-rose-400/35 bg-rose-400/10 text-rose-200',
    slate: 'border-white/10 bg-white/5 text-slate-300',
  }[tone] || 'border-white/10 bg-white/5 text-slate-300'
}

function allSameTimeframe(items) {
  const values = new Set(
    items
      .map((item) => String(item?.timeframe || '').trim().toUpperCase())
      .filter(Boolean),
  )
  return values.size === 1 ? Array.from(values)[0] : null
}

export function SymbolSelectorPanel({ model, onSelectSymbol }) {
  const items = Array.isArray(model?.items) ? model.items : []
  const commonTimeframe = allSameTimeframe(items)

  return (
    <section className="space-y-2">
      <div className="flex flex-wrap items-center justify-between gap-2">
        <div className="flex flex-wrap items-center gap-2">
          <p className="text-sm font-semibold text-slate-100">Symbols</p>
          {commonTimeframe ? (
            <span className="rounded-[3px] border border-white/8 bg-white/5 px-1.5 py-0.5 text-[10px] font-semibold text-slate-400">
              {commonTimeframe}
            </span>
          ) : null}
        </div>
        <span className="text-xs text-slate-500">
          {items.length} tracked
        </span>
      </div>

      {items.length ? (
        <div className="grid max-h-[8.5rem] grid-cols-[repeat(auto-fill,minmax(8.75rem,1fr))] gap-1.5 overflow-auto pr-1">
          {items.map((item) => {
            const pill = statusPill(item)
            return (
              <button
                key={item.key}
                type="button"
                onClick={() => onSelectSymbol(item.key)}
                className={`rounded-[3px] border px-2.5 py-2 text-left transition ${symbolButtonClass(item)}`}
              >
                <div className="flex min-w-0 items-center justify-between gap-2">
                  <div className="min-w-0">
                    <p className="truncate text-sm font-semibold">{item.symbol}</p>
                    {!commonTimeframe ? (
                      <p className="mt-0.5 text-[11px] text-slate-500">
                        {item.timeframe}
                      </p>
                    ) : null}
                  </div>
                  {pill ? (
                    <span className={`shrink-0 rounded-[2px] border px-1.5 py-0.5 text-[10px] font-semibold ${statusPillClass(pill.tone)}`}>
                      {pill.label}
                    </span>
                  ) : (
                    <span
                      className={`size-1.5 shrink-0 rounded-full ${item.isReady ? 'bg-emerald-300/80' : 'bg-slate-500/70'}`}
                      title={item.isReady ? 'Snapshot synced' : 'Snapshot pending'}
                    />
                  )}
                </div>
                <div className="mt-1.5 flex flex-wrap gap-x-2 gap-y-0.5 text-[11px] text-slate-500">
                  <span>Open {item.openTrades}</span>
                  <span>Net {Number.isFinite(item.netPnl) ? item.netPnl.toFixed(2) : '—'}</span>
                </div>
              </button>
            )
          })}
        </div>
      ) : (
        <div className="rounded-[3px] border border-dashed border-white/10 px-3 py-4 text-sm text-slate-400">
          No symbol navigation is available for this run.
        </div>
      )}
    </section>
  )
}
