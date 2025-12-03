const fieldLabels = [
  { key: 'tick_size', label: 'Tick size' },
  { key: 'contract_size', label: 'Contract size' },
  { key: 'tick_value', label: 'Tick value' },
  { key: 'currency', label: 'Currency' },
  { key: 'provider_id', label: 'Provider' },
  { key: 'venue_id', label: 'Venue' },
]

function formatValue(value) {
  if (value === undefined || value === null || value === '') return '—'
  if (typeof value === 'number') return Number.isFinite(value) ? value.toString() : '—'
  return String(value)
}

export function InstrumentDetailsPanel({
  symbol,
  metadata = {},
  providerId,
  venueId,
  timeframe,
  status = {},
  onRefresh,
}) {
  const hasMetadata = metadata && Object.keys(metadata).length > 0
  return (
    <div className="space-y-3 rounded-xl border border-white/5 bg-black/30 p-4 text-sm text-slate-200">
      <div className="flex items-center justify-between">
        <div>
          <p className="text-xs uppercase tracking-[0.25em] text-slate-500">Instrument</p>
          <p className="text-base font-semibold text-white">{symbol || 'Unnamed symbol'}</p>
          <p className="text-xs text-slate-400">
            {providerId || 'Provider'} · {venueId || 'Venue'} · {timeframe || 'Timeframe'}
          </p>
        </div>
        {typeof onRefresh === 'function' ? (
          <button
            type="button"
            className="rounded-lg bg-white/10 px-3 py-2 text-xs font-semibold text-slate-100 transition hover:bg-white/20"
            onClick={onRefresh}
            disabled={status.loading}
          >
            {status.loading ? 'Loading…' : 'Refresh metadata'}
          </button>
        ) : null}
      </div>
      {status.error ? <p className="text-xs text-rose-300">{status.error}</p> : null}
      <div className="grid grid-cols-2 gap-3 md:grid-cols-3">
        {fieldLabels.map((field) => (
          <div key={field.key} className="rounded-lg border border-white/5 bg-white/5 px-3 py-2">
            <p className="text-[11px] uppercase tracking-[0.2em] text-slate-500">{field.label}</p>
            <p className="truncate text-sm text-white">{formatValue(metadata[field.key])}</p>
          </div>
        ))}
      </div>
      {!hasMetadata ? <p className="text-xs text-slate-400">No metadata loaded for this instrument.</p> : null}
      {status.updatedAt ? (
        <p className="text-[11px] uppercase tracking-[0.2em] text-slate-500">
          Updated {new Date(status.updatedAt).toLocaleString()}
        </p>
      ) : null}
    </div>
  )
}

export default InstrumentDetailsPanel
