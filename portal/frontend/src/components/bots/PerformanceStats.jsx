export function PerformanceStats({ statEntries, formatStatValue }) {
  return (
    <div className="grid gap-4 rounded-3xl border border-white/5 bg-white/5 p-4 text-sm text-slate-200 sm:grid-cols-3">
      {statEntries.map(([key, value]) => (
        <div key={key} className="rounded-2xl border border-white/10 bg-black/20 p-3">
          <p className="text-xs uppercase tracking-[0.35em] text-slate-400">{key.replace(/_/g, ' ')}</p>
          <p className="text-2xl font-semibold text-white">{formatStatValue(key, value)}</p>
        </div>
      ))}
    </div>
  )
}
