function toneClass(label) {
  const value = String(label || '').toLowerCase()
  if (value.includes('open') || value.includes('running')) return 'border-emerald-500/30 bg-emerald-500/10 text-emerald-100'
  if (value.includes('error') || value.includes('closed')) return 'border-rose-500/30 bg-rose-500/10 text-rose-100'
  if (value.includes('loading') || value.includes('connecting') || value.includes('bootstrapping')) return 'border-amber-500/30 bg-amber-500/10 text-amber-100'
  return 'border-white/10 bg-white/5 text-slate-200'
}

export function BotLensHeader({ header }) {
  return (
    <section className="overflow-hidden rounded-2xl border border-white/10 bg-[radial-gradient(circle_at_top_left,var(--accent-alpha-12),transparent_42%),linear-gradient(180deg,rgba(255,255,255,0.03),rgba(255,255,255,0))]">
      <div className="border-b border-white/10 px-5 py-4">
        <p className="text-[10px] font-semibold uppercase tracking-[0.32em] text-[color:var(--accent-text-kicker)]">
          {header.kicker}
        </p>
        <p className="mt-1 text-xl font-semibold text-slate-100">{header.title}</p>
        <p className="mt-2 text-sm leading-relaxed text-slate-300">{header.description}</p>
        <p className="mt-2 text-xs text-slate-500">{header.meta}</p>
      </div>
      <div className="flex flex-wrap gap-2 px-5 py-3">
        {header.pills.map((pill) => (
          <div key={pill.key} className={`rounded-lg border px-3 py-2 text-xs ${toneClass(pill.value)}`}>
            <p className="text-[10px] uppercase tracking-[0.24em] text-slate-400">{pill.label}</p>
            <p className="mt-1 font-semibold">{pill.value}</p>
          </div>
        ))}
      </div>
    </section>
  )
}
