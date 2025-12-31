export function ActiveTradeChip({ chip, visible, onHover, isActiveSymbol }) {
  if (!chip) return null

  const directionStyles =
    chip.direction === 'short'
      ? {
          dot: 'bg-rose-400 shadow-[0_0_0_6px] shadow-rose-500/20',
          pill: 'border-rose-400/40 bg-rose-500/10 text-rose-200',
          glow: 'from-rose-500/15 via-transparent to-transparent',
        }
      : {
          dot: 'bg-emerald-400 shadow-[0_0_0_6px] shadow-emerald-400/20',
          pill: 'border-emerald-400/40 bg-emerald-500/10 text-emerald-200',
          glow: 'from-emerald-500/15 via-transparent to-transparent',
        }

  return (
    <div
      className={`relative w-full rounded-xl border px-3 py-2 text-white shadow-[0_10px_28px_-22px_rgba(0,0,0,0.85)] transition-all duration-300 ease-out ${
        visible
          ? 'border-white/10 bg-gradient-to-r from-white/5 via-black/40 to-black/70 opacity-100 translate-y-0'
          : 'border-white/5 bg-black/20 opacity-0 -translate-y-2'
      } ${isActiveSymbol ? 'ring-1 ring-white/10' : 'opacity-70'}`}
      onMouseEnter={() => onHover(true)}
      onMouseLeave={() => onHover(false)}
    >
      <div className={`pointer-events-none absolute inset-0 rounded-xl bg-gradient-to-r ${directionStyles.glow}`} />

      <div className="relative flex flex-wrap items-center justify-between gap-2">
        <div className="flex flex-wrap items-center gap-2">
          <span className="rounded-full border border-white/10 bg-white/10 px-2 py-0.5 text-[10px] uppercase tracking-[0.3em] text-slate-200">
            {chip.symbol}
          </span>
          <span
            className={`rounded-full border px-2 py-0.5 text-[10px] uppercase tracking-[0.25em] ${directionStyles.pill}`}
          >
            {chip.directionLabel}
          </span>
          <span className="rounded-full border border-white/10 bg-white/5 px-2 py-0.5 text-[10px] uppercase tracking-[0.2em] text-slate-300">
            {chip.sizeLabel}
          </span>
        </div>
        <span className={`relative h-2 w-2 rounded-full ${directionStyles.dot}`}>
          {visible ? (
            <span
              className={`absolute inset-0 rounded-full animate-ping ${
                chip.direction === 'short' ? 'bg-rose-400' : 'bg-emerald-400'
              }`}
              style={{ animationDuration: '2.4s' }}
            />
          ) : null}
        </span>
      </div>

      <div className="relative mt-2 grid grid-cols-3 gap-2 text-xs text-slate-200">
        <div className="rounded-lg border border-white/10 bg-black/40 px-2 py-1">
          <p className="text-[9px] uppercase tracking-[0.25em] text-slate-400">Entry</p>
          <p className="mt-0.5 font-semibold text-white">{chip.entry}</p>
        </div>
        <div className="rounded-lg border border-white/10 bg-black/30 px-2 py-1">
          <p className="text-[9px] uppercase tracking-[0.25em] text-slate-400">Stop</p>
          <p className="mt-0.5 font-semibold text-slate-100">{chip.stop}</p>
        </div>
        <div className="rounded-lg border border-white/10 bg-black/30 px-2 py-1">
          <p className="text-[9px] uppercase tracking-[0.25em] text-slate-400">Target</p>
          <p className="mt-0.5 font-semibold text-slate-100">{chip.target}</p>
        </div>
      </div>
    </div>
  )
}
