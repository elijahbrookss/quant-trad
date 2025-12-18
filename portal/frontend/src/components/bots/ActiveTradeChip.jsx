export function ActiveTradeChip({ chip, visible, onHover }) {
  if (!chip) return null

  return (
    <div
      className={`flex flex-wrap items-center gap-2 rounded-full border px-3 py-2 text-xs text-white shadow-lg transition-all duration-300 ease-out ${
        visible ? 'border-sky-400/40 bg-white/5 opacity-100 scale-100' : 'border-sky-400/10 bg-white/0 opacity-0 scale-95'
      } ${visible ? 'translate-y-0' : '-translate-y-2'}`}
      onMouseEnter={() => onHover(true)}
      onMouseLeave={() => onHover(false)}
    >
      <span
        className={`relative h-2.5 w-2.5 rounded-full ${
          chip.direction === 'short'
            ? 'bg-rose-400 shadow-[0_0_0_3px] shadow-rose-400/20'
            : 'bg-emerald-400 shadow-[0_0_0_3px] shadow-emerald-400/20'
        }`}
      >
        {visible && (
          <span
            className={`absolute inset-0 rounded-full animate-ping ${
              chip.direction === 'short' ? 'bg-rose-400' : 'bg-emerald-400'
            }`}
            style={{ animationDuration: '2s' }}
          />
        )}
      </span>
      <span className="text-sm font-semibold text-white">{chip.headline}</span>
      <span className="rounded-full bg-emerald-500/10 px-2 py-0.5 text-[11px] text-emerald-200">{chip.r}</span>
      <span className="rounded-full bg-sky-500/10 px-2 py-0.5 text-[11px] text-sky-200">{chip.pnl}</span>
      <span className="text-[11px] uppercase tracking-[0.2em] text-slate-300">SL {chip.sl}</span>
      <span className="text-[11px] uppercase tracking-[0.2em] text-slate-300">TP {chip.tp}</span>
    </div>
  )
}
