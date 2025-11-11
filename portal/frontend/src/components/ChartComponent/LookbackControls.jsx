const DEFAULT_PRESETS = [
  { label: '1D', days: 1 },
  { label: '5D', days: 5 },
  { label: '10D', days: 10 },
  { label: '1M', days: 30 },
  { label: '3M', days: 90 },
  { label: '6M', days: 180 },
  { label: '1Y', days: 365 },
];

export function HistoricalLookbackControl({
  value,
  onSelect,
  presets = DEFAULT_PRESETS,
  maxDays = 365,
  onActivate,
  active = true,
  className = '',
}) {
  const clamped = Math.min(maxDays, Math.max(1, Number(value) || 1));

  return (
    <div
      className={`flex min-w-[16rem] flex-col gap-4 rounded-2xl border border-white/12 bg-gradient-to-br from-[#0f172a]/95 via-[#0b1220]/95 to-[#060a12]/95 px-5 py-4 transition ${
        active
          ? 'ring-1 ring-[color:var(--accent-ring-strong)]'
          : 'opacity-75 hover:border-[color:var(--accent-alpha-30)] hover:opacity-100'
      } ${className}`}
      onClick={() => onActivate?.('lookback')}
      role="group"
    >
      <div className="flex items-center justify-between gap-2">
        <div>
          <span className="text-[11px] font-medium uppercase tracking-[0.28em] text-slate-400/80">Days Back</span>
          <p className="text-sm font-semibold text-slate-100 tracking-tight">Rolling lookback presets</p>
        </div>
        <span
          className={`text-[10px] font-semibold uppercase tracking-[0.32em] ${
            active ? 'text-[color:var(--accent-text-strong)]' : 'text-slate-500'
          }`}
        >
          {active ? 'Active' : 'Tap to activate'}
        </span>
      </div>
      <div className="flex flex-wrap gap-1.5 rounded-xl border border-white/10 bg-[#0b1324]/90 p-1.5">
        {presets.map((preset) => {
          const isActive = clamped === preset.days;
          return (
            <button
              key={preset.label}
              type="button"
              onClick={(event) => {
                event.stopPropagation();
                onActivate?.('lookback');
                onSelect?.(preset.days);
              }}
              className={`rounded-lg px-3 py-1.5 text-xs font-semibold uppercase tracking-[0.28em] transition focus-visible:outline focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-[color:var(--accent-outline)] ${
                isActive
                  ? 'bg-[color:var(--accent-alpha-28)] text-[color:var(--accent-text-strong)] shadow-inner'
                  : 'text-slate-300 hover:bg-[#111d34] hover:text-[color:var(--accent-text-soft)]'
              }`}
            >
              {preset.label}
            </button>
          );
        })}
      </div>
      <span className="text-[11px] uppercase tracking-[0.28em] text-slate-500">
        Last {clamped} day{clamped === 1 ? '' : 's'}
      </span>
    </div>
  );
}

export function LiveLookbackControl({ value, onChange, onCommit, maxDays = 365, className = '' }) {
  const numeric = Number(value);
  const showClampNotice = Number.isFinite(numeric) && numeric > maxDays;

  return (
    <div
      className={`flex min-w-[13rem] flex-col gap-3 rounded-2xl border border-white/12 bg-gradient-to-br from-[#0f172a]/95 via-[#0b1220]/95 to-[#060a12]/95 px-5 py-4 shadow-lg shadow-black/30 ${className}`}
    >
      <span className="text-[11px] font-medium uppercase tracking-[0.28em] text-slate-400/80">Live Window</span>
      <div className="flex items-center gap-2 rounded-xl border border-white/10 bg-[#0b1324]/90 px-3 py-2">
        <input
          type="text"
          inputMode="numeric"
          pattern="[0-9]*"
          value={value}
          onChange={onChange}
          onBlur={onCommit}
          onKeyDown={(event) => {
            if (event.key === 'Enter') {
              event.preventDefault();
              onCommit?.();
            }
          }}
          placeholder="90"
          className="w-24 rounded-md border border-transparent bg-transparent text-sm font-semibold uppercase tracking-[0.32em] text-slate-100 outline-none focus:border-[color:var(--accent-alpha-40)] focus:ring-0"
        />
        <span className="text-xs uppercase tracking-[0.28em] text-slate-400">Days</span>
      </div>
      <span className="text-[11px] text-slate-500">
        Max {maxDays} days{showClampNotice ? ` · using ${maxDays}` : ''}
      </span>
    </div>
  );
}

