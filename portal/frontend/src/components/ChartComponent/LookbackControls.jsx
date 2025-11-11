const DEFAULT_PRESETS = [
  { label: '1D', days: 1 },
  { label: '5D', days: 5 },
  { label: '10D', days: 10 },
  { label: '1M', days: 30 },
  { label: '3M', days: 90 },
  { label: '6M', days: 180 },
  { label: '1Y', days: 365 },
];

export function HistoricalLookbackControl({ value, onSelect, presets = DEFAULT_PRESETS, maxDays = 365 }) {
  const clamped = Math.min(maxDays, Math.max(1, Number(value) || 1));

  return (
    <div className="flex min-w-[16rem] flex-col gap-2">
      <span className="text-[11px] uppercase tracking-[0.2em] text-neutral-400">Historical Window</span>
      <div className="flex flex-wrap gap-1.5 rounded-lg border border-slate-600/60 bg-slate-900/50 p-1.5">
        {presets.map((preset) => {
          const isActive = clamped === preset.days;
          return (
            <button
              key={preset.label}
              type="button"
              onClick={() => onSelect?.(preset.days)}
              className={`rounded-md px-3 py-1.5 text-xs font-semibold uppercase tracking-[0.25em] transition focus-visible:outline focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-[color:var(--accent-outline)] ${
                isActive
                  ? 'bg-[color:var(--accent-alpha-30)] text-[color:var(--accent-text-strong)] shadow-inner'
                  : 'text-slate-300 hover:bg-[color:var(--accent-alpha-15)] hover:text-[color:var(--accent-text-soft)]'
              }`}
            >
              {preset.label}
            </button>
          );
        })}
      </div>
      <span className="text-[11px] uppercase tracking-[0.25em] text-slate-500">
        Last {clamped} day{clamped === 1 ? '' : 's'}
      </span>
    </div>
  );
}

export function LiveLookbackControl({ value, onChange, onCommit, maxDays = 365 }) {
  const numeric = Number(value);
  const showClampNotice = Number.isFinite(numeric) && numeric > maxDays;

  return (
    <div className="flex min-w-[13rem] flex-col gap-2">
      <span className="text-[11px] uppercase tracking-[0.2em] text-neutral-400">Live Window</span>
      <div className="flex items-center gap-2 rounded-lg border border-slate-600/60 bg-slate-900/50 px-3 py-2">
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
          className="w-16 rounded-md border border-transparent bg-transparent text-sm font-semibold uppercase tracking-[0.3em] text-slate-100 outline-none focus:border-[color:var(--accent-alpha-40)] focus:ring-0"
        />
        <span className="text-xs uppercase tracking-[0.25em] text-slate-400">Days</span>
      </div>
      <span className="text-[11px] text-neutral-500">
        Max {maxDays} days{showClampNotice ? ` · using ${maxDays}` : ''}
      </span>
    </div>
  );
}

