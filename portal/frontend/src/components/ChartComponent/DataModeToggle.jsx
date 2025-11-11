import clsx from 'clsx';

const MODES = [
  { value: 'historical', label: 'Historical' },
  { value: 'live', label: 'Live' },
];

export default function DataModeToggle({
  mode,
  onChange,
  supportsLive,
  disabledReason,
  liveDescription,
  className = '',
}) {
  return (
    <div
      className={`flex min-w-[14rem] flex-col gap-3 rounded-2xl border border-white/12 bg-gradient-to-br from-[#0f172a]/95 via-[#0b1220]/95 to-[#060a12]/95 p-5 shadow-lg shadow-black/30 ${className}`}
    >
      <span className="text-[11px] font-medium uppercase tracking-[0.28em] text-slate-400/80">Data mode</span>
      <div className="inline-flex w-full gap-1 rounded-xl border border-white/10 bg-[#0b1324]/90 p-1 shadow-sm shadow-black/20">
        {MODES.map(({ value, label }) => {
          const active = mode === value;
          const isLive = value === 'live';
          const isDisabled = isLive && !supportsLive;

          return (
            <button
              key={value}
              type="button"
              onClick={() => {
                if (isDisabled) return;
                onChange?.(value);
              }}
              className={clsx(
                'flex-1 rounded-lg px-3 py-1.5 text-xs font-semibold uppercase tracking-[0.28em] transition focus-visible:outline focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-[color:var(--accent-outline)]',
                active
                  ? 'bg-[color:var(--accent-alpha-30)] text-[color:var(--accent-text-strong)] shadow-inner'
                  : 'text-slate-200 hover:bg-[color:var(--accent-alpha-12)] hover:text-[color:var(--accent-text-soft)]',
                isDisabled &&
                  'cursor-not-allowed border border-dashed border-white/15 text-slate-500 hover:bg-transparent hover:text-slate-500',
              )}
              title={isDisabled ? disabledReason : undefined}
            >
              {label}
            </button>
          );
        })}
      </div>
      {supportsLive ? (
        <p className="text-[11px] leading-relaxed text-slate-400/80">
          {liveDescription || 'Live updates poll the selected datasource roughly every ~10s.'}
        </p>
      ) : (
        <p className="text-[11px] leading-relaxed text-slate-400/70">
          {disabledReason || 'Live updates require a supported real-time datasource.'}
        </p>
      )}
    </div>
  );
}
