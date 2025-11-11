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
}) {
  return (
    <div className="flex min-w-[14rem] flex-col gap-3 rounded-2xl border border-slate-700/60 bg-slate-900/50 p-4 shadow-[0_18px_50px_-30px_rgba(0,0,0,0.85)]">
      <span className="text-[11px] uppercase tracking-[0.2em] text-neutral-400">Data mode</span>
      <div className="inline-flex w-full gap-1 rounded-lg border border-white/12 bg-[#141824]/85 p-1 shadow-sm shadow-black/20">
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
                'flex-1 rounded-md px-3 py-1.5 text-xs font-semibold uppercase tracking-[0.25em] transition focus-visible:outline focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-[color:var(--accent-outline)]',
                active
                  ? 'bg-[color:var(--accent-alpha-30)] text-[color:var(--accent-text-strong)] shadow-inner'
                  : 'text-slate-200 hover:bg-[color:var(--accent-alpha-15)] hover:text-[color:var(--accent-text-soft)]',
                isDisabled &&
                  'cursor-not-allowed border border-dashed border-slate-600/70 text-slate-500 hover:bg-transparent hover:text-slate-500',
              )}
              title={isDisabled ? disabledReason : undefined}
            >
              {label}
            </button>
          );
        })}
      </div>
      {supportsLive ? (
        <p className="text-[10px] text-slate-400/80">{liveDescription || 'Live updates poll the selected datasource roughly every ~10s.'}</p>
      ) : (
        <p className="text-[10px] text-slate-400/70">{disabledReason || 'Live updates require a supported real-time datasource.'}</p>
      )}
    </div>
  );
}
