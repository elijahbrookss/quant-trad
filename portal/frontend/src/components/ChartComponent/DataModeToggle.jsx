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
    <div className="flex min-w-[12rem] flex-col gap-2">
      <span className="text-[11px] uppercase tracking-[0.2em] text-neutral-400">Data mode</span>
      <div className="inline-flex rounded-lg border border-slate-600/60 bg-slate-900/60 p-1">
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
                'min-w-[5.5rem] rounded-md px-3 py-1.5 text-xs font-semibold uppercase tracking-[0.25em] transition focus-visible:outline focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-[color:var(--accent-outline)]',
                active
                  ? 'bg-[color:var(--accent-alpha-30)] text-[color:var(--accent-text-strong)] shadow-inner'
                  : 'text-slate-300 hover:bg-[color:var(--accent-alpha-15)] hover:text-[color:var(--accent-text-soft)]',
                isDisabled && 'cursor-not-allowed opacity-40 hover:bg-transparent hover:text-slate-400',
              )}
            >
              {label}
            </button>
          );
        })}
      </div>
      {supportsLive ? (
        <p className="text-[10px] text-slate-400/80">{liveDescription || 'Live updates poll the selected datasource roughly every ~10s.'}</p>
      ) : (
        <p className="text-[10px] text-slate-400/60">{disabledReason || 'Live updates require a supported real-time datasource.'}</p>
      )}
    </div>
  );
}
