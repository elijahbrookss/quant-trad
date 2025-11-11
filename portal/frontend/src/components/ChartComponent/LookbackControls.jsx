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
  inputValue,
  onInputChange,
  onInputCommit,
  title = 'Days Back',
  subtitle = 'Rolling lookback presets',
  footnote,
  disabled = false,
}) {
  const clamped = Math.min(maxDays, Math.max(1, Number(value) || 1));
  const displayInput = inputValue ?? String(clamped);
  const canInteract = !disabled && active;
  const resolvedFootnote =
    footnote || `Last ${clamped} day${clamped === 1 ? '' : 's'}`;

  return (
    <div
      className={`flex min-w-[16rem] flex-col gap-3 rounded-[18px] border border-white/12 bg-gradient-to-br from-[#0f172a]/95 via-[#0b1220]/95 to-[#060a12]/95 px-4 py-3 transition ${
        active && !disabled
          ? 'ring-1 ring-[color:var(--accent-ring-strong)]'
          : 'opacity-70 hover:border-[color:var(--accent-alpha-26)] hover:opacity-95'
      } ${disabled ? 'cursor-not-allowed opacity-55' : ''} ${className}`}
      onClick={() => {
        if (!disabled) {
          onActivate?.('lookback');
        }
      }}
      role="group"
      aria-disabled={disabled}
    >
      <div className="flex items-center justify-between gap-2">
        <div>
          <span className="text-[11px] font-medium uppercase tracking-[0.28em] text-slate-400/80">{title}</span>
          <p className="text-xs font-semibold text-slate-200 tracking-tight sm:text-sm">{subtitle}</p>
        </div>
        <span
          className={`text-[10px] font-semibold uppercase tracking-[0.32em] ${
            active && !disabled
              ? 'text-[color:var(--accent-text-strong)]'
              : 'text-slate-500'
          }`}
        >
          {active && !disabled
            ? 'Active'
            : disabled
              ? 'Locked'
              : 'Tap to activate'}
        </span>
      </div>
      <div
        className={`flex flex-wrap gap-1.5 rounded-xl border border-white/10 bg-[#0b1324]/90 p-1.5 ${
          disabled ? 'pointer-events-none opacity-60' : ''
        }`}
      >
        {presets.map((preset) => {
          const isActive = clamped === preset.days;
          return (
            <button
              key={preset.label}
              type="button"
              onClick={(event) => {
                event.stopPropagation();
                if (!disabled) {
                  onActivate?.('lookback');
                  onSelect?.(preset.days);
                }
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
      <div className="flex flex-col gap-1.5 rounded-xl border border-white/10 bg-[#0b1324]/90 px-2.5 py-2">
        <label className="text-[10px] uppercase tracking-[0.26em] text-slate-500">
          Custom days
        </label>
        <div className="flex items-center gap-2">
          <input
            type="text"
            inputMode="numeric"
            pattern="[0-9]*"
            value={displayInput}
            onChange={(event) => {
              if (!disabled) {
                onInputChange?.(event);
              }
            }}
            onBlur={() => {
              if (!disabled) {
                onInputCommit?.();
              }
            }}
            onKeyDown={(event) => {
              if (event.key === 'Enter' && !disabled) {
                event.preventDefault();
                onInputCommit?.();
              }
            }}
            placeholder={String(clamped)}
            disabled={!canInteract}
            className={`w-24 rounded-md border border-transparent bg-[#050912]/80 px-2 py-1.5 text-sm font-semibold uppercase tracking-[0.28em] text-slate-100 outline-none ${
              canInteract
                ? 'focus:border-[color:var(--accent-alpha-40)] focus:ring-0'
                : 'cursor-not-allowed text-slate-500'
            }`}
          />
          <span className="text-xs uppercase tracking-[0.28em] text-slate-400">Days</span>
        </div>
      </div>
      <span className="text-[11px] uppercase tracking-[0.28em] text-slate-500">{resolvedFootnote}</span>
    </div>
  );
}

