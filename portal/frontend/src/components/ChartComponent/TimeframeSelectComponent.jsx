import React, { useMemo, useState } from 'react';

/**
 * Available timeframes:
 * '1m', '5m', '15m', '30m', '1h', '4h', '1d', '1w', '1M'
 */
const options = [
  { label: '1 Minute', value: '1m' },
  { label: '5 Minutes', value: '5m' },
  { label: '15 Minutes', value: '15m' },
  { label: '30 Minutes', value: '30m' },
  { label: '1 Hour', value: '1h', featured: true },
  { label: '4 Hours', value: '4h', featured: true },
  { label: '1 Day', value: '1d', featured: true },
  { label: '1 Week', value: '1w' },
  { label: '1 Month', value: '1M' },
];

/**
 * TimeframeSelect props:
 * @param {Object} props
 * @param {string} props.selected - Currently selected timeframe value
 * @param {function} props.onChange - Callback when a timeframe is selected
 * @param {string} [props.placeholder='Select timeframe...'] - Placeholder text
 */
export function TimeframeSelect({ selected, onChange, className = '' }) {
  const [open, setOpen] = useState(false);
  const activeOption = useMemo(
    () => options.find(option => option.value === selected) ?? options[0],
    [selected]
  );

  const toggle = () => setOpen(prev => !prev);
  const handleSelect = (value) => {
    onChange(value);
    setOpen(false);
  };

  return (
    <div
      className={`flex min-w-[14rem] flex-col gap-3 rounded-2xl border border-white/12 bg-gradient-to-br from-[#0f172a]/95 via-[#0b1220]/95 to-[#060a12]/95 p-5 shadow-lg shadow-black/30 ${className}`}
    >
      <span className="text-[11px] font-medium uppercase tracking-[0.28em] text-slate-400/80">Timeframe</span>
      <div className="relative">
        <button
          type="button"
          onClick={toggle}
          aria-haspopup="listbox"
          aria-expanded={open}
          className={`flex w-full items-center justify-between rounded-xl border border-white/10 bg-[#0b1324]/90 px-3 py-2 text-sm font-semibold tracking-[0.08em] text-slate-100 transition focus-visible:outline focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-[color:var(--accent-outline)] ${
            open ? 'border-[color:var(--accent-alpha-40)] bg-[#111d34]' : 'hover:border-[color:var(--accent-alpha-30)] hover:bg-[#0f1a30]'
          }`}
        >
          <span className="uppercase tracking-[0.2em] text-[13px] text-slate-200">
            {(activeOption?.label || activeOption?.value || '').toUpperCase()}
          </span>
          <svg
            className={`h-4 w-4 transition-transform ${open ? 'rotate-180' : ''}`}
            xmlns="http://www.w3.org/2000/svg"
            viewBox="0 0 24 24"
            fill="none"
            stroke="currentColor"
            strokeWidth={1.5}
          >
            <path strokeLinecap="round" strokeLinejoin="round" d="m6 9 6 6 6-6" />
          </svg>
        </button>

        <div
          role="listbox"
          className={`absolute z-10 mt-2 w-full overflow-hidden rounded-2xl border border-white/10 bg-[#050912]/95 shadow-[0_22px_70px_-30px_rgba(0,0,0,0.85)] backdrop-blur transition-all ${
            open ? 'max-h-80 opacity-100' : 'pointer-events-none max-h-0 opacity-0'
          }`}
        >
          <div className="divide-y divide-white/10">
            <div className="grid grid-cols-2 gap-px bg-[#0d1526]/90 p-2">
              {options.filter(o => o.featured).map(option => {
                const isActive = option.value === selected;
                return (
                  <button
                    key={option.value}
                    type="button"
                    onClick={() => handleSelect(option.value)}
                    className={`rounded-xl px-3 py-2 text-sm font-medium tracking-tight transition focus-visible:outline focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-[color:var(--accent-outline)] ${
                      isActive
                        ? 'bg-[color:var(--accent-alpha-25)] text-[color:var(--accent-text-strong)] ring-1 ring-[color:var(--accent-ring-strong)]'
                        : 'text-slate-200 hover:bg-[color:var(--accent-alpha-12)] hover:text-[color:var(--accent-text-soft)]'
                    }`}
                  >
                    {option.label}
                  </button>
                );
              })}
            </div>

            <div className="flex flex-col gap-1.5 p-3">
              {options.filter(o => !o.featured).map(option => {
                const isActive = option.value === selected;
                return (
                  <button
                    key={option.value}
                    type="button"
                    onClick={() => handleSelect(option.value)}
                    className={`flex items-center justify-between rounded-xl px-3 py-2 text-sm transition focus-visible:outline focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-[color:var(--accent-outline)] ${
                      isActive
                        ? 'bg-[color:var(--accent-alpha-20)] text-[color:var(--accent-text-strong)] ring-1 ring-[color:var(--accent-ring-strong)]'
                        : 'text-slate-200 hover:bg-[#111b2d] hover:text-[color:var(--accent-text-soft)]'
                    }`}
                  >
                    <span className="font-medium tracking-tight">{option.label}</span>
                    <span className="text-[11px] uppercase tracking-[0.28em] text-slate-500">{option.value}</span>
                  </button>
                );
              })}
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}


export function SymbolInput({
  value,
  onChange,
  onRequestPick,
  placeholder = 'Symbol',
  className = '',
}) {
  const displayValue = (value || '').toString();

  return (
    <div
      className={`flex min-w-[14rem] flex-col gap-3 rounded-2xl border border-white/12 bg-gradient-to-br from-[#0f172a]/95 via-[#0b1220]/95 to-[#060a12]/95 p-5 shadow-lg shadow-black/30 ${className}`}
    >
      <span className="text-[11px] font-medium uppercase tracking-[0.28em] text-slate-400/80">Symbol</span>
      <div className="flex items-center gap-2 rounded-xl border border-white/10 bg-[#0b1324]/90 px-3 py-2">
        <input
          type="text"
          value={displayValue}
          onChange={(event) => onChange?.(event.target.value)}
          placeholder={placeholder}
          className="flex-1 border-none bg-transparent text-sm font-semibold uppercase tracking-[0.28em] text-slate-100 placeholder:text-slate-600 focus:outline-none"
        />
        <button
          type="button"
          onClick={() => onRequestPick?.()}
          className="inline-flex items-center gap-2 rounded-lg border border-white/12 bg-[#10192d]/90 px-3 py-1 text-[10px] font-semibold uppercase tracking-[0.32em] text-slate-200 transition hover:border-[color:var(--accent-alpha-40)] hover:bg-[color:var(--accent-alpha-18)] hover:text-[color:var(--accent-text-strong)] focus-visible:outline focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-[color:var(--accent-outline)]"
        >
          <span className="rounded border border-white/20 bg-white/5 px-1 py-[1px] text-[9px]">/</span>
          Presets
        </button>
      </div>
      <span className="text-[11px] text-slate-500">
        Press <kbd className="rounded border border-white/15 bg-white/5 px-1 py-0.5 text-[10px] text-slate-200">/</kbd> to search
      </span>
    </div>
  );
}
