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
export function TimeframeSelect({ selected, onChange }) {
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
    <div className="flex min-w-[14rem] flex-col gap-3 rounded-2xl border border-slate-700/60 bg-slate-900/50 p-4 shadow-[0_18px_50px_-30px_rgba(0,0,0,0.85)]">
      <span className="text-[11px] uppercase tracking-[0.2em] text-neutral-300">Timeframe</span>
      <div className="relative">
        <button
          type="button"
          onClick={toggle}
          aria-haspopup="listbox"
          aria-expanded={open}
          className="flex w-full items-center justify-between rounded-lg border border-slate-600/60 bg-slate-900/50 px-3 py-2 text-sm font-semibold text-slate-100 transition hover:border-[color:var(--accent-alpha-40)] focus-visible:outline focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-[color:var(--accent-outline)]"
        >
          <span>{(activeOption?.label || activeOption?.value || '').toUpperCase()}</span>
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
          className={`absolute z-10 mt-2 w-full overflow-hidden rounded-xl border border-slate-700/70 bg-slate-900/95 shadow-lg backdrop-blur transition-all ${open ? 'max-h-80 opacity-100' : 'pointer-events-none max-h-0 opacity-0'}`}
        >
          <div className="divide-y divide-slate-800/80">
            <div className="grid grid-cols-2 gap-px bg-slate-800/70 p-2">
              {options.filter(o => o.featured).map(option => {
                const isActive = option.value === selected;
                return (
                  <button
                    key={option.value}
                    type="button"
                    onClick={() => handleSelect(option.value)}
                    className={`rounded-lg px-3 py-2 text-sm font-medium transition focus-visible:outline focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-[color:var(--accent-outline)] ${isActive ? 'bg-[color:var(--accent-alpha-20)] text-[color:var(--accent-text-strong)] ring-1 ring-[color:var(--accent-ring-strong)]' : 'text-slate-200 hover:bg-[color:var(--accent-alpha-10)] hover:text-[color:var(--accent-text-strong)]'}`}
                  >
                    {option.label}
                  </button>
                );
              })}
            </div>

            <div className="flex flex-col p-2">
              {options.filter(o => !o.featured).map(option => {
                const isActive = option.value === selected;
                return (
                  <button
                    key={option.value}
                    type="button"
                    onClick={() => handleSelect(option.value)}
                    className={`flex items-center justify-between rounded-lg px-3 py-2 text-sm transition focus-visible:outline focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-[color:var(--accent-outline)] ${isActive ? 'bg-indigo-500/20 text-indigo-100 ring-1 ring-indigo-400/60' : 'text-slate-200 hover:bg-indigo-500/10 hover:text-indigo-100'}`}
                  >
                    <span className="font-medium">{option.label}</span>
                    <span className="text-xs uppercase tracking-[0.25em] text-slate-400">{option.value}</span>
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


export function SymbolInput({ value, onChange, onRequestPick, placeholder = 'Symbol' }) {
  const displayValue = (value || '').toString();

  return (
    <div className="flex min-w-[14rem] flex-col gap-3 rounded-2xl border border-slate-700/60 bg-slate-900/50 p-4 shadow-[0_18px_50px_-30px_rgba(0,0,0,0.85)]">
      <span className="text-[11px] uppercase tracking-[0.2em] text-neutral-400">Symbol</span>
      <div className="flex items-center gap-2 rounded-xl border border-slate-600/60 bg-slate-900/60 px-3 py-2 shadow-[0_12px_32px_-24px_rgba(0,0,0,0.75)]">
        <input
          type="text"
          value={displayValue}
          onChange={(event) => onChange?.(event.target.value)}
          placeholder={placeholder}
          className="flex-1 border-none bg-transparent text-sm font-semibold uppercase tracking-[0.28em] text-slate-100 placeholder:text-slate-500 focus:outline-none"
        />
        <button
          type="button"
          onClick={() => onRequestPick?.()}
          className="inline-flex items-center gap-2 rounded-lg border border-slate-600/70 bg-slate-800/60 px-3 py-1 text-[10px] font-semibold uppercase tracking-[0.32em] text-slate-200 transition hover:border-[color:var(--accent-alpha-40)] hover:bg-[color:var(--accent-alpha-20)] hover:text-[color:var(--accent-text-strong)] focus-visible:outline focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-[color:var(--accent-outline)]"
        >
          <span className="rounded border border-slate-500/70 px-1 py-[1px] text-[9px]">/</span>
          Presets
        </button>
      </div>
      <span className="text-[11px] text-neutral-500">
        Press <kbd className="rounded border border-neutral-600 bg-neutral-800 px-1 py-0.5 text-[10px]">/</kbd> to search
      </span>
    </div>
  );
}
