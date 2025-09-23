import React from 'react';

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
  return (
    <div className="flex flex-col gap-2 min-w-[13rem]">
      <span className="text-[11px] uppercase tracking-[0.2em] text-neutral-400">Timeframe</span>
      <div className="flex flex-wrap gap-2">
        {options.map(option => {
          const isActive = option.value === selected;
          const baseClass = 'rounded-md border px-3 py-1.5 text-sm font-medium transition focus-visible:outline focus-visible:outline-2 focus-visible:outline-offset-2';
          const activeClass = option.featured
            ? 'border-sky-400 bg-sky-500/20 text-sky-100'
            : 'border-indigo-400 bg-indigo-500/20 text-indigo-100';
          const inactiveClass = option.featured
            ? 'border-neutral-800 bg-neutral-900/70 text-neutral-200 hover:border-sky-400 hover:text-sky-200'
            : 'border-neutral-800 bg-neutral-900/60 text-neutral-300 hover:border-indigo-400 hover:text-indigo-200';

          return (
            <button
              key={option.value}
              type="button"
              title={option.label}
              aria-pressed={isActive}
              onClick={() => onChange(option.value)}
              className={`${baseClass} ${isActive ? activeClass : inactiveClass}`}
            >
              {option.value.toUpperCase()}
            </button>
          );
        })}
      </div>
    </div>
  );
}


export function SymbolInput({ value, onChange, placeholder = 'Symbol' }) {
  return (
    <div className="flex flex-col gap-2 min-w-[10rem]">
      <span className="text-[11px] uppercase tracking-[0.2em] text-neutral-400">Symbol</span>
      <div className="relative flex items-center">
        <span className="pointer-events-none absolute left-3 text-neutral-500">
          <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth={1.5} className="h-4 w-4">
            <path strokeLinecap="round" strokeLinejoin="round" d="m19 19-3.5-3.5m1-4.5a5.5 5.5 0 1 1-11 0 5.5 5.5 0 0 1 11 0Z" />
          </svg>
        </span>
        <input
          type="text"
          inputMode="text"
          spellCheck={false}
          autoCapitalize="characters"
          autoComplete="off"
          className="w-40 rounded-md border border-neutral-800 bg-neutral-900/70 py-2 pl-9 pr-3 text-sm font-semibold uppercase tracking-wide text-neutral-100 outline-none transition focus:border-sky-400 focus:ring-1 focus:ring-sky-500/50"
          value={value}
          onChange={(e) => onChange(e.target.value.toUpperCase())}
          placeholder={placeholder}
        />
      </div>
    </div>
  );
}