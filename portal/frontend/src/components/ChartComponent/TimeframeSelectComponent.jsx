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
    <div className="flex min-w-[13rem] flex-col gap-2">
      <span className="text-[11px] uppercase tracking-[0.24em] text-zinc-400">Timeframe</span>
      <div className="relative">
        <button
          type="button"
          onClick={toggle}
          aria-haspopup="listbox"
          aria-expanded={open}
          className="flex w-full items-center justify-between rounded-lg border border-zinc-300 bg-zinc-50 px-3 py-2 text-sm font-semibold text-zinc-700 transition hover:border-zinc-400 hover:bg-white focus-visible:outline focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-zinc-400"
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
          className={`absolute z-40 mt-2 w-full overflow-hidden rounded-xl border border-zinc-200 bg-white shadow-lg transition-all ${open ? 'max-h-80 opacity-100' : 'pointer-events-none max-h-0 opacity-0'}`}
        >
          <div className="divide-y divide-zinc-100">
            <div className="grid grid-cols-2 gap-px bg-zinc-100 p-2">
              {options.filter(o => o.featured).map(option => {
                const isActive = option.value === selected;
                return (
                  <button
                    key={option.value}
                    type="button"
                    onClick={() => handleSelect(option.value)}
                    className={`rounded-lg px-3 py-2 text-sm font-medium transition focus-visible:outline focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-zinc-400 ${isActive ? 'bg-zinc-200 text-zinc-900 ring-1 ring-zinc-300' : 'text-zinc-600 hover:bg-zinc-100 hover:text-zinc-900'}`}
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
                    className={`flex items-center justify-between rounded-lg px-3 py-2 text-sm transition focus-visible:outline focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-zinc-400 ${isActive ? 'bg-zinc-200 text-zinc-900 ring-1 ring-zinc-300' : 'text-zinc-600 hover:bg-zinc-100 hover:text-zinc-900'}`}
                  >
                    <span className="font-medium">{option.label}</span>
                    <span className="text-xs uppercase tracking-[0.25em] text-zinc-400">{option.value}</span>
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