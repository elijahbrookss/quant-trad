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
      <span className="text-[11px] uppercase tracking-[0.24em] text-neutral-500">Timeframe</span>
      <div className="relative">
        <button
          type="button"
          onClick={toggle}
          aria-haspopup="listbox"
          aria-expanded={open}
          className="flex w-full items-center justify-between rounded-lg border border-neutral-800 bg-neutral-950/60 px-3 py-2 text-sm font-semibold text-neutral-200 transition hover:border-neutral-600 hover:text-neutral-50 focus-visible:outline focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-neutral-500"
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
          className={`absolute z-40 mt-2 w-full overflow-hidden rounded-xl border border-neutral-800 bg-neutral-950/95 shadow-xl backdrop-blur transition-all ${open ? 'max-h-80 opacity-100' : 'pointer-events-none max-h-0 opacity-0'}`}
        >
          <div className="divide-y divide-neutral-800/60">
            <div className="grid grid-cols-2 gap-px bg-neutral-900/60 p-2">
              {options.filter(o => o.featured).map(option => {
                const isActive = option.value === selected;
                return (
                  <button
                    key={option.value}
                    type="button"
                    onClick={() => handleSelect(option.value)}
                    className={`rounded-lg px-3 py-2 text-sm font-medium transition focus-visible:outline focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-neutral-500 ${isActive ? 'bg-neutral-800 text-neutral-50 ring-1 ring-neutral-500' : 'text-neutral-400 hover:bg-neutral-800/70 hover:text-neutral-100'}`}
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
                    className={`flex items-center justify-between rounded-lg px-3 py-2 text-sm transition focus-visible:outline focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-neutral-500 ${isActive ? 'bg-neutral-800 text-neutral-50 ring-1 ring-neutral-500' : 'text-neutral-400 hover:bg-neutral-800/70 hover:text-neutral-100'}`}
                  >
                    <span className="font-medium">{option.label}</span>
                    <span className="text-xs uppercase tracking-[0.25em] text-neutral-500">{option.value}</span>
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