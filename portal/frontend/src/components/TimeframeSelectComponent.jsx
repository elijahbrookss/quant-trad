import React, { useState, useRef, useEffect } from 'react';

/**
 * Available timeframes:
 * '1m', '5m', '15m', '30m', '1h', '4h', '1d', '1w', '1M'
 */
const options = [
  { label: '1 Minute', value: '1m' },
  { label: '5 Minutes', value: '5m' },
  { label: '15 Minutes', value: '15m' },
  { label: '30 Minutes', value: '30m' },
  { label: '1 Hour', value: '1h' },
  { label: '4 Hours', value: '4h' },
  { label: '1 Day', value: '1d' },
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
export function TimeframeSelect({ selected, onChange, placeholder = 'Select timeframe...' }) {
  const [isOpen, setIsOpen] = useState(false);
  const containerRef = useRef(null);

  // Close dropdown on outside click
  useEffect(() => {
    const handleClickOutside = (e) => {
      if (containerRef.current && !containerRef.current.contains(e.target)) {
        setIsOpen(false);
      }
    };
    document.addEventListener('mousedown', handleClickOutside);
    return () => document.removeEventListener('mousedown', handleClickOutside);
  }, []);

  const toggleOpen = () => setIsOpen(prev => !prev);

  const handleSelect = (option) => {
    onChange(option.value);
    setIsOpen(false);
  };

  const selectedOption = options.find(o => o.value === selected);

  return (
    <div ref={containerRef} className="relative inline-block w-40">
      <button
        type="button"
        onClick={toggleOpen}
        className="w-full text-left bg-neutral-800 border border-neutral-600 rounded-md shadow-sm px-2 py-2 focus:outline-none focus:ring-2 focus:ring-indigo-500"
      >
        {selectedOption ? selectedOption.label : placeholder}
        <span className="float-right ml-2">▽</span>
        {/* ▼◿⌟ⅴ∇∨⋁⋎⨈⩔⩒⩖⩛⩢ */}
      </button>

      {isOpen && (
        <ul className="absolute z-10 mt-1 w-full bg-neutral-800 border border-neutral-600 rounded-md shadow-lg max-h-60 overflow-auto">
          {options.map(option => (
            <li
              key={option.value}
              onClick={() => handleSelect(option)}
              className="px-4 py-2 hover:bg-indigo-600 hover:text-white cursor-pointer"
            >
              {option.label}
            </li>
          ))}
        </ul>
      )}
    </div>
  );
}


export function SymbolInput({ value, onChange, placeholder = 'Symbol' }) {
  return (
    <input
      type="text"
      className="w-40 bg-neutral-800 border border-neutral-600 rounded-md shadow-sm px-4 py-2 focus:outline-none focus:ring-2 focus:ring-indigo-500"
      value={value}
      onChange={(e) => onChange(e.target.value)}
      placeholder={placeholder}
    />
  );
}
