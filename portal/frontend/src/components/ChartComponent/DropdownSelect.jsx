import { useEffect, useMemo, useRef, useState } from 'react';

/**
 * Renders a styled dropdown select that matches the QuantLab portal aesthetic.
 * Supports both flat option arrays and grouped option sets.
 */
export function DropdownSelect({
  label,
  value,
  onChange,
  options = [],
  placeholder = 'Select option',
  disabled = false,
  className = '',
}) {
  const [open, setOpen] = useState(false);
  const containerRef = useRef(null);

  const groups = useMemo(() => {
    if (!Array.isArray(options)) return [];
    if (options.length > 0 && options.every((item) => Array.isArray(item?.options))) {
      return options.map((group, index) => ({
        id: `group-${group.label ?? index}`,
        label: group.label,
        options: Array.isArray(group.options) ? group.options : [],
      }));
    }
    return [
      {
        id: 'group-default',
        label: null,
        options,
      },
    ];
  }, [options]);

  const selectedOption = useMemo(() => {
    for (const group of groups) {
      for (const option of group.options) {
        if (option?.value === value) return option;
      }
    }
    return null;
  }, [groups, value]);

  useEffect(() => {
    if (!open) return undefined;

    const handleClick = (event) => {
      if (!containerRef.current) return;
      if (!containerRef.current.contains(event.target)) {
        setOpen(false);
      }
    };

    const handleKey = (event) => {
      if (event.key === 'Escape') {
        setOpen(false);
      }
    };

    document.addEventListener('mousedown', handleClick);
    document.addEventListener('keydown', handleKey);
    return () => {
      document.removeEventListener('mousedown', handleClick);
      document.removeEventListener('keydown', handleKey);
    };
  }, [open]);

  const displayLabel = selectedOption?.label ?? placeholder;
  const hasSelection = Boolean(selectedOption);

  const handleSelect = (optionValue, optionDisabled = false) => {
    if (disabled || optionDisabled) return;
    if (typeof onChange === 'function') {
      onChange(optionValue);
    }
    setOpen(false);
  };

  return (
    <div ref={containerRef} className={`flex flex-col gap-2 ${className}`}>
      {label ? (
        <span className="text-[11px] uppercase tracking-[0.2em] text-neutral-400">{label}</span>
      ) : null}
      <div className="relative">
        <button
          type="button"
          disabled={disabled}
          onClick={() => setOpen((prev) => !prev)}
          aria-haspopup="listbox"
          aria-expanded={open}
          className={`flex w-full items-center justify-between rounded-lg border border-white/15 bg-[#141824]/85 px-3 py-2 text-sm font-medium text-slate-200 transition focus-visible:outline focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-[color:var(--accent-outline)] ${
            disabled
              ? 'cursor-not-allowed opacity-60'
              : 'hover:border-[color:var(--accent-alpha-30)] hover:bg-[#192236]'
          } ${open ? 'border-[color:var(--accent-alpha-40)] bg-[#1b263c] shadow-lg shadow-black/40' : ''}`}
        >
          <span className={`${hasSelection ? 'text-slate-100' : 'text-slate-400'}`}>
            {displayLabel}
          </span>
          <svg
            className={`h-4 w-4 text-slate-300 transition-transform ${open ? 'rotate-180' : ''}`}
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
          className={`absolute z-20 mt-2 w-full overflow-hidden rounded-xl border border-white/12 bg-[#0e111c]/95 shadow-[0_18px_48px_rgba(0,0,0,0.45)] backdrop-blur transition-all ${
            open ? 'max-h-96 opacity-100' : 'pointer-events-none max-h-0 opacity-0'
          }`}
        >
          <div className="divide-y divide-white/10">
            {groups.map((group) => (
              <div key={group.id} className="flex flex-col p-2">
                {group.label ? (
                  <span className="px-2 pb-2 text-[10px] font-semibold uppercase tracking-[0.28em] text-slate-500">
                    {group.label}
                  </span>
                ) : null}
                {group.options.map((option) => {
                  const isActive = option?.value === value;
                  const isOptionDisabled = Boolean(option?.disabled);
                  return (
                    <button
                      key={option.value}
                      type="button"
                      role="option"
                      aria-selected={isActive}
                      aria-disabled={isOptionDisabled}
                      disabled={isOptionDisabled}
                      onClick={() => handleSelect(option.value, isOptionDisabled)}
                      className={`flex w-full items-center justify-between rounded-lg px-3 py-2 text-left text-sm transition focus-visible:outline focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-[color:var(--accent-outline)] ${
                        isOptionDisabled
                          ? 'cursor-not-allowed text-slate-500'
                          : isActive
                              ? 'bg-[color:var(--accent-alpha-20)] text-[color:var(--accent-text-strong)] ring-1 ring-[color:var(--accent-ring-strong)]'
                              : 'text-slate-200 hover:bg-[color:var(--accent-alpha-12)] hover:text-[color:var(--accent-text-soft)]'
                      }`}
                    >
                      <span className="flex flex-col">
                        <span className="font-medium tracking-tight">{option.label}</span>
                        {option.description ? (
                          <span className="text-[11px] text-slate-400">{option.description}</span>
                        ) : null}
                      </span>
                      {option.badge ? (
                        <span className="rounded-full border border-slate-700/70 bg-slate-800/60 px-2 py-0.5 text-[10px] font-semibold uppercase tracking-[0.28em] text-slate-300">
                          {option.badge}
                        </span>
                      ) : null}
                    </button>
                  );
                })}
              </div>
            ))}
          </div>
        </div>
      </div>
    </div>
  );
}

export default DropdownSelect;
