// src/components/IndicatorCard.jsx
import React, { Fragment, useMemo, useState } from 'react';
import {
  Switch,
  Popover,
  PopoverButton,
  PopoverPanel,
  Transition,
  Menu,
  MenuButton,
  MenuItems,
  MenuItem,
} from '@headlessui/react';
import { MoreVertical, Copy, Edit3, Trash2 } from 'lucide-react';

const HIDE_KEYS = new Set(['symbol', 'interval', 'start', 'end', 'debug']);
const isAdvancedKey = (key) =>
  key.startsWith('ransac_') || key.includes('dedupe') || key.includes('max_windows') || key.includes('min_inliers');

/**
 * IndicatorCard
 *
 * Compact, readable card for indicators with many params.
 * - Shows name, type, enable switch, actions
 * - Color swatch popover
 * - Param pills for Essentials; "+N more" expands Advanced pills inline
 * - Copy JSON action
 *
 * Parent provides all actions so this stays dumb:
 *   onToggle(id)
 *   onEdit(indicator)
 *   onDelete(id)
 *   onGenerateSignals(id)
 *   onSelectColor(id, color)
 */

export default function IndicatorCard({
  indicator,
  color = '#60a5fa',
  colorSwatches = [
    '#facc15', '#b91c1c', '#f97316', '#a855f7', '#84cc16', '#6b7280',
    '#3b82f6', '#10b981', '#ec4899', '#14b8a6', '#eab308', '#f43f5e',
  ],
  onToggle,
  onEdit,
  onDelete,
  onGenerateSignals,
  onSelectColor,
  isGeneratingSignals = false,
  disableSignalAction = false,
}) {
  const [showAdvanced, setShowAdvanced] = useState(false);

  // Essentials first, advanced folded
  const { essentials, advanced } = useMemo(() => {
    const entries = Object.entries(indicator?.params || {})
      .filter(([k, v]) => !HIDE_KEYS.has(k) && v !== undefined && v !== null && String(v) !== '');

    const ess = [];
    const adv = [];
    for (const [k, v] of entries) {
      (isAdvancedKey(k) ? adv : ess).push([k, v]);
    }

    // keep essentials stable by name
    ess.sort((a, b) => a[0].localeCompare(b[0]));
    adv.sort((a, b) => a[0].localeCompare(b[0]));
    return { essentials: ess, advanced: adv };
  }, [indicator?.params]);

  const formatType = (value) => {
    if (!value) return '';
    return value
      .split(/[_-]+/)
      .filter(Boolean)
      .map((part) => part.charAt(0).toUpperCase() + part.slice(1))
      .join(' ');
  };

  const formatVal = (v) => {
    if (Array.isArray(v)) return v.join(',');
    if (typeof v === 'boolean') return v ? 'on' : 'off';
    if (typeof v === 'number') {
      // trim unhelpful decimals
      const s = v.toFixed(6);
      return s.replace(/\.0+$/, '').replace(/(\.\d*?)0+$/, '$1');
    }
    return String(v);
  };

  const copyParams = async () => {
    try {
      await navigator.clipboard.writeText(JSON.stringify(indicator?.params ?? {}, null, 2));
    } catch {
      // clipboard unavailable
    }
  };

  return (
    <div className="rounded-2xl border border-white/10 bg-[#1d1e26]/80 p-5 shadow-[0_25px_60px_-40px_rgba(0,0,0,0.85)]">
      <div className="flex flex-col gap-4">
        <div className="flex items-start justify-between gap-4">
          <div className="min-w-0 space-y-2">
            <div className="flex items-center gap-3">
              <div className="min-w-0">
                <p className="truncate text-sm font-semibold text-slate-100" title={indicator?.name}>{indicator?.name}</p>
                <p className="text-xs uppercase tracking-[0.28em] text-slate-500">{formatType(indicator?.type)}</p>
              </div>
              <Popover className="relative">
                {({ close }) => (
                  <>
                    <PopoverButton
                      className="h-6 w-6 rounded-md border border-slate-600/60 shadow-[inset_0_0_0_1px_rgba(255,255,255,0.05)]"
                      style={{ backgroundColor: color }}
                      title="Set color"
                    />
                    <Transition
                      enter="transition ease-out duration-100"
                      enterFrom="opacity-0 translate-y-1"
                      enterTo="opacity-100 translate-y-0"
                      leave="transition ease-in duration-75"
                      leaveFrom="opacity-100 translate-y-0"
                      leaveTo="opacity-0 translate-y-1"
                    >
                      <PopoverPanel className="absolute left-0 top-full z-20 mt-2 rounded-xl border border-white/10 bg-[#1a1b22] p-3 shadow-xl">
                        <div className="grid grid-cols-6 gap-2">
                          {colorSwatches.map((c) => (
                            <button
                              key={c}
                              className="h-6 w-6 rounded-md border border-white/10 focus:outline-none focus:ring-2 focus:ring-purple-400/60"
                              style={{ backgroundColor: c }}
                              onClick={() => {
                                onSelectColor?.(indicator.id, c);
                                close();
                              }}
                              aria-label={`Set color ${c}`}
                            />
                          ))}
                        </div>
                      </PopoverPanel>
                    </Transition>
                  </>
                )}
              </Popover>
            </div>

            <div className="flex flex-wrap gap-2">
              {essentials.map(([k, v]) => (
                <span key={k} className="inline-flex items-center gap-1 rounded-full border border-white/10 bg-white/5 px-2.5 py-1 text-[11px] text-slate-200">
                  <span className="text-slate-400">{k}</span>
                  <span>=</span>
                  <span className="font-medium text-slate-100/90">{formatVal(v)}</span>
                </span>
              ))}

              {advanced.length > 0 && !showAdvanced && (
                <button
                  className="inline-flex items-center gap-1 rounded-full border border-purple-500/20 bg-purple-500/10 px-2.5 py-1 text-[11px] text-purple-200 transition hover:border-purple-400/30 hover:bg-purple-500/20"
                  onClick={() => setShowAdvanced(true)}
                >
                  +{advanced.length} more
                </button>
              )}
            </div>
          </div>

          <div className="flex items-center gap-3">
            <Switch
              checked={!!indicator?.enabled}
              onChange={() => onToggle?.(indicator.id)}
              className={`${indicator?.enabled ? 'bg-purple-500' : 'bg-slate-600'} relative inline-flex h-7 w-12 items-center rounded-full transition`}
            >
              <span className={`${indicator?.enabled ? 'translate-x-6' : 'translate-x-1'} inline-block h-5 w-5 transform rounded-full bg-white transition`} />
            </Switch>

            <button
              type="button"
              onClick={() => onGenerateSignals?.(indicator.id)}
              className={`relative flex h-9 w-9 items-center justify-center rounded-full border border-emerald-500/40 text-emerald-200 transition ${
                disableSignalAction ? 'cursor-not-allowed opacity-40' : 'hover:border-emerald-400 hover:text-emerald-100'
              }`}
              title={isGeneratingSignals ? 'Generating…' : 'Generate signals'}
              disabled={disableSignalAction || isGeneratingSignals}
              aria-busy={isGeneratingSignals}
            >
              {isGeneratingSignals ? (
                <svg className="size-4 animate-spin" viewBox="0 0 24 24" role="status" aria-label="Generating signals">
                  <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4" fill="none" />
                  <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8v4a4 4 0 00-4 4H4z" />
                </svg>
              ) : (
                <svg xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" strokeWidth="1.6" stroke="currentColor" className="size-5">
                  <path strokeLinecap="round" strokeLinejoin="round" d="M21 12a9 9 0 1 1-18 0 9 9 0 0 1 18 0Z" />
                  <path strokeLinecap="round" strokeLinejoin="round" d="M15.91 11.672a.375.375 0 0 1 0 .656l-5.603 3.113a.375.375 0 0 1-.557-.328V8.887c0-.286.307-.466.557-.327l5.603 3.112Z" />
                </svg>
              )}
            </button>

            <Menu as="div" className="relative">
              <MenuButton
                className="flex h-9 w-9 items-center justify-center rounded-full border border-white/10 text-slate-400 transition hover:border-white/20 hover:text-slate-200"
                aria-label="More actions"
              >
                <MoreVertical className="size-4" />
              </MenuButton>
              <Transition
                as={Fragment}
                enter="transition ease-out duration-100"
                enterFrom="opacity-0 translate-y-1"
                enterTo="opacity-100 translate-y-0"
                leave="transition ease-in duration-75"
                leaveFrom="opacity-100 translate-y-0"
                leaveTo="opacity-0 translate-y-1"
              >
                <MenuItems className="absolute right-0 z-40 mt-2 w-44 overflow-hidden rounded-xl border border-white/10 bg-[#1a1b22] text-sm text-slate-200 shadow-xl">
                  <MenuItem>
                    {({ active }) => (
                      <button
                        onClick={() => onEdit?.(indicator)}
                        className={`flex w-full items-center gap-2 px-4 py-2 text-left ${active ? 'bg-white/10 text-slate-100' : ''}`}
                      >
                        <Edit3 className="size-4" />
                        Edit configuration
                      </button>
                    )}
                  </MenuItem>
                  <MenuItem>
                    {({ active }) => (
                      <button
                        onClick={copyParams}
                        className={`flex w-full items-center gap-2 px-4 py-2 text-left ${active ? 'bg-white/10 text-slate-100' : ''}`}
                      >
                        <Copy className="size-4" />
                        Copy params JSON
                      </button>
                    )}
                  </MenuItem>
                  <MenuItem>
                    {({ active }) => (
                      <button
                        onClick={() => onDelete?.(indicator.id)}
                        className={`flex w-full items-center gap-2 px-4 py-2 text-left text-rose-300 ${active ? 'bg-rose-500/20 text-rose-100' : ''}`}
                      >
                        <Trash2 className="size-4" />
                        Delete indicator
                      </button>
                    )}
                  </MenuItem>
                </MenuItems>
              </Transition>
            </Menu>
          </div>
        </div>

        {showAdvanced && (
          <div className="flex flex-wrap gap-2 border-t border-white/5 pt-3">
            {advanced.map(([k, v]) => (
              <span key={k} className="inline-flex items-center gap-1 rounded-full border border-white/5 bg-white/5 px-2.5 py-1 text-[11px] text-slate-300">
                <span className="text-slate-500">{k}</span>
                <span>=</span>
                <span>{formatVal(v)}</span>
              </span>
            ))}
            <button
              className="inline-flex items-center gap-1 rounded-full border border-slate-600/40 bg-slate-700/30 px-2.5 py-1 text-[11px] text-slate-200 transition hover:border-slate-500/60 hover:bg-slate-700/50"
              onClick={() => setShowAdvanced(false)}
            >
              Hide extras
            </button>
          </div>
        )}
      </div>
    </div>
  );
}
