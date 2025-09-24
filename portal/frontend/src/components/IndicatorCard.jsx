// src/components/IndicatorCard.jsx
import React, { Fragment, useMemo, useState } from "react";
import { Switch, Popover, PopoverButton, PopoverPanel, Transition } from "@headlessui/react";
import { MoreHorizontal, Copy } from "lucide-react";

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
  color = "#60a5fa",
  colorSwatches = [
    "#facc15", "#b91c1c", "#f97316", "#a855f7", "#84cc16", "#6b7280",
    "#3b82f6", "#10b981", "#ec4899", "#14b8a6", "#eab308", "#f43f5e"
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
  const [confirmingDelete, setConfirmingDelete] = useState(false);

  // Heuristics for which params to hide or mark advanced
  const HIDE_KEYS = new Set(["symbol", "interval", "start", "end", "debug"]);
  const isAdvanced = (k) =>
    k.startsWith("ransac_") || k.includes("dedupe") || k.includes("max_windows") || k.includes("min_inliers");

  // Essentials first, advanced folded
  const { essentials, advanced } = useMemo(() => {
    const entries = Object.entries(indicator?.params || {})
      .filter(([k, v]) => !HIDE_KEYS.has(k) && v !== undefined && v !== null && String(v) !== "");

    const ess = [];
    const adv = [];
    for (const [k, v] of entries) {
      (isAdvanced(k) ? adv : ess).push([k, v]);
    }

    // keep essentials stable by name
    ess.sort((a, b) => a[0].localeCompare(b[0]));
    adv.sort((a, b) => a[0].localeCompare(b[0]));
    return { essentials: ess, advanced: adv };
  }, [indicator?.params]);

  const formatVal = (v) => {
    if (Array.isArray(v)) return v.join(",");
    if (typeof v === "boolean") return v ? "on" : "off";
    if (typeof v === "number") {
      // trim unhelpful decimals
      const s = v.toFixed(6);
      return s.replace(/\.0+$/, "").replace(/(\.\d*?)0+$/, "$1");
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

  const typeLabel = useMemo(() => {
    const raw = indicator?.type
    if (!raw) return 'Custom'
    return raw
      .split(/[_-]+/)
      .filter(Boolean)
      .map((token) => token.charAt(0).toUpperCase() + token.slice(1))
      .join(' ')
  }, [indicator?.type])

  return (
    <div className="flex items-start justify-between gap-4 rounded-2xl border border-white/10 bg-[#1f2230]/80 p-4 shadow-[0_20px_60px_-40px_rgba(0,0,0,0.85)]">
      <div className="min-w-0 flex-1 space-y-2">
        <div className="flex flex-wrap items-center gap-2">
          <div className="truncate text-sm font-semibold text-slate-100" title={indicator?.name}>{indicator?.name}</div>
          <span className="rounded-full border border-white/10 bg-white/5 px-2 py-0.5 text-[10px] uppercase tracking-[0.3em] text-slate-400">
            {typeLabel}
          </span>
          <span className="flex h-2 w-2 rounded-full border border-white/20" style={{ backgroundColor: color }} aria-hidden="true" />
        </div>

        <div className="flex flex-wrap gap-1 text-xs text-slate-300">
          {essentials.map(([k, v]) => (
            <span key={k} className="inline-flex items-center gap-1 rounded-full border border-white/10 bg-white/5 px-2 py-0.5">
              <span className="text-slate-400">{k}</span>
              <span>={formatVal(v)}</span>
            </span>
          ))}

          {advanced.length > 0 && !showAdvanced && (
            <button
              className="inline-flex items-center gap-1 rounded-full border border-white/10 bg-white/5 px-2 py-0.5 text-xs text-purple-200 transition hover:border-purple-400/40 hover:bg-purple-500/20"
              onClick={() => setShowAdvanced(true)}
            >
              +{advanced.length} more
            </button>
          )}
        </div>

        {showAdvanced && (
          <div className="mt-1 flex flex-wrap gap-1 text-xs text-slate-300">
            {advanced.map(([k, v]) => (
              <span key={k} className="inline-flex items-center gap-1 rounded-full border border-white/10 bg-[#1a1d27] px-2 py-0.5">
                <span className="text-slate-400">{k}</span>
                <span>={formatVal(v)}</span>
              </span>
            ))}
            <button
              className="inline-flex items-center gap-1 rounded-full border border-white/10 bg-[#1a1d27] px-2 py-0.5 text-xs text-slate-300 transition hover:border-white/20 hover:bg-[#202333]"
              onClick={() => setShowAdvanced(false)}
            >
              Show less
            </button>
          </div>
        )}
      </div>

      <div className="flex shrink-0 items-center gap-3">
        <Switch
          checked={!!indicator?.enabled}
          onChange={() => onToggle?.(indicator.id)}
          className={`${indicator?.enabled ? 'bg-purple-500/80' : 'bg-slate-600/70'} relative inline-flex h-6 w-11 cursor-pointer items-center rounded-full transition`}
        >
          <span className={`${indicator?.enabled ? 'translate-x-6' : 'translate-x-1'} inline-block h-4 w-4 transform rounded-full bg-white transition`} />
        </Switch>

        <button
          type="button"
          onClick={() => onGenerateSignals?.(indicator.id)}
          className={`relative flex h-8 w-8 items-center justify-center rounded-full border border-emerald-400/40 text-emerald-200 transition ${
            disableSignalAction ? 'cursor-not-allowed opacity-50' : 'hover:border-emerald-300/60 hover:text-emerald-100'
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

        <Popover className="relative">
          {({ close }) => (
            <>
              <PopoverButton
                onClick={() => setConfirmingDelete(false)}
                className="flex h-9 w-9 items-center justify-center rounded-full border border-white/10 bg-white/5 text-slate-300 transition hover:border-purple-400/40 hover:bg-purple-500/20 hover:text-purple-100"
                title="Indicator settings"
              >
                <MoreHorizontal className="size-4" />
              </PopoverButton>
              <Transition
                as={Fragment}
                enter="transition ease-out duration-100"
                enterFrom="opacity-0 translate-y-1"
                enterTo="opacity-100 translate-y-0"
                leave="transition ease-in duration-75"
                leaveFrom="opacity-100 translate-y-0"
                leaveTo="opacity-0 translate-y-1"
              >
                <PopoverPanel className="absolute right-0 top-full z-30 mt-3 w-56 rounded-2xl border border-white/10 bg-[#202432] p-4 shadow-xl ring-1 ring-black/20">
                  <div className="space-y-4 text-sm text-slate-200">
                    <div>
                      <p className="text-[10px] uppercase tracking-[0.3em] text-slate-400">Color</p>
                      <div className="mt-2 grid grid-cols-6 gap-1">
                        {colorSwatches.map((c) => (
                          <button
                            key={c}
                            className="h-5 w-5 rounded-sm border border-white/20 transition hover:border-purple-300/60 focus:outline-none focus:ring-2 focus:ring-purple-300/40"
                            style={{ backgroundColor: c }}
                            onClick={() => {
                              onSelectColor?.(indicator.id, c)
                              setConfirmingDelete(false)
                              close()
                            }}
                            aria-label={`Set color ${c}`}
                          />
                        ))}
                      </div>
                    </div>

                    <div className="grid gap-2 text-xs">
                      <button
                        onClick={() => {
                          onEdit?.(indicator)
                          setConfirmingDelete(false)
                          close()
                        }}
                        className="flex items-center justify-between rounded-lg border border-white/10 bg-white/5 px-3 py-2 text-left text-slate-200 transition hover:border-purple-400/30 hover:bg-purple-500/10"
                      >
                        <span>Edit parameters</span>
                        <span className="text-[10px] uppercase tracking-[0.3em] text-slate-500">E</span>
                      </button>

                      <button
                        onClick={async () => {
                          await copyParams()
                          setConfirmingDelete(false)
                          close()
                        }}
                        className="flex items-center justify-between rounded-lg border border-white/10 bg-white/5 px-3 py-2 text-left text-slate-200 transition hover:border-purple-400/30 hover:bg-purple-500/10"
                      >
                        <span className="inline-flex items-center gap-2"><Copy className="size-4" /> Copy params JSON</span>
                      </button>

                      {!confirmingDelete && (
                        <button
                          onClick={() => setConfirmingDelete(true)}
                          className="flex items-center justify-between rounded-lg border border-rose-500/30 bg-rose-500/10 px-3 py-2 text-left text-rose-200 transition hover:border-rose-400/50 hover:bg-rose-500/20"
                        >
                          <span>Delete indicator</span>
                        </button>
                      )}

                      {confirmingDelete && (
                        <div className="flex items-center justify-between gap-2 rounded-lg border border-rose-500/40 bg-rose-500/10 px-3 py-2 text-xs text-rose-100">
                          <span>Confirm delete?</span>
                          <div className="flex items-center gap-2">
                            <button
                              onClick={() => {
                                onDelete?.(indicator.id)
                                setConfirmingDelete(false)
                                close()
                              }}
                              className="rounded border border-rose-400/40 px-2 py-1 text-[11px] uppercase tracking-[0.2em] text-rose-200 hover:bg-rose-500/20"
                            >
                              Yes
                            </button>
                            <button
                              onClick={() => setConfirmingDelete(false)}
                              className="rounded border border-white/10 px-2 py-1 text-[11px] uppercase tracking-[0.2em] text-slate-300 hover:border-white/20"
                            >
                              No
                            </button>
                          </div>
                        </div>
                      )}
                    </div>
                  </div>
                </PopoverPanel>
              </Transition>
            </>
          )}
        </Popover>
      </div>
    </div>
  );
}
