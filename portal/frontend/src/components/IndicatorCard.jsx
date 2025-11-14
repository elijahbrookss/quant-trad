// src/components/IndicatorCard.jsx
import React, { Fragment, useMemo, useState } from "react";
import { Switch, Popover, PopoverButton, PopoverPanel, Transition } from "@headlessui/react";
import { MoreHorizontal, Copy } from "lucide-react";

const HIDE_KEYS = new Set(["symbol", "interval", "start", "end", "debug"]);
const isAdvancedKey = (key) =>
  key.startsWith("ransac_") || key.includes("dedupe") || key.includes("max_windows") || key.includes("min_inliers");

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
  onDuplicate,
  onGenerateSignals,
  onSelectColor,
  isGeneratingSignals = false,
  disableSignalAction = false,
  selected = false,
  onSelectionToggle,
  duplicatePending = false,
}) {
  const [showAllParams, setShowAllParams] = useState(false);
  const [confirmingDelete, setConfirmingDelete] = useState(false);
  const cardBorderClass = selected
    ? 'border border-[color:var(--accent-alpha-60)] ring-1 ring-[color:var(--accent-alpha-25)] shadow-[0_25px_60px_-45px_var(--accent-shadow-strong)]'
    : 'border border-white/10';
  const handleSelectionClick = () => {
    if (typeof onSelectionToggle === 'function') {
      onSelectionToggle();
    }
  };
  const duplicateDisabled = duplicatePending || typeof onDuplicate !== 'function';

  const paramsList = useMemo(() => {
    const entries = Object.entries(indicator?.params || {})
      .filter(([k, v]) => !HIDE_KEYS.has(k) && v !== undefined && v !== null && String(v) !== "");

    const essentials = [];
    const advanced = [];

    for (const [key, value] of entries) {
      const payload = { key, value, isAdvanced: isAdvancedKey(key) };
      (payload.isAdvanced ? advanced : essentials).push(payload);
    }

    essentials.sort((a, b) => a.key.localeCompare(b.key));
    advanced.sort((a, b) => a.key.localeCompare(b.key));

    return [...essentials, ...advanced];
  }, [indicator?.params]);

  const visibleParams = showAllParams ? paramsList : paramsList.slice(0, 5);
  const hiddenCount = Math.max(paramsList.length - visibleParams.length, 0);

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
    const raw = indicator?.type;
    if (!raw) return "Custom";
    return raw
      .split(/[_-]+/)
      .filter(Boolean)
      .map((token) => token.charAt(0).toUpperCase() + token.slice(1))
      .join(" ");
  }, [indicator?.type]);

  return (
    <div className={`flex items-start justify-between gap-4 rounded-2xl bg-[#1f2230]/80 p-4 shadow-[0_20px_60px_-40px_rgba(0,0,0,0.85)] ${cardBorderClass}`}>
      <div className="min-w-0 flex-1 space-y-3">
        <div className="flex items-start gap-3">
          <button
            type="button"
            onClick={handleSelectionClick}
            className={`mt-0.5 inline-flex h-5 w-5 items-center justify-center rounded-full border text-[10px] font-semibold uppercase tracking-[0.2em] transition ${
              selected
                ? 'border-[color:var(--accent-alpha-70)] bg-[color:var(--accent-alpha-25)] text-[color:var(--accent-text-strong)]'
                : 'border-white/15 text-slate-400 hover:border-white/30'
            }`}
            aria-pressed={selected}
            aria-label={selected ? 'Deselect indicator' : 'Select indicator'}
          >
            {selected ? '✓' : ''}
          </button>
          <span
            className="mt-0.5 inline-flex h-5 w-5 items-center justify-center rounded-full border border-white/15 bg-[#131621] shadow-inner"
            aria-hidden="true"
          >
            <span className="h-2.5 w-2.5 rounded-full border border-white/25" style={{ backgroundColor: color }} />
          </span>
          <div className="min-w-0 flex-1">
            <div className="truncate text-base font-semibold text-slate-100" title={indicator?.name}>
              {indicator?.name}
            </div>
            <p className="mt-1 text-[11px] font-medium uppercase tracking-[0.28em] text-[color:var(--accent-text-soft-alpha)]">
              {typeLabel}
            </p>
          </div>
        </div>

        {visibleParams.length > 0 && (
          <div className="flex flex-wrap gap-1 text-xs text-slate-300">
            {visibleParams.map(({ key, value, isAdvanced }) => (
              <span
                key={key}
                className={`inline-flex items-center gap-1 rounded-full border px-2 py-0.5 ${
                  isAdvanced
                  ? 'border-[color:var(--accent-alpha-30)] bg-[color:var(--accent-alpha-10)] text-[color:var(--accent-text-strong-alpha)]'
                    : 'border-white/10 bg-white/5 text-slate-200'
                }`}
              >
                <span className={isAdvanced ? 'text-[color:var(--accent-text-soft-alpha)]' : 'text-slate-400'}>{key}</span>
                <span>= {formatVal(value)}</span>
              </span>
            ))}

            {hiddenCount > 0 && !showAllParams && (
              <button
                className="inline-flex items-center gap-1 rounded-full border border-white/15 bg-white/5 px-2 py-0.5 text-xs text-slate-200 transition hover:border-[color:var(--accent-alpha-40)] hover:bg-[color:var(--accent-alpha-15)] hover:text-[color:var(--accent-text-strong)]"
                onClick={() => setShowAllParams(true)}
              >
                +{hiddenCount} more
              </button>
            )}
          </div>
        )}

        {showAllParams && paramsList.length > 5 && (
          <button
            className="inline-flex items-center gap-1 text-xs text-slate-300 underline-offset-4 transition hover:text-[color:var(--accent-text-strong)] hover:underline"
            onClick={() => setShowAllParams(false)}
          >
            Show less
          </button>
        )}
      </div>

      <div className="flex shrink-0 items-center gap-3">
        <Switch
          checked={!!indicator?.enabled}
          onChange={() => onToggle?.(indicator.id)}
          className={`${indicator?.enabled ? 'bg-[color:var(--accent-alpha-80)]' : 'bg-slate-600/70'} relative inline-flex h-6 w-11 cursor-pointer items-center rounded-full transition`}
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
                className="flex h-9 w-9 items-center justify-center rounded-full border border-white/10 bg-white/5 text-slate-300 transition hover:border-[color:var(--accent-alpha-40)] hover:bg-[color:var(--accent-alpha-20)] hover:text-[color:var(--accent-text-strong)]"
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
                            className="h-5 w-5 rounded-sm border border-white/20 transition hover:border-[color:var(--accent-alpha-60)] focus:outline-none focus:ring-2 focus:ring-[color:var(--accent-outline-soft)]"
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
                        className="flex items-center justify-between rounded-lg border border-white/10 bg-white/5 px-3 py-2 text-left text-slate-200 transition hover:border-[color:var(--accent-alpha-30)] hover:bg-[color:var(--accent-alpha-10)]"
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
                        className="flex items-center justify-between rounded-lg border border-white/10 bg-white/5 px-3 py-2 text-left text-slate-200 transition hover:border-[color:var(--accent-alpha-30)] hover:bg-[color:var(--accent-alpha-10)]"
                      >
                        <span className="inline-flex items-center gap-2"><Copy className="size-4" /> Copy params JSON</span>
                      </button>

                      <button
                        onClick={() => {
                          if (duplicateDisabled) return
                          onDuplicate?.(indicator.id)
                          setConfirmingDelete(false)
                          close()
                        }}
                        disabled={duplicateDisabled}
                        className={`flex items-center justify-between rounded-lg border px-3 py-2 text-left text-slate-200 transition ${
                          duplicateDisabled
                            ? 'cursor-not-allowed border-white/5 text-slate-500'
                            : 'border-white/10 bg-white/5 hover:border-[color:var(--accent-alpha-30)] hover:bg-[color:var(--accent-alpha-10)]'
                        }`}
                      >
                        <span>{duplicatePending ? 'Duplicating…' : 'Duplicate indicator'}</span>
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
