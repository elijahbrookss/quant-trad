// src/components/IndicatorCard.jsx
import React, { Fragment, useMemo, useState } from "react";
import { Switch, Popover, PopoverButton, PopoverPanel, Transition } from "@headlessui/react";
import { MoreHorizontal, Copy, Info, ChevronDown } from "lucide-react";

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
}) {
  const [showAdvanced, setShowAdvanced] = useState(false);

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
    } catch {}
  };

  return (
    <div className="flex items-start justify-between gap-4 px-4 py-3 rounded-lg bg-neutral-900 shadow-lg">
      {/* Left: title + pills */}
      <div className="min-w-0 flex-1">
        <div className="flex items-center gap-2">
          <div className="font-medium text-white truncate" title={indicator?.name}>{indicator?.name}</div>

          {/* Color selector */}
          <Popover className="relative">
            {({ close }) => (
              <>
                <PopoverButton
                  className="h-4 w-4 rounded-sm border border-neutral-500 shadow-[inset_0_0_0_1px_rgba(255,255,255,.08)]"
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
                  <PopoverPanel className="absolute z-20 mt-2 rounded-md bg-neutral-800 p-2 shadow-lg ring-1 ring-black/20">
                    <div className="flex gap-2">
                      {colorSwatches.map((c) => (
                        <button
                          key={c}
                          className="h-5 w-5 rounded-sm border border-white/20 focus:outline-none focus:ring-2 focus:ring-white/40"
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
        <div className="text-sm text-gray-500">{indicator?.type}</div>

        {/* Pills */}
        <div className="mt-1 flex flex-wrap gap-1">
          {essentials.map(([k, v]) => (
            <span key={k} className="inline-flex items-center gap-1 rounded-full bg-neutral-800 text-neutral-300 border border-neutral-700 px-2 py-0.5 text-xs">
              <span className="text-neutral-400">{k}</span>
              <span>={formatVal(v)}</span>
            </span>
          ))}

          {/* Advanced fold */}
          {advanced.length > 0 && !showAdvanced && (
            <button
              className="inline-flex items-center gap-1 rounded-full bg-neutral-800 text-blue-300 border border-neutral-700 px-2 py-0.5 text-xs hover:bg-neutral-700"
              onClick={() => setShowAdvanced(true)}
            >
              +{advanced.length} more
            </button>
          )}
        </div>

        {showAdvanced && (
          <div className="mt-2 flex flex-wrap gap-1">
            {advanced.map(([k, v]) => (
              <span key={k} className="inline-flex items-center gap-1 rounded-full bg-neutral-900 text-neutral-300 border border-neutral-700 px-2 py-0.5 text-xs">
                <span className="text-neutral-500">{k}</span>
                <span>={formatVal(v)}</span>
              </span>
            ))}
            <button
              className="inline-flex items-center gap-1 rounded-full bg-neutral-900 text-neutral-300 border border-neutral-700 px-2 py-0.5 text-xs hover:bg-neutral-800"
              onClick={() => setShowAdvanced(false)}
            >
              Show less
            </button>
          </div>
        )}
      </div>

      {/* Right: actions */}
      <div className="flex items-center gap-3 shrink-0">
        {/* Enable/disable */}
        <Switch
          checked={!!indicator?.enabled}
          onChange={() => onToggle?.(indicator.id)}
          className={`${indicator?.enabled ? "bg-indigo-500" : "bg-gray-600"} relative inline-flex h-6 w-11 items-center rounded-full mouse-pointer`}
        >
          <span className={`${indicator?.enabled ? "translate-x-6" : "translate-x-1"} inline-block h-4 w-4 transform rounded-full bg-white transition mouse-pointer`} />
        </Switch>

        {/* Edit */}
        <button
          onClick={() => onEdit?.(indicator)}
          className="text-gray-400 hover:text-white mouse-pointer"
          title="Edit"
        >
          <svg xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" strokeWidth="1.6" stroke="currentColor" className="size-6 mouse-pointer">
            <path strokeLinecap="round" strokeLinejoin="round" d="m16.862 4.487 1.687-1.688a1.875 1.875 0 1 1 2.652 2.652L10.582 16.07a4.5 4.5 0 0 1-1.897 1.13L6 18l.8-2.685a4.5 4.5 0 0 1 1.13-1.897l8.932-8.931Zm0 0L19.5 7.125M18 14v4.75A2.25 2.25 0 0 1 15.75 21H5.25A2.25 2.25 0 0 1 3 18.75V8.25A2.25 2.25 0 0 1 5.25 6H10" />
          </svg>
        </button>

        {/* Generate Signals */}
        <button
          onClick={() => onGenerateSignals?.(indicator.id)}
          className="text-green-400 hover:text-green-200"
          title="Generate signals"
        >
          <svg xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" strokeWidth="1.6" stroke="currentColor" className="size-6">
            <path strokeLinecap="round" strokeLinejoin="round" d="M21 12a9 9 0 1 1-18 0 9 9 0 0 1 18 0Z" />
            <path strokeLinecap="round" strokeLinejoin="round" d="M15.91 11.672a.375.375 0 0 1 0 .656l-5.603 3.113a.375.375 0 0 1-.557-.328V8.887c0-.286.307-.466.557-.327l5.603 3.112Z" />
          </svg>
        </button>

        {/* Copy JSON */}
        <button onClick={copyParams} className="text-neutral-400 hover:text-neutral-100" title="Copy params JSON">
          <Copy className="size-5" />
        </button>

        {/* Delete with tiny confirm popover */}
        <Popover className="relative">
          {({ close }) => (
            <>
              <PopoverButton className="text-red-400 hover:text-red-200" title="Delete">
                <svg xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" strokeWidth="1.6" stroke="currentColor" className="size-6">
                  <path strokeLinecap="round" strokeLinejoin="round" d="m14.74 9-.346 9m-4.788 0L9.26 9m9.968-3.21c.342.052.682.107 1.022.166m-1.022-.165L18.16 19.673a2.25 2.25 0 0 1-2.244 2.077H8.084a2.25 2.25 0 0 1-2.244-2.077L4.772 5.79m14.456 0a48.108 48.108 0 0 0-3.478-.397m-12 .562c.34-.059.68-.114 1.022-.165m0 0a48.11 48.11 0 0 1 3.478-.397m7.5 0v-.916c0-1.18-.91-2.164-2.09-2.201a51.964 51.964 0 0 0-3.32 0c-1.18.037-2.09 1.022-2.09 2.201v.916m7.5 0a48.667 48.667 0 0 0-7.5 0" />
                </svg>
              </PopoverButton>
              <Transition
                as={Fragment}
                enter="transition ease-out duration-100"
                enterFrom="opacity-0 scale-95"
                enterTo="opacity-100 scale-100"
                leave="transition ease-in duration-75"
                leaveFrom="opacity-100 scale-100"
                leaveTo="opacity-0 scale-95"
              >
                <PopoverPanel className="absolute z-50 -top-2 right-0 -translate-y-full rounded-md border border-neutral-700 bg-neutral-900 shadow-xl p-1">
                  <div className="flex items-center gap-1">
                    <button
                      onClick={() => { onDelete?.(indicator.id); close(); }}
                      className="p-1 rounded hover:bg-green-600/20 text-green-400 hover:text-green-300"
                      aria-label="Confirm delete"
                    >
                      <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.8" className="size-5">
                        <path strokeLinecap="round" strokeLinejoin="round" d="M4.5 12.75l6 6 9-13.5"/>
                      </svg>
                    </button>
                    <PopoverButton
                      aria-label="Cancel"
                      className="p-1 rounded hover:bg-neutral-700 text-neutral-300 hover:text-white"
                    >
                      <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.8" className="size-5">
                        <path strokeLinecap="round" strokeLinejoin="round" d="M6 18L18 6M6 6l12 12"/>
                      </svg>
                    </PopoverButton>
                  </div>
                  <div className="absolute -bottom-1 right-3 w-2 h-2 bg-neutral-900 border-b border-r border-neutral-700 rotate-45" />
                </PopoverPanel>
              </Transition>
            </>
          )}
        </Popover>
      </div>
    </div>
  );
}
