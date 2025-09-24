// src/components/IndicatorCard.jsx
import React, { Fragment, useMemo, useState } from "react";
import {
  Switch,
  Popover,
  PopoverButton,
  PopoverPanel,
  Transition,
  Menu,
  MenuButton,
  MenuItem,
  MenuItems,
} from "@headlessui/react";
import { MoreHorizontal, CopyPlus, FileJson, Sparkles, Trash2, Pencil } from "lucide-react";

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
  onClone,
  onGenerateSignals,
  onSelectColor,
  isGeneratingSignals = false,
  disableSignalAction = false,
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
    <div className="flex items-start justify-between gap-4 rounded-xl border border-neutral-800 bg-neutral-900/70 px-4 py-3 shadow-[0_12px_32px_-24px_rgba(0,0,0,0.9)]">
      <div className="min-w-0 flex-1">
        <div className="flex items-center gap-2">
          <div className="truncate font-medium text-neutral-100" title={indicator?.name}>{indicator?.name}</div>
          <Popover className="relative">
            {({ close }) => (
              <>
                <PopoverButton
                  className="h-4 w-4 rounded-sm border border-neutral-700 shadow-inner"
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
                  <PopoverPanel className="absolute z-30 mt-2 rounded-md border border-neutral-800 bg-neutral-900 p-2 shadow-xl">
                    <div className="flex gap-2">
                      {colorSwatches.map((c) => (
                        <button
                          key={c}
                          className="h-5 w-5 rounded-sm border border-neutral-700 focus:outline-none focus:ring-2 focus:ring-neutral-500"
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
        <div className="text-sm text-neutral-400">{indicator?.type}</div>

        <div className="mt-1 flex flex-wrap gap-1">
          {essentials.map(([k, v]) => (
            <span key={k} className="inline-flex items-center gap-1 rounded-full border border-neutral-800 bg-neutral-950/60 px-2 py-0.5 text-xs text-neutral-300">
              <span className="text-neutral-500">{k}</span>
              <span>={formatVal(v)}</span>
            </span>
          ))}

          {advanced.length > 0 && !showAdvanced && (
            <button
              className="inline-flex items-center gap-1 rounded-full border border-neutral-800 bg-neutral-900 px-2 py-0.5 text-xs text-neutral-400 transition hover:border-neutral-600 hover:text-neutral-200"
              onClick={() => setShowAdvanced(true)}
            >
              +{advanced.length} more
            </button>
          )}
        </div>

        {showAdvanced && (
          <div className="mt-2 flex flex-wrap gap-1">
            {advanced.map(([k, v]) => (
              <span key={k} className="inline-flex items-center gap-1 rounded-full border border-neutral-800 bg-neutral-950/60 px-2 py-0.5 text-xs text-neutral-300">
                <span className="text-neutral-500">{k}</span>
                <span>={formatVal(v)}</span>
              </span>
            ))}
            <button
              className="inline-flex items-center gap-1 rounded-full border border-neutral-800 bg-neutral-900 px-2 py-0.5 text-xs text-neutral-400 transition hover:border-neutral-600 hover:text-neutral-200"
              onClick={() => setShowAdvanced(false)}
            >
              Show less
            </button>
          </div>
        )}
      </div>

      <div className="flex shrink-0 items-center gap-3 text-neutral-400">
        <Switch
          checked={!!indicator?.enabled}
          onChange={() => onToggle?.(indicator.id)}
          className={`${indicator?.enabled ? 'bg-emerald-500' : 'bg-neutral-700'} relative inline-flex h-6 w-11 items-center rounded-full cursor-pointer transition`}
        >
          <span className={`${indicator?.enabled ? 'translate-x-6' : 'translate-x-1'} inline-block h-4 w-4 transform rounded-full bg-white shadow transition`} />
        </Switch>

        <button
          onClick={() => onEdit?.(indicator)}
          className="inline-flex items-center gap-1 rounded-full border border-neutral-700 bg-neutral-900 px-3 py-1 text-xs font-medium text-neutral-200 transition hover:border-neutral-500 hover:text-neutral-50"
        >
          <Pencil className="size-3.5" />
          Edit
        </button>

        <button
          type="button"
          onClick={() => onGenerateSignals?.(indicator.id)}
          className={`relative inline-flex items-center gap-2 rounded-full border border-emerald-500/60 px-3 py-1 text-xs font-medium text-emerald-300 transition ${
            disableSignalAction ? 'cursor-not-allowed opacity-40' : 'hover:border-emerald-400 hover:text-emerald-200'
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
            <>
              <Sparkles className="size-3.5" />
              Run
            </>
          )}
        </button>

        <Menu as="div" className="relative">
          <MenuButton className="inline-flex h-8 w-8 items-center justify-center rounded-full border border-neutral-800 bg-neutral-900 text-neutral-500 transition hover:border-neutral-600 hover:text-neutral-200">
            <MoreHorizontal className="size-4" />
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
            <MenuItems className="absolute right-0 z-40 mt-2 w-48 origin-top-right divide-y divide-neutral-800 overflow-hidden rounded-xl border border-neutral-800 bg-neutral-950/95 text-sm text-neutral-200 shadow-xl backdrop-blur">
              <div className="p-1">
                <MenuItem>
                  {({ active }) => (
                    <button
                      type="button"
                      onClick={() => onClone?.(indicator.id)}
                      className={`${active ? 'bg-neutral-800 text-neutral-50' : ''} flex w-full items-center gap-2 rounded-lg px-3 py-2 text-left`}
                    >
                      <CopyPlus className="size-4" />
                      Duplicate indicator
                    </button>
                  )}
                </MenuItem>
                <MenuItem>
                  {({ active }) => (
                    <button
                      type="button"
                      onClick={copyParams}
                      className={`${active ? 'bg-neutral-800 text-neutral-50' : ''} flex w-full items-center gap-2 rounded-lg px-3 py-2 text-left`}
                    >
                      <FileJson className="size-4" />
                      Copy params JSON
                    </button>
                  )}
                </MenuItem>
              </div>
              <div className="p-1">
                <MenuItem>
                  {({ active }) => (
                    <button
                      type="button"
                      onClick={() => onDelete?.(indicator.id)}
                      className={`${active ? 'bg-rose-600/20 text-rose-200' : 'text-rose-300'} flex w-full items-center gap-2 rounded-lg px-3 py-2 text-left`}
                    >
                      <Trash2 className="size-4" />
                      Delete indicator
                    </button>
                  )}
                </MenuItem>
              </div>
            </MenuItems>
          </Transition>
        </Menu>
      </div>
    </div>
  );
}
