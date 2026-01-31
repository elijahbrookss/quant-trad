import React, { Fragment, useMemo, useState } from "react";
import { Popover, PopoverButton, PopoverPanel, Transition } from "@headlessui/react";
import {
  MoreHorizontal,
  Copy,
  ChevronDown,
  ChevronUp,
  Palette,
  RefreshCw,
  Pencil,
  CopyPlus,
  Trash2,
  Loader2,
} from "lucide-react";
import VisibilityToggle from "./ui/VisibilityToggle";

// Keys to hide from param display
const HIDE_KEYS = new Set([
  "symbol",
  "interval",
  "start",
  "end",
  "debug",
  "datasource",
  "exchange",
  "provider_id",
  "venue_id",
  "instrument_id",
]);

// Advanced params shown only in expanded view
const isAdvancedKey = (key) =>
  key.startsWith("ransac_") || key.includes("dedupe") || key.includes("max_windows") || key.includes("min_inliers");

// Transient status states - only show these, not "ready" or "disabled"
const TRANSIENT_STATUS = {
  creating: { label: "Creating", tone: "text-amber-100 bg-amber-500/10 border-amber-400/30", dot: "bg-amber-300" },
  computing: { label: "Computing", tone: "text-amber-100 bg-amber-500/10 border-amber-400/30", dot: "bg-amber-300" },
  updating: { label: "Updating", tone: "text-amber-100 bg-amber-500/10 border-amber-400/30", dot: "bg-amber-300" },
  failed: { label: "Error", tone: "text-rose-100 bg-rose-500/10 border-rose-400/30", dot: "bg-rose-400" },
};

const formatRelativeTime = (value) => {
  if (!value) return null;
  const ts = typeof value === "string" ? Date.parse(value) : Number(value);
  if (!Number.isFinite(ts)) return null;
  const diff = Date.now() - ts;
  if (diff < 60_000) return "just now";
  if (diff < 3_600_000) return `${Math.max(1, Math.floor(diff / 60_000))}m ago`;
  if (diff < 86_400_000) return `${Math.floor(diff / 3_600_000)}h ago`;
  return `${Math.floor(diff / 86_400_000)}d ago`;
};

const formatValue = (v) => {
  if (Array.isArray(v)) return v.join(", ");
  if (typeof v === "boolean") return v ? "on" : "off";
  if (typeof v === "number") {
    const s = v.toFixed(6);
    return s.replace(/\.0+$/, "").replace(/(\.\d*?)0+$/, "$1");
  }
  return String(v);
};

// Convert snake_case to Title Case for display
const formatParamKey = (key) => {
  return key
    .replace(/_/g, " ")
    .replace(/([A-Z])/g, " $1")
    .split(" ")
    .filter(Boolean)
    .map((word) => word.charAt(0).toUpperCase() + word.slice(1).toLowerCase())
    .join(" ");
};

export default function IndicatorCard({
  indicator,
  color = "#60a5fa",
  colorSwatches = [
    "#facc15", "#b91c1c", "#f97316", "#a855f7", "#84cc16", "#6b7280",
    "#3b82f6", "#10b981", "#ec4899", "#14b8a6", "#eab308", "#f43f5e",
  ],
  onToggle,
  onEdit,
  onDelete,
  onDuplicate,
  onGenerateSignals,
  onSelectColor,
  onRecompute,
  isGeneratingSignals = false,
  disableSignalAction = false,
  selected = false,
  onSelectionToggle,
  duplicatePending = false,
  busy = false,
  activeJobId = null,
  onRetryCreate,
  onRemoveLocal,
}) {
  const [expanded, setExpanded] = useState(false);

  // Process params for display
  const paramsList = useMemo(() => {
    const entries = Object.entries(indicator?.params || {}).filter(
      ([k, v]) => !HIDE_KEYS.has(k) && v !== undefined && v !== null && String(v) !== ""
    );

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

  // Show 2-3 curated params in collapsed view
  const summaryParams = useMemo(() => {
    return paramsList.filter((item) => !item.isAdvanced).slice(0, 3);
  }, [paramsList]);

  const typeLabel = useMemo(() => {
    const raw = indicator?.type;
    if (!raw) return "Custom";
    return raw
      .split(/[_-]+/)
      .filter(Boolean)
      .map((token) => token.charAt(0).toUpperCase() + token.slice(1))
      .join(" ");
  }, [indicator?.type]);

  const displayName = indicator?.name?.trim() || typeLabel || "Indicator";
  const lastUpdated = indicator?.updated_at || indicator?.created_at || null;
  const relativeTime = formatRelativeTime(lastUpdated);
  const isVisible = !!indicator?.enabled;

  // Determine status - only show transient states
  const statusKey = useMemo(() => {
    const raw = indicator?._status;
    if (raw && TRANSIENT_STATUS[raw]) return raw;
    if (activeJobId && activeJobId === indicator?.id) return "computing";
    return null; // No status badge for stable indicators
  }, [activeJobId, indicator?._status, indicator?.id]);

  const statusMeta = statusKey ? TRANSIENT_STATUS[statusKey] : null;

  // Disable actions during compute/create/update
  const disableActions = busy || statusKey === "creating" || statusKey === "computing" || statusKey === "updating";
  const duplicateDisabled = duplicatePending || typeof onDuplicate !== "function" || disableActions;
  const canRetry = statusKey === "failed" && indicator?._local && typeof onRetryCreate === "function";
  const canRemoveLocal = statusKey === "failed" && indicator?._local && typeof onRemoveLocal === "function";

  const handleSelectionClick = () => {
    if (typeof onSelectionToggle === "function") {
      onSelectionToggle();
    }
  };

  const copyParams = async () => {
    try {
      await navigator.clipboard.writeText(JSON.stringify(indicator?.params ?? {}, null, 2));
    } catch {
      // clipboard unavailable
    }
  };

  const copyId = async () => {
    if (!indicator?.id) return;
    try {
      await navigator.clipboard.writeText(String(indicator.id));
    } catch {
      // clipboard unavailable
    }
  };

  // Hidden state styling - subtle desaturation
  const hiddenStyles = !isVisible ? "opacity-60 saturate-[0.85]" : "";

  return (
    <div
      className={`
        group relative overflow-visible rounded-lg border transition-all
        ${selected ? "border-[color:var(--accent-alpha-60)] shadow-[0_8px_30px_-20px_rgba(0,0,0,0.9)]" : "border-white/8"}
        bg-[#0d1422]/90 hover:bg-[#0f1626] px-3 py-2.5
        ${hiddenStyles}
      `}
    >
      {/* Left color accent bar */}
      <div
        className="absolute left-0 top-0 h-full w-1 rounded-l-lg"
        style={{ backgroundColor: color }}
        aria-hidden="true"
      />

      {/* Main row layout */}
      <div className="flex items-center gap-3 pl-3">
        {/* Checkbox + status dot */}
        <div className="flex items-center gap-2.5">
          <input
            type="checkbox"
            className="size-4 rounded-sm border border-white/20 bg-transparent accent-[color:var(--accent-base)]"
            checked={selected}
            onChange={handleSelectionClick}
            aria-label={selected ? "Deselect indicator" : "Select indicator"}
          />
          {statusMeta && (
            <span
              className={`h-2 w-2 rounded-full ${statusMeta.dot} animate-pulse`}
              title={statusMeta.label}
            />
          )}
        </div>

        {/* Name + Type + Params section */}
        <div className="min-w-0 flex-1">
          <div className="flex flex-wrap items-center gap-2">
            {/* Expandable name */}
            <button
              type="button"
              onClick={() => setExpanded((prev) => !prev)}
              className="flex items-center gap-1 text-left text-sm font-semibold text-white hover:text-[color:var(--accent-text-soft)] transition"
            >
              <span className="truncate max-w-[180px]" title={displayName}>
                {displayName}
              </span>
              {expanded ? <ChevronUp className="size-3 shrink-0" /> : <ChevronDown className="size-3 shrink-0" />}
            </button>

            {/* Type badge */}
            <span className="rounded bg-white/5 px-2 py-0.5 text-[10px] uppercase tracking-[0.2em] text-slate-400">
              {typeLabel}
            </span>

            {/* Transient status badge - only shown during operations */}
            {statusMeta && (
              <span
                className={`inline-flex items-center gap-1.5 rounded-full border px-2 py-0.5 text-[10px] font-semibold ${statusMeta.tone}`}
              >
                <Loader2 className="size-3 animate-spin" />
                {statusMeta.label}
              </span>
            )}

            {/* Param summary pills (collapsed view) */}
            {!expanded && summaryParams.length > 0 && (
              <div className="hidden sm:flex items-center gap-1.5">
                {summaryParams.map((item) => (
                  <span
                    key={item.key}
                    className="inline-flex items-center gap-1 rounded border border-white/8 bg-white/5 px-1.5 py-0.5 text-[10px] text-slate-300"
                  >
                    <span className="text-slate-500">{formatParamKey(item.key)}</span>
                    <span className="text-slate-200">{formatValue(item.value)}</span>
                  </span>
                ))}
              </div>
            )}
          </div>

          {/* Relative time */}
          {relativeTime && !expanded && (
            <p className="mt-0.5 text-[10px] text-slate-500">Updated {relativeTime}</p>
          )}
        </div>

        {/* Actions section */}
        <div className="flex items-center gap-2 shrink-0">
          {/* Generate Signals button */}
          <button
            type="button"
            onClick={() => onGenerateSignals?.(indicator.id)}
            className={`
              inline-flex items-center gap-1.5 rounded-md px-3 py-1.5 text-xs font-semibold transition
              ${
                disableSignalAction || isGeneratingSignals
                  ? "cursor-not-allowed border border-white/10 text-slate-500"
                  : "border border-emerald-400/30 text-emerald-200 hover:border-emerald-300/50 hover:bg-emerald-500/10"
              }
            `}
            title={isGeneratingSignals ? "Generating signals..." : "Generate signals"}
            disabled={disableSignalAction || isGeneratingSignals}
            aria-busy={isGeneratingSignals}
          >
            {isGeneratingSignals ? (
              <>
                <Loader2 className="size-3 animate-spin" />
                <span>Working</span>
              </>
            ) : (
              <span>Generate</span>
            )}
          </button>

          {/* Color picker popover */}
          <Popover className="relative">
            {({ close }) => (
              <>
                <PopoverButton
                  className="flex h-8 w-8 items-center justify-center rounded-md border border-white/10 bg-white/5 text-slate-300 transition hover:border-[color:var(--accent-alpha-40)] hover:bg-[color:var(--accent-alpha-12)] hover:text-white"
                  title="Change color"
                >
                  <Palette className="size-3.5" />
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
                  <PopoverPanel className="absolute right-0 top-full z-40 mt-2 w-48 rounded-xl border border-white/12 bg-[#131a2b] p-3 shadow-2xl">
                    <div className="grid grid-cols-6 gap-1.5">
                      {colorSwatches.map((c) => (
                        <button
                          key={c}
                          className="h-5 w-5 rounded-sm border border-white/15 transition hover:border-[color:var(--accent-alpha-60)] focus:outline-none focus:ring-2 focus:ring-[color:var(--accent-ring)]"
                          style={{ backgroundColor: c }}
                          onClick={() => {
                            onSelectColor?.(indicator.id, c);
                            close();
                          }}
                          aria-label={`Set color ${c}`}
                          disabled={disableActions}
                        />
                      ))}
                    </div>
                  </PopoverPanel>
                </Transition>
              </>
            )}
          </Popover>

          {/* Visibility toggle - allowed during compute per requirements */}
          <VisibilityToggle
            visible={isVisible}
            onChange={() => onToggle?.(indicator.id)}
            disabled={busy && statusKey !== "computing" && statusKey !== "updating"}
            size="sm"
          />

          {/* Context menu */}
          <Popover className="relative">
            {({ close }) => (
              <>
                <PopoverButton
                  className="flex h-8 w-8 items-center justify-center rounded-md border border-white/10 bg-white/5 text-slate-300 transition hover:border-[color:var(--accent-alpha-40)] hover:bg-[color:var(--accent-alpha-12)] hover:text-white"
                  title="More actions"
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
                  <PopoverPanel className="absolute right-0 top-full z-40 mt-2 w-56 rounded-xl border border-white/12 bg-[#131a2b] p-2 shadow-2xl">
                    {/* Runtime / Trading section */}
                    <div className="mb-2">
                      <p className="px-2 py-1 text-[9px] font-semibold uppercase tracking-[0.3em] text-slate-500">
                        Runtime
                      </p>
                      <button
                        onClick={() => {
                          onRecompute?.(indicator.id);
                          close();
                        }}
                        disabled={disableActions || typeof onRecompute !== "function"}
                        className={`
                          w-full flex items-center gap-2.5 rounded-lg px-3 py-2 text-left text-sm transition
                          ${
                            disableActions || typeof onRecompute !== "function"
                              ? "cursor-not-allowed text-slate-500"
                              : "text-slate-200 hover:bg-white/5"
                          }
                        `}
                      >
                        <RefreshCw className="size-4" />
                        Recompute Overlays
                      </button>
                    </div>

                    <div className="h-px bg-white/8 my-1" />

                    {/* Configuration section */}
                    <div className="my-2">
                      <p className="px-2 py-1 text-[9px] font-semibold uppercase tracking-[0.3em] text-slate-500">
                        Configuration
                      </p>
                      <button
                        onClick={() => {
                          onEdit?.(indicator);
                          close();
                        }}
                        className="w-full flex items-center gap-2.5 rounded-lg px-3 py-2 text-left text-sm text-slate-200 transition hover:bg-white/5"
                      >
                        <Pencil className="size-4" />
                        Edit Parameters
                      </button>
                      <button
                        onClick={() => {
                          if (!duplicateDisabled) {
                            onDuplicate?.(indicator.id);
                            close();
                          }
                        }}
                        disabled={duplicateDisabled}
                        className={`
                          w-full flex items-center gap-2.5 rounded-lg px-3 py-2 text-left text-sm transition
                          ${
                            duplicateDisabled
                              ? "cursor-not-allowed text-slate-500"
                              : "text-slate-200 hover:bg-white/5"
                          }
                        `}
                      >
                        <CopyPlus className="size-4" />
                        {duplicatePending ? "Duplicating..." : "Duplicate"}
                      </button>
                      <button
                        onClick={async () => {
                          await copyParams();
                          close();
                        }}
                        className="w-full flex items-center gap-2.5 rounded-lg px-3 py-2 text-left text-sm text-slate-200 transition hover:bg-white/5"
                      >
                        <Copy className="size-4" />
                        Copy Params JSON
                      </button>
                    </div>

                    <div className="h-px bg-white/8 my-1" />

                    {/* Danger section */}
                    <div className="mt-2">
                      <p className="px-2 py-1 text-[9px] font-semibold uppercase tracking-[0.3em] text-slate-500">
                        Danger
                      </p>
                      <button
                        onClick={() => {
                          if (!disableActions) {
                            onDelete?.(indicator.id);
                            close();
                          }
                        }}
                        disabled={disableActions}
                        className={`
                          w-full flex items-center gap-2.5 rounded-lg px-3 py-2 text-left text-sm transition
                          ${
                            disableActions
                              ? "cursor-not-allowed text-slate-500"
                              : "text-rose-300 hover:bg-rose-500/10"
                          }
                        `}
                      >
                        <Trash2 className="size-4" />
                        Delete Indicator
                      </button>
                    </div>
                  </PopoverPanel>
                </Transition>
              </>
            )}
          </Popover>
        </div>
      </div>

      {/* Failed state banner */}
      {statusKey === "failed" && (
        <div className="mt-2.5 flex flex-wrap items-center justify-between gap-3 rounded-lg border border-rose-500/30 bg-rose-500/8 px-3 py-2 text-xs text-rose-100">
          <span>Indicator job failed. Keep for review or retry.</span>
          <div className="flex items-center gap-2">
            {canRetry && (
              <button
                type="button"
                onClick={() => onRetryCreate?.(indicator)}
                className="rounded border border-amber-400/50 px-2.5 py-1 text-[10px] uppercase tracking-[0.2em] text-amber-100 hover:bg-amber-500/10"
              >
                Retry
              </button>
            )}
            {canRemoveLocal && (
              <button
                type="button"
                onClick={() => onRemoveLocal?.(indicator.id)}
                className="rounded border border-white/20 px-2.5 py-1 text-[10px] uppercase tracking-[0.2em] text-slate-100 hover:border-white/40"
              >
                Remove
              </button>
            )}
          </div>
        </div>
      )}

      {/* Expanded params view */}
      {expanded && (
        <div className="mt-3 rounded-lg border border-white/8 bg-[#0a0f1a]/60 p-3 text-xs">
          <div className="grid gap-2 sm:grid-cols-2">
            {paramsList.map(({ key, value }) => (
              <div
                key={key}
                className="flex items-start gap-2 rounded border border-white/5 bg-white/5 px-2 py-1.5"
              >
                <span className="text-[10px] font-mono uppercase tracking-wide text-slate-500">{key}</span>
                <span className="text-slate-200">{formatValue(value)}</span>
              </div>
            ))}
          </div>
          <div className="mt-3 flex flex-wrap items-center justify-between gap-2 text-[10px] text-slate-400">
            <div className="flex items-center gap-2">
              <span className="font-mono">ID: {indicator?.id ?? "unknown"}</span>
              <button
                type="button"
                onClick={copyId}
                className="rounded border border-white/10 px-2 py-0.5 text-[9px] uppercase tracking-[0.18em] text-slate-300 transition hover:border-[color:var(--accent-alpha-40)]"
              >
                Copy
              </button>
            </div>
            {relativeTime && <span>Updated {relativeTime}</span>}
            <button
              type="button"
              onClick={() => setExpanded(false)}
              className="font-semibold uppercase tracking-wider text-[color:var(--accent-text-soft)] hover:text-[color:var(--accent-text-strong)]"
            >
              Collapse
            </button>
          </div>
        </div>
      )}
    </div>
  );
}
