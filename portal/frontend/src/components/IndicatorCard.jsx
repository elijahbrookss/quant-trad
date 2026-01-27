import React, { Fragment, useMemo, useState } from "react";
import { Switch, Popover, PopoverButton, PopoverPanel, Transition } from "@headlessui/react";
import { MoreHorizontal, Copy, ChevronDown, ChevronUp, Palette } from "lucide-react";

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
const isAdvancedKey = (key) =>
  key.startsWith("ransac_") || key.includes("dedupe") || key.includes("max_windows") || key.includes("min_inliers");

const STATUS_META = {
  creating: { label: "New", tone: "text-amber-100 bg-amber-500/10 border-amber-400/30", dot: "bg-amber-300" },
  computing: { label: "Syncing", tone: "text-amber-100 bg-amber-500/10 border-amber-400/30", dot: "bg-amber-300" },
  updating: { label: "Updating", tone: "text-amber-100 bg-amber-500/10 border-amber-400/30", dot: "bg-amber-300" },
  failed: { label: "Error", tone: "text-rose-100 bg-rose-500/10 border-rose-400/30", dot: "bg-rose-400" },
  ready: { label: "Ready", tone: "text-emerald-100 bg-emerald-500/10 border-emerald-400/30", dot: "bg-emerald-300" },
  disabled: { label: "Hidden", tone: "text-slate-200 bg-slate-700/30 border-white/10", dot: "bg-slate-400" },
};

const formatRelativeTime = (value) => {
  if (!value) return "unrecorded";
  const ts = typeof value === "string" ? Date.parse(value) : Number(value);
  if (!Number.isFinite(ts)) return "unrecorded";
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
  busy = false,
  activeJobId = null,
  onRetryCreate,
  onRemoveLocal,
}) {
  const [expanded, setExpanded] = useState(false);
  const [confirmingDelete, setConfirmingDelete] = useState(false);
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

  const summaryParams = useMemo(() => {
    return paramsList.filter((item) => !item.isAdvanced).slice(0, 2);
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
  const statusKey = useMemo(() => {
    const raw = indicator?._status;
    if (raw) return raw;
    if (activeJobId && activeJobId === indicator?.id) return "computing";
    return indicator?.enabled ? "ready" : "disabled";
  }, [activeJobId, indicator?._status, indicator?.enabled, indicator?.id]);
  const statusMeta = STATUS_META[statusKey] || STATUS_META.ready;
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

  return (
    <div className={`group relative overflow-visible rounded-xl border ${selected ? "border-[color:var(--accent-alpha-60)] shadow-[0_16px_60px_-48px_rgba(0,0,0,0.95)]" : "border-white/8"} bg-[#0f1626] px-3 py-3`}>
      <div className="absolute left-0 top-0 h-full w-1.5 rounded-l-xl" style={{ backgroundColor: color }} aria-hidden="true" />
      <div className="flex flex-wrap items-center gap-3 pl-3">
        <label className="flex items-center gap-2 text-xs text-slate-300">
          <input
            type="checkbox"
            className="size-4 rounded-sm border border-white/20 bg-transparent"
            checked={selected}
            onChange={handleSelectionClick}
            aria-label={selected ? "Deselect indicator" : "Select indicator"}
          />
          <span className={`h-2.5 w-2.5 rounded-full border border-white/10 ${statusMeta.dot}`} />
        </label>

        <div className="min-w-0 flex-1">
          <div className="flex flex-wrap items-center gap-2">
            <button
              type="button"
              onClick={() => setExpanded((prev) => !prev)}
              className="flex items-center gap-1 text-left text-sm font-semibold text-white hover:text-[color:var(--accent-text-soft)]"
            >
              <span className="truncate" title={displayName}>{displayName}</span>
              {expanded ? <ChevronUp className="size-3" /> : <ChevronDown className="size-3" />}
            </button>
            <span className="rounded bg-white/5 px-2 py-0.5 text-[11px] uppercase tracking-[0.2em] text-slate-400">{typeLabel}</span>
            <span className={`inline-flex items-center gap-2 rounded-full border px-2 py-0.5 text-[11px] font-semibold ${statusMeta.tone}`}>
              <span className={`h-1.5 w-1.5 rounded-full ${statusMeta.dot}`} />
              {statusMeta.label}
            </span>
          </div>
          <div className="mt-1 flex flex-wrap items-center gap-2 text-xs text-slate-400">
            {lastUpdated ? <span>Updated {formatRelativeTime(lastUpdated)}</span> : <span>Awaiting first compute</span>}
            {summaryParams.map((item) => (
              <span key={item.key} className="inline-flex items-center gap-1 rounded-md border border-white/8 bg-white/5 px-2 py-0.5 text-[11px] text-slate-200">
                <span className="text-slate-400">{item.key}</span>
                <span>·</span>
                <span>{formatValue(item.value)}</span>
              </span>
            ))}
          </div>
        </div>

        <div className="flex items-center gap-2">
          <button
            type="button"
            onClick={() => onGenerateSignals?.(indicator.id)}
            className={`inline-flex items-center gap-1 rounded-md px-3 py-2 text-xs font-semibold transition ${
              disableSignalAction
                ? "cursor-not-allowed border border-white/10 text-slate-500"
                : "border border-emerald-300/40 text-emerald-100 hover:border-emerald-200/70 hover:text-emerald-50"
            }`}
            title={isGeneratingSignals ? "Generating…" : "Generate signals"}
            disabled={disableSignalAction || isGeneratingSignals}
            aria-busy={isGeneratingSignals}
          >
            {isGeneratingSignals ? 'Working…' : 'Generate'}
          </button>

          <Popover className="relative">
            {({ close }) => (
              <>
                <PopoverButton
                  className="flex h-9 w-9 items-center justify-center rounded-md border border-white/10 bg-white/5 text-slate-300 transition hover:border-[color:var(--accent-alpha-40)] hover:bg-[color:var(--accent-alpha-18)] hover:text-white"
                  title="Color"
                >
                  <Palette className="size-4" />
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
                  <PopoverPanel className="absolute right-0 top-full z-40 mt-2 w-56 rounded-xl border border-white/12 bg-[#131a2b] p-3 shadow-2xl">
                    <div className="grid grid-cols-6 gap-1.5">
                      {colorSwatches.map((c) => (
                        <button
                          key={c}
                          className="h-6 w-6 rounded-sm border border-white/15 transition hover:border-[color:var(--accent-alpha-60)] focus:outline-none focus:ring-2 focus:ring-[color:var(--accent-outline-soft)]"
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

          <Switch
            checked={!!indicator?.enabled}
            onChange={() => onToggle?.(indicator.id)}
            disabled={disableActions}
            className={`${indicator?.enabled ? "bg-[color:var(--accent-alpha-80)]" : "bg-slate-700/70"} relative inline-flex h-6 w-11 cursor-pointer items-center rounded-full transition disabled:cursor-not-allowed disabled:opacity-50`}
          >
            <span className={`${indicator?.enabled ? "translate-x-6" : "translate-x-1"} inline-block h-4 w-4 transform rounded-full bg-white transition`} />
          </Switch>

          <Popover className="relative">
            {({ close }) => (
              <>
                <PopoverButton
                  onClick={() => setConfirmingDelete(false)}
                  className="flex h-9 w-9 items-center justify-center rounded-md border border-white/10 bg-white/5 text-slate-300 transition hover:border-[color:var(--accent-alpha-40)] hover:bg-[color:var(--accent-alpha-18)] hover:text-white"
                  title="Indicator actions"
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
                  <PopoverPanel className="absolute right-0 top-full z-40 mt-2 w-64 rounded-2xl border border-white/12 bg-[#131a2b] p-4 shadow-2xl ring-1 ring-black/20">
                    <div className="space-y-3 text-sm text-slate-200">
                      <button
                        onClick={() => {
                          onEdit?.(indicator);
                          setConfirmingDelete(false);
                          close();
                        }}
                        className="flex items-center justify-between rounded-lg border border-white/12 px-3 py-2 text-left transition hover:border-[color:var(--accent-alpha-35)] hover:bg-[color:var(--accent-alpha-12)]"
                      >
                        <span>Edit / View params</span>
                        <span className="text-[10px] uppercase tracking-[0.3em] text-slate-500">E</span>
                      </button>

                      <button
                        onClick={async () => {
                          await copyParams();
                          setConfirmingDelete(false);
                          close();
                        }}
                        className="flex items-center justify-between rounded-lg border border-white/12 px-3 py-2 text-left transition hover:border-[color:var(--accent-alpha-35)] hover:bg-[color:var(--accent-alpha-12)]"
                      >
                        <span className="inline-flex items-center gap-2"><Copy className="size-4" /> Copy params JSON</span>
                      </button>

                      <button
                        onClick={() => {
                          if (duplicateDisabled) return;
                          onDuplicate?.(indicator.id);
                          setConfirmingDelete(false);
                          close();
                        }}
                        disabled={duplicateDisabled}
                        className={`flex items-center justify-between rounded-lg border px-3 py-2 text-left transition ${
                          duplicateDisabled
                            ? "cursor-not-allowed border-white/5 text-slate-500"
                            : "border-white/12 hover:border-[color:var(--accent-alpha-35)] hover:bg-[color:var(--accent-alpha-12)]"
                        }`}
                      >
                        <span>{duplicatePending ? "Duplicating…" : "Duplicate"}</span>
                      </button>

                      {!confirmingDelete && (
                        <button
                          onClick={() => setConfirmingDelete(true)}
                          className="flex items-center justify-between rounded-lg border border-rose-500/30 bg-rose-500/10 px-3 py-2 text-left text-rose-200 transition hover:border-rose-400/50 hover:bg-rose-500/20"
                        >
                          <span>Delete</span>
                        </button>
                      )}

                      {confirmingDelete && (
                        <div className="flex items-center justify-between gap-2 rounded-lg border border-rose-500/40 bg-rose-500/10 px-3 py-2 text-xs text-rose-100">
                          <span>Confirm delete?</span>
                          <div className="flex items-center gap-2">
                            <button
                              onClick={() => {
                                onDelete?.(indicator.id);
                                setConfirmingDelete(false);
                                close();
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
                  </PopoverPanel>
                </Transition>
              </>
            )}
          </Popover>
        </div>
      </div>

      {statusKey === "failed" && (
        <div className="mt-3 flex flex-wrap items-center justify-between gap-3 rounded-lg border border-rose-500/30 bg-rose-500/8 px-3 py-2 text-xs text-rose-100">
          <span>Indicator job failed. Keep for review or retry.</span>
          <div className="flex items-center gap-2">
            {canRetry && (
              <button
                type="button"
                onClick={() => onRetryCreate?.(indicator)}
                className="rounded border border-amber-400/50 px-2.5 py-1 text-[11px] uppercase tracking-[0.2em] text-amber-100 hover:bg-amber-500/10"
              >
                Retry
              </button>
            )}
            {canRemoveLocal && (
              <button
                type="button"
                onClick={() => onRemoveLocal?.(indicator.id)}
                className="rounded border border-white/20 px-2.5 py-1 text-[11px] uppercase tracking-[0.2em] text-slate-100 hover:border-white/40"
              >
                Remove
              </button>
            )}
          </div>
        </div>
      )}

      {expanded && (
        <div className="mt-3 rounded-lg border border-white/10 bg-[#0d1422]/60 p-3 text-xs text-slate-200">
          <div className="grid gap-2 sm:grid-cols-2">
            {paramsList.map(({ key, value }) => (
              <div key={key} className="flex items-start gap-2 rounded border border-white/5 bg-white/5 px-2 py-1">
                <span className="text-[11px] uppercase tracking-[0.14em] text-slate-400">{key}</span>
                <span className="text-slate-100">{formatValue(value)}</span>
              </div>
            ))}
          </div>
          <div className="mt-3 flex flex-wrap items-center justify-between gap-2 text-[11px] text-slate-400">
            <div className="flex items-center gap-2">
              <span>ID: {indicator?.id ?? "unknown"}</span>
              <button
                type="button"
                onClick={copyId}
                className="rounded border border-white/10 px-2 py-1 text-[10px] uppercase tracking-[0.18em] text-slate-200 transition hover:border-[color:var(--accent-alpha-40)]"
              >
                Copy ID
              </button>
            </div>
            <button
              type="button"
              onClick={() => setExpanded(false)}
              className="text-[11px] font-semibold uppercase tracking-[0.16em] text-[color:var(--accent-text-soft)]"
            >
              Hide details
            </button>
          </div>
        </div>
      )}
    </div>
  );
}
