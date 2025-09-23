// src/components/IndicatorModal.v2.jsx
import React, { useEffect, useMemo, useRef, useState } from "react";
import { Dialog, DialogPanel, DialogTitle, Switch } from "@headlessui/react";
import {
  Braces,
  ChevronDown,
  Copy,
  RotateCcw,
  Search,
  SlidersHorizontal,
} from "lucide-react";
import { fetchIndicatorTypes, fetchIndicatorType } from "../adapters/indicator.adapter";

/**
 * Goals
 * - Keep simple flows simple (Name + Type + a handful of core params)
 * - Make lots of params manageable: search, grouping, folding, raw view, reset
 * - Zero backend changes required, but supports optional ui metadata if present
 *
 * Optional backend shape (if you decide to add later):
 *   {
 *     required_params: string[],
 *     default_params: Record<string, any>,
 *     field_types: Record<string, 'int'|'float'|'number'|'bool'|'string'|'int_list'>,
 *     ui_basic_keys?: string[],        // force these into "Essential"
 *     ui_order?: string[],             // render order across both sections
 *     ui_descriptions?: Record<string, string>,
 *     ui_enums?: Record<string, string[]>, // allowed values for select
 *   }
 */
export default function IndicatorModalV2({
  isOpen,
  initial, // { id?, type, name, params }
  error,
  onClose,
  onSave, // fn({ id?, type, name, params })
}) {
  const [types, setTypes] = useState([]);
  const [typeId, setTypeId] = useState(initial?.type || "");
  const [name, setName] = useState(initial?.name || "");
  const [params, setParams] = useState(initial?.params || {});
  const [metaErr, setMetaErr] = useState(null);
  const [typeMeta, setTypeMeta] = useState({
    required_params: [],
    default_params: {},
    field_types: {},
    ui_basic_keys: undefined,
    ui_order: undefined,
    ui_descriptions: undefined,
    ui_enums: undefined,
  });

  // --- helpers for int lists ---
  const toInt = (v) => {
    if (typeof v === "number") return Number.isFinite(v) ? Math.trunc(v) : null;
    if (typeof v === "string") {
      const n = Number(v.trim());
      return Number.isFinite(n) ? Math.trunc(n) : null;
    }
    return null;
  };
  const toIntList = (v) => {
    if (Array.isArray(v)) return v.map(toInt).filter((n) => n !== null);
    if (typeof v === "string") {
      return v
        .split(/[\s,;]+/)
        .filter(Boolean)
        .map(toInt)
        .filter((n) => n !== null);
    }
    if (v == null) return [];
    const n = toInt(v);
    return n !== null ? [n] : [];
  };
  const listToString = (arr) => (Array.isArray(arr) ? arr.join(", ") : arr ?? "");

  // Detect int-list fields heuristically
  const intListKeys = useMemo(() => {
    const keys = new Set();
    const ft = typeMeta?.field_types || {};
    const dp = typeMeta?.default_params || {};
    Object.entries(ft).forEach(([k, t]) => {
      const s = String(t || "").toLowerCase();
      if (
        s === "int_list" ||
        s === "list<int>" ||
        s === "int[]" ||
        /list.*int/.test(s) ||
        /int.*\[\]/.test(s)
      ) {
        keys.add(k);
      }
    });
    Object.entries(dp).forEach(([k, v]) => {
      if (Array.isArray(v) && v.every((n) => Number.isFinite(n))) keys.add(k);
    });
    return keys;
  }, [typeMeta]);

  // 1) Load list of available types when opening
  useEffect(() => {
    if (!isOpen) return;
    fetchIndicatorTypes()
      .then(setTypes)
      .catch((e) => setMetaErr(e.message));
  }, [isOpen]);

  // 2) Reset form when opening or switching between create/edit
  useEffect(() => {
    if (!isOpen) return;
    if (initial) {
      setTypeId(initial.type);
      setName(initial.name);
      setParams(initial.params || {});
    } else {
      setTypeId("");
      setName("");
      setParams({});
    }
    setTypeMeta({
      required_params: [],
      default_params: {},
      field_types: {},
      ui_basic_keys: undefined,
      ui_order: undefined,
      ui_descriptions: undefined,
      ui_enums: undefined,
    });
    setMetaErr(null);
  }, [initial, isOpen]);

  // 3) When a type is chosen, fetch its metadata
  useEffect(() => {
    if (!isOpen || !typeId) return;
    fetchIndicatorType(typeId)
      .then((meta) => {
        setTypeMeta(meta);
        if (!initial) {
          const seed = {};
          (meta.required_params || []).forEach((key) => {
            seed[key] = "";
          });
          Object.entries(meta.default_params || {}).forEach(([k, v]) => {
            seed[k] = v;
          });
          setParams(seed);
        }
      })
      .catch((e) => setMetaErr(e.message));
  }, [initial, isOpen, typeId]);

  /** UI state **/
  const [filter, setFilter] = useState("");
  const [showRaw, setShowRaw] = useState(false);
  const [advancedOpen, setAdvancedOpen] = useState(false);
  const filterInputRef = useRef(null);

  // Heuristics for grouping: Essential vs Advanced
  const basicHints = useMemo(() => new Set([
    // common essentials for your current trendline example
    "timeframe",
    "lookbacks",
    "tolerance",
    "enforce_direction",
    "algo",
  ]), []);

  const allKeys = useMemo(() => {
    const req = typeMeta?.required_params || [];
    const opt = Object.keys(typeMeta?.default_params || {});
    // preserve an optional backend-provided order if present
    const ordered = typeMeta?.ui_order?.length
      ? typeMeta.ui_order
      : [...req, ...opt.filter((k) => !req.includes(k))];
    return ordered;
  }, [typeMeta]);

  const { basicKeys, advancedKeys } = useMemo(() => {
    const req = new Set(typeMeta?.required_params || []);
    const uiBasic = new Set(typeMeta?.ui_basic_keys || []);
    const adv = [];
    const basic = [];

    allKeys.forEach((k) => {
      const lower = k.toLowerCase();
      const isHeuristicAdv =
        lower.startsWith("ransac_") ||
        lower.includes("dedupe") ||
        lower.includes("debug") ||
        lower.includes("max_windows");

      const chooseBasic = req.has(k) || uiBasic.has(k) || basicHints.has(k);
      if (chooseBasic && !isHeuristicAdv) basic.push(k);
      else adv.push(k);
    });

    return { basicKeys: basic, advancedKeys: adv };
  }, [allKeys, typeMeta, basicHints]);

  const descriptionFor = (k) => typeMeta?.ui_descriptions?.[k];
  const enumsFor = (k) => typeMeta?.ui_enums?.[k];
  const ftypeOf = (k) => (typeMeta?.field_types?.[k] || "string").toLowerCase();

  useEffect(() => {
    if (!isOpen) return;
    const handler = (e) => {
      if (e.key === "/" && document.activeElement !== filterInputRef.current) {
        e.preventDefault();
        filterInputRef.current?.focus();
      }
    };
    window.addEventListener("keydown", handler);
    return () => window.removeEventListener("keydown", handler);
  }, [isOpen]);

  useEffect(() => {
    if (!isOpen) return;
    setAdvancedOpen(false);
    setShowRaw(false);
  }, [isOpen, typeId]);

  const inputClass =
    "w-full rounded-lg border border-neutral-700/80 bg-neutral-900/60 px-3 py-2 text-sm text-neutral-100 placeholder:text-neutral-500 focus:border-indigo-500 focus:outline-none focus:ring-1 focus:ring-indigo-500";
  const pillButtonClass =
    "inline-flex items-center justify-center gap-2 rounded-lg border border-neutral-700/70 bg-neutral-900/60 px-3 py-2 text-sm font-medium text-neutral-200 transition hover:border-indigo-500 hover:text-white";

  const renderField = (key) => {
    const ftype = ftypeOf(key);
    const val = params[key];
    const enumVals = enumsFor(key);
    const boolChecked =
      val === true || val === "true" || val === 1 || val === "1";

    // searchable filter
    if (filter && !key.toLowerCase().includes(filter.toLowerCase())) return null;

    return (
      <div
        key={key}
        className="space-y-2 rounded-lg border border-neutral-700/70 bg-neutral-900/50 p-3 shadow-sm"
      >
        <div className="flex items-start justify-between gap-2">
          <div>
            <label className="block text-sm font-semibold text-neutral-100">
              {key}
            </label>
            {descriptionFor(key) && (
              <p className="mt-1 text-xs leading-5 text-neutral-400">
                {descriptionFor(key)}
              </p>
            )}
          </div>
          {typeMeta.required_params?.includes(key) && (
            <span className="ml-2 inline-flex h-5 items-center justify-center rounded-full border border-red-500/40 bg-red-500/10 px-2 text-xs font-medium text-red-300">
              Required
            </span>
          )}
        </div>
        {intListKeys.has(key) ? (
          <input
            type="text"
            inputMode="numeric"
            pattern="^[0-9\\s,;]*$"
            className={inputClass}
            value={listToString(val)}
            placeholder="e.g., 5, 10, 20"
            onChange={(e) => setParams((p) => ({ ...p, [key]: e.target.value }))}
            onBlur={(e) => setParams((p) => ({ ...p, [key]: toIntList(e.target.value) }))}
          />
        ) : enumVals?.length ? (
          <select
            className={inputClass}
            value={String(val ?? "")}
            onChange={(e) => setParams((p) => ({ ...p, [key]: e.target.value }))}
          >
            {enumVals.map((ev) => (
              <option key={ev} value={ev}>
                {ev}
              </option>
            ))}
          </select>
        ) : ftype === "bool" ? (
          <div className="flex items-center gap-3">
            <Switch
              checked={boolChecked}
              onChange={(checked) => setParams((p) => ({ ...p, [key]: checked }))}
              className={`${boolChecked ? "bg-indigo-500" : "bg-neutral-700"} relative inline-flex h-6 w-11 items-center rounded-full transition-colors focus:outline-none focus:ring-2 focus:ring-indigo-500 focus:ring-offset-2 focus:ring-offset-neutral-900`}
            >
              <span
                className={`${boolChecked ? "translate-x-6" : "translate-x-1"} inline-block h-4 w-4 transform rounded-full bg-white transition-transform`}
              />
            </Switch>
            <span className="text-sm text-neutral-300">{String(val)}</span>
          </div>
        ) : ["int", "float", "number"].includes(ftype) ? (
          <input
            type="number"
            step={ftype === "int" ? 1 : "any"}
            className={inputClass}
            value={Number.isFinite(val) ? val : ""}
            onChange={(e) =>
              setParams((p) => ({ ...p, [key]: e.target.valueAsNumber }))
            }
          />
        ) : (
          <input
            type="text"
            className={inputClass}
            value={val ?? ""}
            onChange={(e) => setParams((p) => ({ ...p, [key]: e.target.value }))}
          />
        )}
      </div>
    );
  };

  const handleSubmit = () => {
    if (!typeId) return setMetaErr("Please select a type.");
    if (!name.trim()) return setMetaErr("Please enter a name.");
    onSave({ id: initial?.id, type: typeId, name, params });
  };

  const resetToDefaults = () => {
    const seed = {};
    (typeMeta.required_params || []).forEach((key) => (seed[key] = ""));
    Object.entries(typeMeta.default_params || {}).forEach(([k, v]) => (seed[k] = v));
    setParams(seed);
  };

  const copyParams = async () => {
    try {
      await navigator.clipboard.writeText(JSON.stringify(params, null, 2));
    } catch (e) {
      console.error("copy failed", e);
    }
  };

  // layout helpers
  const Section = ({ title, keys }) => {
    const fields = keys.map(renderField).filter(Boolean);
    if (!fields.length) return null;
    return (
      <div className="space-y-3 rounded-xl border border-neutral-700/70 bg-neutral-900/40 p-4 shadow-inner">
        <div className="flex items-center justify-between">
          <h4 className="text-sm font-semibold text-neutral-200">{title}</h4>
          <span className="text-xs text-neutral-500">{fields.length} field(s)</span>
        </div>
        <div className="grid grid-cols-1 gap-3 md:grid-cols-2 xl:grid-cols-3">{fields}</div>
      </div>
    );
  };

  return (
    <Dialog open={isOpen} onClose={onClose} className="relative z-50">
      <div className="fixed inset-0 bg-black/40" aria-hidden="true" />
      <div className="fixed inset-0 flex items-center justify-center p-4">
        <DialogPanel className="w-full max-w-5xl overflow-hidden rounded-2xl border border-neutral-700 bg-neutral-900/80 text-neutral-100 shadow-[0_30px_120px_-40px_rgba(99,102,241,0.6)] backdrop-blur">
          <div className="border-b border-neutral-700/70 bg-gradient-to-r from-neutral-900 via-neutral-900 to-neutral-800 px-6 py-5">
            <div className="flex flex-wrap items-center justify-between gap-3">
              <DialogTitle className="text-xl font-semibold text-white">
                {initial?.id ? "Edit Indicator" : "Create Indicator"}
              </DialogTitle>
              {typeId && (
                <span className="inline-flex items-center rounded-full border border-indigo-500/40 bg-indigo-500/10 px-3 py-1 text-xs font-medium uppercase tracking-wide text-indigo-200">
                  {typeId}
                </span>
              )}
            </div>
            <p className="mt-2 text-sm text-neutral-400">
              Tune parameters faster with keyboard access (press <kbd className="rounded bg-neutral-800 px-1">/</kbd> to search) and quick actions for copying or resetting defaults.
            </p>
            {(metaErr || error) && (
              <div className="mt-3 rounded-lg border border-red-500/40 bg-red-500/10 px-3 py-2 text-sm text-red-200">
                {metaErr || error}
              </div>
            )}
          </div>

          <div className="grid max-h-[70vh] grid-cols-1 gap-6 overflow-y-auto px-6 py-6 lg:grid-cols-[320px,1fr]">
            <div className="space-y-4">
              <div className="space-y-4 rounded-xl border border-neutral-700/70 bg-neutral-900/50 p-4 shadow-inner">
                <div className="flex items-start justify-between gap-3">
                  <div className="space-y-1">
                    <p className="text-xs uppercase tracking-widest text-neutral-500">
                      {initial?.id ? "Editing existing indicator" : "New indicator"}
                    </p>
                    <p className="text-lg font-semibold text-white" title={name || "Untitled indicator"}>
                      {name || "Untitled indicator"}
                    </p>
                  </div>
                  {typeId ? (
                    <span className="inline-flex items-center rounded-full border border-indigo-500/40 bg-indigo-500/10 px-3 py-1 text-xs font-medium text-indigo-200">
                      {typeId}
                    </span>
                  ) : (
                    <span className="text-xs text-neutral-500">Select a type</span>
                  )}
                </div>

                <div className="space-y-3">
                  <div>
                    <label className="mb-1 block text-xs font-semibold uppercase tracking-wide text-neutral-400">
                      Name
                    </label>
                    <input
                      type="text"
                      className={inputClass}
                      value={name}
                      placeholder="Name your indicator"
                      onChange={(e) => setName(e.target.value)}
                    />
                  </div>
                  <div>
                    <label className="mb-1 block text-xs font-semibold uppercase tracking-wide text-neutral-400">
                      Indicator type
                    </label>
                    {initial?.id ? (
                      <div className="rounded-lg border border-neutral-700/70 bg-neutral-900/50 px-3 py-2 text-sm text-neutral-200">
                        {typeId}
                      </div>
                    ) : (
                      <select
                        className={`${inputClass} appearance-none`}
                        value={typeId}
                        onChange={(e) => setTypeId(e.target.value)}
                      >
                        <option value="">— select type —</option>
                        {types.map((t) => (
                          <option key={t} value={t}>
                            {t}
                          </option>
                        ))}
                      </select>
                    )}
                  </div>
                </div>
              </div>

              <div className="space-y-3 rounded-xl border border-neutral-700/70 bg-neutral-900/40 p-4 shadow-inner">
                <h3 className="text-sm font-semibold text-neutral-200">Workflow shortcuts</h3>
                <div className="grid grid-cols-1 gap-2">
                  <button
                    type="button"
                    className={`${pillButtonClass} ${advancedOpen ? "border-indigo-500 bg-indigo-500/10 text-indigo-200" : ""}`}
                    onClick={() => setAdvancedOpen((prev) => !prev)}
                    disabled={!advancedKeys.length}
                  >
                    <SlidersHorizontal className="h-4 w-4" />
                    {advancedOpen ? "Hide advanced fields" : `Show ${advancedKeys.length || "no"} advanced fields`}
                    <ChevronDown
                      className={`h-4 w-4 transition-transform ${advancedOpen ? "rotate-180" : ""}`}
                    />
                  </button>
                  <button
                    type="button"
                    className={`${pillButtonClass} ${showRaw ? "border-indigo-500 bg-indigo-500/10 text-indigo-200" : ""}`}
                    onClick={() => setShowRaw((prev) => !prev)}
                  >
                    <Braces className="h-4 w-4" />
                    {showRaw ? "Hide raw JSON" : "Show raw JSON"}
                  </button>
                  <button
                    type="button"
                    className={pillButtonClass}
                    onClick={copyParams}
                  >
                    <Copy className="h-4 w-4" /> Copy params JSON
                  </button>
                  <button
                    type="button"
                    className={pillButtonClass}
                    onClick={resetToDefaults}
                  >
                    <RotateCcw className="h-4 w-4" /> Reset to defaults
                  </button>
                </div>
                <p className="text-xs text-neutral-500">
                  Required fields: {typeMeta?.required_params?.length || 0}. Defaults are applied when selecting a type.
                </p>
              </div>
            </div>

            <div className="space-y-5">
              <div className="rounded-xl border border-neutral-700/70 bg-neutral-900/40 p-4 shadow-inner">
                <div className="flex flex-col gap-3 md:flex-row md:items-center md:justify-between">
                  <div className="relative w-full md:max-w-sm">
                    <Search className="pointer-events-none absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-neutral-500" />
                    <input
                      ref={filterInputRef}
                      type="text"
                      className={`${inputClass} pl-9`}
                      placeholder={typeId ? "Filter by parameter name (press /)" : "Select a type to configure"}
                      value={filter}
                      onChange={(e) => setFilter(e.target.value)}
                    />
                  </div>
                  <div className="flex flex-wrap gap-3 text-xs text-neutral-500">
                    <span>Essential fields: {basicKeys.length}</span>
                    <span>Advanced fields: {advancedKeys.length}</span>
                  </div>
                </div>
              </div>

              {typeId ? (
                <div className="space-y-5">
                  <Section title="Essential" keys={basicKeys} />

                  {advancedKeys.length > 0 && (
                    <div className="space-y-3 rounded-xl border border-neutral-700/70 bg-neutral-900/40 p-4 shadow-inner">
                      <button
                        type="button"
                        className="flex w-full items-center justify-between rounded-lg border border-neutral-700/70 bg-neutral-900/60 px-3 py-2 text-left text-sm font-semibold text-neutral-200 transition hover:border-indigo-500 hover:text-white"
                        onClick={() => setAdvancedOpen((prev) => !prev)}
                      >
                        <span className="inline-flex items-center gap-2">
                          <SlidersHorizontal className="h-4 w-4 text-indigo-300" /> Advanced ({advancedKeys.length})
                        </span>
                        <ChevronDown
                          className={`h-4 w-4 text-neutral-400 transition-transform ${advancedOpen ? "rotate-180" : ""}`}
                        />
                      </button>
                      {advancedOpen && (
                        <div className="grid grid-cols-1 gap-3 md:grid-cols-2 xl:grid-cols-3">
                          {advancedKeys.map(renderField).filter(Boolean)}
                        </div>
                      )}
                    </div>
                  )}

                  {showRaw && (
                    <div className="space-y-3 rounded-xl border border-neutral-700/70 bg-neutral-900/40 p-4">
                      <div className="flex items-center justify-between text-sm font-semibold text-neutral-200">
                        <span className="inline-flex items-center gap-2">
                          <Braces className="h-4 w-4 text-indigo-300" /> Raw params JSON
                        </span>
                        <span className="text-xs text-neutral-500">Read only</span>
                      </div>
                      <pre className="max-h-60 overflow-auto rounded-lg border border-neutral-800 bg-neutral-950/70 p-3 text-xs leading-5 text-neutral-200">
                        {JSON.stringify(params, null, 2)}
                      </pre>
                    </div>
                  )}
                </div>
              ) : (
                <div className="rounded-xl border border-dashed border-neutral-700/70 bg-neutral-900/30 p-8 text-center text-sm text-neutral-400">
                  Choose an indicator type to reveal configurable parameters.
                </div>
              )}
            </div>
          </div>

          <div className="border-t border-neutral-700/70 bg-neutral-900/60 px-6 py-4">
            <div className="flex flex-col gap-3 sm:flex-row sm:items-center sm:justify-end">
              <button
                onClick={onClose}
                className="inline-flex items-center justify-center rounded-lg border border-neutral-700/70 px-4 py-2 text-sm font-medium text-neutral-200 transition hover:border-neutral-500 hover:text-white"
              >
                Cancel
              </button>
              <button
                onClick={handleSubmit}
                className="inline-flex items-center justify-center rounded-lg bg-indigo-600 px-4 py-2 text-sm font-semibold text-white shadow hover:bg-indigo-500 disabled:cursor-not-allowed disabled:opacity-60"
              >
                {initial?.id ? "Update indicator" : "Create indicator"}
              </button>
            </div>
          </div>
        </DialogPanel>
      </div>
    </Dialog>
  );
}
