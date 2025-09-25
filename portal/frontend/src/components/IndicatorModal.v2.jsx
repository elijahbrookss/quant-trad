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
import { createLogger } from "../utils/logger.js";

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
  const [availableSignalRules, setAvailableSignalRules] = useState([]);
  const [selectedSignalRules, setSelectedSignalRules] = useState([]);
  const initialSignalRulesRef = useRef(null);

  const logger = useMemo(
    () => createLogger("IndicatorModal", { indicatorId: initial?.id ?? null }),
    [initial?.id]
  );
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
    setAvailableSignalRules([]);
    const initialRules = initial?.signalRules ? [...initial.signalRules] : [];
    initialSignalRulesRef.current = initialRules.length ? initialRules : null;
    setSelectedSignalRules(initialRules);
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
        const rules = Array.isArray(meta.signal_rules) ? meta.signal_rules : [];
        setAvailableSignalRules(rules);
        const seeded = initialSignalRulesRef.current;
        let nextSelection = Array.isArray(seeded) ? seeded.filter((id) => rules.some((rule) => rule.id === id)) : null;
        if (nextSelection && !nextSelection.length) {
          nextSelection = null;
        }
        if (!nextSelection || !nextSelection.length) {
          nextSelection = rules.map((rule) => rule.id);
        }
        setSelectedSignalRules(nextSelection);
        initialSignalRulesRef.current = null;
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
  }, [isOpen, typeId]);

  /** UI state **/
  const [filter, setFilter] = useState("");
  const [rawOpen, setRawOpen] = useState(false);
  const [advancedOpen, setAdvancedOpen] = useState(false);

  useEffect(() => {
    if (!isOpen) return;
    setAdvancedOpen(false);
    setRawOpen(false);
  }, [isOpen, typeId]);

  useEffect(() => {
    if (metaErr === "Please enable at least one signal rule." && selectedSignalRules.length > 0) {
      setMetaErr(null);
    }
  }, [metaErr, selectedSignalRules.length]);

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

  const renderField = (key) => {
    const ftype = ftypeOf(key);
    const val = params[key];
    const enumVals = enumsFor(key);
    const boolValue = !!val;

    // searchable filter
    if (filter && !key.toLowerCase().includes(filter.toLowerCase())) return null;

    return (
      <div
        key={key}
        className="space-y-2 rounded-lg border border-neutral-700/70 bg-neutral-800/50 p-3"
      >
        <div className="flex items-start justify-between gap-3">
          <div>
            <label className="block text-sm font-semibold text-neutral-100">
              {key}
              {typeMeta.required_params?.includes(key) && (
                <span className="text-red-500 ml-1">*</span>
              )}
            </label>
            {descriptionFor(key) && (
              <p className="text-xs text-neutral-400 leading-5">
                {descriptionFor(key)}
              </p>
            )}
          </div>
        </div>

        {intListKeys.has(key) ? (
          <input
            type="text"
            inputMode="numeric"
            pattern="^[0-9\\s,;]*$"
            className="w-full rounded-lg border border-neutral-700/70 bg-neutral-900/60 px-3 py-2 text-sm focus:border-indigo-500/70 focus:outline-none focus:ring-2 focus:ring-indigo-500/40"
            value={listToString(val)}
            placeholder="e.g., 5, 10, 20"
            onChange={(e) => setParams((p) => ({ ...p, [key]: e.target.value }))}
            onBlur={(e) => setParams((p) => ({ ...p, [key]: toIntList(e.target.value) }))}
          />
        ) : enumVals?.length ? (
          <select
            className="w-full rounded-lg border border-neutral-700/70 bg-neutral-900/60 px-3 py-2 text-sm focus:border-indigo-500/70 focus:outline-none focus:ring-2 focus:ring-indigo-500/40"
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
              checked={boolValue}
              onChange={(checked) => setParams((p) => ({ ...p, [key]: checked }))}
              className={`${boolValue ? "bg-indigo-600" : "bg-neutral-600"} relative inline-flex h-6 w-11 items-center rounded-full transition-colors`}
            >
              <span className={`${boolValue ? "translate-x-6" : "translate-x-1"} inline-block h-4 w-4 transform rounded-full bg-white transition-transform`} />
            </Switch>
            <span className="text-sm text-neutral-300">
              {boolValue ? "Enabled" : "Disabled"}
            </span>
          </div>
        ) : ["int", "float", "number"].includes(ftype) ? (
          <input
            type="number"
            step={ftype === "int" ? 1 : "any"}
            className="w-full rounded-lg border border-neutral-700/70 bg-neutral-900/60 px-3 py-2 text-sm focus:border-indigo-500/70 focus:outline-none focus:ring-2 focus:ring-indigo-500/40"
            value={Number.isFinite(val) ? val : ""}
            onChange={(e) =>
              setParams((p) => ({ ...p, [key]: e.target.valueAsNumber }))
            }
          />
        ) : (
          <input
            type="text"
            className="w-full rounded-lg border border-neutral-700/70 bg-neutral-900/60 px-3 py-2 text-sm focus:border-indigo-500/70 focus:outline-none focus:ring-2 focus:ring-indigo-500/40"
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
    if (availableSignalRules.length > 0 && selectedSignalRules.length === 0) {
      return setMetaErr("Please enable at least one signal rule.");
    }
    onSave({ id: initial?.id, type: typeId, name, params, signalRules: selectedSignalRules });
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
      logger.error("copy_params_failed", e);
    }
  };

  const toggleSignalRule = (ruleId) => {
    setSelectedSignalRules((prev) => {
      const next = new Set(prev);
      if (next.has(ruleId)) {
        next.delete(ruleId);
      } else {
        next.add(ruleId);
      }
      return availableSignalRules
        .map((rule) => rule.id)
        .filter((id) => next.has(id));
    });
  };

  const selectAllSignalRules = () => {
    setSelectedSignalRules(availableSignalRules.map((rule) => rule.id));
  };

  const clearSignalRules = () => {
    setSelectedSignalRules([]);
  };

  const allRulesSelected =
    availableSignalRules.length > 0 && selectedSignalRules.length === availableSignalRules.length;

  // layout helpers
  const Section = ({ title, keys }) => {
    const fields = keys.map(renderField).filter(Boolean);
    if (!fields.length) return null;
    return (
      <div className="rounded-xl border border-neutral-700/80 bg-neutral-800/40 p-4">
        <div className="mb-3 flex items-center justify-between">
          <h4 className="text-sm font-semibold text-neutral-200">{title}</h4>
          <span className="text-xs text-neutral-400">{fields.length} fields</span>
        </div>
        <div className="grid grid-cols-1 gap-3 md:grid-cols-2">{fields}</div>
      </div>
    );
  };

  return (
    <Dialog open={isOpen} onClose={onClose} className="relative z-50">
      <div className="fixed inset-0 bg-black/40" aria-hidden="true" />
      <div className="fixed inset-0 flex items-center justify-center p-4">
        <DialogPanel className="w-full max-w-5xl overflow-hidden rounded-2xl border border-neutral-700/80 bg-neutral-950/90 text-neutral-100 shadow-2xl backdrop-blur">
          <div className="border-b border-neutral-800 bg-neutral-900/80 px-6 py-4">
            <DialogTitle className="text-lg font-semibold">
              {initial?.id ? "Edit Indicator" : "Create Indicator"}
            </DialogTitle>
            <p className="mt-1 text-sm text-neutral-400">
              Configure signal logic quickly with grouped essentials and one-click tools.
            </p>
          </div>

          <div className="flex flex-col gap-6 px-6 py-6 lg:flex-row">
            <div className="flex-1 space-y-6">
              {(metaErr || error) && (
                <div className="rounded-lg border border-red-500/40 bg-red-500/10 px-4 py-2 text-sm text-red-200">
                  {metaErr || error}
                </div>
              )}

              {/* Top: Name + Type */}
              <div className="grid grid-cols-1 gap-4 md:grid-cols-2 xl:grid-cols-3">
                <div className="space-y-1">
                  <label className="text-sm font-medium text-neutral-200">Name</label>
                  <input
                    type="text"
                    className="w-full rounded-lg border border-neutral-700/70 bg-neutral-900/60 px-3 py-2 text-sm focus:border-indigo-500/70 focus:outline-none focus:ring-2 focus:ring-indigo-500/40"
                    value={name}
                    onChange={(e) => setName(e.target.value)}
                  />
                </div>
                <div className="space-y-1">
                  <label className="text-sm font-medium text-neutral-200">Indicator Type</label>
                  {initial?.id ? (
                    <div className="rounded-lg border border-neutral-700/70 bg-neutral-900/60 px-3 py-2 text-sm">
                      {typeId}
                    </div>
                  ) : (
                    <select
                      className="w-full rounded-lg border border-neutral-700/70 bg-neutral-900/60 px-3 py-2 text-sm focus:border-indigo-500/70 focus:outline-none focus:ring-2 focus:ring-indigo-500/40"
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

                {/* Quick search for params */}
                {typeId && (
                  <div className="space-y-1">
                    <label className="text-sm font-medium text-neutral-200">
                      Search params
                    </label>
                    <div className="relative">
                      <Search className="pointer-events-none absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-neutral-500" />
                      <input
                        type="text"
                        className="w-full rounded-lg border border-neutral-700/70 bg-neutral-900/60 pl-9 pr-3 py-2 text-sm focus:border-indigo-500/70 focus:outline-none focus:ring-2 focus:ring-indigo-500/40"
                        placeholder="Filter by name (press / to focus)"
                        value={filter}
                        onChange={(e) => setFilter(e.target.value)}
                        onKeyDown={(e) => {
                          if (e.key === "/") {
                            e.preventDefault();
                            e.currentTarget.focus();
                          }
                        }}
                      />
                    </div>
                  </div>
                )}
              </div>

              {/* PARAMS */}
              {typeId && (
                <div className="space-y-4">
                  <Section title="Essential parameters" keys={basicKeys} />

                  {advancedKeys.length > 0 && (
                    <div className="rounded-xl border border-neutral-700/80 bg-neutral-800/40 p-4">
                      <button
                        type="button"
                        onClick={() => setAdvancedOpen((prev) => !prev)}
                        className="flex w-full items-center justify-between text-left text-sm font-semibold text-neutral-200"
                      >
                        <span className="inline-flex items-center gap-2">
                          <SlidersHorizontal className="h-4 w-4" />
                          Advanced parameters
                        </span>
                        <span className="flex items-center gap-2 text-xs font-normal text-neutral-400">
                          {advancedKeys.length} fields
                          <ChevronDown
                            className={`h-4 w-4 transition-transform ${advancedOpen ? "rotate-180" : ""}`}
                          />
                        </span>
                      </button>
                      {advancedOpen ? (
                        <div className="mt-4 grid grid-cols-1 gap-3 md:grid-cols-2">
                          {advancedKeys.map(renderField).filter(Boolean)}
                        </div>
                      ) : (
                        <p className="mt-3 text-xs text-neutral-400">
                          Keep noise out of your workflow until you need to fine tune.
                        </p>
                      )}
                    </div>
                  )}

                  {availableSignalRules.length > 0 && (
                    <div className="rounded-xl border border-[color:var(--accent-alpha-30)] bg-[color:var(--accent-alpha-05)] p-4">
                      <div className="flex items-start justify-between gap-3">
                        <div>
                          <h4 className="text-sm font-semibold text-[color:var(--accent-text-strong)]">Signal rules</h4>
                          <p className="text-xs text-[color:var(--accent-text-soft-alpha)]">
                            Choose which detections run when generating signals for this indicator.
                          </p>
                        </div>
                        <div className="flex items-center gap-2 text-xs">
                          <button
                            type="button"
                            onClick={selectAllSignalRules}
                            className="rounded-full border border-[color:var(--accent-alpha-40)] bg-[color:var(--accent-alpha-10)] px-3 py-1 text-[color:var(--accent-text-strong)] transition hover:border-[color:var(--accent-alpha-60)] hover:bg-[color:var(--accent-alpha-20)]"
                          >
                            Select all
                          </button>
                          <button
                            type="button"
                            onClick={clearSignalRules}
                            className="rounded-full border border-slate-500/40 bg-slate-800/40 px-3 py-1 text-slate-200 transition hover:border-slate-400/60 hover:bg-slate-800/70"
                          >
                            Clear
                          </button>
                        </div>
                      </div>

                      <div className="mt-4 space-y-3">
                        {availableSignalRules.map((rule) => {
                          const checked = selectedSignalRules.includes(rule.id);
                          return (
                            <label
                              key={rule.id}
                              className={`flex items-start justify-between gap-3 rounded-lg border px-3 py-2 text-sm transition ${
                                checked
                                  ? "border-[color:var(--accent-alpha-60)] bg-[color:var(--accent-alpha-15)] text-[color:var(--accent-text-strong)]"
                                  : "border-white/10 bg-[#12141d] text-slate-200 hover:border-[color:var(--accent-alpha-40)] hover:text-[color:var(--accent-text-strong)]"
                              }`}
                            >
                              <div className="space-y-1">
                                <span className="font-medium">{rule.label || rule.id}</span>
                                {rule.description && (
                                  <p className="text-xs text-slate-300/80">{rule.description}</p>
                                )}
                              </div>
                              <Switch
                                checked={checked}
                                onChange={() => toggleSignalRule(rule.id)}
                                className={`${checked ? "bg-[color:var(--accent-alpha-80)]" : "bg-slate-600/70"} relative inline-flex h-6 w-11 items-center rounded-full transition`}
                              >
                                <span className={`${checked ? "translate-x-6" : "translate-x-1"} inline-block h-4 w-4 transform rounded-full bg-white transition`} />
                              </Switch>
                            </label>
                          );
                        })}
                      </div>

                      {availableSignalRules.length > 0 && (
                        <p className="mt-3 text-xs text-[color:var(--accent-text-muted)]">
                          {allRulesSelected
                            ? "All signal rules are enabled."
                            : selectedSignalRules.length === 0
                              ? "No signal rules selected. Signals will not be generated."
                              : `${selectedSignalRules.length} of ${availableSignalRules.length} rules enabled.`}
                        </p>
                      )}
                    </div>
                  )}
                </div>
              )}
            </div>

            {typeId && (
              <aside className="lg:w-72 space-y-4">
                <div className="rounded-xl border border-neutral-700/80 bg-neutral-900/40 p-4 space-y-4">
                  <div>
                    <h4 className="text-sm font-semibold text-neutral-200">
                      Workflow shortcuts
                    </h4>
                    <p className="text-xs text-neutral-400">
                      Fast access to the tools you reach for every session.
                    </p>
                  </div>

                  {advancedKeys.length > 0 && (
                    <button
                      type="button"
                      onClick={() => setAdvancedOpen((prev) => !prev)}
                      className="flex w-full items-center justify-between rounded-lg border border-neutral-700/70 bg-neutral-950/40 px-3 py-2 text-left text-sm text-neutral-200 transition hover:border-indigo-500/60 hover:text-neutral-100"
                    >
                      <span className="inline-flex items-center gap-2">
                        <SlidersHorizontal className="h-4 w-4" />
                        {advancedOpen ? "Hide" : "Show"} advanced
                      </span>
                      <span className="text-xs text-neutral-400">{advancedKeys.length}</span>
                    </button>
                  )}

                  <button
                    type="button"
                    onClick={copyParams}
                    className="flex w-full items-center gap-2 rounded-lg border border-neutral-700/70 bg-neutral-950/40 px-3 py-2 text-sm text-neutral-200 transition hover:border-indigo-500/60 hover:text-neutral-100"
                  >
                    <Copy className="h-4 w-4" /> Copy raw params
                  </button>

                  <button
                    type="button"
                    onClick={resetToDefaults}
                    className="flex w-full items-center gap-2 rounded-lg border border-neutral-700/70 bg-neutral-950/40 px-3 py-2 text-sm text-neutral-200 transition hover:border-indigo-500/60 hover:text-neutral-100"
                  >
                    <RotateCcw className="h-4 w-4" /> Reset to defaults
                  </button>
                </div>

                <div className="rounded-xl border border-neutral-700/80 bg-neutral-900/40 p-4">
                  <button
                    type="button"
                    onClick={() => setRawOpen((prev) => !prev)}
                    className="flex w-full items-center justify-between text-left text-sm font-semibold text-neutral-200"
                  >
                    <span className="inline-flex items-center gap-2">
                      <Braces className="h-4 w-4" />
                      Raw params JSON
                    </span>
                    <ChevronDown
                      className={`h-4 w-4 transition-transform ${rawOpen ? "rotate-180" : ""}`}
                    />
                  </button>
                  {rawOpen && (
                    <pre className="mt-3 max-h-64 overflow-auto rounded-lg border border-neutral-800 bg-neutral-950/80 p-3 text-xs leading-5 text-neutral-200">
                      {JSON.stringify(params, null, 2)}
                    </pre>
                  )}
                </div>

                <div className="rounded-xl border border-neutral-700/80 bg-neutral-900/40 p-4 text-xs text-neutral-400">
                  <p className="text-sm font-semibold text-neutral-200">Keyboard</p>
                  <div className="mt-3 space-y-2">
                    <div className="flex items-center justify-between gap-3">
                      <span>Focus parameter search</span>
                      <kbd className="rounded border border-neutral-700 bg-neutral-950 px-2 py-1 text-[10px] uppercase text-neutral-300">
                        /
                      </kbd>
                    </div>
                    <div className="flex items-center justify-between gap-3">
                      <span>Submit indicator</span>
                      <kbd className="rounded border border-neutral-700 bg-neutral-950 px-2 py-1 text-[10px] uppercase text-neutral-300">
                        Enter
                      </kbd>
                    </div>
                    <div className="flex items-center justify-between gap-3">
                      <span>Close panel</span>
                      <kbd className="rounded border border-neutral-700 bg-neutral-950 px-2 py-1 text-[10px] uppercase text-neutral-300">
                        Esc
                      </kbd>
                    </div>
                  </div>
                </div>
              </aside>
            )}
          </div>

          {/* ACTIONS */}
          <div className="border-t border-neutral-800 bg-neutral-900/80 px-6 py-4">
            <div className="flex justify-end gap-3">
              <button
                onClick={onClose}
                className="rounded-lg border border-neutral-700/70 px-4 py-2 text-sm text-neutral-200 transition hover:border-neutral-500 hover:text-white"
              >
                Cancel
              </button>
              <button
                onClick={handleSubmit}
                className="rounded-lg bg-indigo-600 px-4 py-2 text-sm font-semibold text-white transition hover:bg-indigo-500 disabled:opacity-50"
              >
                {initial?.id ? "Update" : "Create"}
              </button>
            </div>
          </div>
        </DialogPanel>
      </div>
    </Dialog>
  );
}
