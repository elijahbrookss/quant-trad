// src/components/IndicatorModal.v2.jsx
import React, { useEffect, useMemo, useState } from "react";
import { Dialog, DialogPanel, DialogTitle, Disclosure, Switch } from "@headlessui/react";
import { ChevronDown, Copy, RotateCcw } from "lucide-react";
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
  }, [isOpen, typeId]);

  /** UI state **/
  const [filter, setFilter] = useState("");
  const [showRaw, setShowRaw] = useState(false);
  const [expandAll, setExpandAll] = useState(false);

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

    // searchable filter
    if (filter && !key.toLowerCase().includes(filter.toLowerCase())) return null;

    return (
      <div key={key} className="space-y-1">
        <div className="flex items-center justify-between">
          <label className="block text-sm font-medium text-neutral-200">
            {key}
            {typeMeta.required_params?.includes(key) && (
              <span className="text-red-500 ml-1">*</span>
            )}
          </label>
          {descriptionFor(key) && (
            <span className="text-xs text-neutral-400 ml-2">{descriptionFor(key)}</span>
          )}
        </div>

        {intListKeys.has(key) ? (
          <input
            type="text"
            inputMode="numeric"
            pattern="^[0-9\\s,;]*$"
            className="w-full p-2 rounded bg-neutral-700"
            value={listToString(val)}
            placeholder="e.g., 5, 10, 20"
            onChange={(e) => setParams((p) => ({ ...p, [key]: e.target.value }))}
            onBlur={(e) => setParams((p) => ({ ...p, [key]: toIntList(e.target.value) }))}
          />
        ) : enumVals?.length ? (
          <select
            className="w-full p-2 rounded bg-neutral-700"
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
              checked={Boolean(val)}
              onChange={(checked) => setParams((p) => ({ ...p, [key]: checked }))}
              className={`${Boolean(val) ? "bg-indigo-600" : "bg-neutral-600"} relative inline-flex h-6 w-11 items-center rounded-full transition-colors`}
            >
              <span className={`${Boolean(val) ? "translate-x-6" : "translate-x-1"} inline-block h-4 w-4 transform rounded-full bg-white transition-transform`} />
            </Switch>
            <span className="text-sm text-neutral-300">{String(val)}</span>
          </div>
        ) : ["int", "float", "number"].includes(ftype) ? (
          <input
            type="number"
            step={ftype === "int" ? 1 : "any"}
            className="w-full p-2 rounded bg-neutral-700"
            value={Number.isFinite(val) ? val : ""}
            onChange={(e) =>
              setParams((p) => ({ ...p, [key]: e.target.valueAsNumber }))
            }
          />
        ) : (
          <input
            type="text"
            className="w-full p-2 rounded bg-neutral-700"
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
      <div className="space-y-3">
        <h4 className="text-sm font-semibold text-neutral-300">{title}</h4>
        <div className="grid grid-cols-1 md:grid-cols-2 gap-3">{fields}</div>
      </div>
    );
  };

  return (
    <Dialog open={isOpen} onClose={onClose} className="relative z-50">
      <div className="fixed inset-0 bg-black/40" aria-hidden="true" />
      <div className="fixed inset-0 flex items-center justify-center p-4">
        <DialogPanel className="w-full max-w-3xl bg-neutral-800 text-neutral-200 rounded-xl p-6 space-y-4 shadow-2xl border border-neutral-700">
          <DialogTitle className="text-lg font-semibold">
            {initial?.id ? "Edit Indicator" : "Create Indicator"}
          </DialogTitle>

          {(metaErr || error) && (
            <div className="text-red-400 text-sm">{metaErr || error}</div>
          )}

          {/* Top: Name + Type */}
          <div className="grid grid-cols-1 md:grid-cols-3 gap-3">
            <div className="md:col-span-1">
              <label className="block text-sm mb-1">Name</label>
              <input
                type="text"
                className="w-full p-2 rounded bg-neutral-700"
                value={name}
                onChange={(e) => setName(e.target.value)}
              />
            </div>
            <div className="md:col-span-1">
              <label className="block text-sm mb-1">Indicator Type</label>
              {initial?.id ? (
                <div className="px-3 py-2 bg-neutral-700 rounded">{typeId}</div>
              ) : (
                <select
                  className="w-full p-2 rounded bg-neutral-700"
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
              <div className="md:col-span-1">
                <label className="block text-sm mb-1">Search params</label>
                <input
                  type="text"
                  className="w-full p-2 rounded bg-neutral-700"
                  placeholder="Filter by name ( / to focus )"
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
            )}
          </div>

          {/* PARAMS */}
          {typeId && (
            <div className="space-y-4">
              <Section title="Essential" keys={basicKeys} />

              {/* Advanced is collapsible with count */}
              {advancedKeys.length > 0 && (
                <Disclosure defaultOpen={expandAll}>
                  {({ open }) => (
                    <div className="border-t border-neutral-700 pt-3">
                      <Disclosure.Button className="w-full flex items-center justify-between text-left">
                        <span className="text-sm font-semibold text-neutral-300">
                          Advanced ({advancedKeys.length})
                        </span>
                        <ChevronDown
                          className={`h-4 w-4 transition-transform ${open ? "rotate-180" : ""}`}
                        />
                      </Disclosure.Button>
                      <Disclosure.Panel className="mt-3">
                        <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
                          {advancedKeys.map(renderField).filter(Boolean)}
                        </div>
                      </Disclosure.Panel>
                    </div>
                  )}
                </Disclosure>
              )}

              {/* Raw params viewer */}
              <Disclosure>
                {({ open }) => (
                  <div className="border-t border-neutral-700 pt-3">
                    <div className="flex items-center justify-between">
                      <Disclosure.Button className="text-sm font-semibold text-neutral-300 flex items-center gap-2">
                        <ChevronDown
                          className={`h-4 w-4 transition-transform ${open ? "rotate-180" : ""}`}
                        />
                        Raw params JSON
                      </Disclosure.Button>
                      <div className="flex items-center gap-2">
                        <button
                          onClick={copyParams}
                          type="button"
                          className="inline-flex items-center gap-1 text-xs px-2 py-1 rounded border border-neutral-600 hover:bg-neutral-700"
                          title="Copy JSON to clipboard"
                        >
                          <Copy className="h-3 w-3" /> Copy
                        </button>
                        <button
                          onClick={resetToDefaults}
                          type="button"
                          className="inline-flex items-center gap-1 text-xs px-2 py-1 rounded border border-neutral-600 hover:bg-neutral-700"
                          title="Reset to defaults"
                        >
                          <RotateCcw className="h-3 w-3" /> Reset
                        </button>
                      </div>
                    </div>
                    <Disclosure.Panel>
                      <pre className="mt-2 max-h-56 overflow-auto bg-neutral-900 rounded p-3 text-xs leading-5">
                        {JSON.stringify(params, null, 2)}
                      </pre>
                    </Disclosure.Panel>
                  </div>
                )}
              </Disclosure>
            </div>
          )}

          {/* ACTIONS */}
          <div className="sticky bottom-0 pt-4">
            <div className="flex justify-end gap-3 border-t border-neutral-700 pt-4">
              <button
                onClick={onClose}
                className="px-4 py-2 rounded border border-gray-600 cursor-pointer hover:bg-neutral-700"
              >
                Cancel
              </button>
              <button
                onClick={handleSubmit}
                className="px-4 py-2 rounded bg-indigo-600 hover:bg-indigo-700 text-white cursor-pointer disabled:opacity-50"
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
