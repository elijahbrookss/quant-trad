// src/components/IndicatorModal.jsx
import React, { useState, useEffect } from 'react'
import { Dialog, DialogPanel, DialogTitle } from '@headlessui/react'
import {
  fetchIndicatorTypes,
  fetchIndicatorType,
} from '../adapters/indicator.adapter'

export default function IndicatorModal({
  isOpen,
  initial,   // { id?, type, name, params }
  error,
  onClose,
  onSave,    // fn({ id?, type, name, params })
}) {
  const [types, setTypes]       = useState([])
  const [typeId, setTypeId]     = useState(initial?.type || '')
  const [name, setName]         = useState(initial?.name || '')
  const [params, setParams]     = useState(initial?.params || {})
  const [metaErr, setMetaErr]   = useState(null)
  const [typeMeta, setTypeMeta] = useState({
    required_params: [],
    default_params: {},
    field_types: {},
  })

    // helpers for int lists
  const toInt = (v) => {
    if (typeof v === 'number') return Number.isFinite(v) ? Math.trunc(v) : null;
    if (typeof v === 'string') {
      const n = Number(v.trim());
      return Number.isFinite(n) ? Math.trunc(n) : null;
    }
    return null;
  };

  const toIntList = (v) => {
    if (Array.isArray(v)) return v.map(toInt).filter(n => n !== null);
    if (typeof v === 'string') {
      return v.split(/[\s,;]+/).filter(Boolean).map(toInt).filter(n => n !== null);
    }
    if (v == null) return [];
    const n = toInt(v);
    return n !== null ? [n] : [];
  };

  const listToString = (arr) => Array.isArray(arr) ? arr.join(', ') : (arr ?? '');

  // detect int-list fields from field_types or default arrays
  const intListKeys = React.useMemo(() => {
    const keys = new Set();
    const ft = typeMeta?.field_types || {};
    const dp = typeMeta?.default_params || {};
    Object.entries(ft).forEach(([k, t]) => {
      const s = String(t || '').toLowerCase();
      if (s === 'int_list' || s === 'list<int>' || s === 'int[]' || /list.*int/.test(s) || /int.*\[\]/.test(s)) {
        keys.add(k);
      }
    });
    Object.entries(dp).forEach(([k, v]) => {
      if (Array.isArray(v) && v.every(n => Number.isFinite(n))) keys.add(k);
    });
    return keys;
  }, [typeMeta]);

  // 1) Load list of available types when opening
  useEffect(() => {
    if (!isOpen) return
    fetchIndicatorTypes()
      .then(setTypes)
      .catch(e => setMetaErr(e.message))
  }, [isOpen])

  // 2) Reset form when opening or switching between create/edit
  useEffect(() => {
    if (!isOpen) return
    if (initial) {
      setTypeId(initial.type)
      setName(initial.name)
      setParams(initial.params || {})
    } else {
      setTypeId('')
      setName('')
      setParams({})
    }
    setTypeMeta({ required_params: [], default_params: {}, field_types: {} })
    setMetaErr(null)
  }, [initial, isOpen])

  // 3) When a type is chosen, fetch its metadata (required, defaults, types)
  useEffect(() => {
    if (!isOpen || !typeId) return; // fetch on each open even if typeId is unchanged
    fetchIndicatorType(typeId)
      .then(meta => {
        setTypeMeta(meta);
        if (!initial) {
          const seed = {};
          meta.required_params.forEach(key => { seed[key] = '' });
          Object.entries(meta.default_params).forEach(([k, v]) => { seed[k] = v });
          setParams(seed);
        }
      })
      .catch(e => setMetaErr(e.message));
  }, [isOpen, typeId]);


  const handleParamChange = (key, val) => {
    setParams(p => ({ ...p, [key]: val }))
  }

  const handleSubmit = () => {
    if (!typeId)   return setMetaErr('Please select a type.')
    if (!name.trim()) return setMetaErr('Please enter a name.')
    onSave({ id: initial?.id, type: typeId, name, params })
  }

  return (
    <Dialog open={isOpen} onClose={onClose} className="relative z-50">
      <div className="fixed inset-0 bg-black/30" aria-hidden="true" />
      <div className="fixed inset-0 flex items-center justify-center p-4">
        <DialogPanel className="w-full max-w-md bg-neutral-800 text-neutral-300 rounded p-6 space-y-4 shadow-lg">
          <DialogTitle className="text-lg font-semibold">
            {initial?.id ? 'Edit Indicator' : 'Create Indicator'}
          </DialogTitle>

          {metaErr && <div className="text-red-500">{metaErr}</div>}
          {error  && <div className="text-red-500">{error}</div>}

          
          {/* NAME */}
          <div>
            <label className="block text-sm mb-1">Name</label>
            <input
              type="text"
              className="w-full p-2 rounded bg-neutral-700"
              value={name}
              onChange={e => setName(e.target.value)}
            />
          </div>

          {/* TYPE */}
          <div>
            <label className="block text-sm mb-1">Indicator Type</label>
            {initial?.id ? (
              <div className="px-3 py-2 bg-neutral-700 rounded">{typeId}</div>
            ) : (
              <select
                className="w-full p-2 rounded bg-neutral-700"
                value={typeId}
                onChange={e => setTypeId(e.target.value)}
              >
                <option value="">— select type —</option>
                {types.map(t => (
                  <option key={t} value={t}>{t}</option>
                ))}
              </select>
            )}
          </div>


          {/* DYNAMIC PARAMS */}
          {typeId && (
            <div className="space-y-3 pt-2">
              {/* Required Params */}
              {typeMeta.required_params.map(key => {
                const ftype = typeMeta.field_types[key] || 'string'
                return (
                  <div key={key}>
                    <label className="block text-sm mb-1">
                      {key} <span className="text-red-500">*</span>
                    </label>

                    {intListKeys.has(key) ? (
                      <input
                        type="text"
                        inputMode="numeric"
                        pattern="^[0-9\\s,;]*$"
                        className="w-full p-2 rounded bg-neutral-700"
                        value={listToString(params[key])}
                        onChange={e => handleParamChange(key, e.target.value)}
                        onBlur={e => handleParamChange(key, toIntList(e.target.value))}
                        placeholder="e.g., 5, 10, 20"
                      />
                    ) : ['int','float','number'].includes(ftype.toLowerCase()) ? (
                      <input
                        type="number"
                        step="any"
                        className="w-full p-2 rounded bg-neutral-700"
                        value={Number.isFinite(params[key]) ? params[key] : ''}
                        onChange={e => handleParamChange(key, e.target.valueAsNumber)}
                      />
                    ) : ftype.toLowerCase() === 'bool' ? (
                      <input
                        type="checkbox"
                        checked={Boolean(params[key])}
                        onChange={e => handleParamChange(key, e.target.checked)}
                      />
                    ) : (
                      <input
                        type="text"
                        className="w-full p-2 rounded bg-neutral-700"
                        value={params[key] ?? ''}
                        onChange={e => handleParamChange(key, e.target.value)}
                      />
                    )}

                  </div>
                )
              })}

              {/* Optional Params */}
              {Object.entries(typeMeta.default_params).map(([key, def]) => {
                const ftype = typeMeta.field_types[key] || 'string';
                return (
                  <div key={key}>
                    <label className="block text-sm mb-1">{key}</label>

                    {intListKeys.has(key) ? (
                      <input
                        type="text"
                        inputMode="numeric"
                        pattern="^[0-9\\s,;]*$"
                        className="w-full p-2 rounded bg-neutral-700"
                        value={listToString(params[key])}
                        placeholder={Array.isArray(def) ? def.join(', ') : String(def)}
                        onChange={e => handleParamChange(key, e.target.value)}
                        onBlur={e => handleParamChange(key, toIntList(e.target.value))}
                      />
                    ) : ['int','float','number'].includes(ftype.toLowerCase()) ? (
                      <input
                        type="number"
                        step="any"
                        className="w-full p-2 rounded bg-neutral-700"
                        value={Number.isFinite(params[key]) ? params[key] : ''}
                        placeholder={String(def)}
                        onChange={e => handleParamChange(key, e.target.valueAsNumber)}
                      />
                    ) : ftype.toLowerCase() === 'bool' ? (
                      <input
                        type="checkbox"
                        checked={Boolean(params[key])}
                        onChange={e => handleParamChange(key, e.target.checked)}
                      />
                    ) : (
                      <input
                        type="text"
                        className="w-full p-2 rounded bg-neutral-700"
                        value={params[key]}
                        placeholder={String(def)}
                        onChange={e => handleParamChange(key, e.target.value)}
                      />
                    )}
                  </div>
                );
              })}
            </div>
          )}

          {/* ACTIONS */}
          <div className="flex justify-end space-x-3 pt-4">
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
              {initial?.id ? 'Update' : 'Create'}
            </button>
          </div>
        </DialogPanel>
      </div>
    </Dialog>
  )
}
