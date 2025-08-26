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
    if (!typeId) return
    fetchIndicatorType(typeId)
      .then(meta => {
        setTypeMeta(meta)
        if (!initial) {
          // seed params in create mode
          const seed = {}
          meta.required_params.forEach(key => { seed[key] = '' })

          console.log("[IndicatorModal] Fetched type metadata:", meta)

          Object.entries(meta.default_params).forEach(([k, v]) => {
            seed[k] = v
          })
          setParams(seed)
        }
      })
      .catch(e => setMetaErr(e.message))
  }, [typeId, initial])

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
              {typeMeta.required_params.map(key => {
                const ftype = typeMeta.field_types[key] || 'string'
                return (
                  <div key={key}>
                    <label className="block text-sm mb-1">
                      {key} <span className="text-red-500">*</span>
                    </label>
                    {['int','float','number'].includes(ftype.toLowerCase()) ? (
                      <input
                        type="number"
                        step="any"
                        className="w-full p-2 rounded bg-neutral-700"
                        value={params[key] ?? ''}
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

              {Object.entries(typeMeta.default_params).map(([key, def]) => {
                const ftype = typeMeta.field_types[key] || 'string'
                return (
                  <div key={key}>
                    <label className="block text-sm mb-1">{key}</label>
                    {['int','float','number'].includes(ftype.toLowerCase()) ? (
                      <input
                        type="number"
                        step="any"
                        className="w-full p-2 rounded bg-neutral-700"
                        value={params[key]}
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
                )
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
