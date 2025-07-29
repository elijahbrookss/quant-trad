import { useState, useEffect } from 'react'
import { Dialog } from '@headlessui/react'
import { fetchIndicatorTypes } from '../adapters/indicator.adapter'

export default function IndicatorModal({
  isOpen,
  initial,   // { id?, type, name, params }
  error,
  onClose,
  onSave,    // fn({ id?, type, name, params })
}) {
  const [types, setTypes] = useState([])
  const [typeId, setTypeId]   = useState(initial?.type   || '')
  const [name, setName]       = useState(initial?.name   || '')
  const [params, setParams]   = useState(initial?.params || {})
  const [metaErr, setMetaErr] = useState(null)

  // load type-metadata once
  useEffect(() => {
    if (!isOpen) return
    fetchIndicatorTypes().then(setTypes).catch(e=>setMetaErr(e.message))
  }, [isOpen])

  // reset when we open or switch between create/edit
  useEffect(() => {
    if (!isOpen) return
    if (initial) {
      setTypeId(initial.type)
      setName(initial.name)
      setParams(initial.params)
    } else {
      setTypeId('')
      setName('')
      setParams({})
    }
    setMetaErr(null)
  }, [initial, isOpen])

  // whenever you pick a new type in **create** mode, seed its params
  useEffect(() => {
    if (!typeId || initial) return
    // here types is just an array of strings; if you
    // actually need param‐metadata you’d have to fetch it
    // (see note below)
    const seed = {}
    // assume you know which fields each type needs:
    // e.g. for “vwap” you need “window” & “source”
    // for “pivot_level” you need “lookback”
    // etc.
    // hard-code or fetch a separate /api/indicator-types/:type endpoint
    if (typeId === 'vwap') {
      seed.window = ''
      seed.source = 'close'
    } else if (typeId === 'pivot_level') {
      seed.lookback = ''
    }
    // … repeat for each type …
    setParams(seed)
  }, [typeId, initial, types])

  const handleChangeParam = (key, val) => {
    setParams(p => ({ ...p, [key]: val }))
  }

  const handleSubmit = () => {
    if (!typeId) return setMetaErr('Please select a type.')
    if (!name.trim()) return setMetaErr('Please enter a name.')
    onSave({ id: initial?.id, type: typeId, name, params })
  }

  // Determine which param fields to render
  let requiredFields = []
  if (typeId === 'vwap') requiredFields = ['window','source']
  if (typeId === 'pivot_level') requiredFields = ['lookback']
  // … and so on …

  return (
    <Dialog open={isOpen} onClose={onClose} className="relative z-50">
      <div className="fixed inset-0 bg-black/30" />
      <div className="fixed inset-0 flex items-center justify-center p-4">
        <Dialog.Panel className="w-full max-w-md bg-neutral-800 text-neutral-300 rounded p-6 space-y-4 shadow-lg">
          <Dialog.Title className="text-lg font-semibold">
            {initial?.id ? 'Edit Indicator' : 'Create Indicator'}
          </Dialog.Title>

          {metaErr && <div className="text-red-500">{metaErr}</div>}
          {error   && <div className="text-red-500">{error}</div>}

          {/* 1) TYPE SELECT */}
          <div>
            <label className="block text-sm mb-1">Indicator Type</label>
            {initial?.id
              ? <div className="px-3 py-2 bg-neutral-700 rounded">{typeId}</div>
              : (
                <select
                  className="w-full p-2 rounded bg-neutral-700"
                  value={typeId}
                  onChange={e=>setTypeId(e.target.value)}
                >
                  <option value="">— select type —</option>
                  {types.map(t=> <option key={t} value={t}>{t}</option>)}
                </select>
              )
            }
          </div>

          {/* 2) NAME */}
          <div>
            <label className="block text-sm mb-1">Name</label>
            <input
              type="text"
              className="w-full p-2 rounded bg-neutral-700"
              value={name}
              onChange={e=>setName(e.target.value)}
            />
          </div>

          {/* 3) DYNAMIC PARAM FIELDS */}
          {typeId && (
            <div className="space-y-3">
              {requiredFields.map(key => (
                <div key={key}>
                  <label className="block text-sm mb-1">{key}</label>
                  <input
                    type="text"
                    className="w-full p-2 rounded bg-neutral-700"
                    value={params[key]||''}
                    onChange={e=>handleChangeParam(key, e.target.value)}
                  />
                </div>
              ))}
            </div>
          )}

          {/* ACTIONS */}
          <div className="flex justify-end space-x-3 pt-4">
            <button
              onClick={onClose}
              className="px-4 py-2 rounded border border-gray-600"
            >
              Cancel
            </button>
            <button
              onClick={handleSubmit}
              className="px-4 py-2 rounded bg-indigo-600 hover:bg-indigo-700 text-white"
            >
              {initial?.id ? 'Update' : 'Create'}
            </button>
          </div>
        </Dialog.Panel>
      </div>
    </Dialog>
  )
}
