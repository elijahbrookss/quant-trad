import React, { useState, useEffect } from 'react'
import { Switch } from '@headlessui/react'
import {
  fetchIndicators,
  createIndicator,
  updateIndicator,
  deleteIndicator,
} from '../adapters/indicator.adapter'
import IndicatorModal from './IndicatorModal'

export const IndicatorSection = () => {
  const [indicators, setIndicators] = useState([])
  const [modalOpen, setModalOpen] = useState(false)
  const [editing, setEditing] = useState(null)
  const [isLoading, setIsLoading] = useState(true)
  const [error, setError] = useState()


  // 1) load existing instances
  useEffect(() => {
    fetchIndicators()
      .then(setIndicators)
      .catch(e => setError(e.message))
      .finally(() => setIsLoading(false))
  }, [])

    // 2) open modal in Create or Edit mode
  const openCreate = () =>  { setEditing(null);    setError(null); setModalOpen(true) }
  const openEdit   = (item) => { setEditing(item);  setError(null); setModalOpen(true) }

    // 3) Save handler delegates to create/update adapter
  const handleSave = async (meta) => {
    try {
      let result
      if (meta.id) {
        result = await updateIndicator(meta.id, { type: meta.type, params: meta.params })
        setIndicators(indicators.map(i => i.id === result.id ? result : i))
      } else {
        result = await createIndicator({    type: meta.type, params: meta.params })
        setIndicators([...indicators, result])
      }
      setModalOpen(false)
    } catch (e) {
      setError(e.message)
    }
  }

    // 4) Delete instance
  const handleDelete = async (id) => {
    try {
      await deleteIndicator(id)
      setIndicators(indicators.filter(i => i.id !== id))
    } catch (e) {
      setError(e.message)
    }
  }

  const toggleEnable = (id) => {
    setIndicators((prev) =>
      prev.map((i) =>
        i.id === id ? { ...i, enabled: !i.enabled } : i
      )
    )
  }

  const openEditModal = (indicator) => {
    setEditing(indicator)
    setModalOpen(true)
    setError(undefined)
  }


  if (isLoading) return <div>Loading indicators…</div>
  if (error)     return <div className="text-red-500">Error: {error}</div>

  return (
    <div className="space-y-6">

        <button
          onClick={() => openEditModal()}
          className="bg-indigo-600 text-white px-4 py-2 rounded hover:bg-indigo-700 cursor-pointer transition-colors"
        >
          Create Indicator
        </button>
      {/* Indicator List */}
      <div className="space-y-3">
        {indicators.map((indicator) => (
        <div
            key={indicator.id}
            className="flex items-center justify-between px-4 py-3 rounded-lg shadow-lg bg-neutral-900"
        >
            <div>
                <div className="font-medium">{indicator.name}</div>
                <div className="text-sm text-gray-500">{indicator.type}</div>
                <div className="text-xs text-gray-600 italic">
                    Params:{' '}
                    {Object.entries(indicator.params)
                    .map(([k, v]) => `${k}=${v}`)
                    .join(', ')}
                </div>
            </div>

            <div className="flex items-center gap-4">
                {/* Enable/Disable Toggle */}
                <Switch
                    checked={indicator.enabled}
                    onChange={() => toggleEnable(indicator.id)}
                    className={`cursor-pointer ${
                    indicator.enabled ? 'bg-indigo-500' : 'bg-gray-600'
                    } relative inline-flex h-6 w-11 items-center rounded-full`}
                >
                    <span
                    className={`${
                        indicator.enabled ? 'translate-x-6' : 'translate-x-1'
                    } inline-block h-4 w-4 transform rounded-full bg-white transition`}
                    />
                </Switch>


                {/* Generate Signals */}
                <button
                    onClick={() => generateSignals(indicator.id)}
                    className="text-gray-400 hover:text-white cursor-pointer transition-colors"
                >
                    <svg xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" strokeWidth={1.5} stroke="currentColor" className="size-6">
                        <path strokeLinecap="round" strokeLinejoin="round" d="M21 12a9 9 0 1 1-18 0 9 9 0 0 1 18 0Z" />
                        <path strokeLinecap="round" strokeLinejoin="round" d="M15.91 11.672a.375.375 0 0 1 0 .656l-5.603 3.113a.375.375 0 0 1-.557-.328V8.887c0-.286.307-.466.557-.327l5.603 3.112Z" />
                    </svg>

                </button>

                {/* Edit Indicator */}
                <button
                    onClick={() => openEditModal(indicator)}
                    className="text-gray-400 hover:text-white cursor-pointer transition-colors"
                >
                    <svg xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" strokeWidth={1.5} stroke="currentColor" className="size-6">
                        <path strokeLinecap="round" strokeLinejoin="round" d="m16.862 4.487 1.687-1.688a1.875 1.875 0 1 1 2.652 2.652L10.582 16.07a4.5 4.5 0 0 1-1.897 1.13L6 18l.8-2.685a4.5 4.5 0 0 1 1.13-1.897l8.932-8.931Zm0 0L19.5 7.125M18 14v4.75A2.25 2.25 0 0 1 15.75 21H5.25A2.25 2.25 0 0 1 3 18.75V8.25A2.25 2.25 0 0 1 5.25 6H10" />
                    </svg>
                </button>

                {/* Delete */}
                <button
                    onClick={() => handleDelete(indicator.id)}
                    className="text-gray-400 hover:text-white cursor-pointer transition-colors"
                >
                    <svg xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" strokeWidth={1.5} stroke="currentColor" className="size-6">
                        <path strokeLinecap="round" strokeLinejoin="round" d="m14.74 9-.346 9m-4.788 0L9.26 9m9.968-3.21c.342.052.682.107 1.022.166m-1.022-.165L18.16 19.673a2.25 2.25 0 0 1-2.244 2.077H8.084a2.25 2.25 0 0 1-2.244-2.077L4.772 5.79m14.456 0a48.108 48.108 0 0 0-3.478-.397m-12 .562c.34-.059.68-.114 1.022-.165m0 0a48.11 48.11 0 0 1 3.478-.397m7.5 0v-.916c0-1.18-.91-2.164-2.09-2.201a51.964 51.964 0 0 0-3.32 0c-1.18.037-2.09 1.022-2.09 2.201v.916m7.5 0a48.667 48.667 0 0 0-7.5 0" />
                    </svg>                    
                </button>
            </div>
        </div>
        ))}
      </div>

      <IndicatorModal
        isOpen={modalOpen}
        onClose={() => setModalOpen(false)}
        initial={editing}
        onSave={handleSave}
        error={error}
        />
    </div>
  )
}
