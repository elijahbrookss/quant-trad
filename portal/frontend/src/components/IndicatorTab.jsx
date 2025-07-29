import { useState, useEffect } from 'react'
import { Dialog, DialogPanel, DialogTitle } from '@headlessui/react'
import { Switch } from '@headlessui/react'
import { fetchIndicators } from '../adapters/indicator.adapter'

export const IndicatorSection = () => {
  const [indicators, setIndicators] = useState([])
  const [isModalOpen, setIsModalOpen] = useState(false)
  const [editing, setEditing] = useState(null)
  const [isLoading, setIsLoading] = useState(true)
  const [error, setError] = useState()


    useEffect(() => {
        const load = async () => {
        try {
            const data = await fetchIndicators()
            setIndicators(data)
        } catch (err) {
            console.error(err)
            setError(err.message || 'Failed to load indicators')
        } finally {
            setIsLoading(false)
        }
        }
        load()
    }, [])

  const toggleEnable = (id) => {
    setIndicators((prev) =>
      prev.map((i) =>
        i.id === id ? { ...i, enabled: !i.enabled } : i
      )
    )
  }

  const openEditModal = (indicator) => {
    setEditing(indicator)
    setIsModalOpen(true)
  }

  const handleSave = () => {
    setIndicators((prev) =>
      prev.map((i) => (i.id === editing?.id ? editing : i))
    )
    setIsModalOpen(false)
  }

  if (isLoading) return <div>Loading indicators…</div>
  if (error)     return <div className="text-red-500">Error: {error}</div>

  return (
    <div className="space-y-6">

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
                <Switch
                    checked={indicator.enabled}
                    onChange={() => toggleEnable(indicator.id)}
                    className={`${
                    indicator.enabled ? 'bg-indigo-500' : 'bg-gray-600'
                    } relative inline-flex h-6 w-11 items-center rounded-full`}
                >
                    <span
                    className={`${
                        indicator.enabled ? 'translate-x-6' : 'translate-x-1'
                    } inline-block h-4 w-4 transform rounded-full bg-white transition`}
                    />
                </Switch>

                <button
                    onClick={() => openEditModal(indicator)}
                    className="text-gray-400 hover:text-white cursor-pointer transition-colors"
                >
                    <svg xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" strokeWidth={1.5} stroke="currentColor" className="size-6">
                        <path strokeLinecap="round" strokeLinejoin="round" d="m16.862 4.487 1.687-1.688a1.875 1.875 0 1 1 2.652 2.652L10.582 16.07a4.5 4.5 0 0 1-1.897 1.13L6 18l.8-2.685a4.5 4.5 0 0 1 1.13-1.897l8.932-8.931Zm0 0L19.5 7.125M18 14v4.75A2.25 2.25 0 0 1 15.75 21H5.25A2.25 2.25 0 0 1 3 18.75V8.25A2.25 2.25 0 0 1 5.25 6H10" />
                    </svg>
                </button>

                <button
                    onClick={() => generateSignals(indicator.id)}
                    className="text-gray-400 hover:text-white cursor-pointer transition-colors"
                >
                    <svg xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" strokeWidth={1.5} stroke="currentColor" className="size-6">
                        <path strokeLinecap="round" strokeLinejoin="round" d="M21 12a9 9 0 1 1-18 0 9 9 0 0 1 18 0Z" />
                        <path strokeLinecap="round" strokeLinejoin="round" d="M15.91 11.672a.375.375 0 0 1 0 .656l-5.603 3.113a.375.375 0 0 1-.557-.328V8.887c0-.286.307-.466.557-.327l5.603 3.112Z" />
                    </svg>

                </button>
            </div>
        </div>
        ))}
      </div>

      {/* Edit/Create Modal */}
      <Dialog open={isModalOpen} onClose={() => setIsModalOpen(false)} className="relative z-50">
        <div className="fixed inset-0 bg-black/30" aria-hidden="true" />
        <div className="fixed inset-0 flex items-center justify-center p-4">
          <DialogPanel className="w-full max-w-md rounded p-6 shadow-lg space-y-4 bg-neutral-800 text-neutral-300">
            <DialogTitle className="text-lg font-semibold">
              {editing ? 'Edit Indicator' : 'Create Indicator'}
            </DialogTitle>

            <div className="space-y-2">
              <div>
                <label className="block text-sm font-medium">Name</label>
                <input
                  type="text"
                  className="w-full border rounded px-3 py-1.5"
                  value={editing?.name || ''}
                  onChange={(e) =>
                    setEditing((prev) =>
                      prev ? { ...prev, name: e.target.value } : null
                    )
                  }
                />
              </div>
              <div>
                <label className="block text-sm font-medium">Type</label>
                <input
                  type="text"
                  className="w-full border rounded px-3 py-1.5"
                  value={editing?.type || ''}
                  onChange={(e) =>
                    setEditing((prev) =>
                      prev ? { ...prev, type: e.target.value } : null
                    )
                  }
                />
              </div>
              <div>
                <label className="block text-sm font-medium">Params (JSON)</label>
                <textarea
                  rows={3}
                  className="w-full border rounded px-3 py-1.5 font-mono text-xs"
                  value={
                    editing?.params
                      ? JSON.stringify(editing.params, null, 2)
                      : '{}'
                  }
                  onChange={(e) =>
                    setEditing((prev) => {
                      try {
                        return prev
                          ? { ...prev, params: JSON.parse(e.target.value) }
                          : null
                      } catch {
                        return prev
                      }
                    })
                  }
                />
              </div>
            </div>

            <div className="flex justify-end pt-4 space-x-3">
              <button onClick={() => setIsModalOpen(false)} className="px-4 py-2 rounded border border-gray-300">
                Cancel
              </button>
              <button
                onClick={handleSave}
                className="px-4 py-2 rounded bg-indigo-600 text-white hover:bg-indigo-700"
              >
                Save
              </button>
            </div>
          </DialogPanel>
        </div>
      </Dialog>
    </div>
  )
}
