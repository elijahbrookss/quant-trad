import  { useState, useEffect } from 'react'
import { Switch } from '@headlessui/react'
import {
  fetchIndicators,
  createIndicator,
  updateIndicator,
  deleteIndicator,
} from '../adapters/indicator.adapter'
import IndicatorModal from './IndicatorModal'
import { useChartState } from '../contexts/ChartStateContext'

// Manages the list of indicators and syncs enabled ones to the chart context
export const IndicatorSection = ({ chartId }) => {
  const [indicators, setIndicators] = useState([])
  const [modalOpen, setModalOpen] = useState(false)
  const [editing, setEditing] = useState(null)
  const [isLoading, setIsLoading] = useState(true)
  const [error, setError] = useState(null)

  const { updateChart, getChart } = useChartState()

  console.log(chartId, "chartId in IndicatorSection")
  const chartState = getChart(chartId, "indicatorTab")

  // Fetch indicators whenever chart parameters change
  useEffect(() => {
    if (!chartState) {
      console.log("returning", chartState)
      return
    }

    let isMounted = true
    setIsLoading(true)

    console.log ("Updating indicators!!")

    fetchIndicators({
      symbol: chartState.symbol,
      interval: chartState.interval,
      // add other params if needed
    })
      .then(data => {
        if (!isMounted) return
        setIndicators(data)
        // Initialize overlays for enabled indicators
        const initialOverlays = data
          .filter(i => i.enabled)
          .map(i => ({ id: i.id, type: i.type, params: i.params }))

        updateChart(chartId, { overlays: initialOverlays, indicators: data })
        console.log("Updated chart overlays:", initialOverlays)
      })
      .catch(e => {
        if (!isMounted) return
        setError(e.message)
      })
      .finally(() => {
        if (isMounted) setIsLoading(false)
      })
    return () => { isMounted = false }
  }, [chartState, chartState?.symbol, chartState?.interval, chartState?.dateRange])

  // Create or update indicator
  const handleSave = async (meta) => {
    try {
      let result
      if (meta.id) {
        result = await updateIndicator(meta.id, { type: meta.type, params: meta.params })
        setIndicators(prev => prev.map(i => i.id === result.id ? result : i))
      } else {
        result = await createIndicator({ type: meta.type, params: meta.params, name: meta.name })
        setIndicators(prev => [...prev, result])
      }
      setModalOpen(false)
    } catch (e) {
      setError(e.message)
    }
  }

  // Delete indicator
  const handleDelete = async (id) => {
    try {
      await deleteIndicator(id)
      setIndicators(prev => prev.filter(i => i.id !== id))
    } catch (e) {
      setError(e.message)
    }
  }

  // Toggle enable/disable
  const toggleEnable = (id) => {
    setIndicators(prev =>
      prev.map(i => i.id === id ? { ...i, enabled: !i.enabled } : i)
    )
  }

  // Open modal for create/edit
  const openEditModal = (indicator = null) => {
    setEditing(indicator)
    setModalOpen(true)
    setError(null)
  }

  if (isLoading) return <div>Loading indicators…</div>
  if (error) return <div className="text-red-500">Error: {error}</div>

  return (
    <div className="space-y-6">
      <button
        onClick={() => openEditModal()}
        className="flex flex-col items-center w-full px-4 py-3 rounded-lg bg-neutral-900 text-neutral-400 hover:text-neutral-100 shadow-lg"
      >
        {/* plus icon preserved */}
        <svg xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" strokeWidth={1.5} stroke="currentColor" className="size-6 mb-2">
          <path strokeLinecap="round" strokeLinejoin="round" d="M12 9v6m3-3H9m12 0a9 9 0 1 1-18 0 9 9 0 0 1 18 0Z" />
        </svg>
        Create Indicator
      </button>

      {/* List of indicators */}
      <div className="space-y-1">
        {indicators.map(indicator => (
          <div key={indicator.id} className="flex items-center justify-between px-4 py-3 rounded-lg bg-neutral-900 shadow-lg">
            <div>
              <div className="font-medium text-white">{indicator.name}</div>
              <div className="text-sm text-gray-500">{indicator.type}</div>
              <div className="text-xs text-gray-600 italic">
                Params: {Object.entries(indicator.params).map(([k, v]) => `${k}=${v}`).join(', ')}
              </div>
            </div>
            <div className="flex items-center gap-4">
              {/* Enable/Disable switch */}
              <Switch
                checked={indicator.enabled}
                onChange={() => toggleEnable(indicator.id)}
                className={`${indicator.enabled ? 'bg-indigo-500' : 'bg-gray-600'} relative inline-flex h-6 w-11 items-center rounded-full cursor-pointer`}
              >
                <span className={`${indicator.enabled ? 'translate-x-6' : 'translate-x-1'} inline-block h-4 w-4 transform rounded-full bg-white transition`} />
              </Switch>

              {/* Edit icon */}
              <button onClick={() => openEditModal(indicator)} className="text-gray-400 hover:text-white">
                <svg xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" strokeWidth={1.5} stroke="currentColor" className="size-5">
                  <path strokeLinecap="round" strokeLinejoin="round" d="M16.862 4.487l1.651 1.651m-2.121-2.12a2.062 2.062 0 013.166 2.679L12.75 15.5H9.25v-3.5l6.119-6.119z" />
                </svg>
              </button>

              {/* Delete icon */}
              <button onClick={() => handleDelete(indicator.id)} className="text-gray-400 hover:text-white">
                <svg xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" strokeWidth={1.5} stroke="currentColor" className="size-5">
                  <path strokeLinecap="round" strokeLinejoin="round" d="M6 18L18 6M6 6l12 12" />
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
