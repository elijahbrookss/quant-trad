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

  // Debug: chartId and chartState
  console.log("[IndicatorSection] chartId:", chartId)
  const chartState = getChart(chartId, "indicatorTab")
  console.log("[IndicatorSection] chartState:", chartState)


  // Fetch indicators whenever chart parameters change
  useEffect(() => {
    if (!chartState?.refreshKey) {
      console.warn("[IndicatorSection] No chartState found, skipping fetch")
      return
    }


    let isMounted = true
    setIsLoading(true)
    console.log("[IndicatorSection] Fetching indicators for", chartState.symbol, chartState.interval)

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
        console.log("[IndicatorSection] Updated chart overlays:", initialOverlays)
      })
      .catch(e => {
        if (!isMounted) return
        setError(e.message)
        console.error("[IndicatorSection] Error fetching indicators:", e)
      })
      .finally(() => {
        if (isMounted) setIsLoading(false)
        console.log("[IndicatorSection] Indicator fetch complete")
      })
    return () => { isMounted = false }
  }, [chartState?.refreshKey])

  // Create or update indicator
  const handleSave = async (meta) => {
    try {
      const params = { ...meta.params, start: chartState.start, end: chartState.end, symbol: chartState.symbol, interval: chartState.interval }

      let result
      if (meta.id) {
        // combine params and chartState fields (start, end, etc.)

        result = await updateIndicator(meta.id, { type: meta.type, params, name: meta.name })
        setIndicators(prev => prev.map(i => i.id === result.id ? result : i))
        console.log("[IndicatorSection] Updated indicator:", result)
      } else {
        result = await createIndicator({ type: meta.type, params, name: meta.name })
        setIndicators(prev => [...prev, result])
        console.log("[IndicatorSection] Created new indicator:", result)
      }
      setModalOpen(false)
    } catch (e) {
      setError(e.message)
      console.error("[IndicatorSection] Error saving indicator:", e)
    }
  }

  // Delete indicator
  const handleDelete = async (id) => {
    try {
      await deleteIndicator(id)
      setIndicators(prev => prev.filter(i => i.id !== id))
      console.log("[IndicatorSection] Deleted indicator with id:", id)
    } catch (e) {
      setError(e.message)
      console.error("[IndicatorSection] Error deleting indicator:", e)
    }
  }

  // Toggle enable/disable
  const toggleEnable = (id) => {
    setIndicators(prev =>
      prev.map(i => i.id === id ? { ...i, enabled: !i.enabled } : i)
    )
    console.log("[IndicatorSection] Toggled enabled for indicator id:", id)
  }

  // Open modal for create/edit
  const openEditModal = (indicator = null) => {
    setEditing(indicator)
    setModalOpen(true)
    setError(null)
    console.log("[IndicatorSection] Opened modal for indicator:", indicator)
  }

  if (isLoading) {
    console.log("[IndicatorSection] Loading indicators…")
    return <div>Loading indicators…</div>
  }
  if (error) {
    console.error("[IndicatorSection] Error:", error)
    return <div className="text-red-500">Error: {error}</div>
  }
  
  // If no chart state or chartId is missing, log error
  if (!chartState || !chartId ) {
    console.warn("[IndicatorSection] No chart state found or chartId is missing")
    return <div className="text-red-500">Error: No chart state found</div>
  }

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

              {/* Edit Button */}
              <button
                onClick={() => openEditModal(indicator)}
                className="text-gray-400 hover:text-white cursor-pointer transition-colors"
              >
                <svg xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" strokeWidth={1.5} stroke="currentColor" className="size-6">
                  <path strokeLinecap="round" strokeLinejoin="round" d="m16.862 4.487 1.687-1.688a1.875 1.875 0 1 1 2.652 2.652L10.582 16.07a4.5 4.5 0 0 1-1.897 1.13L6 18l.8-2.685a4.5 4.5 0 0 1 1.13-1.897l8.932-8.931Zm0 0L19.5 7.125M18 14v4.75A2.25 2.25 0 0 1 15.75 21H5.25A2.25 2.25 0 0 1 3 18.75V8.25A2.25 2.25 0 0 1 5.25 6H10" />
                </svg>
              </button>

              {/* Generate Signals */}
              <button
                  onClick={() => generateSignals(indicator.id)}
                  className="text-green-400 hover:text-green-200 cursor-pointer transition-colors"
              >
                <svg xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" strokeWidth={1.5} stroke="currentColor" className="size-6">
                    <path strokeLinecap="round" strokeLinejoin="round" d="M21 12a9 9 0 1 1-18 0 9 9 0 0 1 18 0Z" />
                    <path strokeLinecap="round" strokeLinejoin="round" d="M15.91 11.672a.375.375 0 0 1 0 .656l-5.603 3.113a.375.375 0 0 1-.557-.328V8.887c0-.286.307-.466.557-.327l5.603 3.112Z" />
                </svg>
              </button>

              {/* Delete Button */}
              <button
                onClick={() => handleDelete(indicator.id)}
                className="text-red-400 hover:text-red-200 cursor-pointer transition-colors"
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
