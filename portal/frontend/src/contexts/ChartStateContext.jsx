import { createContext, useContext, useState } from 'react'

// Shape of a single chart's state
// {
//   symbol: string,
//   interval: string,
//   dateRange: [Date, Date],
//   overlays: Array<any>
// }

const ChartStateContext = createContext(null)

export const ChartStateProvider = ({ children }) => {
  const [charts, setCharts] = useState({})

  // Register a new chart or reset an existing one
  const registerChart = (chartId, initialState = {}) => {
    setCharts(prev => ({
      ...prev,
      [chartId]: {
        symbol: initialState.symbol || '',
        interval: initialState.interval || '',
        dateRange: initialState.dateRange || [],
        overlays: initialState.overlays || [],
      },
    }))
  }

  // Update fields on an existing chart
  const updateChart = (chartId, newState) => {
    setCharts(prev => ({
      ...prev,
      [chartId]: {
        ...prev[chartId],
        ...newState,
      },
    }))
  }

  // Retrieve chart state by ID
  const getChart = (chartId, place) => { 
    console.log("Getting chart state for ID and place:", chartId, place)
    return charts[chartId] || null
  }

  return (
    <ChartStateContext.Provider value={{ registerChart, updateChart, getChart }}>
      {children}
    </ChartStateContext.Provider>
  )
}

export const useChartState = () => {
  const context = useContext(ChartStateContext)
  if (!context) {
    throw new Error('useChartState must be used within a ChartStateProvider')
  }
  return context
}
