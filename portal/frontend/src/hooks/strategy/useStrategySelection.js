import { useEffect, useMemo, useState } from 'react'

const useStrategySelection = (strategies) => {
  const [selectedId, setSelectedId] = useState(null)

  useEffect(() => {
    if (!Array.isArray(strategies) || strategies.length === 0) {
      setSelectedId(null)
      return
    }
    if (!strategies.some((strategy) => strategy.id === selectedId)) {
      setSelectedId(strategies[0].id)
    }
  }, [strategies, selectedId])

  const selectedStrategy = useMemo(
    () => strategies.find((strategy) => strategy.id === selectedId) || null,
    [strategies, selectedId],
  )

  return {
    selectedId,
    setSelectedId,
    selectedStrategy,
  }
}

export default useStrategySelection
