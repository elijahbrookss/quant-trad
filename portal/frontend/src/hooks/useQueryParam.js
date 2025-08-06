// src/hooks/useQueryParam.js
import { useState, useEffect, useCallback } from 'react'

/**
 * Hook for a single query-string param:
 *  • reads initialValue from ?key=…
 *  • when setter is called, does history.replaceState to update the URL
 */
export function useQueryParam(key, defaultValue) {
  // helper to read current value
  const readValue = () => {
    try {
      const params = new URLSearchParams(window.location.search)
      return params.get(key) ?? defaultValue
    } catch {
      return defaultValue
    }
  }

  const [value, setValue] = useState(readValue)

  // when the URL changes (popstate), re-read
  useEffect(() => {
    const onPop = () => setValue(readValue())
    window.addEventListener('popstate', onPop)
    return () => window.removeEventListener('popstate', onPop)
  }, [])

  // setter that also pushes into the URL
  const update = useCallback(newVal => {
    const params = new URLSearchParams(window.location.search)
    if (newVal == null || newVal === '') params.delete(key)
    else params.set(key, newVal)
    const newQuery = params.toString()
    window.history.replaceState(
      {}, 
      '', 
      `${window.location.pathname}${newQuery ? '?' + newQuery : ''}`
    )
    setValue(newVal)
  }, [key])

  return [value, update]
}
