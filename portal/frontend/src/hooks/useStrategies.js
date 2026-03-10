/**
 * Custom hook for managing strategy state and CRUD operations.
 */

import { useState, useCallback } from 'react'
import { strategyService } from '../services'

export function useStrategies() {
  const [strategies, setStrategies] = useState([])
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState(null)

  /**
   * Load all strategies.
   */
  const loadStrategies = useCallback(async () => {
    setLoading(true)
    setError(null)
    try {
      const data = await strategyService.getAll()
      setStrategies(data)
      return data
    } catch (err) {
      setError(err.message)
      throw err
    } finally {
      setLoading(false)
    }
  }, [])

  /**
   * Create a new strategy.
   */
  const createStrategy = useCallback(async (strategyData) => {
    setLoading(true)
    setError(null)
    try {
      const newStrategy = await strategyService.create(strategyData)
      setStrategies(prev => [...prev, newStrategy])
      return newStrategy
    } catch (err) {
      setError(err.message)
      throw err
    } finally {
      setLoading(false)
    }
  }, [])

  /**
   * Update an existing strategy.
   */
  const updateStrategy = useCallback(async (id, updates) => {
    setLoading(true)
    setError(null)
    try {
      const updated = await strategyService.update(id, updates)
      setStrategies(prev =>
        prev.map(s => s.id === id ? updated : s)
      )
      return updated
    } catch (err) {
      setError(err.message)
      throw err
    } finally {
      setLoading(false)
    }
  }, [])

  /**
   * Delete a strategy.
   */
  const deleteStrategy = useCallback(async (id) => {
    setLoading(true)
    setError(null)
    try {
      await strategyService.delete(id)
      setStrategies(prev => prev.filter(s => s.id !== id))
    } catch (err) {
      setError(err.message)
      throw err
    } finally {
      setLoading(false)
    }
  }, [])

  /**
   * Attach an indicator to a strategy.
   */
  const attachIndicator = useCallback(async (strategyId, indicatorId) => {
    setLoading(true)
    setError(null)
    try {
      const updated = await strategyService.attachIndicator(strategyId, indicatorId)
      setStrategies(prev =>
        prev.map(s => s.id === strategyId ? updated : s)
      )
      return updated
    } catch (err) {
      setError(err.message)
      throw err
    } finally {
      setLoading(false)
    }
  }, [])

  /**
   * Detach an indicator from a strategy.
   */
  const detachIndicator = useCallback(async (strategyId, indicatorId) => {
    setLoading(true)
    setError(null)
    try {
      const updated = await strategyService.detachIndicator(strategyId, indicatorId)
      setStrategies(prev =>
        prev.map(s => s.id === strategyId ? updated : s)
      )
      return updated
    } catch (err) {
      setError(err.message)
      throw err
    } finally {
      setLoading(false)
    }
  }, [])

  /**
   * Create a rule for a strategy.
   */
  const createRule = useCallback(async (strategyId, ruleData) => {
    setLoading(true)
    setError(null)
    try {
      const updated = await strategyService.createRule(strategyId, ruleData)
      setStrategies(prev =>
        prev.map(s => s.id === strategyId ? updated : s)
      )
      return updated
    } catch (err) {
      setError(err.message)
      throw err
    } finally {
      setLoading(false)
    }
  }, [])

  /**
   * Update a rule.
   */
  const updateRule = useCallback(async (strategyId, ruleId, updates) => {
    setLoading(true)
    setError(null)
    try {
      const updated = await strategyService.updateRule(strategyId, ruleId, updates)
      setStrategies(prev =>
        prev.map(s => s.id === strategyId ? updated : s)
      )
      return updated
    } catch (err) {
      setError(err.message)
      throw err
    } finally {
      setLoading(false)
    }
  }, [])

  /**
   * Delete a rule.
   */
  const deleteRule = useCallback(async (strategyId, ruleId) => {
    setLoading(true)
    setError(null)
    try {
      const updated = await strategyService.deleteRule(strategyId, ruleId)
      setStrategies(prev =>
        prev.map(s => s.id === strategyId ? updated : s)
      )
      return updated
    } catch (err) {
      setError(err.message)
      throw err
    } finally {
      setLoading(false)
    }
  }, [])

  return {
    strategies,
    loading,
    error,
    loadStrategies,
    createStrategy,
    updateStrategy,
    deleteStrategy,
    attachIndicator,
    detachIndicator,
    createRule,
    updateRule,
    deleteRule,
  }
}
