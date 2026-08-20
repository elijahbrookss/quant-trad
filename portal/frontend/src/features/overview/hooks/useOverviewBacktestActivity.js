import { useCallback, useEffect, useState } from 'react'
import { fetchReportActivity } from '../../../adapters/report.adapter.js'
import {
  fetchResearchActivity,
  listResearchItems,
} from '../../../adapters/research.adapter.js'

const HEATMAP_DAYS = 182

export const ACTIVITY_FILTERS = [
  { value: 'backtests_completed', label: 'Backtests completed' },
  { value: 'checks_completed', label: 'Checks completed' },
  { value: 'hypotheses_created', label: 'Hypotheses created' },
  { value: 'observations_recorded', label: 'Observations recorded' },
]

function activityRequest(activityType) {
  if (activityType === 'backtests_completed') {
    return fetchReportActivity({ type: 'backtest', days: HEATMAP_DAYS })
      .then((payload) => ({
        ...payload,
        activity_type: activityType,
        timestamp_field: 'ended_at',
        timezone: 'UTC',
        description: 'Completed backtests by persisted ended_at UTC day.',
      }))
  }
  return fetchResearchActivity({ type: activityType, days: HEATMAP_DAYS })
}

export function useOverviewBacktestActivity(
  activityType = 'backtests_completed',
) {
  const [activity, setActivity] = useState(null)
  const [researchItems, setResearchItems] = useState([])
  const [researchLoading, setResearchLoading] = useState(true)
  const [activityLoading, setActivityLoading] = useState(true)
  const [errors, setErrors] = useState([])
  const [observedAt, setObservedAt] = useState(null)
  const [refreshRevision, setRefreshRevision] = useState(0)
  const refresh = useCallback(() => setRefreshRevision((value) => value + 1), [])

  useEffect(() => {
    let mounted = true
    let idleHandle = null
    let timeoutHandle = null

    setResearchLoading(true)
    setActivityLoading(true)
    setErrors([])

    listResearchItems({ limit: 40 })
      .then((items) => {
        if (mounted) setResearchItems(items)
      })
      .catch((error) => {
        if (!mounted) return
        setErrors((current) => [...current, {
          component: 'Research attention',
          error: error?.message || 'Research memory unavailable.',
        }])
      })
      .finally(() => {
        if (mounted) setResearchLoading(false)
      })

    async function loadActivity() {
      try {
        const next = await activityRequest(activityType)
        if (mounted) setActivity(next)
      } catch (error) {
        if (mounted) {
          setErrors((current) => [...current, {
            component: 'Research activity',
            error: error?.message || 'Activity aggregation unavailable.',
          }])
        }
      } finally {
        if (mounted) {
          setActivityLoading(false)
          setObservedAt(new Date().toISOString())
        }
      }
    }

    if ('requestIdleCallback' in window) {
      idleHandle = window.requestIdleCallback(loadActivity, { timeout: 1_500 })
    } else {
      timeoutHandle = window.setTimeout(loadActivity, 300)
    }

    return () => {
      mounted = false
      if (idleHandle !== null) window.cancelIdleCallback(idleHandle)
      if (timeoutHandle !== null) window.clearTimeout(timeoutHandle)
    }
  }, [activityType, refreshRevision])

  return {
    activity,
    researchItems,
    researchLoading,
    activityLoading,
    loading: researchLoading || activityLoading,
    errors,
    observedAt,
    refresh,
  }
}
