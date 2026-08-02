import { useCallback, useEffect, useState } from 'react'
import {
  fetchReportActivity,
  getReport,
  listReports,
} from '../../../adapters/report.adapter.js'
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

function outcomeRows(reportItems, researchItems) {
  const reports = reportItems.map((report) => ({
    id: `run:${report.run_id}`,
    kind: 'backtest',
    title: report.strategy_name || report.bot_name || 'Backtest',
    status: report.status,
    occurredAt: report.completed_at,
    detail: [
      (report.symbols || []).join(', '),
      report.timeframe,
      report.summary?.net_pnl == null
        ? null
        : `Net P&L ${Number(report.summary.net_pnl).toFixed(2)}`,
    ].filter(Boolean).join(' · '),
    href: `/operations/runs/${report.run_id}`,
    state: { run: report, from: '/overview' },
  }))
  const checks = researchItems
    .filter((item) =>
      item?.kind === 'research_check'
      && ['tested', 'blocked'].includes(item?.status))
    .map((item) => ({
      id: `research:${item.id}`,
      kind: 'check',
      title: item.title || 'Research check',
      status: item.status,
      occurredAt: item.created_at,
      detail: [item.symbol, item.timeframe, item.payload?.result?.recommendation]
        .filter(Boolean)
        .join(' · '),
      href: `/operations/research/${item.id}`,
      state: { item, from: '/overview' },
    }))
  return [...reports, ...checks]
    .sort((left, right) =>
      (Date.parse(right.occurredAt || '') || 0)
      - (Date.parse(left.occurredAt || '') || 0))
    .slice(0, 8)
}

export function useOverviewBacktestActivity(
  activityType = 'backtests_completed',
) {
  const [topResult, setTopResult] = useState(null)
  const [topResultDataset, setTopResultDataset] = useState(null)
  const [activity, setActivity] = useState(null)
  const [outcomes, setOutcomes] = useState([])
  const [researchItems, setResearchItems] = useState([])
  const [loading, setLoading] = useState(true)
  const [errors, setErrors] = useState([])
  const [observedAt, setObservedAt] = useState(null)
  const [refreshRevision, setRefreshRevision] = useState(0)
  const refresh = useCallback(() => setRefreshRevision((value) => value + 1), [])

  useEffect(() => {
    let mounted = true
    async function load() {
      setLoading(true)
      const requests = await Promise.allSettled([
        listReports({
          type: 'backtest',
          status: 'completed',
          sort: 'net_pnl_desc',
          limit: 1,
        }),
        listReports({
          type: 'backtest',
          status: 'completed',
          limit: 8,
        }),
        listResearchItems({ limit: 40 }),
        activityRequest(activityType),
      ])
      if (!mounted) return
      const nextErrors = []
      const top = requests[0].status === 'fulfilled'
        ? requests[0].value?.items?.[0] || null
        : null
      if (requests[0].status === 'rejected') nextErrors.push('Top-result projection unavailable.')
      const recentReports = requests[1].status === 'fulfilled'
        ? requests[1].value?.items || []
        : []
      if (requests[1].status === 'rejected') nextErrors.push('Recent backtests unavailable.')
      const nextResearchItems = requests[2].status === 'fulfilled'
        ? requests[2].value
        : []
      if (requests[2].status === 'rejected') nextErrors.push('Research memory unavailable.')
      const nextActivity = requests[3].status === 'fulfilled'
        ? requests[3].value
        : null
      if (requests[3].status === 'rejected') nextErrors.push('Activity aggregation unavailable.')

      let dataset = null
      if (top?.run_id) {
        try {
          const report = await getReport(top.run_id)
          dataset = report?.identity || report?.context?.dataset_identity || null
        } catch {
          nextErrors.push('Top-result dataset identity unavailable.')
        }
      }
      if (!mounted) return
      setTopResult(top)
      setTopResultDataset(dataset)
      setResearchItems(nextResearchItems)
      setOutcomes(outcomeRows(recentReports, nextResearchItems))
      setActivity(nextActivity)
      setErrors(nextErrors)
      setObservedAt(new Date().toISOString())
      setLoading(false)
    }
    load()
    return () => {
      mounted = false
    }
  }, [activityType, refreshRevision])

  return {
    topResult,
    topResultDataset,
    activity,
    outcomes,
    researchItems,
    loading,
    errors,
    observedAt,
    refresh,
  }
}
