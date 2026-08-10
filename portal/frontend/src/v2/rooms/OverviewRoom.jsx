import { useMemo, useState } from 'react'
import { Link } from 'react-router-dom'
import { RefreshCcw } from 'lucide-react'
import { useCollectorsFeed } from '../../features/collectors/useCollectorsFeed.js'
import { useActiveRunsFeed } from '../../features/operations/useActiveRunsFeed.js'
import {
  ACTIVITY_FILTERS,
  useOverviewBacktestActivity,
} from '../../features/overview/hooks/useOverviewBacktestActivity.js'
import {
  ATTENTION_CONTRACT,
  buildCurrentOperations,
  rankAttentionItems,
} from '../../features/overview/buildOverviewViewModel.js'
import { AttentionRail } from '../../features/overview/components/AttentionRail.jsx'
import { ActivityHeatmap } from '../../features/overview/components/ActivityHeatmap.jsx'
import { OperatorErrorNotice, OperatorSkeleton } from '../components/OperatorErrorNotice.jsx'

function formatTime(value) {
  if (!value) return 'Evidence time unavailable'
  const parsed = new Date(value)
  return Number.isNaN(parsed.getTime()) ? 'Evidence time unavailable' : parsed.toLocaleString()
}

function SummaryCard({ label, value, detail, tone = 'neutral', to, loading = false, error = null, partial = false }) {
  return (
    <Link className={'qt2-summary-card is-' + (error || partial ? 'warning' : tone)} to={to} aria-busy={loading}>
      <span>{label}</span>
      {loading ? <div className="qt2-summary-skeleton qt2-skeleton" /> : <strong>{error ? 'Unavailable' : value}</strong>}
      <small>{error ? 'Open the owning view for details' : partial ? `${detail} · Partial evidence` : detail}</small>
    </Link>
  )
}

function ComponentAvailability({ issues }) {
  if (!issues.length) return null
  return (
    <details className="qt2-component-availability">
      <summary>{issues.length} evidence source{issues.length === 1 ? '' : 's'} unavailable</summary>
      <div>{issues.map(({ component, error }, index) => (
        <section data-component={component} key={`${component}:${index}`}>
          <span>{component}</span>
          <OperatorErrorNotice error={error} compact />
        </section>
      ))}</div>
    </details>
  )
}

function CurrentOperations({ operations }) {
  if (!operations.length) {
    return <div className="qt2-dashboard-empty">No active run is currently evidenced.</div>
  }
  return (
    <div className="qt2-dashboard-list">
      {operations.slice(0, 4).map((operation) => (
        <Link key={operation.id} to={operation.href} state={operation.state}>
          <span className="qt2-status-dot" />
          <span><strong>{operation.title}</strong><small>{operation.kind} · {operation.status}</small></span>
          <time>{formatTime(operation.evidenceAt)}</time>
        </Link>
      ))}
    </div>
  )
}

export function OverviewRoom() {
  const activeRunsFeed = useActiveRunsFeed()
  const collectorFeed = useCollectorsFeed()
  const [activityType, setActivityType] = useState('backtests_completed')
  const researchFeed = useOverviewBacktestActivity(activityType)
  const projectedRuns = activeRunsFeed.runs
  const nowEpochMs = Date.now()

  const attentionItems = useMemo(() => rankAttentionItems({
    runs: projectedRuns,
    providerSummaries: collectorFeed.providers,
    researchItems: researchFeed.researchItems,
    nowEpochMs,
  }), [projectedRuns, collectorFeed.providers, researchFeed.researchItems, nowEpochMs])
  const currentOperations = useMemo(
    () => buildCurrentOperations({ runs: projectedRuns }),
    [projectedRuns],
  )
  const activeRuns = currentOperations.filter((item) => ['run', 'backtest'].includes(item.kind)).length
  const fleet = collectorFeed.fleet || {}
  const states = fleet.operational_state_counts || {}
  const runningCollectors = Number(states.RUNNING || 0)
  const marketDetail = fleet.attention_count
    ? `${fleet.attention_count} actionable exception${fleet.attention_count === 1 ? '' : 's'}`
    : `${fleet.accepted_last_minute || 0}/min · ${fleet.active_schema_count || 0} schemas`
  const filter = ACTIVITY_FILTERS.find((item) => item.value === activityType)

  const currentOperationIssues = [
    { component: 'Active runs', error: activeRunsFeed.error },
  ].filter((issue) => issue.error)
  const attentionIssues = [
    ...currentOperationIssues,
    { component: 'Collector summaries', error: collectorFeed.error },
    ...researchFeed.errors.filter((issue) => issue.component === 'Research attention'),
  ].filter((issue) => issue.error)
  const researchActivityIssues = researchFeed.errors.filter((issue) => issue.component === 'Research activity')
  const operationsLoading = activeRunsFeed.loading || collectorFeed.loading

  function refresh() {
    activeRunsFeed.refresh()
    collectorFeed.refresh()
    researchFeed.refresh()
  }

  return (
    <div className="qt2-room qt2-dashboard">
      <div className="qt2-room-head qt2-overview-head">
        <div>
          <h1 className="qt2-title">Overview</h1>
          <p className="qt2-sub">What needs you now, with deeper evidence one click away.</p>
        </div>
        <button type="button" className="qt2-icon-button" onClick={refresh}><RefreshCcw size={14} />Refresh</button>
      </div>

      <div className="qt2-summary-grid qt2-summary-grid-three">
        <SummaryCard label="Attention" value={attentionItems.length || 'Clear'} detail={attentionItems.length ? `Within ${ATTENTION_CONTRACT.lookbackHours} hours` : 'No known actionable issues'} tone={attentionItems.length ? 'danger' : 'success'} to="/operations" loading={operationsLoading && !attentionItems.length} partial={attentionIssues.length > 0} />
        <SummaryCard label="Active runs" value={activeRuns} detail={activeRuns === 1 ? 'One live run instance' : 'Run instances currently owned'} tone={activeRuns ? 'info' : 'neutral'} to="/operations?tab=runs" loading={activeRunsFeed.loading && !projectedRuns.length} error={activeRunsFeed.error && !projectedRuns.length ? activeRunsFeed.error : null} partial={Boolean(activeRunsFeed.error)} />
        <SummaryCard label="Market data" value={runningCollectors ? `${runningCollectors} running` : 'Idle'} detail={marketDetail} tone={fleet.attention_count ? 'warning' : 'success'} to="/operations?tab=market" loading={collectorFeed.loading && !collectorFeed.providers.length} error={collectorFeed.error && !collectorFeed.providers.length ? collectorFeed.error : null} partial={Boolean(collectorFeed.error || collectorFeed.streamError)} />
      </div>

      <div className="qt2-dashboard-grid">
        <section className="qt2-dashboard-panel">
          <div className="qt2-dashboard-panel-head">
            <div><h2>Needs attention</h2><p>Only current actionable evidence.</p></div>
            <Link to="/operations">Open operations</Link>
          </div>
          <ComponentAvailability issues={attentionIssues} />
          {operationsLoading && !attentionItems.length ? <OperatorSkeleton rows={3} label="Loading attention evidence" /> : <AttentionRail items={attentionItems.slice(0, 3)} lookbackHours={ATTENTION_CONTRACT.lookbackHours} />}
          {attentionItems.length > 3 ? <p className="qt2-dashboard-more">+{attentionItems.length - 3} more in Operations</p> : null}
        </section>

        <section className="qt2-dashboard-panel">
          <div className="qt2-dashboard-panel-head">
            <div><h2>Now</h2><p>Active run evidence, not routine collector polls.</p></div>
            <span>{currentOperations.length}</span>
          </div>
          <ComponentAvailability issues={currentOperationIssues} />
          {activeRunsFeed.loading && !currentOperations.length ? <OperatorSkeleton rows={4} label="Loading current operations" /> : <CurrentOperations operations={currentOperations} />}
        </section>
      </div>

      <section className="qt2-dashboard-panel qt2-research-activity-panel">
        <div className="qt2-dashboard-panel-head">
          <div><h2>Research activity</h2><p>{researchFeed.activity?.description || 'Persisted activity by UTC day.'}</p></div>
          <select className="qt2-select" value={activityType} onChange={(event) => setActivityType(event.target.value)}>
            {ACTIVITY_FILTERS.map((item) => <option key={item.value} value={item.value}>{item.label}</option>)}
          </select>
        </div>
        <ComponentAvailability issues={researchActivityIssues} />
        {researchFeed.activityLoading && !researchFeed.activity
          ? <div className="qt2-deferred-shimmer" aria-label="Loading research activity"><span /><span /><span /></div>
          : <ActivityHeatmap days={researchFeed.activity?.days || []} activityLabel={filter?.label || 'Persisted activity'} />}
      </section>
    </div>
  )
}
