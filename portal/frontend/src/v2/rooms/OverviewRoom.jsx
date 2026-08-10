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
import { TopResultCard } from '../../features/overview/components/TopResultCard.jsx'
import { ActivityHeatmap } from '../../features/overview/components/ActivityHeatmap.jsx'
import { OperatorErrorNotice, OperatorSkeleton } from '../components/OperatorErrorNotice.jsx'

function formatTime(value) {
  if (!value) return 'Evidence time unavailable'
  const parsed = new Date(value)
  return Number.isNaN(parsed.getTime()) ? 'Evidence time unavailable' : parsed.toLocaleString()
}

function SummaryCard({ label, value, detail, tone = "neutral", to, loading = false, error = null, partial = false }) {
  return (
    <Link className={"qt2-summary-card is-" + (error || partial ? "warning" : tone)} to={to} aria-busy={loading}>
      <span>{label}</span>
      {loading ? <div className="qt2-summary-skeleton qt2-skeleton" /> : <strong>{error ? "Unavailable" : value}</strong>}
      <small>{error ? "Open the owning view for details" : partial ? `${detail} · Partial evidence` : detail}</small>
    </Link>
  )
}

function ComponentAvailability({ issues }) {
  if (!issues.length) return null
  return (
    <details className="qt2-component-availability">
      <summary>{issues.length} evidence source{issues.length === 1 ? "" : "s"} unavailable</summary>
      <div>
        {issues.map(({ component, error }, index) => (
          <section data-component={component} key={`${component}:${index}`}>
            <span>{component}</span>
            <OperatorErrorNotice error={error} compact />
          </section>
        ))}
      </div>
    </details>
  )
}

function CurrentOperations({ operations }) {
  if (!operations.length) {
    return <div className="qt2-dashboard-empty">No active run, in-flight collector attempt, or leased stream session is currently evidenced.</div>
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
  const nowEpochMs = Date.now()
  const collectorFeed = useCollectorsFeed()
  const [activityType, setActivityType] = useState('backtests_completed')
  const researchFeed = useOverviewBacktestActivity(activityType)
  const projectedRuns = activeRunsFeed.runs

  const attentionItems = useMemo(() => rankAttentionItems({
    runs: projectedRuns,
    collectors: collectorFeed.collectors,
    researchItems: researchFeed.researchItems,
    nowEpochMs,
  }), [projectedRuns, collectorFeed.collectors, researchFeed.researchItems, nowEpochMs])
  const currentOperations = useMemo(() => buildCurrentOperations({
    runs: projectedRuns,
    collectors: collectorFeed.collectors,
  }), [projectedRuns, collectorFeed.collectors])
  const collectorSummary = useMemo(() => {
    const enabled = collectorFeed.collectors.filter((collector) => collector.configured_state === 'enabled')
    const healthy = enabled.filter((collector) => collector.actual_state === 'HEALTHY').length
    const issues = enabled.filter((collector) => ['DEGRADED', 'FAILED', 'RETRYING'].includes(collector.actual_state)).length
    return { enabled: enabled.length, healthy, issues }
  }, [collectorFeed.collectors])
  const activeRuns = currentOperations.filter((item) => ['run', 'backtest'].includes(item.kind)).length
  const staleStreams = Number(collectorFeed.dataPlane?.stale_stream_count || 0)
  const filter = ACTIVITY_FILTERS.find((item) => item.value === activityType)
  const currentOperationIssues = [
    { component: "Active runs", error: activeRunsFeed.error },
    { component: "Collector schedules", error: collectorFeed.error },
    { component: "Collector live updates", error: collectorFeed.streamError },
  ].filter((issue) => issue.error)
  const attentionIssues = [
    ...currentOperationIssues.filter((issue) => !issue.component.endsWith("live updates")),
    ...researchFeed.errors.filter((issue) => issue.component === "Research attention"),
  ]
  const researchActivityIssues = researchFeed.errors.filter((issue) => issue.component === "Research activity")
  const topResultIssues = researchFeed.errors.filter((issue) => issue.component === "Top result")
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

      <div className="qt2-summary-grid">
        <SummaryCard label="Attention" value={attentionItems.length || 'Clear'} detail={attentionItems.length ? "Within " + ATTENTION_CONTRACT.lookbackHours + " hours" : 'No known actionable issues'} tone={attentionItems.length ? 'danger' : 'success'} to="/operations" loading={operationsLoading && !attentionItems.length} partial={attentionIssues.length > 0} />
        <SummaryCard label="Active runs" value={activeRuns} detail={activeRuns === 1 ? 'One live run instance' : 'Run instances currently owned'} tone={activeRuns ? 'info' : 'neutral'} to="/operations?tab=runs" loading={activeRunsFeed.loading && !projectedRuns.length} error={activeRunsFeed.error && !projectedRuns.length ? activeRunsFeed.error : null} partial={Boolean(activeRunsFeed.error)} />
        <SummaryCard label="Collectors" value={collectorSummary.enabled ? `${collectorSummary.healthy}/${collectorSummary.enabled}` : 'None'} detail={collectorSummary.issues ? `${collectorSummary.issues} collector${collectorSummary.issues === 1 ? '' : 's'} need attention` : 'Canonical lifecycle evidence'} tone={collectorSummary.issues ? 'warning' : 'success'} to="/operations?tab=market" loading={collectorFeed.loading && !collectorFeed.collectors.length} error={collectorFeed.error && !collectorFeed.collectors.length ? collectorFeed.error : null} partial={Boolean(collectorFeed.error || collectorFeed.streamError)} />
        <SummaryCard label="Market data plane" value={collectorFeed.dataPlane?.active_schema_count ?? 'Unavailable'} detail={staleStreams ? `${staleStreams} stale stream${staleStreams === 1 ? '' : 's'}` : `${collectorFeed.dataPlane?.ingestion_rate_per_minute ?? 0} accepted facts/min`} tone={staleStreams ? 'warning' : 'success'} to="/operations?tab=market" loading={collectorFeed.loading && !collectorFeed.dataPlane} error={collectorFeed.error && !collectorFeed.dataPlane ? collectorFeed.error : null} partial={Boolean(collectorFeed.error)} />
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
            <div><h2>Now</h2><p>Active evidence, not configured intent.</p></div>
            <span>{currentOperations.length}</span>
          </div>
          <ComponentAvailability issues={currentOperationIssues} />
          {operationsLoading && !currentOperations.length ? <OperatorSkeleton rows={4} label="Loading current operations" /> : <CurrentOperations operations={currentOperations} />}
        </section>
      </div>

      <div className="qt2-dashboard-grid qt2-dashboard-grid-secondary">
        <section className="qt2-dashboard-panel">
          <div className="qt2-dashboard-panel-head">
            <div><h2>Research activity</h2><p>{researchFeed.activity?.description || 'Persisted activity by UTC day.'}</p></div>
            <select className="qt2-select" value={activityType} onChange={(event) => setActivityType(event.target.value)}>
              {ACTIVITY_FILTERS.map((item) => <option key={item.value} value={item.value}>{item.label}</option>)}
            </select>
          </div>
          <ComponentAvailability issues={researchActivityIssues} />
          {researchFeed.loading && !researchFeed.activity ? <OperatorSkeleton rows={4} label="Loading research activity" /> : <ActivityHeatmap days={researchFeed.activity?.days || []} activityLabel={filter?.label || 'Persisted activity'} />}
        </section>
        <div className="qt2-component-boundary">
          <ComponentAvailability issues={topResultIssues} />
          {researchFeed.loading && !researchFeed.topResult ? <OperatorSkeleton rows={4} label="Loading top result" /> : <TopResultCard result={researchFeed.topResult} dataset={researchFeed.topResultDataset} />}
        </div>
      </div>
    </div>
  )
}
