import { useMemo, useState } from 'react'
import { Link } from 'react-router-dom'
import { RefreshCcw } from 'lucide-react'
import { useFleetBotsFeed } from '../../features/bots/page/useFleetBotsFeed.js'
import { buildCollectorCardViewModel } from '../../features/collectors/buildCollectorCardViewModel.js'
import { useCollectorsFeed } from '../../features/collectors/useCollectorsFeed.js'
import { formatMarketStructureComponentError, useMarketStructureFeed } from '../../features/market-structure/useMarketStructureFeed.js'
import {
  buildMarketPostureRows,
  buildStreamSessionRows,
} from '../../features/market-structure/buildMarketPosture.js'
import { buildProjectedRunsFromBots } from '../../features/operations/buildOperationsViewModel.js'
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

function marketIssueCount(rows) {
  return rows.filter((row) => [
    row.collection,
    row.coverage,
    row.book,
    row.archive,
    row.normalization,
    row.admission,
  ].some((state) => ['danger', 'warning', 'rose', 'amber'].includes(state?.tone)) || Number(row.unavailableStatusCount || 0) > 0).length
}

export function OverviewRoom() {
  const { sortedBots, nowEpochMs, loading: botsLoading, error: botsError, refresh: refreshBots } = useFleetBotsFeed()
  const collectorFeed = useCollectorsFeed()
  const marketFeed = useMarketStructureFeed()
  const [activityType, setActivityType] = useState('backtests_completed')
  const researchFeed = useOverviewBacktestActivity(activityType)
  const projectedRuns = useMemo(
    () => buildProjectedRunsFromBots(sortedBots, { nowEpochMs }),
    [sortedBots, nowEpochMs],
  )

  const postureRows = useMemo(() => buildMarketPostureRows({
    definitions: marketFeed.definitions,
    sessions: marketFeed.sessions,
    statusByDefinition: marketFeed.statusByDefinition,
    normalizationSpecs: marketFeed.normalizationSpecs,
    normalizationAvailable: !marketFeed.componentErrors.normalization_specs,
    collectors: collectorFeed.collectors,
    nowEpochMs,
  }), [marketFeed.definitions, marketFeed.sessions, marketFeed.statusByDefinition, marketFeed.normalizationSpecs, marketFeed.componentErrors.normalization_specs, collectorFeed.collectors, nowEpochMs])
  const streamRows = useMemo(() => buildStreamSessionRows({
    definitions: marketFeed.definitions,
    sessions: marketFeed.sessions,
  }), [marketFeed.definitions, marketFeed.sessions])
  const attentionItems = useMemo(() => rankAttentionItems({
    runs: projectedRuns,
    collectors: collectorFeed.collectors,
    postureRows,
    researchItems: researchFeed.researchItems,
    nowEpochMs,
  }), [projectedRuns, collectorFeed.collectors, postureRows, researchFeed.researchItems, nowEpochMs])
  const currentOperations = useMemo(() => buildCurrentOperations({
    runs: projectedRuns,
    collectors: collectorFeed.collectors,
    streamRows,
  }), [projectedRuns, collectorFeed.collectors, streamRows])
  const collectorSummary = useMemo(() => {
    const rows = collectorFeed.collectors
      .map(({ definition, attempts }) => ({
        definition,
        vm: buildCollectorCardViewModel(definition, attempts, { nowEpochMs }),
      }))
      .filter(({ definition }) => definition?.enabled)
    const healthy = rows.filter(({ vm }) => vm.health.status === 'healthy').length
    return { enabled: rows.length, healthy, issues: rows.length - healthy }
  }, [collectorFeed.collectors, nowEpochMs])
  const activeRuns = currentOperations.filter((item) => ['run', 'backtest'].includes(item.kind)).length
  const marketIssues = marketIssueCount(postureRows)
  const filter = ACTIVITY_FILTERS.find((item) => item.value === activityType)
  const currentOperationIssues = [
    { component: "Run definitions", error: botsError },
    { component: "Collector schedules", error: collectorFeed.error },
    { component: "Collector live updates", error: collectorFeed.streamError },
    { component: "Market structure", error: marketFeed.error },
    { component: "Market live updates", error: marketFeed.streamError },
  ].filter((issue) => issue.error)
  const attentionIssues = [
    ...currentOperationIssues.filter((issue) => !issue.component.endsWith("live updates")),
    ...researchFeed.errors.filter((issue) => issue.component === "Research attention"),
  ]
  const researchActivityIssues = researchFeed.errors.filter((issue) => issue.component === "Research activity")
  const topResultIssues = researchFeed.errors.filter((issue) => issue.component === "Top result")
  const operationsLoading = botsLoading || collectorFeed.loading || marketFeed.loading

  function refresh() {
    refreshBots()
    collectorFeed.refresh()
    marketFeed.refresh()
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
        <SummaryCard label="Active runs" value={activeRuns} detail={activeRuns === 1 ? 'One live run projection' : 'Run instances evidenced active'} tone={activeRuns ? 'info' : 'neutral'} to="/operations?tab=runs" loading={botsLoading && !projectedRuns.length} error={botsError && !projectedRuns.length ? botsError : null} partial={Boolean(botsError)} />
        <SummaryCard label="Collectors" value={collectorSummary.enabled ? `${collectorSummary.healthy}/${collectorSummary.enabled}` : 'None'} detail={collectorSummary.issues ? `${collectorSummary.issues} schedule${collectorSummary.issues === 1 ? '' : 's'} need attention` : 'On-schedule delivery evidence'} tone={collectorSummary.issues ? 'warning' : 'success'} to="/operations?tab=market" loading={collectorFeed.loading && !collectorFeed.collectors.length} error={collectorFeed.error && !collectorFeed.collectors.length ? collectorFeed.error : null} partial={Boolean(collectorFeed.error || collectorFeed.streamError)} />
        <SummaryCard label="Market pairs" value={postureRows.length || 'None'} detail={marketIssues ? `${marketIssues} pair${marketIssues === 1 ? '' : 's'} need review` : 'No known quality issues'} tone={marketIssues ? 'warning' : 'success'} to="/operations?tab=market" loading={marketFeed.loading && !postureRows.length} error={!postureRows.length ? marketFeed.error || formatMarketStructureComponentError(marketFeed.componentErrors.definitions) : null} partial={Boolean(marketFeed.error || marketFeed.streamError || Object.keys(marketFeed.componentErrors).length)} />
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
