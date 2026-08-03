import { useMemo, useState } from 'react'
import { Link } from 'react-router-dom'
import { RefreshCcw } from 'lucide-react'
import { useFleetBotsFeed } from '../../features/bots/page/useFleetBotsFeed.js'
import { buildCollectorCardViewModel } from '../../features/collectors/buildCollectorCardViewModel.js'
import { useCollectorsFeed } from '../../features/collectors/useCollectorsFeed.js'
import { useMarketStructureFeed } from '../../features/market-structure/useMarketStructureFeed.js'
import {
  buildMarketPostureRows,
  buildStreamSessionRows,
} from '../../features/market-structure/buildMarketPosture.js'
import { useRunInventory } from '../../features/operations/useRunInventory.js'
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
import { OperatorErrorNotice } from '../components/OperatorErrorNotice.jsx'

function formatTime(value) {
  if (!value) return 'Evidence time unavailable'
  const parsed = new Date(value)
  return Number.isNaN(parsed.getTime()) ? 'Evidence time unavailable' : parsed.toLocaleString()
}

function SummaryCard({ label, value, detail, tone = 'neutral', to }) {
  return (
    <Link className={`qt2-summary-card is-${tone}`} to={to}>
      <span>{label}</span>
      <strong>{value}</strong>
      <small>{detail}</small>
    </Link>
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
  const { sortedBots, nowEpochMs, error: botsError, refresh: refreshBots } = useFleetBotsFeed()
  const runInventory = useRunInventory(sortedBots)
  const collectorFeed = useCollectorsFeed()
  const marketFeed = useMarketStructureFeed()
  const [activityType, setActivityType] = useState('backtests_completed')
  const researchFeed = useOverviewBacktestActivity(activityType)

  const postureRows = useMemo(() => buildMarketPostureRows({
    definitions: marketFeed.definitions,
    sessions: marketFeed.sessions,
    statusByDefinition: marketFeed.statusByDefinition,
    normalizationSpecs: marketFeed.normalizationSpecs,
    collectors: collectorFeed.collectors,
    nowEpochMs,
  }), [marketFeed.definitions, marketFeed.sessions, marketFeed.statusByDefinition, marketFeed.normalizationSpecs, collectorFeed.collectors, nowEpochMs])
  const streamRows = useMemo(() => buildStreamSessionRows({
    definitions: marketFeed.definitions,
    sessions: marketFeed.sessions,
  }), [marketFeed.definitions, marketFeed.sessions])
  const attentionItems = useMemo(() => rankAttentionItems({
    runs: runInventory.runs,
    collectors: collectorFeed.collectors,
    postureRows,
    researchItems: researchFeed.researchItems,
    nowEpochMs,
  }), [runInventory.runs, collectorFeed.collectors, postureRows, researchFeed.researchItems, nowEpochMs])
  const currentOperations = useMemo(() => buildCurrentOperations({
    runs: runInventory.runs,
    collectors: collectorFeed.collectors,
    streamRows,
  }), [runInventory.runs, collectorFeed.collectors, streamRows])
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
  const errors = [botsError, runInventory.error, collectorFeed.error, marketFeed.error, ...researchFeed.errors].filter(Boolean)

  function refresh() {
    refreshBots()
    runInventory.refresh()
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

      {errors.map((error, index) => <OperatorErrorNotice error={error} key={String(error) + index} />)}

      <div className="qt2-summary-grid">
        <SummaryCard label="Attention" value={attentionItems.length || 'Clear'} detail={attentionItems.length ? `Within ${ATTENTION_CONTRACT.lookbackHours} hours` : 'No known actionable issues'} tone={attentionItems.length ? 'danger' : 'success'} to="/operations" />
        <SummaryCard label="Active runs" value={activeRuns} detail={activeRuns === 1 ? 'One live run projection' : 'Run instances evidenced active'} tone={activeRuns ? 'info' : 'neutral'} to="/operations?tab=runs" />
        <SummaryCard label="Collectors" value={collectorSummary.enabled ? `${collectorSummary.healthy}/${collectorSummary.enabled}` : 'None'} detail={collectorSummary.issues ? `${collectorSummary.issues} schedule${collectorSummary.issues === 1 ? '' : 's'} need attention` : 'On-schedule delivery evidence'} tone={collectorSummary.issues ? 'warning' : 'success'} to="/operations?tab=collectors" />
        <SummaryCard label="Market pairs" value={postureRows.length || 'None'} detail={marketIssues ? `${marketIssues} pair${marketIssues === 1 ? '' : 's'} need review` : 'No known quality issues'} tone={marketIssues ? 'warning' : 'success'} to="/operations?tab=market-data" />
      </div>

      <div className="qt2-dashboard-grid">
        <section className="qt2-dashboard-panel">
          <div className="qt2-dashboard-panel-head">
            <div><h2>Needs attention</h2><p>Only current actionable evidence.</p></div>
            <Link to="/operations">Open operations</Link>
          </div>
          <AttentionRail items={attentionItems.slice(0, 3)} lookbackHours={ATTENTION_CONTRACT.lookbackHours} />
          {attentionItems.length > 3 ? <p className="qt2-dashboard-more">+{attentionItems.length - 3} more in Operations</p> : null}
        </section>

        <section className="qt2-dashboard-panel">
          <div className="qt2-dashboard-panel-head">
            <div><h2>Now</h2><p>Active evidence, not configured intent.</p></div>
            <span>{currentOperations.length}</span>
          </div>
          <CurrentOperations operations={currentOperations} />
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
          <ActivityHeatmap days={researchFeed.activity?.days || []} activityLabel={filter?.label || 'Persisted activity'} />
        </section>
        <TopResultCard result={researchFeed.topResult} dataset={researchFeed.topResultDataset} />
      </div>
    </div>
  )
}
