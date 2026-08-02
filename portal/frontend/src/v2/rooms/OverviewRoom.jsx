import { useMemo, useState } from 'react'
import { Link } from 'react-router-dom'
import { RefreshCcw } from 'lucide-react'
import { useFleetBotsFeed } from '../../features/bots/page/useFleetBotsFeed.js'
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
import { GreetingBanner } from '../../features/overview/components/GreetingBanner.jsx'
import { TopResultCard } from '../../features/overview/components/TopResultCard.jsx'
import { ActivityHeatmap } from '../../features/overview/components/ActivityHeatmap.jsx'

function formatTime(value) {
  if (!value) return 'Unavailable'
  const parsed = new Date(value)
  return Number.isNaN(parsed.getTime()) ? 'Unavailable' : parsed.toLocaleString()
}

function EvidenceState({ state }) {
  return <span className={'qt2-evidence-state is-' + (state?.tone || 'neutral')}>{state?.label || 'Unavailable'}</span>
}

function CurrentOperations({ operations }) {
  if (!operations.length) return <div className="qt2-empty">No active run, collector-attempt, or leased stream-session evidence.</div>
  return (
    <div className="qt2-compact-list">
      {operations.slice(0, 8).map((operation) => (
        <Link key={operation.id} to={operation.href} state={operation.state} className="qt2-operation-row">
          <span className="qt2-kind-label">{operation.kind}</span>
          <span><strong>{operation.title}</strong><small>{operation.detail || 'Context unavailable'}</small></span>
          <span className="qt2-state-text">{operation.status}</span>
          <time>{formatTime(operation.evidenceAt)}</time>
        </Link>
      ))}
    </div>
  )
}

function MarketPosture({ rows, loading }) {
  if (loading && !rows.length) return <div className="qt2-empty">Loading market-data evidence…</div>
  if (!rows.length) return <div className="qt2-empty">No configured market-structure pair definitions.</div>
  return (
    <div className="qt2-table-wrap">
      <table className="qt2-data-table">
        <thead>
          <tr>
            <th>Pair / products</th>
            <th>Collection</th>
            <th>Coverage / book</th>
            <th>Archive</th>
            <th>Normalization</th>
            <th>Admission</th>
            <th>Latest evidence</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((row) => (
            <tr key={row.id}>
              <td><strong>{row.label}</strong><small>{row.products.join(', ') || 'Products unavailable'}</small></td>
              <td><EvidenceState state={row.collection} /><small>{row.collection.detail}</small></td>
              <td><EvidenceState state={row.coverage} /><small>{row.book.label} · {row.qualityCount} quality records</small></td>
              <td><EvidenceState state={row.archive} /></td>
              <td><EvidenceState state={row.normalization} /></td>
              <td><EvidenceState state={row.admission} /></td>
              <td>{formatTime(row.latestEvidenceAt)}<small>{row.stream.label}</small></td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}

function RecentOutcomes({ outcomes }) {
  if (!outcomes.length) return <div className="qt2-empty">No completed checks or backtests are available.</div>
  return (
    <div className="qt2-compact-list">
      {outcomes.map((outcome) => (
        <Link key={outcome.id} to={outcome.href} state={outcome.state} className="qt2-operation-row">
          <span className="qt2-kind-label">{outcome.kind}</span>
          <span><strong>{outcome.title}</strong><small>{outcome.detail || 'Context unavailable'}</small></span>
          <span className="qt2-state-text">{outcome.status || 'unknown'}</span>
          <time>{formatTime(outcome.occurredAt)}</time>
        </Link>
      ))}
    </div>
  )
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
    <div className="qt2-room">
      <div className="qt2-room-head qt2-overview-head">
        <div>
          <span className="qt2-kicker">Operator console</span>
          <h1 className="qt2-title">Overview</h1>
          <p className="qt2-sub">Attention, active evidence, data usability, and recent research outcomes.</p>
        </div>
        <div className="qt2-head-actions">
          <GreetingBanner nowEpochMs={nowEpochMs} />
          <button type="button" className="qt2-button" onClick={refresh}><RefreshCcw size={14} />Refresh evidence</button>
        </div>
      </div>

      {errors.map((error, index) => <div className="qt2-error" key={error + index}>{error}</div>)}

      <section className="qt2-section">
        <div className="qt2-section-hd">
          <div><span className="qt2-step">01</span><span className="qt2-kicker">Needs attention</span></div>
          <span className="qt2-count">{attentionItems.length}</span>
        </div>
        <p className="qt2-section-note">Current {ATTENTION_CONTRACT.lookbackHours}-hour window · critical then warning, newest evidence first · deduplicated by evidence identity.</p>
        <AttentionRail items={attentionItems} lookbackHours={ATTENTION_CONTRACT.lookbackHours} />
      </section>

      <section className="qt2-section">
        <div className="qt2-section-hd">
          <div><span className="qt2-step">02</span><span className="qt2-kicker">Current operations</span></div>
          <span className="qt2-count">{currentOperations.length}</span>
        </div>
        <p className="qt2-section-note">Only active run instances, in-flight collector attempts, and currently leased stream sessions. Enabled schedules are not called running.</p>
        <CurrentOperations operations={currentOperations} />
      </section>

      <section className="qt2-section">
        <div className="qt2-section-hd">
          <div><span className="qt2-step">03</span><span className="qt2-kicker">Market-data posture</span></div>
          <Link to="/operations?tab=data-plane" className="qt2-text-link">Inspect data plane</Link>
        </div>
        <p className="qt2-section-note">Independent evidence states; no roll-up health claim. Collector process liveness remains unknown without a heartbeat.</p>
        <MarketPosture rows={postureRows} loading={marketFeed.loading} />
      </section>

      <section className="qt2-section">
        <div className="qt2-section-hd">
          <div><span className="qt2-step">04</span><span className="qt2-kicker">Research activity</span></div>
          <select className="qt2-select" value={activityType} onChange={(event) => setActivityType(event.target.value)}>
            {ACTIVITY_FILTERS.map((item) => <option key={item.value} value={item.value}>{item.label}</option>)}
          </select>
        </div>
        <p className="qt2-section-note">{researchFeed.activity?.description || 'Persisted activity by UTC day.'} Zero-count days are explicit. Default range: 182 days.</p>
        <div className="qt2-stat-card qt2-heatmap-card">
          <ActivityHeatmap days={researchFeed.activity?.days || []} activityLabel={filter?.label || 'Persisted activity'} />
        </div>
      </section>

      <section className="qt2-section">
        <div className="qt2-section-hd">
          <div><span className="qt2-step">05</span><span className="qt2-kicker">Recent outcomes</span></div>
          <Link to="/operations?tab=research" className="qt2-text-link">Browse evidence</Link>
        </div>
        <div className="qt2-outcomes-grid">
          <TopResultCard result={researchFeed.topResult} dataset={researchFeed.topResultDataset} />
          <div className="qt2-stat-card">
            <div className="qt2-card-heading-row"><span className="qt2-kicker">Latest completed evidence</span><span className="qt2-muted">Checks + backtests</span></div>
            <RecentOutcomes outcomes={researchFeed.outcomes} />
          </div>
        </div>
      </section>
    </div>
  )
}
