import { useEffect, useMemo, useState } from 'react'
import { Link, useSearchParams } from 'react-router-dom'
import { RefreshCcw, Search } from 'lucide-react'
import { listResearchItems } from '../../adapters/research.adapter.js'
import { useFleetBotsFeed } from '../../features/bots/page/useFleetBotsFeed.js'
import { buildCollectorCardViewModel } from '../../features/collectors/buildCollectorCardViewModel.js'
import { useCollectorsFeed } from '../../features/collectors/useCollectorsFeed.js'
import {
  buildMarketPostureRows,
  buildStreamSessionRows,
} from '../../features/market-structure/buildMarketPosture.js'
import { useMarketStructureFeed } from '../../features/market-structure/useMarketStructureFeed.js'
import {
  buildRunRows,
  filterAndSortRunRows,
  filterResearchRows,
} from '../../features/operations/buildOperationsViewModel.js'
import { useRunInventory } from '../../features/operations/useRunInventory.js'

const TABS = [
  { id: 'runs', label: 'Runs' },
  { id: 'data-plane', label: 'Data plane' },
  { id: 'research', label: 'Research evidence' },
  { id: 'definitions', label: 'Definitions' },
]

function formatTime(value) {
  if (!value) return 'Unavailable'
  const date = new Date(value)
  return Number.isNaN(date.getTime()) ? 'Unavailable' : date.toLocaleString()
}

function formatDuration(value) {
  if (!Number.isFinite(value)) return '—'
  const seconds = Math.floor(value / 1000)
  if (seconds < 60) return seconds + 's'
  if (seconds < 3600) return Math.floor(seconds / 60) + 'm'
  return Math.floor(seconds / 3600) + 'h ' + Math.floor((seconds % 3600) / 60) + 'm'
}

function StatusBadge({ value, tone }) {
  const normalized = tone || (
    ['completed', 'succeeded', 'tested', 'promoted', 'healthy'].includes(value)
      ? 'success'
      : ['failed', 'crashed', 'blocked', 'overdue', 'invalid'].includes(value)
        ? 'danger'
        : ['running', 'starting', 'active', 'open_valid'].includes(value)
          ? 'info'
          : 'neutral'
  )
  return <span className={'qt2-evidence-state is-' + normalized}>{value || 'unknown'}</span>
}

function RunsTable({ rows, loading }) {
  if (loading && !rows.length) return <div className="qt2-empty">Loading persisted run inventory…</div>
  if (!rows.length) return <div className="qt2-empty">No runs match the current filters.</div>
  return (
    <div className="qt2-table-wrap">
      <table className="qt2-data-table">
        <thead><tr><th>Run instance</th><th>Type / mode</th><th>State</th><th>Instrument</th><th>Wall-clock / simulated</th><th>Evidence</th></tr></thead>
        <tbody>
          {rows.map((row) => (
            <tr key={row.id}>
              <td>
                <strong>{row.definitionName}</strong>
                <small className="qt-mono">{row.id}</small>
                <small>{row.strategy}</small>
              </td>
              <td><strong>{row.runType}</strong><small>{row.executionMode}</small></td>
              <td><StatusBadge value={row.status} /><small>{row.phase || 'Lifecycle phase unavailable'}</small></td>
              <td><strong>{row.instruments.join(', ') || 'Unavailable'}</strong><small>{row.timeframe}</small></td>
              <td>
                <strong>{formatDuration(row.durationMs)}</strong>
                <small>{row.simulatedStart || row.simulatedEnd ? formatTime(row.simulatedStart) + ' → ' + formatTime(row.simulatedEnd) : 'No simulated window'}</small>
              </td>
              <td>
                <Link className="qt2-text-link" to={'/operations/runs/' + row.id} state={{ run: row.run, definition: row.definition, from: '/operations?tab=runs' }}>Inspect run</Link>
                <small>{row.botLensAvailable ? 'Persisted projection available' : row.botLensReason || 'Projection availability unreported'}</small>
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}

function DataPlaneTable({ collectorFeed, marketFeed, postureRows, streamRows, nowEpochMs, query }) {
  const needle = query.trim().toLowerCase()
  const collectors = collectorFeed.collectors
    .map((entry) => ({ ...entry, vm: buildCollectorCardViewModel(entry.definition, entry.attempts, { nowEpochMs }) }))
    .filter(({ vm }) => !needle || [vm.displayName, vm.instrumentLabel, vm.venueLabel, vm.health.status].some((value) => String(value).toLowerCase().includes(needle)))
  const sessions = streamRows.filter((row) => !needle || [row.productId, row.pairId, row.eventType, ...row.channels].some((value) => String(value || '').toLowerCase().includes(needle)))

  return (
    <div className="qt2-stack">
      <section>
        <div className="qt2-subsection-head"><div><h2>Data quality by pair</h2><p>Coverage, validity, archive, normalization, and admission remain independent facts.</p></div><span className="qt2-count">{postureRows.length}</span></div>
        {marketFeed.loading && !postureRows.length ? <div className="qt2-empty">Loading data-quality evidence…</div> : (
          <div className="qt2-table-wrap"><table className="qt2-data-table"><thead><tr><th>Pair</th><th>Collection</th><th>Coverage</th><th>Book</th><th>Archive</th><th>Normalization</th><th>Admission</th></tr></thead><tbody>
            {postureRows.map((row) => <tr key={row.id}>
              <td><strong>{row.label}</strong><small>{row.products.join(', ')}</small></td>
              <td><StatusBadge value={row.collection.label} tone={row.collection.tone} /><small>{row.collection.detail}</small></td>
              <td><StatusBadge value={row.coverage.label} tone={row.coverage.tone} /><small>{row.qualityCount} quality records</small></td>
              <td><StatusBadge value={row.book.label} tone={row.book.tone} /></td>
              <td><StatusBadge value={row.archive.label} tone={row.archive.tone} /></td>
              <td><StatusBadge value={row.normalization.label} tone={row.normalization.tone} /></td>
              <td><StatusBadge value={row.admission.label} tone={row.admission.tone} /></td>
            </tr>)}
          </tbody></table></div>
        )}
      </section>

      <section>
        <div className="qt2-subsection-head"><div><h2>Collector schedules</h2><p>Schedule and delivery evidence only; no collector heartbeat exists.</p></div><span className="qt2-count">{collectors.length}</span></div>
        {collectorFeed.loading && !collectors.length ? <div className="qt2-empty">Loading collector schedules…</div> : null}
        {!collectors.length && !collectorFeed.loading ? <div className="qt2-empty">No collector schedules match the current filter.</div> : (
          <div className="qt2-table-wrap"><table className="qt2-data-table"><thead><tr><th>Schedule definition</th><th>Configured</th><th>Delivery evidence</th><th>Last attempt</th><th>Next expected</th><th>Attempts</th></tr></thead><tbody>
            {collectors.map(({ definition, attempts, attemptsAvailable, attemptsError, vm }) => <tr key={definition.id}>
              <td><strong>{vm.displayName}</strong><small className="qt-mono">{definition.id}</small></td>
              <td><StatusBadge value={definition.enabled ? 'enabled' : 'disabled'} tone={definition.enabled ? 'info' : 'neutral'} /><small>{vm.cadenceLabel}</small></td>
              <td><StatusBadge value={vm.statusLabel} tone={vm.display.tone === 'rose' ? 'danger' : vm.display.tone === 'amber' ? 'warning' : vm.display.tone === 'emerald' ? 'success' : 'neutral'} /><small>Process liveness unknown</small></td>
              <td>{formatTime(vm.health.lastAttemptAt)}<small>{vm.health.lastAttemptStatus || 'Status unavailable'}</small></td>
              <td>{formatTime(vm.health.nextExpectedAt)}</td>
              <td><Link className="qt2-text-link" to={'/operations/collectors/' + definition.id} state={{ from: '/operations?tab=data-plane' }}>Inspect {attempts.length} recent</Link><small>{attemptsAvailable ? 'History projection available' : attemptsError}</small></td>
            </tr>)}
          </tbody></table></div>
        )}
      </section>

      <section>
        <div className="qt2-subsection-head"><div><h2>Continuous stream sessions</h2><p>Latest persisted session event per definition and session identity.</p></div><span className="qt2-count">{sessions.length}</span></div>
        {!sessions.length ? <div className="qt2-empty">No stream-session evidence matches the current filter.</div> : (
          <div className="qt2-table-wrap"><table className="qt2-data-table"><thead><tr><th>Product</th><th>Channels</th><th>Last event</th><th>Ownership evidence</th><th>Observed</th></tr></thead><tbody>
            {sessions.map((row) => <tr key={row.id}>
              <td><strong>{row.productId}</strong><small>{row.pairId || 'Pair unavailable'}</small></td>
              <td>{row.channels.join(' + ') || 'Unavailable'}</td>
              <td><StatusBadge value={row.eventLabel} tone={row.tone} /><small className="qt-mono">{row.sessionId}</small></td>
              <td>{row.leaseCurrent ? 'Current fenced lease' : 'No current lease'}<small>{row.bounded ? 'Bounded session' : 'Continuous session'}</small></td>
              <td>{formatTime(row.occurredAt)}</td>
            </tr>)}
          </tbody></table></div>
        )}
      </section>
    </div>
  )
}

function ResearchTable({ rows, loading }) {
  if (loading && !rows.length) return <div className="qt2-empty">Loading research evidence…</div>
  if (!rows.length) return <div className="qt2-empty">No research evidence matches the current filters.</div>
  return (
    <div className="qt2-table-wrap"><table className="qt2-data-table"><thead><tr><th>Evidence</th><th>Kind</th><th>State</th><th>Scope</th><th>Persisted</th><th>Trace</th></tr></thead><tbody>
      {rows.map((item) => <tr key={item.id}>
        <td><strong>{item.title || 'Untitled evidence'}</strong><small className="qt-mono">{item.id}</small></td>
        <td>{item.kind}</td>
        <td><StatusBadge value={item.status} /></td>
        <td>{[item.symbol, item.timeframe].filter(Boolean).join(' · ') || 'Unavailable'}<small>{(item.tags || []).join(', ') || 'No tags'}</small></td>
        <td>{formatTime(item.created_at)}</td>
        <td><Link className="qt2-text-link" to={'/operations/research/' + item.id} state={{ item, from: '/operations?tab=research' }}>Inspect trail</Link></td>
      </tr>)}
    </tbody></table></div>
  )
}

function DefinitionsTable({ definitions, query }) {
  const needle = query.trim().toLowerCase()
  const rows = definitions.filter((definition) => !needle || [
    definition.id,
    definition.name,
    definition.strategy_id,
    definition.run_type,
    ...(definition.symbols || []),
  ].some((value) => String(value || '').toLowerCase().includes(needle)))
  if (!rows.length) return <div className="qt2-empty">No bot definitions match the current filter.</div>
  return (
    <div className="qt2-table-wrap"><table className="qt2-data-table"><thead><tr><th>Bot definition</th><th>Strategy</th><th>Configured role</th><th>Instrument</th><th>Current run reference</th><th>Latest run reference</th></tr></thead><tbody>
      {rows.map((definition) => <tr key={definition.id}>
        <td><strong>{definition.name || 'Unnamed definition'}</strong><small className="qt-mono">{definition.id}</small></td>
        <td>{definition.strategy_name || definition.strategy_id || 'Unavailable'}</td>
        <td>{definition.run_type || 'Unavailable'}<small>{definition.execution_mode || 'Execution mode unavailable'}</small></td>
        <td>{(definition.symbols || []).join(', ') || 'Unavailable'}<small>{definition.timeframe || 'Timeframe unavailable'}</small></td>
        <td>{definition.active_run_id ? <Link className="qt2-text-link qt-mono" to={'/operations/runs/' + definition.active_run_id} state={{ definition, from: '/operations?tab=definitions' }}>{definition.active_run_id}</Link> : 'None'}</td>
        <td className="qt-mono">{definition.latest_run_id || 'None'}</td>
      </tr>)}
    </tbody></table></div>
  )
}

export function FleetRoom() {
  const [params, setParams] = useSearchParams()
  const tab = TABS.some((item) => item.id === params.get('tab')) ? params.get('tab') : 'runs'
  const { sortedBots, loading: definitionsLoading, error: botsError, nowEpochMs, hasReceivedSnapshot, refresh: refreshBots } = useFleetBotsFeed()
  const runInventory = useRunInventory(sortedBots)
  const collectorFeed = useCollectorsFeed()
  const marketFeed = useMarketStructureFeed()
  const [researchItems, setResearchItems] = useState([])
  const [researchLoading, setResearchLoading] = useState(true)
  const [researchError, setResearchError] = useState(null)
  const [researchRevision, setResearchRevision] = useState(0)
  const [query, setQuery] = useState('')
  const [status, setStatus] = useState('all')
  const [runType, setRunType] = useState('all')
  const [sort, setSort] = useState('recent')

  useEffect(() => {
    let mounted = true
    setResearchLoading(true)
    listResearchItems({ limit: 200 })
      .then((items) => {
        if (mounted) {
          setResearchItems(items)
          setResearchError(null)
        }
      })
      .catch((error) => {
        if (mounted) setResearchError(error?.message || 'Research evidence unavailable')
      })
      .finally(() => {
        if (mounted) setResearchLoading(false)
      })
    return () => { mounted = false }
  }, [researchRevision])

  const runRows = useMemo(() => filterAndSortRunRows(
    buildRunRows(runInventory.runs, { nowEpochMs }),
    { query, status, runType, sort },
  ), [runInventory.runs, nowEpochMs, query, status, runType, sort])
  const researchRows = useMemo(() => filterResearchRows(researchItems, { query, status }), [researchItems, query, status])
  const streamRows = useMemo(() => buildStreamSessionRows({
    definitions: marketFeed.definitions,
    sessions: marketFeed.sessions,
  }), [marketFeed.definitions, marketFeed.sessions])
  const postureRows = useMemo(() => buildMarketPostureRows({
    definitions: marketFeed.definitions,
    sessions: marketFeed.sessions,
    statusByDefinition: marketFeed.statusByDefinition,
    normalizationSpecs: marketFeed.normalizationSpecs,
    collectors: collectorFeed.collectors,
    nowEpochMs,
  }), [marketFeed.definitions, marketFeed.sessions, marketFeed.statusByDefinition, marketFeed.normalizationSpecs, collectorFeed.collectors, nowEpochMs])
  const statusOptions = useMemo(() => {
    const source = tab === 'research' ? researchItems.map((item) => item.status) : runInventory.runs.map((run) => run.runtime_status || run.status)
    return [...new Set(source.filter(Boolean))].sort()
  }, [tab, researchItems, runInventory.runs])
  const errors = [botsError, runInventory.error, collectorFeed.error, marketFeed.error, researchError].filter(Boolean)

  function selectTab(nextTab) {
    setStatus('all')
    setParams({ tab: nextTab }, { replace: true })
  }

  function refresh() {
    refreshBots()
    runInventory.refresh()
    collectorFeed.refresh()
    marketFeed.refresh()
    setResearchRevision((value) => value + 1)
  }

  return (
    <div className="qt2-room">
      <div className="qt2-room-head">
        <div>
          <span className="qt2-kicker">Read-only inventory</span>
          <h1 className="qt2-title">Operations</h1>
          <p className="qt2-sub">Definitions, run instances, schedules, attempts, streams, quality, and research evidence—kept distinct. Run history is bounded to the latest 50 per definition.</p>
        </div>
        <div className="qt2-head-actions">
          <span className="qt2-observation-note">{hasReceivedSnapshot ? 'Definition projection streaming' : 'Definition stream connecting'} · runs and data evidence polled</span>
          <button type="button" className="qt2-button" onClick={refresh}><RefreshCcw size={14} />Refresh</button>
        </div>
      </div>

      {errors.map((error, index) => <div className="qt2-error" key={error + index}>{error}</div>)}

      <div className="qt2-tabs" role="tablist" aria-label="Operations inventory">
        {TABS.map((item) => <button type="button" role="tab" aria-selected={tab === item.id} key={item.id} className={tab === item.id ? 'is-active' : ''} onClick={() => selectTab(item.id)}>{item.label}</button>)}
      </div>

      <div className="qt2-filterbar">
        <label className="qt2-search"><Search size={14} /><input value={query} onChange={(event) => setQuery(event.target.value)} placeholder="Filter current inventory" /></label>
        {(tab === 'runs' || tab === 'research') ? <select className="qt2-select" value={status} onChange={(event) => setStatus(event.target.value)}><option value="all">All states</option>{statusOptions.map((value) => <option value={value} key={value}>{value}</option>)}</select> : null}
        {tab === 'runs' ? <select className="qt2-select" value={runType} onChange={(event) => setRunType(event.target.value)}><option value="all">All run types</option><option value="live">Live</option><option value="paper">Paper</option><option value="backtest">Backtest</option></select> : null}
        {tab === 'runs' ? <select className="qt2-select" value={sort} onChange={(event) => setSort(event.target.value)}><option value="recent">Newest started</option><option value="oldest">Oldest started</option><option value="status">State</option><option value="definition">Definition</option></select> : null}
      </div>

      <section className="qt2-section qt2-operations-body">
        {tab === 'runs' ? <RunsTable rows={runRows} loading={runInventory.loading} /> : null}
        {tab === 'data-plane' ? <DataPlaneTable collectorFeed={collectorFeed} marketFeed={marketFeed} postureRows={postureRows} streamRows={streamRows} nowEpochMs={nowEpochMs} query={query} /> : null}
        {tab === 'research' ? <ResearchTable rows={researchRows} loading={researchLoading} /> : null}
        {tab === 'definitions' ? (definitionsLoading && !sortedBots.length ? <div className="qt2-empty">Loading definitions…</div> : <DefinitionsTable definitions={sortedBots} query={query} />) : null}
      </section>
    </div>
  )
}
