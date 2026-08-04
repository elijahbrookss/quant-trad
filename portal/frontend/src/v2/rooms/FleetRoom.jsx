import { useEffect, useMemo, useState } from 'react'
import { Link, useSearchParams } from 'react-router-dom'
import { Copy, MoreHorizontal, RefreshCcw, Search } from 'lucide-react'
import { listResearchItems } from '../../adapters/research.adapter.js'
import { useFleetBotsFeed } from '../../features/bots/page/useFleetBotsFeed.js'
import { buildCollectorCardViewModel } from '../../features/collectors/buildCollectorCardViewModel.js'
import { useCollectorsFeed } from '../../features/collectors/useCollectorsFeed.js'
import {
  buildMarketPostureRows,
  buildStreamSessionRows,
} from '../../features/market-structure/buildMarketPosture.js'
import { formatMarketStructureComponentError, useMarketStructureFeed } from '../../features/market-structure/useMarketStructureFeed.js'
import {
  buildRunRows,
  filterAndSortRunRows,
  filterResearchRows,
} from '../../features/operations/buildOperationsViewModel.js'
import { useActiveRunsFeed } from '../../features/operations/useActiveRunsFeed.js'
import { useRunInventory } from '../../features/operations/useRunInventory.js'
import { OperatorErrorNotice, OperatorSkeleton } from '../components/OperatorErrorNotice.jsx'
import { Pagination, paginateRows } from '../components/Pagination.jsx'

const PAGE_SIZE = 12
const TABS = [
  { id: 'runs', label: 'Runs' },
  { id: 'market', label: 'Market' },
  { id: 'research', label: 'Research' },
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

function formatNumber(value) {
  return Number.isFinite(Number(value)) ? Number(value).toLocaleString(undefined, { maximumFractionDigits: 2 }) : '—'
}

function StatusBadge({ value, tone }) {
  const normalized = tone || (
    ['completed', 'succeeded', 'tested', 'promoted', 'healthy', 'on schedule'].includes(String(value || '').toLowerCase())
      ? 'success'
      : ['failed', 'crashed', 'blocked', 'overdue', 'invalid'].includes(String(value || '').toLowerCase())
        ? 'danger'
        : ['running', 'starting', 'active', 'open_valid'].includes(String(value || '').toLowerCase())
          ? 'info'
          : 'neutral'
  )
  return <span className={'qt2-evidence-state is-' + normalized}>{value || 'unknown'}</span>
}

async function copyText(value) {
  if (!value) return
  await navigator.clipboard.writeText(String(value))
}

function RunMenu({ row }) {
  const canCopyRerun = row.runType === 'backtest' && row.definitionId && row.datasetId
  const rerunCommand = canCopyRerun
    ? `qt bots start ${row.definitionId} --run-type backtest --dataset-id ${row.datasetId}`
    : null
  return (
    <details className="qt2-context-menu">
      <summary aria-label={`Actions for run ${row.id}`}><MoreHorizontal size={16} /></summary>
      <div>
        <button type="button" onClick={() => copyText(row.id)}><Copy size={13} />Copy run ID</button>
        {rerunCommand ? <button type="button" onClick={() => copyText(rerunCommand)}><Copy size={13} />Copy rerun command</button> : null}
        {!rerunCommand && row.runType === 'backtest' ? <span>Rerun command unavailable: immutable dataset identity was not reported.</span> : null}
      </div>
    </details>
  )
}

function RunsTable({ pageModel, loading, hasMore, loadingMore, onLoadMore }) {
  if (loading && !pageModel.total) return <OperatorSkeleton rows={5} label="Loading persisted run inventory" />
  if (!pageModel.total) return <div className="qt2-empty">No runs match the current filters.</div>
  return (
    <>
      <div className="qt2-table-wrap">
        <table className="qt2-data-table qt2-run-table">
          <thead><tr><th>Run</th><th>Scope</th><th>State</th><th>Result</th><th aria-label="Actions" /></tr></thead>
          <tbody>
            {pageModel.rows.map((row) => (
              <tr key={row.id}>
                <td><strong>{row.definitionName}</strong><small>{row.strategy}</small><small>{formatTime(row.startedAt)}</small></td>
                <td>
                  <strong>{row.instruments.join(', ') || 'Unavailable'}</strong>
                  <small>{row.runType} · {row.executionMode} · {row.timeframe}</small>
                  {row.runType === 'backtest' ? (
                    <small
                      className="qt2-run-window"
                      title="Backtests evaluate the start boundary through, but not including, the displayed end boundary."
                    >
                      Test window · {row.simulatedWindowLabel || 'Unavailable'}
                    </small>
                  ) : null}
                </td>
                <td><StatusBadge value={row.status} /><small>{row.phase || 'Lifecycle phase unavailable'}</small></td>
                <td><strong>{row.status === 'completed' ? `${formatNumber(row.netPnl)} net P&L` : formatDuration(row.durationMs)}</strong><small>{row.totalTrades !== null ? `${row.totalTrades} trades` : 'Trade count unavailable'}</small></td>
                <td className="qt2-actions-cell">
                  {row.botLensAvailable ? (
                    <Link className="qt2-button qt2-button-primary" to={'/operations/runs/' + row.id} state={{ run: row.run, definition: row.definition, from: '/operations?tab=runs' }}>{row.isActive ? 'Open BotLens' : 'Open replay'}</Link>
                  ) : (
                    <button type="button" className="qt2-button" disabled title={row.botLensReason || 'No persisted BotLens projection is available for this run.'}>BotLens unavailable</button>
                  )}
                  <RunMenu row={row} />
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      <div className="qt2-cursor-footer">
        <span>{pageModel.total} loaded run{pageModel.total === 1 ? '' : 's'}</span>
        {hasMore ? <button type="button" className="qt2-button" onClick={onLoadMore} disabled={loadingMore}>{loadingMore ? 'Loading older…' : 'Load older runs'}</button> : <span>Beginning of available history</span>}
      </div>
    </>
  )
}

function CurrentRunCards({ rows, loading }) {
  if (loading && !rows.length) return <OperatorSkeleton rows={4} label="Connecting to active operations" />
  if (!rows.length) return <div className="qt2-empty">No active runs.</div>
  return (
    <div className="qt2-active-run-grid">
      {rows.map((row) => (
        <article className="qt2-active-run-card" key={row.id}>
          <div className="qt2-active-run-head">
            <StatusBadge value={row.status} />
            <span>{row.runType} · {row.executionMode}</span>
          </div>
          <div>
            <h2>{row.definitionName}</h2>
            <p>{row.strategy}</p>
          </div>
          <dl>
            <div><dt>Scope</dt><dd>{row.instruments.join(', ') || 'Instrument unavailable'} · {row.timeframe}</dd></div>
            <div><dt>Runtime</dt><dd>{formatDuration(row.durationMs)}</dd></div>
            <div><dt>Current work</dt><dd>{row.phaseLabel}</dd></div>
            <div><dt>Liveness</dt><dd>{row.livenessLabel}</dd></div>
            <div><dt>Last update</dt><dd>{formatTime(row.knownAt || row.startedAt)}</dd></div>
          </dl>
          {row.progress !== null ? (
            <div className="qt2-run-progress" aria-label={`Run progress ${row.progressPercent}%`}>
              <div><span>Replay progress</span><strong>{row.progressCurrent !== null && row.progressTotal !== null ? `${row.progressCurrent.toLocaleString()} / ${row.progressTotal.toLocaleString()}` : `${row.progressPercent}%`}</strong></div>
              <progress max="1" value={row.progress} />
            </div>
          ) : null}
          <div className="qt2-active-run-actions">
            {row.botLensAvailable ? (
              <Link className="qt2-button qt2-button-primary" to={'/operations/runs/' + row.id} state={{ run: row.run, definition: row.definition, from: '/operations?tab=runs' }}>Open BotLens</Link>
            ) : (
              <button type="button" className="qt2-button" disabled title={row.botLensReason}>BotLens connecting</button>
            )}
            <RunMenu row={row} />
          </div>
        </article>
      ))}
    </div>
  )
}

function instrumentLookup(instruments = []) {
  return new Map(instruments.map((instrument) => [String(instrument.id), instrument]))
}

function buildCollectorGroups(collectorFeed, nowEpochMs, query) {
  const instruments = instrumentLookup(collectorFeed.instruments)
  const groups = new Map()
  collectorFeed.collectors.forEach((entry) => {
    const { definition, attempts } = entry
    const vm = buildCollectorCardViewModel(definition, attempts, { nowEpochMs })
    const instrument = instruments.get(String(definition.instrument_id)) || null
    const productId = definition?.config?.provider_product_id
      || definition?.provider_product_id
      || instrument?.symbol
      || 'Instrument unavailable'
    const provider = String(definition.provider || 'Provider unavailable').toUpperCase()
    const key = `${provider}:${productId}`
    if (!groups.has(key)) {
      groups.set(key, {
        id: key,
        provider,
        productId,
        canonicalSymbol: instrument?.symbol || null,
        instrumentId: definition.instrument_id || null,
        facts: [],
      })
    }
    groups.get(key).facts.push({ ...entry, vm })
  })

  const needle = query.trim().toLowerCase()
  return [...groups.values()]
    .map((group) => {
      const enabled = group.facts.filter(({ definition }) => definition.enabled)
      const issues = enabled.filter(({ vm }) => vm.health.status !== 'healthy')
      const healthy = enabled.length - issues.length
      return { ...group, enabledCount: enabled.length, healthyCount: healthy, issues }
    })
    .filter((group) => !needle || [
      group.provider,
      group.productId,
      group.canonicalSymbol,
      group.instrumentId,
      ...group.facts.flatMap(({ definition, vm }) => [definition.fact_type, vm.statusLabel]),
    ].some((value) => String(value || '').toLowerCase().includes(needle)))
    .sort((left, right) => left.provider.localeCompare(right.provider) || left.productId.localeCompare(right.productId))
}

function CollectorsTable({ pageModel, loading, onPageChange, showPagination = true }) {
  if (loading && !pageModel.total) return <OperatorSkeleton rows={4} label="Loading collector health" />
  if (!pageModel.total) return <div className="qt2-empty">No collectors match the current filter.</div>
  return (
    <>
      <div className="qt2-collector-summary">
        <span><strong>{pageModel.total}</strong> provider/instrument groups</span>
        <span>Healthy requires a live worker heartbeat, current schedule, and fresh delivery.</span>
      </div>
      <div className="qt2-table-wrap">
        <table className="qt2-data-table qt2-collector-table">
          <thead><tr><th>Instrument</th><th>Provider</th><th>Facts</th><th>Delivery</th><th aria-label="Actions" /></tr></thead>
          <tbody>
            {pageModel.rows.map((group) => {
              const firstIssue = group.issues[0] || group.facts[0]
              return (
                <tr key={group.id}>
                  <td><strong>{group.productId}</strong><small>{group.canonicalSymbol || 'Canonical symbol unavailable'}</small></td>
                  <td><strong>{group.provider}</strong><small>{group.instrumentId || 'Instrument ID unavailable'}</small></td>
                  <td><div className="qt2-chip-list">{group.facts.map(({ definition }) => <span key={definition.id}>{definition.fact_type}</span>)}</div></td>
                  <td>
                    <StatusBadge value={group.issues.length ? `${group.issues.length} problem${group.issues.length === 1 ? '' : 's'}` : 'On schedule'} tone={group.issues.length ? 'danger' : 'success'} />
                    <small>{group.healthyCount}/{group.enabledCount} enabled schedules on time</small>
                  </td>
                  <td className="qt2-actions-cell">
                    {firstIssue ? <Link className="qt2-button" to={'/operations/market/' + firstIssue.definition.id} state={{ from: '/operations?tab=market' }}>Inspect</Link> : null}
                    <details className="qt2-context-menu">
                      <summary aria-label={`Collector schedules for ${group.productId}`}><MoreHorizontal size={16} /></summary>
                      <div>
                        {group.facts.map(({ definition, vm }) => (
                          <Link key={definition.id} to={'/operations/market/' + definition.id} state={{ from: '/operations?tab=market' }}>
                            {definition.fact_type}<small>{vm.statusLabel}</small>
                          </Link>
                        ))}
                      </div>
                    </details>
                  </td>
                </tr>
              )
            })}
          </tbody>
        </table>
      </div>
      {showPagination ? <Pagination {...pageModel} onChange={onPageChange} /> : null}
    </>
  )
}

function MarketDataTable({ pageModel, loading, streamCount, onPageChange, showPagination = true }) {
  if (loading && !pageModel.total) return <OperatorSkeleton rows={3} label="Loading market evidence" />
  if (!pageModel.total) return <div className="qt2-empty">No market-data pairs match the current filter.</div>
  return (
    <>
      <div className="qt2-collector-summary">
        <span><strong>{pageModel.total}</strong> configured pairs</span>
        <span>{streamCount} persisted stream-session records are available to deeper forensic tooling.</span>
      </div>
      <div className="qt2-table-wrap">
        <table className="qt2-data-table qt2-market-table">
          <thead><tr><th>Pair</th><th>Collection</th><th>Coverage / book</th><th>Archive / normalization</th><th>Admission</th></tr></thead>
          <tbody>
            {pageModel.rows.map((row) => (
              <tr key={row.id}>
                <td><strong>{row.label}</strong><small>{row.products.join(', ') || 'Products unavailable'}</small></td>
                <td><StatusBadge value={row.collection.label} tone={row.collection.tone} /><small>{row.collection.detail}</small></td>
                <td><StatusBadge value={row.coverage.label} tone={row.coverage.tone} /><small>{row.book.label} · {row.qualityCount} quality records</small></td>
                <td><StatusBadge value={row.archive.label} tone={row.archive.tone} /><small>{row.normalization.label}</small></td>
                <td><StatusBadge value={row.admission.label} tone={row.admission.tone} /></td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      {showPagination ? <Pagination {...pageModel} onChange={onPageChange} /> : null}
    </>
  )
}

function ResearchTable({ pageModel, loading, onPageChange }) {
  if (loading && !pageModel.total) return <OperatorSkeleton rows={5} label="Loading research evidence" />
  if (!pageModel.total) return <div className="qt2-empty">No research evidence matches the current filters.</div>
  return (
    <>
      <div className="qt2-table-wrap"><table className="qt2-data-table qt2-research-table"><thead><tr><th>Evidence</th><th>Kind</th><th>State</th><th>Scope</th><th>Persisted</th><th /></tr></thead><tbody>
        {pageModel.rows.map((item) => <tr key={item.id}>
          <td><strong>{item.title || 'Untitled evidence'}</strong><small className="qt-mono">{item.id}</small></td>
          <td>{item.kind}</td>
          <td><StatusBadge value={item.status} /></td>
          <td>{[item.symbol, item.timeframe].filter(Boolean).join(' · ') || 'Unavailable'}<small>{(item.tags || []).join(', ') || 'No tags'}</small></td>
          <td>{formatTime(item.created_at)}</td>
          <td><Link className="qt2-button" to={'/operations/research/' + item.id} state={{ item, from: '/operations?tab=research' }}>Inspect</Link></td>
        </tr>)}
      </tbody></table></div>
      <Pagination {...pageModel} onChange={onPageChange} />
    </>
  )
}

function matchesMarketQuery(row, query) {
  const needle = query.trim().toLowerCase()
  return !needle || [row.label, ...row.products, row.collection.label, row.coverage.label, row.admission.label]
    .some((value) => String(value || '').toLowerCase().includes(needle))
}

export function FleetRoom() {
  const [params, setParams] = useSearchParams()
  const rawTab = params.get('tab')
  const requestedTab = ['data-plane', 'collectors', 'market-data'].includes(rawTab)
    ? 'market'
    : rawTab
  const tab = TABS.some((item) => item.id === requestedTab) ? requestedTab : 'runs'
  const runView = params.get('view') === 'history' ? 'history' : 'current'
  const definitionsEnabled = tab === 'runs' && runView === 'history'
  const { sortedBots, error: botsError, nowEpochMs, hasReceivedSnapshot, refresh: refreshBots } = useFleetBotsFeed({ enabled: definitionsEnabled })
  const activeRunsFeed = useActiveRunsFeed({ enabled: tab === 'runs' && runView === 'current' })
  const runInventory = useRunInventory(sortedBots, { enabled: tab === 'runs' && runView === 'history' })
  const collectorFeed = useCollectorsFeed({ enabled: tab === 'market' })
  const marketFeed = useMarketStructureFeed({ enabled: tab === 'market' })
  const [researchItems, setResearchItems] = useState([])
  const [researchLoading, setResearchLoading] = useState(true)
  const [researchError, setResearchError] = useState(null)
  const [researchRevision, setResearchRevision] = useState(0)
  const [query, setQuery] = useState('')
  const [status, setStatus] = useState('all')
  const [runType, setRunType] = useState('all')
  const [sort, setSort] = useState('recent')
  const [page, setPage] = useState(1)

  useEffect(() => {
    if (tab !== 'research') {
      setResearchLoading(false)
      return undefined
    }
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
  }, [researchRevision, tab])

  useEffect(() => setPage(1), [tab, query, status, runType, sort])

  const runRows = useMemo(() => filterAndSortRunRows(
    runView === 'history'
      ? buildRunRows(runInventory.runs, { nowEpochMs })
      : buildRunRows(activeRunsFeed.runs, { nowEpochMs }),
    { query, status, runType, sort },
  ), [activeRunsFeed.runs, runInventory.runs, runView, nowEpochMs, query, status, runType, sort])
  const collectorGroups = useMemo(
    () => buildCollectorGroups(collectorFeed, nowEpochMs, query),
    [collectorFeed, nowEpochMs, query],
  )
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
    normalizationAvailable: !marketFeed.componentErrors.normalization_specs,
    collectors: collectorFeed.collectors,
    nowEpochMs,
  }).filter((row) => matchesMarketQuery(row, query)), [marketFeed.definitions, marketFeed.sessions, marketFeed.statusByDefinition, marketFeed.normalizationSpecs, marketFeed.componentErrors.normalization_specs, collectorFeed.collectors, nowEpochMs, query])
  const statusOptions = useMemo(() => {
    const source = tab === 'research' ? researchItems.map((item) => item.status) : runRows.map((run) => run.status)
    return [...new Set(source.filter(Boolean))].sort()
  }, [tab, researchItems, runRows])
  const visibleRows = tab === 'research' ? researchRows : []
  const pageModel = paginateRows(visibleRows, page, PAGE_SIZE)
  const runPageModel = paginateRows(runRows, 1, Math.max(PAGE_SIZE, runRows.length))
  const collectorPageModel = paginateRows(collectorGroups, 1, Math.max(PAGE_SIZE, collectorGroups.length))
  const marketPageModel = paginateRows(postureRows, 1, Math.max(PAGE_SIZE, postureRows.length))
  const marketComponentErrors = Object.entries(marketFeed.componentErrors)
    .map(([component, error]) => ({ component, error: formatMarketStructureComponentError(error) }))
    .filter((entry) => entry.error)

  function selectTab(nextTab) {
    setStatus('all')
    setParams({ tab: nextTab }, { replace: true })
  }

  function selectRunView(nextView) {
    setStatus('all')
    setPage(1)
    setParams({ tab: 'runs', ...(nextView === 'history' ? { view: 'history' } : {}) }, { replace: true })
  }

  function refresh() {
    if (tab === 'runs') {
      if (runView === 'current') activeRunsFeed.refresh()
      else {
        refreshBots()
        runInventory.refresh()
      }
    } else if (tab === 'market') {
      collectorFeed.refresh()
      marketFeed.refresh()
    } else {
      setResearchRevision((value) => value + 1)
    }
  }

  return (
    <div className="qt2-room">
      <div className="qt2-room-head">
        <div>
          <h1 className="qt2-title">Operations</h1>
          <p className="qt2-sub">Find the thing first. Open the evidence only when you need it.</p>
        </div>
        <div className="qt2-head-actions">
          <span className="qt2-observation-note">{tab === 'runs' ? (runView === 'current' ? (activeRunsFeed.hasReceivedSnapshot ? 'Active run stream live' : 'Active run stream connecting') : (hasReceivedSnapshot ? 'Run history ready' : 'Run history connecting')) : tab === 'market' ? 'Market snapshots + live deltas' : 'Bounded research inventory'}</span>
          <button type="button" className="qt2-icon-button" onClick={refresh}><RefreshCcw size={14} />Refresh</button>
        </div>
      </div>

      <div className="qt2-tabs" role="tablist" aria-label="Operations inventory">
        {TABS.map((item) => <button type="button" role="tab" aria-selected={tab === item.id} key={item.id} className={tab === item.id ? 'is-active' : ''} onClick={() => selectTab(item.id)}>{item.label}</button>)}
      </div>

      {tab === 'runs' ? (
        <div className="qt2-run-view-switch" role="tablist" aria-label="Run inventory scope">
          <button type="button" role="tab" aria-selected={runView === 'current'} className={runView === 'current' ? 'is-active' : ''} onClick={() => selectRunView('current')}>Current</button>
          <button type="button" role="tab" aria-selected={runView === 'history'} className={runView === 'history' ? 'is-active' : ''} onClick={() => selectRunView('history')}>History</button>
          <span>{runView === 'current' ? 'Live run instances, one card per container' : 'Persisted terminal and prior runs · 20 at a time'}</span>
        </div>
      ) : null}

      <div className="qt2-filterbar">
        <label className="qt2-search"><Search size={14} /><input value={query} onChange={(event) => setQuery(event.target.value)} placeholder={tab === 'market' ? 'Provider, instrument, pair, or fact' : 'Filter this inventory'} /></label>
        {(tab === 'runs' || tab === 'research') ? <select className="qt2-select" value={status} onChange={(event) => setStatus(event.target.value)}><option value="all">All states</option>{statusOptions.map((value) => <option value={value} key={value}>{value}</option>)}</select> : null}
        {tab === 'runs' ? <select className="qt2-select" value={runType} onChange={(event) => setRunType(event.target.value)}><option value="all">All run types</option><option value="live">Live</option><option value="paper">Paper</option><option value="backtest">Backtest</option></select> : null}
        {tab === 'runs' ? <select className="qt2-select" value={sort} onChange={(event) => setSort(event.target.value)}><option value="recent">Newest started</option><option value="oldest">Oldest started</option><option value="status">State</option><option value="definition">Definition</option></select> : null}
      </div>

      <section className="qt2-section qt2-operations-body">
        {tab === 'runs' ? (
          <>
            {runView === 'current' && activeRunsFeed.error ? <OperatorErrorNotice error={activeRunsFeed.error} compact /> : null}
            {runView === 'history' && botsError ? <OperatorErrorNotice error={botsError} compact /> : null}
            {runView === 'history' && runInventory.error ? <OperatorErrorNotice error={runInventory.error} compact /> : null}
            {runView === 'current' ? (
              <CurrentRunCards rows={runRows} loading={activeRunsFeed.loading && !activeRunsFeed.runs.length} />
            ) : (
              <RunsTable pageModel={runPageModel} loading={runInventory.loading} hasMore={runInventory.hasMore} loadingMore={runInventory.loadingMore} onLoadMore={runInventory.loadMore} />
            )}
          </>
        ) : null}
        {tab === 'market' ? (
          <div className="qt2-market-inventory-stack">
            <section aria-labelledby="scheduled-facts-heading">
              <div className="qt2-inventory-heading"><div><h2 id="scheduled-facts-heading">Scheduled facts</h2><p>Provider, instrument, worker liveness, schedule, and delivery.</p></div><span>{collectorGroups.length}</span></div>
              {collectorFeed.error ? <OperatorErrorNotice error={collectorFeed.error} compact /> : null}
              {collectorFeed.streamError ? <OperatorErrorNotice error={collectorFeed.streamError} compact /> : null}
              <CollectorsTable pageModel={collectorPageModel} loading={collectorFeed.loading} showPagination={false} />
            </section>
            <section aria-labelledby="structure-streams-heading">
              <div className="qt2-inventory-heading"><div><h2 id="structure-streams-heading">Structure streams</h2><p>Coverage, book validity, archive, normalization, and admission.</p></div><span>{postureRows.length}</span></div>
              {marketFeed.error ? <OperatorErrorNotice error={marketFeed.error} compact /> : null}
              {marketFeed.streamError ? <OperatorErrorNotice error={marketFeed.streamError} compact /> : null}
              {marketComponentErrors.map(({ component, error }) => <div className="qt2-component-boundary" data-component={component} key={component}><OperatorErrorNotice error={error} compact /></div>)}
              <MarketDataTable pageModel={marketPageModel} loading={marketFeed.loading} streamCount={streamRows.length} showPagination={false} />
            </section>
          </div>
        ) : null}
        {tab === 'research' ? (
          <>
            {researchError ? <OperatorErrorNotice error={researchError} compact /> : null}
            <ResearchTable pageModel={pageModel} loading={researchLoading} onPageChange={setPage} />
          </>
        ) : null}
      </section>
    </div>
  )
}
