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
import { useMarketStructureFeed } from '../../features/market-structure/useMarketStructureFeed.js'
import {
  buildRunRows,
  filterAndSortRunRows,
  filterResearchRows,
} from '../../features/operations/buildOperationsViewModel.js'
import { useRunInventory } from '../../features/operations/useRunInventory.js'
import { OperatorErrorNotice } from '../components/OperatorErrorNotice.jsx'
import { Pagination, paginateRows } from '../components/Pagination.jsx'

const PAGE_SIZE = 12
const TABS = [
  { id: 'runs', label: 'Runs' },
  { id: 'collectors', label: 'Collectors' },
  { id: 'market-data', label: 'Market data' },
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

function RunsTable({ pageModel, loading, onPageChange }) {
  if (loading && !pageModel.total) return <div className="qt2-empty">Loading persisted run inventory…</div>
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
                <td><strong>{row.instruments.join(', ') || 'Unavailable'}</strong><small>{row.runType} · {row.executionMode} · {row.timeframe}</small></td>
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
      <Pagination {...pageModel} onChange={onPageChange} />
    </>
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

function CollectorsTable({ pageModel, loading, onPageChange }) {
  if (loading && !pageModel.total) return <div className="qt2-empty">Loading collector delivery evidence…</div>
  if (!pageModel.total) return <div className="qt2-empty">No collectors match the current filter.</div>
  return (
    <>
      <div className="qt2-collector-summary">
        <span><strong>{pageModel.total}</strong> provider/instrument groups</span>
        <span>“On schedule” describes delivery evidence only. Process liveness is not observed.</span>
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
                    {firstIssue ? <Link className="qt2-button" to={'/operations/collectors/' + firstIssue.definition.id} state={{ from: '/operations?tab=collectors' }}>Inspect</Link> : null}
                    <details className="qt2-context-menu">
                      <summary aria-label={`Collector schedules for ${group.productId}`}><MoreHorizontal size={16} /></summary>
                      <div>
                        {group.facts.map(({ definition, vm }) => (
                          <Link key={definition.id} to={'/operations/collectors/' + definition.id} state={{ from: '/operations?tab=collectors' }}>
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
      <Pagination {...pageModel} onChange={onPageChange} />
    </>
  )
}

function MarketDataTable({ pageModel, loading, streamCount, onPageChange }) {
  if (loading && !pageModel.total) return <div className="qt2-empty">Loading market-data evidence…</div>
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
      <Pagination {...pageModel} onChange={onPageChange} />
    </>
  )
}

function ResearchTable({ pageModel, loading, onPageChange }) {
  if (loading && !pageModel.total) return <div className="qt2-empty">Loading research evidence…</div>
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
  const requestedTab = params.get('tab') === 'data-plane' ? 'market-data' : params.get('tab')
  const tab = TABS.some((item) => item.id === requestedTab) ? requestedTab : 'runs'
  const { sortedBots, error: botsError, nowEpochMs, hasReceivedSnapshot, refresh: refreshBots } = useFleetBotsFeed()
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
  const [page, setPage] = useState(1)

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

  useEffect(() => setPage(1), [tab, query, status, runType, sort])

  const runRows = useMemo(() => filterAndSortRunRows(
    buildRunRows(runInventory.runs, { nowEpochMs }),
    { query, status, runType, sort },
  ), [runInventory.runs, nowEpochMs, query, status, runType, sort])
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
    collectors: collectorFeed.collectors,
    nowEpochMs,
  }).filter((row) => matchesMarketQuery(row, query)), [marketFeed.definitions, marketFeed.sessions, marketFeed.statusByDefinition, marketFeed.normalizationSpecs, collectorFeed.collectors, nowEpochMs, query])
  const statusOptions = useMemo(() => {
    const source = tab === 'research' ? researchItems.map((item) => item.status) : runInventory.runs.map((run) => run.runtime_status || run.status)
    return [...new Set(source.filter(Boolean))].sort()
  }, [tab, researchItems, runInventory.runs])
  const visibleRows = tab === 'runs' ? runRows : tab === 'collectors' ? collectorGroups : tab === 'market-data' ? postureRows : researchRows
  const pageModel = paginateRows(visibleRows, page, PAGE_SIZE)
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
          <h1 className="qt2-title">Operations</h1>
          <p className="qt2-sub">Find the thing first. Open the evidence only when you need it.</p>
        </div>
        <div className="qt2-head-actions">
          <span className="qt2-observation-note">{hasReceivedSnapshot ? 'Definitions streaming' : 'Definitions connecting'} · inventories polled</span>
          <button type="button" className="qt2-icon-button" onClick={refresh}><RefreshCcw size={14} />Refresh</button>
        </div>
      </div>

      {errors.map((error, index) => <OperatorErrorNotice error={error} key={String(error) + index} />)}

      <div className="qt2-tabs" role="tablist" aria-label="Operations inventory">
        {TABS.map((item) => <button type="button" role="tab" aria-selected={tab === item.id} key={item.id} className={tab === item.id ? 'is-active' : ''} onClick={() => selectTab(item.id)}>{item.label}</button>)}
      </div>

      <div className="qt2-filterbar">
        <label className="qt2-search"><Search size={14} /><input value={query} onChange={(event) => setQuery(event.target.value)} placeholder={tab === 'collectors' ? 'Provider, instrument, or fact' : 'Filter this inventory'} /></label>
        {(tab === 'runs' || tab === 'research') ? <select className="qt2-select" value={status} onChange={(event) => setStatus(event.target.value)}><option value="all">All states</option>{statusOptions.map((value) => <option value={value} key={value}>{value}</option>)}</select> : null}
        {tab === 'runs' ? <select className="qt2-select" value={runType} onChange={(event) => setRunType(event.target.value)}><option value="all">All run types</option><option value="live">Live</option><option value="paper">Paper</option><option value="backtest">Backtest</option></select> : null}
        {tab === 'runs' ? <select className="qt2-select" value={sort} onChange={(event) => setSort(event.target.value)}><option value="recent">Newest started</option><option value="oldest">Oldest started</option><option value="status">State</option><option value="definition">Definition</option></select> : null}
      </div>

      <section className="qt2-section qt2-operations-body">
        {tab === 'runs' ? <RunsTable pageModel={pageModel} loading={runInventory.loading} onPageChange={setPage} /> : null}
        {tab === 'collectors' ? <CollectorsTable pageModel={pageModel} loading={collectorFeed.loading} onPageChange={setPage} /> : null}
        {tab === 'market-data' ? <MarketDataTable pageModel={pageModel} loading={marketFeed.loading} streamCount={streamRows.length} onPageChange={setPage} /> : null}
        {tab === 'research' ? <ResearchTable pageModel={pageModel} loading={researchLoading} onPageChange={setPage} /> : null}
      </section>
    </div>
  )
}
