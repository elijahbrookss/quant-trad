import { useEffect, useMemo, useState } from 'react'
import { Link, useSearchParams } from 'react-router-dom'
import { Copy, MoreHorizontal, RefreshCcw, Search } from 'lucide-react'
import { listResearchItems } from '../../adapters/research.adapter.js'
import { useFleetBotsFeed } from '../../features/bots/page/useFleetBotsFeed.js'
import { CollectorFleetConsole } from '../../features/collectors/CollectorFleetConsole.jsx'
import { useCollectorsFeed } from '../../features/collectors/useCollectorsFeed.js'
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
  const researchRows = useMemo(() => filterResearchRows(researchItems, { query, status }), [researchItems, query, status])
  const statusOptions = useMemo(() => {
    const source = tab === 'research' ? researchItems.map((item) => item.status) : runRows.map((run) => run.status)
    return [...new Set(source.filter(Boolean))].sort()
  }, [tab, researchItems, runRows])
  const visibleRows = tab === 'research' ? researchRows : []
  const pageModel = paginateRows(visibleRows, page, PAGE_SIZE)
  const runPageModel = paginateRows(runRows, 1, Math.max(PAGE_SIZE, runRows.length))

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
          <span className="qt2-observation-note">{tab === 'runs' ? (runView === 'current' ? (activeRunsFeed.hasReceivedSnapshot ? 'Active run stream live' : 'Active run stream connecting') : (hasReceivedSnapshot ? 'Run history ready' : 'Run history connecting')) : tab === 'market' ? 'Provider summaries live · details lazy' : 'Bounded research inventory'}</span>
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
            {collectorFeed.error ? <OperatorErrorNotice error={collectorFeed.error} compact /> : null}
            {collectorFeed.streamError ? <OperatorErrorNotice error={collectorFeed.streamError} compact /> : null}
            {collectorFeed.loading && !collectorFeed.providers.length
              ? <OperatorSkeleton rows={6} label="Loading canonical collector operations" />
              : <CollectorFleetConsole feed={collectorFeed} query={query} />}
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
