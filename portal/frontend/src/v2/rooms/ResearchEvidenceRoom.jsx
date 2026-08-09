import { useEffect, useState } from 'react'
import { Link, useLocation, useNavigate, useParams } from 'react-router-dom'
import { RefreshCcw, X } from 'lucide-react'
import { fetchResearchItem, fetchResearchTrail } from '../../adapters/research.adapter.js'

function safeOrigin(value) {
  if (value === '/overview') return value
  if (String(value || '').startsWith('/operations')) return value
  return '/operations?tab=research'
}

function Readout({ label, value }) {
  return <div className="qt2-readout"><span>{label}</span><strong>{value ?? 'Unavailable'}</strong></div>
}

function ItemLink({ item }) {
  return (
    <Link className="qt2-related-card" to={'/operations/research/' + item.id} state={{ item, from: '/operations?tab=research' }}>
      <span className="qt2-kind-label">{item.kind}</span>
      <strong>{item.title || 'Untitled evidence'}</strong>
      <small>{[item.status, item.symbol, item.timeframe].filter(Boolean).join(' · ')}</small>
    </Link>
  )
}

export function ResearchEvidenceRoom() {
  const { itemId } = useParams()
  const location = useLocation()
  const navigate = useNavigate()
  const [item, setItem] = useState(() => location.state?.item?.id === itemId ? location.state.item : null)
  const [trail, setTrail] = useState(null)
  const [loading, setLoading] = useState(true)
  const [errors, setErrors] = useState([])
  const [revision, setRevision] = useState(0)

  useEffect(() => {
    let mounted = true
    setLoading(true)
    Promise.allSettled([fetchResearchItem(itemId), fetchResearchTrail(itemId)])
      .then(([itemResult, trailResult]) => {
        if (!mounted) return
        const nextErrors = []
        if (itemResult.status === 'fulfilled') setItem(itemResult.value)
        else nextErrors.push(itemResult.reason?.message || 'Research item unavailable')
        if (trailResult.status === 'fulfilled') setTrail(trailResult.value)
        else nextErrors.push(trailResult.reason?.message || 'Research trail unavailable')
        setErrors(nextErrors)
      })
      .finally(() => {
        if (mounted) setLoading(false)
      })
    return () => { mounted = false }
  }, [itemId, revision])

  const from = safeOrigin(location.state?.from)
  if (loading && !item) return <div className="qt2-room"><div className="qt2-empty">Loading research evidence…</div></div>
  if (!item) return <div className="qt2-room"><div className="qt2-error">{errors.join(' · ') || 'Research evidence not found.'}</div></div>

  const relatedItems = trail?.related_items || []
  const runs = trail?.runs || []

  return (
    <div className="qt2-lens-page">
      <header className="qt2-lens-page-head">
        <div>
          <span className="qt2-kicker">Research evidence · read only</span>
          <h1 className="qt2-title">{item.title || 'Untitled evidence'}</h1>
          <p className="qt2-sub qt-mono">{item.id}</p>
        </div>
        <div className="qt2-head-actions">
          <button className="qt2-button" type="button" onClick={() => setRevision((value) => value + 1)}><RefreshCcw size={14} />Refresh</button>
          <button className="qt2-button" type="button" onClick={() => navigate(from)}><X size={14} />Exit evidence</button>
        </div>
      </header>

      {errors.map((error) => <div className="qt2-error" key={error}>{error}</div>)}

      <div className="qt2-evidence-layout">
        <section className="qt2-stat-card">
          <div className="qt2-card-heading-row"><span className="qt2-kicker">Canonical identity</span><span className="qt2-evidence-state is-info">{item.status}</span></div>
          <div className="qt2-readout-grid">
            <Readout label="Kind" value={item.kind} />
            <Readout label="Status" value={item.status} />
            <Readout label="Instrument" value={item.instrument_id || item.symbol} />
            <Readout label="Timeframe" value={item.timeframe} />
            <Readout label="Datasource" value={item.datasource} />
            <Readout label="Exchange" value={item.exchange} />
            <Readout label="Window start" value={item.window_start} />
            <Readout label="Window end" value={item.window_end} />
            <Readout label="Persisted" value={item.created_at} />
            <Readout label="Source revision" value={item.source_revision} />
          </div>
          {item.body ? <p className="qt2-evidence-body">{item.body}</p> : <p className="qt2-empty">Narrative body unavailable.</p>}
        </section>

        <section className="qt2-stat-card">
          <div className="qt2-card-heading-row"><span className="qt2-kicker">Trace relationships</span><span className="qt2-count">{trail?.summary?.link_count ?? '—'}</span></div>
          {!trail ? <div className="qt2-empty">Relationship projection unavailable.</div> : null}
          {trail && !relatedItems.length && !runs.length ? <div className="qt2-empty">No persisted related-item or run links.</div> : null}
          <div className="qt2-related-grid">
            {relatedItems.map((related) => <ItemLink key={related.id} item={related} />)}
            {runs.map((run) => (
              <Link key={run.run_id} className="qt2-related-card" to={'/operations/runs/' + run.run_id} state={{ run, from: '/operations/research/' + item.id }}>
                <span className="qt2-kind-label">run</span>
                <strong>{run.strategy_name || run.bot_name || run.run_id}</strong>
                <small>{[run.status, run.run_type, run.timeframe].filter(Boolean).join(' · ')}</small>
              </Link>
            ))}
          </div>
        </section>

        <section className="qt2-stat-card qt2-raw-evidence">
          <div className="qt2-card-heading-row"><span className="qt2-kicker">Persisted payload</span><span className="qt2-muted">Provider-free evidence</span></div>
          <pre>{JSON.stringify(item.payload || {}, null, 2)}</pre>
        </section>
      </div>
    </div>
  )
}
