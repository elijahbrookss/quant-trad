import { useEffect, useState } from 'react'
import { Link, useLocation, useSearchParams } from 'react-router-dom'
import { fetchResearchInventory } from '../../adapters/research.adapter.js'
import { useResearchRead } from '../../features/research/useResearchRead.js'
import { ReadState, StudyTable } from '../../features/research/ResearchViews.jsx'

export function StudiesRoom() {
  const [params, setParams] = useSearchParams()
  const location = useLocation()
  const [query, setQuery] = useState(params.get('q') || '')
  useEffect(() => setQuery(params.get('q') || ''), [params])
  const kind = params.get('kind') || 'study'
  const filters = Object.fromEntries(['q', 'kind', 'status', 'symbol', 'timeframe', 'publication_state', 'cursor'].filter(key => params.get(key)).map(key => [key, params.get(key)]))
  const read = useResearchRead(fetchResearchInventory, [{ ...filters, kind, limit: 25 }])
  function update(key, value) {
    const next = new URLSearchParams(params)
    next.delete('cursor')
    if (value) next.set(key, value); else next.delete(key)
    setParams(next)
  }
  function nextPage() {
    const next = new URLSearchParams(params)
    next.set('cursor', read.data.next_cursor)
    setParams(next)
  }
  return <div className="qt2-room qt2-research-room">
    <header className="qt2-room-head"><div><h1 className="qt2-title">{kind === 'study' ? 'Studies' : 'All research records'}</h1></div><div className="qt2-head-actions"><Link className="qt2-button" to={kind === 'study' ? '/studies?kind=all' : '/studies'}>{kind === 'study' ? 'All research records' : 'Studies'}</Link><button className="qt2-button" onClick={read.refresh}>Refresh</button></div></header>
    <form className="qt2-study-filters" onSubmit={event => { event.preventDefault(); update('q', query.trim()) }}>
      <input aria-label="Search research" maxLength={200} value={query} onChange={event => setQuery(event.target.value)} placeholder="Search title, recorded symbol or ID" />
      <button className="qt2-button" type="submit">Search</button>
      {kind !== 'study' ? <select aria-label="Record kind" value={kind} onChange={event => update('kind', event.target.value)}><option value="all">All records</option><option value="research_check">Checks</option><option value="observation">Observations</option><option value="hypothesis">Hypotheses</option><option value="study">Studies</option></select> : <select aria-label="Summary state" value={params.get('publication_state') || ''} onChange={event => update('publication_state', event.target.value)}><option value="">All summaries</option><option value="published">Published summaries</option><option value="unpublished">No published summary</option></select>}
      <input aria-label="Recorded symbol" placeholder="Recorded symbol" maxLength={64} defaultValue={params.get('symbol') || ''} key={'symbol:' + params.get('symbol')} onBlur={event => { if (event.target.value !== (params.get('symbol') || '')) update('symbol', event.target.value) }} />
      <input aria-label="Timeframe" placeholder="Timeframe" maxLength={64} defaultValue={params.get('timeframe') || ''} key={'timeframe:' + params.get('timeframe')} onBlur={event => { if (event.target.value !== (params.get('timeframe') || '')) update('timeframe', event.target.value) }} />
    </form>
    <ReadState read={read} label="Loading compact research inventory" />
    {read.data ? <section className="qt2-research-panel">
      {read.data.items.length ? <StudyTable items={read.data.items} from={location.pathname + location.search} /> : <p className="qt2-empty">No records match these filters.</p>}
      <div className="qt2-pagination"><span>{read.data.items.length} shown · {read.data.total} matching records</span><div>{params.has('cursor') ? <button className="qt2-button" onClick={() => update('cursor', '')}>First page</button> : null}<button className="qt2-button" disabled={!read.data.next_cursor} onClick={nextPage}>Next page</button></div></div>
    </section> : null}
    <p className="qt2-research-caption">Record status describes the saved record. Research decisions are in published summaries; running work is shown separately.</p>
    <p className="qt2-research-caption">Saved records are not independent scientific replications. Scope shown here is recorded metadata; linked evidence may contain additional scope.</p>
  </div>
}
