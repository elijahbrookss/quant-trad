import { useMemo, useState } from 'react'
import { Link } from 'react-router-dom'
import { fetchResearchOverview, fetchResearchInventory } from '../../adapters/research.adapter.js'
import { useResearchRead } from '../../features/research/useResearchRead.js'
import { ReadState, StudyTable } from '../../features/research/ResearchViews.jsx'
import { ResearchActivity } from '../../features/research/ResearchActivity.jsx'
import { AttentionRail } from '../../features/overview/components/AttentionRail.jsx'
import { rankAttentionItems } from '../../features/overview/buildOverviewViewModel.js'
import { useActiveRunsFeed } from '../../features/operations/useActiveRunsFeed.js'
import { useCollectorsFeed } from '../../features/collectors/useCollectorsFeed.js'
import { OperatorErrorNotice } from '../components/OperatorErrorNotice.jsx'

const coverageLabel = (value, dimension) => dimension === 'procedure' ? value.replaceAll('Unknown', 'Definition not recorded') : value.replaceAll('Unknown', 'Not recorded at Study level')
const DIMENSIONS = { symbol: 'Symbols', timeframe: 'Timeframes', period: 'Historical periods', procedure: 'Linked Check definitions' }
export function OverviewRoom() {
  const summary = useResearchRead(fetchResearchOverview)
  const recent = useResearchRead(fetchResearchInventory, [{ kind: 'study', limit: 4 }])
  const [dimension, setDimension] = useState('symbol')
  const runs = useActiveRunsFeed()
  const collectors = useCollectorsFeed()
  const attention = useMemo(() => rankAttentionItems({ runs: runs.runs, providerSummaries: collectors.providers }), [runs.runs, collectors.providers])
  const coverage = summary.data?.dimensions?.[dimension]
  const maximum = Math.max(1, ...(coverage?.items || []).map(row => row.count))
  const unavailable = runs.error || collectors.error || collectors.streamError
  return <div className="qt2-room qt2-research-room">
    <header className="qt2-room-head"><h1 className="qt2-title">Overview</h1><Link className="qt2-button" to="/studies">Browse studies</Link></header>
    <ReadState read={summary} label="Loading research summaries" />
    <div className="qt2-research-kpis">{[['Studies', 'studies', '/studies'], ['Published summaries', 'published', '/studies?publication_state=published'], ['Studies without summaries', 'unpublished', '/studies?publication_state=unpublished']].map(([label, key, to]) => <Link key={key} className="qt2-research-panel" to={to}><span>{label}</span><strong>{summary.data ? summary.data[key] : '—'}</strong></Link>)}</div>
    <section className="qt2-research-panel qt2-compact-attention"><div><h2>Needs attention</h2><span className="qt2-research-caption">Bot and collector evidence · 72 hours</span></div><div className="qt2-head-actions"><Link to="/bot-runs">Bot runs</Link><Link to="/collectors">Collectors</Link></div>
      {runs.loading || collectors.loading ? <p>Checking current evidence…</p> : attention.length ? <AttentionRail items={attention.slice(0, 3)} /> : <p>{unavailable ? 'Partial evidence; no all-clear established.' : 'No known actionable bot or collector issues.'}</p>}
      {unavailable ? <details><summary>Some evidence sources unavailable</summary>{[runs.error, collectors.error, collectors.streamError].filter(Boolean).map((error, index) => <OperatorErrorNotice key={index} error={error} compact />)}</details> : null}
    </section>
    <div className="qt2-research-overview-grid"><ResearchActivity />
      <section className="qt2-research-panel"><div className="qt2-research-panel-head"><h2>Inventory coverage</h2><select aria-label="Coverage dimension" value={dimension} onChange={event => setDimension(event.target.value)}>{Object.entries(DIMENSIONS).map(([value, label]) => <option key={value} value={value}>{label}</option>)}</select></div>
        {!summary.data ? <p>Coverage unavailable until the summary read succeeds.</p> : !coverage?.items?.length ? <p>No classified Studies.</p> : coverage.items.map(row => <div className="qt2-coverage-row" key={row.label}><span title={coverageLabel(row.label, dimension)}>{coverageLabel(row.label, dimension)}</span><meter min="0" max={maximum} value={row.count} aria-label={row.label + ': ' + row.count + ' saved Studies'} /><strong>{row.count}</strong></div>)}
        {coverage?.other ? <p className="qt2-research-caption">Other: {coverage.other}</p> : null}{coverage?.unknown_studies ? <p className="qt2-research-caption">No directly linked Check definition: {coverage.unknown_studies}</p> : null}{coverage?.other_memberships ? <p className="qt2-research-caption">Other definition memberships: {coverage.other_memberships}</p> : null}
        <p className="qt2-research-caption">Saved Study counts. {dimension === 'procedure' ? 'A Study can belong to several linked definitions; only direct Check links are counted.' : dimension === 'period' ? 'Stored windows; continuous source coverage is not established.' : 'Root metadata only; values not recorded at Study level remain separate.'} Independent evidence is not established by inventory counts.</p>
      </section></div>
    <div className="qt2-research-panel-head"><h2>Recent studies</h2><Link to="/studies">View all</Link></div><ReadState read={recent} label="Loading recent Study summaries" />{recent.data ? <section className="qt2-research-panel qt2-table-panel">{recent.data.items.length ? <StudyTable items={recent.data.items} from="/overview" /> : <p>No saved Studies.</p>}</section> : null}
  </div>
}
