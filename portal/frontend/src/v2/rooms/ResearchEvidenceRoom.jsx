import { useEffect, useRef, useState } from 'react'
import { Link, useLocation, useParams, useSearchParams } from 'react-router-dom'
import { fetchResearchSummary, fetchResearchRelationships, fetchResearchImages, fetchResearchRecord, fetchResearchComparison,
  fetchResearchExportPreflight, researchImageContentUrl, researchRecordUrl, downloadResearchExport } from '../../adapters/research.adapter.js'
import { useResearchRead } from '../../features/research/useResearchRead.js'
import { ResearchText, ResearchScope, instrumentEntries } from '../../features/research/ResearchText.jsx'
import { ReadState } from '../../features/research/ResearchViews.jsx'

function origin(value) {
  return typeof value === 'string' && /^\/(studies(?:[/?]|$)|overview$|operations(?:[/?]|$))/.test(value) ? value : '/studies'
}
function EvidenceImage({ itemId, image }) {
  const [failed, setFailed] = useState(false)
  const url = researchImageContentUrl(itemId, image.image_id)
  return <figure className="qt2-evidence-image"><div>{failed ? <p>Image unavailable. Reopen Files to retry.</p> : <a href={url} target="_blank" rel="noreferrer"><img src={url} alt={image.alt_text} loading="lazy" onError={() => setFailed(true)} /></a>}</div><figcaption><h3>{image.title}</h3><p className="qt2-evidence-body">{image.explanation}</p><p>{image.binding.kind === 'study' ? `Study summary ${image.binding.revision}` : 'Exact Check evidence'} · Presentation only</p><details><summary>Source binding and image identity</summary><pre>{JSON.stringify({ image_id: image.image_id, sha256: image.sha256, binding: image.binding }, null, 2)}</pre></details></figcaption></figure>
}
function RecordLink({ id, title, kind, from }) {
  return <Link className="qt2-related-card" to={'/studies/' + encodeURIComponent(id)} state={{ from }}><span className="qt2-kind-label">{kind}</span><strong>{title || id}</strong></Link>
}
export function ResearchEvidenceRoom() {
  const { itemId } = useParams()
  const location = useLocation()
  const [params, setParams] = useSearchParams()
  const selectedRevision = params.get('revision') ? Number(params.get('revision')) : undefined
  const summary = useResearchRead(fetchResearchSummary, [itemId, { revision: selectedRevision }])
  const requestedTab = params.get('tab') === 'summary' ? 'interpretation' : params.get('tab')
  const tab = ['interpretation', 'evidence', 'files', 'history', 'export'].includes(requestedTab) ? requestedTab : 'interpretation'
  const item = summary.data?.item
  const publication = summary.data?.publication
  const latestRevision = Math.max(0, ...(summary.data?.history || []).map(version => Number(version.revision) || 0))
  const historical = Boolean(publication && Number(publication.revision) < latestRevision)
  const relations = useResearchRead(fetchResearchRelationships, [itemId], tab === 'evidence')
  const images = useResearchRead(fetchResearchImages, [itemId], tab === 'files')
  const [compareSelection, setCompareSelection] = useState(['', ''])
  const [compareIds, setCompareIds] = useState([])
  const comparison = useResearchRead(fetchResearchComparison, compareIds, tab === 'evidence' && compareIds.length === 2)
  const checkIds = [...new Set([...(publication?.references || []).filter(ref => ref.kind === 'research_check').map(ref => ref.item_id), ...(relations.data?.items || []).filter(row => row.kind === 'research_check').map(row => row.id)])]
  const [rawOpen, setRawOpen] = useState(false)
  const raw = useResearchRead(fetchResearchRecord, [itemId], rawOpen && tab === 'evidence')
  const exportParams = { revision: publication?.revision, publication_hash: publication?.publication_hash }
  const preflight = useResearchRead(fetchResearchExportPreflight, [itemId, exportParams], tab === 'export' && Boolean(summary.data))
  const downloadRef = useRef(null)
  const [download, setDownload] = useState({ busy: false, error: null, cancelled: false })
  const publicationHash = publication?.publication_hash
  const previousView = useRef({ itemId, selectedRevision, publicationHash, tab, settled: false })
  useEffect(() => {
    const previous = previousView.current
    const sameView = previous.itemId === itemId && previous.selectedRevision === selectedRevision && previous.tab === tab
    const refreshStarted = sameView && previous.settled && summary.loading
    const changed = !sameView || refreshStarted || (previous.publicationHash && !summary.loading && previous.publicationHash !== publicationHash)
    previousView.current = { itemId, selectedRevision, tab, settled: (sameView && previous.settled) || Boolean(summary.data), publicationHash: sameView && summary.loading ? previous.publicationHash : publicationHash }
    downloadRef.current?.abort()
    downloadRef.current = null
    if (changed) {
      setCompareIds([])
      setCompareSelection(['', ''])
      setRawOpen(false)
    }
    setDownload({ busy: false, error: null, cancelled: false })
    return () => { downloadRef.current?.abort(); downloadRef.current = null }
  }, [itemId, selectedRevision, publicationHash, tab, summary.loading])
  function selectTab(next) { const query = new URLSearchParams(params); query.set('tab', next === 'interpretation' ? 'summary' : next); setParams(query) }
  function selectRevision(value) { const query = new URLSearchParams(params); if (value) query.set('revision', value); else query.delete('revision'); setParams(query) }
  async function downloadBundle() {
    const controller = new AbortController()
    downloadRef.current?.abort()
    downloadRef.current = controller
    setDownload({ busy: true, error: null, cancelled: false })
    try {
      const blob = await downloadResearchExport(itemId, exportParams, { signal: controller.signal })
      if (controller.signal.aborted) return
      const url = URL.createObjectURL(blob)
      const anchor = document.createElement('a'); anchor.href = url; anchor.download = 'research-evidence.zip'; anchor.click()
      setTimeout(() => URL.revokeObjectURL(url), 1000)
      setDownload({ busy: false, error: null, cancelled: false })
    } catch (error) {
      if (downloadRef.current === controller) setDownload({ busy: false, error: error.name === 'AbortError' ? null : error.message, cancelled: error.name === 'AbortError' })
    }
  }
  const from = origin(location.state?.from)
  const self = location.pathname + location.search
  if (!item) return <div className="qt2-room qt2-research-room"><Link to={from}>Back to research</Link><ReadState read={summary} label="Loading Study summary" /></div>
  const scopeRows = summary.data.scope ? summary.data.scope.items || [] : [item]
  const scopeNames = instrumentEntries(scopeRows).map(row => row.label)
  const scopeFrames = [...new Set(scopeRows.map(row => row.timeframe).filter(Boolean))]
  const displayedImages = (images.data?.items || []).filter(image => image.binding?.kind !== 'study' || image.binding.publication_hash === publication?.publication_hash)
  return <div className="qt2-room qt2-research-room">
    <Link className="qt2-research-back" to={from}>← {from === '/overview' ? 'Overview' : 'Studies / records'}</Link>
    <header className="qt2-room-head"><div><span className="qt2-kicker">{item.kind === 'study' ? 'Study' : item.kind} · read only</span><h1 className="qt2-title">{item.title}</h1><p className="qt2-sub">{[scopeNames.join(' · '), scopeFrames.join(' · ')].filter(Boolean).join(' · ') || 'Instrument not declared at this level'}</p></div><div className="qt2-head-actions"><button className="qt2-button" onClick={summary.refresh}>Refresh summary</button><button className="qt2-button" onClick={() => selectTab('export')}>Export</button></div></header>
    <nav className="qt2-research-tabs" aria-label="Research record sections">{['interpretation', 'evidence', 'files', 'history'].map(value => <button className={tab === value ? 'is-active' : ''} key={value} onClick={() => selectTab(value)}>{value === 'interpretation' ? 'Summary' : value[0].toUpperCase() + value.slice(1)}</button>)}</nav>
    {summary.data.history.length ? <label className="qt2-revision-picker">Summary version <select aria-label="Summary revision" value={publication?.revision || ''} onChange={event => selectRevision(event.target.value)}>{summary.data.history.map(version => <option key={version.revision} value={version.revision}>Version {version.revision}</option>)}</select></label> : null}
    {publication ? <p className="qt2-research-caption">{historical ? `Historical summary · v${publication.revision} · latest is v${latestRevision}` : `Latest summary · v${publication.revision}`}{historical ? <> <button className="qt2-button" onClick={() => selectRevision('')}>View latest</button></> : null}</p> : null}
    {tab === 'interpretation' ? <div className="qt2-study-layout"><section className="qt2-research-panel qt2-interpretation">
      {publication ? <><h2>{historical ? 'Historical summary' : 'Latest published summary'}</h2><ResearchText text={publication.conclusion} /><h2>Scope</h2><ResearchText text={publication.scope} /><section className="qt2-readable-limits"><h2>Limitations</h2><ResearchText text={publication.limitations} /></section><p className="qt2-research-caption">Saved summary, not scientific certification.</p><details><summary>Publication details</summary><p>Scientific validity and actual replay require separate evidence.</p><code>{publication.completion_meaning}</code><p className="qt-mono">{publication.publication_hash}</p></details></> : <><h2>{item.kind === 'study' ? 'No published summary' : ({ research_check: 'Check record', observation: 'Observation', hypothesis: 'Hypothesis' }[item.kind] || 'Research record')}</h2><ResearchText text={item.body || (item.kind === 'study' ? 'No summary has been published for this Study.' : 'No human summary is recorded. Open Evidence for exact retained outputs.')} /></>}
      {summary.data.question ? <details><summary>Research question</summary><ResearchText text={summary.data.question.question} /><ResearchText text={summary.data.question.scope} /><small>A research-memory question, distinct from an executable StudyDefinition.</small></details> : null}
      <div className="qt2-head-actions"><button className="qt2-button" onClick={() => selectTab('evidence')}>Inspect evidence</button><button className="qt2-button" onClick={() => selectTab('files')}>View files</button></div>
    </section><aside><ResearchScope item={item} scope={summary.data.scope} /></aside></div> : null}
    {tab === 'evidence' ? <><section className="qt2-research-panel"><h2>Selected summary references</h2>{publication?.references?.length ? <div className="qt2-related-grid">{publication.references.map((ref, index) => <div key={ref.item_id + ':' + index}><RecordLink id={ref.item_id} kind={ref.kind + ' · ' + ref.role} from={self} /><details><summary>Exact version and hashes</summary><pre>{JSON.stringify(ref, null, 2)}</pre></details></div>)}</div> : <p>No published references. Current links below are not a frozen summary.</p>}</section>
      <section className="qt2-research-panel"><h2>Current relationships</h2><ReadState read={relations} label="Loading compact relationships" />{relations.data ? <>{relations.data.items.length ? <div className="qt2-related-grid">{relations.data.items.map(link => link.id ? <RecordLink key={link.link_id} id={link.id} title={link.title} kind={link.kind + ' · ' + link.relation} from={self} /> : ['run', 'bot_run'].includes(link.target_type) ? <Link key={link.link_id} className="qt2-related-card" to={'/operations/runs/' + encodeURIComponent(link.target_id)} state={{ from: self }}><strong>Bot run</strong><small>{link.target_id}</small></Link> : <p key={link.link_id}>{link.relation} · {link.target_type} · {link.target_id}</p>)}</div> : <p>No saved relationships.</p>}{relations.data.truncated ? <p>First {relations.data.limit} links shown; relationship projection is incomplete.</p> : null}</> : null}</section>
      {checkIds.length >= 2 ? <section className="qt2-research-panel"><details><summary>Compare related Checks</summary><div className="qt2-head-actions">{[0, 1].map(index => <select key={index} aria-label={index === 0 ? 'Left Check' : 'Right Check'} value={compareSelection[index]} onChange={event => setCompareSelection(values => values.map((value, i) => i === index ? event.target.value : value))}><option value="">Choose Check</option>{checkIds.map(id => <option key={id} value={id}>{id}</option>)}</select>)}<button className="qt2-button" disabled={!compareSelection[0] || !compareSelection[1] || compareSelection[0] === compareSelection[1]} onClick={() => setCompareIds([...compareSelection])}>Compare</button></div><ReadState read={comparison} label="Loading Check comparison" />{comparison.data ? <><p className="qt2-research-caption">Inspect each scope, Dataset and population before interpreting deltas. Shared inputs and revisions do not establish independent evidence.</p><pre>{JSON.stringify(comparison.data, null, 2)}</pre></> : null}</details></section> : null}
      <section className="qt2-research-panel"><button className="qt2-button" onClick={() => setRawOpen(value => !value)}>{rawOpen ? 'Hide exact payload' : 'Load exact retained payload'}</button>{rawOpen ? <><ReadState read={raw} label="Loading exact record" />{raw.data ? <pre>{JSON.stringify(raw.data, null, 2)}</pre> : null}</> : null}<p className="qt2-research-caption">Exact payload can be large. It is loaded only on request.</p></section></> : null}
    {tab === 'files' ? <section className="qt2-research-panel"><h2>Files</h2><ReadState read={images} label="Loading image metadata" />{images.data ? displayedImages.length ? displayedImages.map(image => <EvidenceImage key={itemId + ':' + image.image_id} itemId={itemId} image={image} />) : <p>No retained images for this summary.</p> : null}<a className="qt2-button" href={researchRecordUrl(itemId)} download>Download original record JSON</a><p className="qt2-research-caption">Encoded reports remain in their original record JSON. Extracted report-file downloads are not yet supported.</p></section> : null}
    {tab === 'history' ? <section className="qt2-research-panel"><h2>Summary history</h2>{summary.data.history.length ? summary.data.history.map(version => <article className="qt2-history-row" key={version.publication_hash}><button className="qt2-button" onClick={() => { selectRevision(version.revision); const query = new URLSearchParams(params); query.set('revision', version.revision); query.set('tab', 'summary'); setParams(query) }}>Version {version.revision}</button><span>{version.published_at || 'Publication time unknown'}</span><code>{version.publication_hash}</code></article>) : <p>No published summaries.</p>}<p className="qt2-research-caption">Summary versions are not independent replications.</p></section> : null}
    {tab === 'export' ? <section className="qt2-research-panel"><h2>Export review</h2><ReadState read={preflight} label="Reviewing bounded saved-record export" />{preflight.data ? <><p>{preflight.data.file_count} files · {preflight.data.uncompressed_bytes.toLocaleString()} bytes before compression</p><p>Selected summary: {preflight.data.selected_revision || 'None'}</p><p className="qt2-study-limitations">Saved-record verification only. No separate raw-source files are included; raw-input recomputation is unavailable.</p><details><summary>Included files and exclusions</summary><ul>{preflight.data.exclusions.map(note => <li key={note}>{note}</li>)}</ul><pre>{JSON.stringify(preflight.data.files, null, 2)}</pre></details><button className="qt2-button qt2-button-primary" onClick={downloadBundle} disabled={download.busy}>Download evidence ZIP</button>{download.busy ? <button className="qt2-button" onClick={() => downloadRef.current?.abort()}>Cancel download</button> : null}</> : null}{download.error ? <p role="alert">{download.error}</p> : null}{download.cancelled ? <p role="status">Download cancelled. Research jobs continue. Server reads may finish within their deadline.</p> : null}</section> : null}
  </div>
}
