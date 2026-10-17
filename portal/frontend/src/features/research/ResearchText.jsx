import { Fragment } from 'react'

// Deliberately small escaped text subset; no HTML, links, scripts or Markdown runtime.
function inline(value) {
  return value.split(/(\*\*[^*\n]+\*\*)/g).map((part, i) => part.startsWith('**') && part.endsWith('**')
    ? <strong key={i}>{part.slice(2, -2)}</strong> : <Fragment key={i}>{part}</Fragment>)
}
export function ResearchText({ text = '' }) {
  const blocks = []; let lines = []; let mode = null
  function flush() { if (lines.length) blocks.push({ mode, lines }); lines = []; mode = null }
  String(text || '').replace(/\r\n/g, '\n').split('\n').forEach(line => {
    if (!line.trim()) { flush(); return }
    const heading = line.match(/^##\s+(.+)$/), bullet = line.match(/^[-*•]\s+(.+)$/)
    if (heading) { flush(); blocks.push({ mode: 'heading', lines: [heading[1]] }); return }
    const next = bullet ? 'list' : 'paragraph'
    if (mode && mode !== next) flush()
    mode = next; lines.push(bullet ? bullet[1] : line)
  }); flush()
  return <div className="qt2-research-text">{blocks.map((block, i) => block.mode === 'heading'
    ? <h3 key={i}>{inline(block.lines[0])}</h3> : block.mode === 'list'
      ? <ul key={i}>{block.lines.map((line, j) => <li key={j}>{inline(line)}</li>)}</ul>
      : <p key={i}>{block.lines.map((line, j) => <Fragment key={j}>{j > 0 && <br />}{inline(line)}</Fragment>)}</p>)}</div>
}
export function instrumentName(item) {
  return item?.instrument?.symbol || item?.symbol || (item?.instrument_id ? 'Instrument unavailable' : '')
}
export function instrumentEntries(rows) {
  const found = new Map()
  rows.forEach(row => { const label = instrumentName(row); if (label) found.set(row.instrument_id || 'symbol:' + label, { key: row.instrument_id || label, label, detail: [row.instrument?.instrument_type, row.instrument?.exchange].filter(Boolean).join(' · ') }) })
  const values = [...found.values()]
  return values.map(value => ({ ...value, label: values.filter(other => other.label === value.label).length > 1 ? value.label + ' (' + (value.detail || 'distinct instrument ID') + ')' : value.label })).sort((a,b) => a.label.localeCompare(b.label) || a.key.localeCompare(b.key))
}
export function ResearchScope({ item, scope }) {
  const rows = scope ? scope.items || [] : [item]
  const names = instrumentEntries(rows)
  const frames = [...new Set(rows.map(row => row.timeframe).filter(Boolean))]
  const windows = [...new Map(rows.filter(row => row.window_start || row.window_end).map(row => [[row.window_start, row.window_end].join('|'), row])).values()]
  const clock = value => value ? new Date(value).toLocaleString('en-GB', { timeZone: 'UTC', year: 'numeric', month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit', hour12: false }) : 'Not recorded'
  return <section className="qt2-research-panel"><h2>{item.kind === 'study' ? 'Study scope' : 'Record scope'}</h2>
    {names.length || frames.length || windows.length ? <><dl className="qt2-study-facts">
      {names.length > 0 && <div><dt>{names.length > 1 ? 'Instruments' : 'Instrument'}</dt><dd>{names.map(entry => entry.label).join(' · ')}</dd></div>}
      {frames.length > 0 && <div><dt>Candles</dt><dd>{frames.join(' · ')}</dd></div>}
      {windows.length > 0 && <div><dt>Period · UTC</dt><dd>{windows.map((row, i) => <p key={i}>{clock(row.window_start)} → {clock(row.window_end)}<small> End exclusive</small></p>)}</dd></div>}
    </dl>{rows.some(row => !row.instrument_id && !row.symbol) && <p>Some instrument metadata is not declared.</p>}</> : <p>Scope metadata is not recorded at {item.kind === 'study' ? 'Study' : 'record'} level. See the summary and linked evidence.</p>}
    {scope?.basis !== 'record_metadata' && scope && <p className="qt2-research-caption">Current metadata of Checks directly referenced by the selected summary; stored result/evidence identities match. {scope.complete ? '' : 'Some referenced Check scope is unavailable.'}</p>}
    {rows.some(row => row.instrument) && <p className="qt2-research-caption">Names from the current instrument catalog; frozen evidence remains authoritative.</p>}
    <details><summary>Technical identity</summary><p className="qt-mono">{item.id}</p><p>{item.source_revision || 'Source revision not recorded'}</p><pre>{JSON.stringify(rows.map(row => ({ check_or_record_id: row.id, instrument_id: row.instrument_id, timeframe: row.timeframe, from: row.window_start, to: row.window_end })), null, 2)}</pre></details>
  </section>
}
