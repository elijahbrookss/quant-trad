import { useEffect, useState } from 'react'
import { useSearchParams } from 'react-router-dom'
import { fetchActivityRange } from '../../adapters/research.adapter.js'
import { useResearchRead } from './useResearchRead.js'
import { ReadState } from './ResearchViews.jsx'

const TYPES = { checks_completed: 'Checks saved after evaluation', backtests_completed: 'Completed backtests', hypotheses_created: 'Hypotheses created', observations_recorded: 'Observations recorded' }
const PRESETS = { '5m': ['Last 5 minutes', 300000], '2h': ['Last 2 hours', 7200000], '24h': ['Last 24 hours', 86400000], '7d': ['Last 7 days', 604800000] }
export function activityWindow(preset, now = Date.now()) { return { from: new Date(now - PRESETS[preset][1]).toISOString(), to: new Date(now).toISOString() } }
function utcInput(value) { const time = Date.parse(value); return Number.isFinite(time) ? new Date(time).toISOString().slice(0, 16) : '' }
export function parseUTC(value) { if (!/^\d{4}-\d\d-\d\dT\d\d:\d\d$/.test(value)) return NaN; const parsed = Date.parse(value + ':00Z'); return Number.isFinite(parsed) && new Date(parsed).toISOString().slice(0, 16) === value ? parsed : NaN }
export function ResearchActivity() {
  const [params, setParams] = useSearchParams()
  const type = TYPES[params.get('activity')] ? params.get('activity') : 'checks_completed'
  const [initial] = useState(() => activityWindow('24h'))
  const from = params.get('from') || initial.from, to = params.get('to') || initial.to
  const requestedPreset = params.get('range') || '24h'
  const activePreset = PRESETS[requestedPreset] && Date.parse(to) - Date.parse(from) === PRESETS[requestedPreset][1] ? requestedPreset : 'custom'
  useEffect(() => {
    if (!params.has('from') || !params.has('to')) {
      const next = new URLSearchParams(params); next.set('from', from); next.set('to', to); next.set('range', activePreset); setParams(next, { replace: true })
    }
  }, [from, to, params, activePreset, setParams])
  const [preset, setPreset] = useState(activePreset)
  const [start, setStart] = useState(utcInput(from)), [end, setEnd] = useState(utcInput(to))
  const [error, setError] = useState('')
  useEffect(() => { setPreset(activePreset); setStart(utcInput(from)); setEnd(utcInput(to)); setError('') }, [from, to, activePreset])
  const read = useResearchRead(fetchActivityRange, [type, { from, to }])
  function apply(event) {
    event?.preventDefault(); let range
    if (preset === 'custom') {
      const a = parseUTC(start), b = parseUTC(end)
      if (!Number.isFinite(a) || !Number.isFinite(b) || b <= a || b - a > 366 * 86400000) { setError('Choose a valid UTC start before end, at most 366 days apart.'); return }
      range = { from: new Date(a).toISOString(), to: new Date(b).toISOString() }
    } else range = activityWindow(preset)
    const next = new URLSearchParams(params); next.set('activity', type); next.set('range', preset); next.set('from', range.from); next.set('to', range.to); setParams(next); setError('')
  }
  function selectType(value) { const next = new URLSearchParams(params); next.set('activity', value); next.set('from', from); next.set('to', to); setParams(next) }
  const data = read.data
  return <section className="qt2-research-panel qt2-activity"><h2>Research activity</h2>
    <form onSubmit={apply} className="qt2-activity-controls">
      <label>Activity<select aria-label="Activity type" value={type} onChange={e => selectType(e.target.value)}>{Object.entries(TYPES).map(([key, label]) => <option key={key} value={key}>{label}</option>)}</select></label>
      <label>Time range<select aria-label="Activity time range" value={preset} onChange={e => setPreset(e.target.value)}>{Object.entries(PRESETS).map(([key, [label]]) => <option key={key} value={key}>{label}</option>)}<option value="custom">Custom range</option></select></label>
      {preset === 'custom' && <><label>From · UTC<input aria-label="Activity from UTC" type="datetime-local" value={start} onChange={e => setStart(e.target.value)} required /></label><label>To · UTC (exclusive)<input aria-label="Activity to UTC" type="datetime-local" value={end} onChange={e => setEnd(e.target.value)} required /></label></>}
      <button className="qt2-button" type="submit">{preset === activePreset && preset !== 'custom' ? 'Refresh range' : 'Apply range'}</button>
    </form>{error && <p role="alert">{error}</p>}
    <p className="qt2-research-caption">UTC · {from} → {to} · End exclusive. Relative ranges are anchored when applied.</p>
    <ReadState read={read} label="Loading research activity" />
    {data && <><p className="qt2-activity-total"><strong>{data.total.toLocaleString()}</strong> {TYPES[type].toLowerCase()} in this range</p>
      <p>{data.description} Counts cover all matching saved records, not just a page.</p>
      {data.total === 0 ? <p>No matching activity in this range.</p> : <details open={data.buckets.length <= 24}><summary>Counts by {data.bucket_unit} · UTC</summary><div className="qt2-activity-buckets">{data.buckets.map(row => <div key={row.at}><time dateTime={row.at}>{new Date(row.at).toLocaleString('en-GB', { timeZone: 'UTC', month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit', hour12: false })}</time><meter min="0" max={Math.max(1, ...data.buckets.map(x => x.total))} value={row.total} /><span>{row.total}</span></div>)}</div></details>}
      <p className="qt2-research-caption">{data.bucket_unit} buckets; first and last may be partial. Activity counts do not establish independent scientific evidence.</p></>}
  </section>
}
