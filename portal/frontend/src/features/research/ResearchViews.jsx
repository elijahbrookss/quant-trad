import { Link } from 'react-router-dom'
import { OperatorErrorNotice, OperatorSkeleton } from '../../v2/components/OperatorErrorNotice.jsx'

export function ReadState({ read, label = 'Loading evidence' }) {
  if (read.loading) return <div><OperatorSkeleton rows={3} label={label} /><button className="qt2-button" onClick={read.cancel}>Cancel load</button></div>
  if (read.cancelled) return <p role="status">Page load cancelled. Research jobs continue. <button className="qt2-button" onClick={read.refresh}>Load again</button></p>
  if (read.error) return <div><OperatorErrorNotice error={read.error.message} /><button className="qt2-button" onClick={read.refresh}>Retry section</button></div>
  return null
}
export function StudyTable({ items = [], from = '/studies' }) {
  return <div className="qt2-table-wrap"><table className="qt2-data-table qt2-study-table"><thead><tr><th>Study / record</th><th>Scope</th><th>Interpretation</th><th>Updated</th></tr></thead><tbody>{items.map(item => <tr key={item.id}>
    <td><Link to={'/studies/' + encodeURIComponent(item.id)} state={{ from }}><strong>{item.title}</strong><small>{item.kind === 'study' ? item.status : item.kind + ' · ' + item.status}</small></Link></td>
    <td>{[item.symbol, item.timeframe].filter(Boolean).join(' · ') || 'Unknown'}<small>{[item.window_start, item.window_end].filter(Boolean).join(' → ') || 'Period unknown'}</small></td>
    <td>{item.kind !== 'study' ? 'Evidence record' : Number(item.publication_count) > 0 ? `Published · v${item.latest_revision}` : 'Unpublished'}</td>
    <td>{item.updated_at ? new Date(item.updated_at).toLocaleDateString() : 'Unknown'}</td>
  </tr>)}</tbody></table></div>
}
