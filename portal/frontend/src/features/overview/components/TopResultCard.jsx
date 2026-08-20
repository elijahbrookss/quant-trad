import { Link } from 'react-router-dom'

function formatPercent(value) {
  if (value === null || value === undefined || !Number.isFinite(Number(value))) return '—'
  return (Number(value) * 100).toFixed(2) + '%'
}

function formatNumber(value, digits = 2) {
  if (value === null || value === undefined || !Number.isFinite(Number(value))) return '—'
  return Number(value).toFixed(digits)
}

function formatPeriod(window) {
  if (!window?.start && !window?.end) return '—'
  const start = window?.start ? new Date(window.start).toLocaleDateString() : '—'
  const end = window?.end ? new Date(window.end).toLocaleDateString() : '—'
  return start + ' → ' + end
}

function datasetLabel(dataset) {
  return dataset?.dataset_id || dataset?.id || dataset?.fingerprint || dataset?.dataset_fingerprint || 'Unavailable'
}

export function TopResultCard({ result, dataset }) {
  if (!result) {
    return (
      <div className="qt2-stat-card">
        <span className="qt2-kicker">Top result by net P&amp;L</span>
        <p className="qt2-empty">No completed backtests are available.</p>
      </div>
    )
  }

  const summary = result.summary || {}

  return (
    <div className="qt2-stat-card">
      <div className="qt2-card-heading-row">
        <span className="qt2-kicker">Top result by net P&amp;L</span>
        <span className="qt2-muted">Completed backtests only</span>
      </div>
      <div className="qt2-stat-card-title">{result.strategy_name || result.bot_name || 'Strategy unavailable'}</div>
      <div className="qt2-fleet-card-sub">{(result.symbols || []).join(', ') || 'Instrument unavailable'} · {result.timeframe || 'Timeframe unavailable'}</div>
      <div className="qt2-stat-grid">
        <div><span className="qt2-kicker">Net P&amp;L</span><div className="qt2-stat-value">{formatNumber(summary.net_pnl)}</div></div>
        <div><span className="qt2-kicker">Return</span><div className="qt2-stat-value">{formatPercent(summary.total_return)}</div></div>
        <div><span className="qt2-kicker">Sharpe</span><div className="qt2-stat-value">{formatNumber(summary.sharpe)}</div></div>
        <div><span className="qt2-kicker">Max drawdown</span><div className="qt2-stat-value">{formatPercent(summary.max_drawdown_pct)}</div></div>
        <div><span className="qt2-kicker">Trades</span><div className="qt2-stat-value">{summary.total_trades ?? '—'}</div></div>
        <div><span className="qt2-kicker">Period</span><div className="qt2-stat-value qt2-stat-value-sm">{formatPeriod(result.simulated_window)}</div></div>
      </div>
      <div className="qt2-evidence-strip">
        <span>Dataset <strong className="qt-mono">{datasetLabel(dataset)}</strong></span>
        <Link to="/operations?tab=runs">Find run evidence</Link>
      </div>
    </div>
  )
}
