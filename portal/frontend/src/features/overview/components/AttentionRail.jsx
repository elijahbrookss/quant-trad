import { Link } from 'react-router-dom'

const SEVERITY_CLASS = {
  critical: 'qt2-attention-critical',
  warning: 'qt2-attention-warning',
  info: 'qt2-attention-info',
}

const SEVERITY_LABEL = {
  critical: 'Critical',
  warning: 'Warning',
  info: 'Info',
}

function formatEvidenceTime(value) {
  if (!value) return 'time unavailable'
  const parsed = new Date(value)
  return Number.isNaN(parsed.getTime()) ? 'time unavailable' : parsed.toLocaleString()
}

export function AttentionRail({ items, lookbackHours = 72 }) {
  if (!items.length) {
    return (
      <div className="qt2-attention-empty">
        <span className="qt2-status-dot" />
        No known actionable issues in the current {lookbackHours}-hour evidence window.
      </div>
    )
  }

  return (
    <div className="qt2-attention-rail">
      {items.map((item) => (
        <Link
          key={item.id}
          to={item.href}
          state={item.state}
          className={'qt2-attention-item ' + (SEVERITY_CLASS[item.severity] || SEVERITY_CLASS.info)}
        >
          <span className="qt2-attention-severity">{SEVERITY_LABEL[item.severity] || 'Info'}</span>
          <span className="qt2-attention-title">{item.title}</span>
          <span className="qt2-attention-detail">{item.detail}</span>
          <time className="qt2-attention-time" dateTime={item.evidenceAt || undefined}>
            {formatEvidenceTime(item.evidenceAt)}
          </time>
        </Link>
      ))}
    </div>
  )
}
