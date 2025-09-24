import { useMemo } from 'react'
import { useChartValue } from '../contexts/ChartStateContext.jsx'

const formatDate = (iso) => {
  if (!iso) return '—'
  try {
    return new Intl.DateTimeFormat(undefined, {
      hour: '2-digit',
      minute: '2-digit',
      second: '2-digit',
      hour12: false,
    }).format(new Date(iso))
  } catch {
    return '—'
  }
}

export function QuantLabSummary({ chartId }) {
  const chart = useChartValue(chartId) || {}
  const { symbol = 'CL', interval = '15m', lastUpdatedAt, connectionStatus, connectionMessage } = chart

  const status = useMemo(() => {
    const label = connectionStatus?.toUpperCase?.() || 'IDLE'
    let tone = 'text-slate-300'
    let badge = 'bg-slate-800/80 border border-slate-700'
    if (connectionStatus === 'online') {
      tone = 'text-emerald-300'
      badge = 'bg-emerald-500/10 border border-emerald-400/40'
    } else if (connectionStatus === 'connecting' || connectionStatus === 'recovering') {
      tone = 'text-amber-300'
      badge = 'bg-amber-500/10 border border-amber-400/40'
    } else if (connectionStatus === 'error') {
      tone = 'text-rose-300'
      badge = 'bg-rose-500/10 border border-rose-400/40'
    }

    return {
      label,
      tone,
      badge,
      message: connectionMessage,
    }
  }, [connectionStatus, connectionMessage])

  return (
    <div className="grid gap-4 md:grid-cols-3">
      <SummaryCard
        title="Active Symbol"
        value={symbol}
        hint="Synced with global presets (/ shortcut)"
      />
      <SummaryCard
        title="Interval"
        value={interval}
        hint="Timeframe linked to chart controls"
      />
      <SummaryCard
        title="QuantLab Feed"
        value={status.label}
        valueClassName={`${status.tone}`}
        badgeClassName={status.badge}
        hint={status.message || `Last refresh at ${formatDate(lastUpdatedAt)}`}
      />
    </div>
  )
}

function SummaryCard({ title, value, hint, valueClassName = 'text-slate-100', badgeClassName = 'bg-slate-900/60 border border-slate-800/60' }) {
  return (
    <div className={`rounded-2xl ${badgeClassName} px-4 py-5 shadow-lg shadow-black/20 transition`}> 
      <span className="text-[11px] uppercase tracking-[0.3em] text-slate-400">{title}</span>
      <div className={`mt-3 text-2xl font-semibold ${valueClassName}`}>{value}</div>
      <p className="mt-2 text-xs text-slate-400">{hint}</p>
    </div>
  )
}
