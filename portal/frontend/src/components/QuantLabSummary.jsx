import { useMemo } from 'react'
import { useChartValue } from '../contexts/ChartStateContext.jsx'

const formatTime = (iso) => {
  if (!iso) return null
  try {
    return new Intl.DateTimeFormat(undefined, {
      hour: '2-digit',
      minute: '2-digit',
      second: '2-digit',
      hour12: false,
    }).format(new Date(iso))
  } catch {
    return null
  }
}

export function QuantLabSummary({ chartId }) {
  const chart = useChartValue(chartId) || {}
  const { lastUpdatedAt, connectionStatus, connectionMessage } = chart

  const status = useMemo(() => {
    const base = {
      label: 'Standby',
      badge: 'border-slate-700 bg-slate-900/70 text-slate-200',
      dot: 'bg-slate-500',
    }

    if (connectionStatus === 'online') {
      return {
        label: 'Online',
        badge: 'border-emerald-400/40 bg-emerald-500/15 text-emerald-200',
        dot: 'bg-emerald-400 shadow-[0_0_12px] shadow-emerald-400/70',
      }
    }

    if (connectionStatus === 'connecting' || connectionStatus === 'recovering') {
      return {
        label: 'Syncing',
        badge: 'border-amber-400/40 bg-amber-500/15 text-amber-200',
        dot: 'bg-amber-300 shadow-[0_0_12px] shadow-amber-400/60',
      }
    }

    if (connectionStatus === 'error') {
      return {
        label: 'Alert',
        badge: 'border-rose-500/40 bg-rose-500/15 text-rose-200',
        dot: 'bg-rose-400 shadow-[0_0_12px] shadow-rose-500/60',
      }
    }

    return base
  }, [connectionStatus])

  const refreshCopy = connectionStatus === 'error'
    ? connectionMessage || 'Connection issue detected.'
    : lastUpdatedAt
      ? `Last load at ${formatTime(lastUpdatedAt)}`
      : 'Load data to populate the workspace.'

  return (
    <div className="flex flex-col gap-6 rounded-3xl border border-white/5 bg-black/30 p-6 shadow-[0_30px_70px_-50px_rgba(0,0,0,0.8)] sm:flex-row sm:items-center sm:justify-between">
      <div className="space-y-2">
        <span className="text-[11px] uppercase tracking-[0.35em] text-sky-300/70">QuantLab status</span>
        <p className="max-w-xl text-sm text-slate-400">Monitoring backend connectivity and recent refresh activity for the research canvas.</p>
      </div>
      <div className="flex flex-col items-start gap-2 sm:items-end">
        <span className={`inline-flex items-center gap-2 rounded-full border px-4 py-2 text-[11px] uppercase tracking-[0.35em] ${status.badge}`}>
          <span className={`h-2 w-2 rounded-full ${status.dot}`} />
          {status.label}
        </span>
        <p className="text-xs text-slate-400">{refreshCopy}</p>
      </div>
    </div>
  )
}
