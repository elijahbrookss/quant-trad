import { useEffect, useMemo } from 'react'
import { ChartStateProvider } from './contexts/ChartStateContext'
import { ChartComponent } from './components/ChartComponent/ChartComponent'
import { TabManager } from './components/TabManager'
import { createLogger } from './utils/logger.js'
import { useFeedStatus } from './hooks/useFeedStatus.js'

const STATUS_THEME = {
  checking: {
    base: 'border-zinc-300 bg-zinc-100 text-zinc-500',
    dot: 'bg-zinc-400',
  },
  online: {
    base: 'border-emerald-200 bg-emerald-50 text-emerald-700',
    dot: 'bg-emerald-500',
  },
  degraded: {
    base: 'border-amber-200 bg-amber-50 text-amber-700',
    dot: 'bg-amber-500',
  },
  offline: {
    base: 'border-rose-200 bg-rose-50 text-rose-700',
    dot: 'bg-rose-500',
  },
}

const DEFAULT_THEME = STATUS_THEME.checking

const StatusBadge = ({ label, status, detail }) => {
  const theme = STATUS_THEME[status] ?? DEFAULT_THEME
  return (
    <span
      className={`inline-flex items-center gap-2 rounded-full border px-3 py-1 text-xs font-medium transition ${theme.base}`}
    >
      <span className={`h-1.5 w-1.5 rounded-full ${theme.dot}`} aria-hidden />
      <span>{label}</span>
      {detail ? <span className="text-[11px] font-normal text-zinc-500">{detail}</span> : null}
    </span>
  )
}

export default function App() {
  const chartId = 'main'
  const { info } = useMemo(() => createLogger('App', { chartId }), [chartId])
  const { statuses } = useFeedStatus({ refreshMs: 60000 })

  useEffect(() => {
    info('app_mounted')
  }, [info])

  return (
    <ChartStateProvider>
      <div className="min-h-screen bg-zinc-100 text-zinc-900">
        <header className="border-b border-zinc-200 bg-white/95 backdrop-blur">
          <div className="mx-auto flex w-full max-w-6xl flex-col gap-6 px-6 py-8">
            <div className="flex flex-wrap items-center justify-between gap-6">
              <div className="flex items-center gap-4">
                <span className="flex h-11 w-11 items-center justify-center rounded-2xl border border-zinc-200 bg-white text-sm font-semibold uppercase tracking-[0.35em] text-zinc-600">
                  QT
                </span>
                <div className="flex flex-col">
                  <span className="text-xs font-medium uppercase tracking-[0.35em] text-zinc-400">QuantTrad Portal</span>
                  <h1 className="text-3xl font-semibold text-zinc-900 sm:text-4xl">QuantLab</h1>
                </div>
              </div>
              <button
                type="button"
                onClick={() => window.dispatchEvent(new CustomEvent('qt-open-symbol-palette'))}
                className="inline-flex items-center gap-2 rounded-full border border-zinc-300 bg-white px-4 py-2 text-sm font-medium text-zinc-600 shadow-sm transition hover:border-zinc-400 hover:text-zinc-800 focus-visible:outline focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-zinc-400"
              >
                <span className="text-zinc-500">Search symbols</span>
                <kbd className="rounded border border-zinc-200 bg-zinc-50 px-1.5 py-0.5 text-[11px] font-semibold text-zinc-500">/</kbd>
              </button>
            </div>
            <div className="flex flex-wrap items-center gap-2 text-xs text-zinc-500">
              {statuses.map((s) => (
                <StatusBadge key={s.key} label={s.label} status={s.status} detail={s.detail} />
              ))}
            </div>
          </div>
        </header>

        <main className="mx-auto flex w-full max-w-6xl flex-1 flex-col gap-8 px-6 py-10 lg:grid lg:grid-cols-[2fr,1fr]">
          <section className="flex flex-col">
            <ChartComponent chartId={chartId} />
          </section>
          <aside className="flex flex-col">
            <TabManager chartId={chartId} />
          </aside>
        </main>
      </div>
    </ChartStateProvider>
  )
}
