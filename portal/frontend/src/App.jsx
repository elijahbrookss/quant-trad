import { useEffect, useMemo } from 'react'
import { ChartStateProvider } from './contexts/ChartStateContext'
import { ChartComponent } from './components/ChartComponent/ChartComponent'
import { TabManager } from './components/TabManager'
import { createLogger } from './utils/logger.js'
import { useFeedStatus } from './hooks/useFeedStatus.js'

const STATUS_THEME = {
  checking: {
    base: 'border-neutral-700/70 bg-neutral-900/70 text-neutral-400',
    dot: 'bg-neutral-500',
  },
  online: {
    base: 'border-emerald-500/40 bg-emerald-500/15 text-emerald-300',
    dot: 'bg-emerald-400',
  },
  degraded: {
    base: 'border-amber-500/40 bg-amber-500/10 text-amber-300',
    dot: 'bg-amber-400',
  },
  offline: {
    base: 'border-rose-500/40 bg-rose-500/10 text-rose-300',
    dot: 'bg-rose-400',
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
      {detail ? <span className="text-[11px] font-normal text-neutral-500">{detail}</span> : null}
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
      <div className="min-h-screen bg-neutral-950 text-neutral-100">
        <header className="border-b border-neutral-800/70 bg-neutral-950/80 backdrop-blur">
          <div className="mx-auto flex w-full max-w-6xl flex-col gap-6 px-6 py-8">
            <div className="flex flex-wrap items-center justify-between gap-6">
              <div className="flex items-center gap-4">
                <span className="flex h-11 w-11 items-center justify-center rounded-2xl border border-neutral-700/80 bg-neutral-900 text-sm font-semibold uppercase tracking-[0.35em] text-neutral-200">
                  QT
                </span>
                <div className="flex flex-col">
                  <span className="text-xs font-medium uppercase tracking-[0.35em] text-neutral-500">QuantTrad Portal</span>
                  <h1 className="text-3xl font-semibold text-neutral-100 sm:text-4xl">QuantLab</h1>
                </div>
              </div>
              <button
                type="button"
                onClick={() => window.dispatchEvent(new CustomEvent('qt-open-symbol-palette'))}
                className="inline-flex items-center gap-2 rounded-full border border-neutral-700/70 bg-neutral-900 px-4 py-2 text-sm font-medium text-neutral-300 shadow-sm transition hover:border-neutral-500 hover:text-neutral-100 focus-visible:outline focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-neutral-500"
              >
                <span className="text-neutral-400">Search symbols</span>
                <kbd className="rounded border border-neutral-700/60 bg-neutral-900 px-1.5 py-0.5 text-[11px] font-semibold text-neutral-400">/</kbd>
              </button>
            </div>
            <div className="flex flex-wrap items-center gap-2 text-xs text-neutral-500">
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
