import { useEffect, useMemo } from 'react'
import { ChartStateProvider } from './contexts/ChartStateContext'
import { ChartComponent } from './components/ChartComponent/ChartComponent'
import { TabManager } from './components/TabManager'
import { createLogger } from './utils/logger.js'

const NAV_ITEMS = [
  { key: 'lab', label: 'QuantLab', active: true },
  { key: 'ops', label: 'Ops' },
  { key: 'strategies', label: 'Strategies' },
]

export default function App() {
  const chartId = 'main'
  const { info } = useMemo(() => createLogger('App', { chartId }), [chartId])

  useEffect(() => {
    info('app_mounted')
  }, [info])

  return (
    <ChartStateProvider>
      <div className="min-h-screen bg-slate-950 text-slate-100">
        <div className="mx-auto flex min-h-screen max-w-7xl flex-col gap-10 px-6 py-10">
          <header className="flex flex-col gap-8 border-b border-slate-800/70 pb-8">
            <div className="flex flex-col gap-6 sm:flex-row sm:items-center sm:justify-between">
              <div className="flex items-center gap-4">
                <span className="flex h-11 w-11 items-center justify-center rounded-2xl border border-sky-500/40 bg-sky-500/10 text-base font-semibold uppercase tracking-[0.3em] text-sky-200">
                  QT
                </span>
                <span className="text-sm font-semibold uppercase tracking-[0.4em] text-slate-400">
                  QuantTrad
                </span>
              </div>
              <nav className="flex items-center gap-1 rounded-full border border-slate-800/70 bg-slate-900/60 p-1 text-sm font-medium text-slate-300">
                {NAV_ITEMS.map((item) => {
                  const isActive = Boolean(item.active)
                  return (
                    <button
                      key={item.key}
                      type="button"
                      className={`relative inline-flex items-center gap-2 rounded-full px-4 py-2 transition ${
                        isActive
                          ? 'bg-slate-100 text-slate-900 shadow-[0_12px_25px_-15px_rgba(148,163,184,0.65)]'
                          : 'text-slate-400 hover:text-slate-200'
                      }`}
                      aria-current={isActive ? 'page' : undefined}
                      disabled={!isActive}
                    >
                      <span className={`h-1.5 w-1.5 rounded-full ${isActive ? 'bg-slate-900' : 'bg-slate-600'}`} />
                      {item.label}
                    </button>
                  )
                })}
              </nav>
              <div className="flex items-center gap-2 text-xs text-slate-400">
                <span className="h-2 w-2 rounded-full bg-emerald-400" />
                Live feed
              </div>
            </div>
            <div className="flex flex-col gap-2 sm:flex-row sm:items-center sm:justify-between">
              <h1 className="text-3xl font-semibold text-slate-50 sm:text-4xl">QuantLab</h1>
              <div className="flex items-center gap-3 text-xs text-slate-500">
                <span className="inline-flex items-center gap-2 rounded-full border border-slate-800/70 bg-slate-900/60 px-3 py-1 uppercase tracking-[0.35em] text-slate-400">
                  Workspace
                </span>
                <span className="hidden items-center gap-2 rounded-full border border-slate-800/70 bg-slate-900/60 px-3 py-1 uppercase tracking-[0.35em] text-slate-400 sm:inline-flex">
                  Charts
                </span>
              </div>
            </div>
          </header>

          <main className="grid flex-1 gap-8 py-4 lg:grid-cols-[2.1fr,1fr]">
            <section className="rounded-[28px] border border-slate-800/80 bg-slate-950/70 p-8 shadow-[0_40px_80px_-55px_rgba(15,23,42,0.9)] backdrop-blur">
              <ChartComponent chartId={chartId} />
            </section>
            <aside className="rounded-[28px] border border-slate-800/80 bg-slate-950/70 p-8 shadow-[0_40px_80px_-55px_rgba(15,23,42,0.9)] backdrop-blur">
              <TabManager chartId={chartId} />
            </aside>
          </main>
        </div>
      </div>
    </ChartStateProvider>
  )
}
