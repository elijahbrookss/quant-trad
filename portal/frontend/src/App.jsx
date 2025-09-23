import { useEffect, useMemo } from 'react'
import { ChartStateProvider } from './contexts/ChartStateContext'
import { ChartComponent } from './components/ChartComponent/ChartComponent'
import { TabManager } from './components/TabManager'
import { createLogger } from './utils/logger.js'

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
          <header className="flex flex-col gap-4 border-b border-slate-800/70 pb-6 sm:flex-row sm:items-end sm:justify-between">
            <div className="flex flex-col gap-2">
              <span className="inline-flex w-fit items-center gap-2 rounded-full border border-slate-800/70 bg-slate-900/60 px-4 py-1 text-xs font-semibold uppercase tracking-[0.35em] text-slate-300">
                <span className="h-1.5 w-1.5 rounded-full bg-emerald-300" />
                QuantTrad Portal
              </span>
              <h1 className="text-3xl font-semibold text-slate-50 sm:text-4xl">Trading workspace</h1>
              <p className="max-w-2xl text-sm text-slate-400 sm:text-base">
                Focused execution environment for monitoring markets, managing indicators, and reviewing strategy calls without distractions.
              </p>
            </div>
            <div className="flex gap-6 text-xs text-slate-500">
              <div className="flex flex-col items-start gap-1 text-right sm:items-end">
                <span className="font-semibold uppercase tracking-[0.3em] text-slate-400">Session</span>
                <span className="text-sm text-slate-200">Active</span>
              </div>
              <div className="hidden flex-col items-end gap-1 sm:flex">
                <span className="font-semibold uppercase tracking-[0.3em] text-slate-400">Latency</span>
                <span className="text-sm text-slate-200">&lt; 50ms</span>
              </div>
            </div>
          </header>

          <main className="grid flex-1 gap-8 py-4 lg:grid-cols-[2.1fr,1fr]">
            <section className="rounded-[32px] border border-slate-800/80 bg-slate-950/70 p-8 shadow-[0_50px_90px_-60px_rgba(15,23,42,0.9)] backdrop-blur">
              <ChartComponent chartId={chartId} />
            </section>
            <aside className="rounded-[32px] border border-slate-800/80 bg-slate-950/70 p-8 shadow-[0_50px_90px_-60px_rgba(15,23,42,0.9)] backdrop-blur">
              <TabManager chartId={chartId} />
            </aside>
          </main>
        </div>
      </div>
    </ChartStateProvider>
  )
}
