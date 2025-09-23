import { useEffect, useMemo } from 'react'
import { ChartStateProvider } from './contexts/ChartStateContext'
import { ChartComponent } from './components/ChartComponent/ChartComponent'
import { TabManager } from './components/TabManager'
import { createLogger } from './utils/logger.js'
import { CheckCircle2, LifeBuoy, Sparkles } from 'lucide-react'

export default function App() {
  const chartId = 'main'
  const { info } = useMemo(() => createLogger('App', { chartId }), [chartId])

  useEffect(() => {
    info('app_mounted')
  }, [info])

  return (
    <ChartStateProvider>
      <div className="min-h-screen bg-slate-950 text-slate-100">
        <header className="border-b border-slate-900/70 bg-slate-950/80 backdrop-blur">
          <div className="mx-auto flex max-w-6xl flex-col gap-4 px-6 py-6 lg:flex-row lg:items-center lg:justify-between">
            <div>
              <p className="text-sm font-semibold uppercase tracking-[0.4em] text-slate-400">QuantTrad Lab</p>
              <h1 className="mt-2 text-3xl font-semibold text-white lg:text-4xl">Confident trading for new investors</h1>
              <p className="mt-2 max-w-xl text-sm text-slate-400">
                Streamlined tools, curated presets, and plain-language cues to help you learn the rhythm of the markets without feeling overwhelmed.
              </p>
            </div>
            <div className="flex items-center gap-3 rounded-2xl border border-slate-800/80 bg-slate-900/70 px-4 py-3 text-sm text-slate-300 shadow-lg shadow-slate-950/40">
              <Sparkles className="h-5 w-5 text-sky-300" aria-hidden="true" />
              <span className="max-w-[16rem] leading-relaxed">
                Tip: Use the <span className="font-semibold text-white">/</span> key to open quick symbol presets anytime.
              </span>
            </div>
          </div>
        </header>

        <main className="mx-auto flex max-w-6xl flex-col gap-8 px-6 py-10">
          <section className="grid gap-6 lg:grid-cols-[minmax(0,3fr)_minmax(0,2fr)] xl:gap-8">
            <div className="flex flex-col gap-6">
              <ChartComponent chartId={chartId} />
              <TabManager chartId={chartId} />
            </div>

            <aside className="flex flex-col gap-6">
              <div className="rounded-3xl border border-slate-900/70 bg-slate-900/60 p-6 shadow-xl shadow-slate-950/40">
                <div className="flex items-center gap-3 text-sky-200">
                  <LifeBuoy className="h-5 w-5" aria-hidden="true" />
                  <h2 className="text-base font-semibold tracking-wide text-white">Beginner safety net</h2>
                </div>
                <p className="mt-3 text-sm text-slate-300">
                  Stay oriented with a guided workflow. Each step is focused on clarity so you can build confidence one trade at a time.
                </p>
                <ul className="mt-4 space-y-3 text-sm">
                  <li className="flex items-start gap-3 rounded-2xl border border-slate-800/70 bg-slate-950/70 px-4 py-3">
                    <CheckCircle2 className="mt-0.5 h-5 w-5 flex-shrink-0 text-emerald-300" aria-hidden="true" />
                    <div>
                      <p className="font-medium text-white">Start with presets</p>
                      <p className="text-slate-400">Choose a ready-made layout and observe how signals react before you customize.</p>
                    </div>
                  </li>
                  <li className="flex items-start gap-3 rounded-2xl border border-slate-800/70 bg-slate-950/70 px-4 py-3">
                    <CheckCircle2 className="mt-0.5 h-5 w-5 flex-shrink-0 text-emerald-300" aria-hidden="true" />
                    <div>
                      <p className="font-medium text-white">Enable one indicator at a time</p>
                      <p className="text-slate-400">Layer insights gradually to understand what each tool contributes to your decision.</p>
                    </div>
                  </li>
                  <li className="flex items-start gap-3 rounded-2xl border border-slate-800/70 bg-slate-950/70 px-4 py-3">
                    <CheckCircle2 className="mt-0.5 h-5 w-5 flex-shrink-0 text-emerald-300" aria-hidden="true" />
                    <div>
                      <p className="font-medium text-white">Review signals in context</p>
                      <p className="text-slate-400">The chart bubbles now mirror each indicator’s color so you always know what fired.</p>
                    </div>
                  </li>
                </ul>
              </div>
            </aside>
          </section>
        </main>
      </div>
    </ChartStateProvider>
  )
}
