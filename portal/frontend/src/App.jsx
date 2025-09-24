import { useEffect, useMemo } from 'react'
import { ChartStateProvider, useChartValue } from './contexts/ChartStateContext'
import { ChartComponent } from './components/ChartComponent/ChartComponent'
import { TabManager } from './components/TabManager'
import { createLogger } from './utils/logger.js'

const sections = [
  { id: 'quantlab', label: 'QuantLab', description: 'Strategy workbench for indicators, charts, and overlays.' },
  { id: 'reports', label: 'Reports', description: 'Performance intelligence and trade-by-trade walkthroughs.' },
]

function ApiStatusPill({ chartId }) {
  const chart = useChartValue(chartId) || {}
  const status = chart.connectionStatus || 'idle'
  const label = status === 'online' ? 'Online' : status === 'error' ? 'Alert' : status === 'connecting' ? 'Syncing' : 'Standby'
  const tone = status === 'online'
    ? 'bg-emerald-500/20 text-emerald-200 border-emerald-500/40'
    : status === 'error'
      ? 'bg-rose-500/15 text-rose-200 border-rose-500/40'
      : status === 'connecting' || status === 'recovering'
        ? 'bg-amber-500/15 text-amber-200 border-amber-500/40'
        : 'bg-slate-700/40 text-slate-200 border-slate-600/50'

  return (
    <span className={`inline-flex items-center gap-2 rounded-full border px-3 py-1 text-[11px] uppercase tracking-[0.3em] transition ${tone}`}>
      <span className="block h-2 w-2 rounded-full bg-current" />
      {label}
    </span>
  )
}

function SectionHeading({ title, description, kicker }) {
  return (
    <div className="space-y-3">
      {kicker ? (
        <span className="text-[11px] uppercase tracking-[0.35em] text-purple-300/80">{kicker}</span>
      ) : null}
      <h2 className="text-3xl font-semibold tracking-tight text-slate-100">{title}</h2>
      <p className="max-w-2xl text-sm text-slate-400">{description}</p>
    </div>
  )
}

export default function App() {
  const chartId = 'main'
  const { info } = useMemo(() => createLogger('App', { chartId }), [chartId])

  useEffect(() => {
    info('app_mounted')
  }, [info])

  return (
    <ChartStateProvider>
      <div className="min-h-screen bg-[radial-gradient(circle_at_top,_#1b1b1d_0%,_#0d0d10_45%,_#060608_100%)] text-slate-100">
        <header className="sticky top-0 z-30 border-b border-white/5 bg-[#0d0d10]/90 backdrop-blur">
          <div className="mx-auto flex max-w-7xl flex-col gap-5 px-6 py-6 md:flex-row md:items-center md:justify-between">
            <div className="space-y-1">
              <div className="flex items-center gap-3 text-lg font-semibold text-slate-100">
                <span className="inline-flex h-10 w-10 items-center justify-center rounded-2xl bg-purple-500/20 text-purple-300">QT</span>
                <span>QuantTrad Portal</span>
              </div>
              <p className="text-sm text-slate-400">QuantLab • Ops Command • Insight Reports</p>
            </div>
            <nav className="flex flex-wrap items-center gap-2 text-xs uppercase tracking-[0.3em] text-slate-400">
              {sections.map((section) => (
                <a
                  key={section.id}
                  href={`#${section.id}`}
                  className="rounded-full border border-transparent bg-white/5 px-4 py-2 transition hover:border-purple-600/60 hover:bg-purple-900/30 hover:text-purple-200"
                >
                  {section.label}
                </a>
              ))}
            </nav>
            <ApiStatusPill chartId={chartId} />
          </div>
        </header>

        <main className="mx-auto max-w-7xl space-y-20 px-6 py-12">
          <section id="quantlab" className="space-y-10">
            <SectionHeading
              title="QuantLab"
              description="Visualize price action, overlays, and execution signals in a focused, minimal environment."
            />
            <div className="space-y-10">
              <ChartComponent chartId={chartId} />

              <section className="rounded-3xl border border-white/5 bg-black/40 p-6 shadow-[0_30px_80px_-60px_rgba(0,0,0,0.85)]">
                <header className="flex flex-col gap-3 border-b border-white/5 pb-4 sm:flex-row sm:items-center sm:justify-between">
                  <div className="space-y-1">
                    <h3 className="text-lg font-semibold text-slate-100">Indicator &amp; Signal Console</h3>
                    <p className="text-xs text-slate-400">Configure overlays today and plan strategies, signals, and presets tomorrow.</p>
                  </div>
                  <span className="rounded-full border border-purple-500/30 bg-purple-900/40 px-3 py-1 text-[11px] uppercase tracking-[0.3em] text-purple-200">QuantLab linked</span>
                </header>
                <div className="pt-4">
                  <TabManager chartId={chartId} />
                </div>
              </section>
            </div>
          </section>

          <section id="reports" className="space-y-10">
            <SectionHeading
              title="Reports"
              description="Dive into trade-level context, compare bots, and narrate every decision path from indicator to execution."
            />

            <div className="grid gap-6 lg:grid-cols-[minmax(0,1.3fr)_minmax(0,1fr)]">
              <div className="rounded-3xl border border-white/5 bg-white/[0.04] p-6">
                <h3 className="text-lg font-semibold text-slate-100">Bot scorecards</h3>
                <p className="mt-2 text-sm text-slate-400">Summaries for each trading bot with win rates, exposure, risk, and anomaly detection. Integrate walk-forward stats and breakdowns per indicator.</p>
                <div className="mt-4 grid gap-3 text-xs text-slate-400 sm:grid-cols-2">
                  <div className="rounded-2xl border border-white/10 bg-white/5 p-4">Equity curve overlays with drawdown callouts.</div>
                  <div className="rounded-2xl border border-white/10 bg-white/5 p-4">Signal attribution tree to trace decision pipelines.</div>
                  <div className="rounded-2xl border border-white/10 bg-white/5 p-4">Monte Carlo replays to stress test execution variance.</div>
                  <div className="rounded-2xl border border-white/10 bg-white/5 p-4">Export-ready PDF &amp; Notion embeds for stakeholder updates.</div>
                </div>
              </div>

              <div className="flex flex-col gap-4 rounded-3xl border border-purple-500/20 bg-purple-500/5 p-6">
                <h3 className="text-lg font-semibold text-purple-200">Trade walkthroughs</h3>
                <p className="text-sm text-purple-100/80">Replay every order with contextual overlays. Capture indicator states, signal weights, and execution metadata.</p>
                <div className="rounded-2xl border border-purple-400/30 bg-purple-500/10 p-4 text-xs text-purple-100/70">
                  Future UX includes: scrubbable timelines, indicator snapshots, and risk commentary sidebars for each decision point.
                </div>
                <ul className="space-y-2 text-sm text-purple-100/70">
                  <li>• Align QuantLab overlays with executed trades.</li>
                  <li>• Annotate decisions for compliance + research sharing.</li>
                  <li>• Integrate PnL, slippage, and volatility context.</li>
                </ul>
              </div>
            </div>
          </section>
        </main>

        <footer className="border-t border-white/5 bg-black/50 py-8">
          <div className="mx-auto flex max-w-7xl flex-col gap-3 px-6 text-xs text-slate-500 sm:flex-row sm:items-center sm:justify-between">
            <p>QuantTrad Portal — unified intelligence for research, ops, and reporting.</p>
            <p>Accent palette: graphite foundations with violet highlights.</p>
          </div>
        </footer>
      </div>
    </ChartStateProvider>
  )
}

