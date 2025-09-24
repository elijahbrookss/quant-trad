import { useEffect, useMemo } from 'react'
import { ChartStateProvider, useChartValue } from './contexts/ChartStateContext'
import { ChartComponent } from './components/ChartComponent/ChartComponent'
import { TabManager } from './components/TabManager'
import { QuantLabSummary } from './components/QuantLabSummary'
import { createLogger } from './utils/logger.js'

const sections = [
  { id: 'quantlab', label: 'QuantLab', description: 'Strategy workbench for indicators, signals, and overlays.' },
  { id: 'command', label: 'Ops Command', description: 'Control plane for orchestrating trading bot infrastructure.' },
  { id: 'reports', label: 'Reports', description: 'Performance intelligence and trade-by-trade walkthroughs.' },
]

function HeaderStatus({ chartId }) {
  const chart = useChartValue(chartId) || {}
  const status = chart.connectionStatus || 'idle'
  const label = status === 'online' ? 'Online' : status === 'error' ? 'Alert' : status === 'connecting' ? 'Syncing' : 'Standby'
  const indicator = status === 'online'
    ? 'bg-emerald-400 shadow-[0_0_12px] shadow-emerald-400/80'
    : status === 'error'
      ? 'bg-rose-400 shadow-[0_0_12px] shadow-rose-500/70'
      : status === 'connecting' || status === 'recovering'
        ? 'bg-amber-300 shadow-[0_0_12px] shadow-amber-300/70'
        : 'bg-slate-500'

  return (
    <div className="flex items-center gap-2 rounded-full border border-white/10 bg-white/5 px-3 py-1 text-xs text-slate-200">
      <span className={`h-2 w-2 rounded-full transition ${indicator}`} />
      <span className="uppercase tracking-[0.3em] text-[10px]">{label}</span>
    </div>
  )
}

function SectionHeading({ title, description, eyebrow }) {
  return (
    <div className="space-y-3">
      {eyebrow ? (
        <span className="text-[11px] uppercase tracking-[0.35em] text-purple-300/80">{eyebrow}</span>
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
                  className="rounded-full border border-transparent bg-white/5 px-4 py-2 transition hover:border-purple-500/60 hover:bg-purple-500/10 hover:text-purple-200"
                >
                  {section.label}
                </a>
              ))}
            </nav>
            <HeaderStatus chartId={chartId} />
          </div>
        </header>

        <main className="mx-auto max-w-7xl px-6 py-12 space-y-20">
          <section id="quantlab" className="space-y-8">
            <SectionHeading
              eyebrow="1 — QuantLab"
              title="Strategy workbench"
              description="Visualize price action, overlays, and execution signals in a focused, minimal environment."
            />

            <QuantLabSummary chartId={chartId} />

            <div className="grid gap-6 xl:grid-cols-[minmax(0,1.65fr)_minmax(0,1fr)]">
              <div className="rounded-3xl border border-white/5 bg-black/40 shadow-[0_30px_60px_-40px_rgba(0,0,0,0.8)]">
                <div className="border-b border-white/5 px-6 py-5">
                  <div className="flex flex-wrap items-center justify-between gap-4">
                    <div>
                      <h3 className="text-lg font-medium text-slate-100">QuantLab Canvas</h3>
                      <p className="text-xs text-slate-400">Link presets, adjust timeframes, and stream data in real time.</p>
                    </div>
                    <div className="flex items-center gap-2">
                      <span className="rounded-full border border-purple-400/40 bg-purple-500/10 px-3 py-1 text-[11px] uppercase tracking-[0.3em] text-purple-200">/ Presets</span>
                      <span className="rounded-full border border-slate-700 bg-slate-900/70 px-3 py-1 text-[11px] uppercase tracking-[0.3em] text-slate-400">Multi-pane soon</span>
                    </div>
                  </div>
                </div>
                <div className="p-6">
                  <ChartComponent chartId={chartId} />
                </div>
              </div>

              <aside className="space-y-6">
                <div className="rounded-3xl border border-purple-500/20 bg-purple-500/5 p-6 shadow-inner shadow-purple-900/40">
                  <h3 className="text-lg font-semibold text-purple-200">Focus Presets</h3>
                  <p className="mt-2 text-sm text-purple-100/80">
                    Global presets stay in sync wherever you are. Linking a symbol from the chart instantly updates watchlists, indicator templates, and historical walks.
                  </p>
                  <ul className="mt-4 space-y-2 text-sm text-purple-100/70">
                    <li>• Tap <kbd className="rounded border border-purple-400/40 bg-purple-500/10 px-1">/</kbd> for the command palette.</li>
                    <li>• Save favorite instruments for faster recall.</li>
                    <li>• Seamless handoff into backtest + execution flows.</li>
                  </ul>
                </div>

                <div className="rounded-3xl border border-white/5 bg-white/[0.04] p-6">
                  <h3 className="text-base font-semibold text-slate-100">Upcoming modules</h3>
                  <ul className="mt-4 space-y-3 text-sm text-slate-400">
                    <li>
                      <span className="font-medium text-slate-200">Scenario Lab</span>
                      <p className="text-xs text-slate-500">Stitch signals and overlays into reusable playbooks.</p>
                    </li>
                    <li>
                      <span className="font-medium text-slate-200">Collaboration mode</span>
                      <p className="text-xs text-slate-500">Share chart states + strategies with teams in a click.</p>
                    </li>
                    <li>
                      <span className="font-medium text-slate-200">Alert studio</span>
                      <p className="text-xs text-slate-500">Trigger Discord / Slack notifications from signal events.</p>
                    </li>
                  </ul>
                </div>
              </aside>
            </div>

            <div className="rounded-3xl border border-white/5 bg-black/40 p-6 shadow-[0_30px_60px_-40px_rgba(0,0,0,0.8)]">
              <div className="flex flex-wrap items-center justify-between gap-4 border-b border-white/5 pb-4">
                <div>
                  <h3 className="text-lg font-medium text-slate-100">Indicator &amp; Signal Console</h3>
                  <p className="text-xs text-slate-400">Switch between indicator stacks, execution signals, and strategy recipes.</p>
                </div>
                <div className="flex items-center gap-2 text-[11px] uppercase tracking-[0.3em] text-slate-400">
                  <span className="rounded-full border border-purple-500/30 bg-purple-500/10 px-3 py-1 text-purple-200">Drag to reorder</span>
                  <span className="rounded-full border border-white/10 bg-white/5 px-3 py-1">Syncs with QuantLab</span>
                </div>
              </div>
              <div className="pt-4">
                <TabManager chartId={chartId} />
              </div>
            </div>
          </section>

          <section id="command" className="space-y-8">
            <SectionHeading
              eyebrow="2 — Ops Command"
              title="Infrastructure control plane"
              description="Coordinate trading bot infrastructure across clusters. Container orchestration hooks land here soon."
            />

            <div className="grid gap-6 lg:grid-cols-2">
              <div className="rounded-3xl border border-white/5 bg-white/[0.04] p-6">
                <h3 className="text-lg font-semibold text-slate-100">Instance overview</h3>
                <p className="mt-2 text-sm text-slate-400">Track cluster health, runtime versions, and deployment targets. Hooks into Docker, ECS, or Kubernetes in the next phase.</p>
                <div className="mt-5 grid grid-cols-2 gap-3 text-xs text-slate-300">
                  <Metric label="Active pods" value="—" />
                  <Metric label="Pending rollouts" value="—" />
                  <Metric label="Avg. latency" value="—" />
                  <Metric label="Last deploy" value="—" />
                </div>
              </div>

              <div className="flex flex-col gap-4 rounded-3xl border border-purple-500/20 bg-purple-500/5 p-6">
                <h3 className="text-lg font-semibold text-purple-200">Action queue</h3>
                <p className="text-sm text-purple-100/80">Plan future automation: start, stop, reboot, and redeploy trading services individually or in batches.</p>
                <ul className="space-y-2 text-sm text-purple-100/70">
                  <li>• Blueprint restart workflows with pre-flight checks.</li>
                  <li>• Schedule container rotations around session windows.</li>
                  <li>• Stream logs + metrics alongside lifecycle events.</li>
                </ul>
                <div className="rounded-2xl border border-purple-400/30 bg-purple-500/10 p-4 text-xs text-purple-100/70">
                  API surface will expose: <span className="text-purple-200">start</span>, <span className="text-purple-200">stop</span>, <span className="text-purple-200">restart</span>, <span className="text-purple-200">bounce</span>, and <span className="text-purple-200">scale</span> operations.
                </div>
              </div>
            </div>

            <div className="rounded-3xl border border-white/5 bg-black/40 p-6">
              <h3 className="text-base font-semibold text-slate-100">Runbook notes</h3>
              <p className="mt-3 text-sm text-slate-400">Document upgrade steps, maintenance windows, and escalation contacts. Embed Grafana dashboards or terminal sessions as modules later.</p>
              <div className="mt-5 grid gap-3 text-xs text-slate-400 sm:grid-cols-2">
                <div className="rounded-2xl border border-white/10 bg-white/5 p-4">Docker daemon health • Metrics stream hooks coming soon.</div>
                <div className="rounded-2xl border border-white/10 bg-white/5 p-4">Canary deployment lane for new strategies + runtime kernels.</div>
                <div className="rounded-2xl border border-white/10 bg-white/5 p-4">Webhook bridge for incident alerts into Ops channels.</div>
                <div className="rounded-2xl border border-white/10 bg-white/5 p-4">Audit trail and approval queue for human-in-the-loop actions.</div>
              </div>
            </div>
          </section>

          <section id="reports" className="space-y-8">
            <SectionHeading
              eyebrow="3 — Reports"
              title="Performance intelligence"
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

function Metric({ label, value }) {
  return (
    <div className="rounded-2xl border border-white/10 bg-black/40 px-4 py-3">
      <span className="text-[10px] uppercase tracking-[0.35em] text-slate-500">{label}</span>
      <div className="mt-2 text-lg font-semibold text-slate-200">{value}</div>
    </div>
  )
}
