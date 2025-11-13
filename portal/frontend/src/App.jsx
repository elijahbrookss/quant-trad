import { useCallback, useEffect, useMemo, useRef, useState } from 'react'
import { ChartStateProvider, useChartState, useChartValue } from './contexts/ChartStateContext'
import { ChartComponent } from './components/ChartComponent/ChartComponent'
import { TabManager } from './components/TabManager'
import { BotPanel } from './components/bots/BotPanel.jsx'
import { createLogger } from './utils/logger.js'
import { RefreshCw } from 'lucide-react'
import { pingApi } from './adapters/health.adapter.js'

const sections = [
  { id: 'quantlab', label: 'QuantLab', description: 'Strategy workbench for indicators, charts, and overlays.' },
  { id: 'reports', label: 'Reports', description: 'Performance intelligence and trade-by-trade walkthroughs.' },
]

function ApiStatusPill({ chartId }) {
  const chart = useChartValue(chartId) || {}
  const status = chart.healthStatus || chart.connectionStatus || 'idle'
  const label = status === 'online'
    ? 'Online'
    : status === 'error'
      ? 'Alert'
      : status === 'connecting' || status === 'recovering'
        ? 'Checking'
        : 'Standby'
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
        <span className="text-[11px] uppercase tracking-[0.35em] text-[color:var(--accent-text-kicker)]">{kicker}</span>
      ) : null}
      <h2 className="text-3xl font-semibold tracking-tight text-slate-100">{title}</h2>
      <p className="max-w-2xl text-sm text-slate-400">{description}</p>
    </div>
  )
}

function AppShell({ chartId }) {
  const { info, error: logError } = useMemo(() => createLogger('App', { chartId }), [chartId])
  const { updateChart } = useChartState()
  const [checkingHealth, setCheckingHealth] = useState(false)
  const healthErrorRef = useRef(null)
  const mountedRef = useRef(true)

  useEffect(() => {
    info('app_mounted')
  }, [info])

  const chart = useChartValue(chartId) || {}
  const lastHealthCheckLabel = useMemo(() => {
    if (checkingHealth && !chart?.lastHealthCheckAt) return 'Checking API…'
    if (!chart?.lastHealthCheckAt) return 'Awaiting health check'
    if (checkingHealth) return 'Checking API…'
    try {
      const parsed = new Date(chart.lastHealthCheckAt)
      const formatted = new Intl.DateTimeFormat(undefined, {
        hour: '2-digit',
        minute: '2-digit',
        second: '2-digit',
        hour12: false,
      }).format(parsed)
      const prefix = chart.healthStatus === 'error' ? 'Last check failed' : 'Last check'
      return `${prefix} ${formatted}`
    } catch {
      const prefix = chart.healthStatus === 'error' ? 'Last check failed' : 'Last check'
      return `${prefix} ${new Date(chart.lastHealthCheckAt).toLocaleTimeString()}`
    }
  }, [chart?.lastHealthCheckAt, chart?.healthStatus, checkingHealth])

  const runHealthCheck = useCallback(async () => {
    if (!mountedRef.current) return
    setCheckingHealth(true)
    updateChart(chartId, {
      healthStatus: 'connecting',
      healthMessage: 'Pinging API…',
    })

    try {
      const payload = await pingApi()
      const nowIso = new Date().toISOString()
      if (!mountedRef.current) return
      healthErrorRef.current = null
      updateChart(chartId, {
        healthStatus: 'online',
        healthMessage: payload?.status === 'ok' ? 'API responded normally.' : 'API reachable.',
        lastHealthCheckAt: nowIso,
      })
    } catch (err) {
      const nowIso = new Date().toISOString()
      if (!mountedRef.current) return
      const message = err?.message || 'Unable to reach API'
      healthErrorRef.current = message
      logError('api_health_check_failed', err)
      updateChart(chartId, {
        healthStatus: 'error',
        healthMessage: message,
        lastHealthCheckAt: nowIso,
      })
    } finally {
      if (mountedRef.current) {
        setCheckingHealth(false)
      }
    }
  }, [chartId, updateChart, logError])

  useEffect(() => {
    mountedRef.current = true
    runHealthCheck()
    const id = setInterval(() => {
      runHealthCheck()
    }, 60000)
    return () => {
      mountedRef.current = false
      clearInterval(id)
    }
  }, [runHealthCheck])

  const healthMessage = chart.healthStatus === 'error'
    ? (chart.healthMessage || healthErrorRef.current)
    : null

  return (
    <div className="min-h-screen bg-[#14171f] bg-[radial-gradient(circle_at_top,_var(--accent-gradient-spot)_0%,_rgba(20,23,31,1)_55%)] text-slate-100">
        <header className="sticky top-0 z-30 border-b border-white/5 bg-[#1c1f2b]/90 backdrop-blur">
          <div className="mx-auto flex max-w-[1600px] flex-col gap-5 px-8 py-6 md:flex-row md:items-center md:justify-between">
            <div className="space-y-1">
              <div className="flex items-center gap-3 text-lg font-semibold text-slate-100">
                <span className="inline-flex h-10 w-10 items-center justify-center rounded-2xl bg-[color:var(--accent-alpha-15)] text-[color:var(--accent-text-soft)]">QT</span>
                <span>QuantTrad Portal</span>
              </div>
              <p className="text-sm text-slate-400">QuantLab • Ops Command • Insight Reports</p>
            </div>
            <nav className="flex flex-wrap items-center gap-2 text-xs uppercase tracking-[0.32em] text-slate-400">
              {sections.map((section) => (
                <a
                  key={section.id}
                  href={`#${section.id}`}
                  className="rounded-full border border-white/5 bg-white/5 px-4 py-2 transition hover:border-[color:var(--accent-alpha-40)] hover:bg-[color:var(--accent-alpha-15)] hover:text-[color:var(--accent-text-strong)]"
                >
                  {section.label}
                </a>
              ))}
            </nav>
          </div>
        </header>

        <main className="mx-auto max-w-[1600px] space-y-20 px-8 py-12">
          <section id="quantlab" className="space-y-10">
            <div className="flex flex-col gap-6 lg:flex-row lg:items-start lg:justify-between">
              <SectionHeading
                title="QuantLab"
                description="Visualize price action, overlays, and execution signals in a focused, minimal environment."
              />
              <div className="flex flex-col items-start gap-3 rounded-2xl border border-white/10 bg-white/5 p-4 text-xs text-slate-300 sm:items-end">
                <div className="flex items-center gap-3">
                  <ApiStatusPill chartId={chartId} />
                  <button
                    type="button"
                    onClick={runHealthCheck}
                    className="inline-flex h-8 w-8 items-center justify-center rounded-full border border-[color:var(--accent-alpha-40)] bg-[color:var(--accent-alpha-10)] text-[color:var(--accent-text-strong)] transition hover:border-[color:var(--accent-alpha-60)] hover:bg-[color:var(--accent-alpha-20)] disabled:opacity-60"
                    aria-label="Check API health"
                    disabled={checkingHealth}
                  >
                    <RefreshCw className="size-4" />
                  </button>
                </div>
                <span className="text-[11px] tracking-[0.25em] text-slate-400">{lastHealthCheckLabel}</span>
                {healthMessage ? (
                  <span className="text-[11px] text-rose-300/80">{healthMessage}</span>
                ) : null}
              </div>
            </div>
            <div className="space-y-10">
              <ChartComponent chartId={chartId} />

              <section className="rounded-3xl border border-white/8 bg-[#1a1d27]/80 p-6 shadow-[0_40px_120px_-70px_rgba(0,0,0,0.85)]">
                <header className="flex flex-col gap-3 border-b border-white/5 pb-4 sm:flex-row sm:items-center sm:justify-between">
                  <div className="space-y-1">
                    <h3 className="text-lg font-semibold text-slate-100">Indicator &amp; Signal Console</h3>
                    <p className="text-xs text-slate-400">Configure overlays today and plan strategies, signals, and presets tomorrow.</p>
                  </div>
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
              <BotPanel />

              <div className="flex flex-col gap-4 rounded-3xl border border-[color:var(--accent-alpha-30)] bg-[color:var(--accent-alpha-10)] p-6">
                <h3 className="text-lg font-semibold text-[color:var(--accent-text-strong)]">Trade walkthroughs</h3>
                <p className="text-sm text-[color:var(--accent-text-strong-alpha)]">Replay every order with contextual overlays. Capture indicator states, signal weights, and execution metadata.</p>
                <div className="rounded-2xl border border-[color:var(--accent-alpha-30)] bg-[color:var(--accent-alpha-15)] p-4 text-xs text-[color:var(--accent-text-strong-alpha)]">
                  Future UX includes: scrubbable timelines, indicator snapshots, and risk commentary sidebars for each decision point.
                </div>
                <ul className="space-y-2 text-sm text-[color:var(--accent-text-strong-alpha)]">
                  <li>• Align QuantLab overlays with executed trades.</li>
                  <li>• Annotate decisions for compliance + research sharing.</li>
                  <li>• Integrate PnL, slippage, and volatility context.</li>
                </ul>
              </div>
            </div>
          </section>
        </main>

        <footer className="border-t border-white/5 bg-[#181b25]/80 py-8">
          <div className="mx-auto flex max-w-[1600px] flex-col gap-4 px-8 text-xs text-slate-400 sm:flex-row sm:items-center sm:justify-between">
            <p>QuantTrad Portal — unified intelligence for research, ops, and reporting.</p>
            <div className="flex flex-wrap items-center gap-3 text-xs text-slate-300">
              <a
                href="https://github.com/elijahbrookss/quant-trad"
                target="_blank"
                rel="noreferrer"
                className="rounded-full border border-white/10 bg-white/5 px-3 py-1 transition hover:border-[color:var(--accent-alpha-40)] hover:bg-[color:var(--accent-alpha-15)] hover:text-[color:var(--accent-text-strong)]"
              >
                GitHub
              </a>
              <a
                href="https://quad-trad.gitbook.io/docs/"
                target="_blank"
                rel="noreferrer"
                className="rounded-full border border-white/10 bg-white/5 px-3 py-1 transition hover:border-[color:var(--accent-alpha-40)] hover:bg-[color:var(--accent-alpha-15)] hover:text-[color:var(--accent-text-strong)]"
              >
                Documentation
              </a>
            </div>
          </div>
        </footer>
    </div>
  )
}

export default function App() {
  const chartId = 'main'
  return (
    <ChartStateProvider>
      <AppShell chartId={chartId} />
    </ChartStateProvider>
  )
}

