import { Suspense, lazy, useCallback, useEffect, useMemo, useRef, useState } from 'react'
import { BrowserRouter, NavLink, Navigate, Route, Routes, useLocation } from 'react-router-dom'
import { ChartStateProvider, useChartState, useChartValue } from './contexts/ChartStateContext'
import { createLogger } from './utils/logger.js'
import { Bot, ChevronLeft, ChevronRight, FileText, FlaskConical, Layers, Menu, RefreshCw, Settings, X } from 'lucide-react'
import { pingApi } from './adapters/health.adapter.js'
import { usePortalSettings } from './contexts/PortalSettingsContext.jsx'
import { useAccentColor } from './contexts/AccentColorContext.jsx'

const ChartComponent = lazy(() =>
  import('./components/ChartComponent/ChartComponent').then((module) => ({ default: module.ChartComponent })),
)
const IndicatorSection = lazy(() =>
  import('./components/IndicatorTab.jsx').then((module) => ({ default: module.IndicatorSection })),
)
const StrategyTab = lazy(() => import('./components/StrategyTab.jsx'))
const BotPanel = lazy(() =>
  import('./components/bots/BotPanel.jsx').then((module) => ({ default: module.BotPanel })),
)
const ReportsPage = lazy(() =>
  import('./components/reports/ReportsPage.jsx').then((module) => ({ default: module.ReportsPage })),
)
const GlobalSettingsModal = lazy(() =>
  import('./components/GlobalSettingsModal.jsx').then((module) => ({ default: module.GlobalSettingsModal })),
)

const navItems = [
  {
    id: 'quantlab',
    label: 'QuantLab',
    description: 'Research workspace for indicators, charts, and overlays.',
    kicker: 'Research Lens',
    to: '/quantlab',
    icon: FlaskConical,
  },
  {
    id: 'strategy',
    label: 'Strategy',
    description: 'Decision logic builder for signals, rules, and risk.',
    kicker: 'Decision Lens',
    to: '/strategy',
    icon: Layers,
  },
  {
    id: 'bots',
    label: 'Bots',
    description: 'Execution layer for backtests, playback, and live runs.',
    kicker: 'Execution Lens',
    to: '/bots',
    icon: Bot,
  },
  {
    id: 'reports',
    label: 'Reports',
    description: 'Backtest report archive with full analytics.',
    kicker: 'Analysis Lens',
    to: '/reports',
    icon: FileText,
  },
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

function SectionHeading({ title, description, kicker, actions }) {
  return (
    <div className="flex flex-col gap-6 lg:flex-row lg:items-start lg:justify-between">
      <div className="space-y-3">
        {kicker ? (
          <span className="text-[11px] uppercase tracking-[0.35em] text-[color:var(--accent-text-kicker)]">{kicker}</span>
        ) : null}
        <h2 className="text-3xl font-semibold tracking-tight text-slate-100">{title}</h2>
        <p className="max-w-2xl text-sm text-slate-400">{description}</p>
      </div>
      {actions ? <div className="w-full max-w-sm">{actions}</div> : null}
    </div>
  )
}

function RouteSectionFallback({ title }) {
  return (
    <div className="rounded-3xl border border-white/10 bg-[#151924]/70 p-6 text-sm text-slate-400">
      Loading {title.toLowerCase()}…
    </div>
  )
}

function Sidebar({ collapsed, open, onClose, onToggleCollapse }) {
  return (
    <>
      <div
        className={`fixed inset-0 z-30 bg-black/40 transition lg:hidden ${open ? 'opacity-100' : 'pointer-events-none opacity-0'}`}
        onClick={onClose}
        aria-hidden={!open}
      />
      <aside
        className={`fixed inset-y-0 left-0 z-40 flex w-72 flex-col border-r border-white/5 bg-[#151924]/95 px-3 py-6 backdrop-blur transition lg:static lg:z-auto ${
          open ? 'translate-x-0' : '-translate-x-full'
        } ${collapsed ? 'lg:w-24' : 'lg:w-64'} lg:translate-x-0`}
      >
        <div className={`${collapsed ? 'flex flex-col items-center gap-3' : 'flex items-center justify-between'}`}>
          <div className={`flex items-center gap-3 ${collapsed ? '' : ''}`}>
            {collapsed ? (
              <button
                type="button"
                onClick={onToggleCollapse}
                className="group relative inline-flex h-10 w-10 items-center justify-center rounded-2xl border border-white/10 bg-[color:var(--accent-alpha-12)] text-[color:var(--accent-text-soft)] transition hover:border-[color:var(--accent-alpha-40)] hover:bg-[color:var(--accent-alpha-18)]"
                aria-label="Expand sidebar"
              >
                <span className="transition group-hover:opacity-0">QT</span>
                <ChevronRight className="pointer-events-none absolute h-4 w-4 opacity-0 transition group-hover:opacity-100" />
              </button>
            ) : (
              <span className="inline-flex h-10 w-10 items-center justify-center rounded-2xl border border-white/10 bg-[color:var(--accent-alpha-12)] text-[color:var(--accent-text-soft)]">
                QT
              </span>
            )}
            {!collapsed ? (
              <div className="space-y-1">
                <div className="text-sm font-semibold text-slate-100">QuantTrad</div>
                <div className="text-[11px] uppercase tracking-[0.3em] text-slate-500">Portal</div>
              </div>
            ) : null}
          </div>
          {!collapsed ? (
            <div className="flex items-center gap-2">
              <button
                type="button"
                onClick={onToggleCollapse}
                className="hidden h-9 w-9 items-center justify-center rounded-full border border-white/10 bg-white/5 text-slate-300 transition hover:border-[color:var(--accent-alpha-40)] hover:bg-[color:var(--accent-alpha-15)] hover:text-[color:var(--accent-text-strong)] lg:inline-flex"
                aria-label="Collapse sidebar"
              >
                <ChevronLeft className="size-4" />
              </button>
              <button
                type="button"
                onClick={onClose}
                className="inline-flex h-9 w-9 items-center justify-center rounded-full border border-white/10 bg-white/5 text-slate-300 transition hover:border-[color:var(--accent-alpha-40)] hover:bg-[color:var(--accent-alpha-15)] hover:text-[color:var(--accent-text-strong)] lg:hidden"
                aria-label="Close sidebar"
              >
                <X className="size-4" />
              </button>
            </div>
          ) : null}
        </div>

        <nav className="mt-8 space-y-2 text-sm">
          {navItems.map((item) => {
            const Icon = item.icon
            return (
              <NavLink
                key={item.id}
                to={item.to}
                title={item.label}
                className={({ isActive }) =>
                  [
                    'flex items-center gap-3 rounded-2xl border px-3 py-3 transition',
                    collapsed ? 'justify-center lg:px-3' : '',
                    isActive
                      ? 'border-[color:var(--accent-alpha-60)] bg-[color:var(--accent-alpha-20)] text-[color:var(--accent-text-strong)] shadow-[0_20px_40px_-24px_var(--accent-shadow-strong)]'
                      : 'border-transparent text-slate-300 hover:border-[color:var(--accent-alpha-40)] hover:bg-[color:var(--accent-alpha-10)] hover:text-[color:var(--accent-text-strong)]',
                 ]
                    .filter(Boolean)
                    .join(' ')
                }
              >
                <span className="flex h-10 w-10 items-center justify-center rounded-xl bg-white/5 text-[color:var(--accent-text-soft)]">
                  <Icon className="size-5" />
                </span>
                {!collapsed ? (
                  <div className="min-w-0">
                    <div className="font-semibold">{item.label}</div>
                    <div className="text-[11px] text-slate-500">{item.kicker}</div>
                  </div>
                ) : null}
              </NavLink>
            )
          })}
        </nav>

        {!collapsed ? (
          <div className="mt-auto rounded-2xl border border-white/10 bg-white/5 p-4 text-xs text-slate-300">
            <div className="uppercase tracking-[0.3em] text-slate-500">Flow</div>
            <p className="mt-2 text-sm text-slate-200">QuantLab → Strategy → Bot</p>
            <p className="mt-1 text-[11px] text-slate-500">Each lens stays isolated to preserve walk-forward integrity.</p>
          </div>
        ) : null}
      </aside>
    </>
  )
}

function AppShell({ chartId }) {
  const { info, error: logError } = useMemo(() => createLogger('App', { chartId }), [chartId])
  const { updateChart } = useChartState()
  const [checkingHealth, setCheckingHealth] = useState(false)
  const healthErrorRef = useRef(null)
  const mountedRef = useRef(true)
  const [sidebarCollapsed, setSidebarCollapsed] = useState(false)
  const [sidebarOpen, setSidebarOpen] = useState(false)
  const [settingsOpen, setSettingsOpen] = useState(false)
  const { settings } = usePortalSettings()
  const { setAccentColor } = useAccentColor()
  const location = useLocation()

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

  useEffect(() => {
    setSidebarOpen(false)
  }, [location.pathname])

  useEffect(() => {
    if (settings?.accentColor) {
      setAccentColor(settings.accentColor)
    }
  }, [settings?.accentColor, setAccentColor])

  const healthMessage = chart.healthStatus === 'error'
    ? (chart.healthMessage || healthErrorRef.current)
    : null

  const currentNav = useMemo(() => {
    return navItems.find((item) => location.pathname.startsWith(item.to)) || navItems[0]
  }, [location.pathname])

  return (
    <div className="min-h-screen bg-[#14171f] bg-[radial-gradient(circle_at_top,_var(--accent-gradient-spot)_0%,_rgba(20,23,31,1)_55%)] text-slate-100">
      <div className="flex min-h-screen">
        <Sidebar
          collapsed={sidebarCollapsed}
          open={sidebarOpen}
          onClose={() => setSidebarOpen(false)}
          onToggleCollapse={() => setSidebarCollapsed((prev) => !prev)}
        />

        <div className="flex min-w-0 flex-1 flex-col">
          <header className="sticky top-0 z-30 border-b border-white/5 bg-[#1c1f2b]/90 px-6 py-4 backdrop-blur lg:px-10">
            <div className="flex items-center gap-3">
              <button
                type="button"
                onClick={() => setSidebarOpen(true)}
                className="inline-flex h-10 w-10 items-center justify-center rounded-full border border-white/10 bg-white/5 text-slate-200 transition hover:border-[color:var(--accent-alpha-40)] hover:bg-[color:var(--accent-alpha-15)] hover:text-[color:var(--accent-text-strong)] lg:hidden"
                aria-label="Open sidebar"
              >
                <Menu className="size-5" />
              </button>
              <div className="space-y-1">
                <span className="text-[11px] uppercase tracking-[0.35em] text-slate-500">{currentNav?.kicker}</span>
                <div className="text-lg font-semibold text-slate-100">{currentNav?.label}</div>
              </div>
              <div className="ml-auto hidden items-center gap-3 text-xs text-slate-400 md:flex">
                <button
                  type="button"
                  onClick={() => setSettingsOpen(true)}
                  className="inline-flex h-9 w-9 items-center justify-center rounded-full border border-white/10 bg-white/5 text-slate-300 transition hover:border-[color:var(--accent-alpha-40)] hover:bg-[color:var(--accent-alpha-15)] hover:text-[color:var(--accent-text-strong)]"
                  aria-label="Open global settings"
                >
                  <Settings className="size-4" />
                </button>
                <span className="rounded-full border border-white/10 bg-white/5 px-3 py-1 uppercase tracking-[0.2em]">QuantTrad</span>
              </div>
            </div>
          </header>

          <main className="flex-1 px-6 py-10 lg:px-10">
            <div className="mx-auto w-full max-w-[1600px] space-y-10">
              <Routes>
                <Route path="/" element={<Navigate to="/quantlab" replace />} />
                <Route
                  path="/quantlab"
                  element={
                    <div className="space-y-6">
                      <SectionHeading
                        title="QuantLab"
                        kicker="Research Lens"
                        description="Visualize price action, overlays, and indicator signals in a focused, minimal workspace."
                        actions={
                          <div className="flex flex-wrap items-center gap-3 text-xs text-slate-300">
                            <ApiStatusPill chartId={chartId} />
                            <button
                              type="button"
                              onClick={runHealthCheck}
                              className="inline-flex h-8 items-center gap-2 rounded-full border border-[color:var(--accent-alpha-40)] bg-[color:var(--accent-alpha-10)] px-3 text-[11px] font-semibold uppercase tracking-[0.25em] text-[color:var(--accent-text-strong)] transition hover:border-[color:var(--accent-alpha-60)] hover:bg-[color:var(--accent-alpha-20)] disabled:opacity-60"
                              aria-label="Check API health"
                              disabled={checkingHealth}
                            >
                              <RefreshCw className="size-4" />
                              Health
                            </button>
                            <span className="text-[11px] tracking-[0.25em] text-slate-500">{lastHealthCheckLabel}</span>
                            {healthMessage ? (
                              <span className="text-[11px] text-rose-300/80">{healthMessage}</span>
                            ) : null}
                          </div>
                        }
                      />

                      <div className="space-y-6">
                        <Suspense fallback={<RouteSectionFallback title="QuantLab chart" />}>
                          <ChartComponent chartId={chartId} />
                        </Suspense>

                        <section className="rounded-3xl border border-white/10 bg-gradient-to-br from-[#0f1320]/95 via-[#0c101a]/95 to-[#0b0f18]/95 p-5 shadow-[0_40px_140px_-90px_rgba(0,0,0,0.85)]">
                          <header className="flex items-center justify-between border-b border-white/5 pb-3">
                            <div>
                              <h3 className="text-sm font-semibold text-slate-100">Indicators</h3>
                              <p className="text-[11px] uppercase tracking-[0.3em] text-slate-500">Overlays and signals</p>
                            </div>
                          </header>
                          <div className="pt-3">
                            <Suspense fallback={<RouteSectionFallback title="indicator panel" />}>
                              <IndicatorSection chartId={chartId} />
                            </Suspense>
                          </div>
                        </section>
                      </div>
                    </div>
                  }
                />
                <Route
                  path="/strategy"
                  element={
                    <div className="space-y-10">
                      <SectionHeading
                        title="Strategy"
                        kicker="Decision Lens"
                        description="Author decision logic, attach indicators, and preview rule outputs without execution realism."
                        actions={
                          <div className="rounded-2xl border border-white/10 bg-white/5 p-4 text-xs text-slate-300">
                            <div className="uppercase tracking-[0.3em] text-slate-500">Focus</div>
                            <p className="mt-2 text-sm text-slate-200">Signals, rules, and ATM templates.</p>
                            <p className="mt-1 text-[11px] text-slate-500">Execution realism stays in Bot runs.</p>
                          </div>
                        }
                      />
                      <section className="rounded-3xl border border-white/8 bg-[#1a1d27]/80 p-6 shadow-[0_40px_120px_-70px_rgba(0,0,0,0.85)]">
                        <Suspense fallback={<RouteSectionFallback title="strategy workspace" />}>
                          <StrategyTab chartId={chartId} />
                        </Suspense>
                      </section>
                    </div>
                  }
                />
                <Route
                  path="/bots"
                  element={
                    <div className="space-y-10">
                      <SectionHeading
                        title="Bots"
                        kicker="Execution Lens"
                        description="Run walk-forward backtests, paper sims, or live runs with realistic execution constraints."
                        actions={
                          <div className="rounded-2xl border border-white/10 bg-white/5 p-4 text-xs text-slate-300">
                            <div className="uppercase tracking-[0.3em] text-slate-500">Playback</div>
                            <p className="mt-2 text-sm text-slate-200">Trade lifecycles, stops, and targets.</p>
                            <p className="mt-1 text-[11px] text-slate-500">All runs respect walk-forward timing.</p>
                          </div>
                        }
                      />
                      <section className="rounded-3xl border border-white/8 bg-[#1a1d27]/80 p-6 shadow-[0_40px_120px_-70px_rgba(0,0,0,0.85)]">
                        <Suspense fallback={<RouteSectionFallback title="bot panel" />}>
                          <BotPanel />
                        </Suspense>
                      </section>
                    </div>
                  }
                />
                <Route
                  path="/reports"
                  element={
                    <div className="space-y-10">
                      <SectionHeading
                        title="Reports"
                        kicker="Analysis Lens"
                        description="Review completed backtests, compare outcomes, and export performance summaries."
                        actions={
                          <div className="rounded-2xl border border-white/10 bg-white/5 p-4 text-xs text-slate-300">
                            <div className="uppercase tracking-[0.3em] text-slate-500">Archive</div>
                            <p className="mt-2 text-sm text-slate-200">Every completed run becomes a report.</p>
                            <p className="mt-1 text-[11px] text-slate-500">Open a report to see charts and trade analytics.</p>
                          </div>
                        }
                      />
                      <Suspense fallback={<RouteSectionFallback title="reports" />}>
                        <ReportsPage />
                      </Suspense>
                    </div>
                  }
                />
                <Route path="*" element={<Navigate to="/quantlab" replace />} />
              </Routes>
            </div>
          </main>
          <Suspense fallback={null}>
            <GlobalSettingsModal open={settingsOpen} onClose={() => setSettingsOpen(false)} />
          </Suspense>
        </div>
      </div>
    </div>
  )
}

export default function App() {
  const chartId = 'main'
  return (
    <BrowserRouter>
      <ChartStateProvider>
        <AppShell chartId={chartId} />
      </ChartStateProvider>
    </BrowserRouter>
  )
}
