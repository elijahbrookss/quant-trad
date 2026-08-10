import { Suspense, lazy, useEffect, useState } from 'react'
import { BrowserRouter, NavLink, Navigate, Route, Routes } from 'react-router-dom'
import { Activity, LayoutDashboard, PanelLeftClose, PanelLeftOpen, Settings } from 'lucide-react'
import { ChartStateProvider } from '../contexts/ChartStateContext.jsx'
import { usePortalSettings } from '../contexts/PortalSettingsContext.jsx'
import { useAccentColor } from '../contexts/AccentColorContext.jsx'
import { pingApi } from '../adapters/health.adapter.js'

const OverviewRoom = lazy(() =>
  import('./rooms/OverviewRoom.jsx').then((module) => ({ default: module.OverviewRoom })),
)
const OperationsRoom = lazy(() =>
  import('./rooms/FleetRoom.jsx').then((module) => ({ default: module.FleetRoom })),
)
const BotLensRoom = lazy(() =>
  import('./rooms/BotLensRoom.jsx').then((module) => ({ default: module.BotLensRoom })),
)
const CollectorLensRoom = lazy(() =>
  import('./rooms/CollectorLensRoom.jsx').then((module) => ({ default: module.CollectorLensRoom })),
)
const ResearchEvidenceRoom = lazy(() =>
  import('./rooms/ResearchEvidenceRoom.jsx').then((module) => ({ default: module.ResearchEvidenceRoom })),
)
const GlobalSettingsModal = lazy(() =>
  import('../components/GlobalSettingsModal.jsx').then((module) => ({ default: module.GlobalSettingsModal })),
)

const SIDEBAR_STORAGE_KEY = 'quanttrad.operator.sidebar.collapsed'
const ROOMS = [
  { id: 'overview', label: 'Overview', to: '/overview', icon: LayoutDashboard },
  { id: 'operations', label: 'Operations', to: '/operations', icon: Activity },
]

function initialSidebarCollapsed() {
  try {
    return window.localStorage.getItem(SIDEBAR_STORAGE_KEY) === 'true'
  } catch {
    return false
  }
}

function StatusPill() {
  const [status, setStatus] = useState('idle')

  useEffect(() => {
    let mounted = true
    async function check() {
      setStatus((previous) => (previous === 'reachable' ? previous : 'checking'))
      try {
        await pingApi()
        if (mounted) setStatus('reachable')
      } catch {
        if (mounted) setStatus('unavailable')
      }
    }
    check()
    const id = setInterval(check, 60_000)
    return () => {
      mounted = false
      clearInterval(id)
    }
  }, [])

  const label = status === 'reachable'
    ? 'API reachable'
    : status === 'unavailable'
      ? 'API unavailable'
      : status === 'checking'
        ? 'Checking API'
        : 'API not checked'

  return (
    <span className="qt2-status-pill" data-status={status} title="Connectivity only; not a platform-health claim.">
      <span className="qt2-status-dot" />
      <span className="qt2-status-label">{label}</span>
    </span>
  )
}

function RoomNav({ collapsed }) {
  return (
    <nav className="qt2-roomnav" aria-label="Primary">
      {ROOMS.map((room) => {
        const Icon = room.icon
        return (
          <NavLink
            key={room.id}
            to={room.to}
            className={({ isActive }) => "qt2-roomnav-btn " + (isActive ? "is-active" : "")}
            title={collapsed ? room.label : undefined}
          >
            <Icon size={18} aria-hidden="true" />
            <span>{room.label}</span>
          </NavLink>
        )
      })}
    </nav>
  )
}

function RoomFallback({ label }) {
  return <div className="qt2-room-loading">Loading {label}…</div>
}

function AppV2Shell() {
  const { settings } = usePortalSettings()
  const { setAccentColor } = useAccentColor()
  const [sidebarCollapsed, setSidebarCollapsed] = useState(initialSidebarCollapsed)
  const [settingsOpen, setSettingsOpen] = useState(false)

  useEffect(() => {
    try {
      window.localStorage.setItem(SIDEBAR_STORAGE_KEY, String(sidebarCollapsed))
    } catch {
      // Browser storage is optional; the in-memory preference still works.
    }
  }, [sidebarCollapsed])

  useEffect(() => {
    setAccentColor(settings.accentColor)
  }, [setAccentColor, settings.accentColor])
  const motionClass = settings.motion === 'reduced' ? 'app-motion-reduced' : ''

  return (
    <div className={'qt2-shell ' + motionClass + (sidebarCollapsed ? ' is-sidebar-collapsed' : '')}>
      <aside className="qt2-sidebar">
        <div className="qt2-brand">
          <span className="qt2-brand-mark">QT</span>
          <div>
            <div className="qt2-brand-title">QuantTrad</div>
            <div className="qt2-brand-kicker">Operator console</div>
          </div>
        </div>
        <RoomNav collapsed={sidebarCollapsed} />
        <div className="qt2-sidebar-foot">
          <StatusPill />
          <button
            type="button"
            className="qt2-sidebar-toggle"
            onClick={() => setSettingsOpen(true)}
            aria-label="Open appearance settings"
            title="Appearance settings"
          >
            <Settings size={17} />
            <span>Appearance</span>
          </button>
          <button
            type="button"
            className="qt2-sidebar-toggle"
            onClick={() => setSidebarCollapsed((value) => !value)}
            aria-label={sidebarCollapsed ? 'Expand navigation' : 'Collapse navigation'}
            aria-expanded={!sidebarCollapsed}
            title={sidebarCollapsed ? 'Expand navigation' : 'Collapse navigation'}
          >
            {sidebarCollapsed ? <PanelLeftOpen size={17} /> : <PanelLeftClose size={17} />}
            <span>{sidebarCollapsed ? 'Expand' : 'Collapse'}</span>
          </button>
        </div>
      </aside>

      <main className="qt2-main">
        <Routes>
          <Route path="/" element={<Navigate to="/overview" replace />} />
          <Route path="/overview" element={<Suspense fallback={<RoomFallback label="Overview" />}><OverviewRoom /></Suspense>} />
          <Route path="/operations" element={<Suspense fallback={<RoomFallback label="Operations" />}><OperationsRoom /></Suspense>} />
          <Route path="/operations/runs/:runId" element={<Suspense fallback={<RoomFallback label="BotLens" />}><BotLensRoom /></Suspense>} />
          <Route path="/operations/market/:collectorKind/:collectorId" element={<Suspense fallback={<RoomFallback label="collector operations" />}><CollectorLensRoom /></Suspense>} />
          <Route path="/operations/research/:itemId" element={<Suspense fallback={<RoomFallback label="research evidence" />}><ResearchEvidenceRoom /></Suspense>} />

          <Route path="/fleet" element={<Navigate to="/operations" replace />} />
          <Route path="/fleet/bots/:botId" element={<Navigate to="/operations?tab=definitions" replace />} />
          <Route path="/studio/*" element={<Navigate to="/overview" replace />} />
          <Route path="/research/*" element={<Navigate to="/operations?tab=research" replace />} />
          <Route path="/memory/*" element={<Navigate to="/operations?tab=research" replace />} />
          <Route path="/vault/*" element={<Navigate to="/overview" replace />} />
          <Route path="/reports/*" element={<Navigate to="/operations?tab=runs" replace />} />
          <Route path="*" element={<Navigate to="/overview" replace />} />
        </Routes>
      </main>
      <Suspense fallback={null}>
        <GlobalSettingsModal open={settingsOpen} onClose={() => setSettingsOpen(false)} />
      </Suspense>
    </div>
  )
}

export function AppV2() {
  return (
    <BrowserRouter>
      <ChartStateProvider>
        <AppV2Shell />
      </ChartStateProvider>
    </BrowserRouter>
  )
}
