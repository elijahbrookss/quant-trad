import { Suspense, lazy, useEffect, useState } from 'react'
import { BrowserRouter, NavLink, Navigate, Route, Routes, useLocation, useSearchParams } from 'react-router-dom'
import { Activity, HardDrive, LayoutDashboard, PanelLeftClose, PanelLeftOpen, Settings, BookOpen, Radio } from 'lucide-react'
import { ChartStateProvider } from '../contexts/ChartStateContext.jsx'
import { usePortalSettings } from '../contexts/PortalSettingsContext.jsx'
import { useAccentColor } from '../contexts/AccentColorContext.jsx'
import { pingApi } from '../adapters/health.adapter.js'
import { RoomLoadingFrame } from './components/RoomLoadingFrame.jsx'

const StudiesRoom = lazy(() => import('./rooms/StudiesRoom.jsx').then(module => ({ default: module.StudiesRoom })))

const StorageRoom = lazy(() => import('./rooms/StorageRoom.jsx').then((module) => ({ default: module.StorageRoom })))

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
  { id: 'studies', label: 'Studies', to: '/studies', icon: BookOpen },
]

function initialSidebarCollapsed() {
  try {
    return window.localStorage.getItem(SIDEBAR_STORAGE_KEY) === 'true'
  } catch {
    return false
  }
}

function LegacyOperations() {
  const [params] = useSearchParams()
  const tab = params.get('tab')
  const target = tab === 'research' ? '/studies' : ['market', 'data-plane', 'collectors', 'market-data'].includes(tab) ? '/collectors' : '/bot-runs'
  return <Navigate to={target + (target === '/bot-runs' && params.get('view') === 'history' ? '?view=history' : '')} replace />
}

function SecondaryNav() {
  const location = useLocation()
  return <nav className="qt2-secondary-nav" aria-label="Secondary"><NavLink to="/bot-runs" className={({ isActive }) => 'qt2-roomnav-btn ' + (isActive || location.pathname.startsWith('/operations/runs/') ? 'is-active' : '')}><Activity size={18} /><span>Bot runs</span></NavLink><NavLink to="/collectors" className={({ isActive }) => 'qt2-roomnav-btn ' + (isActive || location.pathname.startsWith('/operations/market/') ? 'is-active' : '')}><Radio size={18} /><span>Collectors</span></NavLink></nav>
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
  const location = useLocation()
  return (
    <nav className="qt2-roomnav" aria-label="Primary">
      {ROOMS.map((room) => {
        const Icon = room.icon
        return (
          <NavLink
            key={room.id}
            to={room.to}
            className={({ isActive }) => "qt2-roomnav-btn " + (isActive || (room.id === 'studies' && location.pathname.startsWith('/operations/research/')) ? "is-active" : "")}
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
  if (label === 'Overview') {
    return <RoomLoadingFrame room="Overview" />
  }
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
            <div className="qt2-brand-kicker">Research</div>
          </div>
        </div>
        <RoomNav collapsed={sidebarCollapsed} /><SecondaryNav />
        <div className="qt2-sidebar-foot">
          <StatusPill />
          <details className="qt2-settings-menu"><summary aria-label="Settings"><Settings size={17} /><span>Settings</span></summary><div>
            <NavLink to="/settings/storage" onClick={event => event.currentTarget.closest('details').removeAttribute('open')}><HardDrive size={17} /><span>Storage</span></NavLink>
            <button type="button" onClick={event => { event.currentTarget.closest('details').removeAttribute('open'); setSettingsOpen(true) }} aria-label="Open appearance settings"><Settings size={17} /><span>Appearance</span></button>
          </div></details>
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
          <Route path="/settings/storage" element={<Suspense fallback={<RoomFallback label="Storage" />}><StorageRoom /></Suspense>} />
          <Route path="/overview" element={<Suspense fallback={<RoomFallback label="Overview" />}><OverviewRoom /></Suspense>} />
          <Route path="/studies" element={<Suspense fallback={<RoomFallback label="Studies" />}><StudiesRoom /></Suspense>} />
          <Route path="/studies/:itemId" element={<Suspense fallback={<RoomFallback label="Study" />}><ResearchEvidenceRoom /></Suspense>} />
          <Route path="/bot-runs" element={<Suspense fallback={<RoomFallback label="Bot runs" />}><OperationsRoom domain="runs" /></Suspense>} />
          <Route path="/collectors" element={<Suspense fallback={<RoomFallback label="Collectors" />}><OperationsRoom domain="market" /></Suspense>} />
          <Route path="/operations" element={<LegacyOperations />} />
          <Route path="/operations/runs/:runId" element={<Suspense fallback={<RoomFallback label="BotLens" />}><BotLensRoom /></Suspense>} />
          <Route path="/operations/market/:collectorKind/:collectorId" element={<Suspense fallback={<RoomFallback label="collector operations" />}><CollectorLensRoom /></Suspense>} />
          <Route path="/operations/research/:itemId" element={<Suspense fallback={<RoomFallback label="research evidence" />}><ResearchEvidenceRoom /></Suspense>} />

          <Route path="/fleet" element={<Navigate to="/operations" replace />} />
          <Route path="/fleet/bots/:botId" element={<Navigate to="/operations?tab=definitions" replace />} />
          <Route path="/studio/*" element={<Navigate to="/overview" replace />} />
          <Route path="/research/*" element={<Navigate to="/studies" replace />} />
          <Route path="/memory/*" element={<Navigate to="/studies" replace />} />
          <Route path="/vault/*" element={<Navigate to="/overview" replace />} />
          <Route path="/reports/*" element={<Navigate to="/bot-runs" replace />} />
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
