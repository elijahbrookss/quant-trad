import { Suspense, lazy, useEffect, useState } from 'react'
import { BrowserRouter, NavLink, Navigate, Route, Routes, useParams } from 'react-router-dom'
import { ChartStateProvider } from '../contexts/ChartStateContext.jsx'
import { usePortalSettings } from '../contexts/PortalSettingsContext.jsx'
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

const ROOMS = [
  { id: 'overview', label: 'Overview', to: '/overview' },
  { id: 'operations', label: 'Operations', to: '/operations' },
]

function LegacyCollectorRedirect() {
  const { definitionId } = useParams()
  return <Navigate to={'/operations/collectors/' + definitionId} replace />
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
      {label}
    </span>
  )
}

function RoomNav() {
  return (
    <nav className="qt2-roomnav" aria-label="Primary">
      {ROOMS.map((room) => (
        <NavLink
          key={room.id}
          to={room.to}
          className={({ isActive }) => 'qt2-roomnav-btn ' + (isActive ? 'is-active' : '')}
        >
          {room.label}
        </NavLink>
      ))}
    </nav>
  )
}

function RoomFallback({ label }) {
  return <div className="qt2-room-loading">Loading {label}…</div>
}

function AppV2Shell() {
  const { settings } = usePortalSettings()
  const motionClass = settings.motion === 'reduced' ? 'app-motion-reduced' : ''

  return (
    <div className={'qt2-shell ' + motionClass}>
      <header className="qt2-topbar">
        <div className="qt2-brand">
          <span className="qt2-brand-mark">QT</span>
          <div>
            <div className="qt2-brand-title">QuantTrad</div>
            <div className="qt2-brand-kicker">Operator console</div>
          </div>
        </div>
        <RoomNav />
        <StatusPill />
      </header>

      <main className="qt2-main">
        <Routes>
          <Route path="/" element={<Navigate to="/overview" replace />} />
          <Route path="/overview" element={<Suspense fallback={<RoomFallback label="Overview" />}><OverviewRoom /></Suspense>} />
          <Route path="/operations" element={<Suspense fallback={<RoomFallback label="Operations" />}><OperationsRoom /></Suspense>} />
          <Route path="/operations/runs/:runId" element={<Suspense fallback={<RoomFallback label="BotLens" />}><BotLensRoom /></Suspense>} />
          <Route path="/operations/collectors/:definitionId" element={<Suspense fallback={<RoomFallback label="collector evidence" />}><CollectorLensRoom /></Suspense>} />
          <Route path="/operations/research/:itemId" element={<Suspense fallback={<RoomFallback label="research evidence" />}><ResearchEvidenceRoom /></Suspense>} />

          <Route path="/fleet" element={<Navigate to="/operations" replace />} />
          <Route path="/fleet/bots/:botId" element={<Navigate to="/operations?tab=definitions" replace />} />
          <Route path="/fleet/collectors/:definitionId" element={<LegacyCollectorRedirect />} />
          <Route path="/studio/*" element={<Navigate to="/overview" replace />} />
          <Route path="/research/*" element={<Navigate to="/operations?tab=research" replace />} />
          <Route path="/memory/*" element={<Navigate to="/operations?tab=research" replace />} />
          <Route path="/vault/*" element={<Navigate to="/overview" replace />} />
          <Route path="/reports/*" element={<Navigate to="/operations?tab=runs" replace />} />
          <Route path="*" element={<Navigate to="/overview" replace />} />
        </Routes>
      </main>
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
