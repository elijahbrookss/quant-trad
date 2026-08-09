import { Link } from 'react-router-dom'

const TONE_CLASS = {
  emerald: 'qt2-tone-emerald',
  amber: 'qt2-tone-amber',
  sky: 'qt2-tone-sky',
  rose: 'qt2-tone-rose',
  slate: 'qt2-tone-slate',
}

/**
 * Overview's "active workload" card — bots, backtests, and collectors, one
 * per card, sized larger than Fleet's compact list cards (this is the
 * landing page's hero content, not a dense index). "Workload," not
 * "container": collector definitions aren't containers, and their process
 * liveness is explicitly unknown (see collectorHealth.js).
 */
export function ActiveWorkloadCard({ entry }) {
  if (entry.kind === 'collector') {
    const { vm } = entry
    const toneClass = TONE_CLASS[vm.display.tone] || TONE_CLASS.slate
    return (
      <div className={`qt2-workload-card ${toneClass}`}>
        <div className="qt2-workload-card-head">
          <span className="qt2-workload-card-kind">Collector</span>
          <span className="qt2-fleet-card-status">{vm.statusLabel}</span>
        </div>
        <div className="qt2-workload-card-title">{vm.displayName}</div>
        <div className="qt2-fleet-card-sub">
          {vm.instrumentLabel} · {vm.venueLabel} · {vm.cadenceLabel}
        </div>
        <div className="qt2-fleet-card-detail">{vm.statusDetail}</div>
        <Link to={`/fleet/collectors/${vm.id}`} state={{ from: '/overview' }} className="qt2-fleet-card-lens">
          Open Lens
        </Link>
      </div>
    )
  }

  const { vm, bot } = entry
  const toneClass = TONE_CLASS[vm.display.tone] || TONE_CLASS.slate
  const netPnlStat = vm.metricStats.find((stat) => stat.key === 'net-pnl')
  const openTradesStat = vm.metricStats.find((stat) => stat.key === 'open-trades')

  return (
    <div className={`qt2-workload-card ${toneClass}`}>
      <div className="qt2-workload-card-head">
        <span className="qt2-workload-card-kind">{entry.kind === 'backtest' ? 'Backtest' : 'Bot'}</span>
        <span className="qt2-fleet-card-status">{vm.statusLabel}</span>
        <span className="qt2-fleet-card-mode">{vm.runMode.label}</span>
      </div>
      <div className="qt2-workload-card-title">{vm.strategyLabel}</div>
      <div className="qt2-fleet-card-sub">
        {vm.symbolsLabel} · {vm.timeframeLabel}
      </div>
      <div className="qt2-fleet-card-detail">{vm.statusDetail}</div>
      <div className="qt2-fleet-card-stats">
        <span>P&amp;L <strong>{netPnlStat?.value ?? '—'}</strong></span>
        <span>Open <strong>{openTradesStat?.value ?? '0'}</strong></span>
      </div>
      {vm.display.controls?.canOpenLens ? (
        <Link to={`/fleet/bots/${bot.id}`} state={{ bot, from: '/overview' }} className="qt2-fleet-card-lens">
          Open Lens
        </Link>
      ) : null}
    </div>
  )
}
