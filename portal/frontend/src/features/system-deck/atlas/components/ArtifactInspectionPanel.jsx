import { Crosshair, X } from 'lucide-react'

function formatCurrency(value) {
  const sign = value > 0 ? '+' : ''
  return `${sign}${value.toLocaleString(undefined, { maximumFractionDigits: 2, minimumFractionDigits: 2 })}`
}

function formatPercent(value) {
  return `${Math.round(value * 100)}%`
}

export function ArtifactInspectionPanel({ artifact, onClose, onFocus }) {
  if (!artifact) return null
  const run = artifact.run
  const pnlTone = run.pnl >= 0 ? 'positive' : 'negative'

  return (
    <aside className="atlas-inspector" aria-label="Atlas artifact inspection">
      <header className="atlas-inspector-header">
        <div>
          <span className="atlas-kicker">Artifact</span>
          <h2>{run.id}</h2>
        </div>
        <div className="atlas-inspector-actions">
          <button type="button" className="atlas-icon-button" onClick={() => onFocus(artifact.id)} aria-label="Focus artifact">
            <Crosshair size={15} />
          </button>
          <button type="button" className="atlas-icon-button" onClick={onClose} aria-label="Close artifact inspection">
            <X size={15} />
          </button>
        </div>
      </header>

      <div className="atlas-inspector-identity">
        <strong>{run.strategy}</strong>
        <span>{artifact.district.label}</span>
      </div>

      <dl className="atlas-metric-grid">
        <div>
          <dt>P&L</dt>
          <dd data-tone={pnlTone}>{formatCurrency(run.pnl)}</dd>
        </div>
        <div>
          <dt>Drawdown</dt>
          <dd>{formatCurrency(-Math.abs(run.drawdown))}</dd>
        </div>
        <div>
          <dt>Trades</dt>
          <dd>{run.tradeCount}</dd>
        </div>
        <div>
          <dt>Win rate</dt>
          <dd>{formatPercent(run.winRate)}</dd>
        </div>
        <div>
          <dt>Timeframe</dt>
          <dd>{run.timeframe.toUpperCase()}</dd>
        </div>
        <div>
          <dt>Status</dt>
          <dd>{run.status}</dd>
        </div>
      </dl>

      <section className="atlas-inspector-section">
        <span className="atlas-kicker">Symbols</span>
        <div className="atlas-symbol-list">
          {run.symbols.map((symbol) => (
            <span key={symbol}>{symbol}</span>
          ))}
        </div>
      </section>

      <section className="atlas-inspector-section">
        <span className="atlas-kicker">Generation</span>
        <div className="atlas-generation-readout">
          <span>family={artifact.family}</span>
          <span>height={artifact.height.toFixed(2)}</span>
          <span>damage={artifact.damage.toFixed(2)}</span>
          <span>windows={artifact.windows.length}</span>
        </div>
      </section>
    </aside>
  )
}
