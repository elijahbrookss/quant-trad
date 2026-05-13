import { useEffect, useMemo, useState } from 'react'
import {
  Activity,
  ArrowRight,
  Bot,
  Box,
  CheckCircle2,
  ChevronLeft,
  ChevronRight,
  Clock,
  Database,
  FileText,
  FlaskConical,
  Gamepad2,
  GitBranch,
  Layers,
  Network,
  Pause,
  Play,
  RefreshCw,
  Shield,
  Sparkles,
  Wallet,
  Zap,
} from 'lucide-react'
import './SystemDeck.css'

const pipelineStages = [
  {
    id: 'provider',
    label: 'Provider Data',
    short: 'OHLCV',
    icon: Database,
    color: 'amber',
    proof: 'Ordered candles and canonical instrument context enter the runtime boundary.',
    invariant: 'No unresolved provider state is allowed inside indicator config.',
  },
  {
    id: 'indicator',
    label: 'Indicator Engine',
    short: 'State',
    icon: Activity,
    color: 'green',
    proof: 'Each indicator owns its state and advances one bar at a time.',
    invariant: 'initialize -> apply_bar -> snapshot',
  },
  {
    id: 'outputs',
    label: 'Typed Outputs',
    short: 'Truth',
    icon: Box,
    color: 'cyan',
    proof: 'Strategy-visible outputs, chart overlays, and debugger details remain separate.',
    invariant: 'Strategies never inspect overlays or internals.',
  },
  {
    id: 'strategy',
    label: 'Strategy Decision',
    short: 'Rules',
    icon: Layers,
    color: 'violet',
    proof: 'Compiled strategy rules consume typed outputs and emit decision artifacts.',
    invariant: 'Readonly, pure decision evaluation.',
  },
  {
    id: 'bot',
    label: 'Bot Runtime',
    short: 'Execute',
    icon: Bot,
    color: 'rose',
    proof: 'Execution realism, fills, fees, risk, intrabar handling, and lifecycle live here.',
    invariant: 'The bot owns execution outcomes.',
  },
  {
    id: 'ledger',
    label: 'Event Ledger',
    short: 'Causality',
    icon: GitBranch,
    color: 'orange',
    proof: 'Runtime facts are append-only events with parent/root/correlation links.',
    invariant: 'Views are derived from events, not alternate truth.',
  },
  {
    id: 'botlens',
    label: 'BotLens',
    short: 'Debug',
    icon: Network,
    color: 'blue',
    proof: 'Run and symbol projectors turn durable facts into live and historical inspection state.',
    invariant: 'Playback is a debugger, not a second engine.',
  },
]

const slides = [
  {
    id: 'opening',
    eyebrow: 'System Story',
    title: 'Quant-Trad is an explainable walk-forward trading platform.',
    summary:
      'The system is built around one idea: every research artifact, strategy decision, trade outcome, report, and playback view should be explainable from the same bar-by-bar runtime timeline.',
    status: 'Active development',
    accent: 'cyan',
    visual: 'orbit',
    bullets: [
      'QuantLab explores indicators and overlays.',
      'Strategy turns typed indicator outputs into readonly decisions.',
      'Bot runtime owns execution realism, fills, risk, wallet, and lifecycle.',
      'BotLens and reports inspect what the runtime actually did.',
    ],
    callout: 'Valuable now: the architecture already protects causality, explainability, and semantic drift.',
  },
  {
    id: 'layers',
    eyebrow: 'Layer Ownership',
    title: 'Research, decision logic, execution, and playback are intentionally separate.',
    summary:
      'Each layer has a narrow job. That keeps a visual overlay from quietly becoming a strategy input, and keeps playback from becoming a second source of execution truth.',
    status: 'Core invariant',
    accent: 'green',
    visual: 'flow',
    bullets: [
      'QuantLab is research only.',
      'Strategy is decision logic only.',
      'Bot runtime is execution plus realism only.',
      'Playback reveals causality after the fact.',
    ],
    callout: 'If two surfaces disagree, the runtime timeline wins.',
  },
  {
    id: 'timeline',
    eyebrow: 'Canonical Timeline',
    title: 'The key primitive is not a chart. It is a state engine timeline.',
    summary:
      'All derived outputs are expected to follow one sequence: initialize runtime state, apply one bar, publish snapshots, evaluate decisions, append facts, and project views.',
    status: 'Single-path rule',
    accent: 'amber',
    visual: 'timeline',
    bullets: [
      'Indicators mutate owned state through apply_bar.',
      'snapshot publishes typed outputs for downstream truth.',
      'overlay_snapshot publishes chart read models from the same state.',
      'Bot runtime and BotLens consume the same bar result, not hidden internals.',
    ],
    callout: 'Nothing should snap into existence retroactively.',
  },
  {
    id: 'indicators',
    eyebrow: 'Indicator Engine',
    title: 'Indicators are small state machines with explicit public surfaces.',
    summary:
      'The best indicators separate source facts, evidence, committed state, trust semantics, and overlays. Market Profile and Regime are the rich examples; Candle Stats is the lean metric example.',
    status: 'Runtime contract',
    accent: 'teal',
    visual: 'stack',
    bullets: [
      'Typed outputs are the only strategy-visible truth.',
      'Overlays are chart read models owned by the indicator.',
      'Details are optional debugger/operator payloads.',
      'known_at timing is part of the product surface.',
    ],
    callout: 'Market Profile may precompute immutable source facts, but final state still forms walk-forward.',
  },
  {
    id: 'strategy',
    eyebrow: 'Decision Layer',
    title: 'Strategy preview and bot runtime share the same decision semantics.',
    summary:
      'A persisted StrategySpec is compiled into a concrete executable spec. The evaluator reads current-bar typed outputs and emits decision artifacts plus at most one selected candidate.',
    status: 'North star',
    accent: 'violet',
    visual: 'decision',
    bullets: [
      'The evaluator is pure and readonly.',
      'Rules consume typed outputs, never overlays.',
      'Preview is a replay surface, not a parallel grammar.',
      'Execution-facing StrategySignal is mapped explicitly from the selected decision artifact.',
    ],
    callout: 'Strategy decides what should happen. Bot runtime decides what can actually happen.',
  },
  {
    id: 'runtime',
    eyebrow: 'Bot Runtime',
    title: 'The bot turns decisions into realistic execution outcomes.',
    summary:
      'Runtime preparation builds series state and indicator engines. The step loop processes candles, evaluates rules, emits facts, gates entries through wallet/risk, handles intrabar refinement, persists events, and publishes derived views.',
    status: 'Execution truth',
    accent: 'rose',
    visual: 'loop',
    bullets: [
      'StartContext exists before warm-up facts.',
      'RunContext owns run status, wallet gateway, and runtime events.',
      'SharedWalletGateway protects capital across symbol workers.',
      'LadderRiskEngine owns fill, fee, slippage, PnL, and trade identity logic.',
    ],
    callout: 'The runtime is allowed to reject a good-looking signal for explicit risk, wallet, or lifecycle reasons.',
  },
  {
    id: 'services',
    eyebrow: 'Service Orchestration',
    title: 'The backend starts and supervises runtime work through explicit seams.',
    summary:
      'BotRuntimeControlService delegates startup to an orchestrator, DockerBotRunner launches container_runtime.py, and the container supervises one symbol worker per shard with shared wallet coordination.',
    status: 'Docker target',
    accent: 'orange',
    visual: 'service',
    bullets: [
      'Backend owns run_id before container launch.',
      'container_runtime.py supervises workers, lifecycle, status, telemetry, and step traces.',
      'Worker runtime receives explicit BotRuntimeDeps instead of importing portal services directly.',
      'Mode-aware seams exist for backtest, paper, and live composition.',
    ],
    callout: 'Startup failures preserve run identity and lifecycle detail instead of leaving vague starting state.',
  },
  {
    id: 'botlens',
    eyebrow: 'BotLens',
    title: 'BotLens is the runtime debugger and projection layer.',
    summary:
      'Committed BotLens domain facts are persisted before transport. Run and symbol projectors build live state, websocket deltas maintain viewers, and historical reads stay separate from live projection.',
    status: 'Debugger surface',
    accent: 'blue',
    visual: 'botlens',
    bullets: [
      'Durable truth is botlens_domain.* rows.',
      'Run projection owns lifecycle, health, faults, catalog, and open trades.',
      'Symbol projection owns candles, overlays, signals, decisions, trades, diagnostics, and stats.',
      'Selected-symbol switching reads projector snapshots and replays proven deltas.',
    ],
    callout: 'BotLens should never invent a base state to hide a projection gap.',
  },
  {
    id: 'storage',
    eyebrow: 'Storage, Reports, Observability',
    title: 'Persistence is a boundary, not a backdoor reconstruction engine.',
    summary:
      'The repo uses one PG_DSN, explicit storage gateways, append-only runtime events, lifecycle rows for durable startup truth, and run-scoped report artifact bundles with provenance.',
    status: 'Audit trail',
    accent: 'slate',
    visual: 'reports',
    bullets: [
      'Runtime services consume storage through injectable boundaries.',
      'Runtime-event rows carry hot columns for common query dimensions.',
      'Report bundles are rooted by bot_id and run_id.',
      'Post-run enrichment is allowed only with explicit provenance.',
    ],
    callout: 'Indicator history belongs in the run bundle, not a shadow indicator-history table.',
  },
  {
    id: 'state',
    eyebrow: 'Current State',
    title: 'This is active development, but the core spine is already strong.',
    summary:
      'The codebase is still evolving. Some APIs and internals are changing, but the valuable center is clear: strict contracts, causality-first runtime design, explicit boundaries, and a serious debugger story.',
    status: 'Still moving',
    accent: 'lime',
    visual: 'northstar',
    bullets: [
      'Valuable: typed indicator contracts, strategy/runtime parity, runtime event causality, BotLens projectors, report provenance.',
      'Still evolving: mode-specific runtime branches, report/deepdive cleanup, UI contract polish, provider/live hardening.',
      'Non-negotiable: fail loud, preserve known-at timing, keep one runtime state-engine timeline.',
      'Operator caution: this is explainability-first infrastructure, not a polished capital deployment product.',
    ],
    callout: 'The system is worth explaining because the hard part is the semantic discipline, not only the UI.',
  },
]

const layerNodes = [
  { label: 'QuantLab', detail: 'Research lens', icon: FlaskConical, tone: 'green' },
  { label: 'Strategy', detail: 'Decision lens', icon: Layers, tone: 'violet' },
  { label: 'Bot', detail: 'Execution lens', icon: Bot, tone: 'rose' },
  { label: 'Playback', detail: 'Audit lens', icon: Network, tone: 'blue' },
]

const timelineNodes = ['initialize', 'apply_bar', 'snapshot', 'decision', 'runtime event', 'projection']

const stackNodes = [
  { label: 'Source facts', detail: 'provider candles, dependencies, immutable profile facts' },
  { label: 'Evidence', detail: 'metrics, scores, structural observations' },
  { label: 'Committed state', detail: 'public state, transitions, signal policy' },
  { label: 'Trust', detail: 'known_at, maturity, stability, readiness' },
  { label: 'Overlay projection', detail: 'chart read model from committed state' },
]

const runtimeLoopNodes = [
  'fetch bar',
  'step indicators',
  'evaluate rules',
  'append facts',
  'wallet gate',
  'settle trade',
  'project state',
]

const serviceNodes = [
  { label: 'API', detail: 'start/stop and validation' },
  { label: 'Startup Orchestrator', detail: 'run_id, lifecycle, readiness' },
  { label: 'Docker Runner', detail: 'container target and env contract' },
  { label: 'Container Runtime', detail: 'symbol sharding and supervision' },
  { label: 'Symbol Workers', detail: 'one series timeline per worker' },
  { label: 'Telemetry Stream', detail: 'projection intake and live fanout' },
]

const botlensNodes = [
  { label: 'Domain rows', detail: 'durable botlens_domain.* truth' },
  { label: 'Run projector', detail: 'lifecycle, health, catalog, open trades' },
  { label: 'Symbol projector', detail: 'candles, overlays, signals, decisions, trades' },
  { label: 'Websocket deltas', detail: 'transport-only live concern updates' },
  { label: 'Retrieval', detail: 'chart history and forensics stay separate' },
]

const reportNodes = [
  { label: 'PG_DSN', detail: 'single persistence DSN' },
  { label: 'Event ledger', detail: 'append-only runtime causality' },
  { label: 'Lifecycle rows', detail: 'startup and terminal truth' },
  { label: 'Run bundle', detail: 'artifact files with provenance' },
  { label: 'Grafana/Loki', detail: 'operator observability surfaces' },
]

const northstarNodes = [
  { label: 'Keep', detail: 'one runtime timeline' },
  { label: 'Harden', detail: 'paper/live branches and provider edges' },
  { label: 'Explain', detail: 'projection gaps and failure semantics' },
  { label: 'Ship', detail: 'focused workflows from existing contracts' },
]

const colorText = {
  amber: 'text-amber-200',
  blue: 'text-sky-200',
  cyan: 'text-cyan-200',
  green: 'text-emerald-200',
  lime: 'text-lime-200',
  orange: 'text-orange-200',
  rose: 'text-rose-200',
  slate: 'text-slate-200',
  teal: 'text-teal-200',
  violet: 'text-violet-200',
}

function DeckButton({ children, icon: Icon, onClick, disabled, label }) {
  return (
    <button
      type="button"
      className="qt-deck-button"
      onClick={onClick}
      disabled={disabled}
      aria-label={label}
      title={label}
    >
      {Icon ? <Icon className="size-4" /> : null}
      {children}
    </button>
  )
}

function SlideRail({ activeIndex, onSelect }) {
  return (
    <div className="qt-deck-rail" aria-label="System deck slides">
      {slides.map((slide, index) => (
        <button
          key={slide.id}
          type="button"
          className="qt-deck-rail-item"
          data-active={index === activeIndex}
          onClick={() => onSelect(index)}
          aria-label={slide.title}
          title={slide.title}
        >
          <span className="qt-deck-rail-index">{String(index + 1).padStart(2, '0')}</span>
          <span className="qt-deck-rail-copy">
            <span>{slide.eyebrow}</span>
            <strong>{slide.status}</strong>
          </span>
        </button>
      ))}
    </div>
  )
}

function FlowDiagram() {
  return (
    <div className="qt-flow-diagram">
      {layerNodes.map((node, index) => {
        const Icon = node.icon
        return (
          <div className="qt-flow-step-wrap" key={node.label}>
            <div className="qt-flow-step" data-tone={node.tone}>
              <Icon className="size-5" />
              <span>{node.label}</span>
              <small>{node.detail}</small>
            </div>
            {index < layerNodes.length - 1 ? (
              <ArrowRight className="qt-flow-arrow size-5" />
            ) : null}
          </div>
        )
      })}
    </div>
  )
}

function TimelineDiagram() {
  return (
    <div className="qt-timeline-diagram">
      <div className="qt-timeline-line" />
      {timelineNodes.map((node, index) => (
        <div
          key={node}
          className="qt-timeline-node"
          style={{ '--delay': `${index * 120}ms` }}
        >
          <span>{index + 1}</span>
          <strong>{node}</strong>
        </div>
      ))}
    </div>
  )
}

function StackDiagram() {
  return (
    <div className="qt-stack-diagram">
      {stackNodes.map((node, index) => (
        <div
          key={node.label}
          className="qt-stack-layer"
          style={{ '--layer': index }}
        >
          <strong>{node.label}</strong>
          <span>{node.detail}</span>
        </div>
      ))}
    </div>
  )
}

function DecisionDiagram() {
  return (
    <div className="qt-decision-diagram">
      <div className="qt-decision-source">
        <Activity className="size-6" />
        <strong>Typed outputs</strong>
        <span>current-bar output map</span>
      </div>
      <ArrowRight className="qt-decision-arrow size-5" />
      <div className="qt-decision-core">
        <div>
          <Shield className="size-5" />
          <span>Compiler validates refs</span>
        </div>
        <div>
          <Zap className="size-5" />
          <span>Evaluator emits artifacts</span>
        </div>
        <div>
          <CheckCircle2 className="size-5" />
          <span>Selected candidate is explicit</span>
        </div>
      </div>
      <ArrowRight className="qt-decision-arrow size-5" />
      <div className="qt-decision-source qt-decision-signal">
        <GitBranch className="size-6" />
        <strong>StrategySignal</strong>
        <span>execution-facing mapper output</span>
      </div>
    </div>
  )
}

function LoopDiagram() {
  return (
    <div className="qt-loop-diagram">
      {runtimeLoopNodes.map((node, index) => (
        <div
          key={node}
          className="qt-loop-node"
          style={{ '--angle': `${index * (360 / runtimeLoopNodes.length)}deg` }}
        >
          <span>{index + 1}</span>
          <strong>{node}</strong>
        </div>
      ))}
      <div className="qt-loop-core">
        <Bot className="size-7" />
        <span>BotRuntime</span>
      </div>
    </div>
  )
}

function LadderDiagram({ nodes }) {
  return (
    <div className="qt-ladder-diagram">
      {nodes.map((node, index) => (
        <div className="qt-ladder-row" key={node.label}>
          <span>{String(index + 1).padStart(2, '0')}</span>
          <div>
            <strong>{node.label}</strong>
            <small>{node.detail}</small>
          </div>
        </div>
      ))}
    </div>
  )
}

function OrbitDiagram() {
  return (
    <div className="qt-orbit-diagram">
      <div className="qt-orbit-ring qt-orbit-ring-one" />
      <div className="qt-orbit-ring qt-orbit-ring-two" />
      <div className="qt-orbit-core">
        <img src="/qt-mark.svg" alt="" />
        <strong>single runtime truth</strong>
      </div>
      {pipelineStages.slice(0, 6).map((stage, index) => {
        const Icon = stage.icon
        return (
          <div
            key={stage.id}
            className="qt-orbit-node"
            data-color={stage.color}
            style={{ '--angle': `${index * 60}deg` }}
          >
            <Icon className="size-5" />
          </div>
        )
      })}
    </div>
  )
}

function SlideVisual({ slide }) {
  if (slide.visual === 'orbit') return <OrbitDiagram />
  if (slide.visual === 'flow') return <FlowDiagram />
  if (slide.visual === 'timeline') return <TimelineDiagram />
  if (slide.visual === 'stack') return <StackDiagram />
  if (slide.visual === 'decision') return <DecisionDiagram />
  if (slide.visual === 'loop') return <LoopDiagram />
  if (slide.visual === 'service') return <LadderDiagram nodes={serviceNodes} />
  if (slide.visual === 'botlens') return <LadderDiagram nodes={botlensNodes} />
  if (slide.visual === 'reports') return <LadderDiagram nodes={reportNodes} />
  return <LadderDiagram nodes={northstarNodes} />
}

function RuntimeArena({ stageIndex, onStage, autoRun, onToggleAuto, onReset }) {
  const stage = pipelineStages[stageIndex]
  const progress = Math.round(((stageIndex + 1) / pipelineStages.length) * 100)
  const tokenX = 9 + stageIndex * 13.6
  const tokenY = stageIndex % 2 === 0 ? 30 : 58

  return (
    <section className="qt-runtime-arena" aria-label="Runtime arena">
      <header className="qt-runtime-arena-header">
        <div>
          <span className="qt-mini-kicker">Runtime Arena</span>
          <h3>{stage.label}</h3>
        </div>
        <div className="qt-runtime-arena-actions">
          <button
            type="button"
            className="qt-icon-button"
            onClick={onToggleAuto}
            aria-label={autoRun ? 'Pause arena' : 'Play arena'}
            title={autoRun ? 'Pause arena' : 'Play arena'}
          >
            {autoRun ? <Pause className="size-4" /> : <Play className="size-4" />}
          </button>
          <button
            type="button"
            className="qt-icon-button"
            onClick={onReset}
            aria-label="Reset arena"
            title="Reset arena"
          >
            <RefreshCw className="size-4" />
          </button>
        </div>
      </header>

      <div className="qt-arena-board">
        <div className="qt-arena-plane">
          <div className="qt-arena-grid" />
          <div
            className="qt-arena-token"
            style={{
              '--token-x': `${tokenX}%`,
              '--token-y': `${tokenY}%`,
            }}
          >
            <span />
          </div>
          {pipelineStages.map((item, index) => {
            const Icon = item.icon
            const x = 9 + index * 13.6
            const y = index % 2 === 0 ? 30 : 58
            return (
              <button
                key={item.id}
                type="button"
                className="qt-arena-node"
                data-active={index === stageIndex}
                data-complete={index <= stageIndex}
                data-color={item.color}
                style={{
                  '--node-x': `${x}%`,
                  '--node-y': `${y}%`,
                }}
                onClick={() => onStage(index)}
                aria-label={item.label}
                title={item.label}
              >
                <Icon className="size-4" />
              </button>
            )
          })}
        </div>
      </div>

      <div className="qt-arena-status">
        <div className="qt-arena-progress">
          <span style={{ width: `${progress}%` }} />
        </div>
        <div className="qt-arena-fact">
          <strong>{stage.short}</strong>
          <p>{stage.proof}</p>
        </div>
        <div className="qt-arena-invariant">
          <Shield className="size-4" />
          <span>{stage.invariant}</span>
        </div>
      </div>
    </section>
  )
}

export default function SystemDeck() {
  const [activeIndex, setActiveIndex] = useState(0)
  const [stageIndex, setStageIndex] = useState(0)
  const [autoRun, setAutoRun] = useState(true)

  const activeSlide = slides[activeIndex]
  const activeColorClass = colorText[activeSlide.accent] || colorText.cyan

  const nextSlide = () => setActiveIndex((index) => Math.min(index + 1, slides.length - 1))
  const previousSlide = () => setActiveIndex((index) => Math.max(index - 1, 0))
  const nextStage = () => setStageIndex((index) => (index + 1) % pipelineStages.length)

  useEffect(() => {
    if (!autoRun) return undefined
    const intervalId = window.setInterval(() => {
      setStageIndex((index) => (index + 1) % pipelineStages.length)
    }, 2200)
    return () => window.clearInterval(intervalId)
  }, [autoRun])

  useEffect(() => {
    const handleKeyDown = (event) => {
      if (event.key === 'ArrowRight') {
        nextSlide()
      }
      if (event.key === 'ArrowLeft') {
        previousSlide()
      }
      if (event.key === ' ') {
        event.preventDefault()
        nextStage()
      }
    }
    window.addEventListener('keydown', handleKeyDown)
    return () => window.removeEventListener('keydown', handleKeyDown)
  }, [])

  const slideProgress = useMemo(() => {
    return `${Math.round(((activeIndex + 1) / slides.length) * 100)}%`
  }, [activeIndex])

  return (
    <div className="qt-system-deck">
      <div className="qt-system-deck-topline">
        <div className="qt-system-brand">
          <span className="qt-system-brand-mark">
            <img src="/qt-mark.svg" alt="" />
          </span>
          <div>
            <span>Quant-Trad</span>
            <strong>System Deck</strong>
          </div>
        </div>
        <div className="qt-system-progress" aria-hidden="true">
          <span style={{ width: slideProgress }} />
        </div>
      </div>

      <div className="qt-system-deck-layout">
        <SlideRail activeIndex={activeIndex} onSelect={setActiveIndex} />

        <article className="qt-system-slide" data-accent={activeSlide.accent}>
          <div className="qt-slide-copy">
            <div className="qt-slide-meta">
              <span>{activeSlide.eyebrow}</span>
              <strong>{activeSlide.status}</strong>
            </div>
            <h1 className={activeColorClass}>{activeSlide.title}</h1>
            <p className="qt-slide-summary">{activeSlide.summary}</p>
            <div className="qt-slide-bullets">
              {activeSlide.bullets.map((bullet) => (
                <div key={bullet} className="qt-slide-bullet">
                  <CheckCircle2 className="size-4" />
                  <span>{bullet}</span>
                </div>
              ))}
            </div>
            <div className="qt-slide-callout">
              <Sparkles className="size-4" />
              <span>{activeSlide.callout}</span>
            </div>
          </div>

          <div className="qt-slide-visual">
            <SlideVisual slide={activeSlide} />
          </div>

          <footer className="qt-slide-footer">
            <DeckButton
              icon={ChevronLeft}
              onClick={previousSlide}
              disabled={activeIndex === 0}
              label="Previous slide"
            >
              Previous
            </DeckButton>
            <div className="qt-slide-counter">
              <Clock className="size-4" />
              <span>{String(activeIndex + 1).padStart(2, '0')} / {String(slides.length).padStart(2, '0')}</span>
            </div>
            <DeckButton
              icon={ChevronRight}
              onClick={nextSlide}
              disabled={activeIndex === slides.length - 1}
              label="Next slide"
            >
              Next
            </DeckButton>
          </footer>
        </article>

        <aside className="qt-system-side">
          <RuntimeArena
            stageIndex={stageIndex}
            onStage={setStageIndex}
            autoRun={autoRun}
            onToggleAuto={() => setAutoRun((value) => !value)}
            onReset={() => setStageIndex(0)}
          />

          <section className="qt-system-principles">
            <header>
              <Gamepad2 className="size-4" />
              <span>Core Components</span>
            </header>
            <div className="qt-principle-grid">
              <div>
                <strong>Engine</strong>
                <span>state, snapshots, overlays</span>
              </div>
              <div>
                <strong>Runtime</strong>
                <span>signals, risk, fills, events</span>
              </div>
              <div>
                <strong>Projectors</strong>
                <span>run state and symbol state</span>
              </div>
              <div>
                <strong>Reports</strong>
                <span>artifact bundles and provenance</span>
              </div>
            </div>
          </section>

          <section className="qt-system-source-note">
            <FileText className="size-4" />
            <span>Built from the platform contracts, architecture docs, runtime package map, and BotLens service docs in this repo.</span>
          </section>
        </aside>
      </div>
    </div>
  )
}
