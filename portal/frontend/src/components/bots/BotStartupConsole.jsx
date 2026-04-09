import { memo, useEffect, useRef, useState } from 'react'
import { Activity, AlertTriangle, CheckCircle2, CircleDot, RadioTower, XCircle } from 'lucide-react'
import { buildBotStartupConsoleState } from './botStartupConsoleModel.js'

const ENTRY_TONE_CLASSES = {
  emerald: {
    dot: 'bg-emerald-300 shadow-[0_0_18px_rgba(74,222,128,0.28)]',
    label: 'text-emerald-200',
    message: 'text-emerald-50/95',
    meta: 'text-emerald-200/60',
    icon: CheckCircle2,
  },
  sky: {
    dot: 'bg-sky-300 shadow-[0_0_18px_rgba(56,189,248,0.24)]',
    label: 'text-sky-200',
    message: 'text-slate-100',
    meta: 'text-slate-400',
    icon: Activity,
  },
  amber: {
    dot: 'bg-amber-300 shadow-[0_0_18px_rgba(251,191,36,0.26)]',
    label: 'text-amber-200',
    message: 'text-amber-50/95',
    meta: 'text-amber-200/65',
    icon: AlertTriangle,
  },
  rose: {
    dot: 'bg-rose-300 shadow-[0_0_18px_rgba(251,113,133,0.28)]',
    label: 'text-rose-200',
    message: 'text-rose-50/95',
    meta: 'text-rose-200/70',
    icon: XCircle,
  },
  slate: {
    dot: 'bg-slate-500 shadow-[0_0_14px_rgba(148,163,184,0.16)]',
    label: 'text-slate-300',
    message: 'text-slate-200',
    meta: 'text-slate-500',
    icon: CircleDot,
  },
}

function toneClasses(tone) {
  return ENTRY_TONE_CLASSES[tone] || ENTRY_TONE_CLASSES.slate
}

function shortRunId(runId) {
  const normalized = String(runId || '').trim()
  return normalized ? normalized.slice(0, 8) : 'pending'
}

function buildEntryOpacity(index, total) {
  const distanceFromLatest = Math.max(0, total - index - 1)
  return Math.max(0.18, 1 - distanceFromLatest * 0.12)
}

function ActiveDots() {
  return (
    <span className="inline-flex items-center gap-1">
      {[0, 1, 2].map((index) => (
        <span
          key={index}
          className="h-1 w-1 rounded-full bg-current opacity-30 animate-pulse"
          style={{ animationDelay: `${index * 180}ms` }}
        />
      ))}
    </span>
  )
}

function LiveCaret() {
  return <span aria-hidden className="qt-console-caret inline-block h-[1.05em] w-px bg-current align-middle" />
}

const LifecycleStreamEntry = memo(function LifecycleStreamEntry({ entry, index, total, latestActive }) {
  const tone = toneClasses(entry.tone)
  const Icon = tone.icon
  const opacity = buildEntryOpacity(index, total)

  return (
    <li
      className="grid grid-cols-[auto_minmax(0,1fr)] gap-3 transition-transform duration-300"
      style={{ opacity, transform: `translateY(${Math.min(10, (total - index - 1) * -1)}px)` }}
    >
      <div className="relative flex flex-col items-center pt-1">
        <span className={`relative z-10 inline-flex h-2.5 w-2.5 rounded-full ${tone.dot}`} />
        {index < total - 1 ? (
          <span className="absolute left-1/2 top-4 h-[calc(100%+0.85rem)] w-px -translate-x-1/2 bg-gradient-to-b from-white/12 via-white/8 to-transparent" />
        ) : null}
      </div>
      <div className="min-w-0">
        <div className={`flex flex-wrap items-center gap-x-2 gap-y-1 qt-mono text-[10px] uppercase tracking-[0.24em] ${tone.label}`}>
          <span className="text-slate-600">{entry.timeLabel}</span>
          <span className="inline-flex items-center gap-1">
            <Icon className="size-3" />
            {entry.kind === 'series' ? 'Series' : 'Phase'}
          </span>
          {entry.symbol ? <span>[{entry.symbol}]</span> : <span>{entry.label}</span>}
          {latestActive ? <LiveCaret /> : null}
        </div>
        <p className={`mt-1 text-sm leading-relaxed ${tone.message}`}>{entry.message}</p>
        {entry.meta ? <p className={`mt-1 text-xs ${tone.meta}`}>{entry.meta}</p> : null}
      </div>
    </li>
  )
})

export const BotStartupConsole = memo(function BotStartupConsole({ bot }) {
  const [consoleState, setConsoleState] = useState(() => buildBotStartupConsoleState(null, bot))
  const streamRef = useRef(null)
  const autoFollowRef = useRef(true)

  useEffect(() => {
    setConsoleState((previous) => buildBotStartupConsoleState(previous, bot))
  }, [bot])

  useEffect(() => {
    const node = streamRef.current
    if (!node || !autoFollowRef.current) return
    requestAnimationFrame(() => {
      node.scrollTo({ top: node.scrollHeight, behavior: consoleState.current?.animated ? 'smooth' : 'auto' })
    })
  }, [consoleState.current?.animated, consoleState.current?.key, consoleState.entries.length])

  const handleScroll = () => {
    const node = streamRef.current
    if (!node) return
    const distance = node.scrollHeight - node.scrollTop - node.clientHeight
    autoFollowRef.current = distance < 28
  }

  const current = consoleState.current
  const currentTone = toneClasses(current?.tone)
  const showActivity = current?.animated
  const showResolved = current?.stable
  const showDegraded = current?.degraded

  return (
    <section className="relative overflow-hidden rounded-[1.2rem] bg-[linear-gradient(180deg,rgba(2,6,23,0.88)_0%,rgba(2,6,23,0.74)_58%,rgba(15,23,42,0.42)_100%)] px-4 py-4 shadow-[inset_0_1px_0_rgba(148,163,184,0.05)]">
      <div className="pointer-events-none absolute inset-0 bg-[radial-gradient(circle_at_85%_18%,rgba(56,189,248,0.12),transparent_26%),radial-gradient(circle_at_12%_100%,rgba(14,165,233,0.08),transparent_34%)]" />
      {showActivity ? (
        <div className="qt-console-scan pointer-events-none absolute inset-x-6 bottom-4 h-20 rounded-full bg-[radial-gradient(circle,rgba(56,189,248,0.14)_0%,rgba(56,189,248,0.04)_44%,transparent_74%)]" />
      ) : null}

      <div className="relative">
        <div className="flex items-start justify-between gap-4">
          <div className="min-w-0 flex-1">
            <div className="flex items-center gap-2 text-[10px] font-semibold uppercase tracking-[0.3em] text-slate-500">
              <RadioTower className="size-3" />
              Activity Stream
            </div>
            <div className={`mt-2 flex flex-wrap items-center gap-2 text-sm font-medium ${currentTone.message}`}>
              <span className="inline-flex items-center gap-2 qt-mono text-[11px] uppercase tracking-[0.28em] text-slate-500">
                {current?.label || 'Standby'}
                {showActivity ? <ActiveDots /> : null}
              </span>
              {showResolved ? (
                <span className="inline-flex items-center gap-1 text-emerald-200/80">
                  <CheckCircle2 className="size-3.5" />
                  Stable
                </span>
              ) : null}
              {showDegraded ? (
                <span className="inline-flex items-center gap-1 text-amber-200/80">
                  <AlertTriangle className="size-3.5" />
                  Attention
                </span>
              ) : null}
            </div>
            <p className="mt-1 max-w-2xl text-sm leading-relaxed text-slate-200">{current?.message || 'No active lifecycle checkpoint.'}</p>
            {current?.meta ? <p className="mt-1 text-xs text-slate-500">{current.meta}</p> : null}
          </div>

          <div className="shrink-0 text-right qt-mono text-[10px] uppercase tracking-[0.24em] text-slate-500">
            <div>Run {shortRunId(current?.runId)}</div>
            <div className="mt-1 text-slate-600">{String(current?.phase || 'idle').replaceAll('_', ' ')}</div>
          </div>
        </div>

        <div className="relative mt-4">
          <div className="pointer-events-none absolute inset-x-0 top-0 z-10 h-8 bg-gradient-to-b from-slate-950 via-slate-950/78 to-transparent" />
          <div className="pointer-events-none absolute inset-x-0 bottom-0 z-10 h-14 bg-gradient-to-t from-slate-950 via-slate-950/75 to-transparent" />
          <div
            ref={streamRef}
            onScroll={handleScroll}
            className="qt-scrollbar-hidden qt-console-stream-mask max-h-56 overflow-y-auto pr-1"
            aria-label="Bot startup lifecycle activity stream"
          >
            <ol className="space-y-3 pb-6 pt-2">
              {consoleState.entries.length > 0 ? (
                consoleState.entries.map((entry, index) => (
                  <LifecycleStreamEntry
                    key={entry.id}
                    entry={entry}
                    index={index}
                    total={consoleState.entries.length}
                    latestActive={index === consoleState.entries.length - 1 && showActivity}
                  />
                ))
              ) : (
                <li className="px-1 py-4 text-sm text-slate-500">
                  No active lifecycle events yet. Start the bot to watch the backend-owned startup sequence stream in.
                </li>
              )}
            </ol>
          </div>
        </div>
      </div>
    </section>
  )
})

