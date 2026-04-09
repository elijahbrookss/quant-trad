import { Dialog, DialogPanel, DialogTitle } from '@headlessui/react'
import { AlertTriangle, Check, Copy, RefreshCw, X } from 'lucide-react'
import { useCallback, useEffect, useMemo, useRef, useState } from 'react'

import { fetchBotRunLifecycleEvents } from '../../adapters/bot.adapter.js'
import { createLogger } from '../../utils/logger.js'
import {
  buildDiagnosticsViewModel,
  copyDiagnosticsIdentifier,
  DIAGNOSTICS_COPY_RESET_MS,
} from './botDiagnosticsModel.js'
import { describeBotLifecycle, getBotRunId, normalizeBotStatus } from './botStatusModel.js'

const STATUS_TONE = {
  running: 'bg-emerald-500/10 text-emerald-200 border-emerald-500/30',
  completed: 'bg-sky-500/10 text-sky-200 border-sky-500/30',
  stopped: 'bg-slate-500/10 text-slate-300 border-slate-500/30',
  degraded: 'bg-amber-500/10 text-amber-200 border-amber-500/30',
  telemetry_degraded: 'bg-amber-500/10 text-amber-200 border-amber-500/30',
  starting: 'bg-sky-500/10 text-sky-200 border-sky-500/30',
  crashed: 'bg-rose-500/10 text-rose-200 border-rose-500/30',
  startup_failed: 'bg-rose-500/10 text-rose-200 border-rose-500/30',
  failed: 'bg-rose-500/10 text-rose-200 border-rose-500/30',
}

function statusToneClass(status) {
  return STATUS_TONE[normalizeBotStatus(status, '')] || 'bg-slate-500/10 text-slate-300 border-slate-500/30'
}

function lifecycleBadgeClass(status) {
  if (status === 'failed' || status === 'crashed' || status === 'startup_failed') {
    return 'border-rose-500/30 bg-rose-500/10 text-rose-200'
  }
  if (status === 'running') {
    return 'border-sky-500/30 bg-sky-500/10 text-sky-200'
  }
  if (status === 'completed') {
    return 'border-emerald-500/25 bg-emerald-500/10 text-emerald-200'
  }
  return 'border-white/[0.06] bg-black/25 text-slate-300'
}

function lifecycleRowClass(status) {
  if (status === 'failed' || status === 'crashed' || status === 'startup_failed') {
    return 'border-rose-900/35 bg-rose-950/6'
  }
  if (status === 'completed') {
    return 'border-white/[0.06] bg-black/18'
  }
  if (status === 'running') {
    return 'border-sky-900/35 bg-sky-950/8'
  }
  return 'border-white/[0.06] bg-black/24'
}

function DenseSection({ title, facts, tone = 'default', children = null }) {
  const rows = Array.isArray(facts) ? facts.filter((fact) => Boolean(fact?.label)) : []
  return (
    <section className={`rounded-lg border ${
      tone === 'failure' ? 'border-rose-900/35 bg-rose-950/6' : 'border-white/[0.06] bg-black/24'
    }`}>
      <div className="px-4 py-3">
        <p className={`text-[11px] font-semibold uppercase tracking-[0.2em] ${
          tone === 'failure' ? 'text-rose-300/70' : 'text-slate-500'
        }`}>
          {title}
        </p>
        <dl className="mt-3 grid gap-x-5 gap-y-3 sm:grid-cols-2">
          {rows.map((fact) => (
            <div key={fact.label} className="min-w-0">
              <dt className="text-[10px] font-semibold uppercase tracking-[0.16em] text-slate-500">{fact.label}</dt>
              <dd className="mt-1 text-sm leading-6 text-slate-100">{fact.value}</dd>
            </div>
          ))}
        </dl>
        {children}
      </div>
    </section>
  )
}

function CopyableIdentifier({ identifier, copied, onCopy }) {
  return (
    <div className="inline-flex items-center gap-1.5">
      <span className="text-[10px] font-semibold uppercase tracking-[0.16em] text-slate-500">{identifier.label}</span>
      <span className="font-mono text-[11px] text-slate-300" title={identifier.value || identifier.displayValue}>
        {identifier.displayValue}
      </span>
      {identifier.value ? (
        <button
          type="button"
          onClick={() => onCopy(identifier)}
          className="inline-flex shrink-0 items-center justify-center rounded-md p-1 text-slate-500 transition hover:bg-white/[0.05] hover:text-slate-100"
          aria-label={`Copy ${identifier.label}`}
          title={`Copy ${identifier.label}`}
        >
          {copied ? <Check className="size-3.5 text-emerald-300" /> : <Copy className="size-3.5" />}
        </button>
      ) : null}
    </div>
  )
}

function QuickFactsStrip({ facts }) {
  const items = Array.isArray(facts) ? facts.filter(Boolean) : []
  if (items.length === 0) return null
  return (
    <div className="mt-2 flex flex-wrap items-center gap-x-3 gap-y-1 text-[11px] text-slate-400">
      {items.map((fact) => (
        <span key={fact} className="inline-flex items-center gap-3">
          <span>{fact}</span>
        </span>
      ))}
    </div>
  )
}

function FailureSection({ title, message, contextLine, keyFacts }) {
  return (
    <section className="rounded-lg border border-rose-900/35 bg-rose-950/6 xl:col-span-2">
      <div className="px-4 py-4">
        <div className="flex items-start gap-3">
          <span className="mt-0.5 inline-flex h-8 w-8 items-center justify-center rounded-md bg-rose-500/10 text-rose-200">
            <AlertTriangle className="size-4" />
          </span>
          <div className="min-w-0">
            <p className="text-[11px] font-semibold uppercase tracking-[0.2em] text-rose-300/70">Primary Failure</p>
            <p className="mt-1 text-lg font-semibold text-rose-50">{title}</p>
            <p className="mt-2 text-sm leading-relaxed text-rose-100">{message}</p>
            {contextLine ? <p className="mt-2 text-xs text-rose-200/80">{contextLine}</p> : null}
            {Array.isArray(keyFacts) && keyFacts.length > 0 ? (
              <dl className="mt-4 grid gap-x-5 gap-y-3 sm:grid-cols-3">
                {keyFacts.map((fact) => (
                  <div key={fact.label} className="min-w-0">
                    <dt className="text-[10px] font-semibold uppercase tracking-[0.16em] text-rose-300/65">{fact.label}</dt>
                    <dd className="mt-1 text-sm text-rose-50">{fact.value}</dd>
                  </div>
                ))}
              </dl>
            ) : null}
          </div>
        </div>
      </div>
    </section>
  )
}

function PayloadBlock({ detail }) {
  return (
    <div className="min-w-0">
      <p className={`mb-2 text-[10px] font-semibold uppercase tracking-[0.2em] ${
        detail.tone === 'failure' ? 'text-rose-300/70' : 'text-slate-500'
      }`}>
        {detail.label}
      </p>
      <pre className={`max-h-56 overflow-auto rounded-md border border-white/[0.06] bg-black/18 p-3 text-[11px] ${
        detail.tone === 'failure' ? 'text-rose-100/90' : 'text-slate-300'
      }`}>
        {detail.value}
      </pre>
    </div>
  )
}

function LifecycleRow({ event }) {
  return (
    <li className={`rounded-lg border px-3 py-3 ${lifecycleRowClass(event.badgeStatus)}`}>
      <div className="flex flex-wrap items-start justify-between gap-3">
        <div className="min-w-0">
          <div className="flex flex-wrap items-center gap-2">
            <span className="qt-mono text-[10px] uppercase tracking-[0.2em] text-slate-500">
              Seq {Number(event.seq || 0)}
            </span>
            <span className={`inline-flex items-center rounded-md border px-2 py-0.5 text-[10px] font-semibold uppercase tracking-[0.16em] ${lifecycleBadgeClass(event.badgeStatus)}`}>
              {event.badgeLabel}
            </span>
            <span className="qt-mono text-[10px] uppercase tracking-[0.18em] text-slate-600">
              {event.owner}
            </span>
          </div>
          <p className="mt-2 text-sm font-semibold text-slate-100">{event.phase}</p>
          <p className="mt-1 text-sm leading-relaxed text-slate-300">{event.message}</p>
        </div>
        <p className="qt-mono shrink-0 text-[10px] uppercase tracking-[0.18em] text-slate-500">{event.at}</p>
      </div>

      {event.details.length > 0 ? (
        <details className="mt-2.5 border-l border-white/[0.06] pl-3">
          <summary className="cursor-pointer list-none text-xs font-medium text-slate-500 hover:text-slate-300">
            Inspect payload
          </summary>
          <div className="mt-3 space-y-3">
            {event.details.map((detail) => (
              <PayloadBlock key={`${event.key}-${detail.label}`} detail={detail} />
            ))}
          </div>
        </details>
      ) : null}
    </li>
  )
}

function WorkerFailureEntries({ entries }) {
  if (!Array.isArray(entries) || entries.length === 0) return null
  return (
    <ul className="mt-4 space-y-2 border-t border-white/[0.06] pt-3">
      {entries.map((entry) => (
        <li key={entry.key} className="rounded-md border border-white/[0.06] bg-black/16 px-3 py-2">
          <p className="text-sm text-slate-100">{entry.summary}</p>
          {entry.message ? <p className="mt-1 text-xs text-slate-400">{entry.message}</p> : null}
        </li>
      ))}
    </ul>
  )
}

export function BotDiagnosticsModal({ bot, open, onClose }) {
  const logger = useMemo(() => createLogger('BotDiagnosticsModal'), [])
  const lifecycle = useMemo(() => describeBotLifecycle(bot), [bot])
  const runId = getBotRunId(bot)
  const [diagnostics, setDiagnostics] = useState({ summary: null, checkpoints: [], events: [] })
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState(null)
  const [copiedKeys, setCopiedKeys] = useState({})
  const copyResetRef = useRef({})

  const loadEvents = useCallback(async () => {
    if (!bot?.id || !runId) {
      setDiagnostics({ summary: null, checkpoints: [], events: [] })
      setError(null)
      return
    }
    setLoading(true)
    setError(null)
    try {
      logger.info('bot_diagnostics_load_start', { bot_id: bot.id, run_id: runId })
      const payload = await fetchBotRunLifecycleEvents(bot.id, runId)
      setDiagnostics({
        summary: payload?.summary || null,
        checkpoints: Array.isArray(payload?.checkpoints) ? payload.checkpoints : [],
        events: Array.isArray(payload?.events) ? payload.events : [],
        run_status: payload?.run_status || null,
      })
      logger.info('bot_diagnostics_load_success', {
        bot_id: bot.id,
        run_id: runId,
        count: Array.isArray(payload?.events) ? payload.events.length : 0,
        has_summary: Boolean(payload?.summary),
      })
    } catch (err) {
      logger.error('bot_diagnostics_load_failed', { bot_id: bot?.id, run_id: runId, message: err?.message }, err)
      setDiagnostics({ summary: null, checkpoints: [], events: [] })
      setError(err?.message || 'Unable to load lifecycle diagnostics')
    } finally {
      setLoading(false)
    }
  }, [bot?.id, logger, runId])

  useEffect(() => {
    if (!open) return
    loadEvents()
  }, [loadEvents, open])

  useEffect(() => () => {
    Object.values(copyResetRef.current).forEach((timerId) => clearTimeout(timerId))
  }, [])

  const scheduleCopiedReset = useCallback((copyKey, reset, delay) => {
    if (copyResetRef.current[copyKey]) clearTimeout(copyResetRef.current[copyKey])
    copyResetRef.current[copyKey] = setTimeout(() => {
      delete copyResetRef.current[copyKey]
      reset()
    }, delay)
  }, [])

  const handleCopiedChange = useCallback((copyKey, copied) => {
    setCopiedKeys((current) => ({ ...current, [copyKey]: copied }))
  }, [])

  const handleCopyIdentifier = useCallback(async (identifier) => {
    try {
      await copyDiagnosticsIdentifier({
        copyKey: identifier.key,
        value: identifier.value,
        writeText: navigator?.clipboard?.writeText?.bind(navigator.clipboard),
        onCopiedChange: handleCopiedChange,
        scheduleReset: (reset, resetMs = DIAGNOSTICS_COPY_RESET_MS) => scheduleCopiedReset(identifier.key, reset, resetMs),
      })
    } catch (err) {
      logger.warn('bot_diagnostics_copy_failed', {
        bot_id: bot?.id,
        run_id: runId,
        copy_key: identifier.key,
        message: err?.message,
      })
    }
  }, [bot?.id, handleCopiedChange, logger, runId, scheduleCopiedReset])

  const viewModel = useMemo(() => buildDiagnosticsViewModel({
    botId: bot?.id,
    runId,
    lifecycle,
    diagnostics,
    loading,
  }), [bot?.id, diagnostics, lifecycle, loading, runId])

  if (!open || !bot) return null

  const workerFacts = viewModel.workerFailureSummary.facts

  return (
    <Dialog open={open} onClose={onClose} className="relative z-[80]">
      <div className="fixed inset-0 bg-black/80 backdrop-blur-sm" aria-hidden="true" />
      <div className="fixed inset-0 flex items-center justify-center p-4">
        <DialogPanel className="flex max-h-[calc(100vh-2rem)] w-full max-w-5xl flex-col overflow-hidden rounded-lg border border-white/[0.06] bg-[#0b1019]/96 shadow-[0_30px_80px_rgba(0,0,0,0.45)]">
          <div className="flex items-start justify-between gap-4 border-b border-white/[0.06] px-5 py-3">
            <div className="min-w-0">
              <div className="flex flex-wrap items-center gap-2">
                <DialogTitle className="text-lg font-semibold text-slate-50">{viewModel.header.title}</DialogTitle>
                <span className={`inline-flex items-center rounded-full border px-2 py-0.5 text-[10px] font-semibold uppercase tracking-[0.18em] ${statusToneClass(viewModel.header.status)}`}>
                  {viewModel.header.statusLabel}
                </span>
              </div>
              <p className="mt-1 text-sm text-slate-200">{viewModel.header.subtitle}</p>
              <QuickFactsStrip facts={viewModel.header.quickFacts} />
              <div className="mt-2 flex flex-wrap items-center gap-x-4 gap-y-1.5 text-xs text-slate-500">
                <span className="text-slate-500">{viewModel.header.eventCountLabel}</span>
                {viewModel.header.identifiers.map((identifier) => (
                  <CopyableIdentifier
                    key={identifier.key}
                    identifier={identifier}
                    copied={Boolean(copiedKeys[identifier.key])}
                    onCopy={handleCopyIdentifier}
                  />
                ))}
              </div>
            </div>
            <div className="flex items-center gap-2">
              <button
                type="button"
                onClick={loadEvents}
                className="inline-flex items-center gap-1.5 rounded-md border border-white/[0.06] bg-black/30 px-3 py-2 text-xs font-medium text-slate-300 transition-colors hover:border-white/[0.1] hover:bg-black/45 hover:text-slate-100 disabled:opacity-50"
                disabled={loading || !runId}
              >
                <RefreshCw className={`size-3.5 ${loading ? 'animate-spin' : ''}`} />
                Refresh
              </button>
              <button
                type="button"
                onClick={onClose}
                className="inline-flex h-9 w-9 items-center justify-center rounded-md border border-white/[0.06] bg-black/30 text-slate-400 transition-colors hover:border-white/[0.1] hover:bg-black/45 hover:text-slate-200"
                aria-label="Close"
              >
                <X className="size-4" />
              </button>
            </div>
          </div>

          <div className="overflow-y-auto px-5 py-3">
            <div className="grid gap-3 xl:grid-cols-2">
              <FailureSection
                title={viewModel.primaryFailure.title}
                message={viewModel.primaryFailure.message}
                contextLine={viewModel.primaryFailure.contextLine}
                keyFacts={viewModel.primaryFailure.keyFacts}
              />
              <DenseSection title="Final State" facts={viewModel.finalState.facts} />
              <DenseSection title={viewModel.workerFailureSummary.title} facts={workerFacts}>
                <WorkerFailureEntries entries={viewModel.workerFailureSummary.entries} />
              </DenseSection>
            </div>

            <section className="mt-4 rounded-lg border border-white/[0.06] bg-black/24">
              <div className="border-b border-white/[0.06] px-4 py-3">
                <p className="text-sm font-semibold text-slate-100">{viewModel.lifecycleTrail.title}</p>
                <p className="mt-1 text-xs text-slate-500">Supporting lifecycle evidence, newest first.</p>
              </div>

              {!runId ? (
                <div className="px-4 py-8 text-sm text-slate-500">No run id is attached to this bot state yet.</div>
              ) : loading ? (
                <div className="px-4 py-8 text-sm text-slate-400">Loading lifecycle events…</div>
              ) : error ? (
                <div className="px-4 py-8 text-sm text-rose-300">{error}</div>
              ) : viewModel.lifecycleTrail.rows.length === 0 ? (
                <div className="px-4 py-8 text-sm text-slate-500">No lifecycle events were recorded for this run.</div>
              ) : (
                <ol className="space-y-2 px-4 py-4">
                  {viewModel.lifecycleTrail.rows.map((event) => (
                    <LifecycleRow key={event.key} event={event} />
                  ))}
                </ol>
              )}
            </section>
          </div>
        </DialogPanel>
      </div>
    </Dialog>
  )
}
