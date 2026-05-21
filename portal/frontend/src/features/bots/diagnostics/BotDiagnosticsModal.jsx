import { useCallback, useEffect, useMemo, useRef, useState } from 'react'

import { fetchBotRunLifecycleEvents } from '../../../adapters/bot.adapter.js'
import { createLogger } from '../../../utils/logger.js'
import { describeBotLifecycle, getBotRunId } from '../state/botRuntimeStatus.js'
import {
  buildBotDiagnosticsViewModel,
  copyDiagnosticsIdentifier,
  DIAGNOSTICS_COPY_RESET_MS,
} from './buildBotDiagnosticsViewModel.js'
import { BotDiagnosticsView } from './BotDiagnosticsView.jsx'

export function BotDiagnosticsModal({ bot, open, onClose }) {
  const logger = useMemo(() => createLogger('BotDiagnosticsModal'), [])
  const lifecycle = useMemo(() => describeBotLifecycle(bot), [bot])
  const runId = getBotRunId(bot)
  const [diagnostics, setDiagnostics] = useState({ summary: null, checkpoints: [], events: [] })
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState(null)
  const [copiedKeys, setCopiedKeys] = useState({})
  const copyResetRef = useRef({})
  const loadTokenRef = useRef(0)

  const loadEvents = useCallback(async () => {
    const loadToken = ++loadTokenRef.current
    if (!bot?.id || !runId) {
      setDiagnostics({ summary: null, checkpoints: [], events: [] })
      setError(null)
      setLoading(false)
      return
    }
    setLoading(true)
    setError(null)
    try {
      logger.info('bot_diagnostics_load_start', { bot_id: bot.id, run_id: runId })
      const payload = await fetchBotRunLifecycleEvents(bot.id, runId)
      if (loadToken !== loadTokenRef.current) return
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
      if (loadToken !== loadTokenRef.current) return
      logger.error('bot_diagnostics_load_failed', { bot_id: bot?.id, run_id: runId, message: err?.message }, err)
      setDiagnostics({ summary: null, checkpoints: [], events: [] })
      setError(err?.message || 'Unable to load lifecycle diagnostics')
    } finally {
      if (loadToken === loadTokenRef.current) {
        setLoading(false)
      }
    }
  }, [bot?.id, logger, runId])

  useEffect(() => {
    if (!open) {
      loadTokenRef.current += 1
      return
    }
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

  const handleCopyItem = useCallback(async (identifier) => {
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

  const model = useMemo(() => buildBotDiagnosticsViewModel({
    botId: bot?.id,
    diagnostics,
    error,
    loading,
    lifecycle,
    runId,
  }), [bot?.id, diagnostics, error, lifecycle, loading, runId])

  if (!open || !bot) return null

  return (
    <BotDiagnosticsView
      copiedKeys={copiedKeys}
      model={model}
      onClose={onClose}
      onCopyItem={handleCopyItem}
      onRefresh={loadEvents}
      open={open}
    />
  )
}
