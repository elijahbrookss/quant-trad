import { useCallback, useEffect, useMemo, useRef, useState } from 'react'
import { AlertTriangle, ChevronDown, LocateFixed, Maximize2, Minimize2, RefreshCcw, X } from 'lucide-react'

import { BotLensChart } from './BotLensChart.jsx'
import { OverlayToggleBar } from './OverlayToggleBar.jsx'
import { ActiveTradeChip } from './ActiveTradeChip.jsx'
import { TradeLogList } from './TradeLogList.jsx'
import DecisionTrace from './DecisionTrace/index.jsx'
import { useOverlayControls } from './hooks/useOverlayControls.js'
import { createLogger } from '../../utils/logger.js'
import {
  fetchBotLensSeriesHistory,
  fetchBotLensSession,
  fetchBotLensSymbolDetail,
  fetchBotRunLedgerEvents,
  openBotLensLiveStream,
} from '../../adapters/bot.adapter.js'
import { describeBotLifecycle, getBotRunId, getBotStatus, normalizeBotStatus } from './botStatusModel.js'
import {
  applyDetailDelta,
  applyDetailSnapshot,
  applyHistoryPage,
  applyOpenTradesDelta,
  applySummaryDelta,
  canonicalSeriesKey,
  createRunStore,
  getSelectedDetail,
  normalizeSeriesKey,
  selectSymbol,
} from './botlensProjection.js'

function delay(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms))
}

function formatPercent(value) {
  if (typeof value !== 'number' || Number.isNaN(value)) return '—'
  return `${(value * 100).toFixed(1)}%`
}

function formatNumber(value, digits = 2) {
  if (typeof value !== 'number' || Number.isNaN(value)) return '—'
  return value.toFixed(digits)
}

function formatMoment(value) {
  if (!value) return '—'
  try {
    return new Date(value).toLocaleString()
  } catch {
    return String(value)
  }
}

function formatRelativeTime(value) {
  if (!value) return 'just now'
  const timestamp = new Date(value).getTime()
  if (!Number.isFinite(timestamp)) return 'recently'
  const deltaMs = Math.max(0, Date.now() - timestamp)
  const seconds = Math.floor(deltaMs / 1000)
  if (seconds < 10) return 'just now'
  if (seconds < 60) return `${seconds}s ago`
  const minutes = Math.floor(seconds / 60)
  if (minutes < 60) return `${minutes}m ago`
  const hours = Math.floor(minutes / 60)
  if (hours < 24) return `${hours}h ago`
  const days = Math.floor(hours / 24)
  return `${days}d ago`
}

function warningRowTitle(warning) {
  const indicator = String(warning?.indicator_id || warning?.context?.indicator_id || '').trim() || 'indicator'
  const symbol = String(warning?.symbol || warning?.context?.symbol || '').trim()
  const title = String(warning?.title || '').trim()
  if (!title) return symbol ? `${indicator} · ${symbol}` : indicator
  return symbol ? `${indicator} · ${symbol} · ${title}` : `${indicator} · ${title}`
}

function normalizeSymbolKey(value) {
  return String(value || '').trim().toUpperCase()
}

function isOpenTrade(trade) {
  if (!trade || typeof trade !== 'object') return false
  if (trade.closed_at) return false
  const status = String(trade.status || '').toLowerCase()
  if (status === 'closed' || status === 'completed' || status === 'complete') return false
  const legs = Array.isArray(trade.legs) ? trade.legs : []
  if (!legs.length) return true
  return legs.some((leg) => {
    if (!leg || typeof leg !== 'object') return false
    if (!leg.exit_time) return true
    return String(leg.status || '').toLowerCase() === 'open'
  })
}

function buildTradeChip(trade) {
  if (!trade || typeof trade !== 'object') return null
  const direction = String(trade.direction || '').toLowerCase() === 'short' ? 'short' : 'long'
  const quantityRaw = Number(
    trade?.entry_order?.contracts ?? trade?.entry_order?.quantity ?? trade?.qty ?? trade?.quantity ?? trade?.contracts,
  )
  const quantityLabel = Number.isFinite(quantityRaw) && quantityRaw > 0 ? String(Number(quantityRaw.toFixed(4))) : null
  return {
    symbol: String(trade.symbol || '—'),
    direction,
    directionLabel: direction.toUpperCase(),
    sizeLabel: quantityLabel || `${Math.max((trade.legs || []).length, 1)}x`,
    entry: trade.entry_price,
  }
}

function detailTabClass(active) {
  return active
    ? 'border-[color:var(--accent-alpha-40)] bg-[color:var(--accent-alpha-15)] text-[color:var(--accent-text-strong)]'
    : 'border-white/10 bg-white/5 text-slate-300 hover:border-[color:var(--accent-alpha-30)] hover:text-[color:var(--accent-text-strong)]'
}

function summarizeRun(runMeta, health) {
  if (!runMeta) return 'No active runtime attached'
  const parts = [
    runMeta.strategy_name || 'Runtime',
    normalizeBotStatus(health?.status || runMeta.status || 'idle'),
  ]
  if (runMeta.started_at) parts.push(`started ${formatMoment(runMeta.started_at)}`)
  if (runMeta.datasource || runMeta.exchange) parts.push([runMeta.datasource, runMeta.exchange].filter(Boolean).join(' · '))
  return parts.filter(Boolean).join(' · ')
}

function DataRows({ rows }) {
  if (!rows.length) {
    return <div className="rounded-xl border border-dashed border-white/10 px-4 py-5 text-sm text-slate-400">No data.</div>
  }
  return (
    <div className="overflow-hidden rounded-xl border border-white/10">
      <table className="min-w-full text-left text-sm text-slate-200">
        <tbody className="divide-y divide-white/5">
          {rows.map((row) => (
            <tr key={row.key}>
              <td className="w-1/3 px-4 py-3 text-[11px] uppercase tracking-[0.24em] text-slate-500">{row.label}</td>
              <td className={`px-4 py-3 ${row.className || ''}`}>{row.value}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}

export function BotLensLiveModal({ open, bot, onClose }) {
  const logger = useMemo(() => createLogger('BotLensLiveModal', { botId: bot?.id || null }), [bot?.id])
  const socketRef = useRef(null)
  const reconnectRef = useRef(0)
  const bootstrapTokenRef = useRef(0)
  const ledgerTokenRef = useRef(0)
  const detailLoadRef = useRef(new Set())
  const symbolSwitchRef = useRef({ symbolKey: null, requestedAt: 0, cacheHit: false, resolved: false })
  const [runStore, setRunStore] = useState(null)
  const [statusMessage, setStatusMessage] = useState('')
  const [error, setError] = useState(null)
  const [streamState, setStreamState] = useState('idle')
  const [detailTab, setDetailTab] = useState('overview')
  const [logTab, setLogTab] = useState('trade')
  const [hoveredTradeId, setHoveredTradeId] = useState(null)
  const [overlayPanelCollapsed, setOverlayPanelCollapsed] = useState(false)
  const [warningPanelOpen, setWarningPanelOpen] = useState(false)
  const [followLive, setFollowLive] = useState(true)
  const [fullScreen, setFullScreen] = useState(false)
  const [loadingDetailSymbolKey, setLoadingDetailSymbolKey] = useState(null)
  const [reloadTick, setReloadTick] = useState(0)
  const [ledgerEvents, setLedgerEvents] = useState([])
  const [ledgerState, setLedgerState] = useState({ runId: null, nextAfterSeq: 0, status: 'idle', error: null })

  const closeSocket = useCallback(() => {
    if (socketRef.current) {
      try {
        socketRef.current.close()
      } catch {}
    }
    socketRef.current = null
  }, [])

  const syncSocketSubscription = useCallback((socket, store) => {
    if (!socket || socket.readyState !== WebSocket.OPEN || !store) return
    const selectedSymbolKey = normalizeSeriesKey(store.selectedSymbolKey || '')
    const hotSymbols = Object.keys(store.detailCache || {})
      .map((entry) => normalizeSeriesKey(entry))
      .filter(Boolean)
    if (selectedSymbolKey) {
      socket.send(JSON.stringify({ type: 'set_selected_symbol', symbol_key: selectedSymbolKey }))
    }
    socket.send(JSON.stringify({ type: 'set_hot_symbols', symbol_keys: hotSymbols }))
  }, [])

  const refreshSession = useCallback(() => {
    setReloadTick((value) => value + 1)
  }, [])

  useEffect(() => {
    if (!open || !bot?.id) {
      closeSocket()
      setRunStore(null)
      setStatusMessage('')
      setError(null)
      setStreamState('idle')
      setWarningPanelOpen(false)
      setLedgerEvents([])
      setLedgerState({ runId: null, nextAfterSeq: 0, status: 'idle', error: null })
      return
    }

    let cancelled = false
    const token = ++bootstrapTokenRef.current
    const load = async () => {
      const bootstrapStartedAt = performance.now()
      setError(null)
      setStatusMessage('Bootstrapping BotLens run session...')
      setStreamState('bootstrapping')
      closeSocket()
      try {
        const session = await fetchBotLensSession(bot.id, { limit: 320 })
        if (cancelled || token !== bootstrapTokenRef.current) return
        if (session?.state !== 'ready') {
          setRunStore(null)
          setStatusMessage(String(session?.message || 'BotLens session unavailable'))
          setStreamState('idle')
          return
        }
        const store = createRunStore(session)
        setRunStore(store)
        setStatusMessage(String(session?.message || 'BotLens session ready'))
        logger.info('botlens_session_loaded', {
          bot_id: bot.id,
          run_id: store.runMeta?.run_id || null,
          symbol_count: Object.keys(store.symbolIndex || {}).length,
          open_trade_count: Object.keys(store.openTradesIndex || {}).length,
          detail_cache_size: Object.keys(store.detailCache || {}).length,
          bootstrap_ms: Math.round((performance.now() - bootstrapStartedAt) * 1000) / 1000,
        })
        if (!session.live) {
          setStreamState('historical')
          return
        }

        setStreamState('connecting')
        const socket = openBotLensLiveStream(bot.id, {
          symbolKey: store.selectedSymbolKey,
          cursorSeq: store.seq || 0,
        })
        socketRef.current = socket

        socket.onopen = () => {
          if (cancelled || token !== bootstrapTokenRef.current) return
          setStreamState('open')
          syncSocketSubscription(socket, store)
          logger.info('botlens_run_ws_open', {
            bot_id: bot.id,
            run_id: store.runMeta?.run_id || null,
            selected_symbol_key: store.selectedSymbolKey || null,
            cursor_seq: store.seq || 0,
          })
        }

        socket.onmessage = (event) => {
          if (cancelled || token !== bootstrapTokenRef.current) return
          try {
            const message = JSON.parse(event.data)
            const type = String(message?.type || '')
            if (type === 'botlens_run_connected') {
              setStreamState('open')
              return
            }
            if (type === 'botlens_run_resync_required') {
              setStatusMessage(`BotLens resync required: ${message?.payload?.reason || 'stream continuity changed'}`)
              setStreamState('resyncing')
              closeSocket()
              refreshSession()
              return
            }
            if (type === 'botlens_run_summary_delta') {
              setRunStore((current) => (current ? applySummaryDelta(current, message) : current))
              return
            }
            if (type === 'botlens_open_trades_delta') {
              setRunStore((current) => (current ? applyOpenTradesDelta(current, message) : current))
              return
            }
            if (type === 'botlens_symbol_detail_delta') {
              setRunStore((current) => (current ? applyDetailDelta(current, message) : current))
            }
          } catch (err) {
            logger.warn('botlens_run_ws_parse_failed', { bot_id: bot.id }, err)
          }
        }

        socket.onerror = (err) => {
          if (cancelled || token !== bootstrapTokenRef.current) return
          logger.warn('botlens_run_ws_error', { bot_id: bot.id }, err)
          setStreamState('error')
        }

        socket.onclose = () => {
          if (cancelled || token !== bootstrapTokenRef.current) return
          socketRef.current = null
          const liveRunId = store.runMeta?.run_id || null
          const shouldRetry = liveRunId && reconnectRef.current < 2
          setStreamState(shouldRetry ? 'reconnecting' : 'closed')
          if (!shouldRetry) return
          reconnectRef.current += 1
          window.setTimeout(() => {
            if (cancelled || token !== bootstrapTokenRef.current) return
            refreshSession()
          }, 500)
        }
      } catch (err) {
        if (cancelled || token !== bootstrapTokenRef.current) return
        setError(err?.message || 'BotLens bootstrap failed')
        setStatusMessage('BotLens bootstrap failed.')
        setStreamState('error')
        logger.warn('botlens_session_load_failed', { bot_id: bot.id }, err)
      }
    }

    reconnectRef.current = 0
    load()
    return () => {
      cancelled = true
      closeSocket()
    }
  }, [bot?.id, closeSocket, logger, open, refreshSession, reloadTick, syncSocketSubscription])

  const selectedSymbolKey = normalizeSeriesKey(runStore?.selectedSymbolKey || '')
  const selectedDetail = useMemo(() => getSelectedDetail(runStore), [runStore])
  const selectedSummary = selectedSymbolKey ? runStore?.symbolIndex?.[selectedSymbolKey] || null : null
  const selectedLabel = selectedDetail?.display_label || selectedSummary?.display_label || selectedSymbolKey || '—'

  useEffect(() => {
    if (!open || !runStore || !runStore.runMeta?.run_id || !selectedSymbolKey) return
    const socket = socketRef.current
    syncSocketSubscription(socket, runStore)
    if (runStore.detailCache?.[selectedSymbolKey]) {
      const switchState = symbolSwitchRef.current
      if (switchState.symbolKey === selectedSymbolKey && !switchState.resolved) {
        switchState.resolved = true
        logger.info('botlens_symbol_detail_cache_hit', {
          bot_id: bot?.id || null,
          run_id: runStore.runMeta.run_id,
          symbol_key: selectedSymbolKey,
          switch_ms: Math.round((performance.now() - Number(switchState.requestedAt || performance.now())) * 1000) / 1000,
          detail_cache_size: Object.keys(runStore.detailCache || {}).length,
        })
      }
      return
    }
    if (detailLoadRef.current.has(selectedSymbolKey)) return

    let cancelled = false
    const fetchStartedAt = performance.now()
    detailLoadRef.current.add(selectedSymbolKey)
    setLoadingDetailSymbolKey(selectedSymbolKey)
    setStatusMessage(`Loading detail for ${selectedLabel}...`)
    logger.info('botlens_symbol_detail_fetch_started', {
      bot_id: bot?.id || null,
      run_id: runStore.runMeta.run_id,
      symbol_key: selectedSymbolKey,
      detail_cache_size: Object.keys(runStore.detailCache || {}).length,
    })

    fetchBotLensSymbolDetail(runStore.runMeta.run_id, selectedSymbolKey, { limit: 320 })
      .then((detail) => {
        if (cancelled) return
        setRunStore((current) => (current ? applyDetailSnapshot(current, detail) : current))
        setStatusMessage(`Viewing ${selectedLabel}`)
        const switchState = symbolSwitchRef.current
        if (switchState.symbolKey === selectedSymbolKey) {
          switchState.resolved = true
        }
        logger.info('botlens_symbol_detail_fetch_succeeded', {
          bot_id: bot?.id || null,
          run_id: runStore.runMeta.run_id,
          symbol_key: selectedSymbolKey,
          fetch_ms: Math.round((performance.now() - fetchStartedAt) * 1000) / 1000,
          candle_count: Array.isArray(detail?.detail?.candles)
            ? detail.detail.candles.length
            : Array.isArray(detail?.candles)
              ? detail.candles.length
              : 0,
        })
      })
      .catch((err) => {
        if (cancelled) return
        setError(err?.message || `Failed to load detail for ${selectedLabel}`)
        logger.warn('botlens_symbol_detail_failed', {
          bot_id: bot?.id || null,
          run_id: runStore.runMeta.run_id,
          symbol_key: selectedSymbolKey,
          fetch_ms: Math.round((performance.now() - fetchStartedAt) * 1000) / 1000,
        }, err)
      })
      .finally(() => {
        detailLoadRef.current.delete(selectedSymbolKey)
        if (!cancelled) setLoadingDetailSymbolKey(null)
      })

    return () => {
      cancelled = true
    }
  }, [bot?.id, logger, open, runStore, selectedLabel, selectedSymbolKey, syncSocketSubscription])

  useEffect(() => {
    const activeRunId = runStore?.runMeta?.run_id || null
    if (!open || !bot?.id || !activeRunId) {
      setLedgerEvents([])
      setLedgerState({ runId: null, nextAfterSeq: 0, status: 'idle', error: null })
      return
    }

    let cancelled = false
    const token = ++ledgerTokenRef.current
    let nextAfterSeq = 0
    setLedgerEvents([])
    setLedgerState({ runId: activeRunId, nextAfterSeq: 0, status: 'syncing', error: null })

    const poll = async () => {
      while (!cancelled && token === ledgerTokenRef.current) {
        try {
          const response = await fetchBotRunLedgerEvents(bot.id, activeRunId, {
            afterSeq: nextAfterSeq,
            limit: 500,
          })
          if (cancelled || token !== ledgerTokenRef.current) return
          const incoming = Array.isArray(response?.events) ? response.events : []
          nextAfterSeq = Number(response?.next_after_seq || nextAfterSeq) || nextAfterSeq
          if (incoming.length) {
            setLedgerEvents((current) => [...current, ...incoming].slice(-3000))
          }
          setLedgerState({ runId: activeRunId, nextAfterSeq, status: 'open', error: null })
          await delay(incoming.length >= 500 ? 100 : 800)
        } catch (err) {
          if (cancelled || token !== ledgerTokenRef.current) return
          setLedgerState({
            runId: activeRunId,
            nextAfterSeq,
            status: 'error',
            error: err?.message || 'Ledger query failed',
          })
          await delay(1200)
        }
      }
    }

    poll()
    return () => {
      cancelled = true
    }
  }, [bot?.id, open, runStore?.runMeta?.run_id])

  const symbolOptions = useMemo(() => {
    return Object.values(runStore?.symbolIndex || {}).sort((left, right) => {
      const leftLabel = String(left?.display_label || left?.symbol_key || '')
      const rightLabel = String(right?.display_label || right?.symbol_key || '')
      return leftLabel.localeCompare(rightLabel)
    })
  }, [runStore?.symbolIndex])

  const openTrades = useMemo(() => {
    return Object.values(runStore?.openTradesIndex || {}).filter((trade) => isOpenTrade(trade))
  }, [runStore?.openTradesIndex])

  const tradeCards = useMemo(() => {
    return openTrades
      .map((trade, index) => ({
        id: String(trade?.trade_id || `${trade?.entry_time || ''}|${trade?.symbol || ''}|${index}`),
        trade,
        chip: buildTradeChip(trade),
      }))
      .filter((entry) => entry.chip)
  }, [openTrades])

  const symbolPriceContext = useMemo(() => {
    const byKey = new Map()
    Object.entries(runStore?.symbolIndex || {}).forEach(([symbolKey, summary]) => {
      byKey.set(symbolKey, {
        currentPrice: Number(summary?.last_price),
        latestBarTime: Number.isFinite(Number(summary?.last_bar_time))
          ? new Date(Number(summary.last_bar_time) * 1000).toISOString()
          : null,
      })
    })
    return byKey
  }, [runStore?.symbolIndex])

  const chartCandles = Array.isArray(selectedDetail?.candles) ? selectedDetail.candles : []
  const chartTrades = Array.isArray(selectedDetail?.recent_trades) ? selectedDetail.recent_trades : []
  const chartOverlays = Array.isArray(selectedDetail?.overlays) ? selectedDetail.overlays : []
  const logs = Array.isArray(selectedDetail?.logs) ? selectedDetail.logs : []
  const { overlayOptions, visibility, visibleOverlays, toggleOverlay } = useOverlayControls({ overlays: chartOverlays })
  const botLifecycle = describeBotLifecycle(bot)
  const activeRunId = runStore?.runMeta?.run_id || getBotRunId(bot)
  const botStatus = normalizeBotStatus(getBotStatus(bot))
  const runSummaryText = summarizeRun(runStore?.runMeta, runStore?.health)
  const warningItems = useMemo(() => {
    return Array.isArray(runStore?.health?.warnings) ? runStore.health.warnings : []
  }, [runStore?.health?.warnings])
  const activeWarningCount = Math.max(
    warningItems.length,
    Number(runStore?.health?.warning_count || 0) || 0,
  )
  const seriesStats = useMemo(() => {
    return symbolOptions.map((summary) => ({
      key: summary.symbol_key,
      symbol: summary.symbol || '—',
      timeframe: summary.timeframe || '—',
      trades: Number(summary?.stats?.total_trades || 0),
      netPnl: Number(summary?.stats?.net_pnl || 0),
      winRate: Number(summary?.stats?.win_rate || 0),
      maxDrawdown: Number(summary?.stats?.max_drawdown || 0),
    }))
  }, [symbolOptions])

  const changeSelectedSymbol = useCallback((symbolKey) => {
    const normalizedSymbolKey = normalizeSeriesKey(symbolKey)
    if (!normalizedSymbolKey) return
    const cacheHit = Boolean(runStore?.detailCache?.[normalizedSymbolKey])
    symbolSwitchRef.current = {
      symbolKey: normalizedSymbolKey,
      requestedAt: performance.now(),
      cacheHit,
      resolved: false,
    }
    logger.info('botlens_symbol_switch_requested', {
      bot_id: bot?.id || null,
      run_id: runStore?.runMeta?.run_id || null,
      symbol_key: normalizedSymbolKey,
      cache_hit: cacheHit,
      detail_cache_size: Object.keys(runStore?.detailCache || {}).length,
    })
    setRunStore((current) => (current ? selectSymbol(current, symbolKey) : current))
  }, [bot?.id, logger, runStore])

  const loadOlderHistory = useCallback(async () => {
    if (!runStore?.runMeta?.run_id || !selectedSymbolKey || !selectedDetail?.candles?.length) return
    const oldest = selectedDetail.candles[0]
    const beforeTs = oldest?.time ? new Date(Number(oldest.time) * 1000).toISOString() : undefined
    try {
      const historyStartedAt = performance.now()
      const page = await fetchBotLensSeriesHistory(runStore.runMeta.run_id, selectedSymbolKey, { beforeTs, limit: 240 })
      const candles = Array.isArray(page?.candles) ? page.candles : []
      if (!candles.length) return
      setRunStore((current) => (current ? applyHistoryPage(current, { symbolKey: selectedSymbolKey, candles }) : current))
      logger.info('botlens_history_page_loaded', {
        bot_id: bot?.id || null,
        run_id: runStore.runMeta.run_id,
        symbol_key: selectedSymbolKey,
        candle_count: candles.length,
        fetch_ms: Math.round((performance.now() - historyStartedAt) * 1000) / 1000,
      })
    } catch (err) {
      logger.warn('botlens_history_page_failed', {
        bot_id: bot?.id || null,
        run_id: runStore.runMeta.run_id,
        symbol_key: selectedSymbolKey,
      }, err)
    }
  }, [bot?.id, logger, runStore, selectedDetail?.candles, selectedSymbolKey])

  useEffect(() => {
    if (!hoveredTradeId) return
    const stillVisible = tradeCards.some((entry) => entry.id === hoveredTradeId)
    if (!stillVisible) setHoveredTradeId(null)
  }, [hoveredTradeId, tradeCards])

  if (!open || !bot) return null

  const modalShellClassName = fullScreen
    ? 'h-screen w-full max-w-none overflow-hidden border-0 bg-[#14171f] shadow-2xl'
    : 'h-[88vh] w-full max-w-[92rem] overflow-hidden rounded-2xl border border-white/10 bg-[#14171f] shadow-[0_30px_80px_-32px_rgba(0,0,0,0.8)]'
  const modalBodyHeightClass = fullScreen ? 'h-[calc(100vh-62px)]' : 'h-[calc(88vh-62px)]'

  return (
    <div className={`fixed inset-0 z-[70] flex items-center justify-center bg-slate-950/82 ${fullScreen ? 'p-0' : 'p-4'}`} onClick={onClose}>
      <div className={modalShellClassName} onClick={(event) => event.stopPropagation()}>
        <div className="flex items-center justify-between border-b border-white/10 px-5 py-3">
          <div>
            <p className="text-[11px] font-semibold uppercase tracking-[0.32em] text-[color:var(--accent-text-kicker)]">BotLens</p>
            <p className="mt-1 text-sm font-semibold text-slate-100">Run-scoped runtime inspection</p>
            <p className="text-xs text-slate-500">
              bot_id={bot.id} · run_id={activeRunId || '—'} · stream={streamState} · selected={selectedLabel}
            </p>
          </div>
          <div className="flex items-center gap-2">
            <button
              type="button"
              onClick={refreshSession}
              className="inline-flex items-center gap-1.5 rounded-lg border border-white/10 bg-white/5 px-3 py-2 text-xs font-medium text-slate-300 transition hover:border-[color:var(--accent-alpha-30)] hover:text-[color:var(--accent-text-strong)]"
            >
              <RefreshCcw className="size-3.5" />
              Refresh
            </button>
            <button
              type="button"
              onClick={() => setFollowLive((prev) => !prev)}
              className={`inline-flex items-center gap-1.5 rounded-lg border px-3 py-2 text-xs font-medium transition-colors ${
                followLive
                  ? 'border-[color:var(--accent-alpha-40)] bg-[color:var(--accent-alpha-15)] text-[color:var(--accent-text-strong)]'
                  : 'border-white/10 bg-white/5 text-slate-300 hover:border-[color:var(--accent-alpha-30)] hover:text-[color:var(--accent-text-strong)]'
              }`}
            >
              <LocateFixed className="size-3.5" />
              Follow Live
            </button>
            <button
              type="button"
              onClick={() => setFullScreen((prev) => !prev)}
              className="inline-flex h-9 w-9 items-center justify-center rounded-lg border border-white/10 bg-white/5 text-slate-300 transition-colors hover:border-[color:var(--accent-alpha-30)] hover:text-[color:var(--accent-text-strong)]"
            >
              {fullScreen ? <Minimize2 className="size-4" /> : <Maximize2 className="size-4" />}
            </button>
            <button
              type="button"
              onClick={onClose}
              className="inline-flex h-9 w-9 items-center justify-center rounded-lg border border-white/10 bg-white/5 text-slate-400 transition-colors hover:border-white/20 hover:text-slate-200"
            >
              <X className="size-4" />
            </button>
          </div>
        </div>

        <div className={`${modalBodyHeightClass} overflow-auto p-5`}>
          <div className="mb-4 overflow-hidden rounded-2xl border border-white/10 bg-[radial-gradient(circle_at_top_left,var(--accent-alpha-12),transparent_42%),linear-gradient(180deg,rgba(255,255,255,0.03),rgba(255,255,255,0))]">
            <div className="border-b border-white/10 px-5 py-4">
              <p className="text-[11px] font-semibold uppercase tracking-[0.3em] text-[color:var(--accent-text-kicker)]">Lifecycle</p>
              <p className="mt-1 text-xl font-semibold text-slate-100">{botLifecycle.label}</p>
              <p className="mt-2 text-sm leading-relaxed text-slate-300">{statusMessage || runSummaryText || botLifecycle.detail}</p>
              <p className="mt-2 text-xs text-slate-500">{runSummaryText || `bot=${botStatus}`}</p>
            </div>
          </div>

          {statusMessage ? (
            <div className="mb-3 rounded border border-white/10 bg-black/20 px-3 py-2 text-sm text-slate-300">{statusMessage}</div>
          ) : null}
          {error ? <div className="mb-3 rounded-xl border border-rose-500/30 bg-rose-500/10 px-3 py-2 text-sm text-rose-100">{error}</div> : null}
          {ledgerState?.error ? (
            <div className="mb-3 rounded-xl border border-amber-500/30 bg-amber-500/10 px-3 py-2 text-sm text-amber-100">
              DB ledger unavailable: {ledgerState.error}
            </div>
          ) : null}

          {!runStore ? (
            <div className="rounded-2xl border border-white/10 bg-black/20 px-6 py-6 text-sm text-slate-300">
              {statusMessage || 'Waiting for an active BotLens run session.'}
            </div>
          ) : (
            <>
              <div className="mb-4 rounded-2xl border border-white/10 bg-black/20 p-4">
                <div className="flex flex-col gap-4 xl:flex-row xl:items-center xl:justify-between">
                  <div className="min-w-0 flex-1">
                    <p className="text-[10px] font-semibold uppercase tracking-[0.32em] text-slate-500">Symbols</p>
                    <div className="mt-2 flex flex-wrap gap-2">
                      {symbolOptions.map((entry) => {
                        const selected = entry.symbol_key === selectedSymbolKey
                        return (
                          <button
                            key={entry.symbol_key}
                            type="button"
                            onClick={() => changeSelectedSymbol(entry.symbol_key)}
                            className={`rounded-lg border px-3 py-1.5 text-xs font-semibold uppercase tracking-[0.24em] transition ${detailTabClass(selected)}`}
                          >
                            {entry.symbol || entry.symbol_key} · {entry.timeframe || '—'}
                          </button>
                        )
                      })}
                    </div>
                    <p className="mt-3 text-sm text-slate-400">
                      One live session per run. Switching symbols is local and keeps the websocket attached to run {activeRunId || '—'}.
                    </p>
                  </div>
                  <div className="flex flex-wrap items-center gap-2">
                    <div className="rounded-lg border border-white/10 bg-white/[0.03] px-3 py-2 text-xs text-slate-300">
                      <p className="text-[10px] uppercase tracking-[0.24em] text-slate-500">Stream</p>
                      <p className="mt-1 font-semibold text-slate-100">{streamState}</p>
                    </div>
                    <div className="rounded-lg border border-white/10 bg-white/[0.03] px-3 py-2 text-xs text-slate-300">
                      <p className="text-[10px] uppercase tracking-[0.24em] text-slate-500">Selected</p>
                      <p className="mt-1 font-semibold text-slate-100">{selectedLabel}</p>
                    </div>
                    <button
                      type="button"
                      onClick={() => setWarningPanelOpen((prev) => !prev)}
                      className={`inline-flex items-center gap-2 rounded-lg border px-3 py-2 text-xs font-medium transition ${
                        activeWarningCount > 0
                          ? 'border-amber-400/40 bg-amber-400/10 text-amber-100 hover:border-amber-300/60'
                          : 'border-white/10 bg-white/[0.03] text-slate-300 hover:border-white/20 hover:text-slate-100'
                      }`}
                    >
                      <AlertTriangle className="size-3.5" />
                      <span>Warnings</span>
                      <span className={`rounded-full px-1.5 py-0.5 text-[10px] font-semibold ${activeWarningCount > 0 ? 'bg-amber-200/20 text-amber-50' : 'bg-white/10 text-slate-300'}`}>
                        {activeWarningCount}
                      </span>
                      <ChevronDown className={`size-3.5 transition-transform ${warningPanelOpen ? 'rotate-180' : ''}`} />
                    </button>
                    <button
                      type="button"
                      onClick={loadOlderHistory}
                      disabled={!selectedDetail?.candles?.length}
                      className="rounded-lg border border-white/10 bg-white/5 px-3 py-2 text-xs font-medium text-slate-300 transition hover:border-[color:var(--accent-alpha-30)] hover:text-[color:var(--accent-text-strong)] disabled:cursor-not-allowed disabled:opacity-50"
                    >
                      Load older
                    </button>
                  </div>
                </div>
                {warningPanelOpen ? (
                  <div className="mt-4 rounded-2xl border border-white/10 bg-white/[0.03] p-3">
                    <div className="mb-3 flex items-center justify-between gap-3">
                      <div>
                        <p className="text-[10px] font-semibold uppercase tracking-[0.28em] text-slate-500">Indicator Warnings</p>
                        <p className="mt-1 text-sm text-slate-300">
                          Guard warnings are grouped by indicator and symbol. Zero-state stays quiet.
                        </p>
                      </div>
                      <p className="text-xs text-slate-500">{activeWarningCount} active</p>
                    </div>
                    {warningItems.length ? (
                      <div className="grid gap-2">
                        {warningItems.map((warning) => (
                          <div
                            key={warning.warning_id || warning.id}
                            className="rounded-xl border border-white/10 bg-black/20 px-3 py-3"
                          >
                            <div className="flex flex-wrap items-start justify-between gap-2">
                              <div className="min-w-0">
                                <p className="text-sm font-medium text-slate-100">{warningRowTitle(warning)}</p>
                                <p className="mt-1 text-xs leading-relaxed text-slate-400">{warning.message}</p>
                              </div>
                              <div className="flex items-center gap-2 text-[11px] text-slate-400">
                                <span className="rounded-full bg-white/5 px-2 py-1 uppercase tracking-[0.18em]">
                                  x{Math.max(1, Number(warning.count || 1) || 1)}
                                </span>
                                <span>{formatRelativeTime(warning.last_seen_at || warning.first_seen_at)}</span>
                              </div>
                            </div>
                          </div>
                        ))}
                      </div>
                    ) : (
                      <div className="rounded-xl border border-dashed border-white/10 px-4 py-5 text-sm text-slate-400">
                        No runtime indicator warnings.
                      </div>
                    )}
                  </div>
                ) : null}
              </div>

              <div className="mb-4">
                <OverlayToggleBar
                  overlays={overlayOptions}
                  visibility={visibility}
                  onToggle={toggleOverlay}
                  collapsed={overlayPanelCollapsed}
                  onToggleCollapse={() => setOverlayPanelCollapsed((prev) => !prev)}
                />
              </div>

              <div className="mb-4 rounded-2xl border border-white/10 bg-black/20 p-3">
                {selectedDetail ? (
                  <BotLensChart
                    chartId={`botlens-live-${bot.id}`}
                    candles={chartCandles}
                    trades={chartTrades}
                    overlays={visibleOverlays}
                    mode={bot.mode}
                    playbackSpeed={Number(bot.playback_speed || 0)}
                    timeframe={selectedDetail?.timeframe || null}
                    overlayVisibility={visibility}
                    followLive={followLive}
                    heightClass="h-[430px]"
                  />
                ) : (
                  <div className="flex h-[430px] items-center justify-center rounded-xl border border-dashed border-white/10 text-sm text-slate-400">
                    {loadingDetailSymbolKey === selectedSymbolKey ? `Loading detail for ${selectedLabel}...` : 'Select a symbol to load detail.'}
                  </div>
                )}
              </div>

              <div className="rounded-2xl border border-white/10 bg-black/20 p-4">
                <div className="mb-4 flex flex-wrap items-center gap-2">
                  {[
                    ['overview', 'Overview'],
                    ['activity', 'Activity'],
                    ['diagnostics', 'Diagnostics'],
                  ].map(([key, label]) => (
                    <button
                      key={key}
                      type="button"
                      onClick={() => setDetailTab(key)}
                      className={`rounded-lg border px-3 py-2 text-xs font-semibold uppercase tracking-[0.24em] transition ${detailTabClass(detailTab === key)}`}
                    >
                      {label}
                    </button>
                  ))}
                </div>

                {detailTab === 'overview' ? (
                  <div className="space-y-4">
                    <div className="grid gap-4 xl:grid-cols-2">
                      <div>
                        <p className="mb-2 text-[10px] font-semibold uppercase tracking-[0.32em] text-slate-500">Run Summary</p>
                        <DataRows
                          rows={[
                            { key: 'run-status', label: 'Run Status', value: runStore.health?.status || botStatus || '—' },
                            { key: 'phase', label: 'Phase', value: runStore.lifecycle?.phase || '—' },
                            { key: 'symbols', label: 'Tracked Symbols', value: String(symbolOptions.length) },
                            { key: 'open-trades', label: 'Open Trades', value: String(openTrades.length) },
                            { key: 'selected', label: 'Selected Symbol', value: selectedLabel },
                            { key: 'started', label: 'Started', value: formatMoment(runStore.runMeta?.started_at) },
                          ]}
                        />
                      </div>
                      <div>
                        <p className="mb-2 text-[10px] font-semibold uppercase tracking-[0.32em] text-slate-500">Selected Detail</p>
                        <DataRows
                          rows={[
                            { key: 'detail-status', label: 'Detail Status', value: selectedDetail?.status || selectedSummary?.status || '—' },
                            { key: 'continuity', label: 'Continuity', value: selectedDetail?.continuity?.status || selectedSummary?.continuity_status || '—' },
                            { key: 'last-event', label: 'Last Event', value: formatMoment(selectedDetail?.last_event_at || selectedSummary?.last_event_at) },
                            { key: 'candles', label: 'Recent Candles', value: String(chartCandles.length) },
                            { key: 'recent-trades', label: 'Recent Trades', value: String(chartTrades.length) },
                            { key: 'net-pnl', label: 'Net P&L', value: formatNumber(Number(selectedDetail?.stats?.net_pnl || selectedSummary?.stats?.net_pnl || 0)) },
                          ]}
                        />
                      </div>
                    </div>
                    <div>
                      <div className="mb-2 flex items-center justify-between">
                        <p className="text-[10px] font-semibold uppercase tracking-[0.32em] text-slate-500">Per Symbol Stats</p>
                        <p className="text-xs text-slate-500">{seriesStats.length} tracked</p>
                      </div>
                      {seriesStats.length ? (
                        <div className="overflow-hidden rounded-xl border border-white/10">
                          <table className="min-w-full text-left text-sm text-slate-200">
                            <thead className="bg-white/[0.03] text-[10px] uppercase tracking-[0.28em] text-slate-500">
                              <tr>
                                <th className="px-4 py-3">Symbol</th>
                                <th className="px-4 py-3">Timeframe</th>
                                <th className="px-4 py-3">Trades</th>
                                <th className="px-4 py-3">Win Rate</th>
                                <th className="px-4 py-3">Net P&L</th>
                                <th className="px-4 py-3">Max DD</th>
                              </tr>
                            </thead>
                            <tbody className="divide-y divide-white/5">
                              {seriesStats.map((entry) => (
                                <tr key={entry.key} className={entry.key === selectedSymbolKey ? 'bg-[color:var(--accent-alpha-05)]' : ''}>
                                  <td className="px-4 py-3 font-medium text-slate-100">{entry.symbol}</td>
                                  <td className="px-4 py-3">{entry.timeframe}</td>
                                  <td className="px-4 py-3 tabular-nums">{entry.trades}</td>
                                  <td className="px-4 py-3 tabular-nums">{formatPercent(entry.winRate)}</td>
                                  <td className={`px-4 py-3 tabular-nums ${entry.netPnl >= 0 ? 'text-emerald-300' : 'text-rose-300'}`}>
                                    {formatNumber(entry.netPnl)}
                                  </td>
                                  <td className="px-4 py-3 tabular-nums">{formatNumber(entry.maxDrawdown)}</td>
                                </tr>
                              ))}
                            </tbody>
                          </table>
                        </div>
                      ) : (
                        <div className="rounded-xl border border-dashed border-white/10 px-4 py-6 text-sm text-slate-400">
                          No symbol summaries are available yet.
                        </div>
                      )}
                    </div>
                  </div>
                ) : null}

                {detailTab === 'activity' ? (
                  <div className="space-y-4">
                    <div className="grid gap-4 xl:grid-cols-[minmax(0,0.95fr)_minmax(0,1.05fr)]">
                      <div className="rounded-2xl border border-white/10 bg-white/[0.03] p-4">
                        <div className="mb-3 flex items-center justify-between">
                          <p className="text-[10px] font-semibold uppercase tracking-[0.32em] text-slate-500">Open Trades</p>
                          <p className="text-xs text-slate-500">{tradeCards.length} active</p>
                        </div>
                        {tradeCards.length ? (
                          <div className="grid gap-2">
                            {tradeCards.map((entry) => {
                              const context = symbolPriceContext.get(normalizeSeriesKey(entry.trade?.symbol_key || '')) || null
                              return (
                                <ActiveTradeChip
                                  key={entry.id}
                                  chip={entry.chip}
                                  trade={entry.trade}
                                  currentPrice={context?.currentPrice}
                                  latestBarTime={context?.latestBarTime}
                                  visible={!hoveredTradeId || hoveredTradeId === entry.id}
                                  onHover={(hovering) => setHoveredTradeId(hovering ? entry.id : null)}
                                  isActiveSymbol={normalizeSeriesKey(entry.trade?.symbol_key || '') === selectedSymbolKey}
                                  onClick={() => {
                                    if (entry.trade?.symbol_key) changeSelectedSymbol(entry.trade.symbol_key)
                                  }}
                                />
                              )
                            })}
                          </div>
                        ) : (
                          <div className="rounded-xl border border-dashed border-white/10 px-4 py-6 text-sm text-slate-400">
                            No active trades right now.
                          </div>
                        )}
                      </div>
                      <div className="rounded-2xl border border-white/10 bg-white/[0.03] p-4">
                        <div className="mb-3 flex items-center justify-between">
                          <p className="text-[10px] font-semibold uppercase tracking-[0.32em] text-slate-500">Decision Ledger</p>
                          <p className="text-xs text-slate-500">{ledgerEvents.length} events</p>
                        </div>
                        <DecisionTrace ledgerEvents={ledgerEvents} />
                      </div>
                    </div>
                    <TradeLogList logs={logs} logTab={logTab} onTabChange={setLogTab} onFocusLog={() => {}} />
                  </div>
                ) : null}

                {detailTab === 'diagnostics' ? (
                  <div className="grid gap-4 xl:grid-cols-2">
                    <div>
                      <p className="mb-2 text-[10px] font-semibold uppercase tracking-[0.32em] text-slate-500">Transport</p>
                      <DataRows
                        rows={[
                          { key: 'stream-state', label: 'Stream State', value: streamState },
                          { key: 'cursor', label: 'Cursor', value: String(runStore.seq || 0) },
                          { key: 'selected', label: 'Selected Symbol', value: selectedLabel },
                          { key: 'cache-size', label: 'Detail Cache', value: String(Object.keys(runStore.detailCache || {}).length) },
                          { key: 'loading', label: 'Loading Detail', value: loadingDetailSymbolKey || '—' },
                        ]}
                      />
                    </div>
                    <div>
                      <p className="mb-2 text-[10px] font-semibold uppercase tracking-[0.32em] text-slate-500">Run Health</p>
                      <DataRows
                        rows={[
                          { key: 'health-status', label: 'Health Status', value: runStore.health?.status || '—' },
                          { key: 'health-phase', label: 'Lifecycle Phase', value: runStore.lifecycle?.phase || '—' },
                          { key: 'workers', label: 'Workers', value: `${runStore.health?.active_workers || 0}/${runStore.health?.worker_count || 0}` },
                          { key: 'warnings', label: 'Warnings', value: String(activeWarningCount) },
                          { key: 'last-event', label: 'Last Event', value: formatMoment(runStore.health?.last_event_at) },
                        ]}
                      />
                    </div>
                  </div>
                ) : null}
              </div>
            </>
          )}
        </div>
      </div>
    </div>
  )
}
