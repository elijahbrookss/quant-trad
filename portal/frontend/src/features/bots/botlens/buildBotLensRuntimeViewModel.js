import {
  describeBotLifecycle,
  getBotRunId,
  getBotStatus,
  normalizeBotStatus,
} from '../state/botRuntimeStatus.js'
import { normalizeSeriesKey } from '../../../components/bots/botlensProjection.js'
import { executionModeUsesIntrabar, formatExecutionModeLabel, resolveExecutionMode } from '../executionMode.js'

function formatPercent(value) {
  if (typeof value !== 'number' || Number.isNaN(value)) return '—'
  return `${(value * 100).toFixed(1)}%`
}

function formatNumber(value, digits = 2) {
  if (typeof value !== 'number' || Number.isNaN(value)) return '—'
  return value.toFixed(digits)
}

function formatSignedNumber(value, digits = 2) {
  if (typeof value !== 'number' || Number.isNaN(value)) return '—'
  if (value === 0) return value.toFixed(digits)
  return `${value > 0 ? '+' : ''}${value.toFixed(digits)}`
}

function formatPrice(value) {
  const numeric = Number(value)
  if (!Number.isFinite(numeric)) return '—'
  const abs = Math.abs(numeric)
  if (abs >= 1000) return numeric.toFixed(2)
  if (abs >= 100) return numeric.toFixed(3)
  if (abs >= 1) return numeric.toFixed(4)
  return numeric.toFixed(6)
}

function formatMoment(value) {
  if (!value) return '—'
  try {
    return new Date(value).toLocaleString()
  } catch {
    return String(value)
  }
}

function formatDateOnly(value) {
  if (!value) return '—'
  const date = new Date(value)
  if (Number.isNaN(date.getTime())) return String(value)
  return date.toLocaleDateString(undefined, { year: 'numeric', month: 'short', day: 'numeric', timeZone: 'UTC' })
}

function formatRelativeTime(value) {
  if (!value) return '—'
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

function shortId(value, length = 8) {
  const normalized = String(value || '').trim()
  return normalized ? normalized.slice(0, length) : '—'
}

function humanizeToken(value) {
  const normalized = String(value || '').trim().toLowerCase()
  if (!normalized) return '—'
  return normalized
    .split('_')
    .filter(Boolean)
    .map((segment) => segment.charAt(0).toUpperCase() + segment.slice(1))
    .join(' ')
}

function normalizeRunMode(value) {
  const normalized = String(value || '').trim().toLowerCase()
  if (!normalized) return null
  if (['paper', 'paper_trade', 'paper_trading', 'sim_trade', 'sim'].includes(normalized)) return 'paper'
  if (normalized === 'live') return 'live'
  if (normalized === 'backtest') return 'backtest'
  return normalized
}

function buildRunModeBadge(value) {
  const key = normalizeRunMode(value)
  if (!key) return null
  if (key === 'paper') return { key, label: 'Paper', tone: 'amber' }
  if (key === 'live') return { key, label: 'Live', tone: 'rose' }
  if (key === 'backtest') return { key, label: 'Backtest', tone: 'sky' }
  return { key, label: humanizeToken(key), tone: 'slate' }
}

function formatBooleanState(value, { trueLabel = 'Yes', falseLabel = 'No' } = {}) {
  return value ? trueLabel : falseLabel
}

function warningRowTitle(warning) {
  if (String(warning?.warning_type || '').trim() === 'execution_intrabar_fallback_pessimistic') {
    const symbol = String(warning?.symbol || '').trim()
    const timeframe = String(warning?.timeframe || '').trim().toUpperCase()
    const title = String(warning?.title || 'Intrabar fallback').trim()
    return [symbol, timeframe, title].filter(Boolean).join(' · ') || title
  }
  const indicator = String(warning?.indicator_id || '').trim() || 'indicator'
  const symbol = String(warning?.symbol || '').trim()
  const title = String(warning?.title || '').trim()
  if (!title) return symbol ? `${indicator} · ${symbol}` : indicator
  return symbol ? `${indicator} · ${symbol} · ${title}` : `${indicator} · ${title}`
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

function shouldSurfaceStatusMessage(value) {
  const normalized = String(value || '').trim().toLowerCase()
  if (!normalized) return false
  if (normalized.includes('ready')) return false
  return ['loading', 'failed', 'error', 'unavailable', 'required', 'degraded'].some((token) => normalized.includes(token))
}

function buildNotices({ statusMessage, error }) {
  const notices = []
  if (shouldSurfaceStatusMessage(statusMessage)) {
    notices.push({
      key: 'status',
      tone: 'neutral',
      message: statusMessage,
    })
  }
  if (error) {
    notices.push({
      key: 'runtime-error',
      tone: 'error',
      message: error,
    })
  }
  return notices
}

function topBarTone(status) {
  const normalized = normalizeBotStatus(status)
  if (normalized === 'running') return 'emerald'
  if (normalized === 'degraded' || normalized === 'paused' || normalized === 'telemetry_degraded') return 'amber'
  if (normalized === 'starting' || normalized === 'bootstrapping') return 'sky'
  if (['error', 'failed', 'failed_start', 'crashed', 'startup_failed'].includes(normalized)) return 'rose'
  return 'slate'
}

function buildRecentTradeRows(trades = []) {
  return (Array.isArray(trades) ? trades : []).map((trade, index) => ({
    key: String(trade?.event_id || trade?.trade_id || `${trade?.event_ts || trade?.entry_time || 'trade'}-${index}`),
    symbol: String(trade?.symbol || '—'),
    status: humanizeToken(trade?.status || 'open'),
    direction: String(trade?.direction || '').trim().toUpperCase() || '—',
    openedAt: formatMoment(trade?.event_ts || trade?.opened_at || trade?.entry_time || trade?.created_at),
    entryPrice: formatNumber(Number(trade?.entry_price), 2),
    exitPrice: formatNumber(Number(trade?.exit_price), 2),
    netPnl: formatSignedNumber(Number(trade?.net_pnl), 2),
    tradeId: String(trade?.trade_id || '').trim() || '—',
    closedAt: formatMoment(trade?.exit_time || trade?.closed_at),
    quantity: formatNumber(Number(trade?.quantity ?? trade?.qty ?? trade?.contracts), 2),
    exitReason: humanizeToken(trade?.exit_reason || trade?.close_reason || '—'),
    technical: trade,
  }))
}

function stableDiagnosticIdentity(entry) {
  const affected = entry?.affected_identity && typeof entry.affected_identity === 'object'
    ? entry.affected_identity
    : {}
  const details = affected.details && typeof affected.details === 'object' ? affected.details : {}
  return [
    affected.instrument_id,
    affected.series_key,
    affected.symbol,
    affected.timeframe,
    details.component,
    details.operation,
    details.storage_target,
  ].filter(Boolean).map(String).join('|') || 'run'
}

export function groupBotLensDiagnostics(entries = []) {
  const groups = new Map()
  ;(Array.isArray(entries) ? entries : []).forEach((entry, index) => {
    if (!entry || typeof entry !== 'object') return
    const severity = String(entry.severity || entry.level || 'info').trim().toLowerCase()
    const source = String(entry.source || entry.component || 'runtime').trim().toLowerCase()
    const code = String(entry.code || entry.diagnostic_code || entry.operation || 'runtime_event').trim().toLowerCase()
    const identity = stableDiagnosticIdentity(entry)
    const key = [severity, source, code, identity].join(':')
    const timestamp = entry.timestamp || entry.known_at || entry.event_ts || entry.observed_at || null
    const current = groups.get(key) || {
      key, severity, level: severity.toUpperCase(), source,
      component: humanizeToken(source), code, title: humanizeToken(code),
      message: String(entry.message || 'Runtime diagnostic recorded.'),
      count: 0, firstTimestamp: timestamp, lastTimestamp: timestamp,
      readinessImpact: entry.readiness_impact || 'none',
      suggestedNextStep: entry.suggested_next_step || null,
      affectedIdentity: entry.affected_identity || {}, occurrences: [],
    }
    current.count += 1
    if (timestamp && (!current.firstTimestamp || Date.parse(timestamp) < Date.parse(current.firstTimestamp))) current.firstTimestamp = timestamp
    if (timestamp && (!current.lastTimestamp || Date.parse(timestamp) > Date.parse(current.lastTimestamp))) current.lastTimestamp = timestamp
    current.occurrences.push({ index, ...entry })
    groups.set(key, current)
  })
  const rank = { critical: 0, warning: 1, info: 2 }
  return Array.from(groups.values())
    .sort((left, right) => (rank[left.severity] ?? 3) - (rank[right.severity] ?? 3)
      || right.count - left.count || left.code.localeCompare(right.code))
    .map((group) => ({
      ...group,
      occurredAt: group.count > 1
        ? `${formatMoment(group.firstTimestamp)} – ${formatMoment(group.lastTimestamp)}`
        : formatMoment(group.lastTimestamp),
      technical: {
        severity: group.severity, source: group.source, code: group.code, count: group.count,
        affected_identity: group.affectedIdentity, readiness_impact: group.readinessImpact,
        suggested_next_step: group.suggestedNextStep, occurrences: group.occurrences,
      },
    }))
}

function buildDecisionRows(entries = []) {
  return (Array.isArray(entries) ? entries : []).map((entry, index) => ({
    key: String(entry?.decision_id || entry?.event_id || `decision-${index}`),
    occurredAt: formatMoment(entry?.bar_time || entry?.known_at || entry?.event_ts),
    action: humanizeToken(entry?.action || entry?.decision_context?.intent || 'decision'),
    verdict: humanizeToken(entry?.verdict || entry?.status || (entry?.accepted ? 'accepted' : entry?.rejected ? 'rejected' : 'observed')),
    reason: humanizeToken(entry?.reason_code || entry?.rejection_reason || entry?.reason || '—'),
    direction: String(entry?.decision_context?.direction || entry?.direction || entry?.artifact_summary?.side || '').trim().toUpperCase() || '—',
    price: formatPrice(entry?.selected_price ?? entry?.price ?? entry?.decision_context?.signal_price),
    tradeId: String(entry?.trade_id || ''),
    technical: entry,
  }))
}

function buildDiagnosticRows(entries = []) {
  return (Array.isArray(entries) ? entries : []).map((entry, index) => ({
    key: String(entry?.event_id || entry?.id || `diagnostic-${index}`),
    level: String(entry?.level || 'INFO').trim().toUpperCase() || 'INFO',
    code: humanizeToken(entry?.diagnostic_code || entry?.diagnostic_event || entry?.operation || 'runtime_event'),
    message: String(entry?.message || 'Runtime diagnostic recorded.'),
    component: humanizeToken(entry?.component || 'runtime'),
    status: humanizeToken(entry?.status || 'observed'),
    occurredAt: formatMoment(entry?.event_ts || entry?.observed_at || entry?.bar_time),
    technical: entry,
  }))
}

function buildPriceContext(candles = []) {
  const rows = Array.isArray(candles) ? candles : []
  const last = rows[rows.length - 1] || null
  const previous = rows[rows.length - 2] || null
  const lastClose = Number(last?.close)
  const previousClose = Number(previous?.close)
  const change = Number.isFinite(lastClose) && Number.isFinite(previousClose)
    ? lastClose - previousClose
    : null
  const changePct = Number.isFinite(change) && Number.isFinite(previousClose) && previousClose !== 0
    ? change / previousClose
    : null
  const direction = Number.isFinite(change)
    ? change > 0
      ? 'up'
      : change < 0
        ? 'down'
        : 'flat'
    : 'unknown'

  return {
    lastPrice: Number.isFinite(lastClose) ? formatPrice(lastClose) : '—',
    change: Number.isFinite(change) ? formatSignedNumber(change, Math.abs(change) >= 1 ? 2 : 4) : '—',
    changePct: Number.isFinite(changePct) ? `${changePct > 0 ? '+' : ''}${(changePct * 100).toFixed(2)}%` : '—',
    direction,
  }
}

function normalizeTimestamp(value) {
  if (value === undefined || value === null || value === '') return null
  if (typeof value === 'number' && Number.isFinite(value)) {
    const epochMs = value > 1e12 ? value : value * 1000
    const date = new Date(epochMs)
    return Number.isNaN(date.getTime()) ? null : date.toISOString()
  }
  const date = new Date(value)
  if (Number.isNaN(date.getTime())) {
    const text = String(value || '').trim()
    return text || null
  }
  return date.toISOString()
}

function isClosedTradeState(value) {
  const normalized = String(value || '').trim().toLowerCase()
  return ['closed', 'completed', 'complete', 'exited'].includes(normalized)
}

function buildSignalLedgerEntry(entry, index = 0) {
  if (!entry || typeof entry !== 'object') return null
  const timestamp = normalizeTimestamp(entry.event_ts || entry.bar_time || entry.bar_epoch)
  return {
    event_id: String(entry.event_id || entry.signal_id || `signal-${index}`),
    parent_event_id: entry.parent_event_id || entry.parent_id || null,
    root_id: entry.root_id || null,
    created_at: timestamp,
    event_ts: timestamp,
    event_type: 'signal',
    event_subtype: String(entry.signal_type || 'strategy_signal').trim().toLowerCase() || 'strategy_signal',
    reason_code: entry.signal_type || null,
    reason_detail: entry.message || null,
    signal_id: entry.signal_id || entry.event_id || null,
    decision_id: entry.decision_id || null,
    instrument_id: entry.instrument_id || null,
    symbol: entry.symbol || null,
    timeframe: entry.timeframe || null,
    side: entry.direction || null,
    price: Number.isFinite(Number(entry.signal_price)) ? Number(entry.signal_price) : null,
    rule_id: entry.rule_id || null,
    intent: entry.intent || null,
    event_key: entry.event_key || null,
    payload: { ...entry },
  }
}

function buildDecisionLedgerEntry(entry, index = 0) {
  if (!entry || typeof entry !== 'object') return null
  const timestamp = normalizeTimestamp(entry.event_ts || entry.bar_time || entry.bar_epoch)
  const state = String(entry.decision_state || '').trim().toLowerCase()
  return {
    event_id: String(entry.event_id || entry.decision_id || `decision-${index}`),
    parent_event_id: entry.parent_event_id || entry.parent_id || null,
    root_id: entry.root_id || null,
    created_at: timestamp,
    event_ts: timestamp,
    event_type: 'decision',
    event_subtype: state === 'rejected' ? 'signal_rejected' : 'signal_accepted',
    reason_code: entry.reason_code || null,
    reason_detail: entry.message || null,
    signal_id: entry.signal_id || entry.decision_id || null,
    decision_id: entry.decision_id || entry.event_id || null,
    instrument_id: entry.instrument_id || null,
    symbol: entry.symbol || null,
    timeframe: entry.timeframe || null,
    side: entry.direction || null,
    price: Number.isFinite(Number(entry.signal_price)) ? Number(entry.signal_price) : null,
    rule_id: entry.rule_id || null,
    intent: entry.intent || null,
    event_key: entry.event_key || null,
    payload: { ...entry },
  }
}

function buildTradeLedgerEntry(entry, index = 0) {
  if (!entry || typeof entry !== 'object') return null
  const closed = isClosedTradeState(entry.trade_state || entry.status)
  const timestamp = normalizeTimestamp(entry.event_ts || entry.updated_at || entry.closed_at || entry.opened_at)
  const price = closed ? entry.exit_price : entry.entry_price
  return {
    event_id: String(entry.event_id || entry.trade_id || `trade-${index}`),
    parent_event_id: entry.parent_event_id || entry.parent_id || null,
    root_id: entry.root_id || null,
    created_at: timestamp,
    event_ts: timestamp,
    event_type: 'execution',
    event_subtype: closed ? 'close' : 'entry',
    reason_code: entry.trade_state || entry.status || null,
    reason_detail: entry.message || null,
    trade_id: entry.trade_id || null,
    instrument_id: entry.instrument_id || null,
    symbol: entry.symbol || null,
    timeframe: entry.timeframe || null,
    side: entry.direction || entry.side || null,
    qty: Number.isFinite(Number(entry.qty)) ? Number(entry.qty) : null,
    price: Number.isFinite(Number(price)) ? Number(price) : null,
    event_impact_pnl: Number.isFinite(Number(entry.event_impact_pnl)) ? Number(entry.event_impact_pnl) : null,
    trade_net_pnl: Number.isFinite(Number(entry.trade_net_pnl ?? entry.net_pnl)) ? Number(entry.trade_net_pnl ?? entry.net_pnl) : null,
    payload: { ...entry },
  }
}

export function buildBotLensDecisionLedgerEntries({
  signals = [],
  decisions = [],
  trades = [],
} = {}) {
  return [
    ...(Array.isArray(signals) ? signals : []).map((entry, index) => buildSignalLedgerEntry(entry, index)),
    ...(Array.isArray(decisions) ? decisions : []).map((entry, index) => buildDecisionLedgerEntry(entry, index)),
    ...(Array.isArray(trades) ? trades : []).map((entry, index) => buildTradeLedgerEntry(entry, index)),
  ]
    .filter(Boolean)
    .sort((left, right) => {
      const leftTs = Date.parse(left.created_at || left.event_ts || '') || 0
      const rightTs = Date.parse(right.created_at || right.event_ts || '') || 0
      if (leftTs !== rightTs) return leftTs - rightTs
      return String(left.event_id || '').localeCompare(String(right.event_id || ''))
    })
}

export function buildBotLensForensicLedgerEntries(documents = []) {
  const signals = []
  const decisions = []
  const trades = []
  ;(Array.isArray(documents) ? documents : []).forEach((document) => {
    const truth = document?.truth && typeof document.truth === 'object' ? document.truth : {}
    const context = truth.context && typeof truth.context === 'object' ? truth.context : {}
    const eventName = String(truth.event_name || '').trim().toUpperCase()
    const entry = {
      ...context,
      event_id: truth.event_id || document?.document_id || null,
      parent_event_id: truth.parent_event_id || null,
      root_id: truth.root_event_id || null,
      event_ts: truth.event_ts || context.event_ts || context.bar_time || null,
      known_at: truth.known_at || null,
      event_name: eventName,
      series_key: truth.series_key || null,
      seq: truth.seq,
      row_id: truth.row_id,
    }
    if (eventName === 'SIGNAL_EMITTED') {
      signals.push(entry)
    } else if (eventName === 'DECISION_EMITTED') {
      decisions.push(entry)
    } else if (eventName === 'ENTRY_FILLED' || eventName === 'EXIT_FILLED') {
      trades.push({
        ...entry,
        trade_state: eventName === 'EXIT_FILLED' ? 'closed' : 'opened',
        entry_price: eventName === 'ENTRY_FILLED' ? context.price : context.entry_price,
        exit_price: eventName === 'EXIT_FILLED' ? context.price : context.exit_price,
        status: eventName === 'EXIT_FILLED' ? 'closed' : 'open',
      })
    } else if (['TRADE_OPENED', 'TRADE_UPDATED', 'TRADE_CLOSED'].includes(eventName)) {
      trades.push(entry)
    }
  })
  return buildBotLensDecisionLedgerEntries({ signals, decisions, trades })
}

function mergeLedgerEntries(...groups) {
  const byId = new Map()
  groups.flat().filter(Boolean).forEach((entry) => {
    const eventId = String(entry?.event_id || '').trim()
    if (eventId) byId.set(eventId, entry)
  })
  return Array.from(byId.values()).sort((left, right) => {
    const leftTs = Date.parse(left.created_at || left.event_ts || '') || 0
    const rightTs = Date.parse(right.created_at || right.event_ts || '') || 0
    if (leftTs !== rightTs) return leftTs - rightTs
    return String(left.event_id || '').localeCompare(String(right.event_id || ''))
  })
}

function buildDecisionSummaryRows({ ledgerEntries = [] }) {
  const entries = Array.isArray(ledgerEntries) ? ledgerEntries : []
  const signals = entries.filter((entry) => entry.event_type === 'signal').length
  const decisions = entries.filter((entry) => entry.event_type === 'decision')
  const accepted = decisions.filter((entry) => entry.event_subtype === 'signal_accepted').length
  const rejected = decisions.filter((entry) => entry.event_subtype === 'signal_rejected').length
  const trades = entries.filter((entry) => entry.event_type === 'execution').length
  return [
    { key: 'ledger-events', label: 'Ledger Events', value: String(ledgerEntries.length) },
    { key: 'signals', label: 'Signals Emitted', value: String(signals) },
    { key: 'accepted', label: 'Accepted Decisions', value: String(accepted) },
    { key: 'rejected', label: 'Rejected Decisions', value: String(rejected) },
    { key: 'trades', label: 'Trade Executions', value: String(trades) },
  ]
}

function buildDecisionLatestRows({ signals = [], decisions = [], trades = [], runtime = {} }) {
  const lastSignal = (Array.isArray(signals) ? signals : []).at(-1) || null
  const lastDecision = (Array.isArray(decisions) ? decisions : []).at(-1) || null
  const lastTrade = (Array.isArray(trades) ? trades : []).at(-1) || null
  return [
    {
      key: 'last-signal',
      label: 'Last Signal',
      value: lastSignal
        ? [
            humanizeToken(lastSignal.signal_type || 'signal'),
            String(lastSignal.direction || '').trim().toUpperCase() || null,
            formatMoment(lastSignal.event_ts || lastSignal.bar_time || lastSignal.bar_epoch),
          ].filter(Boolean).join(' · ')
        : '—',
    },
    {
      key: 'last-decision',
      label: 'Last Decision',
      value: lastDecision
        ? [
            humanizeToken(lastDecision.decision_state || 'decision'),
            lastDecision.reason_code || null,
            formatMoment(lastDecision.event_ts || lastDecision.bar_time || lastDecision.bar_epoch),
          ].filter(Boolean).join(' · ')
        : '—',
    },
    {
      key: 'last-trade',
      label: 'Last Trade',
      value: lastTrade
        ? [
            humanizeToken(lastTrade.trade_state || lastTrade.status || 'trade'),
            String(lastTrade.direction || lastTrade.side || '').trim().toUpperCase() || null,
            formatMoment(lastTrade.event_ts || lastTrade.updated_at || lastTrade.closed_at || lastTrade.opened_at),
          ].filter(Boolean).join(' · ')
        : '—',
    },
    {
      key: 'runtime-event',
      label: 'Runtime Update',
      value: formatMoment(runtime?.last_event_at),
    },
  ]
}

export function buildBotLensWalletRows({
  openTradeCount = 0,
  recentTrades = [],
  runtime = {},
  stats = {},
} = {}) {
  const normalizedStats = stats && typeof stats === 'object' ? stats : {}
  const runtimeState = String(runtime?.runtime_state || runtime?.status || '').trim()
  const totalFees = normalizedStats.fees_paid ?? normalizedStats.total_fees
  const rows = [
    {
      key: 'quote-currency',
      label: 'Quote Currency',
      value: String(normalizedStats.quote_currency || '').trim().toUpperCase() || '—',
    },
    {
      key: 'net-pnl',
      label: 'Net P&L',
      value: formatSignedNumber(Number(normalizedStats.net_pnl), 2),
    },
    {
      key: 'gross-pnl',
      label: 'Gross P&L',
      value: formatSignedNumber(Number(normalizedStats.gross_pnl), 2),
    },
    {
      key: 'fees-paid',
      label: 'Fees Paid',
      value: formatNumber(Number(totalFees), 2),
    },
    {
      key: 'closed-trades',
      label: 'Closed Trades',
      value: Number.isFinite(Number(normalizedStats.completed_trades))
        ? String(Math.max(Number(normalizedStats.completed_trades), 0))
        : '—',
    },
    {
      key: 'open-trades',
      label: 'Open Trades',
      value: String(Math.max(Number(openTradeCount || 0), 0)),
    },
    {
      key: 'win-rate',
      label: 'Win Rate',
      value: formatPercent(Number(normalizedStats.win_rate)),
    },
    {
      key: 'trade-events',
      label: 'Trade Events',
      value: String((Array.isArray(recentTrades) ? recentTrades : []).length),
    },
    {
      key: 'runtime-state',
      label: 'Runtime State',
      value: runtimeState ? humanizeToken(runtimeState) : '—',
    },
    {
      key: 'last-event',
      label: 'Last Runtime Event',
      value: formatMoment(runtime?.last_event_at),
    },
  ]
  return rows.filter((row) => row.value !== '—' || ['open-trades', 'trade-events'].includes(row.key))
}

export function buildBotLensRuntimeViewModel({
  activeRunId,
  bot,
  chartCandles,
  chartHistory,
  chartHistoryCacheCount,
  chartHistoryStatus,
  chartOverlays,
  chartTrades,
  recentTrades = chartTrades,
  error,
  durableEvidence,
  forensicDocuments,
  forensicError,
  forensicHasMore,
  forensicNextCursor,
  forensicStatus,
  logs,
  openTrades,
  runState,
  runtimeStatus,
  selectedLabel,
  selectedSymbolBootstrapStatus,
  selectedSymbolDecisions,
  selectedSymbolKey,
  selectedSymbolMetadata,
  selectedSymbolSignals,
  selectedSymbolState,
  selectedSummary,
  statusMessage,
  streamState,
  symbolOptions,
  warningItems,
}) {
  const botLifecycle = describeBotLifecycle(bot)
  const botStatus = normalizeBotStatus(getBotStatus(bot))
  const resolvedRunId = activeRunId || getBotRunId(bot) || '—'
  const runSummaryText = summarizeRun(runState?.runMeta, runState?.health)
  const notices = buildNotices({ statusMessage, error })
  const warningCount = Math.max(
    Array.isArray(warningItems) ? warningItems.length : 0,
    Number(runState?.health?.warning_count || 0) || 0,
  )
  const selectedSymbol = String(selectedSymbolMetadata?.symbol || selectedSummary?.symbol || '').trim().toUpperCase() || '—'
  const selectedTimeframe = String(selectedSymbolMetadata?.timeframe || selectedSummary?.timeframe || '').trim().toUpperCase() || '—'
  const selectedNetPnlValue = Number(selectedSymbolState?.stats?.net_pnl ?? selectedSummary?.stats?.net_pnl)
  const selectedNetPnlLabel = formatSignedNumber(selectedNetPnlValue)
  const priceContext = buildPriceContext(chartCandles)
  const openTradeCount = Object.keys(runState?.openTradesIndex || {}).length
  const durableDecisionPage = durableEvidence?.decisions || null
  const durableTradePage = durableEvidence?.trades || null
  const durableDiagnostics = durableEvidence?.diagnostics || null
  const hasDurableDecisions = durableDecisionPage?.total !== null
    && durableDecisionPage?.total !== undefined
    && Number.isFinite(Number(durableDecisionPage.total))
  const hasDurableTrades = durableTradePage?.total !== null
    && durableTradePage?.total !== undefined
    && Number.isFinite(Number(durableTradePage.total))
  const hasDurableDiagnostics = durableDiagnostics?.total !== null
    && durableDiagnostics?.total !== undefined
    && Number.isFinite(Number(durableDiagnostics.total))
  const decisionRecords = hasDurableDecisions
    ? (Array.isArray(durableDecisionPage?.items) ? durableDecisionPage.items : [])
    : (Array.isArray(selectedSymbolDecisions) ? selectedSymbolDecisions : [])
  const tradeRecords = hasDurableTrades
    ? (Array.isArray(durableTradePage?.items) ? durableTradePage.items : [])
    : (Array.isArray(recentTrades) ? recentTrades : [])
  const diagnosticRows = durableDiagnostics?.status === 'ready'
    ? groupBotLensDiagnostics(durableDiagnostics.items)
    : buildDiagnosticRows(logs)
  const diagnosticEvidenceCount = durableDiagnostics?.status === 'ready'
    ? durableDiagnostics.items.length
    : diagnosticRows.length
  const diagnosticCount = hasDurableDiagnostics
    ? Math.max(0, Number(durableDiagnostics.total || 0))
    : warningCount + diagnosticEvidenceCount
  const recentTradeRows = buildRecentTradeRows(tradeRecords)
  const topTone = topBarTone(runState?.health?.status || botStatus)
  const strategyName = String(runState?.runMeta?.strategy_name || bot?.strategy_variant_name || bot?.strategy_id || 'Strategy').trim()
  const runRangeLabel = runState?.runMeta?.backtest_start || runState?.runMeta?.backtest_end
    ? `${formatDateOnly(runState?.runMeta?.backtest_start)} – ${formatDateOnly(runState?.runMeta?.backtest_end)}`
    : formatMoment(runState?.runMeta?.started_at)
  const executionSemantics = runState?.runMeta?.execution_semantics
  const executionSemanticsLabel = humanizeToken(
    executionSemantics?.instrument_type
      || executionSemantics?.source_instrument_type
      || executionSemantics?.execution_surface
      || runState?.runMeta?.instrument_type
      || 'unspecified',
  )
  const selectedStats = selectedSymbolState?.stats && typeof selectedSymbolState.stats === 'object'
    ? selectedSymbolState.stats
    : selectedSummary?.stats && typeof selectedSummary.stats === 'object'
      ? selectedSummary.stats
      : {}
  const runtimeSnapshot = selectedSymbolState?.runtime && typeof selectedSymbolState.runtime === 'object'
    ? selectedSymbolState.runtime
    : runState?.health && typeof runState.health === 'object'
      ? runState.health
      : {}
  const selectedOpenTradeCount = Number(selectedSummary?.open_trade_count || 0)
  const snapshotDecisionLedgerEntries = buildBotLensDecisionLedgerEntries({
    signals: hasDurableDecisions ? [] : selectedSymbolSignals,
    decisions: decisionRecords,
    trades: hasDurableDecisions ? [] : chartTrades,
  })
  const forensicDecisionLedgerEntries = hasDurableDecisions
    ? []
    : buildBotLensForensicLedgerEntries(forensicDocuments)
  const decisionLedgerEntries = mergeLedgerEntries(
    snapshotDecisionLedgerEntries,
    forensicDecisionLedgerEntries,
  )
  const decisionRows = buildDecisionRows(decisionRecords)
  const decisionCount = hasDurableDecisions
    ? Math.max(0, Number(durableDecisionPage.total || 0))
    : decisionLedgerEntries.length
  const tradeCount = hasDurableTrades
    ? Math.max(0, Number(durableTradePage.total || 0))
    : recentTradeRows.length
  const decisionSummaryRows = buildDecisionSummaryRows({
    signals: selectedSymbolSignals,
    decisions: selectedSymbolDecisions,
    trades: chartTrades,
    ledgerEntries: decisionLedgerEntries,
  })
  const decisionLatestRows = buildDecisionLatestRows({
    signals: selectedSymbolSignals,
    decisions: selectedSymbolDecisions,
    trades: chartTrades,
    runtime: runtimeSnapshot,
  })
  const walletRows = buildBotLensWalletRows({
    openTradeCount: selectedOpenTradeCount,
    recentTrades,
    runtime: runtimeSnapshot,
    stats: selectedStats,
  })
  const runReadiness = runState?.readiness && typeof runState.readiness === 'object'
    ? runState.readiness
    : {}
  const executionMode = resolveExecutionMode({
    ...(bot || {}),
    ...(runState?.runMeta || {}),
    run: { execution_mode: runState?.runMeta?.execution_mode },
    runtime: runtimeSnapshot,
    lifecycle: bot?.lifecycle,
  })
  const executionModeLabel = formatExecutionModeLabel(executionMode)
  const intrabarExecution = executionModeUsesIntrabar(executionMode)
  const chartPlaybackMode = bot?.mode || null
  const timerRunMode = bot?.run_type || runState?.runMeta?.run_type || runState?.lifecycle?.phase || bot?.mode || null
  const runModeBadge = buildRunModeBadge(bot?.run_type || runState?.runMeta?.run_type)
  const selectedReadiness = selectedSymbolState?.readiness && typeof selectedSymbolState.readiness === 'object'
    ? selectedSymbolState.readiness
    : selectedSymbolMetadata?.readiness && typeof selectedSymbolMetadata.readiness === 'object'
      ? selectedSymbolMetadata.readiness
      : selectedSummary?.readiness && typeof selectedSummary.readiness === 'object'
        ? selectedSummary.readiness
        : {
            catalog_discovered: Boolean(selectedSymbolKey),
            snapshot_ready: false,
            symbol_live: false,
            run_live: Boolean(runReadiness.run_live),
          }
  const transportEligible = Boolean(runState?.transportEligible)
  const selectedSnapshotReady = Boolean(selectedReadiness.snapshot_ready)
  const overlayProjection = selectedSymbolState?.overlay_projection
    || selectedSymbolState?.live_cursors?.overlay_projection
    || null
  const overlayValidity = selectedSymbolState?.overlay_validity
    && typeof selectedSymbolState.overlay_validity === 'object'
    ? selectedSymbolState.overlay_validity
    : { status: 'valid' }
  const boundedOverlayCount = (Array.isArray(chartOverlays) ? chartOverlays : [])
    .filter((overlay) => String(overlay?.detail_level || '').trim().toLowerCase().startsWith('bounded_'))
    .length

  const header = {
    kicker: 'BotLens Runtime',
    title: bot?.name || 'Runtime workspace',
    description: statusMessage || runSummaryText || botLifecycle.detail,
    meta: `bot_id=${bot?.id || '—'} · run_id=${resolvedRunId} · selected=${selectedLabel || '—'}`,
    pills: [
      { key: 'execution-mode', label: 'Execution', value: executionModeLabel },
      { key: 'stream', label: 'Live Stream', value: streamState || 'idle' },
      { key: 'bootstrap', label: 'Bootstrap', value: selectedSymbolBootstrapStatus || 'idle' },
      { key: 'selected', label: 'Selected Symbol', value: selectedLabel || '—' },
      { key: 'warnings', label: 'Warnings', value: String(warningCount) },
    ],
  }

  const hasRunState = Boolean(
    runState?.runMeta
    || runState?.health
    || runState?.lifecycle
    || runState?.symbolIndex,
  )
  let mode = 'ready'
  if (!bot) {
    mode = 'empty'
  } else if (runtimeStatus === 'bootstrapping') {
    mode = 'loading'
  } else if (!hasRunState) {
    if (runtimeStatus === 'error' || error) mode = 'error'
    else if (String(statusMessage || '').toLowerCase().includes('unavailable')) mode = 'unavailable'
    else mode = 'idle'
  }

  const symbolPriceContext = new Map()
  Object.entries(runState?.symbolIndex || {}).forEach(([symbolKey, summary]) => {
    symbolPriceContext.set(symbolKey, {
      currentPrice: Number(summary?.last_price),
      latestBarTime: Number.isFinite(Number(summary?.last_bar_time))
        ? new Date(Number(summary.last_bar_time) * 1000).toISOString()
        : null,
    })
  })

  const symbolSelector = {
    selectedKey: selectedSymbolKey,
    selectedLabel,
    bootstrapStatus: selectedSymbolBootstrapStatus || 'idle',
    items: (Array.isArray(symbolOptions) ? symbolOptions : []).map((summary) => ({
      key: summary.symbol_key,
      label: summary.display_label || `${summary.symbol || summary.symbol_key} · ${summary.timeframe || '—'}`,
      symbol: summary.symbol || '—',
      timeframe: summary.timeframe || '—',
      status: summary.status || 'waiting',
      lastEventAt: summary.last_event_at || null,
      trades: Number(summary?.stats?.total_trades || 0),
      netPnl: Number(summary?.stats?.net_pnl || 0),
      openTrades: Number(summary?.open_trade_count || 0),
      isSelected: summary.symbol_key === selectedSymbolKey,
      isLoading: summary.symbol_key === selectedSymbolKey && selectedSymbolBootstrapStatus === 'loading',
      isReady: Boolean(summary?.readiness?.snapshot_ready),
    })),
  }

  const currentStatePanels = {
    overview: {
      runRows: [
        { key: 'run-status', label: 'Run Status', value: runState?.health?.status || botStatus || '—' },
        { key: 'phase', label: 'Phase', value: runState?.lifecycle?.phase || '—' },
        { key: 'execution-mode', label: 'Execution Mode', value: executionModeLabel },
        { key: 'intrabar-path', label: 'Intrabar Path', value: intrabarExecution ? 'Enabled' : 'Disabled' },
        { key: 'tracked-symbols', label: 'Tracked Symbols', value: String(symbolSelector.items.length) },
        { key: 'open-trades', label: 'Open Trades', value: String(openTradeCount) },
        { key: 'run-live', label: 'Run Live', value: formatBooleanState(runReadiness.run_live) },
        { key: 'started', label: 'Started', value: formatMoment(runState?.runMeta?.started_at) },
        { key: 'last-event', label: 'Last Event', value: formatMoment(runState?.health?.last_event_at) },
      ],
      selectedRows: [
        { key: 'selected-symbol', label: 'Selected Symbol', value: selectedLabel || '—' },
        { key: 'bootstrap-status', label: 'Bootstrap Status', value: selectedSymbolBootstrapStatus || 'idle' },
        { key: 'catalog-discovered', label: 'Catalog Discovered', value: formatBooleanState(selectedReadiness.catalog_discovered) },
        { key: 'snapshot-ready', label: 'Snapshot Ready', value: formatBooleanState(selectedReadiness.snapshot_ready) },
        { key: 'symbol-live', label: 'Symbol Live', value: formatBooleanState(selectedReadiness.symbol_live) },
        { key: 'runtime-status', label: 'Runtime Status', value: selectedSymbolState?.status || selectedSummary?.status || '—' },
        { key: 'last-event', label: 'Last Symbol Event', value: formatMoment(selectedSymbolState?.last_event_at || selectedSummary?.last_event_at) },
        { key: 'base-candles', label: 'Base Candles', value: String(selectedSymbolState?.candles?.length || 0) },
        { key: 'signals', label: 'Signals', value: String(selectedSymbolSignals?.length || 0) },
        { key: 'decisions', label: 'Decisions', value: String(selectedSymbolDecisions?.length || 0) },
        { key: 'net-pnl', label: 'Net P&L', value: formatSignedNumber(selectedNetPnlValue) },
      ],
    },
    warnings: {
      count: warningCount,
      items: (Array.isArray(warningItems) ? warningItems : []).map((warning) => ({
        ...warning,
        title: warningRowTitle(warning),
        seenLabel: formatRelativeTime(warning.last_seen_at || warning.first_seen_at),
      })),
    },
    tradeActivity: {
      openTrades: (Array.isArray(openTrades) ? openTrades : [])
        .filter((trade) => isOpenTrade(trade))
        .map((trade, index) => {
          const tradeId = String(trade?.trade_id || `${trade?.entry_time || ''}|${trade?.symbol || ''}|${index}`)
          const chip = buildTradeChip(trade)
          const context = symbolPriceContext.get(normalizeSeriesKey(trade?.symbol_key || '')) || null
          return chip
            ? {
                id: tradeId,
                chip,
                trade,
                currentPrice: context?.currentPrice,
                latestBarTime: context?.latestBarTime,
                isActiveSymbol: normalizeSeriesKey(trade?.symbol_key || '') === selectedSymbolKey,
              }
            : null
        })
        .filter(Boolean),
      logs: Array.isArray(logs) ? logs : [],
    },
  }

  const retrievalPanels = {
    chart: {
      chartKey: selectedSymbolKey || selectedLabel || selectedSymbol,
      status: selectedSnapshotReady
        ? 'ready'
        : selectedSymbolBootstrapStatus === 'loading'
          ? 'loading'
          : selectedSymbolBootstrapStatus === 'unavailable'
            ? 'unavailable'
          : selectedSymbolKey
            ? 'empty'
            : 'idle',
      selectedLabel,
      selectedSymbol: {
        label: selectedLabel || '—',
        symbol: selectedSymbol,
        timeframe: selectedTimeframe,
        status: selectedSymbolState?.status || selectedSummary?.status || '—',
        bootstrapStatus: selectedSymbolBootstrapStatus || 'idle',
        lastEventAt: formatMoment(selectedSymbolState?.last_event_at || selectedSummary?.last_event_at),
        signals: String(selectedSymbolSignals?.length || 0),
        decisions: String(selectedSymbolDecisions?.length || 0),
        trades: String((Array.isArray(chartTrades) ? chartTrades : []).length),
        netPnl: selectedNetPnlLabel,
        openTrades: String(selectedOpenTradeCount),
      },
      chartContext: {
        symbol: selectedSymbol,
        label: selectedLabel || selectedSymbol,
        timeframe: selectedTimeframe,
        status: humanizeToken(selectedSymbolState?.status || selectedSummary?.status || runState?.health?.status || botStatus || 'idle'),
        openTradeCount: selectedOpenTradeCount,
        netPnl: selectedNetPnlLabel,
        lastPrice: priceContext.lastPrice,
        priceChange: priceContext.change,
        priceChangePct: priceContext.changePct,
        priceDirection: priceContext.direction,
        runMode: runModeBadge,
      },
      liveTrades: currentStatePanels.tradeActivity.openTrades,
      historyStatus: chartHistoryStatus || 'idle',
      historyError: chartHistory?.error || null,
      historyEvidenceSource: chartHistory?.evidenceSource || null,
      tradeEvidence: chartHistory?.tradeEvidence || null,
      overlayEvidence: chartHistory?.overlayEvidence || null,
      overlayValidity,
      hasMoreBefore: chartHistory?.range?.has_more_before !== false,
      hasMoreAfter: chartHistory?.range?.has_more_after !== false,
      historyCount: Number(chartHistory?.candles?.length || 0),
      cacheCount: Number(chartHistoryCacheCount || 0),
      focusTime: chartHistory?.focusTime || null,
      focusToken: chartHistory?.focusToken || null,
      focusTradeId: chartHistory?.focusTradeId || null,
      focusedTrade: (Array.isArray(chartTrades) ? chartTrades : []).find((trade) => (
        String(trade?.trade_id || '') === String(chartHistory?.focusTradeId || '')
      )) || null,
      showActiveTradeLevels: Boolean(runReadiness.run_live),
      followLatestCandles: Boolean(runReadiness.run_live),
      dataUpdateMode: chartHistory?.lastUpdateMode || null,
      dataUpdateToken: chartHistory?.lastUpdateToken || null,
      candles: Array.isArray(chartCandles) ? chartCandles : [],
      trades: Array.isArray(chartTrades) ? chartTrades : [],
      overlays: Array.isArray(chartOverlays) ? chartOverlays : [],
      overlayProjection: {
        mode: overlayProjection?.mode || null,
        windowBars: Number(overlayProjection?.window_bars || 0) || null,
        emitEveryBars: Number(overlayProjection?.emit_every_bars || 0) || null,
        barIndex: Number.isFinite(Number(overlayProjection?.bar_index)) ? Number(overlayProjection.bar_index) : null,
        overlays: Array.isArray(chartOverlays) ? chartOverlays.length : 0,
        boundedOverlays: boundedOverlayCount,
      },
      timeframe: selectedSymbolMetadata?.timeframe || selectedSymbolState?.timeframe || null,
      mode: chartPlaybackMode,
      timerMode: timerRunMode,
      playbackSpeed: Number(bot?.playback_speed || 0),
      emptyMessage: selectedSymbolBootstrapStatus === 'loading'
        ? `Loading symbol snapshot for ${selectedLabel}...`
        : selectedSymbolBootstrapStatus === 'unavailable'
          ? (statusMessage || `Selected-symbol snapshot is unavailable for ${selectedLabel}.`)
        : selectedSymbolKey
          ? 'Selected-symbol snapshot is required before chart deltas render.'
          : 'Select a symbol to load its runtime chart.',
    },
  }

  return {
    botId: bot?.id || null,
    mode,
    header,
    notices,
    symbolSelector,
    currentStatePanels,
    retrievalPanels,
    topBar: {
      kicker: 'BotLens',
      title: bot?.name || 'Runtime workspace',
      subtitle: [strategyName, selectedLabel, runRangeLabel]
        .filter(Boolean)
        .join(' · '),
      runMode: runModeBadge,
      status: {
        label: humanizeToken(runState?.health?.status || botStatus || 'idle'),
        tone: topTone,
      },
      identifiers: [
        { key: 'bot_id', label: 'bot_id', value: bot?.id || null, displayValue: shortId(bot?.id, 12) },
        { key: 'run_id', label: 'run_id', value: resolvedRunId !== '—' ? resolvedRunId : null, displayValue: shortId(resolvedRunId, 12) },
      ],
      stats: [
        { key: 'range', label: 'Range', value: runRangeLabel },
        { key: 'semantics', label: 'Execution', value: executionSemanticsLabel },
        { key: 'selected-symbol', label: 'Market', value: selectedLabel || '—' },
        { key: 'open-trades', label: 'Open Trades', value: String(openTradeCount) },
        { key: 'warnings', label: 'Warnings', value: String(warningCount) },
        { key: 'last-event', label: 'Last Event', value: formatRelativeTime(runState?.health?.last_event_at) },
      ],
    },
    tabs: [
      { key: 'decisions', label: 'Decisions', badge: String(decisionCount) },
      { key: 'trades', label: 'Trades', badge: String(tradeCount) },
      { key: 'diagnostics', label: 'Diagnostics', badge: String(diagnosticCount) },
    ],
    inspection: {
      state: {
        runRows: currentStatePanels.overview.runRows,
        selectedRows: currentStatePanels.overview.selectedRows,
      },
      trades: {
        openTrades: currentStatePanels.tradeActivity.openTrades,
        recentTrades: recentTradeRows,
        status: hasDurableTrades ? durableTradePage.status : 'ready',
        error: hasDurableTrades ? durableTradePage.error : null,
        total: tradeCount,
        offset: hasDurableTrades ? durableTradePage.offset : 0,
        limit: hasDurableTrades ? durableTradePage.limit : Math.max(recentTradeRows.length, 1),
        pageIndex: hasDurableTrades ? Math.floor(durableTradePage.offset / durableTradePage.limit) : 0,
        pageCount: hasDurableTrades ? Math.max(1, Math.ceil(tradeCount / durableTradePage.limit)) : 1,
        durable: hasDurableTrades,
      },
      decisions: {
        entries: decisionLedgerEntries,
        rows: decisionRows,
        status: hasDurableDecisions
          ? durableDecisionPage.status
          : forensicStatus || (selectedSymbolBootstrapStatus === 'loading' ? 'loading' : 'ready'),
        error: hasDurableDecisions ? durableDecisionPage.error : forensicError || null,
        hasMore: hasDurableDecisions
          ? durableDecisionPage.offset + durableDecisionPage.limit < decisionCount
          : forensicHasMore !== false,
        autoLoad: false,
        nextCursor: forensicNextCursor || { afterSeq: 0, afterRowId: 0 },
        total: decisionCount,
        offset: hasDurableDecisions ? durableDecisionPage.offset : 0,
        limit: hasDurableDecisions ? durableDecisionPage.limit : Math.max(decisionRows.length, 1),
        pageIndex: hasDurableDecisions ? Math.floor(durableDecisionPage.offset / durableDecisionPage.limit) : 0,
        pageCount: hasDurableDecisions ? Math.max(1, Math.ceil(decisionCount / durableDecisionPage.limit)) : 1,
        durable: hasDurableDecisions,
        summaryRows: decisionSummaryRows,
        walletRows,
        latestRows: decisionLatestRows,
      },
      logs: {
        entries: currentStatePanels.tradeActivity.logs,
      },
      diagnostics: {
        warnings: currentStatePanels.warnings,
        entries: diagnosticRows,
        evidenceCount: diagnosticEvidenceCount,
        status: durableDiagnostics?.status || 'ready',
        error: durableDiagnostics?.error || null,
        summary: durableDiagnostics?.summary || {},
        total: hasDurableDiagnostics ? Number(durableDiagnostics.total) : diagnosticEvidenceCount,
        offset: Math.max(0, Number(durableDiagnostics?.offset || 0) || 0),
        limit: Math.max(1, Number(durableDiagnostics?.limit || diagnosticEvidenceCount || 1) || 1),
        pageIndex: hasDurableDiagnostics
          ? Math.floor(Number(durableDiagnostics.offset || 0) / Math.max(1, Number(durableDiagnostics.limit || 1)))
          : 0,
        pageCount: hasDurableDiagnostics
          ? Math.max(1, Math.ceil(Number(durableDiagnostics.total) / Math.max(1, Number(durableDiagnostics.limit || 1))))
          : 1,
        checks: [
          { key: 'runtime', label: 'Runtime', value: humanizeToken(runtimeStatus || 'idle') },
          { key: 'execution-mode', label: 'Execution Mode', value: executionModeLabel },
          { key: 'intrabar-path', label: 'Intrabar Path', value: intrabarExecution ? 'Enabled' : 'Disabled' },
          { key: 'stream', label: 'Live Stream', value: humanizeToken(streamState || 'idle') },
          { key: 'catalog', label: 'Catalog Discovered', value: formatBooleanState(selectedReadiness.catalog_discovered) },
          { key: 'snapshot', label: 'Snapshot Ready', value: formatBooleanState(selectedReadiness.snapshot_ready) },
          { key: 'symbol-live', label: 'Symbol Live', value: formatBooleanState(selectedReadiness.symbol_live) },
          { key: 'run-live', label: 'Run Live', value: formatBooleanState(runReadiness.run_live) },
          { key: 'transport', label: 'Transport Eligible', value: formatBooleanState(transportEligible) },
          { key: 'decisions', label: 'Ledger Events', value: String(decisionCount) },
          { key: 'history', label: 'Chart History', value: humanizeToken(retrievalPanels.chart.historyStatus) },
          { key: 'overlays', label: 'Overlay Evidence', value: humanizeToken(overlayValidity.status || 'valid') },
          { key: 'cache', label: 'Chart Cache', value: String(retrievalPanels.chart.cacheCount) },
        ],
        notices,
      },
    },
    botLifecycle: {
      label: botLifecycle.label,
      detail: botLifecycle.detail,
    },
  }
}

export {
  formatMoment,
  formatNumber,
  formatPercent,
}
