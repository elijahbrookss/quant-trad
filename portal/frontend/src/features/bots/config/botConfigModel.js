const ACTIVE_STATUS_KEYS = new Set(['starting', 'running', 'degraded', 'paused'])

const KNOWN_CONFIG_KEYS = new Set([
  'id',
  'name',
  'strategy_id',
  'strategy_variant_id',
  'strategy_variant_name',
  'atm_template_id',
  'risk_config',
  'resolved_params',
  'datasource',
  'exchange',
  'timeframe',
  'mode',
  'execution_mode',
  'execution_behavior',
  'run_type',
  'playback_speed',
  'backtest_start',
  'backtest_end',
  'focus_symbol',
  'wallet_config',
  'market_data_stream_policy',
  'snapshot_interval_ms',
  'bot_env',
  'execution_semantics',
])

const NON_CONFIG_KEYS = new Set([
  'active_container',
  'active_run_id',
  'container',
  'container_status',
  'created_at',
  'lifecycle',
  'runtime',
  'status',
  'updated_at',
])

const JSON_EDIT_FIELDS = ['risk_config', 'wallet_config', 'market_data_stream_policy']
const EDITABLE_FIELD_KEYS = new Set([
  'name',
  'run_type',
  'mode',
  'execution_mode',
  'execution_behavior',
  'execution_semantics',
  'atm_template_id',
  'focus_symbol',
  'backtest_start',
  'backtest_end',
  'snapshot_interval_ms',
  'playback_speed',
  ...JSON_EDIT_FIELDS,
])

export function isBotConfigActive(display) {
  return ACTIVE_STATUS_KEYS.has(String(display?.statusKey || '').trim().toLowerCase())
}

function isPlainObject(value) {
  return Boolean(value) && typeof value === 'object' && !Array.isArray(value)
}

function isEmptyObject(value) {
  return isPlainObject(value) && Object.keys(value).length === 0
}

function humanizeKey(value) {
  return String(value || '')
    .replace(/[_-]+/g, ' ')
    .replace(/\s+/g, ' ')
    .trim()
    .replace(/\b\w/g, (char) => char.toUpperCase())
}

function humanizeValue(value) {
  const normalized = String(value || '').trim()
  if (!normalized) return '—'
  return humanizeKey(normalized)
}

function formatScalar(value) {
  if (value == null || value === '') return '—'
  if (typeof value === 'boolean') return value ? 'Enabled' : 'Disabled'
  if (typeof value === 'number') return Number.isFinite(value) ? String(value) : '—'
  return String(value)
}

function formatDateTime(value) {
  if (!value) return '—'
  const epochMs = Date.parse(String(value))
  if (!Number.isFinite(epochMs)) return String(value)
  return new Intl.DateTimeFormat(undefined, {
    dateStyle: 'medium',
    timeStyle: 'short',
  }).format(new Date(epochMs))
}

function formatJson(value) {
  if (value == null || value === '') return '{}'
  try {
    return JSON.stringify(value, null, 2)
  } catch {
    return '{}'
  }
}

function compactJsonSummary(value) {
  if (Array.isArray(value)) return `${value.length} item${value.length === 1 ? '' : 's'}`
  if (isPlainObject(value)) {
    const count = Object.keys(value).length
    return `${count} key${count === 1 ? '' : 's'}`
  }
  return formatScalar(value)
}

function isSensitiveKey(key) {
  return /(secret|token|password|credential|private|api[_-]?key|passphrase)/i.test(String(key || ''))
}

function row({ key, label, value, detail, mono = false, jsonValue = null, masked = false }) {
  return {
    key,
    label,
    value: value == null || value === '' ? '—' : String(value),
    detail: detail || null,
    mono,
    jsonValue,
    masked,
  }
}

function objectRows(value, { prefix, maskValues = false } = {}) {
  if (!isPlainObject(value) || !Object.keys(value).length) return []
  return Object.entries(value)
    .sort(([left], [right]) => left.localeCompare(right))
    .map(([key, current]) => {
      const masked = maskValues && isSensitiveKey(key)
      const display = masked
        ? '••••••••'
        : isPlainObject(current) || Array.isArray(current)
          ? compactJsonSummary(current)
          : formatScalar(current)
      return row({
        key: `${prefix}-${key}`,
        label: humanizeKey(key),
        value: display,
        jsonValue: masked || (!isPlainObject(current) && !Array.isArray(current)) ? null : formatJson(current),
        masked,
      })
    })
}

function balanceRows(walletConfig) {
  const balances = isPlainObject(walletConfig?.balances) ? walletConfig.balances : {}
  return Object.entries(balances)
    .sort(([left], [right]) => left.localeCompare(right))
    .map(([currency, amount]) => row({
      key: `balance-${currency}`,
      label: String(currency || '').toUpperCase() || 'Currency',
      value: formatScalar(amount),
      mono: true,
    }))
}

function additionalRows(bot) {
  if (!isPlainObject(bot)) return []
  return Object.entries(bot)
    .filter(([key, value]) => !KNOWN_CONFIG_KEYS.has(key) && !NON_CONFIG_KEYS.has(key) && value !== undefined)
    .sort(([left], [right]) => left.localeCompare(right))
    .map(([key, value]) => {
      const isStructured = isPlainObject(value) || Array.isArray(value)
      return row({
        key: `additional-${key}`,
        label: humanizeKey(key),
        value: isStructured ? compactJsonSummary(value) : formatScalar(value),
        jsonValue: isStructured ? formatJson(value) : null,
      })
    })
}

function strategyVariantLabel(bot, strategy) {
  const variantName = String(bot?.strategy_variant_name || '').trim()
  if (variantName) return variantName

  const variantId = String(bot?.strategy_variant_id || '').trim()
  if (!variantId) return 'Strategy defaults'

  const variants = Array.isArray(strategy?.variants) ? strategy.variants : []
  const variant = variants.find((candidate) => String(candidate?.id || '') === variantId)
  return String(variant?.name || '').trim() || variantId
}

function buildSections(bot, strategy) {
  const walletConfig = isPlainObject(bot?.wallet_config) ? bot.wallet_config : {}
  const walletMeta = Object.fromEntries(
    Object.entries(walletConfig).filter(([key]) => key !== 'balances'),
  )
  const streamPolicy = isPlainObject(bot?.market_data_stream_policy) ? bot.market_data_stream_policy : {}
  const botEnv = isPlainObject(bot?.bot_env) ? bot.bot_env : {}
  const riskConfig = isPlainObject(bot?.risk_config) ? bot.risk_config : {}
  const resolvedParams = isPlainObject(bot?.resolved_params) ? bot.resolved_params : {}

  const sections = [
    {
      key: 'identity',
      title: 'Identity',
      rows: [
        row({ key: 'name', label: 'Name', value: bot?.name }),
        row({ key: 'bot-id', label: 'Bot ID', value: bot?.id, mono: true }),
        row({ key: 'strategy', label: 'Strategy', value: strategy?.name || bot?.strategy_id }),
        row({ key: 'variant', label: 'Variant', value: strategyVariantLabel(bot, strategy) }),
        row({ key: 'atm-template', label: 'ATM Template', value: bot?.atm_template_id, mono: true }),
      ],
    },
    {
      key: 'execution',
      title: 'Execution',
      rows: [
        row({ key: 'run-type', label: 'Run Type', value: humanizeValue(bot?.run_type) }),
        row({ key: 'playback', label: 'Playback', value: humanizeValue(bot?.mode || 'instant') }),
        row({ key: 'execution-mode', label: 'Execution Mode', value: String(bot?.execution_mode || 'fast').toUpperCase() }),
        row({ key: 'execution-behavior', label: 'Execution Behavior', value: humanizeValue(bot?.execution_behavior || 'simulated') }),
        row({ key: 'execution-semantics', label: 'Execution Semantics', value: humanizeValue(bot?.execution_semantics) }),
        row({ key: 'provider', label: 'Provider', value: bot?.datasource || strategy?.datasource }),
        row({ key: 'exchange', label: 'Exchange', value: bot?.exchange || strategy?.exchange }),
        row({ key: 'timeframe', label: 'Timeframe', value: bot?.timeframe || strategy?.timeframe }),
      ],
    },
    {
      key: 'runtime',
      title: 'Runtime',
      rows: [
        row({ key: 'backtest-start', label: 'Backtest Start', value: formatDateTime(bot?.backtest_start), detail: bot?.backtest_start }),
        row({ key: 'backtest-end', label: 'Backtest End', value: formatDateTime(bot?.backtest_end), detail: bot?.backtest_end }),
        row({ key: 'snapshot', label: 'Snapshot Interval', value: bot?.snapshot_interval_ms ? `${bot.snapshot_interval_ms} ms` : '—', mono: true }),
        row({ key: 'playback-speed', label: 'Playback Speed', value: bot?.playback_speed, mono: true }),
        row({ key: 'focus-symbol', label: 'Focus Symbol', value: bot?.focus_symbol }),
      ],
    },
    {
      key: 'funding',
      title: 'Funding',
      rows: [
        ...balanceRows(walletConfig),
        ...objectRows(walletMeta, { prefix: 'wallet' }),
      ],
      emptyLabel: 'No funding config',
    },
    {
      key: 'stream-policy',
      title: 'Market Data Stream',
      rows: objectRows(streamPolicy, { prefix: 'stream-policy' }),
      emptyLabel: 'No stream policy overrides',
    },
    {
      key: 'env',
      title: 'Environment',
      rows: objectRows(botEnv, { prefix: 'env', maskValues: true }),
      emptyLabel: 'No env overrides',
    },
    {
      key: 'strategy-runtime',
      title: 'Strategy Runtime',
      rows: [
        row({
          key: 'risk-config',
          label: 'Risk Config',
          value: isEmptyObject(riskConfig) ? '—' : compactJsonSummary(riskConfig),
          jsonValue: isEmptyObject(riskConfig) ? null : formatJson(riskConfig),
        }),
        row({
          key: 'resolved-params',
          label: 'Resolved Params',
          value: isEmptyObject(resolvedParams) ? '—' : compactJsonSummary(resolvedParams),
          jsonValue: isEmptyObject(resolvedParams) ? null : formatJson(resolvedParams),
        }),
      ],
    },
  ]

  const extraRows = additionalRows(bot)
  if (extraRows.length) {
    sections.push({
      key: 'additional',
      title: 'Additional',
      rows: extraRows,
    })
  }
  return sections
}

export function buildBotConfigModel(bot, { strategy = null, canUpdate = false, active = false } = {}) {
  return {
    title: bot?.name || 'Bot Config',
    subtitle: [strategy?.name || bot?.strategy_id, strategyVariantLabel(bot, strategy), humanizeValue(bot?.run_type)]
      .filter((value) => value && value !== '—')
      .join(' · '),
    canEdit: Boolean(canUpdate && !active),
    active,
    modeLabel: active ? 'Read only' : canUpdate ? 'Editable' : 'Read only',
    sections: buildSections(bot, strategy),
  }
}

export function buildBotConfigEditForm(bot) {
  return {
    name: String(bot?.name || ''),
    run_type: String(bot?.run_type || 'backtest').toLowerCase(),
    mode: String(bot?.mode || 'instant').toLowerCase(),
    execution_mode: String(bot?.execution_mode || 'fast').toLowerCase(),
    execution_behavior: String(bot?.execution_behavior || 'simulated').toLowerCase(),
    execution_semantics: String(bot?.execution_semantics || ''),
    atm_template_id: String(bot?.atm_template_id || ''),
    focus_symbol: String(bot?.focus_symbol || ''),
    backtest_start: String(bot?.backtest_start || ''),
    backtest_end: String(bot?.backtest_end || ''),
    snapshot_interval_ms: String(bot?.snapshot_interval_ms || 1000),
    playback_speed: String(bot?.playback_speed ?? 0),
    risk_config: formatJson(isPlainObject(bot?.risk_config) ? bot.risk_config : {}),
    wallet_config: formatJson(isPlainObject(bot?.wallet_config) ? bot.wallet_config : {}),
    market_data_stream_policy: formatJson(isPlainObject(bot?.market_data_stream_policy) ? bot.market_data_stream_policy : {}),
  }
}

function parseJsonObjectField(value, fieldName) {
  const text = String(value || '').trim()
  if (!text) return {}
  let parsed
  try {
    parsed = JSON.parse(text)
  } catch {
    throw new Error(`${humanizeKey(fieldName)} must be valid JSON.`)
  }
  if (!isPlainObject(parsed)) {
    throw new Error(`${humanizeKey(fieldName)} must be a JSON object.`)
  }
  return parsed
}

export function buildBotConfigUpdatePayload(form) {
  const snapshotInterval = Number(form?.snapshot_interval_ms || 0)
  if (!Number.isFinite(snapshotInterval) || snapshotInterval <= 0) {
    throw new Error('Snapshot interval must be greater than zero.')
  }
  const playbackSpeed = Number(form?.playback_speed || 0)
  if (!Number.isFinite(playbackSpeed) || playbackSpeed < 0) {
    throw new Error('Playback speed must be zero or greater.')
  }

  const payload = {
    name: String(form?.name || '').trim(),
    run_type: String(form?.run_type || 'backtest').trim().toLowerCase(),
    mode: String(form?.mode || 'instant').trim().toLowerCase(),
    execution_mode: String(form?.execution_mode || 'fast').trim().toLowerCase(),
    execution_behavior: String(form?.execution_behavior || 'simulated').trim().toLowerCase(),
    snapshot_interval_ms: Math.round(snapshotInterval),
    playback_speed: playbackSpeed,
    backtest_start: String(form?.backtest_start || '').trim() || null,
    backtest_end: String(form?.backtest_end || '').trim() || null,
    risk_config: parseJsonObjectField(form?.risk_config, 'risk_config'),
    wallet_config: parseJsonObjectField(form?.wallet_config, 'wallet_config'),
    market_data_stream_policy: parseJsonObjectField(form?.market_data_stream_policy, 'market_data_stream_policy'),
  }

  const executionSemantics = String(form?.execution_semantics || '').trim()
  const atmTemplateId = String(form?.atm_template_id || '').trim()
  const focusSymbol = String(form?.focus_symbol || '').trim()
  payload.execution_semantics = executionSemantics || null
  payload.atm_template_id = atmTemplateId || null
  payload.focus_symbol = focusSymbol || null

  if (!payload.name) {
    throw new Error('Bot name is required.')
  }
  return payload
}

export { EDITABLE_FIELD_KEYS, JSON_EDIT_FIELDS }
