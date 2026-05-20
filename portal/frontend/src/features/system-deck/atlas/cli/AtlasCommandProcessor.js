import { ATLAS_COMMAND_TONES, ATLAS_FILTERS } from '../types/atlasTypes.js'

const COMMANDS = [
  'help',
  'atlas list',
  'atlas latest',
  'atlas inspect <run_id>',
  'atlas filter profitable',
  'atlas filter losing',
  'atlas focus <run_id>',
  'atlas reset-view',
  'atlas clear',
  'atlas districts',
]

function line(text, tone = ATLAS_COMMAND_TONES.system) {
  return { text, tone }
}

function formatPnl(value) {
  const sign = value > 0 ? '+' : ''
  return `${sign}${value.toFixed(2)}`
}

function artifactSummary(artifact) {
  return `${artifact.id} | ${artifact.run.strategy} | pnl ${formatPnl(artifact.run.pnl)} | ${artifact.run.tradeCount} trades | ${artifact.district.label}`
}

function inspectLines(artifact) {
  return [
    line(`selected ${artifact.id}`, artifact.run.pnl >= 0 ? ATLAS_COMMAND_TONES.success : ATLAS_COMMAND_TONES.warning),
    line(`strategy=${artifact.run.strategy} experiment=${artifact.run.experiment}`),
    line(`pnl=${formatPnl(artifact.run.pnl)} drawdown=${artifact.run.drawdown.toFixed(2)} trades=${artifact.run.tradeCount} win_rate=${Math.round(artifact.run.winRate * 100)}%`),
    line(`symbols=${artifact.run.symbols.join(', ')} family=${artifact.family} district=${artifact.district.label}`),
  ]
}

function unknownRunLines(runId) {
  return [
    line(`run artifact not found: ${runId}`, ATLAS_COMMAND_TONES.danger),
    line('try atlas list or atlas latest', ATLAS_COMMAND_TONES.muted),
  ]
}

export function processAtlasCommand(input, registry) {
  const raw = String(input || '').trim()
  if (!raw) {
    return { lines: [line('no command received', ATLAS_COMMAND_TONES.muted)], actions: {} }
  }

  const normalized = raw.toLowerCase().replace(/\s+/g, ' ')
  const parts = normalized.split(' ')

  if (normalized === 'help') {
    return {
      lines: [
        line('atlas command surface'),
        ...COMMANDS.map((command) => line(`  ${command}`, ATLAS_COMMAND_TONES.muted)),
      ],
      actions: {},
    }
  }

  if (parts[0] !== 'atlas') {
    return {
      lines: [
        line(`unknown command: ${raw}`, ATLAS_COMMAND_TONES.danger),
        line('type help', ATLAS_COMMAND_TONES.muted),
      ],
      actions: {},
    }
  }

  if (normalized === 'atlas clear') {
    return {
      lines: [line('console buffer cleared', ATLAS_COMMAND_TONES.muted)],
      actions: { clearConsole: true },
    }
  }

  if (normalized === 'atlas list') {
    return {
      lines: registry.list().map((artifact) => line(artifactSummary(artifact), artifact.run.pnl >= 0 ? ATLAS_COMMAND_TONES.success : ATLAS_COMMAND_TONES.warning)),
      actions: {},
    }
  }

  if (normalized === 'atlas latest') {
    const artifact = registry.latest()
    if (!artifact) return { lines: [line('no completed runs indexed', ATLAS_COMMAND_TONES.warning)], actions: {} }
    return {
      lines: [line(`latest ${artifactSummary(artifact)}`, ATLAS_COMMAND_TONES.success)],
      actions: { selectId: artifact.id, focusId: artifact.id },
    }
  }

  if (normalized === 'atlas districts') {
    const districtLines = registry.districtSummaries().map((district) => (
      line(`${district.label} | artifacts ${district.count} | pnl ${formatPnl(district.pnl)} | profitable ${district.profitable} | losing ${district.losing}`)
    ))
    return {
      lines: districtLines.length > 0 ? districtLines : [line('no districts indexed', ATLAS_COMMAND_TONES.warning)],
      actions: {},
    }
  }

  if (normalized === 'atlas filter profitable') {
    return {
      lines: [line('filter=profitable', ATLAS_COMMAND_TONES.success)],
      actions: { filter: ATLAS_FILTERS.profitable },
    }
  }

  if (normalized === 'atlas filter losing') {
    return {
      lines: [line('filter=losing', ATLAS_COMMAND_TONES.warning)],
      actions: { filter: ATLAS_FILTERS.losing },
    }
  }

  if (normalized === 'atlas reset-view') {
    return {
      lines: [line('view reset; filter=all', ATLAS_COMMAND_TONES.system)],
      actions: { resetView: true, filter: ATLAS_FILTERS.all, selectId: null },
    }
  }

  if (parts[1] === 'inspect' || parts[1] === 'focus') {
    const runId = raw.split(/\s+/).slice(2).join(' ')
    const artifact = registry.findById(runId)
    if (!artifact) return { lines: unknownRunLines(runId), actions: {} }
    return {
      lines: parts[1] === 'inspect' ? inspectLines(artifact) : [line(`focus locked ${artifact.id}`, ATLAS_COMMAND_TONES.success)],
      actions: { selectId: artifact.id, focusId: artifact.id },
    }
  }

  return {
    lines: [
      line(`unsupported atlas command: ${raw}`, ATLAS_COMMAND_TONES.danger),
      line('type help', ATLAS_COMMAND_TONES.muted),
    ],
    actions: {},
  }
}
