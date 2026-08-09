import test from 'node:test'
import assert from 'node:assert/strict'
import { readdirSync, readFileSync, statSync } from 'node:fs'
import path from 'node:path'
import { fileURLToPath } from 'node:url'

const here = path.dirname(fileURLToPath(import.meta.url))
const srcRoot = path.join(here, '..', 'src')

const SCAN_DIRS = [
  'v2',
  path.join('features', 'overview'),
  path.join('features', 'operations'),
  path.join('features', 'collectors'),
  path.join('features', 'market-structure'),
]
const FORBIDDEN_IMPORTS = ['createBot', 'updateBot', 'deleteBot', 'startBot', 'stopBot']

function walk(dir, files = []) {
  for (const entry of readdirSync(dir)) {
    const full = path.join(dir, entry)
    const stat = statSync(full)
    if (stat.isDirectory()) walk(full, files)
    else if (/\.jsx?$/.test(entry)) files.push(full)
  }
  return files
}

function source(relative) {
  return readFileSync(path.join(srcRoot, relative), 'utf8')
}

test('v2 UI surface never imports bot-mutation adapter functions', () => {
  const offenders = []
  for (const dir of SCAN_DIRS) {
    const absDir = path.join(srcRoot, dir)
    let files
    try {
      files = walk(absDir)
    } catch {
      continue
    }
    for (const file of files) {
      const content = readFileSync(file, 'utf8')
      for (const name of FORBIDDEN_IMPORTS) {
        const importPattern = new RegExp('import\\s*{[^}]*\\b' + name + '\\b[^}]*}\\s*from')
        if (importPattern.test(content)) offenders.push(path.relative(srcRoot, file) + ' imports ' + name)
      }
    }
  }
  assert.deepEqual(offenders, [])
})

test('v2 primary navigation is bounded to Overview and Operations', () => {
  const app = source(path.join('v2', 'AppV2.jsx'))
  const roomBlock = app.slice(app.indexOf('const ROOMS'), app.indexOf('function LegacyCollectorRedirect'))
  assert.match(roomBlock, /Overview/)
  assert.match(roomBlock, /Operations/)
  assert.match(app, /qt2-sidebar-toggle/)
  assert.match(app, /SIDEBAR_STORAGE_KEY/)
  assert.doesNotMatch(roomBlock, /Fleet|Studio|Research|Reports/)
})

test('v2 registers exact run and evidence routes and redirects legacy mutation-oriented surfaces', () => {
  const app = source(path.join('v2', 'AppV2.jsx'))
  assert.ok(app.includes('path="/operations/runs/:runId"'))
  assert.ok(app.includes('path="/operations/market/:definitionId"'))
  assert.ok(app.includes('path="/operations/collectors/:definitionId"'))
  assert.ok(app.includes('path="/operations/research/:itemId"'))
  assert.ok(app.includes('path="/fleet"'))
  assert.ok(app.includes('path="/studio/*"'))
  assert.ok(app.includes('Navigate to="/operations"'))
})

test('run-scoped BotLens always performs authoritative exact-run and research-evidence reads', () => {
  const room = source(path.join('v2', 'rooms', 'BotLensRoom.jsx'))
  const controller = source(path.join('features', 'bots', 'botlens', 'hooks', 'useBotLensController.js'))
  assert.ok(room.includes('fetchRun(runId, { signal: controller.signal })'))
  assert.ok(room.includes('fetchRunResearchEvidence(runId, { signal: controller.signal })'))
  assert.match(room, /const loadTimer = window\.setTimeout/)
  assert.match(room, /window\.clearTimeout\(loadTimer\)/)
  assert.match(room, /controller\.abort\(\)/)
  assert.match(room, /onTerminal=\{handleRuntimeTerminal\}/)
  assert.match(room, /terminalRefreshRequestedRef/)
  assert.match(controller, /const loadTimer = window\.setTimeout\(load, 0\)/)
  assert.match(controller, /fetchBotLensExactRunBootstrap\(runId(?:,|\))/)
  assert.equal(room.includes('fetchBot(botId)'), false)
})

test('Overview never polls the mutation-bearing watchdog endpoint or generates summaries', () => {
  const overview = source(path.join('v2', 'rooms', 'OverviewRoom.jsx'))
  assert.doesNotMatch(overview, /watchdog/)
  assert.doesNotMatch(overview, /openai|chat|completion|generateSummary/i)
})

test('market and research adapters used by v2 expose GET-only read requests', () => {
  for (const adapter of ['marketData.adapter.js', 'research.adapter.js']) {
    const content = source(path.join('adapters', adapter))
    assert.doesNotMatch(content, /method:\s*['"](?:POST|PUT|PATCH|DELETE)['"]/)
  }
  const market = source(path.join('adapters', 'marketData.adapter.js'))
  assert.doesNotMatch(market, /collectors\/\$\{[^}]+\}\/enabled/)
})
