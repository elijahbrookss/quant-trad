import test from 'node:test'
import assert from 'node:assert/strict'
import { existsSync, readdirSync, readFileSync, statSync } from 'node:fs'
import path from 'node:path'
import { fileURLToPath } from 'node:url'

const here = path.dirname(fileURLToPath(import.meta.url))
const srcRoot = path.join(here, '..', 'src')

const SCAN_DIRS = [
  'v2',
  path.join('features', 'overview'),
  path.join('features', 'operations'),
  path.join('features', 'collectors'),
  path.join('features', 'bots', 'botlens'),
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

function filesForDeclaredRoots(root, declaredDirs) {
  const files = []
  for (const dir of declaredDirs) {
    const absDir = path.join(root, dir)
    assert.ok(existsSync(absDir), `declared V2 scan root does not exist: ${dir}`)
    assert.ok(statSync(absDir).isDirectory(), `declared V2 scan root is not a directory: ${dir}`)
    walk(absDir, files)
  }
  return files
}

test('v2 scan-root discovery fails loud when a declaration is absent', () => {
  assert.throws(
    () => filesForDeclaredRoots(srcRoot, ['missing-v2-root']),
    /declared V2 scan root does not exist: missing-v2-root/,
  )
})

test('v2 UI surface never imports bot-mutation adapter functions', () => {
  const offenders = []
  for (const file of filesForDeclaredRoots(srcRoot, SCAN_DIRS)) {
    const content = readFileSync(file, 'utf8')
    for (const name of FORBIDDEN_IMPORTS) {
      const importPattern = new RegExp('import\\s*{[^}]*\\b' + name + '\\b[^}]*}\\s*from')
      if (importPattern.test(content)) offenders.push(path.relative(srcRoot, file) + ' imports ' + name)
    }
  }
  assert.deepEqual(offenders, [])
})

test('v2 primary navigation is bounded to Overview and Operations', () => {
  const app = source(path.join('v2', 'AppV2.jsx'))
  const roomBlock = app.slice(app.indexOf('const ROOMS'), app.indexOf('function initialSidebarCollapsed'))
  assert.match(roomBlock, /Overview/)
  assert.match(roomBlock, /Operations/)
  assert.match(app, /qt2-sidebar-toggle/)
  assert.match(app, /SIDEBAR_STORAGE_KEY/)
  assert.doesNotMatch(roomBlock, /Fleet|Studio|Research|Reports/)
})

test('v2 registers exact run, canonical collector, and research evidence routes', () => {
  const app = source(path.join('v2', 'AppV2.jsx'))
  assert.ok(app.includes('path="/operations/runs/:runId"'))
  assert.ok(app.includes('path="/operations/market/:collectorKind/:collectorId"'))
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

test('v2 market mutations use only the audited canonical collector action path', () => {
  const research = source(path.join('adapters', 'research.adapter.js'))
  assert.doesNotMatch(research, /method:\s*['"](?:POST|PUT|PATCH|DELETE)['"]/)
  const market = source(path.join('adapters', 'marketData.adapter.js'))
  assert.match(market, /\/api\/market-data\/operations\/collectors\//)
  assert.match(market, /'\/actions\/' \+ encodeURIComponent\(action\)/)
  assert.equal((market.match(/method: 'POST'/g) || []).length, 1)
  assert.doesNotMatch(market, /\/api\/market-data\/collectors\/(?:snapshot|stream)|\/enabled/)
})
