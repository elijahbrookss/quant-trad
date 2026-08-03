import test from 'node:test'
import assert from 'node:assert/strict'
import { readFileSync } from 'node:fs'
import path from 'node:path'
import { fileURLToPath } from 'node:url'

const here = path.dirname(fileURLToPath(import.meta.url))
const srcRoot = path.join(here, '..', 'src')
const source = (relative) => readFileSync(path.join(srcRoot, relative), 'utf8')

test('Overview is a compact dashboard rather than a numbered operational report', () => {
  const overview = source(path.join('v2', 'rooms', 'OverviewRoom.jsx'))
  assert.match(overview, /qt2-summary-grid/)
  assert.match(overview, /attentionItems\.slice\(0, 3\)/)
  assert.doesNotMatch(overview, /qt2-step|function MarketPosture|function RecentOutcomes/)
})

test('Operations uses three task domains, grouped Market evidence, and cursor-backed run history', () => {
  const operations = source(path.join('v2', 'rooms', 'FleetRoom.jsx'))
  assert.match(operations, /id: \x27market\x27/)
  assert.match(operations, /Scheduled facts/)
  assert.match(operations, /Structure streams/)
  assert.match(operations, /buildCollectorGroups/)
  assert.match(operations, /runInventory\.loadMore/)
  assert.match(operations, /BotLens unavailable/)
  assert.match(operations, /Copy rerun command/)
  assert.doesNotMatch(operations, /id: \x27collectors\x27|id: \x27market-data\x27/)
  assert.doesNotMatch(operations, /errors\.map/)
})

test('operator errors preserve technical details behind readable copy', () => {
  const notice = source(path.join('v2', 'components', 'OperatorErrorNotice.jsx'))
  assert.match(notice, /Normalization evidence failed an integrity check/)
  assert.match(notice, /Technical details/)
  assert.match(notice, /Copy details/)
  assert.match(notice, /navigator\.clipboard\.writeText/)
})

test('historical BotLens loading is bounded and left-pan history retrieval is guarded', () => {
  const controller = source(path.join('features', 'bots', 'botlens', 'hooks', 'useBotLensController.js'))
  const viewport = source(path.join('components', 'bots', 'hooks', 'useViewportController.js'))
  assert.match(controller, /BOTLENS_EXACT_BOOTSTRAP_TIMEOUT_MS = 30_000/)
  assert.match(controller, /fetchExactRunBootstrapBeforeDeadline\(runId, exactBootstrapDeadline\)/)
  assert.match(controller, /Historical BotLens replay did not become ready within 30 seconds/)
  assert.match(viewport, /nearHistoryTriggeredRef/)
  assert.match(viewport, /onNearHistoryStart\?\.\(\)/)
})
