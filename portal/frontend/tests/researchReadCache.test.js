import test from 'node:test'
import assert from 'node:assert/strict'
import { createResearchReadCache } from '../src/adapters/researchReadCache.js'

const tick = () => new Promise(resolve => setTimeout(resolve, 5))
test('shared physical read survives one consumer abort, then caches compact metadata', async () => {
  const cache = createResearchReadCache()
  let complete, calls = 0, physical
  const load = signal => { calls++; physical = signal; return new Promise(resolve => { complete = resolve }) }
  const a = new AbortController(), b = new AbortController()
  const first = cache.read('same', load, { signal: a.signal, cache: true })
  const second = cache.read('same', load, { signal: b.signal, cache: true })
  await tick(); a.abort()
  await assert.rejects(first, { name: 'AbortError' }); await tick()
  assert.equal(physical.aborted, false)
  complete({ id: 'correct' })
  assert.deepEqual(await second, { id: 'correct' })
  assert.deepEqual(await cache.read('same', load, { cache: true }), { id: 'correct' })
  assert.equal(calls, 1)
})
test('last consumer cancels transport and late obsolete completion cannot overwrite new key', async () => {
  const cache = createResearchReadCache()
  let obsolete, physical
  const controller = new AbortController()
  const first = cache.read('key', signal => { physical = signal; return new Promise(resolve => { obsolete = resolve }) }, { signal: controller.signal, cache: true })
  await tick(); controller.abort(); await assert.rejects(first, { name: 'AbortError' }); await tick()
  assert.equal(physical.aborted, true)
  await cache.read('key', () => Promise.resolve('new'), { cache: true })
  obsolete('old'); await tick()
  assert.equal(await cache.read('key', () => Promise.reject(new Error('unexpected')), { cache: true }), 'new')
})
test('raw evidence is never cached and compact caches expire and evict', async () => {
  const cache = createResearchReadCache({ ttl: 1, maxEntries: 1, maxBytes: 20 })
  let calls = 0
  const load = () => Promise.resolve(++calls)
  await cache.read('raw', load); await cache.read('raw', load)
  assert.equal(calls, 2)
  await cache.read('compact', load, { cache: true }); await tick()
  await cache.read('compact', load, { cache: true }); assert.equal(calls, 4)
  await cache.read('large', () => Promise.resolve('x'.repeat(300000)), { cache: true })
  await cache.read('large', load, { cache: true }); assert.equal(calls, 5)
})
