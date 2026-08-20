import test from 'node:test'
import assert from 'node:assert/strict'
import { readFileSync } from 'node:fs'
import path from 'node:path'
import { fileURLToPath } from 'node:url'
import { resolveHmrClientPort } from '../vite.config.js'

const here = path.dirname(fileURLToPath(import.meta.url))
const repositoryRoot = path.resolve(here, '..', '..', '..')

test('HMR client ports are explicit at Docker host boundaries', () => {
  const compose = readFileSync(path.join(repositoryRoot, 'docker', 'docker-compose.yml'), 'utf8')
  assert.match(compose, /frontend:[\s\S]*VITE_HMR_CLIENT_PORT=5173[\s\S]*"5173:5173"/)
  assert.match(compose, /frontend-v2:[\s\S]*VITE_HMR_CLIENT_PORT=5174[\s\S]*"5174:5173"/)
})

test('HMR client port validation accepts a configured host port', () => {
  assert.equal(resolveHmrClientPort('5174'), 5174)
  assert.equal(resolveHmrClientPort(undefined), undefined)
})

test('HMR client port validation rejects malformed configuration', () => {
  assert.throws(() => resolveHmrClientPort('not-a-port'), /valid TCP port/)
  assert.throws(() => resolveHmrClientPort('70000'), /valid TCP port/)
})
