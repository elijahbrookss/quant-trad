// Shared physical reads have consumer-owned cancellation. Scientific jobs are
// deliberately outside this module. Only bounded compact metadata is cached.
export function createResearchReadCache({ ttl = 15000, maxEntries = 20, maxBytes = 2 * 1024 * 1024 } = {}) {
  const entries = new Map()
  const abortError = () => new DOMException('Page load cancelled', 'AbortError')
  function trim() {
    let bytes = [...entries.values()].reduce((n, entry) => n + (entry.bytes || 0), 0)
    for (const [key, entry] of entries) {
      if (entries.size <= maxEntries && bytes <= maxBytes) break
      if (entry.settled) { entries.delete(key); bytes -= entry.bytes || 0 }
    }
  }
  function read(key, load, { signal, cache = false, refresh = false } = {}) {
    if (signal?.aborted) return Promise.reject(abortError())
    let entry = entries.get(key)
    if (entry?.settled && (refresh || !cache || entry.expires <= Date.now())) { entries.delete(key); entry = null }
    if (!entry) {
      entry = { controller: new AbortController(), consumers: 0, settled: false, bytes: 0 }
      entries.set(key, entry)
      const owned = entry
      entry.promise = Promise.resolve().then(() => load(owned.controller.signal)).then(value => {
        owned.settled = true
        owned.bytes = cache ? new TextEncoder().encode(JSON.stringify(value)).byteLength : 0
        owned.expires = Date.now() + ttl
        if (!cache || owned.bytes > 256 * 1024) { if (entries.get(key) === owned) entries.delete(key) }
        trim()
        return value
      }, error => {
        owned.settled = true
        if (entries.get(key) === owned) entries.delete(key)
        throw error
      })
    }
    const owned = entry
    owned.consumers += 1
    return new Promise((resolve, reject) => {
      let finished = false
      const release = () => {
        if (finished) return false
        finished = true
        signal?.removeEventListener('abort', cancel)
        owned.consumers -= 1
        if (!owned.settled && owned.consumers === 0) {
          // StrictMode and immediate same-key navigation can attach before the
          // final consumer's transport is released.
          setTimeout(() => {
            if (!owned.settled && owned.consumers === 0) {
              if (entries.get(key) === owned) entries.delete(key)
              owned.controller.abort()
            }
          }, 0)
        }
        return true
      }
      const cancel = () => { if (release()) reject(abortError()) }
      signal?.addEventListener('abort', cancel, { once: true })
      owned.promise.then(value => { if (release()) resolve(value) }, error => { if (release()) reject(error) })
    })
  }
  return { read, clear: () => { for (const [key, value] of entries) if (value.settled) entries.delete(key) } }
}
