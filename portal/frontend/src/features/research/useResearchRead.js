import { useEffect, useRef, useState } from 'react'

export function useResearchRead(reader, args = [], enabled = true) {
  const serialized = JSON.stringify(args)
  const identity = `${reader.name}:${serialized}`
  const [revision, setRevision] = useState(0)
  const [state, setState] = useState({ identity: null, loading: true, data: null, error: null, cancelled: false })
  const controllerRef = useRef(null)
  useEffect(() => {
    if (!enabled) return undefined
    let current = true
    const controller = new AbortController()
    controllerRef.current = controller
    setState({ identity, loading: true, data: null, error: null, cancelled: false })
    reader(...JSON.parse(serialized), { signal: controller.signal, refresh: revision > 0 }).then(data => {
      if (current && !controller.signal.aborted) setState({ identity, loading: false, data, error: null, cancelled: false })
    }).catch(error => {
      if (current) setState({ identity, loading: false, data: null, error: error.name === 'AbortError' ? null : error, cancelled: error.name === 'AbortError' })
    })
    return () => { current = false; controller.abort() }
  }, [reader, serialized, identity, enabled, revision])
  return {
    ...(state.identity === identity ? state : { data: null, error: null, cancelled: false, loading: enabled }),
    loading: enabled && (state.identity !== identity || state.loading),
    refresh: () => setRevision(value => value + 1),
    cancel: () => controllerRef.current?.abort(),
  }
}
