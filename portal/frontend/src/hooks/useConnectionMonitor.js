import { useCallback, useMemo, useRef, useState } from 'react'

const STATUS_MESSAGES = {
  idle: 'Awaiting first connection',
  connecting: 'Contacting QuantLab backend…',
  online: 'Realtime feed stable',
  error: 'Connection lost. Investigate immediately.',
  recovering: 'Re-establishing stream…',
}

export function useConnectionMonitor({ name = 'QuantLab API' } = {}) {
  const [status, setStatus] = useState('idle')
  const [message, setMessage] = useState(STATUS_MESSAGES.idle)
  const lastHeartbeatRef = useRef(null)

  const markAttempt = useCallback(() => {
    setStatus((prev) => (prev === 'error' ? 'recovering' : 'connecting'))
    setMessage(STATUS_MESSAGES.connecting)
  }, [])

  const markSuccess = useCallback(() => {
    lastHeartbeatRef.current = new Date()
    setStatus('online')
    setMessage(STATUS_MESSAGES.online)
  }, [])

  const markError = useCallback((err) => {
    lastHeartbeatRef.current = new Date()
    setStatus('error')
    if (err?.message) {
      setMessage(`${name} error: ${err.message}`)
    } else {
      setMessage(STATUS_MESSAGES.error)
    }
  }, [name])

  return useMemo(() => ({
    status,
    message,
    lastHeartbeat: lastHeartbeatRef.current,
    markAttempt,
    markSuccess,
    markError,
  }), [status, message, markAttempt, markSuccess, markError])
}
