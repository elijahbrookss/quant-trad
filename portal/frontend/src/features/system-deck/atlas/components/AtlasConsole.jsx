import { CornerDownLeft, Terminal, Trash2 } from 'lucide-react'
import { useCallback, useEffect, useRef, useState } from 'react'

import { ATLAS_COMMAND_TONES } from '../types/atlasTypes.js'

const INITIAL_LINES = [
  { id: 'boot-1', tone: ATLAS_COMMAND_TONES.system, text: 'atlas kernel online' },
  { id: 'boot-2', tone: ATLAS_COMMAND_TONES.muted, text: 'mock artifacts indexed; world seed locked' },
]

export function AtlasConsole({ onCommand }) {
  const [input, setInput] = useState('')
  const [lines, setLines] = useState(INITIAL_LINES)
  const timersRef = useRef([])
  const counterRef = useRef(0)
  const endRef = useRef(null)

  const clearTimers = useCallback(() => {
    for (const timer of timersRef.current) clearTimeout(timer)
    timersRef.current = []
  }, [])

  useEffect(() => clearTimers, [clearTimers])

  useEffect(() => {
    endRef.current?.scrollIntoView({ block: 'end' })
  }, [lines])

  const nextId = useCallback((prefix) => {
    counterRef.current += 1
    return `${prefix}-${counterRef.current}`
  }, [])

  const animateLines = useCallback((entries) => {
    entries.forEach((entry, index) => {
      const timer = setTimeout(() => {
        setLines((current) => [
          ...current.slice(-90),
          {
            id: nextId('out'),
            tone: entry.tone || ATLAS_COMMAND_TONES.system,
            text: entry.text,
          },
        ])
      }, 70 + index * 48)
      timersRef.current.push(timer)
    })
  }, [nextId])

  const submitCommand = useCallback((event) => {
    event.preventDefault()
    const command = input.trim()
    if (!command) return
    setInput('')
    clearTimers()
    const result = onCommand(command)

    if (result.actions?.clearConsole) {
      setLines([])
    } else {
      setLines((current) => [
        ...current.slice(-90),
        {
          id: nextId('in'),
          tone: ATLAS_COMMAND_TONES.input,
          text: `> ${command}`,
        },
      ])
    }

    animateLines(result.lines || [])
  }, [animateLines, clearTimers, input, nextId, onCommand])

  const clearConsole = useCallback(() => {
    clearTimers()
    setLines([])
    setInput('')
  }, [clearTimers])

  return (
    <section className="atlas-console" aria-label="Atlas command console">
      <header className="atlas-console-header">
        <div className="atlas-console-title">
          <Terminal size={14} />
          <span>atlas://console</span>
        </div>
        <button type="button" className="atlas-icon-button" onClick={clearConsole} aria-label="Clear console">
          <Trash2 size={13} />
        </button>
      </header>

      <div className="atlas-console-stream qt-scrollbar-hidden">
        {lines.map((line) => (
          <div key={line.id} className="atlas-console-line" data-tone={line.tone}>
            {line.text}
          </div>
        ))}
        <div ref={endRef} />
      </div>

      <form className="atlas-console-form" onSubmit={submitCommand}>
        <span className="atlas-console-prompt">atlas</span>
        <input
          value={input}
          onChange={(event) => setInput(event.target.value)}
          spellCheck={false}
          autoComplete="off"
          placeholder="atlas latest"
          aria-label="Atlas command"
        />
        <button type="submit" className="atlas-command-submit" aria-label="Run Atlas command">
          <CornerDownLeft size={14} />
        </button>
      </form>
    </section>
  )
}
