import { useEffect, useRef } from 'react'
import { useAccentColor } from '../../contexts/AccentColorContext.jsx'

const NODE_COUNT = 62
const CONNECT_DIST = 165
const CONNECT_DIST_SQ = CONNECT_DIST * CONNECT_DIST
const BASE_SPEED = 0.2
const MOUSE_REPEL_DIST = 135
const MOUSE_REPEL_DIST_SQ = MOUSE_REPEL_DIST * MOUSE_REPEL_DIST
const MOUSE_REPEL_STRENGTH = 0.042

function hexToRgb(hex) {
  const m = /^#?([a-f\d]{2})([a-f\d]{2})([a-f\d]{2})$/i.exec(hex)
  return m ? [parseInt(m[1], 16), parseInt(m[2], 16), parseInt(m[3], 16)] : [56, 189, 248]
}

function makeNode(w, h) {
  return {
    x: Math.random() * w,
    y: Math.random() * h,
    vx: (Math.random() - 0.5) * BASE_SPEED * 2,
    vy: (Math.random() - 0.5) * BASE_SPEED * 2,
    r: 1.5 + Math.random() * 1.1,
    phase: Math.random() * Math.PI * 2,
  }
}

export function ParticleField() {
  const { accentColor } = useAccentColor()
  const canvasRef = useRef(null)
  const accentRgbRef = useRef(hexToRgb(accentColor))

  useEffect(() => {
    accentRgbRef.current = hexToRgb(accentColor)
  }, [accentColor])

  useEffect(() => {
    const canvas = canvasRef.current
    if (!canvas) return

    const ctx = canvas.getContext('2d')
    let rafId = null
    let nodes = []
    const mouse = { x: -9999, y: -9999 }

    function resize() {
      canvas.width = window.innerWidth
      canvas.height = window.innerHeight
    }

    function init() {
      resize()
      nodes = Array.from({ length: NODE_COUNT }, () => makeNode(canvas.width, canvas.height))
    }

    function tick() {
      ctx.clearRect(0, 0, canvas.width, canvas.height)
      const [r, g, b] = accentRgbRef.current
      const W = canvas.width
      const H = canvas.height

      for (const n of nodes) {
        n.phase += 0.011

        const dx = n.x - mouse.x
        const dy = n.y - mouse.y
        const d2 = dx * dx + dy * dy
        if (d2 < MOUSE_REPEL_DIST_SQ && d2 > 0) {
          const d = Math.sqrt(d2)
          const f = (1 - d / MOUSE_REPEL_DIST) * MOUSE_REPEL_STRENGTH
          n.vx += (dx / d) * f
          n.vy += (dy / d) * f
        }

        n.vx *= 0.998
        n.vy *= 0.998
        const spd = Math.sqrt(n.vx * n.vx + n.vy * n.vy)
        if (spd > BASE_SPEED * 2.2) {
          n.vx = (n.vx / spd) * BASE_SPEED * 2.2
          n.vy = (n.vy / spd) * BASE_SPEED * 2.2
        }

        n.x += n.vx
        n.y += n.vy

        if (n.x < -14) n.x = W + 14
        else if (n.x > W + 14) n.x = -14
        if (n.y < -14) n.y = H + 14
        else if (n.y > H + 14) n.y = -14
      }

      // edges — squared dist check first to skip sqrt on most pairs
      for (let i = 0; i < nodes.length - 1; i++) {
        for (let j = i + 1; j < nodes.length; j++) {
          const dx = nodes[i].x - nodes[j].x
          const dy = nodes[i].y - nodes[j].y
          const d2 = dx * dx + dy * dy
          if (d2 < CONNECT_DIST_SQ) {
            const dist = Math.sqrt(d2)
            const alpha = (1 - dist / CONNECT_DIST) * 0.15
            ctx.beginPath()
            ctx.moveTo(nodes[i].x, nodes[i].y)
            ctx.lineTo(nodes[j].x, nodes[j].y)
            ctx.strokeStyle = `rgba(${r},${g},${b},${alpha.toFixed(3)})`
            ctx.lineWidth = 0.5
            ctx.stroke()
          }
        }
      }

      // nodes
      for (const n of nodes) {
        const pulse = 0.2 + Math.sin(n.phase) * 0.08

        // soft radial glow
        const grd = ctx.createRadialGradient(n.x, n.y, 0, n.x, n.y, n.r * 6.5)
        grd.addColorStop(0, `rgba(${r},${g},${b},0.09)`)
        grd.addColorStop(1, `rgba(${r},${g},${b},0)`)
        ctx.beginPath()
        ctx.arc(n.x, n.y, n.r * 6.5, 0, Math.PI * 2)
        ctx.fillStyle = grd
        ctx.fill()

        // core dot
        ctx.beginPath()
        ctx.arc(n.x, n.y, n.r, 0, Math.PI * 2)
        ctx.fillStyle = `rgba(${r},${g},${b},${pulse.toFixed(3)})`
        ctx.fill()
      }

      rafId = requestAnimationFrame(tick)
    }

    const onMouseMove = (e) => {
      mouse.x = e.clientX
      mouse.y = e.clientY
    }
    const onMouseLeave = () => {
      mouse.x = -9999
      mouse.y = -9999
    }
    const onResize = () => resize()

    init()
    rafId = requestAnimationFrame(tick)

    window.addEventListener('mousemove', onMouseMove, { passive: true })
    window.addEventListener('mouseleave', onMouseLeave)
    window.addEventListener('resize', onResize)

    return () => {
      if (rafId !== null) cancelAnimationFrame(rafId)
      window.removeEventListener('mousemove', onMouseMove)
      window.removeEventListener('mouseleave', onMouseLeave)
      window.removeEventListener('resize', onResize)
    }
  }, [])

  return (
    <canvas
      ref={canvasRef}
      style={{ position: 'fixed', inset: 0, zIndex: 0, pointerEvents: 'none' }}
      aria-hidden="true"
    />
  )
}
