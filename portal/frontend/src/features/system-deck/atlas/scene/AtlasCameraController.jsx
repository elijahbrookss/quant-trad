import { OrbitControls } from '@react-three/drei'
import { useFrame, useThree } from '@react-three/fiber'
import { useEffect, useMemo, useRef } from 'react'
import * as THREE from 'three'

const DEFAULT_CAMERA = new THREE.Vector3(0, 15, 28)
const DEFAULT_TARGET = new THREE.Vector3(0, 0, 0)

function focusOffsetFor(seed, height) {
  const angle = ((seed % 360) / 360) * Math.PI * 2
  const distance = Math.max(8, 7.4 + height * 1.15)
  return new THREE.Vector3(Math.cos(angle) * distance, Math.max(4.5, height * 0.84 + 3.2), Math.sin(angle) * distance)
}

export function AtlasCameraController({ focusTarget, focusKey, resetKey }) {
  const { camera } = useThree()
  const controlsRef = useRef(null)
  const easingRef = useRef(true)
  const goalRef = useRef({
    camera: DEFAULT_CAMERA.clone(),
    target: DEFAULT_TARGET.clone(),
  })

  const focusSignature = useMemo(() => {
    if (!focusTarget) return 'none'
    return `${focusTarget.x}:${focusTarget.y}:${focusTarget.z}:${focusTarget.seed}:${focusKey}`
  }, [focusKey, focusTarget])

  useEffect(() => {
    goalRef.current = {
      camera: DEFAULT_CAMERA.clone(),
      target: DEFAULT_TARGET.clone(),
    }
    easingRef.current = true
  }, [resetKey])

  useEffect(() => {
    if (!focusTarget) return
    const target = new THREE.Vector3(focusTarget.x, focusTarget.y, focusTarget.z)
    goalRef.current = {
      camera: target.clone().add(focusOffsetFor(focusTarget.seed, focusTarget.height)),
      target,
    }
    easingRef.current = true
  }, [focusSignature, focusTarget])

  useFrame((_, delta) => {
    const controls = controlsRef.current
    if (!controls) return
    if (!easingRef.current) {
      controls.update()
      return
    }
    const alpha = 1 - Math.pow(0.001, delta)
    camera.position.lerp(goalRef.current.camera, alpha * 0.82)
    controls.target.lerp(goalRef.current.target, alpha * 0.9)
    controls.update()
    if (
      camera.position.distanceTo(goalRef.current.camera) < 0.035 &&
      controls.target.distanceTo(goalRef.current.target) < 0.025
    ) {
      easingRef.current = false
    }
  })

  return (
    <OrbitControls
      ref={controlsRef}
      onStart={() => {
        easingRef.current = false
      }}
      enableDamping
      dampingFactor={0.08}
      rotateSpeed={0.42}
      zoomSpeed={0.72}
      panSpeed={0.55}
      minDistance={5}
      maxDistance={72}
      maxPolarAngle={Math.PI * 0.48}
      makeDefault
    />
  )
}
