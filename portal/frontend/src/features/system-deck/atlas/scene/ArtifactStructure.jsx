import { Html, Sparkles } from '@react-three/drei'
import { useFrame } from '@react-three/fiber'
import { useEffect, useMemo, useRef, useState } from 'react'

import { ATLAS_FAMILIES } from '../types/atlasTypes.js'

function clamp(value, min, max) {
  return Math.min(max, Math.max(min, value))
}

function materialProps(artifact, multiplier = 1) {
  return {
    color: artifact.colors.color,
    emissive: artifact.colors.emissive,
    emissiveIntensity: artifact.brightness * 0.16 * multiplier,
    roughness: 0.42,
    metalness: 0.72,
  }
}

function BridgeStructure({ artifact }) {
  const height = artifact.height
  const width = artifact.width
  return (
    <group>
      <mesh position={[-width * 0.46, height * 0.48, 0]} castShadow receiveShadow>
        <boxGeometry args={[width * 0.52, height * 0.96, width * 0.54]} />
        <meshStandardMaterial {...materialProps(artifact)} />
      </mesh>
      <mesh position={[width * 0.46, height * 0.39, 0]} castShadow receiveShadow>
        <boxGeometry args={[width * 0.5, height * 0.78, width * 0.5]} />
        <meshStandardMaterial {...materialProps(artifact, 0.84)} />
      </mesh>
      <mesh position={[0, height * 0.69, 0]} castShadow>
        <boxGeometry args={[width * 1.55, height * 0.09, width * 0.42]} />
        <meshStandardMaterial {...materialProps(artifact, 1.2)} />
      </mesh>
      <mesh position={[0, height + 0.23, 0]} castShadow>
        <boxGeometry args={[width * 0.18, 0.46, width * 0.18]} />
        <meshStandardMaterial {...materialProps(artifact, 1.4)} />
      </mesh>
    </group>
  )
}

function RingStructure({ artifact }) {
  const height = artifact.height
  const width = artifact.width
  const ringCount = Math.max(3, Math.round(artifact.windowDensity * 6))
  return (
    <group>
      <mesh position={[0, height * 0.5, 0]} castShadow receiveShadow>
        <cylinderGeometry args={[width * 0.48, width * 0.62, height, 28]} />
        <meshStandardMaterial {...materialProps(artifact)} />
      </mesh>
      {Array.from({ length: ringCount }, (_, index) => {
        const y = height * (0.18 + (index / ringCount) * 0.72)
        return (
          <mesh key={index} position={[0, y, 0]} rotation={[Math.PI / 2, 0, 0]}>
            <torusGeometry args={[width * (0.73 + index * 0.018), 0.018, 5, 64]} />
            <meshBasicMaterial color={artifact.colors.glow} transparent opacity={0.34} />
          </mesh>
        )
      })}
    </group>
  )
}

function SpireStructure({ artifact }) {
  const height = artifact.height
  const width = artifact.width
  return (
    <group>
      <mesh position={[0, height * 0.47, 0]} castShadow receiveShadow>
        <boxGeometry args={[width * 0.74, height * 0.94, width * 0.74]} />
        <meshStandardMaterial {...materialProps(artifact)} />
      </mesh>
      <mesh position={[0, height + width * 0.38, 0]} castShadow>
        <coneGeometry args={[width * 0.44, width * 0.76, 4]} />
        <meshStandardMaterial {...materialProps(artifact, 1.35)} />
      </mesh>
      <mesh position={[0, height * 0.46, 0]} rotation={[0, Math.PI / 4, 0]}>
        <boxGeometry args={[width * 1.08, height * 0.045, width * 1.08]} />
        <meshBasicMaterial color={artifact.colors.glow} transparent opacity={0.18} />
      </mesh>
    </group>
  )
}

function TriadStructure({ artifact }) {
  const height = artifact.height
  const width = artifact.width
  return (
    <group>
      <mesh position={[0, height * 0.45, 0]} castShadow receiveShadow>
        <cylinderGeometry args={[width * 0.42, width * 0.66, height * 0.9, 3]} />
        <meshStandardMaterial {...materialProps(artifact)} />
      </mesh>
      {[0, 1, 2].map((index) => {
        const angle = index / 3 * Math.PI * 2
        return (
          <mesh
            key={index}
            position={[Math.cos(angle) * width * 0.72, height * 0.33, Math.sin(angle) * width * 0.72]}
            castShadow
          >
            <boxGeometry args={[width * 0.18, height * 0.66, width * 0.18]} />
            <meshStandardMaterial {...materialProps(artifact, 0.82)} />
          </mesh>
        )
      })}
    </group>
  )
}

function ClusterStructure({ artifact }) {
  const height = artifact.height
  const width = artifact.width
  return (
    <group>
      {Array.from({ length: 7 }, (_, index) => {
        const angle = index / 7 * Math.PI * 2
        const localHeight = height * (0.34 + ((artifact.seed + index * 17) % 42) / 100)
        const radius = index === 0 ? 0 : width * (0.52 + (index % 3) * 0.12)
        return (
          <mesh
            key={index}
            position={[Math.cos(angle) * radius, localHeight * 0.5, Math.sin(angle) * radius]}
            rotation={[0, angle * 0.3, 0]}
            castShadow
            receiveShadow
          >
            <boxGeometry args={[width * 0.38, localHeight, width * 0.38]} />
            <meshStandardMaterial {...materialProps(artifact, index === 0 ? 1.08 : 0.78)} />
          </mesh>
        )
      })}
    </group>
  )
}

function ObeliskStructure({ artifact }) {
  const height = artifact.height
  const width = artifact.width
  return (
    <group>
      <mesh position={[0, height * 0.45, 0]} castShadow receiveShadow>
        <cylinderGeometry args={[width * 0.36, width * 0.58, height * 0.9, 6]} />
        <meshStandardMaterial {...materialProps(artifact)} />
      </mesh>
      <mesh position={[0, height * 0.96, 0]} castShadow>
        <coneGeometry args={[width * 0.44, height * 0.26, 6]} />
        <meshStandardMaterial {...materialProps(artifact, 1.24)} />
      </mesh>
    </group>
  )
}

function TwinStructure({ artifact }) {
  const height = artifact.height
  const width = artifact.width
  return (
    <group>
      {[-1, 1].map((side) => (
        <mesh key={side} position={[side * width * 0.38, height * 0.47, 0]} castShadow receiveShadow>
          <cylinderGeometry args={[width * 0.25, width * 0.34, height * (side > 0 ? 0.94 : 0.78), 8]} />
          <meshStandardMaterial {...materialProps(artifact, side > 0 ? 1.1 : 0.86)} />
        </mesh>
      ))}
      <mesh position={[0, height * 0.63, 0]} castShadow>
        <boxGeometry args={[width * 1.1, height * 0.055, width * 0.26]} />
        <meshStandardMaterial {...materialProps(artifact, 1.3)} />
      </mesh>
      <mesh position={[0, height * 0.32, 0]} castShadow>
        <boxGeometry args={[width * 1.0, height * 0.045, width * 0.2]} />
        <meshStandardMaterial {...materialProps(artifact, 0.82)} />
      </mesh>
    </group>
  )
}

function RuinStructure({ artifact }) {
  const height = artifact.height
  const width = artifact.width
  return (
    <group>
      {Array.from({ length: 6 }, (_, index) => {
        const angle = index / 6 * Math.PI * 2
        const blockHeight = height * (0.42 + ((artifact.seed + index * 31) % 35) / 100)
        return (
          <mesh
            key={index}
            position={[Math.cos(angle) * width * 0.42, blockHeight * 0.48, Math.sin(angle) * width * 0.42]}
            rotation={[0.08 * (index % 2 ? 1 : -1), angle, 0.18 * (index % 3 - 1)]}
            castShadow
            receiveShadow
          >
            <boxGeometry args={[width * 0.32, blockHeight, width * 0.34]} />
            <meshStandardMaterial {...materialProps(artifact, 0.68)} />
          </mesh>
        )
      })}
      <pointLight color={artifact.colors.glow} intensity={1.8} distance={4.8} position={[0, height * 0.62, 0]} />
    </group>
  )
}

function StructureBody({ artifact }) {
  if (artifact.family === ATLAS_FAMILIES.bridge) return <BridgeStructure artifact={artifact} />
  if (artifact.family === ATLAS_FAMILIES.ring) return <RingStructure artifact={artifact} />
  if (artifact.family === ATLAS_FAMILIES.spire) return <SpireStructure artifact={artifact} />
  if (artifact.family === ATLAS_FAMILIES.triad) return <TriadStructure artifact={artifact} />
  if (artifact.family === ATLAS_FAMILIES.cluster) return <ClusterStructure artifact={artifact} />
  if (artifact.family === ATLAS_FAMILIES.twin) return <TwinStructure artifact={artifact} />
  if (artifact.family === ATLAS_FAMILIES.ruin) return <RuinStructure artifact={artifact} />
  return <ObeliskStructure artifact={artifact} />
}

function ArtifactWindows({ artifact }) {
  const distance = artifact.width * 0.62
  return (
    <group>
      {artifact.windows.map((window, index) => {
        const x = Math.cos(window.angle) * distance
        const z = Math.sin(window.angle) * distance
        const y = clamp(window.yRatio * artifact.height, 0.3, artifact.height - 0.12)
        const color = artifact.run.pnl < 0 ? '#fb7185' : window.warm ? '#fcd34d' : artifact.colors.glow
        return (
          <mesh key={index} position={[x, y, z]} rotation={[0, -window.angle, 0]}>
            <boxGeometry args={[window.size, window.size * 1.72, 0.012]} />
            <meshBasicMaterial color={color} transparent opacity={0.22 + window.intensity * 0.58} />
          </mesh>
        )
      })}
    </group>
  )
}

function DamageCracks({ artifact }) {
  if (artifact.cracks.length === 0) return null
  const distance = artifact.width * 0.65
  return (
    <group>
      {artifact.cracks.map((crack, index) => (
        <mesh
          key={index}
          position={[
            Math.cos(crack.angle) * distance,
            clamp(crack.yRatio * artifact.height, 0.24, artifact.height - 0.16),
            Math.sin(crack.angle) * distance,
          ]}
          rotation={[0, -crack.angle, crack.skew]}
        >
          <boxGeometry args={[0.022, crack.length, 0.016]} />
          <meshBasicMaterial color={artifact.run.pnl < 0 ? '#fecdd3' : '#020617'} transparent opacity={crack.opacity} />
        </mesh>
      ))}
    </group>
  )
}

function Satellite({ symbol, artifact, selected }) {
  const ref = useRef(null)

  useFrame(({ clock }) => {
    if (!ref.current) return
    ref.current.rotation.y = symbol.angle + clock.getElapsedTime() * symbol.speed
  })

  return (
    <group ref={ref}>
      <mesh position={[0, artifact.height * 0.44 + symbol.y, 0]} rotation={[Math.PI / 2, 0, 0]}>
        <torusGeometry args={[symbol.radius, 0.006, 4, 72]} />
        <meshBasicMaterial color={symbol.color} transparent opacity={0.24} />
      </mesh>
      <mesh position={[symbol.radius, artifact.height * 0.44 + symbol.y, 0]}>
        <sphereGeometry args={[0.075, 14, 14]} />
        <meshStandardMaterial color={symbol.color} emissive={symbol.color} emissiveIntensity={0.85} roughness={0.25} metalness={0.4} />
      </mesh>
      {selected ? (
        <Html
          center
          position={[symbol.radius, artifact.height * 0.44 + symbol.y + 0.22, 0]}
          distanceFactor={12}
          zIndexRange={[6, 0]}
          style={{ pointerEvents: 'none' }}
        >
          <span className="atlas-symbol-label">{symbol.root}</span>
        </Html>
      ) : null}
    </group>
  )
}

function SymbolSatellites({ artifact, selected }) {
  return (
    <>
      {artifact.orbitingSymbols.map((symbol) => (
        <Satellite key={symbol.symbol} symbol={symbol} artifact={artifact} selected={selected} />
      ))}
    </>
  )
}

function SelectionBeacon({ artifact, selected, hovered }) {
  const ref = useRef(null)
  const visible = selected || hovered

  useFrame(({ clock }) => {
    if (!ref.current) return
    const pulse = 1 + Math.sin(clock.getElapsedTime() * 2.4 + artifact.index) * 0.045
    ref.current.scale.setScalar(pulse)
  })

  if (!visible) return null

  return (
    <group ref={ref}>
      <mesh position={[0, 0.045, 0]} rotation={[Math.PI / 2, 0, 0]}>
        <torusGeometry args={[artifact.width * 1.28, 0.018, 6, 96]} />
        <meshBasicMaterial color={selected ? artifact.colors.glow : '#cbd5e1'} transparent opacity={selected ? 0.75 : 0.36} />
      </mesh>
      {selected ? (
        <pointLight color={artifact.colors.glow} intensity={3.8} distance={8} position={[0, artifact.height * 0.58, 0]} />
      ) : null}
    </group>
  )
}

function HoverLabel({ artifact, hovered, selected }) {
  if (!hovered || selected) return null
  return (
    <Html center position={[0, artifact.height + 0.72, 0]} distanceFactor={14} zIndexRange={[5, 0]} style={{ pointerEvents: 'none' }}>
      <div className="atlas-hover-label">
        <strong>{artifact.run.strategy}</strong>
        <span>{artifact.id}</span>
      </div>
    </Html>
  )
}

export function ArtifactStructure({ artifact, selected, onSelect }) {
  const groupRef = useRef(null)
  const [hovered, setHovered] = useState(false)
  const scale = useMemo(() => 0.92 + (artifact.seed % 9) / 100, [artifact.seed])

  useEffect(() => () => {
    if (typeof document !== 'undefined') document.body.style.cursor = ''
  }, [])

  useFrame(({ clock }) => {
    if (!groupRef.current) return
    const target = selected ? 1.08 : hovered ? 1.035 : 1
    const current = groupRef.current.scale.x
    const next = current + (target * scale - current) * 0.08
    groupRef.current.scale.setScalar(next)
    groupRef.current.position.y = Math.sin(clock.getElapsedTime() * 0.72 + artifact.index) * 0.025
  })

  const handlePointerOver = (event) => {
    event.stopPropagation()
    setHovered(true)
    if (typeof document !== 'undefined') document.body.style.cursor = 'pointer'
  }

  const handlePointerOut = () => {
    setHovered(false)
    if (typeof document !== 'undefined') document.body.style.cursor = ''
  }

  const handleClick = (event) => {
    event.stopPropagation()
    onSelect(artifact.id)
  }

  return (
    <group
      ref={groupRef}
      position={[artifact.position.x, artifact.position.y, artifact.position.z]}
      onPointerOver={handlePointerOver}
      onPointerOut={handlePointerOut}
      onClick={handleClick}
    >
      <mesh position={[0, 0.05, 0]} receiveShadow>
        <cylinderGeometry args={[artifact.width * 1.12, artifact.width * 1.28, 0.1, 28]} />
        <meshStandardMaterial color="#0b111a" emissive={artifact.colors.emissive} emissiveIntensity={0.025} roughness={0.62} metalness={0.55} />
      </mesh>
      <StructureBody artifact={artifact} />
      <ArtifactWindows artifact={artifact} />
      <DamageCracks artifact={artifact} />
      <SymbolSatellites artifact={artifact} selected={selected} />
      <SelectionBeacon artifact={artifact} selected={selected} hovered={hovered} />
      <HoverLabel artifact={artifact} hovered={hovered} selected={selected} />
      {artifact.run.pnl > 1200 ? (
        <Sparkles
          count={Math.round(8 + artifact.profitability * 18)}
          speed={0.18}
          size={1.1}
          scale={[artifact.width * 2.8, artifact.height * 1.2, artifact.width * 2.8]}
          position={[0, artifact.height * 0.5, 0]}
          color={artifact.colors.glow}
        />
      ) : null}
    </group>
  )
}
