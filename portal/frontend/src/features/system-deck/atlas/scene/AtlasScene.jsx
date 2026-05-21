import { Sparkles, Stars } from '@react-three/drei'
import { Canvas } from '@react-three/fiber'
import { Suspense } from 'react'

import { ArtifactStructure } from './ArtifactStructure.jsx'
import { AtlasCameraController } from './AtlasCameraController.jsx'
import { DistrictMarkers } from './DistrictMarkers.jsx'

function AtlasAtmosphere() {
  return (
    <>
      <color attach="background" args={['#02040a']} />
      <fog attach="fog" args={['#050814', 16, 92]} />
      <ambientLight intensity={0.16} />
      <hemisphereLight args={['#1e3a5f', '#050814', 0.22]} />
      <directionalLight position={[18, 24, 12]} intensity={0.42} castShadow />
      <pointLight color="#38bdf8" intensity={3.4} distance={44} position={[-12, 8, -10]} />
      <pointLight color="#f59e0b" intensity={1.4} distance={36} position={[-9, 7, 16]} />
      <pointLight color="#2dd4bf" intensity={1.8} distance={38} position={[20, 6, 16]} />
      <Stars radius={150} depth={58} count={5200} factor={4} saturation={0} fade speed={0.09} />
      <Sparkles count={95} scale={[72, 14, 72]} position={[0, 7, 0]} size={0.85} speed={0.08} color="#64748b" opacity={0.18} />
      <gridHelper args={[160, 96, '#1f3b4d', '#0f172a']} position={[0, -0.012, 0]} />
      <mesh rotation={[-Math.PI / 2, 0, 0]} position={[0, -0.04, 0]} receiveShadow>
        <planeGeometry args={[180, 180]} />
        <meshStandardMaterial color="#05070d" roughness={0.92} metalness={0.18} transparent opacity={0.74} />
      </mesh>
    </>
  )
}

export function AtlasScene({
  artifacts,
  districts,
  selectedId,
  focusTarget,
  focusKey,
  resetKey,
  onSelectArtifact,
}) {
  return (
    <Canvas
      className="atlas-canvas"
      camera={{ position: [0, 15, 28], fov: 47, near: 0.1, far: 210 }}
      dpr={[1, 1.75]}
      gl={{ antialias: true, alpha: false, powerPreference: 'high-performance' }}
      shadows
      onPointerMissed={() => onSelectArtifact(null)}
    >
      <Suspense fallback={null}>
        <AtlasAtmosphere />
        <DistrictMarkers districts={districts} />
        {artifacts.map((artifact) => (
          <ArtifactStructure
            key={artifact.id}
            artifact={artifact}
            selected={selectedId === artifact.id}
            onSelect={onSelectArtifact}
          />
        ))}
        <AtlasCameraController
          focusTarget={focusTarget}
          focusKey={focusKey}
          resetKey={resetKey}
        />
      </Suspense>
    </Canvas>
  )
}
