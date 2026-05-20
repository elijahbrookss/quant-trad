import { Html } from '@react-three/drei'

function DistrictMarker({ district }) {
  return (
    <group position={[district.anchor.x, 0.012, district.anchor.z]}>
      <mesh rotation={[Math.PI / 2, 0, 0]}>
        <torusGeometry args={[district.radius, 0.018, 6, 128]} />
        <meshBasicMaterial color={district.color} transparent opacity={0.38} />
      </mesh>
      <mesh rotation={[Math.PI / 2, 0, 0]}>
        <circleGeometry args={[district.radius, 96]} />
        <meshBasicMaterial color={district.color} transparent opacity={0.018} depthWrite={false} />
      </mesh>
      <Html
        center
        position={[0, 0.2, -district.radius - 0.65]}
        distanceFactor={20}
        zIndexRange={[3, 0]}
        style={{ pointerEvents: 'none' }}
      >
        <div className="atlas-district-label">
          <span>{district.count}</span>
          <strong>{district.label}</strong>
        </div>
      </Html>
    </group>
  )
}

export function DistrictMarkers({ districts }) {
  return (
    <>
      {districts.map((district) => (
        <DistrictMarker key={district.key} district={district} />
      ))}
    </>
  )
}
