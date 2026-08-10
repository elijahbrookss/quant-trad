import { Activity, AlertTriangle, ArrowRight, Database, Radio, Server } from 'lucide-react'
import { Link } from 'react-router-dom'
import { buildCollectorCardViewModel } from './buildCollectorCardViewModel.js'

function formatMetric(value, suffix = '') {
  return Number.isFinite(Number(value)) ? `${Number(value).toLocaleString()}${suffix}` : 'Unavailable'
}

function stateTone(state) {
  if (state === 'HEALTHY') return 'success'
  if (['FAILED'].includes(state)) return 'danger'
  if (['DEGRADED', 'RETRYING', 'RECOVERING', 'STOPPING'].includes(state)) return 'warning'
  if (['STARTING'].includes(state)) return 'info'
  return 'neutral'
}

function StateBadge({ state }) {
  return <span className={'qt2-evidence-state is-' + stateTone(state)}>{state || 'UNKNOWN'}</span>
}

function providerGroups(collectors, query) {
  const needle = query.trim().toLowerCase()
  const filtered = collectors.filter((collector) => !needle || [
    collector.provider,
    collector.venue,
    collector.collector_kind,
    collector.collector_id,
    collector.actual_state,
    ...(collector.subjects || []).flatMap((subject) => Object.values(subject || {})),
    ...(collector.fact_schemas || []).flatMap((schema) => [schema.fact_type, schema.schema_version]),
  ].some((value) => String(value || '').toLowerCase().includes(needle)))

  const groups = new Map()
  filtered.forEach((collector) => {
    const provider = collector.provider || 'UNAVAILABLE'
    groups.set(provider, [...(groups.get(provider) || []), collector])
  })
  return [...groups.entries()].sort(([left], [right]) => left.localeCompare(right))
}

function PlaneMetrics({ feed }) {
  const plane = feed.dataPlane || {}
  const fleet = feed.fleet || {}
  const problemCount = Object.entries(fleet.state_counts || {})
    .filter(([state]) => ['DEGRADED', 'FAILED', 'RETRYING'].includes(state))
    .reduce((sum, [, count]) => sum + Number(count || 0), 0)
  const metrics = [
    { label: 'Registered collectors', value: formatMetric(fleet.collector_count), detail: `${formatMetric(fleet.desired_running_count)} desired running · ${formatMetric(fleet.unregistered_definition_count)} non-operational definitions` },
    { label: 'Ingestion', value: formatMetric(plane.ingestion_rate_per_minute, '/min'), detail: 'Accepted canonical facts' },
    { label: 'Active schemas', value: formatMetric(plane.active_schema_count), detail: 'Typed and versioned' },
    { label: 'Needs attention', value: formatMetric(problemCount), detail: `${formatMetric(plane.stale_stream_count)} stale streams` },
  ]
  return (
    <div className="qt2-plane-metrics">
      {metrics.map((metric) => <article key={metric.label}><span>{metric.label}</span><strong>{metric.value}</strong><small>{metric.detail}</small></article>)}
    </div>
  )
}

function CollectorNode({ collector }) {
  const vm = buildCollectorCardViewModel(collector)
  return (
    <Link className={'qt2-topology-collector is-' + vm.tone} to={vm.route} state={{ from: '/operations?tab=market' }}>
      <span><StateBadge state={vm.state} /><small>{vm.kindLabel}</small></span>
      <strong>{vm.displayName}</strong>
      <dl>
        <div><dt>Facts</dt><dd>{vm.throughputLabel}</dd></div>
        <div><dt>Freshness</dt><dd>{vm.freshnessLabel}</dd></div>
        <div><dt>Gaps</dt><dd>{collector.gap?.active_count ?? 'Evidence'}</dd></div>
      </dl>
    </Link>
  )
}

function Topology({ groups }) {
  if (!groups.length) return <div className="qt2-empty">No collectors match the current filter.</div>
  return (
    <div className="qt2-collector-topology" aria-label="Provider to canonical Fact store topology">
      {groups.map(([provider, collectors]) => {
        const schemas = [...new Map(collectors.flatMap((collector) => collector.fact_schemas || []).map((schema) => [`${schema.fact_type}:${schema.schema_version}`, schema])).values()]
        const throughput = collectors.reduce((sum, collector) => sum + Number(collector.throughput?.accepted_last_minute || 0), 0)
        return (
          <section className="qt2-topology-row" key={provider}>
            <div className="qt2-topology-provider"><Radio size={18} /><strong>{provider}</strong><small>{collectors.length} collector{collectors.length === 1 ? '' : 's'}</small></div>
            <ArrowRight className="qt2-topology-arrow" aria-hidden="true" />
            <div className="qt2-topology-collectors">{collectors.map((collector) => <CollectorNode collector={collector} key={`${collector.collector_kind}:${collector.collector_id}`} />)}</div>
            <ArrowRight className="qt2-topology-arrow" aria-hidden="true" />
            <div className="qt2-topology-schemas"><Activity size={17} /><strong>{schemas.length} canonical schema{schemas.length === 1 ? '' : 's'}</strong><small>{throughput.toLocaleString()} facts/min</small><div>{schemas.slice(0, 4).map((schema) => <span key={`${schema.fact_type}:${schema.schema_version}`}>{schema.fact_type}</span>)}</div></div>
            <ArrowRight className="qt2-topology-arrow" aria-hidden="true" />
            <div className="qt2-topology-store"><Database size={18} /><strong>Fact store</strong><small>typed · versioned · causal</small></div>
          </section>
        )
      })}
    </div>
  )
}

function CollectorTable({ groups }) {
  const collectors = groups.flatMap(([, rows]) => rows)
  if (!collectors.length) return null
  return (
    <div className="qt2-table-wrap">
      <table className="qt2-data-table qt2-collector-operations-table">
        <thead><tr><th>Collector</th><th>State</th><th>Acquisition</th><th>Runtime</th><th>Data quality</th><th /></tr></thead>
        <tbody>{collectors.map((collector) => {
          const vm = buildCollectorCardViewModel(collector)
          return <tr key={vm.key}>
            <td><strong>{vm.displayName}</strong><small>{vm.providerLabel} · {vm.kindLabel}</small><small className="qt-mono">{collector.collector_id}</small></td>
            <td><StateBadge state={vm.state} /><small>desired {collector.desired_state} · configured {collector.configured_state}</small></td>
            <td><strong>{vm.throughputLabel}</strong><small>{vm.lastAcceptedLabel}</small><small>{vm.freshnessLabel}</small></td>
            <td><strong>{collector.worker?.alive ? 'Worker alive' : 'Worker unavailable'}</strong><small>{vm.heartbeatLabel}</small><small>{formatMetric(collector.runtime?.restart_count)} restarts</small></td>
            <td><strong>{formatMetric(collector.throughput?.rejected_recent)} rejects</strong><small>{collector.gap?.active_count == null ? 'Gap evidence inspectable' : `${collector.gap.active_count} active gaps`}</small><small>{collector.error?.message || 'No active error'}</small></td>
            <td><Link className="qt2-button" to={vm.route} state={{ from: '/operations?tab=market' }}>Inspect</Link></td>
          </tr>
        })}</tbody>
      </table>
    </div>
  )
}

export function CollectorFleetConsole({ feed, query = '' }) {
  const groups = providerGroups(feed.collectors, query)
  const workerFleet = feed.workerFleet || {}
  return (
    <div className="qt2-collector-console">
      <PlaneMetrics feed={feed} />

      <section className="qt2-plane-banner">
        <div><Server size={18} /><span><strong>{formatMetric(workerFleet.alive_count)} live workers</strong><small>{formatMetric(workerFleet.known_count)} known · supervisor {workerFleet.continuous_supervisor_state || 'unavailable'}</small></span></div>
        {workerFleet.split_ownership_risk ? <span className="is-warning"><AlertTriangle size={15} />Split ownership risk detected</span> : <span>Ownership projection coherent</span>}
        {feed.unregisteredDefinitions?.length ? <span className="is-warning"><AlertTriangle size={15} />{feed.unregisteredDefinitions.length} durable definitions are not in the code-owned operational registry</span> : null}
        <span>Observed {feed.observedAt ? new Date(feed.observedAt).toLocaleTimeString() : 'unavailable'}</span>
      </section>

      <section aria-labelledby="collector-flow-heading">
        <div className="qt2-inventory-heading"><div><h2 id="collector-flow-heading">Live collector flow</h2><p>Provider acquisition, canonical schemas, throughput, freshness, and durable storage.</p></div><span>{groups.reduce((sum, [, rows]) => sum + rows.length, 0)}</span></div>
        <Topology groups={groups} />
      </section>

      <section aria-labelledby="collector-list-heading">
        <div className="qt2-inventory-heading"><div><h2 id="collector-list-heading">Precise fleet inventory</h2><p>Backend-issued lifecycle and telemetry. No state is inferred in the browser.</p></div></div>
        <CollectorTable groups={groups} />
      </section>
    </div>
  )
}
