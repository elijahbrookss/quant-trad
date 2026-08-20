import { useEffect, useMemo, useState } from 'react'
import {
  AlertTriangle,
  ChevronDown,
  ChevronRight,
  Radio,
  Rows3,
  Server,
} from 'lucide-react'
import { Link } from 'react-router-dom'
import { buildCollectorCardViewModel } from './buildCollectorCardViewModel.js'
import { useCollectorPage } from './useCollectorsFeed.js'

const PAGE_SIZE = 50

function formatMetric(value, suffix = '') {
  if (value === null || value === undefined || value === '') return '—'
  return Number.isFinite(Number(value))
    ? `${Number(value).toLocaleString()}${suffix}`
    : '—'
}

function healthTone(value) {
  if (value === 'HEALTHY') return 'success'
  if (value === 'FAILED') return 'danger'
  if (value === 'DELAYED') return 'warning'
  return 'neutral'
}

function HealthBadge({ value }) {
  if (!value || value === 'NOT_APPLICABLE') return <span className="qt2-health-na">—</span>
  return (
    <span className={'qt2-evidence-state is-' + healthTone(value)}>
      {value}
    </span>
  )
}

function providerCounts(provider) {
  const states = provider.operational_state_counts || {}
  return [
    states.RUNNING ? `${states.RUNNING} running` : null,
    states.STOPPED ? `${states.STOPPED} stopped` : null,
    states.PAUSED ? `${states.PAUSED} paused` : null,
    states.DISABLED ? `${states.DISABLED} disabled` : null,
    states.STOPPING ? `${states.STOPPING} stopping` : null,
  ].filter(Boolean).join(' · ') || 'No registered collectors'
}

function providerFreshness(provider) {
  const value = provider.freshness_seconds
  if (value !== null && value !== undefined && value !== '' && Number.isFinite(Number(value))) {
    return `${Math.round(Number(value))}s`
  }
  if (!Number(provider.operational_state_counts?.RUNNING || 0)) return '—'
  return 'Unknown'
}

function FleetMetrics({ feed }) {
  const fleet = feed.fleet || {}
  const metrics = [
    ['Providers', feed.providers.length, 'Code-owned integrations', ''],
    ['Registered', fleet.collector_count, 'Across all lifecycle states', ''],
    ['Ingestion', fleet.accepted_last_minute, 'Accepted canonical Facts', '/min'],
    ['Needs attention', fleet.attention_count, 'Actionable exceptions only', ''],
  ]
  return (
    <div className="qt2-plane-metrics">
      {metrics.map(([label, value, detail, suffix]) => (
        <article key={label}>
          <span>{label}</span>
          <strong>{formatMetric(value, suffix)}</strong>
          <small>{detail}</small>
        </article>
      ))}
    </div>
  )
}

function CollectorRows({ pageFeed, from }) {
  if (pageFeed.loading && !pageFeed.collectors.length) {
    return <div className="qt2-provider-loading"><span className="qt2-skeleton" /><span className="qt2-skeleton" /><span className="qt2-skeleton" /></div>
  }
  if (pageFeed.error) return <div className="qt2-empty">{pageFeed.error}</div>
  if (!pageFeed.collectors.length) return <div className="qt2-empty">No collectors match this view.</div>

  return (
    <div className="qt2-collector-rows">
      {pageFeed.collectors.map((collector) => {
        const vm = buildCollectorCardViewModel(collector)
        const issue = collector.needs_attention
          ? String(collector.attention_reason || 'Needs attention').replaceAll('_', ' ')
          : null
        return (
          <Link className="qt2-collector-row" key={vm.key} to={vm.route} state={{ from }}>
            <span className="qt2-collector-row-name">
              <strong>{vm.displayName}</strong>
              <small>{vm.kindLabel} · {collector.fact_schemas?.[0]?.fact_type || 'Fact schema unavailable'}</small>
            </span>
            <span><small>State</small><strong>{vm.state}</strong></span>
            <span><small>Health</small><HealthBadge value={vm.health} /></span>
            <span><small>Rate</small><strong>{vm.throughputLabel}</strong></span>
            <span><small>{vm.state === 'RUNNING' ? 'Freshness' : 'History'}</small><strong>{vm.freshnessLabel}</strong></span>
            <span className={issue ? 'qt2-collector-row-issue is-visible' : 'qt2-collector-row-issue'}>
              {issue ? <><AlertTriangle size={13} />{issue}</> : null}
            </span>
            <ChevronRight size={15} aria-hidden="true" />
          </Link>
        )
      })}
    </div>
  )
}

function PageFooter({ pageFeed, offset, onOffset }) {
  if (!pageFeed.total) return null
  const end = Math.min(offset + PAGE_SIZE, pageFeed.total)
  return (
    <div className="qt2-collector-page-footer">
      <span>{offset + 1}–{end} of {pageFeed.total}</span>
      <div>
        <button type="button" disabled={offset === 0} onClick={() => onOffset(Math.max(0, offset - PAGE_SIZE))}>Previous</button>
        <button type="button" disabled={end >= pageFeed.total} onClick={() => onOffset(offset + PAGE_SIZE)}>Next</button>
      </div>
    </div>
  )
}

export function CollectorFleetConsole({ feed, query = '' }) {
  const [view, setView] = useState('providers')
  const [expandedProvider, setExpandedProvider] = useState(null)
  const [attentionOnly, setAttentionOnly] = useState(false)
  const [offset, setOffset] = useState(0)

  useEffect(() => {
    if (query.trim()) setView('all')
    setOffset(0)
  }, [query])

  useEffect(() => setOffset(0), [attentionOnly, expandedProvider, view])

  const filteredProviders = useMemo(() => {
    if (!attentionOnly) return feed.providers
    return feed.providers.filter((provider) => Number(provider.attention_count || 0) > 0)
  }, [attentionOnly, feed.providers])

  const pageFeed = useCollectorPage({
    provider: view === 'providers' ? expandedProvider : null,
    query: view === 'all' ? query : '',
    attentionOnly,
    offset,
    limit: PAGE_SIZE,
    enabled: view === 'all' || Boolean(expandedProvider),
  })
  const workerFleet = feed.workerFleet || {}

  return (
    <div className="qt2-collector-console">
      <FleetMetrics feed={feed} />

      <section className="qt2-plane-banner">
        <div>
          <Server size={18} />
          <span>
            <strong>{formatMetric(workerFleet.alive_count)} live workers</strong>
            <small>{formatMetric(workerFleet.known_count)} known · supervisor {workerFleet.continuous_supervisor_state || 'unavailable'}</small>
          </span>
        </div>
        {workerFleet.split_ownership_risk
          ? <span className="is-warning"><AlertTriangle size={15} />Split ownership risk</span>
          : <span>Ownership coherent</span>}
        <span>Summary updates only on material change</span>
      </section>

      <section className="qt2-provider-fleet" aria-labelledby="collector-fleet-heading">
        <div className="qt2-inventory-heading">
          <div>
            <h2 id="collector-fleet-heading">Market data</h2>
            <p>Provider first. Collector evidence loads only when opened.</p>
          </div>
          <div className="qt2-collector-view-controls">
            <button type="button" className={view === 'providers' ? 'is-active' : ''} onClick={() => setView('providers')}><Radio size={14} />Providers</button>
            <button type="button" className={view === 'all' ? 'is-active' : ''} onClick={() => setView('all')}><Rows3 size={14} />All collectors</button>
            <label><input type="checkbox" checked={attentionOnly} onChange={(event) => setAttentionOnly(event.target.checked)} />Attention only</label>
          </div>
        </div>

        {view === 'providers' ? (
          <div className="qt2-provider-list">
            {filteredProviders.map((provider) => {
              const expanded = expandedProvider === provider.provider
              return (
                <article className="qt2-provider-group" key={provider.provider}>
                  <button
                    type="button"
                    className="qt2-provider-summary"
                    aria-expanded={expanded}
                    onClick={() => setExpandedProvider(expanded ? null : provider.provider)}
                  >
                    {expanded ? <ChevronDown size={16} /> : <ChevronRight size={16} />}
                    <span className="qt2-provider-name"><strong>{provider.provider}</strong><small>{providerCounts(provider)}</small></span>
                    <HealthBadge value={provider.health_status} />
                    <span><small>Throughput</small><strong>{formatMetric(provider.accepted_last_minute, '/min')}</strong></span>
                    <span><small>Freshness</small><strong>{providerFreshness(provider)}</strong></span>
                    <span><small>Schemas</small><strong>{formatMetric(provider.fact_schema_count)}</strong></span>
                    {provider.attention_count
                      ? <span className="qt2-provider-attention"><AlertTriangle size={13} />{provider.attention_count}</span>
                      : <span className="qt2-provider-clear">Clear</span>}
                  </button>
                  {expanded ? (
                    <div className="qt2-provider-collectors">
                      <CollectorRows pageFeed={pageFeed} from="/operations?tab=market" />
                      <PageFooter pageFeed={pageFeed} offset={offset} onOffset={setOffset} />
                    </div>
                  ) : null}
                </article>
              )
            })}
            {!filteredProviders.length ? <div className="qt2-empty">No providers match this view.</div> : null}
          </div>
        ) : (
          <div className="qt2-all-collectors">
            <CollectorRows pageFeed={pageFeed} from="/operations?tab=market" />
            <PageFooter pageFeed={pageFeed} offset={offset} onOffset={setOffset} />
          </div>
        )}
      </section>
    </div>
  )
}
