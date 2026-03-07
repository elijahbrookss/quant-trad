import React, { useMemo, useState, useCallback, useEffect } from 'react';
import PropTypes from 'prop-types';
import { ChevronLeft, ChevronRight, Copy, X } from 'lucide-react';
import './DecisionTable.css';

const PAGE_SIZE = 25;
const METRIC_LIMIT = 5;

const EVENT_BADGES = {
  signal: { label: 'SIGNAL', color: 'bg-sky-500/15 text-sky-400 border-sky-500/30' },
  decision: { label: 'DECISION', color: 'bg-amber-500/15 text-amber-400 border-amber-500/30' },
  execution: { label: 'EXEC', color: 'bg-emerald-500/15 text-emerald-400 border-emerald-500/30' },
  outcome: { label: 'OUTCOME', color: 'bg-violet-500/15 text-violet-400 border-violet-500/30' },
  wallet: { label: 'WALLET', color: 'bg-cyan-500/15 text-cyan-400 border-cyan-500/30' },
  runtime: { label: 'RUNTIME', color: 'bg-rose-500/15 text-rose-400 border-rose-500/30' },
};

const formatTimeWithDate = (value) => {
  if (!value) return '—';
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return String(value);
  const month = date.toLocaleDateString('en-US', { month: 'short' });
  const day = date.getDate().toString().padStart(2, '0');
  const time = date.toLocaleTimeString('en-US', {
    hour: '2-digit',
    minute: '2-digit',
    hour12: false,
  });
  return `${month} ${day} ${time}`;
};

const formatNumber = (value, digits = 2) => {
  const num = Number(value);
  if (!Number.isFinite(num)) return null;
  return num.toFixed(digits);
};

const formatSigned = (value, digits = 2) => {
  const formatted = formatNumber(value, digits);
  if (formatted === null) return null;
  return Number(value) >= 0 ? `+${formatted}` : formatted;
};

const formatSide = (side) => {
  if (!side) return '—';
  const normalized = String(side).toLowerCase();
  if (normalized === 'long' || normalized === 'buy') return 'LONG';
  if (normalized === 'short' || normalized === 'sell') return 'SHORT';
  return side.toUpperCase();
};

const formatValue = (value) => {
  if (value === undefined || value === null) return '—';
  if (typeof value === 'number') return formatNumber(value, 4) || String(value);
  if (typeof value === 'string') return value;
  return JSON.stringify(value);
};

const toFiniteNumber = (value) => {
  const num = Number(value);
  return Number.isFinite(num) ? num : null;
};

const feeFromEvent = (event) => {
  if (!event || typeof event !== 'object') return null;
  const direct = toFiniteNumber(event.fee_paid);
  if (direct !== null) return direct;
  const payload = event.payload && typeof event.payload === 'object' ? event.payload : null;
  const payloadFee = toFiniteNumber(payload?.fee_paid);
  if (payloadFee !== null) return payloadFee;
  const walletDelta = payload?.wallet_delta && typeof payload.wallet_delta === 'object' ? payload.wallet_delta : null;
  return toFiniteNumber(walletDelta?.fee_paid);
};

const aggregateMetric = (events, getter) => {
  let total = 0;
  let seen = 0;
  events.forEach((event) => {
    const value = getter(event);
    if (value === null) return;
    total += value;
    seen += 1;
  });
  return { total, hasValue: seen > 0 };
};

const getEventBadge = (eventType) => {
  return EVENT_BADGES[eventType] || { label: 'EVENT', color: 'bg-slate-500/15 text-slate-400 border-slate-500/30' };
};

const actionFromEvent = (event) => {
  const subtype = String(event.event_subtype || '').toLowerCase();
  if (subtype.includes('accepted')) return 'accept';
  if (subtype.includes('rejected')) return 'reject';
  if (['entry', 'open', 'fill'].includes(subtype)) return 'entry';
  if (['stop', 'sl'].includes(subtype)) return 'stop';
  if (['target', 'tp'].includes(subtype)) return 'tp';
  if (['close', 'exit'].includes(subtype)) return 'close';
  if (event.event_type === 'signal') return 'signal';
  if (event.event_type === 'wallet') return subtype || 'wallet';
  if (event.event_type === 'runtime') return subtype || 'runtime';
  return subtype || '—';
};

const buildClusterSummary = (events) => {
  const counts = events.reduce(
    (acc, event) => {
      const type = String(event.event_type || 'event').toUpperCase();
      acc[type] = (acc[type] || 0) + 1;
      return acc;
    },
    {}
  );
  const parts = Object.entries(counts)
    .sort((a, b) => a[0].localeCompare(b[0]))
    .map(([type, count]) => `${count} ${type}`);
  return `Cluster: ${events.length} events (${parts.join(', ')})`;
};

const duplicateKeyForEvent = (event) => {
  const action = actionFromEvent(event);
  const ts = event.event_ts || event.created_at || '';
  const side = event.side || '';
  const price = event.price ?? '';
  return `${ts}|${event.event_type}|${action}|${side}|${price}`;
};

const buildDuplicateRows = (events, groupId, expandedDuplicates) => {
  const rows = [];
  const execGroups = new Map();
  const nonExec = [];
  events.forEach((event) => {
    if (event.event_type === 'execution') {
      const key = duplicateKeyForEvent(event);
      if (!execGroups.has(key)) {
        execGroups.set(key, []);
      }
      execGroups.get(key).push(event);
    } else {
      nonExec.push({ kind: 'event', event, groupId });
    }
  });

  execGroups.forEach((groupEvents, key) => {
    if (groupEvents.length === 1) {
      rows.push({ kind: 'event', event: groupEvents[0], groupId });
      return;
    }
    rows.push({
      kind: 'dup',
      groupId,
      dupKey: key,
      count: groupEvents.length,
      event: groupEvents[0],
      children: groupEvents,
      expanded: expandedDuplicates.has(key),
    });
    if (expandedDuplicates.has(key)) {
      groupEvents.forEach((child) => rows.push({ kind: 'event', event: child, groupId, duplicate: true }));
    }
  });

  return [...nonExec, ...rows].sort((a, b) => {
    const left = new Date((a.event?.created_at || a.event?.event_ts || 0)).getTime();
    const right = new Date((b.event?.created_at || b.event?.event_ts || 0)).getTime();
    return left - right;
  });
};

const buildKeyMetrics = (aiContext) => {
  if (!aiContext || typeof aiContext !== 'object') return [];
  const keys = Object.keys(aiContext).sort();
  return keys.slice(0, METRIC_LIMIT).map((key) => ({ key, value: aiContext[key] }));
};

const buildBreadcrumb = (event, eventIndex) => {
  const chain = [];
  let current = event;
  const seen = new Set();
  while (current && current.event_id && !seen.has(current.event_id)) {
    chain.unshift(current);
    seen.add(current.event_id);
    const parentId = current.parent_event_id;
    if (!parentId) break;
    current = eventIndex.get(parentId);
  }
  return chain;
};

const sortEvents = (events, order) => {
  const sorted = events.slice().sort((a, b) => {
    const left = new Date(a.created_at || a.event_ts || 0).getTime();
    const right = new Date(b.created_at || b.event_ts || 0).getTime();
    return left - right;
  });
  return order === 'asc' ? sorted : sorted.reverse();
};

const filterEvents = (events, { eventType, reasonCode, tradeId }) => {
  return events.filter((event) => {
    if (eventType !== 'all' && event.event_type !== eventType) return false;
    if (reasonCode !== 'all' && event.reason_code !== reasonCode) return false;
    if (tradeId && !(event.trade_id || '').toLowerCase().includes(tradeId.toLowerCase())) return false;
    return true;
  });
};

function InspectModal({ selection, eventIndex, onSelectEvent, onClose }) {
  if (!selection) {
    return null;
  }

  const event = selection.event;
  const breadcrumb = buildBreadcrumb(event, eventIndex);
  const reasonCode = event.reason_code || null;
  const reasonDetail = event.reason_detail || null;
  const evidenceRefs = Array.isArray(event.evidence_refs) ? event.evidence_refs : [];
  const aiContext = event.context || null;
  const keyMetrics = buildKeyMetrics(aiContext);

  return (
    <div className="decision-inspect-backdrop" onClick={onClose}>
      <div
        className="decision-inspect"
        onClick={(eventClick) => eventClick.stopPropagation()}
      >
        <div className="decision-inspect-header">
          <div>
            <p className="decision-explain-title">Inspect</p>
            <p className="decision-explain-muted">Deterministic ledger facts</p>
          </div>
          <button type="button" className="inspect-close" onClick={onClose} aria-label="Close inspect">
            <X className="size-4" />
          </button>
        </div>

        <div className="decision-explain-section">
          <p className="decision-explain-label">Event Summary</p>
          <div className="decision-explain-grid two-col">
            <div>
              <p className="decision-explain-key">created_at</p>
              <p className="decision-explain-id">{event.created_at || event.event_ts || '—'}</p>
            </div>
            <div>
              <p className="decision-explain-key">symbol</p>
              <p className="decision-explain-id">{event.symbol || '—'}</p>
            </div>
            <div>
              <p className="decision-explain-key">action + side</p>
              <p className="decision-explain-id">{actionFromEvent(event)} · {formatSide(event.side)}</p>
            </div>
            <div>
              <p className="decision-explain-key">size / price</p>
              <p className="decision-explain-id">{formatNumber(event.qty, 4) || '—'} / {formatNumber(event.price, 4) || '—'}</p>
            </div>
            <div>
              <p className="decision-explain-key">event_impact_pnl</p>
              <p className="decision-explain-id">{formatSigned(event.event_impact_pnl, 2) || '—'}</p>
            </div>
            <div>
              <p className="decision-explain-key">trade_net_pnl</p>
              <p className="decision-explain-id">{formatSigned(event.trade_net_pnl, 2) || '—'}</p>
            </div>
            <div>
              <p className="decision-explain-key">fees</p>
              <p className="decision-explain-id">{formatNumber(feeFromEvent(event), 4) || '—'}</p>
            </div>
            <div>
              <p className="decision-explain-key">instrument_id</p>
              <div className="identifier-value">
                <span className="decision-explain-id">{event.instrument_id || '—'}</span>
                {event.instrument_id && (
                  <button
                    type="button"
                    className="copy-button"
                    onClick={() => navigator.clipboard?.writeText(String(event.instrument_id))}
                    aria-label="Copy instrument id"
                  >
                    <Copy className="size-3" />
                  </button>
                )}
              </div>
            </div>
          </div>
        </div>

        <div className="decision-explain-section">
          <p className="decision-explain-label">Cause Chain</p>
          {breadcrumb.length ? (
            <div className="decision-explain-chain">
              {breadcrumb.map((node, idx) => {
                const label = EVENT_BADGES[node.event_type]?.label || String(node.event_type || 'EVENT').toUpperCase();
                return (
                  <button
                    key={node.event_id}
                    type="button"
                    className="chain-pill"
                    onClick={() => onSelectEvent(node.event_id)}
                  >
                    <span>{label}</span>
                    {idx < breadcrumb.length - 1 && <ChevronRight className="size-3" />}
                  </button>
                );
              })}
            </div>
          ) : (
            <p className="decision-explain-muted">No parent chain recorded.</p>
          )}
        </div>

        <div className="decision-explain-section">
          <p className="decision-explain-label">Reason</p>
          {reasonCode ? (
            <div className="decision-explain-reason">
              <p className="decision-explain-value">{reasonCode}</p>
              {reasonDetail && <p className="decision-explain-sub">{reasonDetail}</p>}
            </div>
          ) : (
            <p className="decision-explain-muted">Reason not recorded (instrumentation incomplete).</p>
          )}
        </div>

        <div className="decision-explain-section">
          <p className="decision-explain-label">Evidence</p>
          {evidenceRefs.length ? (
            <div className="decision-evidence-list">
              {evidenceRefs.map((ref, idx) => {
                const refType = (ref.ref_type || 'unknown').toString().toUpperCase();
                const refId = ref.ref_id ? String(ref.ref_id) : '—';
                const summary = ref.summary ? String(ref.summary) : '—';
                return (
                  <div key={`${ref.ref_id || idx}`} className="evidence-row">
                    <span className="evidence-pill">{refType}</span>
                    <span className="evidence-id">{refId}</span>
                    <span className="evidence-summary">{summary}</span>
                  </div>
                );
              })}
            </div>
          ) : (
            <p className="decision-explain-muted">No evidence captured.</p>
          )}
        </div>

        <div className="decision-explain-section">
          <p className="decision-explain-label">Key Metrics</p>
          {keyMetrics.length ? (
            <div className="decision-explain-metrics">
              {keyMetrics.map((metric) => (
                <div key={metric.key} className="metric-row">
                  <span className="metric-key">{metric.key}</span>
                  <span className="metric-value">{formatValue(metric.value)}</span>
                </div>
              ))}
              <details className="decision-explain-raw">
                <summary>View raw context</summary>
                <pre className="decision-explain-json">{JSON.stringify(aiContext, null, 2)}</pre>
              </details>
            </div>
          ) : (
            <p className="decision-explain-muted">No metrics recorded.</p>
          )}
        </div>

        <div className="decision-explain-section">
          <p className="decision-explain-label">Payload</p>
          {event.payload && typeof event.payload === 'object' ? (
            <details className="decision-explain-raw">
              <summary>View event payload</summary>
              <pre className="decision-explain-json">{JSON.stringify(event.payload, null, 2)}</pre>
            </details>
          ) : (
            <p className="decision-explain-muted">No payload captured.</p>
          )}
        </div>

        <div className="decision-explain-section">
          <p className="decision-explain-label">Identifiers</p>
          <div className="decision-explain-grid two-col">
            {[
              ['event_id', event.event_id],
              ['parent_event_id', event.parent_event_id],
              ['trade_id', event.trade_id],
              ['run_id', event.run_id],
              ['bot_id', event.bot_id],
            ].map(([label, value]) => (
              <div key={label} className="identifier-row">
                <p className="decision-explain-key">{label}</p>
                <div className="identifier-value">
                  <span className="decision-explain-id">{value || '—'}</span>
                  {value && (
                    <button
                      type="button"
                      className="copy-button"
                      onClick={() => navigator.clipboard?.writeText(String(value))}
                      aria-label={`Copy ${label}`}
                    >
                      <Copy className="size-3" />
                    </button>
                  )}
                </div>
              </div>
            ))}
          </div>
        </div>
      </div>
    </div>
  );
}

InspectModal.propTypes = {
  selection: PropTypes.shape({
    kind: PropTypes.string.isRequired,
    event: PropTypes.object.isRequired,
    children: PropTypes.array,
  }),
  eventIndex: PropTypes.instanceOf(Map).isRequired,
  onSelectEvent: PropTypes.func.isRequired,
  onClose: PropTypes.func.isRequired,
};

export default function DecisionTable({ ledgerEvents, onRowClick }) {
  const [selectedRow, setSelectedRow] = useState(null);
  const [expandedGroups, setExpandedGroups] = useState(new Set());
  const [expandedDuplicates, setExpandedDuplicates] = useState(new Set());
  const [page, setPage] = useState(0);
  const [eventTypeFilter, setEventTypeFilter] = useState('all');
  const [reasonCodeFilter, setReasonCodeFilter] = useState('all');
  const [tradeIdFilter, setTradeIdFilter] = useState('');
  const [sortOrder, setSortOrder] = useState('desc');

  const eventIndex = useMemo(() => {
    const index = new Map();
    ledgerEvents.forEach((event) => {
      if (event.event_id) index.set(event.event_id, event);
    });
    return index;
  }, [ledgerEvents]);

  const reasonCodes = useMemo(() => {
    const codes = new Set();
    ledgerEvents.forEach((event) => {
      if (event.reason_code) codes.add(event.reason_code);
    });
    return Array.from(codes).sort();
  }, [ledgerEvents]);

  const groupedRows = useMemo(() => {
    const filtered = filterEvents(ledgerEvents, {
      eventType: eventTypeFilter,
      reasonCode: reasonCodeFilter,
      tradeId: tradeIdFilter,
    });

    const grouped = new Map();
    const singles = [];
    filtered.forEach((event) => {
      if (!event.trade_id) {
        singles.push({ kind: 'event', event });
        return;
      }
      if (!grouped.has(event.trade_id)) {
        grouped.set(event.trade_id, []);
      }
      grouped.get(event.trade_id).push(event);
    });

    const groupEntries = Array.from(grouped.entries()).map(([tradeId, events]) => {
      const sorted = sortEvents(events, 'asc');
      const symbol = sorted.find((item) => item.symbol)?.symbol || null;
      const createdAt = sortOrder === 'asc'
        ? (sorted[0]?.created_at || sorted[0]?.event_ts || null)
        : (sorted[sorted.length - 1]?.created_at || sorted[sorted.length - 1]?.event_ts || null);
      const impactMetric = aggregateMetric(sorted, (item) => toFiniteNumber(item.event_impact_pnl));
      const netMetric = aggregateMetric(sorted, (item) => toFiniteNumber(item.trade_net_pnl));
      const feeMetric = aggregateMetric(sorted, feeFromEvent);
      return {
        kind: 'group',
        groupId: tradeId,
        tradeId,
        created_at: createdAt,
        symbol,
        children: sorted,
        impactTotal: impactMetric.total,
        netTotal: netMetric.total,
        feeTotal: feeMetric.total,
        hasImpactTotal: impactMetric.hasValue,
        hasNetTotal: netMetric.hasValue,
        hasFeeTotal: feeMetric.hasValue,
      };
    });

    const allEntries = [...groupEntries, ...singles];
    return allEntries.sort((a, b) => {
      const left = new Date((a.event?.created_at || a.event?.event_ts || a.created_at || 0)).getTime();
      const right = new Date((b.event?.created_at || b.event?.event_ts || b.created_at || 0)).getTime();
      return sortOrder === 'asc' ? left - right : right - left;
    });
  }, [ledgerEvents, eventTypeFilter, reasonCodeFilter, tradeIdFilter, sortOrder]);

  const visibleRows = useMemo(() => {
    const filteringActive = eventTypeFilter !== 'all' || reasonCodeFilter !== 'all' || tradeIdFilter.trim() !== '';
    const filtered = [];
    groupedRows.forEach((row) => {
      if (row.kind === 'group') {
        if (filteringActive) {
          const dupRows = buildDuplicateRows(row.children, row.groupId, expandedDuplicates);
          filtered.push(...dupRows);
          return;
        }
        filtered.push(row);
        if (expandedGroups.has(row.groupId)) {
          const dupRows = buildDuplicateRows(row.children, row.groupId, expandedDuplicates);
          filtered.push(...dupRows);
        }
        return;
      }
      filtered.push(row);
    });
    return filtered;
  }, [
    groupedRows,
    expandedGroups,
    expandedDuplicates,
    eventTypeFilter,
    reasonCodeFilter,
    tradeIdFilter,
  ]);

  const pageCount = Math.max(1, Math.ceil(visibleRows.length / PAGE_SIZE));
  const pageStart = page * PAGE_SIZE;
  const pageEnd = pageStart + PAGE_SIZE;
  const currentRows = visibleRows.slice(pageStart, pageEnd);

  useEffect(() => {
    setPage(0);
  }, [ledgerEvents, eventTypeFilter, reasonCodeFilter, tradeIdFilter, sortOrder]);

  const toggleGroup = useCallback((groupId) => {
    setExpandedGroups((prev) => {
      const next = new Set(prev);
      if (next.has(groupId)) {
        next.delete(groupId);
      } else {
        next.add(groupId);
      }
      return next;
    });
  }, []);

  const toggleDuplicate = useCallback((dupKey) => {
    setExpandedDuplicates((prev) => {
      const next = new Set(prev);
      if (next.has(dupKey)) {
        next.delete(dupKey);
      } else {
        next.add(dupKey);
      }
      return next;
    });
  }, []);

  const handleRowClick = useCallback((row) => {
    if (row.kind !== 'event') return;
    setSelectedRow(row);
  }, []);

  const handleRowDoubleClick = useCallback((row) => {
    if (row.kind !== 'event') return;
    const event = row.event;
    onRowClick?.(event.event_ts, event.price, event.symbol);
  }, [onRowClick]);

  const handleSelectEventId = useCallback((eventId) => {
    const event = eventIndex.get(eventId);
    if (!event) return;
    setSelectedRow({ kind: 'event', event });
  }, [eventIndex]);

  const renderGroupRow = (row, idx) => {
    const isExpanded = expandedGroups.has(row.groupId);
    const summary = buildClusterSummary(row.children);
    const impactText = row.hasImpactTotal ? formatSigned(row.impactTotal, 2) : null;
    const netText = row.hasNetTotal ? formatSigned(row.netTotal, 2) : null;
    const feeText = row.hasFeeTotal ? formatNumber(row.feeTotal, 4) : null;
    return (
      <tr key={`group-${row.groupId}-${idx}`} className="group-row">
        <td className="tabular-nums">
          <button
            type="button"
            className="group-toggle"
            onClick={() => toggleGroup(row.groupId)}
            aria-label={isExpanded ? 'Collapse trade group' : 'Expand trade group'}
          >
            <span className={`group-icon ${isExpanded ? 'expanded' : ''}`}>
              <ChevronRight className="size-3" />
            </span>
          </button>
          {row.created_at ? formatTimeWithDate(row.created_at) : '—'}
        </td>
        <td className="truncate">{row.symbol || '—'}</td>
        <td className="uppercase">CLUSTER</td>
        <td className="cluster-summary" title={summary}>{summary}</td>
        <td className="tabular-nums">—</td>
        <td className="tabular-nums">—</td>
        <td className="tabular-nums">—</td>
        <td className={`tabular-nums ${impactText ? (Number(row.impactTotal) >= 0 ? 'pnl-positive' : 'pnl-negative') : ''}`}>
          {impactText || '—'}
        </td>
        <td className={`tabular-nums ${netText ? (Number(row.netTotal) >= 0 ? 'pnl-positive' : 'pnl-negative') : ''}`}>
          {netText || '—'}
        </td>
        <td className="tabular-nums">{feeText || '—'}</td>
        <td className="tabular-nums">—</td>
      </tr>
    );
  };

  const renderDuplicateRow = (row, idx) => {
    const event = row.event;
    const badge = getEventBadge(event.event_type);
    const createdAt = event.created_at || event.event_ts || null;
    const action = actionFromEvent(event).toUpperCase();
    const feeText = formatNumber(feeFromEvent(event), 4);
    return (
      <tr key={`dup-${row.dupKey}-${idx}`} className="dup-row">
        <td className="tabular-nums">
          <button
            type="button"
            className="group-toggle"
            onClick={() => toggleDuplicate(row.dupKey)}
            aria-label={row.expanded ? 'Collapse duplicates' : 'Expand duplicates'}
          >
            <span className={`group-icon ${row.expanded ? 'expanded' : ''}`}>
              <ChevronRight className="size-3" />
            </span>
          </button>
          {createdAt ? formatTimeWithDate(createdAt) : '—'}
        </td>
        <td className="truncate">{event.symbol || '—'}</td>
        <td>
          <span className={`decision-type-badge ${badge.color}`}>
            {badge.label}
          </span>
        </td>
        <td className="uppercase">
          {action} <span className="count-badge">x{row.count}</span>
        </td>
        <td className="tabular-nums">{formatSide(event.side)}</td>
        <td className="tabular-nums">—</td>
        <td className="tabular-nums">{formatNumber(event.price, 4) || '—'}</td>
        <td className="tabular-nums">—</td>
        <td className="tabular-nums">—</td>
        <td className="tabular-nums">{feeText || '—'}</td>
        <td className="tabular-nums">{event.reason_code || '—'}</td>
      </tr>
    );
  };

  const renderEventRow = (row, idx) => {
    const event = row.event;
    const badge = getEventBadge(event.event_type);
    const impactPnl = formatSigned(event.event_impact_pnl, 2);
    const netPnl = formatSigned(event.trade_net_pnl, 2);
    const feeText = formatNumber(feeFromEvent(event), 4);
    const createdAt = event.created_at || event.event_ts || null;
    return (
      <tr
        key={`${event.event_id || event.event_ts}-${idx}`}
        className={`${row.groupId ? 'child-row' : ''} ${row.duplicate ? 'dup-child' : ''} event-row`}
        onClick={() => handleRowClick(row)}
        onDoubleClick={() => handleRowDoubleClick(row)}
      >
        <td className="tabular-nums">{createdAt ? formatTimeWithDate(createdAt) : '—'}</td>
        <td className="truncate">{event.symbol || '—'}</td>
        <td>
          <span className={`decision-type-badge ${badge.color}`}>
            {badge.label}
          </span>
        </td>
        <td className="uppercase">{actionFromEvent(event)}</td>
        <td className="tabular-nums">{formatSide(event.side)}</td>
        <td className="tabular-nums">{formatNumber(event.qty, 4) || '—'}</td>
        <td className="tabular-nums">{formatNumber(event.price, 4) || '—'}</td>
        <td className={`tabular-nums ${impactPnl ? (Number(event.event_impact_pnl) >= 0 ? 'pnl-positive' : 'pnl-negative') : ''}`}>
          {impactPnl || '—'}
        </td>
        <td className={`tabular-nums ${netPnl ? (Number(event.trade_net_pnl) >= 0 ? 'pnl-positive' : 'pnl-negative') : ''}`}>
          {netPnl || '—'}
        </td>
        <td className="tabular-nums">{feeText || '—'}</td>
        <td className="tabular-nums">{event.reason_code || '—'}</td>
      </tr>
    );
  };

  return (
    <div className="decision-ledger">
      <div className="decision-ledger-header">
        <div>
          <p className="decision-ledger-kicker">Decision Ledger</p>
          <p className="decision-ledger-subtitle">Inspect deterministic events (no narrative)</p>
        </div>
        <div className="decision-ledger-controls">
          <div className="decision-ledger-filter">
            <label htmlFor="ledger-type">event_type</label>
            <select id="ledger-type" value={eventTypeFilter} onChange={(event) => setEventTypeFilter(event.target.value)}>
              <option value="all">All</option>
              <option value="signal">SIGNAL</option>
              <option value="decision">DECISION</option>
              <option value="execution">EXEC</option>
              <option value="outcome">OUTCOME</option>
              <option value="wallet">WALLET</option>
              <option value="runtime">RUNTIME</option>
            </select>
          </div>
          <div className="decision-ledger-filter">
            <label htmlFor="ledger-reason">reason_code</label>
            <select id="ledger-reason" value={reasonCodeFilter} onChange={(event) => setReasonCodeFilter(event.target.value)}>
              <option value="all">All</option>
              {reasonCodes.map((code) => (
                <option key={code} value={code}>{code}</option>
              ))}
            </select>
          </div>
          <div className="decision-ledger-filter">
            <label htmlFor="ledger-trade">trade_id</label>
            <input
              id="ledger-trade"
              type="text"
              value={tradeIdFilter}
              onChange={(event) => setTradeIdFilter(event.target.value)}
              placeholder="trade_id"
            />
          </div>
          <div className="decision-ledger-filter">
            <label htmlFor="ledger-sort">sort</label>
            <select id="ledger-sort" value={sortOrder} onChange={(event) => setSortOrder(event.target.value)}>
              <option value="desc">Newest</option>
              <option value="asc">Oldest</option>
            </select>
          </div>
        </div>
        <div className="decision-ledger-pagination">
          <button
            type="button"
            className="decision-table-nav"
            onClick={() => setPage((prev) => Math.max(prev - 1, 0))}
            disabled={page <= 0}
          >
            <ChevronLeft className="size-4" />
          </button>
          <span className="decision-table-page">
            {page + 1} / {pageCount}
          </span>
          <button
            type="button"
            className="decision-table-nav"
            onClick={() => setPage((prev) => Math.min(prev + 1, pageCount - 1))}
            disabled={page >= pageCount - 1}
          >
            <ChevronRight className="size-4" />
          </button>
        </div>
      </div>

      <div className="decision-ledger-body">
        <div className="decision-ledger-table">
          <div className="decision-ledger-scroll">
            <table>
              <thead>
                <tr>
                  <th style={{ width: '150px' }}>created_at</th>
                  <th style={{ width: '110px' }}>symbol</th>
                  <th style={{ width: '90px' }}>event_type</th>
                  <th style={{ width: '90px' }}>action</th>
                  <th style={{ width: '70px' }}>side</th>
                  <th style={{ width: '70px' }}>size</th>
                  <th style={{ width: '90px' }}>price</th>
                  <th style={{ width: '120px' }}>event_impact_pnl</th>
                  <th style={{ width: '120px' }}>trade_net_pnl</th>
                  <th style={{ width: '90px' }}>fees</th>
                  <th>reason_code</th>
                </tr>
              </thead>
              <tbody>
                {currentRows.length ? (
                  currentRows.map((row, idx) => (
                    row.kind === 'group'
                      ? renderGroupRow(row, idx)
                      : row.kind === 'dup'
                        ? renderDuplicateRow(row, idx)
                        : renderEventRow(row, idx)
                  ))
                ) : (
                  <tr>
                    <td colSpan={11} className="decision-table-empty">
                      No ledger events yet.
                    </td>
                  </tr>
                )}
              </tbody>
            </table>
          </div>
        </div>
        <InspectModal
          selection={selectedRow}
          eventIndex={eventIndex}
          onSelectEvent={handleSelectEventId}
          onClose={() => setSelectedRow(null)}
        />
      </div>
    </div>
  );
}

DecisionTable.propTypes = {
  ledgerEvents: PropTypes.arrayOf(PropTypes.object).isRequired,
  onRowClick: PropTypes.func,
};
