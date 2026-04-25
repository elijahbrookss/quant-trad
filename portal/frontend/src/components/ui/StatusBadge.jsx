import { AlertCircle, CheckCircle2, Circle, Clock, Info, XCircle } from 'lucide-react'

const STATUS_META = {
  lifecycle: {
    starting: { label: 'Starting', variant: 'info', icon: Clock },
    running: { label: 'Running', variant: 'success', icon: CheckCircle2 },
    completed: { label: 'Completed', variant: 'success', icon: CheckCircle2 },
    failed: { label: 'Failed', variant: 'danger', icon: XCircle },
    stopped: { label: 'Stopped', variant: 'neutral', icon: Circle },
    cancelled: { label: 'Cancelled', variant: 'neutral', icon: Circle },
    unknown: { label: 'Unknown', variant: 'neutral', icon: Info },
  },
  health: {
    ok: { label: 'Health ok', variant: 'success', icon: CheckCircle2 },
    warning: { label: 'Health warning', variant: 'warning', icon: AlertCircle },
    critical: { label: 'Health critical', variant: 'danger', icon: XCircle },
    unknown: { label: 'Health unknown', variant: 'neutral', icon: Info },
  },
  report: {
    unknown: { label: 'Report unknown', variant: 'neutral', icon: Info },
    not_started: { label: 'Report not started', variant: 'neutral', icon: Circle },
    preparing: { label: 'Report preparing', variant: 'info', icon: Clock },
    ready: { label: 'Report ready', variant: 'success', icon: CheckCircle2 },
    failed: { label: 'Report failed', variant: 'danger', icon: XCircle },
    unavailable: { label: 'Report unavailable', variant: 'neutral', icon: Info },
    stale: { label: 'Report stale', variant: 'warning', icon: AlertCircle },
  },
  comparison: {
    unknown: { label: 'Comparison unknown', variant: 'neutral', icon: Info },
    eligible: { label: 'Comparison eligible', variant: 'success', icon: CheckCircle2 },
    blocked: { label: 'Comparison blocked', variant: 'warning', icon: AlertCircle },
    not_applicable: { label: 'Comparison N/A', variant: 'neutral', icon: Circle },
  },
  severity: {
    info: { label: 'Info', variant: 'info', icon: Info },
    warning: { label: 'Warning', variant: 'warning', icon: AlertCircle },
    critical: { label: 'Critical', variant: 'danger', icon: XCircle },
    unknown: { label: 'Unknown', variant: 'neutral', icon: Info },
  },
}

const VARIANT_CLASS = {
  success: 'border-emerald-500/28 bg-emerald-500/10 text-emerald-200',
  warning: 'border-amber-500/28 bg-amber-500/10 text-amber-200',
  danger: 'border-rose-500/30 bg-rose-500/10 text-rose-200',
  info: 'border-sky-500/24 bg-sky-500/10 text-sky-200',
  neutral: 'border-white/10 bg-white/[0.04] text-slate-300',
}

export function getStatusMeta(kind, value) {
  const normalized = String(value || 'unknown').trim().toLowerCase()
  return STATUS_META[kind]?.[normalized] || STATUS_META[kind]?.unknown || STATUS_META.severity.unknown
}

export function SemanticStatusBadge({ kind, value, label, className = '' }) {
  const meta = getStatusMeta(kind, value)
  const Icon = meta.icon || Info
  const variantClass = VARIANT_CLASS[meta.variant] || VARIANT_CLASS.neutral
  return (
    <span className={`inline-flex items-center gap-1.5 rounded-[4px] border px-2 py-0.5 text-[10px] font-medium ${variantClass} ${className}`}>
      <Icon className="size-3" />
      {label || meta.label}
    </span>
  )
}
