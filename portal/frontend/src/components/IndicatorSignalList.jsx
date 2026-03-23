import {
  buildSignalInspectionKey,
  formatSignalEventLabel,
  formatSignalReferenceText,
  formatSignalTimestamp,
} from './indicatorSignalDebug.js';

export default function IndicatorSignalList({
  signals = [],
  selectedSignalKey = null,
  activeInspectionKey = null,
  inspectionBusyKey = null,
  onSelectSignal,
  onInspectSignal,
}) {
  if (!Array.isArray(signals) || signals.length === 0) return null;

  return (
    <div className="rounded-lg border border-white/8 bg-[#0b111d] px-4 py-3">
      <div className="mb-2 flex items-center justify-between gap-3">
        <div className="text-[11px] font-semibold uppercase tracking-[0.22em] text-slate-400">
          Recent Signals
        </div>
        <div className="text-[11px] text-slate-500">
          Click a row to center the chart. Use inspect to pin overlays to that signal bar.
        </div>
      </div>
      <div className="space-y-2">
        {signals.map((signal) => {
          const signalKey = buildSignalInspectionKey(signal);
          const isSelected = selectedSignalKey === signalKey;
          const isInspecting = activeInspectionKey === signalKey;
          const isInspectBusy = inspectionBusyKey === signalKey;
          return (
            <div
              key={signalKey}
              className={`flex w-full flex-wrap items-center justify-between gap-3 rounded-lg border px-3 py-2 text-left transition ${
                isSelected
                  ? 'border-[color:var(--accent-alpha-40)] bg-[color:var(--accent-alpha-08)]'
                  : isInspecting
                    ? 'border-emerald-400/25 bg-emerald-500/8'
                    : 'border-white/6 bg-white/[0.02] hover:border-white/12 hover:bg-white/[0.03]'
              }`}
            >
              <button
                type="button"
                onClick={() => onSelectSignal?.(signal)}
                className="min-w-0 flex-1 text-left"
              >
                <div className="truncate text-sm font-semibold text-slate-100">
                  {formatSignalEventLabel(signal?.event_key)}
                </div>
                <div className="truncate text-xs text-slate-400">
                  {formatSignalReferenceText(signal) || signal?.output_name || 'Signal'}
                  {formatSignalTimestamp(signal) ? ` • ${formatSignalTimestamp(signal)}` : ''}
                </div>
              </button>
              <button
                type="button"
                disabled={isInspectBusy}
                onClick={() => onInspectSignal?.(signal)}
                className={`rounded border px-3 py-1.5 text-xs font-semibold uppercase tracking-[0.18em] transition ${
                  isInspecting
                    ? 'border-emerald-300/30 text-emerald-100 hover:border-emerald-200/50 hover:text-white'
                    : 'border-white/10 text-slate-200 hover:border-[color:var(--accent-alpha-40)] hover:text-white'
                } ${isInspectBusy ? 'cursor-wait opacity-70' : ''}`}
              >
                {isInspectBusy ? 'Loading…' : (isInspecting ? 'Inspecting' : 'Inspect')}
              </button>
            </div>
          );
        })}
      </div>
    </div>
  );
}
