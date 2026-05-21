export default function ActiveSignalInspectionBanner({
  activeInspection,
  indicatorName,
  onClear,
}) {
  if (!activeInspection) return null;

  return (
    <div className="flex flex-wrap items-center justify-between gap-3 rounded-lg border border-emerald-400/25 bg-emerald-500/8 px-4 py-3">
      <div className="min-w-0 text-sm text-emerald-50">
        <div className="font-semibold">
          Inspecting signal-time overlay state
          {indicatorName ? ` for ${indicatorName}` : ''}
        </div>
        <div className="text-xs text-emerald-100/80">
          {activeInspection.label || 'Signal'}
          {activeInspection.reference ? ` • ${activeInspection.reference}` : ''}
          {activeInspection.cursorTime ? ` • ${activeInspection.cursorTime}` : ''}
        </div>
      </div>
      <button
        type="button"
        onClick={onClear}
        className="rounded border border-emerald-300/30 px-3 py-1.5 text-xs font-semibold uppercase tracking-[0.18em] text-emerald-100 transition hover:border-emerald-200/50 hover:text-white"
      >
        Exit inspect
      </button>
    </div>
  );
}
