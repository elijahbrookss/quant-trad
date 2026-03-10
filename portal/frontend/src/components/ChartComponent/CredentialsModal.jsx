/**
 * CredentialsModal - Modal for entering provider API credentials
 *
 * Isolated modal component for managing provider/venue API keys.
 * Part of ChartComponent refactoring to reduce complexity.
 */
export function CredentialsModal({
  isOpen,
  providerId,
  venueId,
  requiredFields = [],
  inputs,
  onInputChange,
  saving,
  error,
  onClose,
  onSave,
}) {
  if (!isOpen) return null;

  const displayName = venueId || providerId || 'this provider';

  const handleInputChange = (key, value) => {
    if (typeof onInputChange === 'function') {
      onInputChange((prev) => ({ ...prev, [key]: value }));
    }
  };

  const handleSave = () => {
    if (typeof onSave === 'function') {
      void onSave();
    }
  };

  return (
    <div className="fixed inset-0 z-[10000] flex items-center justify-center bg-black/60 px-4 py-6">
      <div className="w-full max-w-lg rounded-2xl border border-white/10 bg-[#0b0f18]/95 p-6 shadow-[0_40px_120px_-60px_rgba(0,0,0,0.85)]">
        <div className="flex items-start justify-between gap-3">
          <div>
            <p className="text-[11px] uppercase tracking-[0.32em] text-[color:var(--accent-text-kicker)]">
              Provider credentials
            </p>
            <h3 className="text-lg font-semibold text-slate-50">Add API keys</h3>
            <p className="text-sm text-slate-400">
              Required to enable {displayName}.
            </p>
          </div>
          <button
            type="button"
            onClick={onClose}
            className="rounded-full bg-white/5 px-3 py-1 text-sm text-slate-300 hover:bg-white/10"
          >
            Close
          </button>
        </div>

        <div className="mt-4 space-y-3">
          {requiredFields.map((key) => (
            <div key={key} className="space-y-1">
              <label className="text-[11px] uppercase tracking-[0.22em] text-slate-500">
                {key}
              </label>
              <input
                type="password"
                value={inputs?.[key] || ''}
                onChange={(e) => handleInputChange(key, e.target.value)}
                className="w-full rounded-lg border border-white/10 bg-[#0f1626] px-3 py-2 text-sm text-slate-100 outline-none transition focus:border-[color:var(--accent-alpha-40)]"
                placeholder={`Enter ${key}`}
              />
            </div>
          ))}

          {error ? (
            <div className="rounded-xl border border-rose-500/40 bg-rose-500/10 px-3 py-2 text-sm text-rose-100">
              {error}
            </div>
          ) : null}
        </div>

        <div className="mt-5 flex items-center justify-end gap-3">
          <button
            type="button"
            onClick={onClose}
            className="rounded-full border border-white/10 px-4 py-2 text-sm text-slate-200 hover:border-white/20 hover:bg-white/5"
          >
            Cancel
          </button>
          <button
            type="button"
            onClick={handleSave}
            disabled={saving}
            className="rounded-full bg-[color:var(--accent-alpha-25)] px-4 py-2 text-sm font-semibold text-[color:var(--accent-text-bright)] shadow-[0_10px_40px_-12px_var(--accent-shadow-strong)] transition hover:bg-[color:var(--accent-alpha-30)] disabled:cursor-not-allowed disabled:opacity-60"
          >
            {saving ? 'Saving…' : 'Save keys'}
          </button>
        </div>
      </div>
    </div>
  );
}

export default CredentialsModal;
