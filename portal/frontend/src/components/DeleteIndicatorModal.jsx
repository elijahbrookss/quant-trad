import React, { useState, useEffect, useCallback } from "react";
import { Dialog, DialogPanel, DialogTitle, Transition, TransitionChild } from "@headlessui/react";
import { AlertTriangle, X, Loader2, AlertCircle } from "lucide-react";
import { fetchIndicatorStrategies } from "../adapters/indicator.adapter";

/**
 * DeleteIndicatorModal - Confirmation dialog with dependency impact fetch
 * Requires typed "DELETE" confirmation and shows affected strategies
 */
export default function DeleteIndicatorModal({
  open = false,
  indicatorId,
  indicatorName,
  onClose,
  onConfirm,
}) {
  const [confirmText, setConfirmText] = useState("");
  const [loading, setLoading] = useState(false);
  const [deleting, setDeleting] = useState(false);
  const [dependencies, setDependencies] = useState(null);
  const [fetchError, setFetchError] = useState(null);

  const confirmMatch = confirmText === "DELETE";
  const canConfirm = confirmMatch && !deleting;

  // Fetch dependencies when modal opens
  useEffect(() => {
    if (!open || !indicatorId) {
      // Reset state when closed
      setConfirmText("");
      setDependencies(null);
      setFetchError(null);
      setDeleting(false);
      return;
    }

    let cancelled = false;
    setLoading(true);
    setFetchError(null);

    fetchIndicatorStrategies(indicatorId)
      .then((data) => {
        if (!cancelled) {
          setDependencies(data);
          setLoading(false);
        }
      })
      .catch((err) => {
        if (!cancelled) {
          setFetchError(err.message || "Failed to load dependency information");
          setLoading(false);
        }
      });

    return () => {
      cancelled = true;
    };
  }, [open, indicatorId]);

  const handleConfirm = useCallback(async () => {
    if (!canConfirm) return;
    setDeleting(true);
    try {
      await onConfirm?.(indicatorId);
      onClose?.();
    } catch {
      setDeleting(false);
    }
  }, [canConfirm, indicatorId, onConfirm, onClose]);

  const handleKeyDown = useCallback(
    (e) => {
      if (e.key === "Enter" && canConfirm) {
        handleConfirm();
      }
    },
    [canConfirm, handleConfirm]
  );

  const strategies = Array.isArray(dependencies) ? dependencies : dependencies?.strategies || [];
  const hasStrategies = strategies.length > 0;

  return (
    <Transition show={open}>
      <Dialog onClose={onClose || (() => {})} className="relative z-50">
        {/* Backdrop */}
        <TransitionChild
          enter="ease-out duration-200"
          enterFrom="opacity-0"
          enterTo="opacity-100"
          leave="ease-in duration-150"
          leaveFrom="opacity-100"
          leaveTo="opacity-0"
        >
          <div className="fixed inset-0 bg-black/80 backdrop-blur-sm" aria-hidden="true" />
        </TransitionChild>

        {/* Panel */}
        <div className="fixed inset-0 flex items-center justify-center p-4">
          <TransitionChild
            enter="ease-out duration-200"
            enterFrom="opacity-0 scale-95"
            enterTo="opacity-100 scale-100"
            leave="ease-in duration-150"
            leaveFrom="opacity-100 scale-100"
            leaveTo="opacity-0 scale-95"
          >
            <DialogPanel className="w-full max-w-md rounded-2xl border border-white/10 bg-[#14171f] p-6 shadow-2xl">
              {/* Close button */}
              <button
                type="button"
                onClick={onClose}
                className="absolute right-4 top-4 text-slate-400 hover:text-white transition"
                aria-label="Close"
              >
                <X className="size-5" />
              </button>

              {/* Header */}
              <div className="flex items-start gap-4">
                <div className="flex h-12 w-12 shrink-0 items-center justify-center rounded-full bg-rose-500/10 border border-rose-500/20">
                  <AlertTriangle className="size-6 text-rose-400" />
                </div>
                <div className="min-w-0 flex-1">
                  <DialogTitle className="text-lg font-semibold text-white">
                    Delete Indicator
                  </DialogTitle>
                  <p className="mt-1 text-sm text-slate-400">
                    This will permanently delete{" "}
                    <span className="font-medium text-slate-200">{indicatorName || "this indicator"}</span>
                    {" "}and all associated data.
                  </p>
                </div>
              </div>

              {/* Dependencies section */}
              <div className="mt-5">
                {loading ? (
                  <div className="flex items-center gap-3 rounded-lg border border-white/8 bg-white/5 px-4 py-3">
                    <Loader2 className="size-4 animate-spin text-slate-400" />
                    <span className="text-sm text-slate-400">Checking for dependencies...</span>
                  </div>
                ) : fetchError ? (
                  <div className="rounded-lg border border-amber-500/30 bg-amber-500/10 px-4 py-3">
                    <div className="flex items-start gap-3">
                      <AlertCircle className="size-5 text-amber-400 shrink-0 mt-0.5" />
                      <div>
                        <p className="text-sm font-medium text-amber-200">
                          Could not load impact data
                        </p>
                        <p className="mt-1 text-xs text-amber-300/70">
                          {fetchError}
                        </p>
                        <p className="mt-2 text-xs text-amber-200">
                          Proceed with caution - this indicator may be used by strategies.
                        </p>
                      </div>
                    </div>
                  </div>
                ) : (
                  <div className="space-y-3">
                    {/* Strategies impact */}
                    <div className="rounded-lg border border-white/8 bg-white/5 px-4 py-3">
                      <p className="text-xs font-semibold uppercase tracking-wider text-slate-500 mb-2">
                        Affected Strategies
                      </p>
                      {hasStrategies ? (
                        <ul className="space-y-1.5">
                          {strategies.slice(0, 5).map((strategy) => (
                            <li
                              key={strategy.id || strategy.name}
                              className="flex items-center gap-2 text-sm"
                            >
                              <span className="h-1.5 w-1.5 rounded-full bg-rose-400" />
                              <span className="text-slate-200 truncate">
                                {strategy.name || strategy.id}
                              </span>
                            </li>
                          ))}
                          {strategies.length > 5 && (
                            <li className="text-xs text-slate-500">
                              +{strategies.length - 5} more strategies
                            </li>
                          )}
                        </ul>
                      ) : (
                        <p className="text-sm text-slate-400">
                          No strategies are using this indicator.
                        </p>
                      )}
                    </div>

                    {/* Data impact warning */}
                    <div className="rounded-lg border border-rose-500/20 bg-rose-500/5 px-4 py-3">
                      <p className="text-xs text-rose-200">
                        <span className="font-medium">Data that will be deleted:</span>
                        <br />
                        <span className="text-rose-300/70">
                          Computed overlays, generated signals, and indicator configuration.
                        </span>
                      </p>
                    </div>
                  </div>
                )}
              </div>

              {/* Confirmation input */}
              <div className="mt-5">
                <label className="block">
                  <span className="text-xs font-medium text-slate-400">
                    Type <span className="font-mono text-rose-300">DELETE</span> to confirm
                  </span>
                  <input
                    type="text"
                    value={confirmText}
                    onChange={(e) => setConfirmText(e.target.value.toUpperCase())}
                    onKeyDown={handleKeyDown}
                    placeholder="DELETE"
                    autoComplete="off"
                    autoFocus
                    className={`
                      mt-2 w-full rounded-lg border px-4 py-2.5 text-sm font-mono uppercase tracking-wider
                      bg-[#0b111d] text-slate-100 placeholder-slate-600
                      outline-none transition
                      ${
                        confirmMatch
                          ? "border-rose-500/50 ring-2 ring-rose-500/20"
                          : "border-white/10 focus:border-white/20"
                      }
                    `}
                  />
                </label>
              </div>

              {/* Actions */}
              <div className="mt-6 flex items-center justify-end gap-3">
                <button
                  type="button"
                  onClick={onClose}
                  disabled={deleting}
                  className="rounded-lg border border-white/10 bg-white/5 px-4 py-2 text-sm font-medium text-slate-200 transition hover:bg-white/10 disabled:opacity-50"
                >
                  Cancel
                </button>
                <button
                  type="button"
                  onClick={handleConfirm}
                  disabled={!canConfirm}
                  className={`
                    rounded-lg px-4 py-2 text-sm font-semibold transition
                    ${
                      canConfirm
                        ? "bg-rose-600 text-white hover:bg-rose-500"
                        : "bg-rose-600/30 text-rose-300/50 cursor-not-allowed"
                    }
                  `}
                >
                  {deleting ? (
                    <span className="flex items-center gap-2">
                      <Loader2 className="size-4 animate-spin" />
                      Deleting...
                    </span>
                  ) : (
                    "Delete Indicator"
                  )}
                </button>
              </div>
            </DialogPanel>
          </TransitionChild>
        </div>
      </Dialog>
    </Transition>
  );
}
