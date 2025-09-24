export default function HotkeyHint() {
  return (
    <div className="fixed bottom-3 right-3 z-30">
      <div className="rounded-md border border-neutral-800 bg-neutral-900/90 px-2.5 py-1.5 text-xs font-semibold text-neutral-300 shadow">
        <kbd className="rounded border border-neutral-700 bg-neutral-900 px-1 py-0.5 text-[11px] text-neutral-200">/</kbd> Presets
      </div>
    </div>
  );
}
