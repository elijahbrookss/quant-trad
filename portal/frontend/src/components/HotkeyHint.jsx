export default function HotkeyHint() {
  return (
    <div className="fixed bottom-3 right-3 z-30">
      <div className="rounded-md border border-zinc-200 bg-white/95 px-2.5 py-1.5 text-xs font-semibold text-zinc-500 shadow">
        <kbd className="rounded border border-zinc-200 bg-zinc-50 px-1 py-0.5 text-[11px]">/</kbd> Presets
      </div>
    </div>
  );
}
