export default function HotkeyHint() {
  return (
    <div className="fixed bottom-3 right-3 z-30">
      <div className="rounded-md bg-black  border-neutral-700 text-white text-xs font-semibold px-2.5 py-1.5 shadow-lg">
        <kbd className="px-1 py-0.5 rounded bg-neutral-800 border border-neutral-700">/</kbd>  Presets
      </div>
    </div>
  );
}
