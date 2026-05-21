const TabButton = ({ active, onClick, children, icon }) => (
  <button
    type="button"
    onClick={onClick}
    className={`flex items-center gap-1.5 border-b-2 px-3 py-2.5 text-xs font-medium uppercase tracking-wide transition-colors focus:outline-none ${
      active
        ? 'border-[color:var(--accent-base)] text-white'
        : 'border-transparent text-slate-500 hover:border-white/20 hover:text-slate-300'
    }`}
  >
    {icon && <span className="h-3.5 w-3.5">{icon}</span>}
    {children}
  </button>
)

export default TabButton
