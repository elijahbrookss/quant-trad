export function BotLensPanel({
  eyebrow,
  title,
  subtitle,
  actions = null,
  children,
  className = '',
  bodyClassName = '',
}) {
  return (
    <section className={`qt-ops-panel overflow-hidden ${className}`}>
      <header className="flex flex-wrap items-start justify-between gap-3 border-b border-white/8 px-4 py-3">
        <div>
          {eyebrow ? <p className="qt-ops-kicker">{eyebrow}</p> : null}
          <p className="mt-1 text-sm font-semibold text-slate-100">{title}</p>
          {subtitle ? <p className="mt-1 text-xs leading-relaxed text-slate-400">{subtitle}</p> : null}
        </div>
        {actions ? <div className="flex items-center gap-2">{actions}</div> : null}
      </header>
      <div className={`px-4 py-4 ${bodyClassName}`}>{children}</div>
    </section>
  )
}
