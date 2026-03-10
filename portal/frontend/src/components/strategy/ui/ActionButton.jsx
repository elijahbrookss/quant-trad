const ActionButton = ({ variant = 'default', className = '', ...props }) => {
  const base =
    'rounded-md px-2.5 py-1.5 text-xs font-medium transition duration-150 focus:outline-none focus-visible:ring-2 focus-visible:ring-offset-2 focus-visible:ring-offset-[#10121a]'

  const styles = {
    default: `${base} bg-[color:var(--accent-alpha-20)] text-[color:var(--accent-text-strong)] hover:bg-[color:var(--accent-alpha-30)]`,
    ghost: `${base} bg-white/[0.04] text-slate-300 hover:bg-white/[0.08] hover:text-white`,
    danger: `${base} bg-rose-500/10 text-rose-400 hover:bg-rose-500/20 hover:text-rose-300`,
    subtle: `${base} bg-transparent text-slate-500 hover:text-slate-200`,
  }

  const classes = [styles[variant] || styles.default, className].filter(Boolean).join(' ')
  return <button className={classes} {...props} />
}

export default ActionButton
