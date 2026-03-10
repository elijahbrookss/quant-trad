import React from 'react'

/**
 * Badge component for displaying labels and status indicators.
 *
 * @param {Object} props
 * @param {'default'|'success'|'warning'|'danger'|'info'} props.variant - Badge color variant
 * @param {'sm'|'md'|'lg'} props.size - Badge size
 * @param {React.ReactNode} props.children - Badge content
 * @param {string} props.className - Additional CSS classes
 */
export const Badge = ({
  variant = 'default',
  size = 'md',
  children,
  className = '',
}) => {
  const baseClasses = 'inline-flex items-center gap-1.5 rounded-full font-medium'

  const variantClasses = {
    default: 'border border-white/10 bg-white/5 text-slate-100',
    success: 'border border-emerald-500/20 bg-emerald-500/10 text-emerald-200',
    warning: 'border border-amber-500/20 bg-amber-500/10 text-amber-200',
    danger: 'border border-rose-500/20 bg-rose-500/10 text-rose-200',
    info: 'border border-[color:var(--accent-alpha-20)] bg-[color:var(--accent-alpha-10)] text-[color:var(--accent-text-soft)]',
  }

  const sizeClasses = {
    sm: 'px-2 py-0.5 text-[10px]',
    md: 'px-3 py-1 text-xs',
    lg: 'px-4 py-1.5 text-sm',
  }

  const classes = `${baseClasses} ${variantClasses[variant]} ${sizeClasses[size]} ${className}`

  return <span className={classes}>{children}</span>
}
