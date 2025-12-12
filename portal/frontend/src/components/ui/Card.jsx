import React from 'react'

/**
 * Card container component with consistent styling.
 *
 * @param {Object} props
 * @param {string} props.title - Optional card title
 * @param {string} props.description - Optional card description
 * @param {React.ReactNode} props.action - Optional action element (button, link, etc.)
 * @param {React.ReactNode} props.children - Card content
 * @param {string} props.className - Additional CSS classes
 * @param {'sm'|'md'|'lg'} props.padding - Padding size
 */
export const Card = ({
  title,
  description,
  action,
  children,
  className = '',
  padding = 'md',
}) => {
  const paddingClasses = {
    sm: 'p-3',
    md: 'p-4',
    lg: 'p-6',
  }

  const baseClasses = 'rounded-2xl border border-white/10 bg-black/20'
  const classes = `${baseClasses} ${paddingClasses[padding]} ${className}`

  return (
    <div className={classes}>
      {(title || description || action) && (
        <div className="mb-4 flex items-start justify-between gap-3">
          <div>
            {title && (
              <h3 className="text-xs font-semibold uppercase tracking-[0.3em] text-slate-400">
                {title}
              </h3>
            )}
            {description && (
              <p className="mt-1 text-sm text-slate-500">
                {description}
              </p>
            )}
          </div>
          {action && <div>{action}</div>}
        </div>
      )}
      {children}
    </div>
  )
}

/**
 * Simple card without header.
 */
export const SimpleCard = ({ children, className = '', padding = 'md' }) => {
  const paddingClasses = {
    sm: 'p-3',
    md: 'p-4',
    lg: 'p-6',
  }

  const classes = `rounded-2xl border border-white/10 bg-black/20 ${paddingClasses[padding]} ${className}`

  return <div className={classes}>{children}</div>
}
