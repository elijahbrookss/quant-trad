import React from 'react'
import ATMTemplateSummary from '../../atm/ATMTemplateSummary'

/**
 * ATM Template tab - wrapper for ATMTemplateSummary component.
 */
export const ATMTab = ({ template }) => {
  return (
    <>
      <p className="mb-4 text-sm text-slate-400">
        Stops, targets, adjustments, and trailing rules for automated trade management.
      </p>
      <ATMTemplateSummary template={template} />
    </>
  )
}
