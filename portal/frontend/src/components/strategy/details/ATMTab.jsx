import React from 'react'
import ATMTemplateSummary from '../../atm/ATMTemplateSummary'

/**
 * ATM Template tab - wrapper for ATMTemplateSummary component.
 */
export const ATMTab = ({ template, templateOptions, currentTemplateId, onTemplateChange }) => {
  return (
    <ATMTemplateSummary
      template={template}
      templateOptions={templateOptions}
      currentTemplateId={currentTemplateId}
      onTemplateChange={onTemplateChange}
    />
  )
}
