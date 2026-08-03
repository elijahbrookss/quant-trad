import { useMemo, useState } from 'react'
import { Check, Clipboard, TriangleAlert } from 'lucide-react'

const KNOWN_ERRORS = [
  {
    match: /market_normalization_spec_storage_corrupt|normalization.*hash mismatch|hash mismatch.*normalization/i,
    title: 'Normalization evidence failed an integrity check',
    message: 'A saved normalization specification no longer matches its recorded fingerprint. That evidence is unavailable until the stored record is repaired.',
  },
  {
    match: /attempt history unavailable/i,
    title: 'Recent collector attempts are unavailable',
    message: 'Collector schedules are still visible, but their recent delivery history could not be read.',
  },
  {
    match: /run history unavailable/i,
    title: 'Some run history is unavailable',
    message: 'The inventory is partial because one or more run-history reads failed.',
  },
  {
    match: /networkerror|failed to fetch|network request failed/i,
    title: 'The operator API could not be reached',
    message: 'This view may be stale or incomplete. Check the API and refresh when connectivity returns.',
  },
]

export function humanizeOperatorError(error) {
  const details = String(error?.message || error || 'Unknown operator-console error').trim()
  const known = KNOWN_ERRORS.find((entry) => entry.match.test(details))
  return known
    ? { ...known, details }
    : {
        title: 'Some operator evidence is unavailable',
        message: 'The rest of this view may still be usable. Open or copy the technical details if you need to investigate.',
        details,
      }
}

export function OperatorErrorNotice({ error }) {
  const [copied, setCopied] = useState(false)
  const model = useMemo(() => humanizeOperatorError(error), [error])

  async function copyDetails() {
    try {
      await navigator.clipboard.writeText(model.details)
      setCopied(true)
      window.setTimeout(() => setCopied(false), 1600)
    } catch {
      setCopied(false)
    }
  }

  return (
    <div className="qt2-operator-error" role="alert">
      <TriangleAlert size={16} aria-hidden="true" />
      <div>
        <strong>{model.title}</strong>
        <p>{model.message}</p>
        <details>
          <summary>Technical details</summary>
          <pre>{model.details}</pre>
        </details>
      </div>
      <button type="button" className="qt2-icon-button" onClick={copyDetails} title="Copy technical error details">
        {copied ? <Check size={14} /> : <Clipboard size={14} />}
        <span>{copied ? 'Copied' : 'Copy details'}</span>
      </button>
    </div>
  )
}
