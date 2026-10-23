import { act, fireEvent, render, screen, waitFor } from '@testing-library/react'
import { beforeEach, describe, expect, it, vi } from 'vitest'
import { Link, MemoryRouter, Route, Routes } from 'react-router-dom'
import { ResearchEvidenceRoom } from './ResearchEvidenceRoom.jsx'
import * as api from '../../adapters/research.adapter.js'

vi.mock('../../adapters/research.adapter.js', () => ({ fetchResearchSummary: vi.fn(), fetchResearchRelationships: vi.fn(),
  fetchResearchImages: vi.fn(), fetchResearchComparison: vi.fn(), fetchResearchRecord: vi.fn(), fetchResearchExportPreflight: vi.fn(), downloadResearchExport: vi.fn(),
  researchRecordUrl: id => '/record/' + id, researchImageContentUrl: (id, image) => '/images/' + id + '/' + image }))
const summary = (id = 'question', publication = true) => ({ item: { id, kind: 'study', title: 'Question ' + id, status: 'active' },
  question: { question: 'Does the claim hold?', scope: 'Development' }, history: publication ? [{ revision: 1, publication_hash: 'old-hash' }] : [],
  publication: publication ? { revision: 1, publication_hash: 'old-hash', conclusion: 'Inconclusive finding', limitations: 'Insufficient outcomes', scope: 'Development', completion_meaning: 'reference_complete_interpretation', references: [] } : null })
function mount(path = '/operations/research/question') {
  return render(<MemoryRouter initialEntries={[path]}><Link to="/studies/other">Other record</Link><Routes><Route path="/operations/research/:itemId" element={<ResearchEvidenceRoom />} /><Route path="/studies/:itemId" element={<ResearchEvidenceRoom />} /></Routes></MemoryRouter>)
}
beforeEach(() => {
  api.fetchResearchSummary.mockImplementation(id => Promise.resolve(summary(id)))
  api.fetchResearchRelationships.mockResolvedValue({ items: [], truncated: false })
  api.fetchResearchImages.mockResolvedValue({ items: [] })
})
describe('Generic Study inspection', () => {
  it('keeps legacy deep links, separates conclusion from lifecycle, and defers evidence/images', async () => {
    mount()
    expect(await screen.findByText('Inconclusive finding')).toBeVisible()
    expect(screen.getByText('Insufficient outcomes')).toBeVisible()
    expect(screen.getByText(/Saved summary, not scientific certification/)).toBeVisible()
    expect(api.fetchResearchRelationships).not.toHaveBeenCalled()
    expect(api.fetchResearchImages).not.toHaveBeenCalled()
    expect(api.fetchResearchRecord).not.toHaveBeenCalled()
    fireEvent.click(screen.getByRole('button', { name: 'Evidence', exact: true }))
    expect(await screen.findByText('No saved relationships.')).toBeVisible()
    expect(api.fetchResearchRelationships).toHaveBeenCalled()
  })
  it('renders retained static image custody only in Files and exposes empty/unpublished state', async () => {
    api.fetchResearchImages.mockResolvedValue({ items: [{ image_id: 'image', title: 'Observed outcomes', explanation: 'Conditional association.', alt_text: 'Observed path', sha256: 'bytes-hash', binding: { kind: 'study', publication_hash: 'old-hash', revision: 1 } }] })
    mount()
    await screen.findByText('Inconclusive finding')
    fireEvent.click(screen.getByRole('button', { name: 'Files', exact: true }))
    expect(await screen.findByRole('img', { name: 'Observed path' })).toHaveAttribute('src', '/images/question/image')
    expect(screen.getByText('Conditional association.')).toBeVisible()
    expect(screen.getByText(/Presentation only/)).toBeVisible()
    expect(screen.getByText(/bytes-hash/)).toHaveTextContent('old-hash')
  })
  it('aborts navigation reads and rejects stale completion from a different record', async () => {
    let finish, oldSignal
    api.fetchResearchSummary.mockImplementation((id, params, options) => id === 'question' ? new Promise(resolve => { finish = resolve; oldSignal = options.signal }) : Promise.resolve(summary(id, false)))
    mount()
    await waitFor(() => expect(oldSignal).toBeDefined())
    fireEvent.click(screen.getByText('Other record'))
    expect(await screen.findByText('Question other')).toBeVisible()
    expect(oldSignal.aborted).toBe(true)
    await act(async () => finish(summary('question')))
    expect(screen.queryByText('Question question')).not.toBeInTheDocument()
    expect(screen.getByText('No published summary')).toBeVisible()
  })
  it('shows section errors and missing raw-input disclosure before export', async () => {
    api.fetchResearchExportPreflight.mockResolvedValue({ file_count: 2, uncompressed_bytes: 200, selected_revision: 1, files: [], exclusions: ['Raw OHLCV not included.'] })
    mount('/studies/question?tab=export')
    expect(await screen.findByText(/Saved-record verification only/)).toBeVisible()
    expect(screen.getByRole('button', { name: 'Download evidence ZIP' })).toBeEnabled()
    expect(api.fetchResearchExportPreflight.mock.calls[0][1]).toEqual({ revision: 1, publication_hash: 'old-hash' })
  })
  it('preserves section errors, technical details and explicit retry', async () => {
    api.fetchResearchImages.mockRejectedValue(new Error('Image metadata unavailable'))
    mount('/studies/question?tab=files')
    await screen.findByRole('button', { name: 'Retry section' })
    fireEvent.click(screen.getByText('Technical details'))
    expect(screen.getByText('Image metadata unavailable')).toBeVisible()
    expect(screen.getByRole('button', { name: 'Retry section' })).toBeVisible()
  })
  it('compares actual related Check identities only after explicit intent', async () => {
    const data = summary()
    data.publication.references = [{ item_id: 'check-a', kind: 'research_check' }, { item_id: 'check-b', kind: 'research_check' }]
    api.fetchResearchSummary.mockResolvedValue(data)
    api.fetchResearchComparison.mockResolvedValue({ left: { id: 'check-a' }, right: { id: 'check-b' }, deltas: {} })
    mount('/studies/question?tab=evidence')
    await screen.findByText('Compare related Checks')
    expect(api.fetchResearchComparison).not.toHaveBeenCalled()
    fireEvent.click(screen.getByText('Compare related Checks'))
    fireEvent.change(screen.getByLabelText('Left Check'), { target: { value: 'check-a' } })
    fireEvent.change(screen.getByLabelText('Right Check'), { target: { value: 'check-b' } })
    fireEvent.click(screen.getByRole('button', { name: 'Compare', exact: true }))
    expect(await screen.findByText(/Inspect each scope/)).toBeVisible()
    expect(api.fetchResearchComparison.mock.calls[0].slice(0, 2)).toEqual(['check-a', 'check-b'])
  })

  it('cancels only a pending page load and offers reload', async () => {
    api.fetchResearchImages.mockImplementation((id, options) => new Promise((resolve, reject) => {
      options.signal.addEventListener('abort', () => reject(new DOMException('Page read cancelled', 'AbortError')))
    }))
    mount('/studies/question?tab=files')
    await screen.findByText('Question question')
    fireEvent.click(screen.getByRole('button', { name: 'Cancel load' }))
    expect(await screen.findByText(/Page load cancelled. Research jobs continue./)).toBeVisible()
    expect(screen.getByRole('button', { name: 'Load again' })).toBeVisible()
  })

  it.each([true, false])('clears comparisons when refresh settles to changed publication=%s', async published => {
    const data = summary()
    data.publication.references = [{ item_id: 'check-a', kind: 'research_check' }, { item_id: 'check-b', kind: 'research_check' }]
    let settleRefresh
    api.fetchResearchSummary.mockResolvedValueOnce(data).mockImplementation(() => new Promise(resolve => { settleRefresh = resolve }))
    api.fetchResearchComparison.mockResolvedValue({ left: { id: 'check-a' }, right: { id: 'check-b' } })
    mount('/studies/question?tab=evidence')
    await screen.findByText('Compare related Checks')
    fireEvent.change(screen.getByLabelText('Left Check'), { target: { value: 'check-a' } })
    fireEvent.change(screen.getByLabelText('Right Check'), { target: { value: 'check-b' } })
    fireEvent.click(screen.getByRole('button', { name: 'Compare', exact: true }))
    await screen.findByText(/Inspect each scope/)
    fireEvent.click(screen.getByRole('button', { name: 'Refresh summary' }))
    await waitFor(() => expect(settleRefresh).toBeDefined())
    const next = structuredClone(data); next.publication = published ? { ...next.publication, publication_hash: 'new-hash' } : null
    await act(async () => settleRefresh(next))
    if (published) {
      expect(await screen.findByText('Compare related Checks')).toBeVisible()
      expect(screen.getByLabelText('Left Check').value).toBe('')
      expect(screen.getByRole('button', { name: 'Compare', exact: true })).toBeDisabled()
    } else expect(screen.queryByText('Compare related Checks')).not.toBeInTheDocument()
    expect(screen.queryByText(/Inspect each scope/)).not.toBeInTheDocument()
  })

  it('aborts an unpublished record raw read when its summary is refreshed', async () => {
    let settleRefresh, rawSignal
    api.fetchResearchSummary.mockResolvedValueOnce(summary('question', false)).mockImplementation(() => new Promise(resolve => { settleRefresh = resolve }))
    api.fetchResearchRecord.mockImplementation((id, options) => new Promise((resolve, reject) => {
      rawSignal = options.signal
      options.signal.addEventListener('abort', () => reject(new DOMException('cancelled', 'AbortError')))
    }))
    mount('/studies/question?tab=evidence')
    fireEvent.click(await screen.findByRole('button', { name: 'Load exact retained payload' }))
    await waitFor(() => expect(rawSignal).toBeDefined())
    fireEvent.click(screen.getByRole('button', { name: 'Refresh summary' }))
    await waitFor(() => expect(rawSignal.aborted).toBe(true))
    await act(async () => settleRefresh(summary('question', false)))
    expect(await screen.findByRole('button', { name: 'Load exact retained payload' })).toBeVisible()
    expect(screen.queryByRole('button', { name: 'Hide exact payload' })).not.toBeInTheDocument()
  })

})

it('keeps an intentional historical summary until View latest is selected', async () => {
 const old = summary('historical-selection'); old.publication.conclusion = 'Original bounded finding'
 old.history = [{revision:1,publication_hash:'old-hash'},{revision:2,publication_hash:'middle-hash'},{revision:3,publication_hash:'latest-hash'}]
 const latest = {...old, publication:{...old.publication,revision:3,publication_hash:'latest-hash',conclusion:'This bounded comparison is closed; no new experiment is authorized.'}}
 api.fetchResearchSummary.mockImplementation((id, params) => Promise.resolve(params.revision === 1 ? old : latest))
 mount('/studies/historical-selection?revision=1&tab=summary')
 expect(await screen.findByText('Original bounded finding')).toBeVisible()
 expect(screen.getByText('Historical summary · v1 · latest is v3')).toBeVisible()
 expect(screen.getByRole('heading',{name:'Historical summary'})).toBeVisible()
 expect(screen.queryByText(latest.publication.conclusion)).toBeNull()
 fireEvent.click(screen.getByRole('button',{name:'View latest'}))
 expect(await screen.findByText(latest.publication.conclusion)).toBeVisible()
 expect(screen.getByRole('heading',{name:'Latest published summary'})).toBeVisible()
 expect(screen.queryByRole('button',{name:'View latest'})).toBeNull()
 expect(api.fetchResearchSummary.mock.calls.at(-1)[1].revision).toBeUndefined()
})
