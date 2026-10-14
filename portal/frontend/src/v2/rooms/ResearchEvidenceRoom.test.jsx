import { render, screen } from '@testing-library/react'
import { describe, expect, it, vi } from 'vitest'
import { MemoryRouter, Route, Routes } from 'react-router-dom'
import { ResearchEvidenceRoom } from './ResearchEvidenceRoom.jsx'
import * as api from '../../adapters/research.adapter.js'

vi.mock('../../adapters/research.adapter.js', () => ({ fetchResearchItem: vi.fn(), fetchResearchTrail: vi.fn(),
  fetchResearchImages: vi.fn().mockResolvedValue({ items: [] }), researchImageContentUrl: (itemId, imageId) => `/api/research/items/${itemId}/images/${imageId}/content` }))

describe('Research question publication history', () => {
  it('shows historical conclusions, limitations and completeness meaning', async () => {
    api.fetchResearchItem.mockResolvedValue({ id: 'question', kind: 'study', title: 'Question', status: 'active',
      payload: { question_contract: { question: { question: 'Does the claim hold?', scope: 'Development' },
        publications: [{ revision: 1, publication_hash: 'old-hash', published_at: '2026-10-10',
          conclusion: 'Inconclusive finding', limitations: 'Insufficient outcomes', scope: 'Development',
          completion_meaning: 'reference_complete_interpretation', references: [{ item_id: 'check', resolution: 'resolved_at_publication' }] }] } } })
    api.fetchResearchTrail.mockResolvedValue({ related_items: [], runs: [], summary: {} })
    api.fetchResearchImages.mockResolvedValue({ items: [] })
    render(<MemoryRouter initialEntries={['/operations/research/question']}><Routes>
      <Route path="/operations/research/:itemId" element={<ResearchEvidenceRoom />} />
    </Routes></MemoryRouter>)
    expect(await screen.findByText('Published interpretation 1')).toBeVisible()
    expect(screen.getByText('Inconclusive finding')).toBeVisible()
    expect(screen.getByText('Limitations: Insufficient outcomes')).toBeVisible()
    expect(screen.getByText(/scientific validity and actual replay/)).toBeVisible()
    expect(screen.getByText(/distinct from executable StudyDefinition/)).toBeVisible()
  })

  it('renders server images with adjacent explanation and exact binding without mutations', async () => {
    api.fetchResearchItem.mockResolvedValue({ id: 'check', kind: 'research_check', title: 'Check', status: 'tested', payload: {} })
    api.fetchResearchTrail.mockResolvedValue({ related_items: [], runs: [], summary: {} })
    api.fetchResearchImages.mockResolvedValue({ items: [{ image_id: 'image', title: 'Observed outcomes',
      explanation: 'Conditional association; timestamps remain uncertain.', alt_text: 'Observed price path',
      sha256: 'bytes-hash', binding: { kind: 'research_check', result_hash: 'result-hash', evidence_hash: 'evidence-hash' } }] })
    render(<MemoryRouter initialEntries={['/operations/research/check']}><Routes>
      <Route path="/operations/research/:itemId" element={<ResearchEvidenceRoom />} />
    </Routes></MemoryRouter>)
    const image = await screen.findByRole('img', { name: 'Observed price path' })
    expect(image).toHaveAttribute('src', '/api/research/items/check/images/image/content')
    expect(screen.getByText('Conditional association; timestamps remain uncertain.')).toBeVisible()
    expect(screen.getByText(/Presentation only/)).toBeVisible()
    expect(screen.getByText(/bytes-hash/)).toHaveTextContent('result-hash')
  })
})
