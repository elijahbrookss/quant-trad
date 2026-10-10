import { render, screen } from '@testing-library/react'
import { describe, expect, it, vi } from 'vitest'
import { MemoryRouter, Route, Routes } from 'react-router-dom'
import { ResearchEvidenceRoom } from './ResearchEvidenceRoom.jsx'
import * as api from '../../adapters/research.adapter.js'

vi.mock('../../adapters/research.adapter.js', () => ({ fetchResearchItem: vi.fn(), fetchResearchTrail: vi.fn() }))

describe('Research question publication history', () => {
  it('shows historical conclusions, limitations and completeness meaning', async () => {
    api.fetchResearchItem.mockResolvedValue({ id: 'question', kind: 'study', title: 'Question', status: 'active',
      payload: { question_contract: { question: { question: 'Does the claim hold?', scope: 'Development' },
        publications: [{ revision: 1, publication_hash: 'old-hash', published_at: '2026-10-10',
          conclusion: 'Inconclusive finding', limitations: 'Insufficient outcomes', scope: 'Development',
          completion_meaning: 'reference_complete_interpretation', references: [{ item_id: 'check', resolution: 'resolved_at_publication' }] }] } } })
    api.fetchResearchTrail.mockResolvedValue({ related_items: [], runs: [], summary: {} })
    render(<MemoryRouter initialEntries={['/operations/research/question']}><Routes>
      <Route path="/operations/research/:itemId" element={<ResearchEvidenceRoom />} />
    </Routes></MemoryRouter>)
    expect(await screen.findByText('Published interpretation 1')).toBeVisible()
    expect(screen.getByText('Inconclusive finding')).toBeVisible()
    expect(screen.getByText('Limitations: Insufficient outcomes')).toBeVisible()
    expect(screen.getByText(/scientific validity and actual replay/)).toBeVisible()
    expect(screen.getByText(/distinct from executable StudyDefinition/)).toBeVisible()
  })
})
