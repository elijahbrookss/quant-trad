import { render, screen, fireEvent, waitFor } from '@testing-library/react'
import { MemoryRouter } from 'react-router-dom'
import { describe, it, expect, vi } from 'vitest'
import { ResearchActivity, activityWindow, parseUTC } from './ResearchActivity.jsx'
import { fetchActivityRange } from '../../adapters/research.adapter.js'
vi.mock('../../adapters/research.adapter.js',()=>({fetchActivityRange:vi.fn(async()=>({total:0,buckets:[],bucket_unit:'minute',description:'Saved records.'}))}))
describe('research activity range',()=>{
 it('anchors presets and parses explicit UTC without calendar rollover',()=>{
  expect(activityWindow('5m',Date.UTC(2026,0,1,1))).toEqual({from:'2026-01-01T00:55:00.000Z',to:'2026-01-01T01:00:00.000Z'})
  expect(parseUTC('2026-01-01T01:00')).toBe(Date.UTC(2026,0,1,1))
  expect(Number.isNaN(parseUTC('2026-02-30T01:00'))).toBe(true)
 })
 it('applies custom ranges on intent and rejects reversed dates',async()=>{
  render(<MemoryRouter><ResearchActivity /></MemoryRouter>)
  await screen.findByText('No matching activity in this range.')
  fireEvent.change(screen.getByLabelText('Activity time range'),{target:{value:'custom'}})
  fireEvent.change(screen.getByLabelText('Activity from UTC'),{target:{value:'2026-01-02T00:00'}})
  fireEvent.change(screen.getByLabelText('Activity to UTC'),{target:{value:'2026-01-01T00:00'}})
  fireEvent.click(screen.getByRole('button',{name:'Apply range'}))
  expect(screen.getByRole('alert').textContent).toMatch(/start before end/)
  fireEvent.change(screen.getByLabelText('Activity to UTC'),{target:{value:'2026-01-02T02:00'}})
  fireEvent.click(screen.getByRole('button',{name:'Apply range'}))
  await waitFor(()=>expect(fetchActivityRange).toHaveBeenCalledWith('checks_completed',{from:'2026-01-02T00:00:00.000Z',to:'2026-01-02T02:00:00.000Z'},expect.anything()))
 })
 it('handles malformed bookmarked date inputs without a render crash',async()=>{
  render(<MemoryRouter initialEntries={['/?from=bad&to=bad&range=unknown']}><ResearchActivity /></MemoryRouter>)
  expect(screen.getByLabelText('Activity from UTC').value).toBe('')
 })
})
